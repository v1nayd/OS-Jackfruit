# OS-Jackfruit: Multi-Container Runtime

This repository contains OS-Jackfruit, a lightweight Linux container runtime written in C. It features a persistent user-space supervisor daemon and a custom Linux Kernel Module (LKM) for atomic physical memory enforcement.

## 1. Team Information
* **Dasari Vinay** - PES2UG24CS145
* **Dwijesh Krishna G** - PES2UG24CS163

---

## 2. Build, Load, and Run Instructions

Follow these step-by-step instructions to compile the project, load the kernel module, and start the supervisor from scratch on a fresh Ubuntu 22.04/24.04 VM.

### Prerequisites
Ensure the environment is prepared by running the preflight check and downloading the Alpine mini root filesystem:
```bash
# Install dependencies
sudo apt update
sudo apt install -y build-essential linux-headers-$(uname -r)

# Check environment
cd boilerplate
chmod +x environment-check.sh
sudo ./environment-check.sh

# Prepare rootfs copies
mkdir rootfs-base
wget [https://dl-cdn.alpinelinux.org/alpine/v3.20/releases/x86_64/alpine-minirootfs-3.20.3-x86_64.tar.gz](https://dl-cdn.alpinelinux.org/alpine/v3.20/releases/x86_64/alpine-minirootfs-3.20.3-x86_64.tar.gz)
tar -xzf alpine-minirootfs-3.20.3-x86_64.tar.gz -C rootfs-base
```

### Build and Load
```bash
# 1. Compile the user-space runtime and kernel module
make

# 2. Load the kernel module
sudo insmod monitor.ko

# 3. Verify the control device was created successfully
ls -l /dev/container_monitor
```

### Run and Test
```bash
# 1. Start the supervisor daemon (runs in the foreground of this terminal)
sudo ./engine supervisor ./rootfs-base

# -- OPEN A SECOND TERMINAL -- #

# 2. Create isolated rootfs environments for the containers
cp -a ./rootfs-base ./rootfs-alpha
cp -a ./rootfs-base ./rootfs-beta

# 3. Launch the containers in the background
sudo ./engine start alpha ./rootfs-alpha /bin/sh --soft-mib 40 --hard-mib 64
sudo ./engine start beta ./rootfs-beta /bin/sh --soft-mib 40 --hard-mib 64

# 4. List active tracked containers and verify metadata
sudo ./engine ps

# 5. Inspect the bounded-buffer logs for a specific container
sudo ./engine logs alpha
```

### Clean Teardown
```bash
# 1. Gracefully stop all running containers
sudo ./engine stop alpha
sudo ./engine stop beta

# 2. Stop the supervisor (go to the first terminal and press Ctrl+C)

# 3. Unload the kernel module
sudo rmmod monitor

# 4. Verify clean teardown (no output should appear except potentially grep/ibus)
ps aux | grep engine | grep -v grep
lsmod | grep monitor
```

---

## 3. Demo with Screenshots

*Note: Replace the bracketed placeholders below with the actual paths to the screenshots captured during your demonstration runs.*

1. **Multi-container supervision:** Two containers running concurrently under one supervisor daemon.
   ![Multi-container supervision](Outputs/1-a.png)
   ![Multi-container supervision](Outputs/1-b.png)
2. **Metadata tracking:** Output of the `ps` command showing tracked container states and host PIDs.
   ![Metadata tracking](Outputs/2.png)
3. **Bounded-buffer logging:** Extracted contents from the logging pipeline validating producer/consumer concurrency.
   ![Bounded-buffer logging](Outputs/3.png)
4. **CLI and IPC:** `engine start` commands being issued and successfully communicating with the UNIX domain socket.
   ![CLI and IPC](Outputs/4.png)
5. **Soft-limit warning:** `dmesg` output demonstrating a soft-limit warning event logged from Ring 0.
   ![Soft-limit warning](Outputs/4.png)
6. **Hard-limit enforcement:** `dmesg` capturing a container being killed after breaching its hard limit, and supervisor metadata reflecting the kill.
   ![Hard-limit enforcement](Outputs/4.png)
7. **Scheduling experiment:** Output comparing completion times of CPU-bound vs. I/O-bound tasks running concurrently.
   ![Scheduling experiment](Outputs/5-a.png)
   ![Scheduling experiment](Outputs/5-b.png)
8. **Clean teardown:** `ps aux` and `rmmod` logs verifying all children reaped and modules detached with zero lingering zombies.
   ![Clean teardown](Outputs/6.png)

---

## 4. Engineering Analysis

### Isolation Mechanisms
The runtime achieves process isolation by leveraging the `clone()` system call with specific namespace flags (`CLONE_NEWPID` for a private process tree, and `CLONE_NEWUTS` for a local hostname). Following process creation, a `chroot` operation creates filesystem isolation by changing the apparent root directory for the container to a dedicated Alpine mini-rootfs folder, ensuring the container cannot traverse into the host OS. However, unlike full virtual machines, these containers still share the host's underlying Linux kernel, hardware drivers, and (since network namespaces were not isolated) the host's network stack.

### Supervisor and Process Lifecycle
A persistent, long-running supervisor daemon manages the container lifecycle. In Linux, when a process terminates, it becomes a "zombie"—its entry remains in the process table so the parent can read its exit status. The supervisor acts similarly to the `init` process (PID 1) for its containers. It implements a robust `SIGCHLD` signal handler to asynchronously call `waitpid()`, reaping terminated child containers to release OS resources and safely updating their shared user-space metadata states.

### IPC, Threads, and Synchronization
The project implements two IPC mechanisms: UNIX domain sockets for CLI-to-supervisor control paths, and standard UNIX pipes for capturing container standard output/error. To safely capture logs concurrently, a bounded-buffer is used. Without synchronization, a severe race condition exists: if two container threads attempt to write logs simultaneously, they might both read the same buffer `tail` index, causing one log entry to overwrite the other. To prevent data corruption, the shared buffer is strictly synchronized using a `pthread_mutex_t` alongside two condition variables (`not_full` and `not_empty`). This classic producer-consumer approach ensures exclusive access and prevents wasteful busy-waiting CPU cycles.

### Memory Management and Enforcement
The custom kernel module enforces physical memory restrictions by checking the Resident Set Size (RSS). RSS measures the portion of a process's memory held actively in RAM, but it does not measure memory paged out to swap space or memory shared with other processes. The module implements two policies: a soft limit designed to emit a warning to alert administrators, and a hard limit that strictly terminates the process (via `SIGKILL`) to protect the host. Enforcement belongs in kernel space because a user-space polling mechanism is fundamentally susceptible to scheduling delays. The LKM safely evaluates RSS using a `spinlock_irqsave` inside a softirq timer, bypassing user-space delays and preventing rapid Out-Of-Memory (OOM) crashes.

### Scheduling Behavior
The controlled experiments running `cpu_hog` (nice 10) and `io_pulse` (nice -10) workloads provided clear evidence of the Linux Completely Fair Scheduler (CFS) logic. When the I/O-bound task was assigned a higher priority, it consistently completed its iterations faster than the CPU-bound task. This illustrates that the CFS prioritizes "interactivity" and "responsiveness." Because the I/O task frequently yields the CPU to wait for disk operations, it is rewarded with a higher dynamic priority.

---

## 5. Design Decisions and Tradeoffs

**1. IPC Choice (CLI to Supervisor): UNIX Domain Sockets**
* **Decision:** Chose UNIX Domain Sockets over TCP sockets or simple FIFOs.
* **Tradeoff:** It requires managing a physical socket file on the filesystem (e.g., `/tmp/mini_runtime.sock`), which must be explicitly cleaned up on exit. If the supervisor crashes, a stale socket file can cause "Address already in use" errors on the next boot.
* **Justification:** UNIX sockets are significantly faster and more secure for local IPC than TCP, and they allow for structured, bidirectional message passing which is difficult to achieve cleanly with FIFOs.

**2. Filesystem Isolation: `chroot`**
* **Decision:** Used `chroot` to change the root directory for the container process.
* **Tradeoff:** `chroot` is generally considered less secure than `pivot_root`. It does not completely detach the old root filesystem, theoretically allowing a sophisticated breakout via directory traversal (`..`) if not perfectly managed.
* **Justification:** For an academic container runtime, `chroot` provides the necessary practical isolation without the extensive mount-namespace setup complexity required by `pivot_root`.

**3. Kernel Synchronization: Spinlock**
* **Decision:** Used a `spinlock` (`spin_lock_irqsave`) to protect the linked list of monitored PIDs in the kernel module.
* **Tradeoff:** Spinlocks waste CPU cycles by "spinning" if a core is waiting for a lock, which can degrade performance if held for too long.
* **Justification:** The memory check happens inside a kernel timer interrupt context (softirq). The kernel scheduler prohibits "sleeping" or blocking in atomic contexts, meaning a standard mutex cannot be used. Since list operations (adding/removing a PID) are extremely fast, the spinlock overhead is negligible and perfectly suited for the task.

---

## 6. Scheduler Experiment Results

**Configuration:**
Two containers were launched simultaneously using the supervisor, executing distinct workloads to observe the behavior of the Completely Fair Scheduler (CFS).
* **Container A (CPU-Bound):** Ran `cpu_hog` mapped to a low priority using `--nice 10`.
* **Container B (I/O-Bound):** Ran `io_pulse` mapped to a high priority using `--nice -10`.

**Observations & Results:**
* The `io_pulse` container maintained consistent, rapid iteration timing despite the `cpu_hog` container attempting to continuously saturate the CPU core.
* The raw accumulator logs demonstrated that the high-priority I/O task received CPU time almost immediately when returning from disk sleep states.
* **Conclusion:** This directly reflects the Linux CFS utilizing `vruntime` to ensure interactive or I/O-bound tasks get low-latency access to the CPU. By penalizing the CPU hog with a higher nice value and rewarding the I/O task for yielding the CPU during disk writes, the scheduler successfully balances throughput with responsiveness.
