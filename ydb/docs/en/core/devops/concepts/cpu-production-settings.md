# CPU settings for production {#cpu-production-settings}

## Problem {#problem}

CPU power-saving modes (such as Intel SpeedStep, AMD Cool'n'Quiet, C-states, and P-states) can hurt {{ ydb-short-name }} performance for several reasons:

1. **Unstable CPU frequency** — {{ ydb-short-name }} expects stable CPU throughput for predictable operation.
2. **Core affinity issues** — changing per-core frequency can break load balancing across threads.
3. **Mode-switching latency** — transitions between power-saving states add extra delays.
4. **Unpredictable throughput** — operations may run at different speeds depending on the current CPU mode.

Typical symptoms:

* Unstable query latency.
* Unexplained performance drops.
* Uneven load across cluster nodes.
* Erratic behavior under heavy load.

## Solution {#solution}

### Recommended BIOS/UEFI settings {#bios}

For {{ ydb-short-name }} servers, configure the following BIOS/UEFI settings:

1. **CPU Power Management** — set to **Performance** or **Max Performance**.
2. **Intel Turbo Boost** — enable for maximum performance.
3. **C-states** — disable or limit to C1/C1E.
4. **P-states** — use a fixed maximum frequency.
5. **Intel SpeedStep / AMD Cool'n'Quiet** — disable.
6. **Package C-State Limit** — set to C0 or C1.

### Operating system settings {#os}

On Linux, for deployments **not** using the [Ansible role](../../deployment-options/ansible/index.md) (which configures this automatically), additionally consider:

```bash
# Force the performance cpufreq governor
echo 'performance' | tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor

# Pin minimum frequency to maximum
echo $(cat /sys/devices/system/cpu/cpu0/cpufreq/cpuinfo_max_freq) | \
tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_min_freq

# Reduce PCIe ASPM power saving
echo 'performance' > /sys/module/pcie_aspm/parameters/policy
```

### Verifying current settings {#check}

Useful checks (after applying settings, the first command should output `performance` for every CPU core):

```bash
# Current governor
cat /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor

# Available frequencies
cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_available_frequencies

# C-states
cat /sys/devices/system/cpu/cpu0/cpuidle/state*/name
```

## Rationale {#rationale}

As a high-performance distributed database, {{ ydb-short-name }} needs stable and predictable CPU performance. Client-style power-saving modes are a poor fit for server workloads that require sustained availability and low latency.

A stable CPU frequency helps with:

* Predictable operation latency.
* Correct behavior of load-balancing mechanisms.
* Steady throughput under load.
* Minimal delays for transaction processing.

## Exceptions {#exceptions}

Compromises may be acceptable when:

* **Test environments** — energy efficiency matters more than raw performance.
* **Standby nodes** — not serving active traffic.
* **Low-load deployments** — power savings are a priority.

For busy production clusters, follow the recommendations above.
