# {{ ydb-short-name }} System Requirements and Recommendations

This section provides recommendations for deploying {{ ydb-short-name }} clusters that are relevant regardless of the chosen deployment method ([Ansible](../deployment-options/ansible/index.md), [Kubernetes](../deployment-options/kubernetes/index.md), or [manual](../deployment-options/manual/index.md)).

## Hardware Configuration {#hardware}

The fault-tolerance requirements determine the necessary number of servers and disks. For more information, see [{#T}](../../concepts/topology.md).

### Processor (CPU)

A {{ ydb-short-name }} server can only run on x86-64 processors with AVX2 instruction support: Intel Haswell (4th generation) and later, AMD EPYC and later.

The ARM architecture is currently not supported for production {{ ydb-short-name }} server deployments. For local development on Apple Silicon Macs, you can run {{ ydb-short-name }} in Docker with x86_64 instruction emulation (Rosetta); see the Docker tab in [{#T}](../../quickstart.md).

{% note info %}

CPU power-saving modes (C-states, P-states, and the cpufreq scaling governor) can increase latency due to delays when leaving idle states.

For production workloads, use the **performance** cpufreq governor (and equivalent BIOS settings).

See [{#T}](./cpu-production-settings.md) for details.

{% endnote %}

### RAM

We recommend using error-correcting code (ECC) memory to protect against hardware failures.

### Disk Subsystem

A {{ ydb-short-name }} server can run on servers with any disk type (HDD/SSD/NVMe). However, we recommend using SSD/NVMe disks for better performance.

{% include [_includes/storage-device-requirements.md](../../_includes/storage-device-requirements.md) %}

{{ ydb-short-name }} works with disk drives directly and does not use any filesystem to store data. Don't mount a file system or perform other operations with partitions used by {{ ydb-short-name }}. Also, avoid sharing the {{ ydb-short-name }}'s block device with the operating system and different processes, which can lead to significant performance degradation.

Prefer to use physical local disk drives for {{ ydb-short-name }} instead of virtual or network storage devices.

Remember that {{ ydb-short-name }} uses some disk space for internal needs when planning disk capacity. For example, on a medium-sized cluster of 8 nodes, you can expect approximately 100 GB to be consumed for a static group on the whole cluster. On a large cluster with more than 1500 nodes, this will be about 200 GB. There are also 25.6 GB of logs on each Pdisk and a system area on each Pdisk. Its size depends on the size of the Pdisk, but is no less than 0.2 GB.

The disk is also used for [spilling](../../concepts/glossary.md#spilling), a memory management mechanism that temporarily saves intermediate query execution results to disk when RAM is insufficient. This is important to consider when planning disk capacity. Detailed spilling configuration is described in the [Spilling Configuration](../../reference/configuration/table_service_config.md) section.

## Software Configuration {#software}

A {{ ydb-short-name }} server can be run on servers with a Linux operating system, kernel 4.19 and higher, and libc 2.30. For example, Ubuntu 20.04, Debian 11, Fedora 34, or newer releases. For optimal performance, we recommend using more recent Linux kernel versions (6.6 or newer), as they include significant improvements in I/O subsystems, scheduling, and memory management that positively impact database workloads.

{{ ydb-short-name }} uses the [TCMalloc](https://google.github.io/tcmalloc) memory allocator. To make it efficient, [enable](https://google.github.io/tcmalloc/tuning.html#system-level-optimizations) Transparent Huge Pages and Memory overcommitment.

To improve disk and network I/O performance in a trusted environment, you can disable IOMMU by setting the Linux boot parameter `intel_iommu=off` or `amd_iommu=off`. In an untrusted environment, as well as under strict security requirements or with active virtualization use (for example, PCI passthrough and device isolation), disabling IOMMU is not recommended. In such cases, use `intel_iommu=on iommu=pt` or `amd_iommu=on iommu=pt`.

The environment can be considered trusted if only {{ ydb-short-name }} and user-controlled applications are running on the server. Configurations that run third-party applications or virtual machines should be considered untrusted.

If the server has more than 32 CPU cores, to increase {{ ydb-short-name }} performance, run each dynamic node in a separate taskset/cpuset of 10 to 32 cores. For example, with 128 CPU cores, an optimal setup is to run 4 dynamic nodes, each in its own 32-core taskset. Cores within one taskset/cpuset should belong to the same NUMA node.

MacOS and Windows operating systems are currently unsupported for running production {{ ydb-short-name }} servers. However, running {{ ydb-short-name }} in a [Docker container](../../quickstart.md) on them is acceptable for development and functional testing.
