# BlobStorage Performance Metrics

BlobStorage has a specific throughput limited by the resources of physical devices in the cluster and can provide low response times if the load does not exceed this capacity. Performance metrics display the amount of available resources of physical devices and allow for the assessment of their consumption level. By tracking the values of performance metrics we can monitor whether the necessary conditions for low response time guatantees are met, specifically, that the average load does not exceed the available limit and that there are no short-term load bursts.

### Request Cost Model

The cost of a request is an estimate of the time that a physical device spends to perform this operation. The cost of a request is calculated using a simple model of the physical device. We assume that the physical device can only handle one request for reading or writing at a time. The execution of the operation takes a certain amount of the device’s working time; therefore, the total time spent on requests over a certain period cannot exceed the duration of that period.

The cost of a request is calculated using a linear formula:

$$
cost(operation) = A + operation.size() \times B
$$

The physical rationale behind the linear dependency is as follows: coefficient $A$ is the time needed for the physical device to access the data, and coefficient $B$ is the time required to read or write one byte of data.

The coefficients $A$ and $B$ depend on the type of request and the type of device. These coefficients were measured experimentally for each device type and each request type.

In the {{ ydb-short-name }} system, all physical devices are divided into three types: HDD, SATA SSD (further referred to as SSD), and NVME SSD (further referred to as NVME). HDDs are rotating hard drives, which are characterized by high data access time. SSD and NVME types differ in their interfaces: NVME provides a higher operation speed.

Operations are divided into three types: reads, writes, and huge-writes. The division of writes into regular and huge-writes is due to the specifics of handling write requests on VDisks.

Besides user requests, the load on BlobStorage is created by background processes of compaction, scrubbing, and defragmentation, as well as internal communication between VDisks. The compaction process can create particularly high loads when there is a substantial flow of small blob writings.

### Available Disk Time {#diskTimeAvailable}

The PDisk scheduler manages the execution order of requests by the PDisk from its client VDisks. PDisk fairly divides the device's time among its VDisks, meaning that each of the $n$ VDisks is guaranteed $1/n$ seconds of the physical device's working time each second. Based on the information about the number of neighboring VDisks for each VDisk, which we denote as $N$, and the configurable parameter `DiskTimeAvailableScale`, the available disk time, further referred to as `DiskTimeAvailable`, is calculated by the formula:
$$
    DiskTimeAvailable = \dfrac{1,000,000,000}{N} \cdot \dfrac{DiskTimeAvailableScale}{1,000}
$$

### Load Burst Detector {#burstDetector}

A burst is a sharp short-term increase in the load on a VDisk, which can lead to degradation in the response time of operations. The values of sensors on cluster nodes are collected at certain intervals, for example, every 15 seconds, making it impossible to reliably detect short-term events using only the metrics of request cost and available disk time. To address this issue, a modified [Token Bucket algorithm](https://en.wikipedia.org/wiki/Token_bucket) is used, in our modification the bucket can have a negative number of tokens and we call such a state underflow. A separate Token Bucket object is associated with each VDisk. The minimum expected response time, at which an increase in load is considered a burst, is determined by the configurable parameter `BurstThresholdNs`. The bucket will underflow if the calculated amount of time needed to process the requests in nanoseconds exceeds the `BurstThresholdNs` value.

### Performance Metrics

Performance metrics are calculated based on the following VDisk sensors:
| Sensor Name | Units | Description |
|-------------|-------|-------------|
| `DiskTimeAvailable` | arb. units | Available disk time. |
| `UserDiskCost` | arb. units | Total cost of requests received by the VDisk from the BlobStorage Proxy. |
| `InternalDiskCost` | arb. units | Total cost of requests received by the VDisk from another VDisk in the group. These requests can be created by, for example, replication process. |
| `CompactionDiskCost` | arb. units | Total cost of requests sent by the VDisk as part of the compaction process. |
| `DefragDiskCost` | arb. units | Total cost of requests sent by the VDisk as part of the defragmentation process. |
| `ScrubDiskCost` | arb. units | Total cost of requests sent by the VDisk as part of the scrubbing process. |
| `BurstDetector_redMs` | ms | The duration in milliseconds during which the Token Bucket was in an underflow state.

### Conditions for Distributed Storage Guarantees {#requirements}

The distributed storage {{ ydb-short-name }} can ensure low response times only under the following conditions:

1. `DiskTimeAvailable >= UserDiskCost + InternalDiskCost + CompactionDiskCost + DefragDiskCost + ScrubDiskCost` — The average load does not exceed the maximum allowable.
2. `BurstDetector_redMs = 0` — There are no short-term load bursts, which lead to formation of request queues on handlers.

### Performance Metrics Configuration

Since the coefficients for the request cost formula were measured on specific physical devices from development clusters, and the performance of other devices may vary, the metrics may require additional adjustments to be used as a source of guarantees for BlobStorage. Performance metric parameters can be managed via dynamic cluster configuration and the Immediate Controls mechanism without restarting {{ ydb-short-name }} processes.

| Parameter Name | Description | Default Value |
|----------------|-------------|---------------|
| `disk_time_available_scale_hdd` | [`DiskTimeAvailableScale` parameter](#diskTimeAvailable) for VDisks running on HDD devices. | `1000` |
| `disk_time_available_scale_ssd` | [`DiskTimeAvailableScale` parameter](#diskTimeAvailable) for VDisks running on SSD devices. | `1000` |
| `disk_time_available_partition_nvme` | [`DiskTimeAvailableScale` parameter](#diskTimeAvailable) for VDisks running on NVME devices. | `1000` |
| `burst_threshold_hdd` | [`BurstThresholdNs` parameter](#burstDetector) for VDisks running on HDD devices. | `200000000` |
| `burst_threshold_ssd` | [`BurstThresholdNs` parameter](#burstDetector) for VDisks running on SSD devices. | `50000000` |
| `burst_threshold_nvme` | [`BurstThresholdNs` parameter](#burstDetector) for VDisks running on NVME devices. | `32000000` |

#### Configuration Examples

If your system uses NVME devices and delivers performance that is 10% higher than the baseline, add the following section to the `immediate_controls_config` in the dynamic configuration of the cluster:

```
vdisk_controls:
  disk_time_available_scale_nvme: 1100
```

If you are using HDD devices and under your workload conditions, the maximum tolerable response time is 500 ms, add the following section to the `immediate_controls_config` in the dynamic configuration of the cluster:

```
vdisk_controls:
  burst_threshold_hdd: 500000000
```

### How to Compare the Performance of Your Installation with the Baseline

To compare the performance of BlobStorage in your system with the baseline, you need to load the distributed storage with requests to the point where the VDisks cannot process the incoming request flow. At this moment, requests start to queue up, and the response time of the VDisks increases sharply. Compute the value $D$ just before the overload:
$$
D = \frac{UserDiskCost + InternalDiskCost + CompactionDiskCost + DefragDiskCost + ScrubDiskCost}{DiskTimeAvailable}
$$
Set the `disk_time_available_scale_<your device type>` configuration parameter equal to the calculated value of $D$, multiplied by 1000 and rounded. We assume that the physical devices in the user cluster are comparable in performance to the baseline, hence by default the `disk_time_available_scale_<your device type>` parameter is set to 1000.

Such a load can be created, for example, using [Storage LoadActor](../../../contributor/load-actors-storage.md).

