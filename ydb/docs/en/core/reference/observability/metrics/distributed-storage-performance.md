# Distributed Storage performance metrics

Distributed storage throughput is limited by the resources of physical devices in the cluster and can provide low response times if the load does not exceed this capacity. Performance metrics provide information about the amount of available resources of physical devices and allow assessing their consumption level. Tracking the values of performance metrics helps to determine whether the necessary conditions for low response time guarantees are met, specifically, that the average load does not exceed the available limit and that there are no short-term load bursts.

## Request cost model

The request cost is an estimate of the time that a physical device spends performing a given operation. The request cost is calculated using a simple physical device model. It assumes that the physical device can only handle one request for reading or writing at a time. The operation execution takes a certain amount of the device’s working time; therefore, the total time spent on requests over a certain period cannot exceed the duration of that period.

The request cost is calculated using a linear formula:

$$
cost(operation) = A + operation.size() \times B
$$

The physical rationale behind the linear dependency is as follows: coefficient $A$ is the time needed for the physical device to access the data, and coefficient $B$ is the time required to read or write one byte of data.

The coefficients $A$ and $B$ depend on the request and device types. These coefficients were measured experimentally for each device type and each request type.

In {{ ydb-short-name }}, all physical devices are divided into three types: HDD, SATA SSD (further referred to as SSD), and NVMe SSD (further referred to as NVMe). HDDs are rotating hard drives characterized by high data access time. SSD and NVMe types differ in their interfaces: NVMe provides higher operation speed.

Operations are divided into three types: reads, writes, and huge-writes. The division of writes into regular and huge-writes is due to the specifics of handling write requests on VDisks.

In addition to user requests, the distributed storage is also loaded by the background processes of compaction, scrubbing, and defragmentation, as well as internal communication between VDisks. The compaction process can create particularly high loads when there is a substantial flow of small blob writes.

## Available disk time {#disk-time-available}

The PDisk scheduler manages the execution order of requests from its client VDisks. PDisk fairly divides the device's time among its VDisks, ensuring that each of the $N$ VDisks is guaranteed $1/N$ seconds of the physical device's working time each second. We estimate the available time of the physical device that the PDisk scheduler allocates to the client VDisk based on the information about the number of neighboring VDisks for each VDisk, denoted as $N$, and the configurable parameter `DiskTimeAvailableScale`. This estimate is called DiskTimeAvailable and is calculated using the formula:

$$
    DiskTimeAvailable = \dfrac{1000000000}{N} \cdot \dfrac{DiskTimeAvailableScale}{1000}
$$

## Load burst detection {#burst-detection}

A burst is a sharp, short-term increase in the load on a VDisk, which may lead to higher response times of operations. The values of sensors on cluster nodes are collected at certain intervals, for example, every 15 seconds, making it impossible to reliably detect short-term events using only the metrics of request cost and available disk time. A modified [Token Bucket algorithm](https://en.wikipedia.org/wiki/Token_bucket) is used to address this issue. In this modification, the bucket can have a negative number of tokens and such a state is called underflow. Each VDisk is associated with a separate burst detector – a specialized object that monitors load bursts using the aforementioned algorithm. The minimum expected response time, at which an increase in load is considered a burst, is determined by the configurable parameter `BurstThresholdNs`. The bucket will underflow if the calculated time needed to process the requests in nanoseconds exceeds the `BurstThresholdNs` value.

## Performance metrics

Performance metrics are calculated based on the following VDisk sensors:
| Sensor Name           | Description                                                                                                                   | Units             |
|-----------------------|-------------------------------------------------------------------------------------------------------------------------------|-------------------|
| `DiskTimeAvailable`   | Available disk time.                                                                                                          | arbitrary units   |
| `UserDiskCost`        | Total cost of requests a VDisk receives from the DS Proxy.                                                                    | arbitrary units   |
| `InternalDiskCost`    | Total cost of requests received by a VDisk from another VDisk in the group, for example, as part of the replication process.  | arbitrary units   |
| `CompactionDiskCost`  | Total cost of requests the VDisk sends as part of the compaction process.                                                     | arbitrary units   |
| `DefragDiskCost`      | Total cost of requests the VDisk sends as part of the defragmentation process.                                                | arbitrary units   |
| `ScrubDiskCost`       | Total cost of requests the VDisk sends as part of the scrubbing process.                                                      | arbitrary units   |
| `BurstDetector_redMs` | Period of time, during which the Token Bucket was in the underflow state.                                         | ms                |

`DiskTimeAvailable` and the request cost are estimates of available and consumed bandwidth, respectively, and are not actually measured. Therefore, both of these values are provided in arbitrary units that, in terms of their physical meaning, are close to nanoseconds.

Performance metrics are displayed on a [dedicated Grafana dashboard](grafana-dashboards.md#ds-performance).

## Conditions for Distributed Storage guarantees {#requirements}

The {{ ydb-short-name }} distributed storage can ensure low response times only under the following conditions:

1. $DiskTimeAvailable >= UserDiskCost + InternalDiskCost + CompactionDiskCost + DefragDiskCost + ScrubDiskCost$ — The average load does not exceed the maximum allowed load.
2. $BurstDetector_redMs = 0$ — There are no short-term load bursts, which would lead to request queues on handlers.

## Performance metrics configuration

Since the coefficients for the request cost formula were measured on specific physical devices from development clusters, and the performance of other devices may vary, the metrics may require additional adjustments to be used as a source of Distributed Storage low response time guarantees. Performance metric parameters can be managed via [dynamic cluster configuration](../../../maintenance/manual/dynamic-config.md) and the Immediate Controls mechanism without restarting {{ ydb-short-name }} processes.

| Parameter Name                        | Description                                                                                   |  Units            | Default Value |
|---------------------------------------|-----------------------------------------------------------------------------------------------|-------------------|---------------|
| `disk_time_available_scale_hdd`       | [`DiskTimeAvailableScale` parameter](#disk-time-available) for VDisks running on HDD devices.   | arbitrary units   | `1000`        |
| `disk_time_available_scale_ssd`       | [`DiskTimeAvailableScale` parameter](#disk-time-available) for VDisks running on SSD devices.   | arbitrary units   | `1000`        |
| `disk_time_available_partition_nvme`  | [`DiskTimeAvailableScale` parameter](#disk-time-available) for VDisks running on NVMe devices.  | arbitrary units   | `1000`        |
| `burst_threshold_ns_hdd`              | [`BurstThresholdNs` parameter](#burst-detection) for VDisks running on HDD devices.             | ns                | `200000000`   |
| `burst_threshold_ns_ssd`              | [`BurstThresholdNs` parameter](#burst-detection) for VDisks running on SSD devices.             | ns                | `50000000`    |
| `burst_threshold_ns_nvme`             | [`BurstThresholdNs` parameter](#burst-detection) for VDisks running on NVMe devices.            | ns                | `32000000`    |

### Configuration examples

If a given {{ ydb-short-name }} cluster uses NVMe devices and delivers performance that is 10% higher than the baseline, add the following section to the `immediate_controls_config` in the dynamic configuration of the cluster:

```text
vdisk_controls:
  disk_time_available_scale_nvme: 1100
```

If a given {{ ydb-short-name }} cluster is using HDD devices and under its workload conditions, the maximum tolerable response time is 500 ms, add the following section to the `immediate_controls_config` in the dynamic configuration of the cluster:

```text
vdisk_controls:
  burst_threshold_ns_hdd: 500000000
```

## How to compare cluster performance with the baseline

To compare the performance of Distributed Storage in a cluster with the baseline, you need to load the distributed storage with requests to the point where the VDisks cannot process the incoming request flow. At this moment, requests start to queue up, and the response time of the VDisks increases sharply. Compute the value $D$ just before the overload:
$$
D = \frac{UserDiskCost + InternalDiskCost + CompactionDiskCost + DefragDiskCost + ScrubDiskCost}{DiskTimeAvailable}
$$
Set the `disk_time_available_scale_<used-device-type>` configuration parameter to the calculated rounded value of $D$, multiplied by 1000. We assume that the physical devices in the user cluster are comparable in performance to the baseline. So the `disk_time_available_scale_<used-device-type>` parameter is set to 1000 by default.

{% note tip %}

You can use [Storage LoadActor](../../../contributor/load-actors-storage.md) to generate the load.

{% endnote %}
