# PDiskLogLoad

All VDisks hosted on a certain PDisk log data about their own performance to the common PDisk log. VDisks gradually delete their obsolete data at the beginning of the log to free up disk space. Sometimes, after one VDisk completes logging data and before another one starts logging it, a section with useless obsolete data may appear. In this case, such data is deleted automatically, and the PDiskLogLoad actor will perform a test to check whether such an operation is running correctly.

<center>

![load-actors](../_assets/pdisklogload.svg)

</center>

{% note info %}

This ad-hoc actor is used for testing specific functionality. This is not a load actor. It is designed to check whether something works properly.

{% endnote %}

## Actor parameters {#options}

{% include [load-actors-params](../_includes/load-actors-params.md) %}

| Parameter | Description |
--- | ---
| `PDiskId` | ID of the Pdisk being loaded on the node. |
| `PDiskGuid` | Globally unique ID of the PDisk being loaded. |
| `VDiskId` | Parameters of the VDisk used to generate load.<ul><li>`GroupID`: Group ID.</li><li>`GroupGeneration`: Group generation.</li><li>`Ring`: Group ring ID.</li><li>`Domain`: Ring fail domain ID.</li><li>`VDisk`: Index of the VDisk in the fail domain.</li></ul> |
| `MaxInFlight` | Number of simultaneously processed requests. |
| `SizeIntervalMin` | Minimum size of log record in bytes. |
| `SizeIntervalMax` | Maximum size of log record in bytes. |
| `BurstInterval` | Interval between logging sessions in bytes. |
| `BurstSize` | Total amount of data to log per session, in bytes. |
| `StorageDuration` | Virtual time in bytes. Indicates how long the VDisk should store its data in the log. |
| `IsWardenlessTest` | Set it to `False` in case the PDiskReadLoad actor is run on the cluster; otherwise, e.g. when it is run during unit tests, set it to `True`. |

## Examples {#example}

The following actor simulates the performance of two VDisks. The first VDisk logs a message of `65536` bytes every `65536` bytes and deletes data that exceeds `1048576` bytes. The second one writes `1024` bytes as messages of `128` bytes every `2147483647` bytes and deletes data that exceeds `2147483647` bytes.

```proto
PDiskLogLoad: {
    Tag: 1
    PDiskId: 1
    PDiskGuid: 12345
    DurationSeconds: 60
    Workers: {
        VDiskId: {GroupID: 1 GroupGeneration: 5 Ring: 1 Domain: 1 VDisk: 1}
        MaxInFlight: 1
        SizeIntervalMin: 65536
        SizeIntervalMax: 65536
        BurstInterval: 65536
        BurstSize: 65536
        StorageDuration: 1048576
    }
    Workers: {
        VDiskId: {GroupID: 2 GroupGeneration: 5 Ring: 1 Domain: 1 VDisk: 1}
        MaxInFlight: 1
        SizeIntervalMin: 128
        SizeIntervalMax: 128
        BurstInterval: 2147483647
        BurstSize: 1024
        StorageDuration: 2147483647
    }
    IsWardenlessTest: false
}
```

The test passes if none of the cluster nodes got overloaded and the status of the PDisk in question is `Normal`. You can check this using the cluster Embedded UI.
