# PDiskReadLoad

Tests the performance of reads from the PDisk. The load is generated on behalf of a VDisk. The actor creates chunks on the specified PDisk, writes random data to them, and reads the data from them using the specified parameters. After the load stops, the data written by the actor is deleted.

You can generate two types of load:

* _Continuous_: The actor ensures the specified number of requests are run concurrently. To generate continuous load, set a zero interval between requests (e.g., `IntervalMsMin: 0` and `IntervalMsMax: 0`), while keeping the `InFlightReads` parameter different from zero.
* _Interval_: The actor runs requests at preset intervals. To generate interval load, set a non-zero interval between requests, e.g., `IntervalMsMin: 10` and `IntervalMsMax: 100`. You can set the maximum number of in-flight requests using the `InFlightReads` parameter. If its value is `0`, their number is unlimited.

## Actor parameters {#options}

{% include [load-actors-params](../_includes/load-actors-params.md) %}

| Parameter | Description |
--- | ---
| `PDiskId` | ID of the Pdisk being loaded on the node. |
| `PDiskGuid` | Globally unique ID of the PDisk being loaded. |
| `VDiskId` | The load is generated on behalf of a VDisk with the following parameters:<ul><li>`GroupID`: Group ID.</li><li>`GroupGeneration`: Group generation.</li><li>`Ring`: Group ring ID.</li><li>`Domain`: Ring fail domain ID.</li><li>`VDisk`: Index of the VDisk in the fail domain.</li></ul> |
| `Chunks` | Chunk parameters.<br/>`Slots`: Number of slots per chunk, determines the write size.<br/>You can specify multiple `Chunks`, in which case a specific chunk to read data from is selected based on its `Weight`. |
| `DurationSeconds` | Load duration in seconds. |
| `IntervalMsMin`,<br/>`IntervalMsMax` | Minimum and maximum intervals between requests under interval load, in milliseconds. The interval value is selected randomly from the specified range. |
| `InFlightReads` | Number of simultaneously processed read requests. |
| `Sequential` | Type of reads.<ul><li>`True`: Sequential.</li><li>`False`: Random.</li></ul> |
| `IsWardenlessTest` | Set it to `False` in case the PDiskReadLoad actor is run on the cluster; otherwise, e.g. when it is run during unit tests, set it to `True`. |

## Examples {#examples}

The following actor reads data blocks of `32`MB during `120` seconds with `64` in-flight requests (continuous load):

```proto
PDiskReadLoad: {
    PDiskId: 1000
    PDiskGuid: 2258451612736857634
    VDiskId: {
        GroupID: 11234
        GroupGeneration: 5
        Ring: 1
        Domain: 1
        VDisk: 3
    }
    Chunks: { Slots: 4096 Weight: 1 }
    Chunks: { Slots: 4096 Weight: 1 }
    Chunks: { Slots: 4096 Weight: 1 }
    Chunks: { Slots: 4096 Weight: 1 }
    Chunks: { Slots: 4096 Weight: 1 }
    Chunks: { Slots: 4096 Weight: 1 }
    Chunks: { Slots: 4096 Weight: 1 }
    Chunks: { Slots: 4096 Weight: 1 }
    DurationSeconds: 120
    IntervalMsMin: 0
    IntervalMsMax: 0
    InFlightReads: 64
    Sequential: false
    IsWardenlessTest: false
}
```

When viewing test results, the following value should be of most interest to you:

* `Average speed since start`: Average read speed since start, in MB/s, e.g., `1257.148154`.
