# PDiskWriteLoad

Tests the performance of writes to the PDisk. The load is generated on behalf of a VDisk. The actor creates chunks on the specified PDisk and writes random data to them. After the load stops, the data written by the actor is deleted.

You can generate two types of load:

* _Continuous_: The actor ensures the specified number of requests are run concurrently. To generate a continuous load, set a zero interval between requests, e.g., `IntervalMsMin: 0` and `IntervalMsMax: 0`, while keeping the `InFlightWrites` parameter different from zero.
* _Interval_: The actor runs requests at preset intervals. To generate interval load, set a non-zero interval between requests, e.g., `IntervalMsMin: 10` and `IntervalMsMax: 100`. You can set the maximum number of in-flight requests using the `InFlightWrites` parameter. If its value is `0`, their number is unlimited.

## Actor parameters {#options}

{% include [load-actors-params](../_includes/load-actors-params.md) %}

| Parameter | Description |
--- | ---
| `PDiskId` | ID of the Pdisk being loaded on the node. |
| `PDiskGuid` | Globally unique ID of the PDisk being loaded. |
| `VDiskId` | The load is generated on behalf of a VDisk with the following parameters:<ul><li>`GroupID`: Group ID.</li><li>`GroupGeneration`: Group generation.</li><li>`Ring`: Group ring ID.</li><li>`Domain`: Ring fail domain ID.</li><li>`VDisk`: Index of the VDisk in the fail domain.</li></ul> |
| `Chunks` | Chunk parameters.<br/>`Slots`: Number of slots per chunk, determines the write size.<br/>You can specify multiple `Chunks`, in which case a specific chunk to write data to is selected based on its `Weight`. |
| `DurationSeconds` | Load duration in seconds. |
| `IntervalMsMin`,<br/>`IntervalMsMax` | Minimum and maximum intervals between requests under interval load, in milliseconds. The interval value is selected randomly from the specified range. |
| `InFlightWrites` | Number of simultaneously processed write requests. |
| `LogMode` | Logging mode. In `LOG_SEQUENTIAL` mode, data is first written to a chunk and then, once the write is committed, to a log. |
| `Sequential` | Type of writes.<ul><li>`True`: Sequential.</li><li>`False`: Random.</li></ul> |
| `IsWardenlessTest` | Set it to `False` in case the PDiskReadLoad actor is run on the cluster; otherwise, e.g. when it is run during unit tests, set it to `True`. |

<!--

```proto
enum ELogMode {
    LOG_PARALLEL = 1; // Make parallel writes to a chunk and log (about the actual write) and consider the write completed only if both writes are successful
    LOG_SEQUENTIAL = 2; // First write data to the chunk and then to the log. Consider the write completed after writing data to the log
    LOG_NONE = 3;
}

message TPDiskWriteLoad {
    message TChunkInfo {
        optional uint32 Slots = 1; // the number of slots per chunk. Actually determines the size of writes/reads,
                                   // that can be calculated by dividing the chunk size by the number of slots
        optional uint32 Weight = 2; // the weight that data will be written to this chunk with
    }
    optional uint64 Tag = 1; // optional. If not specified, the tag is assigned automatically
    optional uint32 PDiskId = 2; // required. ID to generate a load on. You can find it in the cluster UI
    optional uint64 PDiskGuid = 3; // required. GUID of the PDisk that the load is generated on. You can find it in the UI on the PDisk page
    optional NKikimrBlobStorage.TVDiskID VDiskId = 4; // required. The load actor will use this VDiskId to present itself to the PDisk.
                                                    // It must not be duplicated across different load actors.
    repeated TChunkInfo Chunks = 5; // required. Describes the number of chunks to be used for load generation. Allows varying the load.
                                    // For example, you can create two chunks, one with a large number of slots and one with a small number of slots, and set the chunk weights
                                    // so that 95% of writes were small, while 5% were large, simulating VDisk compaction
    optional uint32 DurationSeconds = 6; // required. Test duration
    optional uint32 InFlightWrites = 7; // required. Limits the number of in-flight requests to the PDisk
    optional ELogMode LogMode = 8; // required. See above: ELogMode
    optional bool Sequential = 9 [default = true]; // optional. Make sequential or random writes to chunk slots.
    optional uint32 IntervalMsMin = 10; // optional. See below
    optional uint32 IntervalMsMax = 11; // optional. Allows generating a severe load that will send requests strictly on a regular basis.
                                        // The interval between requests is randomly selected from the range [IntervalMsMin, IntervalMsMax].
                                        // Considers the number of InFlightWrites and doesn't exceed it. If no IntervalMsMin and IntervalMsMax are specified, it only considers
                                        // InFlightWrites
    optional bool Reuse = 12 [default = false]; // shows if the actor must reuse the fully written chunks or allocate
                                                // new chunks and release the old ones
    optional bool IsWardenlessTest = 13 [default = false]; // allows using them in tests with no NodeWarden
}
```
 -->

## Examples {#example}

The following actor writes data blocks of `32` MB during `120` seconds with `64` in-flight requests (continuous load):

```proto
PDiskWriteLoad: {
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
    InFlightWrites: 64
    LogMode: LOG_SEQUENTIAL
    Sequential: false
    IsWardenlessTest: false
}
```

When viewing test results, the following value should be of most interest to you:

* `Average speed since start`: Average write speed since start, in MB/s, e.g., `615.484013`.
