# PDiskWriteLoad

Generates a write-only load on the PDisk. Simulates the VDisk. The actor creates chunks on the specified PDisk and writes random data with the specified parameters to them. The test outputs the write performance in bytes per second.

{% include notitle [addition](../_includes/addition.md) %}

## Actor specification {#proto}

<!--
```proto
enum ELogMode {
    LOG_PARALLEL = 1;
    LOG_SEQUENTIAL = 2;
    LOG_NONE = 3;
}
message TPDiskWriteLoad {
    message TChunkInfo {
        optional uint32 Slots = 1; // number of slots per chunk
        optional uint32 Weight = 2; // probability weight
    }
    optional uint64 Tag = 1;
    optional uint32 PDiskId = 2;
    optional uint64 PDiskGuid = 3;
    optional NKikimrBlobStorage.TVDiskID VDiskId = 4;
    repeated TChunkInfo Chunks = 5;
    optional uint32 DurationSeconds = 6;
    optional uint32 InFlightWrites = 7;
    optional ELogMode LogMode = 8;
    optional bool Sequential = 9 [default = true];
    optional uint32 IntervalMsMin = 10;
    optional uint32 IntervalMsMax = 11;
    optional bool Reuse = 12 [default = false];
    optional bool IsWardenlessTest = 13 [default = false];
}
```
-->

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
