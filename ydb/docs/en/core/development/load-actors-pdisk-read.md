# PDiskReadLoad

Generates a read-only load on the PDisk. Simulates the VDisk. The actor creates chunks on the specified PDisk, writes random data to them, and reads the data from them using the specified parameters. The test outputs the read performance in bytes per second.

{% include notitle [addition](../_includes/addition.md) %}

## Actor specification {#proto}

```proto
message TPDiskReadLoad {
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
    optional uint32 InFlightReads = 7;
    optional bool Sequential = 9 [default = false];

    optional uint32 IntervalMsMin = 10;
    optional uint32 IntervalMsMax = 11;

    optional bool IsWardenlessTest = 13 [default = false];
}
```
