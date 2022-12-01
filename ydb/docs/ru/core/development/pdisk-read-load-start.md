# PDisk нагрузка на чтение

## Структура {#proto}

```proto
message TPDiskReadLoadStart {
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

{% include notitle [addition](../_includes/addition.md) %}
