# VDiskLoad

Подает на VDisk нагрузку write-only. Имитирует Distributed Storage Proxy. Результатом теста является производительность записи в операциях в секунду.

{% include notitle [addition](../_includes/addition.md) %}

## Спецификация актора {#proto}

```proto
message TVDiskLoad {
    optional uint64 Tag = 1;

    // full VDisk identifier
    optional NKikimrBlobStorage.TVDiskID VDiskId = 2;

    reserved 3; // obsolete field
    reserved 4; // obsolete field
    optional NKikimrBlobStorage.TGroupInfo GroupInfo = 16;

    // tablet id, channel and generation used in blob ids and barriers
    optional uint64 TabletId = 5;
    optional uint32 Channel = 6;
    optional uint32 Generation = 7;

    // duration of the test in seconds
    optional uint32 DurationSeconds = 8;

    // a distribution of intervals between adjacent writes
    repeated TIntervalInfo WriteIntervals = 9;

    // a distribution of write block sizes (expressed in bytes of BlobSize; i.e. PartSize bytes are actually written)
    repeated TSizeInfo WriteSizes = 10;

    // maximum number of unconfirmed parallel writes
    optional uint32 InFlightPutsMax = 11;

    // soft maximum of total in flight put bytes
    optional uint64 InFlightPutBytesMax = 12;

    // put handle class
    optional NKikimrBlobStorage.EPutHandleClass PutHandleClass = 13;

    // a distribution of intervals between barrier advances
    repeated TIntervalInfo BarrierAdvanceIntervals = 14;

    // minimum distance kept between current Step of written blobs and CollectStep of barriers
    optional uint32 StepDistance = 15;
}
```
