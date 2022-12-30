# PDiskLogLoad

Тестирует вырезание из середины лога PDisk. Не является нагружающим, в первую очередь направлен на тестирование корректности.

{% include notitle [addition](../_includes/addition.md) %}

## Спецификация актора {#proto}

```proto
message TPDiskLogLoad {
    message TWorkerConfig {
        optional NKikimrBlobStorage.TVDiskID VDiskId = 1;
        optional uint32 MaxInFlight = 2;

        // Measurement units of all parameters is bytes
        optional uint32 SizeIntervalMin = 3;
        optional uint32 SizeIntervalMax = 4;
        optional uint64 BurstInterval = 5;
        optional uint64 BurstSize = 6;
        optional uint64 StorageDuration = 7;

        optional uint64 MaxTotalBytesWritten = 8;
    }

    optional uint64 Tag = 1;
    optional uint32 PDiskId = 2;
    optional uint64 PDiskGuid = 3;

    optional uint32 DurationSeconds = 5;
    repeated TWorkerConfig Workers = 7;

    optional bool IsWardenlessTest = 8 [default = false];
}
```
