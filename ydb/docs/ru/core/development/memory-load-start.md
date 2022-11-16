# Проверка производительности работы памяти

## Описание

```proto
message TMemoryLoadStart {
    optional uint64 Tag = 1;
    optional uint32 DurationSeconds = 2;
    optional uint64 BlockSize = 3;
    optional uint64 IntervalUs = 4;
}
```

## Примеры

```proto
NodeId: 3
Event: { MemoryLoadStart: {
    DurationSeconds: 120
    IntervalUs: 1000
    BlockSize: 4096
}}
```