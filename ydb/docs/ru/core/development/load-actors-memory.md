# Проверка производительности памяти

## Структура {#proto}

```proto
message TMemoryLoadStart {
    optional uint64 Tag = 1;
    optional uint32 DurationSeconds = 2;
    optional uint64 BlockSize = 3;
    optional uint64 IntervalUs = 4;
}
```
<!-- 
## Параметры {#options}

### MemoryLoadStart

Параметр | Описание
--- | ---
`Tag` | Тип: `uint64`.
`DurationSeconds` | Тип: `uint32`.
`BlockSize` | Тип: `uint64`.
`IntervalUs` | Тип: `uint64`.
-->

## Примеры {#examples}

```proto
NodeId: 1
Event: { MemoryLoadStart: {
    DurationSeconds: 120
    BlockSize: 4096
    IntervalUs: 1000
}}
```

{% include notitle [addition](../_includes/addition.md) %}
