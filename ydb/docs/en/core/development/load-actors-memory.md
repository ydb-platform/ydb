# MemoryLoad

Tests the performance of memory allocators. Allocates memory blocks of the specified size at certain intervals.

{% include notitle [addition](../_includes/addition.md) %}

## Actor specification {#proto}

```proto
message TMemoryLoad {
    optional uint64 Tag = 1;
    optional uint32 DurationSeconds = 2;
    optional uint64 BlockSize = 3;
    optional uint64 IntervalUs = 4;
}
```
<!--
## Параметры актора {#options}

Параметр | Описание
--- | ---
`Tag` | Тип: `uint64`.
`DurationSeconds` | Тип: `uint32`.
`BlockSize` | Тип: `uint64`.
`IntervalUs` | Тип: `uint64`.
-->

## Examples {#examples}

{% list tabs %}

- CLI

   ```proto
   NodeId: 1
   Event: { MemoryLoad: {
       DurationSeconds: 120
       BlockSize: 4096
       IntervalUs: 1000
   }}
   ```

{% endlist %}
