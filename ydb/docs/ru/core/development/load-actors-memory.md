# MemoryLoad

Тестирует производительность аллокаторов памяти. Аллоцирует блоки памяти указанного размера через заданные промежутки времени. Результатом теста 

{% include notitle [addition](../_includes/addition.md) %}

## Спецификация актора {#proto}

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

## Примеры {#examples}

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
