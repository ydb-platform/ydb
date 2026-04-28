# Как выбрать количество партиций {#choosing-partition-count}

Количество партиций влияет на то, сколько таблеток [data shard](../../../concepts/glossary.md#data-shard) обрабатывают таблицу параллельно и как данные распределяются по узлам.

Рекомендации из модели данных {{ ydb-short-name }}:

* Одна партиция размещается на одном сервере и для операций изменения использует **не более одного ядра CPU**. Поэтому для таблиц с ожидаемой высокой нагрузкой имеет смысл задавать [`AUTO_PARTITIONING_MIN_PARTITIONS_COUNT`](../../../concepts/datamodel/table.md#auto_partitioning_min_partitions_count) **не меньше числа узлов** (серверов) базы, а лучше **порядка суммарного числа ядер**, выделенных под нагрузку.

* [`AUTO_PARTITIONING_MAX_PARTITIONS_COUNT`](../../../concepts/datamodel/table.md#auto_partitioning_max_partitions_count) ограничивает верхнюю границу роста числа партиций; при достижении потолка новые split по размеру или нагрузке прекращаются — следите за метриками и при необходимости увеличивайте лимит заранее.

* При включённом автопартицировании по нагрузке **не оставляйте** минимальное число партиций равным **1**, если нагрузка «плавающая»: после спада нагрузки merge может свести таблицу к одной партиции и потребоваться заново делить при новом всплеске. Подробнее — [{#T}](../../../concepts/datamodel/table.md#auto_partitioning_min_partitions_count).

Применить рекомендации на практике можно инструкцией YQL:

```yql
ALTER TABLE `my_table` SET (
    AUTO_PARTITIONING_BY_LOAD = ENABLED,
    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 10,
    AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 50
);
```

Начальное равномерное распределение по ключу задаётся опционально через [`UNIFORM_PARTITIONS`](../../../concepts/datamodel/table.md#uniform_partitions) или [`PARTITION_AT_KEYS`](../../../concepts/datamodel/table.md#partition_at_keys) — см. [{#T}](../../../concepts/datamodel/table.md#partitioning_row_table).
