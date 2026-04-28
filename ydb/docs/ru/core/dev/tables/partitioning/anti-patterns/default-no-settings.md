# Настройки по умолчанию без явной конфигурации {#default-no-settings}

Если при создании строковой таблицы **не задавать** параметры `AUTO_PARTITIONING_*` явно, действуют значения по умолчанию из модели данных (см. [{#T}](../../../../concepts/datamodel/table.md#partitioning_row_table)):

По умолчанию `AUTO_PARTITIONING_BY_LOAD = DISABLED` и `AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1` — полный список дефолтов в [{#T}](../../../../concepts/datamodel/table.md#partitioning_row_table).

Для таблиц с **предсказуемо высокой** конкуренцией за узкий ключ или интенсивной записью рекомендуется явно задать:

* [`AUTO_PARTITIONING_BY_LOAD = ENABLED`](../../../../concepts/datamodel/table.md#auto_partitioning_by_load);
* [`AUTO_PARTITIONING_MIN_PARTITIONS_COUNT`](../../../../concepts/datamodel/table.md#auto_partitioning_min_partitions_count) **> 1** и согласованный [`AUTO_PARTITIONING_MAX_PARTITIONS_COUNT`](../../../../concepts/datamodel/table.md#auto_partitioning_max_partitions_count);
* при необходимости — более подходящий порог [`AUTO_PARTITIONING_PARTITION_SIZE_MB`](../../../../concepts/datamodel/table.md#auto_partitioning_partition_size_mb).

Так вы избегаете сценария «остались дефолты → одна партиция → перегрузка shard».
