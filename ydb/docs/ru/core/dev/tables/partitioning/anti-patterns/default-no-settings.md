# Настройки по умолчанию без явной конфигурации {#default-no-settings}

Если при создании строковой таблицы **не задавать** параметры `AUTO_PARTITIONING_*` явно, действуют значения по умолчанию из модели данных (см. [{#T}](../../../../concepts/datamodel/table.md#partitioning_row_table)):

* **`AUTO_PARTITIONING_BY_SIZE`** обычно **включён** — деление по размеру партиции с порогом **`AUTO_PARTITIONING_PARTITION_SIZE_MB`** по умолчанию (порядка 2 ГБ в документации).

* **`AUTO_PARTITIONING_BY_LOAD`** по умолчанию **выключен** — перегрузка партиции по CPU **не приведёт** к автоматическому split, пока режим не включён.

* **`AUTO_PARTITIONING_MIN_PARTITIONS_COUNT`** по умолчанию **1** — при спаде нагрузки merge может оставить одну партицию; при следующем всплеске может потребоваться время на новые split.

Для таблиц с **предсказуемо высокой** конкуренцией за узкий ключ или интенсивной записью рекомендуется явно задать:

* [`AUTO_PARTITIONING_BY_LOAD = ENABLED`](../../../../concepts/datamodel/table.md#auto_partitioning_by_load);
* [`AUTO_PARTITIONING_MIN_PARTITIONS_COUNT`](../../../../concepts/datamodel/table.md#auto_partitioning_min_partitions_count) **> 1** и согласованный [`AUTO_PARTITIONING_MAX_PARTITIONS_COUNT`](../../../../concepts/datamodel/table.md#auto_partitioning_max_partitions_count);
* при необходимости — более подходящий порог [`AUTO_PARTITIONING_PARTITION_SIZE_MB`](../../../../concepts/datamodel/table.md#auto_partitioning_partition_size_mb).

Так вы избегаете сценария «остались дефолты → одна партиция → перегрузка shard».
