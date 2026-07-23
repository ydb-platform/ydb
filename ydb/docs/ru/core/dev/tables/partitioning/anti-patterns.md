# Анти-паттерны при партицировании {#anti-patterns}

Типичные ошибки при проектировании ключа и настроек партиционирования:

Антипаттерны дизайна первичного ключа (монотонные ключи, горячие точки) рассмотрены в [{#T}](../../primary-key/row-oriented.md).

* **Жёсткий лимит размера партиции на уровне кластера** — по умолчанию **2 ГБ**. Если размер партиции превышает этот лимит, она разделится, если только не достигнут [`AUTO_PARTITIONING_MAX_PARTITIONS_COUNT`](auto/limits.md) или [лимит числа шардов таблицы в базе](../../../concepts/limits-ydb.md#schema-object).

* **Игнорирование лимита `AUTO_PARTITIONING_MAX_PARTITIONS_COUNT`** — при достижении потолка таблица перестаёт разделяться, рост задержек и размера партиций неизбежен, пока лимит не увеличат.

## Настройки по умолчанию без явной конфигурации {#default-no-settings}

Если при создании строковой таблицы **не задавать** параметры `AUTO_PARTITIONING_*` явно, действуют значения по умолчанию из модели данных (см. [{#T}](../../../concepts/datamodel/table.md#partitioning_row_table)).

* **`AUTO_PARTITIONING_BY_SIZE`** обычно **включён** — разделение по размеру партиции с порогом **`AUTO_PARTITIONING_PARTITION_SIZE_MB`** по умолчанию (порядка 2 ГБ в документации).

* **`AUTO_PARTITIONING_BY_LOAD`** по умолчанию **выключен** — перегрузка партиции по CPU **не приведёт** к автоматическому split, пока режим не включён.

* **`AUTO_PARTITIONING_MIN_PARTITIONS_COUNT`** по умолчанию **1** — при спаде нагрузки merge может оставить одну партицию; при следующем всплеске может потребоваться время на новые split.

При **высокой конкуренции за единичные ключи** (горячий ключ, низкая кардинальность первого компонента) производительность будет ограничена вне зависимости от настроек `AUTO_PARTITIONING_*` — партицирование по нагрузке здесь мало поможет; пересмотрите модель ключа (см. [{#T}](../../primary-key/row-oriented.md)).

Если нагрузка распределена шире, но дефолтов недостаточно, задайте явно:

* [`AUTO_PARTITIONING_BY_LOAD = ENABLED`](../../../concepts/datamodel/table.md#auto_partitioning_by_load);
* [`AUTO_PARTITIONING_MIN_PARTITIONS_COUNT`](../../../concepts/datamodel/table.md#auto_partitioning_min_partitions_count) и согласованный [`AUTO_PARTITIONING_MAX_PARTITIONS_COUNT`](../../../concepts/datamodel/table.md#auto_partitioning_max_partitions_count): **не выставляйте** `AUTO_PARTITIONING_MIN_PARTITIONS_COUNT` меньше **80%** от `AUTO_PARTITIONING_MAX_PARTITIONS_COUNT`;
* для снижения нагрузки на отдельные партиции стоит уменьшить максимальный размер партиции — [`AUTO_PARTITIONING_PARTITION_SIZE_MB`](../../../concepts/datamodel/table.md#auto_partitioning_partition_size_mb).

Так вы избегаете сценария «остались дефолты → одна партиция → перегрузка shard».
