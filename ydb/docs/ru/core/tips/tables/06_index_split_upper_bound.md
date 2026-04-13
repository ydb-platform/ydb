# Настройка верхней границы для индексов при автопартицировании

## Проблема

Вторичные индексы в {{ ydb-short-name }} реализованы как скрытые таблицы, которые также подвержены автоматическому партицированию. По умолчанию индексные таблицы имеют ограничение на максимальное количество партиций (`AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 50`). Когда индекс достигает этого лимита, дальнейшее разделение партиций (split) прекращается, что может привести к:

- Снижению производительности при росте данных
- Увеличению времени отклика запросов
- Потенциальным сбоям при высокой нагрузке

Без явной настройки верхней границы индексные таблицы могут стать узким местом, ограничивая производительность всей системы независимо от основной таблицы.

## Решение

Явное указание верхней границы для индексных таблиц через параметр `AUTO_PARTITIONING_MAX_PARTITIONS_COUNT` позволяет:

1. **Оптимизировать производительность индексов** независимо от основной таблицы
2. **Предотвратить неограниченный рост** количества партиций
3. **Обеспечить стабильность** работы при высоких нагрузках
4. **Настроить баланс** между производительностью и ресурсами

### Синтаксис настройки

```sql
-- Настройка параметров автопартицирования для существующего индекса
ALTER TABLE table_name
ALTER INDEX index_name
SET (
    AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = значение,
    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = значение,
    AUTO_PARTITIONING_BY_LOAD = ENABLED|DISABLED,
    AUTO_PARTITIONING_BY_SIZE = ENABLED|DISABLED
);
```

### Рекомендуемые значения

- **Для индексов с равномерным распределением данных**: 100-500 партиций
- **Для индексов с горячими ключами**: 500-1000 партиций
- **Для очень больших таблиц**: 1000-5000 партиций

## Примеры кода


### ❌ Плохой пример - использование значений по умолчанию

```sql
-- Создание таблицы с индексом без настройки верхней границы
CREATE TABLE orders (
    order_id Uint64,
    customer_id Uint64,
    order_date Timestamp,
    amount Decimal,
    PRIMARY KEY (order_id)
);

-- Создание индекса без настройки автопартицирования
CREATE TABLE INDEX idx_customer_orders ON orders (customer_id, order_date);

-- Проблема: индекс будет ограничен 50 партициями по умолчанию
-- При большом объеме данных это станет узким местом
```

### ✅ Хороший пример - явная настройка верхней границы

```sql
-- Создание таблицы с правильно настроенным индексом
CREATE TABLE orders (
    order_id Uint64,
    customer_id Uint64,
    order_date Timestamp,
    amount Decimal,
    PRIMARY KEY (order_id)
);

-- Создание индекса с настройкой автопартицирования
CREATE TABLE INDEX idx_customer_orders ON orders (customer_id, order_date);

-- Настройка оптимальных параметров для индекса
ALTER TABLE orders
ALTER INDEX idx_customer_orders
SET (
    AUTO_PARTITIONING_BY_LOAD = ENABLED,
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 10,
    AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 500,
    AUTO_PARTITIONING_PARTITION_SIZE_MB = 2048
);
```

### ✅ Пример для Terraform

```hcl
resource "ycp_ydb_table_index" "orders_index" {
  table_id = ycp_ydb_table.orders.id
  name     = "idx_customer_orders"
  columns  = ["customer_id", "order_date"]
  type     = "global_sync"
  
  # Настройка параметров автопартицирования
  partitioning_settings {
    auto_partitioning_by_load        = true
    auto_partitioning_by_size        = true
    auto_partitioning_min_partitions_count = 10
    auto_partitioning_max_partitions_count = 500
    auto_partitioning_partition_size_mb    = 2048
  }
}
```

### ✅ Пример мониторинга количества партиций

```sql
-- Проверка текущего количества партиций для индекса
SELECT
    table_path,
    partitions_count,
    approximate_rows_count,
    approximate_size_mb
FROM `.sys/partition_stats`
WHERE table_path LIKE '%idx_customer_orders%';

-- Мониторинг приближения к верхней границе
SELECT
    table_path,
    partitions_count,
    (partitions_count * 100.0 / 500) as usage_percent  -- 500 - наша верхняя граница
FROM `.sys/partition_stats`
WHERE table_path LIKE '%idx_%'
  AND partitions_count > 400;  -- Предупреждение при 80% заполнения
```


## Рекомендации

1. **Начинайте с консервативных значений** и увеличивайте по мере необходимости
2. **Мониторьте статистику партиций** регулярно через системные таблицы
3. **Учитывайте паттерны доступа** - для равномерной нагрузки можно использовать меньше партиций
4. **Для горячих ключей** устанавливайте более высокие значения верхней границы
5. **Сочетайте с AUTO_PARTITIONING_MIN_PARTITIONS_COUNT** для предотвращения излишнего объединения партиций

## Важные замечания

- Настройки автопартицирования для индексов можно изменять только после создания индекса
- Изменения применяются немедленно и влияют на дальнейшее поведение системы
- Рекомендуется устанавливать осмысленные значения, соответствующие ожидаемой нагрузке и объему данных
