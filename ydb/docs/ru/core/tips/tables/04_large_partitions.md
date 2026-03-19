# Оптимальный размер партиций для равномерной нагрузки

## Проблема

Большие партиции (превышающие 2 ГБ) создают несколько серьезных проблем:

1. **Снижение производительности** - партиции размером более 2 ГБ увеличивают время ответа на запросы, особенно под высокой нагрузкой
2. **Риск перегрузки серверов** - большие партиции концентрируют данные на одном сервере, создавая "горячие точки"
3. **Задержки при операциях split/merge** - разделение больших партиций занимает значительное время (до 15 секунд на принятие решения)
4. **Неравномерное распределение нагрузки** - партиции разного размера приводят к дисбалансу нагрузки между серверами

По умолчанию {{ ydb-short-name }} разделяет партицию при достижении 2 ГБ, но без правильной настройки это может приводить к избыточным операциям split/merge.

## Решение

### Основные настройки партиционирования

Для оптимальной работы рекомендуется настроить следующие параметры:

- **`AUTO_PARTITIONING_PARTITION_SIZE_MB`** - максимальный размер партиции (по умолчанию 2000 МБ)
- **`AUTO_PARTITIONING_BY_LOAD`** - включение разделения по нагрузке (по умолчанию DISABLED)
- **`AUTO_PARTITIONING_MIN_PARTITIONS_COUNT`** - минимальное количество партиций
- **`AUTO_PARTITIONING_MAX_PARTITIONS_COUNT`** - максимальное количество партиций

### Рекомендуемые значения

1. **Размер партиции**: 100-2000 МБ в зависимости от нагрузки
   - Высокая нагрузка: 100-500 МБ
   - Средняя нагрузка: 500-1000 МБ
   - Низкая нагрузка: 1000-2000 МБ

2. **Минимальное количество партиций**: устанавливать не равным 1, примерно 80-90% от максимального
3. **Разница между min и max**: не более 20% для избежания избыточных split/merge

### Практические рекомендации

- Включайте `AUTO_PARTITIONING_BY_LOAD` для таблиц с заметной нагрузкой
- Мониторьте метрики Split/Merge partitions в Grafana
- Избегайте резких изменений настроек на больших таблицах (десятки ТБ)
- Учитывайте, что одна партиция может использовать не более 1 ядра CPU


## Примеры кода

### ❌ Плохая практика - таблица с настройками по умолчанию

```sql
-- Создание таблицы без настройки партиционирования
CREATE TABLE events (
    id Uint64,
    event_time Timestamp,
    data String,
    PRIMARY KEY (id, event_time)
) WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = DISABLED
);
```

**Проблемы:**
- Разделение только по размеру (2 ГБ по умолчанию)
- Нет защиты от избыточных split/merge операций
- Риск создания больших партиций при неравномерной нагрузке

### ✅ Хорошая практика - оптимальная настройка партиционирования

```sql
-- Создание таблицы с оптимальными настройками для высокой нагрузки
CREATE TABLE events (
    id Uint64,
    event_time Timestamp,
    data String,
    PRIMARY KEY (id, event_time)
) WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_PARTITION_SIZE_MB = 500,
    AUTO_PARTITIONING_BY_LOAD = ENABLED,
    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4,
    AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 20
);

-- Или настройка существующей таблицы
ALTER TABLE events SET (
    AUTO_PARTITIONING_PARTITION_SIZE_MB = 500,
    AUTO_PARTITIONING_BY_LOAD = ENABLED,
    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4,
    AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 20
);
```

**Преимущества:**
- Разделение как по размеру (500 МБ), так и по нагрузке
- Защита от избыточных операций (min/max разница 20%)
- Оптимальный размер партиций для равномерного распределения

### ❌ Опасная практика - резкое изменение настроек на большой таблице

```sql
-- Резкое уменьшение размера партиции на таблице в 10 ТБ
ALTER TABLE large_events SET (
    AUTO_PARTITIONING_PARTITION_SIZE_MB = 100  -- было 2000
);
```

**Проблемы:**
- Может вызвать массовые split операции
- Временная недоступность партиций (до 500 мс)
- Высокая нагрузка на кластер

### ✅ Безопасное изменение настроек

```sql
-- Постепенное уменьшение размера партиции
ALTER TABLE large_events SET (
    AUTO_PARTITIONING_PARTITION_SIZE_MB = 1000  -- сначала до 1 ГБ
);

-- Через некоторое время, после стабилизации
ALTER TABLE large_events SET (
    AUTO_PARTITIONING_PARTITION_SIZE_MB = 500   -- затем до 500 МБ
);
```

### Мониторинг состояния партиций

```sql
-- Проверка текущих настроек таблицы
SELECT * FROM `.sys/partition_stats` WHERE TableName = 'events';

-- Мониторинг операций split/merge
SELECT
    TableName,
    PartitionCount,
    AvgPartitionSizeMB,
    SplitCount,
    MergeCount
FROM `.sys/table_stats`;
```

## Диагностика проблем

### Проверка настроек через {{ ydb-short-name }} CLI

```bash
# Получение информации о настройках таблицы
ydb scheme describe events

# Мониторинг метрик split/merge
ydb monitoring metrics --metric SplitPartitionsPerSecond
ydb monitoring metrics --metric MergePartitionsPerSecond
```

### Графики для мониторинга в Grafana

- **Split/Merge partitions** - количество операций в секунду
- **DataShard metrics** - нагрузка на отдельные партиции
- **Query engine requests** - общая нагрузка на таблицу
