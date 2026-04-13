# Сжатие данных в {{ ydb-short-name }}: эффективная экономия места без потери производительности

## Проблема

При использовании {{ ydb-short-name }} для хранения больших объемов данных (JSON, текстовые поля, бинарные данные) утилизация хранилища может превысить 85%. Расширение хранилища {{ ydb-short-name }} требует дополнительных ресурсов и времени. Особенно критична эта проблема для таблиц с большими текстовыми полями, JSON-документами или бинарными данными, где повторяющиеся паттерны занимают значительное место.

## Решение

Используйте встроенное сжатие данных через группы колонок (Column Families). {{ ydb-short-name }} поддерживает несколько алгоритмов сжатия, которые позволяют значительно сократить объем хранимых данных с минимальным влиянием на производительность.

### Поддерживаемые алгоритмы сжатия

- **LZ4** (по умолчанию) - быстрый алгоритм с хорошим соотношением скорости и степени сжатия
- **ZSTD** - компромисс между скоростью и степенью сжатия, поддерживает уровни сжатия
- **GZIP** - сильное сжатие, но медленнее
- **BROTLI** - оптимален для данных, которые редко меняются

## Практические примеры

### Создание таблицы со сжатием при инициализации

```sql
-- Строковая таблица с разными группами колонок
CREATE TABLE user_data (
    user_id Uint64,
    username Utf8,
    profile_json Utf8 FAMILY json_family,  -- JSON данные в отдельной группе
    avatar_data String FAMILY binary_family, -- Бинарные данные
    created_at Timestamp,
    PRIMARY KEY (user_id),
    FAMILY default (
        DATA = "ssd",
        COMPRESSION = "lz4"
    ),
    FAMILY json_family (
        DATA = "ssd",
        COMPRESSION = "zstd",
        COMPRESSION_LEVEL = 5
    ),
    FAMILY binary_family (
        DATA = "ssd",
        COMPRESSION = "lz4"
    )
);
```

```sql
-- Колоночная таблица с оптимизированным сжатием
CREATE TABLE analytics_events (
    event_id Uint64 NOT NULL,
    user_id Uint64,
    event_type Utf8,
    event_data Json FAMILY event_family,
    timestamp Timestamp,
    PRIMARY KEY (event_id),
    FAMILY default (
        COMPRESSION = "lz4"
    ),
    FAMILY event_family (
        COMPRESSION = "zstd",
        COMPRESSION_LEVEL = 3
    )
)
WITH (STORE = COLUMN);
```

### Включение сжатия для существующей таблицы

```sql
-- Включение сжатия для группы колонок по умолчанию
ALTER TABLE existing_table ALTER FAMILY default SET COMPRESSION "lz4";

-- Создание новой группы колонок и перемещение колонки
ALTER TABLE existing_table
    ADD FAMILY compressed_family (COMPRESSION = "zstd"),
    ALTER COLUMN large_text_column SET FAMILY compressed_family;
```

### Плохая практика (чего избегать)

```sql
-- ❌ ПЛОХО: Создание таблицы без сжатия для больших текстовых полей
CREATE TABLE logs (
    log_id Uint64,
    log_text Utf8,  -- Может занимать гигабайты без сжатия
    timestamp Timestamp,
    PRIMARY KEY (log_id)
);

-- ❌ ПЛОХО: Использование неподходящего алгоритма для частых операций записи
CREATE TABLE session_data (
    session_id Utf8,
    data Json FAMILY session_family,
    PRIMARY KEY (session_id),
    FAMILY session_family (
        COMPRESSION = "gzip"  -- Слишком медленно для частых обновлений
    )
);
```

### Хорошая практика

```sql
-- ✅ ХОРОШО: Оптимальное распределение по группам колонок
CREATE TABLE optimized_table (
    id Uint64,
    metadata Json FAMILY metadata_family,    -- Часто читаемые данные
    audit_log Utf8 FAMILY audit_family,      -- Редко читаемые логи
    binary_data String FAMILY binary_family, -- Бинарные данные
    PRIMARY KEY (id),
    FAMILY default (
        COMPRESSION = "lz4"
    ),
    FAMILY metadata_family (
        COMPRESSION = "zstd",
        COMPRESSION_LEVEL = 3  -- Баланс скорости и сжатия
    ),
    FAMILY audit_family (
        COMPRESSION = "zstd",
        COMPRESSION_LEVEL = 9  -- Максимальное сжатие для архивных данных
    ),
    FAMILY binary_family (
        COMPRESSION = "lz4"    -- Быстрое сжатие для бинарных данных
    )
);
```

## Рекомендации по выбору алгоритма

1. **LZ4** - для часто обновляемых данных, где важна скорость операций
2. **ZSTD (уровень 1-5)** - для баланса между скоростью и степенью сжатия
3. **ZSTD (уровень 6-9)** - для архивных данных, которые редко читаются
4. **GZIP/BROTLI** - для данных с очень высокой степенью избыточности

## Важные особенности

- Сжатие применяется только к новым данным и данным, прошедшим через compaction
- Для существующих таблиц сжатие заработает после фонового процесса compaction
- Влияние на CPU минимально благодаря оптимизированным алгоритмам
- Экономия места может достигать 60-80% для текстовых и JSON данных

## Проверка настроек сжатия

```sql
-- Просмотр текущих настроек сжатия
DESCRIBE TABLE your_table;

-- Или через SHOW CREATE
SHOW CREATE TABLE your_table;
```

Сжатие данных в {{ ydb-short-name }} - это мощный инструмент оптимизации, который позволяет значительно сократить затраты на хранение без существенного влияния на производительность операций.
