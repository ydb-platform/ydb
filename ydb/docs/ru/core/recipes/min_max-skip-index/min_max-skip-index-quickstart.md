# Быстрый старт с min_max-индексом

## Создание таблицы с min_max-индексом

Ниже минимальный пример: колоночная таблица с первичным ключом и локальным индексом типа `min_max` на колонках, которые часто используются в фильтрах.

```yql
CREATE TABLE events (
    id Uint64 NOT NULL,
    created_at Timestamp,
    level Int32,
    resource_id Utf8,
    PRIMARY KEY (id),
    INDEX idx_created_at LOCAL USING min_max
        ON (created_at),
    INDEX idx_level LOCAL USING min_max
        ON (level)
)
WITH (
    STORE = COLUMN
);
```

## Запросы и эффект

После загрузки данных селективные запросы с фильтрами по индексируемым колонкам могут читать меньше данных: во время сканирования хранилища min_max-индекс пропускает фрагменты, диапазон значений которых не может содержать подходящие строки.

Пример данных и запросов для таблицы выше:

```yql
INSERT INTO events (id, created_at, level, resource_id) VALUES
    (1, Timestamp("2024-01-01T00:00:00.000000Z"), 1, "res-1"),
    (2, Timestamp("2024-01-01T00:01:00.000000Z"), 3, "res-42"),
    (3, Timestamp("2024-01-02T00:00:00.000000Z"), 5, "res-2");
```

Диапазонный фильтр по колонке с временной меткой:

```yql
SELECT id, resource_id
FROM events
WHERE created_at >= Timestamp("2024-01-01T00:00:00.000000Z")
  AND created_at <  Timestamp("2024-01-02T00:00:00.000000Z");
```

Диапазонный фильтр по числовой колонке:

```yql
SELECT id, resource_id
FROM events
WHERE level BETWEEN 2 AND 4;
```

## Как проверить эффективность индекса

Чтобы проверить, что min_max-индекс действительно помогает, выполните один и тот же селективный запрос на таблице с достаточным объёмом данных до и после создания индекса и сравните время выполнения.

Дополнительно:

* Подробности и ограничения: [min_max-индекс](../../dev/min_max-skip-index.md)
* Полный синтаксис: [ALTER TABLE ADD INDEX](../../yql/reference/syntax/alter_table/indexes.md#local-min-max)
