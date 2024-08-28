# Дополнительные параметры (WITH)

Для таблицы может быть указан ряд специфичных для {{ backend_name }} параметров. При создании таблицы такие параметры перечисляются в блоке ```WITH```:

```sql
CREATE TABLE table_name (...)
WITH (
    key1 = value1,
    key2 = value2,
    ...
)
```

Здесь key — это название параметра, а value — его значение.

Перечень допустимых имен параметров и их значений приведен на странице [описания таблицы {{ backend_name }}]({{ concept_table }}).

Например, такой запрос создаст строковую таблицу с включенным автоматическим партиционированием по размеру партиции и предпочитаемым размером каждой партиции 512 мегабайт:

```sql
CREATE TABLE my_table (
    id Uint64,
    title Utf8,
    PRIMARY KEY (id)
)
WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_PARTITION_SIZE_MB = 512
);
```

{% if backend_name == "YDB" %}

Также в блоке `WITH` можно задать TTL (Time to Live) — время жизни строки. [TTL](../../../../concepts/ttl.md) автоматически удаляет из строковой таблицы строки, когда проходит указанное количество секунд от времени, записанного в TTL-колонку. TTL можно задать при создании таблицы, а можно добавить позже через `ALTER TABLE`. Код ниже создаст строковую таблицу таблицу с TTL:
```sql
CREATE TABLE my_table (
    id Uint64,
    title Utf8,
    expire_at Timestamp,
    PRIMARY KEY (id)
)
WITH (
    TTL = Interval("PT0S") ON expire_at
);
```

{% endif %}