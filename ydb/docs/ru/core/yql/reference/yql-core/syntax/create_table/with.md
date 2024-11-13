# Дополнительные параметры (WITH)

Для таблицы может быть указан ряд специфичных для {{ backend_name }} параметров. При создании таблицы такие параметры перечисляются в блоке `WITH`:

```yql
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

```yql
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

{% if backend_name == "YDB" and oss == true %}

Колоночная таблица создаётся путём указанием параметра `STORE = COLUMN` в блоке `WITH`:

```yql
 CREATE TABLE table_name (
    a Uint64 NOT NULL,
    b Timestamp NOT NULL,
    c Float,
    PRIMARY KEY (a, b)
  )
  PARTITION BY HASH(b)
  WITH (
    STORE = COLUMN
  );
```

Свойства и возможности колоночных таблиц описаны в статье [{#T}](../../../../concepts/datamodel/table.md), а специфика их создания через YQL описана на странице [{#T}](./index.md). Также в блоке `WITH` можно задать TTL (Time to Live) — время жизни строки для строковых и колоночных таблиц. [TTL](../../../../concepts/ttl.md) автоматически удаляет строки, когда проходит указанное количество секунд от времени, записанного в TTL-колонку. TTL можно задать при создании строковой и колоночной таблицы или добавить позже командой `ALTER TABLE` только в строковую таблицу.

Пример создания строковой и колоночной таблицы с TTL:

{% list tabs %}

- Создание строковой таблицы с TTL

    ```yql
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

- Создание колоночной таблицы с TTL

    ```yql
    CREATE TABLE table_name (
        a Uint64 NOT NULL,
        b Timestamp NOT NULL,
        c Float,
        PRIMARY KEY (a, b)
    )
    PARTITION BY HASH(b)
    WITH (
        STORE = COLUMN,
        TTL = Interval("PT0S") ON b
    );
    ```

{% endlist %}

{% endif %}
