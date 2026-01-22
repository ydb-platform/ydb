# Изменение состава колонок

{{ backend_name }} поддерживает возможность добавлять колонки в {% if backend_name == "YDB" and oss == true %} строковые и колоночные таблицы{% else %} таблицы {% endif %}, а также удалять неключевые колонки из таблиц.

## ADD COLUMN

Строит новую колонку с указанными именем, типом и опциями для указанной таблицы.

```yql
ALTER TABLE table_name ADD COLUMN column_name column_data_type [FAMILY <family_name>] [NULL | NOT NULL] [DEFAULT <default_value>];
```

## Параметры запроса

### table_name

Путь таблицы, для которой требуется добавить новую колонку.

### column_name

Имя колонки, которая будет добавлена в указанную таблицу. При выборе имени для колонки учитывайте общие [правила именования колонок](../../../../concepts/datamodel/table.md#column-naming-rules).

### column_data_type

Тип данных колонки. Полный список типов данных, которые поддерживает {{ ydb-short-name }} доступен в разделе [{#T}](../../types/index.md).

{% include [column_option_list.md](../_includes/column_option_list.md) %}

## Пример

Приведенный ниже код добавит к таблице `episodes` колонку `views` с типом данных `Uint64`.

```yql
ALTER TABLE episodes ADD COLUMN views Uint64;
```

Приведенный ниже код добавит к таблице `episodes` колонку `rate` с типом данных `Double` и значением по умолчанию `5.0`.

```yql
ALTER TABLE episodes ADD COLUMN rate Double NOT NULL DEFAULT 5.0;
ALTER TABLE episodes ADD COLUMN rate Double (DEFAULT 5.0, NOT NULL); -- альтернативный синтаксис
```

## DROP COLUMN

Удаляет колонку из таблицы с указанным именем.

```yql
ALTER TABLE table_name DROP COLUMN column_name;
```

### Параметры запроса

#### table_name

Путь к таблице, в которой требуется удалить колонку.

#### column_name

Имя колонки, которая будет удалена из указанной таблицы.

### Пример

Приведенный ниже код удалит колонку `views` из таблицы `episodes`.

```yql
ALTER TABLE episodes DROP COLUMN views;
```
