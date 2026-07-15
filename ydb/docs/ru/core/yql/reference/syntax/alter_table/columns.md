# Изменение колонок

{{ backend_name }} поддерживает возможность добавлять колонки в {% if backend_name == "YDB" and oss == true %} строковые и колоночные таблицы{% else %} таблицы {% endif %}, удалять неключевые колонки из таблиц, а также изменять свойства существующих колонок.

## ADD COLUMN

Строит новую колонку с указанными именем, типом и опциями для указанной таблицы.

```yql
ALTER TABLE table_name ADD COLUMN column_name column_data_type [FAMILY <family_name>] [NULL | NOT NULL] [DEFAULT <default_value>] [COMPRESSION([algorithm=<algorithm_name>[, level=<value>]])] [ENCODING([OFF|DICT])];
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

Приведённый ниже код добавит к таблице `episodes` колонку `views` с типом данных `Uint64`.

```yql
ALTER TABLE episodes ADD COLUMN views Uint64;
```

Приведённый ниже код добавит к таблице `episodes` колонку `rate` с типом данных `Double` и значением по умолчанию `5.0`.

```yql
ALTER TABLE episodes ADD COLUMN rate Double NOT NULL DEFAULT 5.0;
ALTER TABLE episodes ADD COLUMN rate Double (DEFAULT 5.0, NOT NULL); -- альтернативный синтаксис
```

## ALTER COLUMN

Изменяет свойства существующей колонки в указанной таблице. Изменение свойства происходит без пересоздания колонки. Некоторые свойства применяются только к свежим записанным данным или в процессе компакшена (детали можно найти в описании конкретного свойства)

```yql
ALTER TABLE table_name ALTER COLUMN column_name SET [FAMILY <family_name>] [DEFAULT <default_value>] [COMPRESSION([algorithm=<algorithm_name>[, level=<value>]])] [ENCODING([OFF|DICT])];
ALTER TABLE table_name ALTER COLUMN column_name DROP [FAMILY] [NOT NULL] [DEFAULT] [COMPRESSION] [ENCODING];
```

### Параметры запроса

#### table_name

Путь к таблице, в которой требуется изменить колонку.

#### column_name

Имя колонки, которая будет изменена в указанной таблице.

#### SET

Установить свойство для указанной колонки.

#### DROP

Удалить свойство для указанной колонки.

{% include [column_option_list_alter.md](../_includes/column_option_list_alter.md) %}

В одном запросе `ALTER TABLE` можно указать несколько действий `ALTER COLUMN` через запятую.

### Примеры

Приведённый ниже код задаёт значение по умолчанию для колонки `rate` в таблице `episodes`.

```yql
ALTER TABLE episodes ALTER COLUMN rate SET DEFAULT 5.0;
```

Приведённый ниже код одним запросом изменяет значения по умолчанию для нескольких колонок таблицы — у `col_1` и `col_2` устанавливает новые значения, у `col_3` сбрасывает значение по умолчанию.

```yql
ALTER TABLE default_columns
    ALTER COLUMN col_1 SET DEFAULT "new_a"u,
    ALTER COLUMN col_2 SET DEFAULT 99,
    ALTER COLUMN col_3 DROP DEFAULT;
```

Сброс настроек сжатия колонки

```yql
ALTER TABLE compressed_table ALTER COLUMN info SET COMPRESSION();
```

После выполнения запроса для колонки снова действует алгоритм сжатия по умолчанию (см. описание опции `COMPRESSION` выше).

Включение словарного кодирования на колонке

```yql
ALTER TABLE movies ALTER COLUMN genre SET ENCODING(DICT);
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

Приведённый ниже код удалит колонку `views` из таблицы `episodes`.

```yql
ALTER TABLE episodes DROP COLUMN views;
```
