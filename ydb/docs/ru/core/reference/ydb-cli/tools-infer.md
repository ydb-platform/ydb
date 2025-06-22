# Выведение схемы таблицы из файлов с данными

С помощью подкоманды `{{ ydb-cli }} tools infer csv` можно получить текст `CREATE TABLE` из CSV-файла с данными. Это может быть очень полезно, если нужно [импортировать](./export-import/import-file.md) данные в базу, а таблица еще не создана.

Общий вид команды:

```bash
{{ ydb-cli }} [global options...] tools infer csv [options...]
```

* `global options` — [глобальные параметры](commands/global-options.md).
* `options` — [параметры подкоманды](#options).

Посмотреть актуальное описание команды можно с помощью опции `--help`:

```bash
{{ ydb-cli }} tools infer csv --help
```

## Параметры подкоманды {#options}

Имя параметра | Описание параметра
---|---
`-p, --path` | Путь к создаваемой таблице в базе данных. Значение по умолчанию: `table`.
`--columns` | Явное указание списка колонок таблицы, разделенных запятыми, одной строкой-списком.
`--gen-columns` | Явное указание, что имена колонок должны быть сгенерированы автоматически.
`--header` | Явное указание, что имена колонок находятся в первой строчке CSV-файла.
`--rows-to-analyze` | Количество строк в начале исходного файла для проведения анализа. `0` означает, что следует прочитать весь файл. Значение по умолчанию: `500000`.
`--execute` | Выполнить запрос `CREATE TABLE` сразу после его генерации.

{% note info %}

По умолчанию команда пытается использовать первую строку файла как список имен колонок, если это возможно.

Используйте опции "--columns", "--gen-names" или "--header", чтобы явно указать источник имен колонок.

{% endnote %}


## Примеры {#examples}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

### Имена колонок в первой строке файла

```bash
$ cat data_with_header.csv
key,value
123,abc
456,def

{{ ydb-cli }} tools infer csv data_with_header.csv
CREATE TABLE table (
    key Int64,
    value Text,
    PRIMARY KEY (key) -- First column is chosen. Probably need to change this.
)
WITH (
    STORE = ROW -- or COLUMN
    -- Other useful table options to consider:
    --, AUTO_PARTITIONING_BY_SIZE = ENABLED
    --, AUTO_PARTITIONING_BY_LOAD = ENABLED
    --, UNIFORM_PARTITIONS = 100 -- Initial number of partitions
    --, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 100
    --, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1000
);
```

{% note info %}

В блоке WITH перечислены некоторые полезные опции для таблицы. При необходимости, нужные можно раскомментировать, остальные - убрать.

{% endnote %}

### Явное указание списка колонок

```bash
cat ~/data_no_header.csv
123,abc
456,def

{{ ydb-cli }} tools infer csv -p newtable ~/data_no_header.csv --columns my_key,my_value
CREATE TABLE newtable (
    my_key Int64,
    my_value Utf8,
    PRIMARY KEY (my_key)
)
WITH (
    STORE = ROW -- or COLUMN
    -- Other useful table options to consider:
    --, AUTO_PARTITIONING_BY_SIZE = ENABLED
    --, AUTO_PARTITIONING_BY_LOAD = ENABLED
    --, UNIFORM_PARTITIONS = 100 -- Initial number of partitions
    --, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 100
    --, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1000
);
```

### Явное указание, что имена колонок должны быть сгенерированы автоматически

```bash
cat ~/data_no_header.csv
123,abc
456,def

{{ ydb-cli }} tools infer csv -p newtable ~/data_no_header.csv --gen-columns
CREATE TABLE table (
    f0 Int64,
    f1 Text,
    PRIMARY KEY (f0) -- First column is chosen. Probably need to change this.
)
WITH (
    STORE = ROW -- or COLUMN
    -- Other useful table options to consider:
    --, AUTO_PARTITIONING_BY_SIZE = ENABLED
    --, AUTO_PARTITIONING_BY_LOAD = ENABLED
    --, UNIFORM_PARTITIONS = 100 -- Initial number of partitions
    --, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 100
    --, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1000
);
```

