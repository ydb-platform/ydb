# Генерация скрипта создания таблицы

Команда `{{ ydb-cli }} tools infer csv` позволяет на основе имеющегося CSV-файла с данными сгенерировать скрипт для создания таблицы.

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
`-p, --path` | Путь в базе данных, по которому должна быть создана новая таблица. Значение по умолчанию: `table`.
`--columns` | Список имен колонок таблицы, разделенных запятыми.
`--gen-columns` | Имена колонок таблицы необходимо сгенерировать авторматически.
`--header` | Имена колонок таблицы необходимо считать из первой строчки CSV-файла.
`--rows-to-analyze` | Количество первых строк CSV-файла, подлежащих анализу для выведения типов колонок. `0` означает, что следует прочитать весь файл. Значение по умолчанию: `500000`.
`--execute` | Выполнить создание таблицы по результатам генерации скрипта.

{% note info %}

По умолчанию команда пытается использовать первую строку файла как список имен колонок, если это возможно.

Используйте опции "--columns", "--gen-names" или "--header", чтобы явно указать источник имен колонок.

{% endnote %}


## Примеры {#examples}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

### Имена колонок заданы в первой строке CSV-файла

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

При генерации скрипта автоматически добавляется блок WITH с дополнительными опциями для создаваемой таблицы. Все опции, кроме STORE, имеют дефолтные значения и закомментированы.

{% endnote %}

### Имена колонок таблицы указываются вручную

```bash
cat ~/data_no_header.csv
123,abc
456,def

{{ ydb-cli }} tools infer csv -p newtable ~/data_no_header.csv --columns my_key,my_value
CREATE TABLE newtable (
    my_key Int64,
    my_value Text,
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

### Автоматическая генерация имена колонок таблицы

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

