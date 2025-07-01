# Генерация скрипта создания таблицы

Команда `{{ ydb-cli }} tools infer csv` позволяет на основе имеющегося CSV-файла с данными сгенерировать скрипт для создания таблицы.

Общий вид команды:

```bash
{{ ydb-cli }} [global options...] tools infer csv [options...]
```

* `global options` — [глобальные параметры](commands/global-options.md).
* `options` — [параметры подкоманды](#options).

Получить описание параметров команды можно с помощью опции `--help`:

```bash
{{ ydb-cli }} tools infer csv --help
```

## Параметры подкоманды {#options}

Имя параметра | Описание параметра
---|---
`-p, --path` | Путь в базе данных, по которому должна быть создана новая таблица. Значение по умолчанию: `table`.
`--columns` | Список имен колонок таблицы, разделенных запятыми.
`--gen-columns` | Имена колонок таблицы необходимо сгенерировать автоматически (column1, column2, ...).
`--header` | Имена колонок таблицы необходимо считать из первой строчки CSV-файла.
`--rows-to-analyze` | Количество первых строк CSV-файла, подлежащих анализу для автоматического определения типов колонок. `0` - будут прочитаны и проанализированы все строки из файла. Значение по умолчанию: `500 000`.
`--execute` | Выполнить создание таблицы по результатам генерации скрипта.

{% note info %}

Если ни одна из опций "--columns", "--gen-names" или "--header" явно не указана, то применяется следующий алгоритм. Берется первая строка из файла, и указанные в ней значения проверяются на следующие условия:

* значения соответствуют требованиям к наименованию колонок
* типы этих значений отличаются от типов значений данных из других строк файла

При выполнении обоих условий - значения из первой строки файла используются в качестве имен колонок таблицы. В противном случае - имена колонок генерируются автоматически. Подробнее см. [пример](#example-default) ниже.

{% endnote %}

## Текущее ограничение

В качестве ключа всегда выбирается первая колонка. При необходимости следует изменить первичный ключ на подходящий.

## Примеры {#examples}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

### Имена колонок заданы в первой строке CSV-файла, опции не указаны {#example-default}

В этом примере опции не указаны.
Значения `key` и `value` в первой строке соответствуют требованиям к именам колонок и подходят под типы данных в остальных строках (`Int64` и `Text`). Поэтому команда использует первую строку файла как имена колонок.

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
    --, AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 1000
);
```

{% note info %}

При генерации скрипта автоматически добавляется блок WITH с дополнительными опциями для создаваемой таблицы. Все опции, кроме STORE, имеют дефолтные значения и закомментированы.
Вы можете указать требуемые значения нужных дополнительных опций самостоятельно.

{% endnote %}

### Имена колонок в первой строке, используется опция `--header` {#example-header}

В этом примере значения `key` и `value` в первой строке совпадают с типами данных (`Text`) в остальных строках. Поэтому без опции `--header` команда не будет использовать первую строку как имена колонок, а сгенерирует их автоматически. Чтобы использовать первую строку как имена колонок в таком случае, явно укажите опцию `--header`:

```bash
$ cat data_with_header_text.csv
key,value
aaa,bbb
ccc,ddd

{{ ydb-cli }} tools infer csv data_with_header_text.csv --header
CREATE TABLE table (
    key Text,
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
    --, AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 1000
);
```

### Явное указание списка колонок {#example-columns}

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
    --, AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 1000
);
```

### Автоматическая генерация имён колонок {#example-gen-columns}

```bash
cat ~/data_no_header.csv
123,abc
456,def

{{ ydb-cli }} tools infer csv -p newtable ~/data_no_header.csv --gen-columns
CREATE TABLE newtable (
    column1 Int64,
    column2 Text,
    PRIMARY KEY (f0) -- First column is chosen. Probably need to change this.
)
WITH (
    STORE = ROW -- or COLUMN
    -- Other useful table options to consider:
    --, AUTO_PARTITIONING_BY_SIZE = ENABLED
    --, AUTO_PARTITIONING_BY_LOAD = ENABLED
    --, UNIFORM_PARTITIONS = 100 -- Initial number of partitions
    --, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 100
    --, AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 1000
);
```

