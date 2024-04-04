# Key-Value нагрузка

Простой вид нагрузки, использующий БД YDB как Key-Value хранилище.

## Виды нагрузки {#workload-types}

Данный нагрузочный тест содержит несколько видов нагрузки:
* [upsert](#upsert-kv) — при помощи операции UPSERT вставляет в заранее созданную командой init таблицу строки, которые представляют из себя кортежи (key1, key2, ... keyK, value1, value2, ... valueN), числа K и N задаются в параметрах.
* [insert](#insert-kv) — функционал такой же, как у нагрузки upsert, но для вставки используется операция INSERT.
* [select](#select-kv) — читает данные при помощи операции SELECT * WHERE key = $key. Запрос всегда затрагивает все колонки таблицы, но не всегда является точечным, количеством вариаций primary ключа можно управлять с помощью параметров.
* [read-rows](#read-rows-kv) - читает данные при помощи операции ReadRows, выполняющей более быстрые чтения по ключу, чем select. Запрос всегда затрагивает все колонки таблицы, но не всегда является точечным, количеством вариаций primary ключа можно управлять с помощью параметров.
* [mixed](#mixed-kv) - одновременно пишет и читает данные, дополнительно проверяя что все записанные данные успешно читаются.

## Инициализация нагрузочного теста {#init}

Для начала работы необходимо создать таблицы, при создании можно указать, сколько строк необходимо вставить при инициализации:

```bash
{{ ydb-cli }} workload kv init [init options...]
```

* `init options` — [параметры инициализации](#init-options).

Посмотрите описание команды для инициализации таблицы:

```bash
{{ ydb-cli }} workload kv init --help
```

### Доступные параметры {#init-options}

Имя параметра | Описание параметра
---|---
`--init-upserts <значение>` | Количество операций вставки, которые нужно сделать при инициализации. Значение по умолчанию: 1000.
`--min-partitions` | Минимальное количество шардов для таблиц. Значение по умолчанию: 40.
`--partition-size` | Максимальный размер одной партиции (настройка `AUTO_PARTITIONING_PARTITION_SIZE_MB`). Значение по умолчанию 2000.
`--auto-partition` | Включение/выключение автошардирования. Возможные значения: 0 или 1. Значение по умолчанию: 1.
`--max-first-key` | Максимальное значение primary ключа таблицы. Значение по умолчанию: $2^{64} — 1$.
`--len` | Размер строк в байтах, которые вставляются в таблицу, как values. Значение по умолчанию: 8.
`--cols` | Количество колонок в таблице. Значение по умолчанию: 2, считая Key.
`--int-cols` | Количество первых колонок в таблице которые будут иметь тип `Uint64`, последующие колонки будут иметь тип `String`. Значение по умолчанию 1.
`--key-cols` | Количество первых колонок в таблице входящих в ключ. Значение по умолчанию 1.
`--rows` | Количество затрагиваемых строк в одном запросе. Значение по умолчанию: 1.

Для создания таблицы используется следующая команда:

```yql
CREATE TABLE `kv_test`(
    c0 Uint64,
    c1 Uint64,
    ...
    cI Uint64,
    cI+1 String,
    ...
    cN String,
    PRIMARY KEY(c0, c1, ... cK)) WITH (
        AUTO_PARTITIONING_BY_LOAD = ENABLED,
        AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = partsNum,
        UNIFORM_PARTITIONS = partsNum,
        AUTO_PARTITIONING_PARTITION_SIZE_MB = partSize,
        AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 1000
    )
)
```

### Примеры инициализации нагрузки {#init-kv-examples}

Пример команды создания таблицы с 1000 строками:

```bash
{{ ydb-cli }} workload kv init --init-upserts 1000
```

## Удаление таблицы {#clean}

После завершения работы можно удалить таблицу:

```bash
{{ ydb-cli }} workload kv clean
```

Исполняется следующая YQL команда:

```sql
DROP TABLE `kv_test`
```

### Примеры использования clean {#clean-kv-examples}

```bash
{{ ydb-cli }} workload kv clean
```

## Запуск нагрузочного теста {#run}

Для запуска нагрузки необходимо выполнить команду:

```bash
{{ ydb-cli }} workload kv run [workload type...] [global workload options...] [specific workload options...]
```

В течение теста на экран выводится статистика по нагрузке для каждого временного окна.

* `workload type` — [виды нагрузки](#workload-types).
* `global workload options` — [общие параметры для всех видов нагрузки](#global-workload-options).
* `specific workload options` — параметры конкретного вида нагрузки.

Посмотрите описание команды для запуска нагрузки:

```bash
{{ ydb-cli }} workload kv run --help
```

### Общие параметры для всех видов нагрузки {#global-workload-options}

Имя параметра | Короткое имя | Описание параметра
---|---|---
`--seconds <значение>` | `-s <значение>` | Продолжительность теста, сек. Значение по умолчанию: 10.
`--threads <значение>` | `-t <значение>` | Количество параллельных потоков, создающих нагрузку. Значение по умолчанию: 10.
`--rate <значение>` | - | Суммарная частота запросов всех потоков, в транзакциях в секунду Значение по умолчанию: 0 (не ограничена).
`--quiet` | - | Выводит только итоговый результат теста.
`--print-timestamp` | - | Печатать время вместе со статистикой каждого временного окна.
`--client-timeout` | - | [Транспортный таймаут в миллисекундах](../../dev/timeouts.md).
`--operation-timeout` | - | [Таймаут на операцию в миллисекундах](../../dev/timeouts.md).
`--cancel-after` | - | [Таймаут отмены операции в миллисекундах](../../dev/timeouts.md).
`--window` | - | Длительность окна сбора статистики в секундах. Значение по умолчанию: 1.
`--max-first-key` | - | Максимальное значение primary ключа таблицы. Значение по умолчанию: $2^{64} - 1$.
`--cols` | - | Количество колонок в таблице. Значение по умолчанию: 2, считая Key.
`--int-cols` | - | Количество первых колонок в таблице которые будут иметь тип `Uint64`, последующие колонки будут иметь тип `String`. Значение по умолчанию 1.
`--key-cols` | - | Количество первых колонок в таблице входящих в ключ. Значение по умолчанию 1.
`--rows` | - |Количество затрагиваемых строк в одном запросе. Значение по умолчанию: 1.

## Нагрузка upsert {#upsert-kv}

Данный вид нагрузки вставляет кортежи (key1, key2, ... keyK, value1, value2, ... valueN)

Для запуска данного вида нагрузки необходимо выполнить команду:

```bash
{{ ydb-cli }} workload kv run upsert [global workload options...] [specific workload options...]
```

* `global workload options` - [общие параметры для всех видов нагрузки](#global-workload-options).
* `specific workload options` - [параметры конкретного вида нагрузки](#upsert-options).

Например для параметров `--rows 2 --cols 3 --int-cols 2` YQL Запрос будет выглядеть так:

```yql
DECLARE $c0_0 AS Uint64;
DECLARE $c0_1 AS Uint64;
DECLARE $c0_2 AS String;
DECLARE $c1_0 AS Uint64;
DECLARE $c1_1 AS Uint64;
DECLARE $c1_2 AS String;
UPSERT INTO `kv_test` (c0, c1, c2) VALUES ($c0_0, $c0_1, $c0_2), ($c1_0, $c1_1, $c1_2)
```

### Параметры для upsert {#upsert-options}

Имя параметра | Описание параметра
---|---
`--len` | Размер строк в байтах, которые вставляются в таблицу, как values. Значение по умолчанию: 8.

## Нагрузка insert {#insert-kv}

Данный вид нагрузки вставляет кортежи (key1, key2, ... keyK, value1, value2, ... valueN)

Для запуска данного вида нагрузки необходимо выполнить команду:

```bash
{{ ydb-cli }} workload kv run insert [global workload options...] [specific workload options...]
```

* `global workload options` - [общие параметры для всех видов нагрузки](#global-workload-options).
* `specific workload options` - [параметры конкретного вида нагрузки](#insert-options).

Например для параметров `--rows 2 --cols 3 --int-cols 2` YQL Запрос будет выглядеть так:

```yql
DECLARE $c0_0 AS Uint64;
DECLARE $c0_1 AS Uint64;
DECLARE $c0_2 AS String;
DECLARE $c1_0 AS Uint64;
DECLARE $c1_1 AS Uint64;
DECLARE $c1_2 AS String;
INSERT INTO `kv_test` (c0, c1, c2) VALUES ($c0_0, $c0_1, $c0_2), ($c1_0, $c1_1, $c1_2)
```

### Параметры для insert {#insert-options}

Имя параметра | Описание параметра
---|---
`--len` | Размер строк в байтах, которые вставляются в таблицу, как values. Значение по умолчанию: 8.

## Нагрузка select {#select-kv}

Данный вид нагрузки создает SELECT запросы, возвращающие строки по точному совпадению primary ключа.

Для запуска данного вида нагрузки необходимо выполнить команду:

```bash
{{ ydb-cli }} workload kv run select [global workload options...]
```

* `global workload options` - [общие параметры для всех видов нагрузки](#global-workload-options).

Например для параметров `--rows 2 --cols 3 --int-cols 2 --key-cols 2` YQL Запрос будет выглядеть так:

```yql
DECLARE $r0_0 AS Uint64;
DECLARE $r0_1 AS Uint64;
DECLARE $r1_0 AS Uint64;
DECLARE $r1_1 AS Uint64;
SELECT c0, c1, c2 FROM `kv_test` WHERE c0 = $r0_0 AND c1 = $r0_1 OR c0 = $r1_0 AND c1 = $r1_1
```

## Нагрузка read-rows {#read-rows-kv}

Данный вид нагрузки создает ReadRows запросы, возвращающие строки по точному совпадению primary ключа.

Для запуска данного вида нагрузки необходимо выполнить команду:

```bash
{{ ydb-cli }} workload kv run read-rows [global workload options...]
```

* `global workload options` - [общие параметры для всех видов нагрузки](#global-workload-options).

## Нагрузка mixed {#mixed-kv}

Данный вид нагрузки одновременно вставляет и читает кортежи (key1, key2, ... keyK, value1, value2, ... valueN), дополнительно проверяя что все записанные данные успешно читаются.

Для запуска данного вида нагрузки необходимо выполнить команду:

```bash
{{ ydb-cli }} workload kv run mixed [global workload options...] [specific workload options...]
```

* `global workload options` - [общие параметры для всех видов нагрузки](#global-workload-options).
* `specific workload options` - [параметры конкретного вида нагрузки](#mixed-options).

### Параметры для mixed {#mixed-options}

Имя параметра | Описание параметра
---|---
`--len` | Размер строк в байтах, которые вставляются в таблицу, как values. Значение по умолчанию: 8.
`--change-partitions-size` | Включение/выключение случайного изменения настройки `AUTO_PARTITIONING_PARTITION_SIZE_MB`. Возможные значения: 0 или 1. Значение по умолчанию: 0.
`--do-select` | Включение/выключение чтений при помощи запроса [select](#select-kv). Возможные значения: 0 или 1. Значение по умолчанию: 1.
`--do-read-rows` | Включение/выключение чтений при помощи запроса [read-rows](#read-rows-kv). Возможные значения: 0 или 1. Значение по умолчанию: 1.