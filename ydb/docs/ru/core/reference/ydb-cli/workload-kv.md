# Key-Value нагрузка

Простой вид нагрузки, использующий БД YDB как Key-Value хранилище.

## Виды нагрузки {#workload-types}

Данный нагрузочный тест содержит 3 вида нагрузки:
* [upsert](#upsert-kv) — при помощи операции UPSERT вставляет в заранее созданную командой init таблицу строки, которые представляют из себя кортежи (key, value1, value2, ... valueN), число N задается в параметрах.
* [insert](#insert-kv) — функционал такой же, как у нагрузки upsert, но для вставки используется операция INSERT.
* [select](#select-kv) — читает данные при помощи операции SELECT * WHERE key = $key. Запрос всегда затрагивает все колонки таблицы, но не всегда является точечным, количеством вариаций primary ключа можно управлять с помощью параметров.

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
`--auto-partition` | Включение/выключение автошардирования. Возможные значения: 0 или 1. Значение по умолчанию: 1.
`--max-first-key` | Максимальное значение primary ключа таблицы. Значение по умолчанию: $2^{64} — 1$.
`--len` | Размер строк в байтах, которые вставляются в таблицу, как values. Значение по умолчанию: 8.
`--cols` | Количество колонок в таблице. Значение по умолчанию: 2, считая Key.
`--rows` | Количество затрагиваемых строк в одном запросе. Значение по умолчанию: 1.

Для создания таблицы используется следующая команда:

```yql
CREATE TABLE `kv_test`(
    c0 Uint64,
    c1 String,
    c2 String,
    ...
    cN String,
    PRIMARY KEY(c0)) WITH (
        AUTO_PARTITIONING_BY_LOAD = ENABLED,
        AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = partsNum,
        UNIFORM_PARTITIONS = partsNum,
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
`--quiet` | - | Выводит только итоговый результат теста.
`--print-timestamp` | - | Печатать время вместе со статистикой каждого временного окна.
`--client-timeout` | - | [Транспортный таймаут в миллисекундах](../../best_practices/timeouts.md).
`--operation-timeout` | - | [Таймаут на операцию в миллисекундах](../../best_practices/timeouts.md).
`--cancel-after` | - | [Таймаут отмены операции в миллисекундах](../../best_practices/timeouts.md).
`--window` | - | Длительность окна сбора статистики в секундах. Значение по умолчанию: 1.
`--max-first-key` | - | Максимальное значение primary ключа таблицы. Значение по умолчанию: $2^{64} - 1$.
`--cols` | - | Количество колонок в таблице. Значение по умолчанию: 2, считая Key.
`--rows` | - |Количество затрагиваемых строк в одном запросе. Значение по умолчанию: 1.

## Нагрузка upsert {#upsert-kv}

Данный вид нагрузки вставляет кортежи (key, value1, value2, ..., valueN)

YQL Запрос:

```yql
DECLARE r0 AS Uint64
DECLARE c00 AS String;
DECLARE c01 AS String;
...
DECLARE c0{N - 1} AS String;
DECLARE r1 AS Uint64
DECLARE c10 AS String;
DECLARE c11 AS String;
...
DECLARE c1{N - 1} AS String;
UPSERT INTO `kv_test`(c0, c1, ... cN) VALUES ( (r0, c00, ... c0{N - 1}), (r1, c10, ... c1{N - 1}), ... )
```

Для запуска данного вида нагрузки необходимо выполнить команду:

```bash
{{ ydb-cli }} workload kv run upsert [global workload options...] [specific workload options...]
```

* `global workload options` - [общие параметры для всех видов нагрузки](#global-workload-options).
* `specific workload options` - [параметры конкретного вида нагрузки](#upsert-options).

### Параметры для upsert {#upsert-options}

Имя параметра | Описание параметра
---|---
`--len` | Размер строк в байтах, которые вставляются в таблицу, как values. Значение по умолчанию: 8.

## Нагрузка insert {#insert-kv}

Данный вид нагрузки вставляет кортежи (key, value1, value2, ..., valueN)

YQL Запрос:

```yql
DECLARE r0 AS Uint64
DECLARE c00 AS String;
DECLARE c01 AS String;
...
DECLARE c0{N - 1} AS String;
DECLARE r1 AS Uint64
DECLARE c10 AS String;
DECLARE c11 AS String;
...
DECLARE c1{N - 1} AS String;
INSERT INTO `kv_test`(c0, c1, ... cN) VALUES ( (r0, c00, ... c0{N - 1}), (r1, c10, ... c1{N - 1}), ... )
```

Для запуска данного вида нагрузки необходимо выполнить команду:

```bash
{{ ydb-cli }} workload kv run insert [global workload options...] [specific workload options...]
```

* `global workload options` - [общие параметры для всех видов нагрузки](#global-workload-options).
* `specific workload options` - [параметры конкретного вида нагрузки](#insert-options).

### Параметры для insert {#insert-options}

Имя параметра | Описание параметра
---|---
`--len` | Размер строк в байтах, которые вставляются в таблицу, как values. Значение по умолчанию: 8.

## Нагрузка select {#select-kv}

Данный вид нагрузки создает SELECT запросы, возвращающие строки по точному совпадению primary ключа.

YQL Запрос:

```yql
DECLARE r0 AS Uint64
DECLARE r1 AS Uint64
...
DECLARE rM AS Uint64
SELECT * FROM `kv_test`(c0, c1, ..., cN) WHERE (
    c0 == r0 OR
    c0 == r1 OR
    ...
    c0 == rM
)
```

Для запуска данного вида нагрузки необходимо выполнить команду:

```bash
{{ ydb-cli }} workload kv run select [global workload options...]
```

* `global workload options` - [общие параметры для всех видов нагрузки](#global-workload-options).
