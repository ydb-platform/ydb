# Проведение нагрузочного тестирования

В {{ ydb-short-name }} встроен инструментарий для проведения нагрузочного тестирования несколькими стандартными бенчмарками:

| Бенчмаркам                           | Справка                                                  |
|--------------------------------------|----------------------------------------------------------|
| [TPC-C](https://tpc.org/tpcc/)       | [tpcc](../../reference/ydb-cli/workload-tpcc.md) |
| [TPC-H](https://tpc.org/tpch/)       | [tpch](../../reference/ydb-cli/workload-tpch.md)|
| [TPC-DS](https://tpc.org/tpcds/)     | [tpcds](../../reference/ydb-cli/workload-tpcds.md)|
| [ClickBench](https://benchmark.clickhouse.com/) | [clickbench](../../reference/ydb-cli/workload-click-bench.md)|

Работают сходным образом, детальное описание для каждого см. в соответствующих разделах, ссылки выше.
Все команды для работы с бенчмарками собраны в соответствующие группы, при этом для всех команд единым образом задается путь в БД:

```bash
{{ ydb-cli }} workload tpcc --path path/in/database ...
{{ ydb-cli }} workload clickbench --path path/in/database ...
{{ ydb-cli }} workload tpch --path path/in/database ...
{{ ydb-cli }} workload tpcds --path path/in/database ...
```

Нагрузочное тестирование можно разбить на 3 этапа:

  1. [Подготовка данных](#data-preparation)
  1. [Тестирование](#testing)
  1. [Очистка](#cleanup)

## Подготовка данных {#data-preparation}

Состоит из двух этапов, это инициализация таблиц и наполнение их данными.

### Инициализация

Инициализация производится командой `init`:

```bash
{{ ydb-cli }} workload tpcc --path tpcc/10wh init
{{ ydb-cli }} workload clickbench --path clickbench/hits init --store=row
{{ ydb-cli }} workload tpch --path tpch/s1 init --store=column
{{ ydb-cli }} workload tpcds --path tpcds/s1 init --store=external-s3
```

На данном этапе, если вы запускаете `tpch`, `tpcds` или `clickbench`, можно настроить создаваемые таблицы:

  * Выбрать тип используемых таблиц: строковые, колоночные, внешние и тд. (параметр `--store`);
  * Выбрать типы используемых колонок: строк (параметр `--string`), дат и времени (`--datetime`) и тип вещественных чисел (`--float-mode`).

Также можно указать, что перед созданием таблицы должны быть удалены, если они уже созданы. Параметра `--clear`

Подробнее см. описание команд для каждого бенчмарка:

* [tpcc init](../../reference/ydb-cli/workload-tpcc.md#init)
* [clickbench init](../../reference/ydb-cli/workload-click-bench.md#init)
* [tpch init](../../reference/ydb-cli/workload-tpch.md#init)
* [tpcds init](../../reference/ydb-cli/workload-tpcds.md#init)

### Наполнение данными

Наполнение данными выполняется при помощи команды `import`. Данная команда специфична для каждого бенчмарка и её поведение зависит от подкоманд. При этом есть и общие для всех параметры.

Подробное описание см. в соответствующих разделах:

* [tpcc import](../../reference/ydb-cli/workload-tpcc.md#load)
* [clickbench import](../../reference/ydb-cli/workload-click-bench.md#load)
* [tpch import](../../reference/ydb-cli/workload-tpch.md#load)
* [tpcds import](../../reference/ydb-cli/workload-tpcds.md#load)

Примеры:

```bash
{{ ydb-cli }} workload tpcc --path tpcc/10wh import
{{ ydb-cli }} workload clickbench --path clickbench/hits import files --input hits.csv.gz
{{ ydb-cli }} workload tpch --path tpch/s1 import generator --scale 1
{{ ydb-cli }} workload tpcds --path tpcds/s1 import generator --scale 1
```

## Тестирование {#testing}

Непосредственно тестирование выполняется командой `run`. Её поведение практически одинаково для разных бенчмарков, хотя некоторые различия всё-таки присутствуют.

Примеры:

```bash
{{ ydb-cli }} workload tpcc --path tpcc/10wh run
{{ ydb-cli }} workload clickbench --path clickbench/hits run --include 1-5,8
{{ ydb-cli }} workload tpch --path tpch/s1 run --exсlude 3,4 --iterations 3
{{ ydb-cli }} workload tpcds --path tpcds/s1 run --plan ~/query_plan --include 2 --iterations 5
```

Команда позволяет выбрать запросы для исполнения, сгенерировать несколько видов отчетов, собрать статистику исполнения и тд.

Подробное описание см. в соответствующих разделах:

* [tpcc run](../../reference/ydb-cli/workload-tpcc.md#run)
* [clickbench run](../../reference/ydb-cli/workload-click-bench.md#run)
* [tpch run](../../reference/ydb-cli/workload-tpch.md#run)
* [tpcds run](../../reference/ydb-cli/workload-tpcds.md#run)

## Очистка {#cleanup}

После выполнения всего необходимого тестирования данные могут быть удалены из БД.
Сделано это может быть при помощи команды `clean`:

```bash
{{ ydb-cli }} workload tpcc --path tpcc/10wh clean
{{ ydb-cli }} workload clickbench --path clickbench/hits clean
{{ ydb-cli }} workload tpch --path tpch/s1 clean
{{ ydb-cli }} workload tpcds --path tpcds/s1 clean
```

Подробное описание см. в соответствующих разделах:

* [tpcc clean](../../reference/ydb-cli/workload-tpcc.md#cleanup)
* [clickbench clean](../../reference/ydb-cli/workload-click-bench.md#cleanup)
* [tpch clean](../../reference/ydb-cli/workload-tpch.md#cleanup)
* [tpcds clean](../../reference/ydb-cli/workload-tpcds.md#cleanup)
