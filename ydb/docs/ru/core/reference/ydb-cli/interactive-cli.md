# Интерактивный режим выполнения запросов

Выполнив команду `ydb` без подкоманд, запустится интерактивный режим выполнения запросов. Консоль или терминал будут переведены в интерактивный режим. После этого можно вводить запросы напрямую в консоль или терминал, при вводе символа перевода строки запрос считается законченным и он начинает исполняться. Текст запроса может представлять из себя как YQL запрос, так и [специальную команду](#spec-commands). Также между запусками сохраняется история запросов, доступны автодополнение команд и поиск по истории запросов.

Общий вид команды:

```bash
{{ ydb-cli }} [global options...]
```

* `global options` — [глобальные параметры](commands/global-options.md).

## Горячие клавиши {#hotkeys}

Горячая клавиша | Описание
---|---
`CTRL + D` | Позволяет выйти из интерактивного режима.
`Up arrow` | Пролистывает историю запросов в сторону более старых запросов.
`Down arrow` | Пролистывает историю запросов в сторону более новых запросов.
`TAB` | Дополняет введенный текст до подходящей YQL команды.
`CTRL + R` | Позволяет найти в истории запрос, содержащий заданную подстроку.

## Специальные команды {#spec-commands}

Специальные команды специфичны для CLI и не являются частью синтаксиса YQL. Они предназначены для выполнения различных функций, которые нельзя выполнить через YQL запрос.

Команда | Описание
---|---
`SET param = value` | Команда `SET` устанавливает значение [внутренней переменной](#internal-vars) `param` в `value`.
`EXPLAIN query-text` | Выводит план запроса `query-text`. Эквивалентна команде [ydb table query explain](commands/explain-plan.md#explain-plan).
`EXPLAIN AST query-text` | Выводит план запроса `query-text` вместе с [AST](commands/explain-plan.md). Эквивалентна команде [ydb table query explain --ast](commands/explain-plan.md#ast).

### Список внутренних переменных {#internal-vars}

Внутренние переменные устанавливают поведение команд и задаются с помощью [специальной команды](#spec-commands) `SET`.

Переменная | Описание
---|---
`stats` | Режим сбора статистики для последующих запросов.<br/>Возможные значения:<ul><li>`none` (по умолчанию) — не собирать;</li><li>`basic` — собирать статистику;</li><li>`full` — собирать статистику и план запроса.</li></ul>

## Примеры {#examples}

Выполнение запроса в режиме сбора статистики `full`:

```bash
$ ydb
ydb> SET stats = full
ydb> select * from table1 limit 1
┌────┬─────┬───────┐
│ id │ key │ value │
├────┼─────┼───────┤
│ 10 │ 0   │ ""    │
└────┴─────┴───────┘

Statistics:
query_phases {
  duration_us: 14987
  table_access {
    name: "/ru-central1/a1v7bqj3vtf10qjleyow/laebarufb61tguph3g22/table1"
    reads {
      rows: 9937
      bytes: 248426
    }
  }
  cpu_time_us: 2925
  affected_shards: 1
}
process_cpu_time_us: 3816
total_duration_us: 79530
total_cpu_time_us: 6741


Full statistics:
Query 0:
ResultSet
└──Limit (Limit: 1)
   TotalCpuTimeUs: 175
   TotalTasks: 1
   TotalInputBytes: 6
   TotalInputRows: 1
   TotalOutputBytes: 16
   TotalDurationMs: 0
   TotalOutputRows: 1
   └──<UnionAll>
      └──Limit (Limit: 1)
      └──TableFullScan (ReadColumns: ["id","key","value"], ReadRanges: ["key (-∞, +∞)"], Table: impex_table)
         Tables: ["table1"]
         TotalCpuTimeUs: 154
         TotalTasks: 1
         TotalInputBytes: 0
         TotalInputRows: 0
         TotalOutputBytes: 16
         TotalDurationMs: 0
         TotalOutputRows: 1
```
