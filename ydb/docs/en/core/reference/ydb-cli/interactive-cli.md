# Interactive query execution mode

After executing `{{ ydb-cli }}` command without subcommands, the interactive query execution mode will be launched. The console or terminal will be switched to interactive mode. After that, you can enter queries directly into the console or terminal, and when you enter a newline character, the query is considered completed and it starts to execute. The query text can be either a YQL query or a [special command](#spec-commands).

General format of the command:

```bash
{{ ydb-cli }} [global options...]
```

* `global options` — [global parameters](commands/global-options.md).

## Features {#features}
* [Query history](#query-history).
* Syntax highlighting based on YQL grammar.
* [Hotkeys](#hotkeys).
* [Special commands](#spec-commands).
* [Auto completion](#auto-completion).

### Query history {#query-history}

Using up and down arrow keys you can navigate through the query history:

![History](_assets/history.gif)

History is stored locally, as though persists between CLI launches.

A query search function (`CTRL + R`) is also supported.

## Hotkeys {#hotkeys}

| Hotkey        | Description                                                               |
|---------------|---------------------------------------------------------------------------|
| `CTRL + D`    | Allows you to exit interactive mode.                                      |
| `Up arrow`    | Scrolls through query history toward older queries.                       |
| `Down arrow`  | Scrolls through query history toward newer queries.                       |
| `TAB`         | Autocompletes the entered text to a suitable YQL command.                 |
| `CTRL + R`    | Allows searching for a query in history containing a specified substring. |

## Special commands {#spec-commands}

Special commands are CLI-specific commands and are not part of the YQL syntax. They are intended for performing various functions that cannot be accomplished through a YQL query.

| Command                  | Description                                                                                                                                                                      |
|--------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `SET param = value`      | The `SET` command sets the value of the [internal variable](#internal-vars) `param` to `value`.                                                                                  |
| `EXPLAIN query-text`     | Outputs the query plan for `query-text`. Equivalent to the command [ydb table query explain](commands/explain-plan.md#explain-plan).                                             |
| `EXPLAIN AST query-text` | Outputs the query plan for `query-text` along with the [AST](commands/explain-plan.md). Equivalent to the command [ydb table query explain --ast](commands/explain-plan.md#ast). |

### List of internal variables {#internal-vars}

Internal variables determine the behavior of commands and are set using the [special command](#spec-commands) `SET`.

| Variable | Description |
|----------|---|
| `stats`  | The statistics collection mode for subsequent queries.<br/>Acceptable values:<ul><li>`none` (default): Do not collect.</li><li>`basic`: Collect statistics.</li><li>`full`: Collect statistics and query plan.</li></ul> |

### Examples {#examples}

Executing a query in the `full` statistics collection mode:

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

## Auto completion {#auto-completion}

While in interactive mode, auto completion helps with creating a query text showing suggestions of completion of current word based on the YQL syntax.

There are two types of suggestions: auto completion by pressing `TAB` key and interactive hints.

### Auto completion by pressing `TAB` key {#auto-completion-tab}

While in interactive mode, pressing `TAB` key will show a list of suggestions of completion of current word according to the YQL syntax.

If there is only one suggestion available, pressing TAB will complete current word to it.

### Interactive Hints {#interactive-hints}

While typing in interactive mode, a list of hints will appear under the cursor, showing first 4 suggestions of completion of current word according to the YQL grammar.

When there is only one suggestion left, it appears in-line.
