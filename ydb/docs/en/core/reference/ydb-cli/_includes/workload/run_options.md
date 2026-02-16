### Common parameters for all load types {#run_options}

Name | Description | Default value
---|---|---
`--dry-run` | Do not execute initialization queries, but only display their text. |
`--check-canonical` or `-c` | Use special version of queries (they have deterministic answers) and compare results with canonical ones. |
`--output <value>` | The name of the file where the query execution results will be saved. | `results.out`
`--iterations <value>` | The number of times each load query will be executed. | `1`
`--json <name>` | The name of the file where query execution statistics will be saved in `json` format. | Not saved by default
`--ministat <name>` | The name of the file where query execution statistics will be saved in `ministat` format. | Not saved by default
`--csv <name>` | The name of the file to save the CSV version of the result table. | Not saved by default
`--plan <name>` | The name of the file to save the query plan. Files like `<name>.<query number>.explain` and `<name>.<query number>.<iteration number>` will be saved in formats: `ast`, `json`, `svg`, and `table`. | Not saved by default
`--query-prefix <setting>` | Query prefix. Every prefix is a line that will be added to the beginning of each query. For multiple prefix lines use this option several times. | Not specified by default
`--retries` | Max retry count for every request. | `0`
`--include` | Names, numbers or ranges of query numbers to be executed as part of the load. Specified as a comma-separated list, e.g.: `1,2,4-6`. | All queries executed
`--exclude` | Names, numbers or ranges of query numbers to be excluded from the load. Specified as a comma-separated list, e.g.: `1,2,4-6`. | None excluded by default
`--verbose` or `-v` | Print additional information to the screen during query execution. |
`--global-timeout <value>` | Global timeout for all queries. Supports time units (e.g., '5s', '1m'). Plain number interpreted as milliseconds. | Not specified by default. The time is unlimited.
`--request-timeout <value>` | Timeout for each iteration of each query. Supports time units (e.g., '5s', '1m'). Plain number interpreted as milliseconds. | Not specified by default. The time is unlimited.
`--threads <value>` or `-t <value>` | The number of parallel threads generating the load. Zero means that queries will be executed in the main thread; otherwise, queries will be mixed. | `0`
