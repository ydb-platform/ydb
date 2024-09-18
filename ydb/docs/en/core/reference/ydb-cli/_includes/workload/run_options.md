### Common parameters for all types of load {#run_options}

Option name | Option description
---|---
`--output <value>` | The name of the file where the results of query execution will be saved. The default value is `results.out`.
`--iterations <value>` | The number of executions of each of the load queries that create the load. The default value is `1`.
`--json <name>` | The name of the file where the query execution statistics will be saved in `json` format. By default, the file is not saved.

`--ministat <name>` | The name of the file where the query execution statistics will be saved in `ministat` format. By default, the file is not saved.

`--plan <name>` | The name of the file to save the query plan. If specified, the `<name>.<query number>.explain` and `<name>.<query number>.<iteration number>` files are saved with plans in several formats: `ast`, `json`, `svg`, and `table`. By default, plans are not saved.
`--query-settings <setting>` | Query execution settings. Each setting will be added as a separate line at the beginning of each query. Not specified by default. If you need to specify multiple settings, use the option multiple times.
`--include` | Numbers or segments of query numbers to be executed as part of the load. By default, all queries are executed. Separated by commas, for example `1,2,4-6`.
`--exclude` | Numbers or segments of query numbers to be excluded from the load. By default, all queries are executed. Separated by commas, for example `1,2,4-6`.
`--executer` | Query execution engine, available values: `scan`, `generic`. Default value is `generic`.
`--verbose` or `-v` | Print more information to the screen while queries are being executed.
