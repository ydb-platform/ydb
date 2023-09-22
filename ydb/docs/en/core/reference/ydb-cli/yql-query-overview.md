# Overview

You can use the following {{ ydb-short-name }} CLI commands to run YQL queries:

1. [ydb yql](yql.md): Runs YQL queries and scripts with streaming enabled (without limiting the amount of returned data).
2. [ydb scripting yql](scripting-yql.md): Runs YQL queries and scripts, limiting the amount of data returned. You can also use this command to view the query execution plan and the response metadata.
3. [ydb table query execute](table-query-execute.md): Runs [DML queries](https://en.wikipedia.org/wiki/Data_manipulation_language#SQL) with a given level of transaction isolation and a standard retry policy.
4. [ydb](interactive-cli.md): Switches the console to interactive mode and executes YQL queries and scripts.

Commands [1-3] support the same functionality for [query parameterization in YQL](parameterized-queries-cli.md).
