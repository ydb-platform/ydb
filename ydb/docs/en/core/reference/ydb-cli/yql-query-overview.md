# Overview

You can use the following {{ ydb-short-name }} CLI commands to run YQL queries:

* [ydb yql](yql.md): Runs YQL queries and scripts with streaming enabled (without limiting the amount of returned data).
* [ydb scripting yql](scripting-yql.md): Runs YQL queries and scripts, limiting the amount of data returned. You can also use this command to view the query execution plan and the response metadata.
* [ydb table query execute](table-query-execute.md): Runs [DML queries](https://en.wikipedia.org/wiki/Data_manipulation_language#SQL) with a given level of transaction isolation and a standard retry policy.

All the above commands support the same functionality for [query parameterization in YQL](parameterized-queries-cli.md).

