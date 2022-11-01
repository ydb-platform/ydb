# Overview

You can use the following {{ ydb-short-name }} CLI commands to run YQL queries:

* [ydb yql](yql.md): Runs YQL queries and scripts with streaming enabled (without limiting the amount of returned data).
* [ydb scripting yql](scripting-yql.md): Runs YQL queries and scripts, limiting the amount of data returned. You can also use this command to view the query execution plan and the response metadata.
* [ydb table query execute](table-query-execute.md): Runs single queries of a specified type. You can set the mode of transaction isolation.

{% note info %}

To run YQL queries, we recommend the `ydb yql` command.

In the future, it is planned to abandon `ydb scripting yql` and `ydb table query execute` commands providing all the necessary functionality with `ydb yql` command.

{% endnote %}
