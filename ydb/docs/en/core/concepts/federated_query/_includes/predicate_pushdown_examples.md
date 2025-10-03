|Description|Example|
|---|---|
|`NULL` checks|`WHERE column1 IS NULL` or `WHERE column1 IS NOT NULL`|
|Logical conditions `OR`, `NOT`, `AND` and parentheses for controlling calculation priority. |`WHERE column1 IS NULL OR (column2 IS NOT NULL AND column3 > 10)`.|
|[Comparison operators](../../../yql/reference/syntax/expressions.md#comparison-operators) with other columns or constants. |`WHERE column1 > column2 OR column3 <= 10`, `WHERE column1 + column2 > 10`, `WHERE column1 = (10 + 10)`|

When using other types of filters, pushdown to the data source is not performed: filtering of the external table rows will be executed by the federated {{ ydb-short-name }}, which means that {{ ydb-short-name }} will perform a full scan of the external table when processing the query.