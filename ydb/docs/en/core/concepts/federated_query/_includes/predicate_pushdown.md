The {{ ydb-short-name }} federated query processing system is capable of delegating the execution of certain parts of a query to the system acting as the data source. Query fragments are passed through {{ ydb-short-name }} directly to the external system and processed within it. This optimization, known as "predicate pushdown", significantly reduces the volume of data transferred from the source to the federated query processing engine. This reduces network load and saves computational resources for {{ ydb-short-name }}.

A specific case of predicate pushdown, where filtering expressions specified after the `WHERE` keyword are passed down, is called "filter pushdown". Filter pushdown is possible when using:

|Description|Example|
|---|---|
|Filters like `IS NULL`/`IS NOT NULL`|`WHERE column1 IS NULL` or `WHERE column1 IS NOT NULL`|
|Logical conditions `OR`, `NOT`, `AND`|`WHERE column IS NULL OR column2 is NOT NULL`|
|Comparison conditions `=`, `<>`, `<`, `<=`, `>`, `>=` with other columns or constants|`WHERE column3 > column4 OR column5 <= 10`|

Supported data types for filter pushdown:

|{{ ydb-short-name }} Data Type|
|----|
|`Bool`|
|`Int8`|
|`Uint8`|
|`Int16`|
|`Uint16`|
|`Int32`|
|`Uint32`|
|`Int64`|
|`Uint64`|
|`Float`|
|`Double`|