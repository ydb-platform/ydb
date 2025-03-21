# MATCH_RECOGNIZE

Recognizing patterns in a sequence of rows has been a widely desired feature, but it was not possible until recently with SQL. There were workarounds available, but they were inefficient, difficult to implement and hard to comprehend. Now, the MATCH_RECOGNIZE clause allows you to do this in SQL efficiently.

The ability to detect patterns across multiple rows is important for various business areas, such as fraud detection, pricing analysis in finance, and sensor data processing. This area is known as Complex event processing (CEP), and pattern recognition is a valuable tool for this.

Here is a hands-on example of pattern recognizing in a data table produced by an IoT device, where pressing its buttons triggers certain events. Let's assume you need to find and process the following sequence of button clicks: `button 1`, `button 2`, and `button 3`.

The structure of the data to transmit is as follows:

```json
{"ts": 1700000000, "button": 1, "device_id": 1, "zone_id": 24}
{"ts": 1701000000, "button": 2, "device_id": 2, "zone_id": 12}
```

The body of the SQL query looks like this:

```sql
PRAGMA FeatureR010="prototype";

SELECT * FROM bindings.input_table MATCH_RECOGNIZE ( -- Performing pattern matching from input_table
    PARTITION BY device_id, zone_id -- Partitioning the input data into groups by columns device_id and zone_id
    ORDER BY ts -- Viewing events based on the ts column data sorted ascending
    MEASURES
        LAST(B1.ts) AS b1, -- Going to get the latest timestamp of clicking button 1 in the query results
        LAST(B3.ts) AS b3  -- Going to get the latest timestamp of clicking button 3 in the query results
    ONE ROW PER MATCH            -- Going to get one result row per match hit
    AFTER MATCH SKIP TO NEXT ROW -- Going to move to the next row once the pattern match is hit
    PATTERN (B1 B2+ B3) -- Searching for a pattern that includes one button 1 click, one or more button 2 clicks, and one button 3 click
    DEFINE
        B1 AS B1.button = 1, -- Defining the B1 variable as event of clicking button 1 (the button field equals 1)
        B2 AS B2.button = 2, -- Defining the B2 variable as event of clicking button 2 (the button field equals 2)
        B3 AS B3.button = 3  -- Defining the B3 variable as event of clicking button 3 (the button field equals 3)
);
```

## Syntax {#syntax}

The `MATCH_RECOGNIZE` command searches for data based on a given pattern and returns the hits. Here is the SQL syntax of the `MATCH_RECOGNIZE` command:

```sql
MATCH_RECOGNIZE (
    [ PARTITION BY <partition_1> [ ... , <partition_N> ] ]
    [ ORDER BY <sort_key_1> [ ... , <sort_key_N> ] ]
    [ MEASURES <expression_1> AS <column_name_1> [ ... , <expression_N> AS <column_name_N> ] ]
    [ ONE ROW PER MATCH | ALL ROWS PER MATCH ]
    [ AFTER MATCH SKIP (TO NEXT ROW | PAST LAST ROW) ]
    PATTERN (<search_pattern>)
    DEFINE <variable_1> AS <predicate_1> [ ... , <variable_N> AS <predicate_N> ]
)
```

Here is a brief description of the SQL syntax elements of the `MATCH_RECOGNIZE` command:
* [`DEFINE`](#define): Conditions the rows must meet for each variable: `<variable_1> AS <predicate_1> [ ... , <variable_N> AS <predicate_N> ]`.
* [`PATTERN`](#pattern): Pattern to search for across the data. It consists of variables and search rules of the pattern described in `<search_pattern>`. `PATTERN` works similarly to [regular expressions](https://en.wikipedia.org/wiki/Regular_expressions).
* [`MEASURES`](#measures): Specifies the list of output columns. Each column of the `<expression_1> AS <column_name_1> [ ... , <expression_N> AS <column_name_N> ]` list is an independent construct that sets output columns and describes expressions for their computation.
* [`ONE ROW PER MATCH`](#rows_per_match): Determines the amount of output data for each hit match.
* [`AFTER MATCH SKIP TO NEXT ROW`](#after_match_skip_to_next_row): Defines the method of moving to the point of the next match search.
* [`ORDER BY`](#order_by): Determines sorting of input data. Pattern search is performed within the data sorted according to the list of columns or expressions listed in `<sort_key_1> [ ... , <sort_key_N> ]`.
* [`PARTITION BY`](#partition_by) divides the input data flow as per the specified rules in accordance with `<partition_1> [ ... , <partition_N> ]`. Pattern search is performed independently in each part.

### DEFINE {#define}

```sql
DEFINE <variable_1> AS <predicate_1> [ ... , <variable_N> AS <predicate_N> ]
```

`DEFINE` declares variables that are searched for in the input data. Variables are names of SQL expressions computed over the input data. SQL expressions in `DEFINE` have the same meaning as search expressions in a `WHERE` SQL clause. For example, the `button = 1` expression searches for all rows that contain the `button` column with the `1` value. Any SQL expressions that can be used to perform a search, including aggregation functions like `LAST` or `FIRST`, can act as conditions. such as `button > 2 AND zone_id < 12` or `LAST(button) > 10`.

In your SQL statements, make sure to specify the variable name for which you are searching for matches. For instance, in the following SQL command, you need to specify the variable name for which the calculation is being performed (`A`), for the `button = 1` condition:

```sql
DEFINE
    A AS A.button = 1
```

{% note info %}

The column list does not currently support aggregation functions (e.g., `AVG`, `MIN`, or `MAX`) and `PREV` and `NEXT` functions.

{% endnote %}

When processing each row of data, all logical expressions of all `DEFINE` keyword variables are calculated. If during the calculation of `DEFINE` variable expressions the logical expression gets the `TRUE` value, such a row is labeled with the `DEFINE` variable name and added to the list of rows subject to pattern matching.

#### **Example** {#define-example}

When defining variables in SQL expressions, you can reference other variables:

```sql
DEFINE
    A AS A.button = 1 AND LAST(A.zone_id) = 12,
    B AS B.button = 2 AND FIRST(A.zone_id) = 12
```

An input data row will be computed as variable `A` if it contains a `button` column with value `1` and the last row of the set of previously matched `A` has a `zone_id` column with value `12`. The row will be computed as variable `B` if the data row contains a `button` column with value `2` and the first row of the set of previously matched variables `A` has a `zone_id` column with value `12`.

### PATTERN {#pattern}

```sql
PATTERN (<search_pattern>)
```

The `PATTERN` keyword describes the search pattern in the format derived from variables in the `DEFINE` section. The `PATTERN` syntax is similar to the one of [regular expressions](https://en.wikipedia.org/wiki/Regular_expressions).

{% note warning %}

If a variable used in the `PATTERN` section has not been previously described in the `DEFINE` section, it is assumed that it is always `TRUE`.

{% endnote %}

You can use [quantifiers](https://en.wikipedia.org/wiki/Regular_expression#Quantification) in `PATTERN`. In regular expressions, they determine the number of repetitions of an element or subsequence in the matched pattern. Let’s use the `A`, `B`, `C`, and `D` variables from the `DEFINE` section to explain how quantifiers work. Here is the list of supported quantifiers:

|Quantifier|Description|
|----|-----|
|`A+`|One or more occurrences of `A`|
|`A*`|Zero or more occurrences of `A`|
|`A?`|Zero or one occurrence of `A`|
|`B{n}`|Exactly `n` occurrences of `B`|
|`C{n, m}`|From `n` to `m` occurrences of `C` (`m` inclusive)|
|`D{n,}`|At least `n` occurrences of `D`|
|`(A\|B)`|Occurrence of `A` or `B` in the data|
|`(A\|B){,m}`|No more than `m` occurrences of `A` or `B` (`m` inclusive)|

Supported pattern search sequences:

|Supported sequences|Syntax|Description|
|---|---|----|
|Sequence|`A B+ C+ D+`|The system searches for the exact specified sequence, the occurrence of other variables within the sequence is not allowed. The pattern search is performed in the order of the pattern variables.|
|One of|`A \| B \| C`|Variables are listed in any order with a pipe \| between them. The search is performed for any variable from the specified list.|
|Grouping|`(A \| B)+ \| C`|Variables inside round brackets are considered a single group. In this case, quantifiers apply to the entire group.|
|Exclusion from result|`{- A B+ C -}`|Strings found by the pattern in parentheses will be excluded from the result in [`ALL ROWS PER MATCH`](#rows_per_match) mode|

#### **Example** {#pattern-example}

```sql
PATTERN (B1 E* B2+ B3)
DEFINE
    B1 as B1.button = 1,
    B2 as B2.button = 2,
    B3 as B3.button = 3
```

The `DEFINE` section describes the `B1`, `B2`, and `B3` variables, while it does not describe `E`. Such notation allows interpreting `E` as any event, so the following pattern will be searched: one `button 1` click, one or more `button 2` clicks, and one `button 3` click. Meanwhile, between a click of `button 1` and `button 2`, any number of any other events may occur.

### MEASURES {#measures}

```sql
MEASURES <expression_1> AS <column_name_1> [ ... , <expression_N> AS <column_name_N> ]
```

`MEASURES` describes the set of returned columns when a pattern is found. A set of returned columns should be represented by an SQL expression with the aggregate functions over the variables declared in the [`DEFINE`](#define) statement.

#### **Example** {#measures-example}

The input data for the example are:

```json
{"ts": 100, "button": 1, "device_id": 3, "zone_id": 0}
{"ts": 200, "button": 1, "device_id": 3, "zone_id": 1}
{"ts": 300, "button": 2, "device_id": 2, "zone_id": 0}
{"ts": 400, "button": 3, "device_id": 1, "zone_id": 1}
```

```sql
MEASURES
    AGGREGATE_LIST(B1.zone_id * 10 + B1.device_id) AS ids,
    COUNT(DISTINCT B1.zone_id) AS count_zones,
    LAST(B3.ts) - FIRST(B1.ts) AS time_diff,
    42 AS meaning_of_life
PATTERN (B1+ B2 B3)
DEFINE
    B1 AS B1.button = 1,
    B2 AS B2.button = 2,
    B3 AS B3.button = 3
```

Result:

|ids|count_zones|time_diff|meaning_of_life|
|--|--|--|--|
|[3,13]|2|300|42|

The `ids` column contains the list of `zone_id * 10 + device_id` values counted among the rows matched with the `B1` variable. The `count_zones` column contains the number of unique values of the `zone_id` column among the rows matched with the `B1` variable. Column `time_diff` contains the difference between the value of column `ts` in the last row of the set of rows matched with variable `B3` and the value of column `ts` in the first row of the set of rows matched with variable `B1`. The `meaning_of_life` column contains the constant `42`. Thus, an expression in `MEASURES` may contain aggregate functions over multiple variables, but there must be only one variable within a single aggregate function.

### ROWS PER MATCH {#rows_per_match}

`ROWS PER MATCH` determines the number of pattern matches. Default mode - `ONE ROW PER MATCH`.

`ONE ROW PER MATCH` sets the `ROWS PER MATCH` mode to output one row per recognized pattern. The data schema of the result will be a union of [partitioning columns](#partition_by) and all [measures](#measures) columns.

`ALL ROWS PER MATCH` sets the `ROWS PER MATCH` mode to output all rows of recognized pattern except explicitly excluded by parentheses in [pattern](#pattern). The data schema of the result will be a union of input columns and all [measures](#measures) columns.

#### **Examples** {#rows_per_match-examples}

The input data for all examples are:

```json
{"button": 1, "ts": 100}
{"button": 2, "ts": 200}
{"button": 3, "ts": 300}
```

##### **Example 1** {#rows_per_match-example1}

```sql
MEASURES
    FIRST(B1.ts) AS first_ts,
    FIRST(B2.ts) AS mid_ts,
    LAST(B3.ts) AS last_ts
ONE ROW PER MATCH
PATTERN (B1 {- B2 -} B3)
DEFINE
    B1 AS B1.button = 1,
    B2 AS B2.button = 2,
    B3 AS B3.button = 3
```

Result:

|first_ts|mid_ts|last_ts|
|--|--|--|
|100|200|300|

##### **Example 2** {#rows_per_match-example2}

```sql
MEASURES
    FIRST(B1.ts) AS first_ts,
    FIRST(B2.ts) AS mid_ts,
    LAST(B3.ts) AS last_ts
ONE ROW PER MATCH
PATTERN (B1 {- B2 -} B3)
DEFINE
    B1 AS B1.button = 1,
    B2 AS B2.button = 2,
    B3 AS B3.button = 3
```

Result:

|first_ts|mid_ts|last_ts|button|ts|
|--|--|--|--|--|
|100|200|300|1|100|
|100|200|300|3|300|

### AFTER MATCH SKIP {#after_match_skip}

`AFTER MATCH SKIP TO NEXT ROW` determines the method of transitioning from the found match to searching the next match. Only the `AFTER MATCH SKIP TO NEXT ROW` and `AFTER MATCH SKIP PAST LAST ROW` modes are supported. Default mode - `PAST LAST ROW`.

#### Examples {#after_match_skip-examples}

The input data for all examples are:

```json
{"button": 1, "ts": 100}
{"button": 1, "ts": 200}
{"button": 2, "ts": 300}
{"button": 3, "ts": 400}
```

##### **Example 1** {#after_match_skip-example1}

```sql
MEASURES
    FIRST(B1.ts) AS first_ts,
    LAST(B3.ts) AS last_ts
AFTER MATCH SKIP TO NEXT ROW
PATTERN (B1+ B2 B3)
DEFINE
    B1 AS B1.button = 1,
    B2 AS B2.button = 2,
    B3 AS B3.button = 3
```

Result:

|first_ts|last_ts|
|--|--|
|100|400|
|200|400|

##### **Example 2** {#after_match_skip-example2}

```sql
MEASURES
    FIRST(B1.ts) AS first_ts,
    LAST(B3.ts) AS last_ts
AFTER MATCH SKIP PAST LAST ROW
PATTERN (B1+ B2 B3)
DEFINE
    B1 AS B1.button = 1,
    B2 AS B2.button = 2,
    B3 AS B3.button = 3
```

Result:

|first_ts|last_ts|
|--|--|
|100|400|

### ORDER BY {#order_by}

```sql
ORDER BY <sort_key_1> [ ... , <sort_key_N> ]

<sort_key> ::= { <column_names> | <expression> }
```

`ORDER BY` determines sorting of the input data. That is, before all pattern search operations are performed, the data will be pre-sorted according to the specified keys or expressions. The syntax is similar to the `ORDER BY` SQL expression.

#### **Example** {#order_by-example}

```sql
ORDER BY CAST(ts AS Timestamp)
```

### PARTITION BY {#partition_by}

```sql
PARTITION BY <partition_1> [ ... , <partition_N> ]

<partition> ::= { <column_names> | <expression> }
```

`PARTITION BY` partitions the input data according to the list of the fields specified in this keyword. The expression converts the source data into multiple independent groups, each used for an independent pattern search. If the command is not specified, all data is processed as a single group.

#### **Example** {#partition_by-example}

```sql
PARTITION BY device_id, zone_id
```

## Limitations {#limitations}

Our support for the `MATCH_RECOGNIZE` command will eventually comply with [SQL-2016](https://ru.wikipedia.org/wiki/SQL:2016); currently, however, the following limitations apply:
- [`MEASURES`](#measures). Functions `PREV`/`NEXT` are not supported.
- - [`AFTER MATCH SKIP`](#after_match_skip). Only the `AFTER MATCH SKIP TO NEXT ROW` and `AFTER MATCH SKIP PAST LAST ROW` modes are supported.
- [`PATTERN`](#pattern). Union pattern variables are not implemented.
- [`DEFINE`](#define). Aggregation functions are not supported.
