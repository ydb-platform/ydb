# Optimizer hints

Optimizer hints allow you to influence the behavior of the cost-based optimizer when planning SQL query execution. {{ ydb-short-name }} supports four types of hints for managing joins and statistics.

## Usage

Hints are set via the `PRAGMA ydb.OptimizerHints` pragma at the beginning of the SQL query.
If the optimizer fails to apply at least one of the specified hints to the query, the user will be notified via a warning.

## Syntax

Hints are specified as a string containing an array of expressions of one of four types:


```text
JoinType(TableList JoinType)
Rows(TableList Op Value)
Bytes(TableList Op Value)
JoinOrder(JoinTree)

where:
TableList - enumeration of table names or aliases from the query
Op - operation:
  - `#` - set absolute value
  - `*` - multiply by value
  - `/` - divide by value
  - `+` - add value
  - `-` - subtract value
  - `Number` - numeric value
Value - numeric value
JoinTree - representation of a binary tree using parentheses, for example: (R S) (T U)
```


For example, the following query uses three `Rows` hints that set [cardinality](../../concepts/glossary.md#cardinality), as well as a `JoinOrder` hint for full join order and a `JoinType` hint for join algorithm selection:


```sql
PRAGMA ydb.OptimizerHints =
'
    Rows(R # 10e8)
    Rows(T # 1)
    Rows(R T # 1)
    JoinOrder( (R S) (T U) )
    JoinType(T U Broadcast)
';

SELECT * FROM
    R   INNER JOIN  S   on  R.id = S.id
        INNER JOIN  T   on  R.id = T.id
        INNER JOIN  U   on  T.id = U.id;
```


## Requirements for CBO (Cost Based Optimizer)

{% note info %}

All hints (`Rows`, `Bytes`, `JoinOrder`) work only with the **enabled**[cost-based optimizer](../../concepts/query_execution/optimizer.md), except `JoinType` — you can specify it even when CBO is disabled.

{% endnote %}

## Types of hints

### 1. JoinType — join algorithm

Allows you to force the join algorithm for specific tables.

In {{ ydb-short-name }}, three types of join algorithms are currently supported:

- BroadcastJoin is a type of join in which one of the datasets is small enough to be copied (broadcast) to all the required nodes in the cluster. This allows each node to perform the join locally without transferring data over the network.

{% note info %}

If the join order is not fixed by a separate hint, the optimizer will build both plan variants: where the left and right join inputs are transferred. If the join order is fixed by a hint, the right side of the join will be transferred.

{% endnote %}

- ShuffleJoin is a type of join in which data is shuffled by the join key so that records with the same key are processed on a single processing node. After such data redistribution, each node performs a local join of tables. The results are combined into a single common dataset.
- LookupJoin: for each row of one input, a query is made to the table or index of the other input; currently supported only for [row tables](../../concepts/datamodel/table.md#row-oriented-table).

#### Syntax


```text
JoinType(t1 t2 ... tn Broadcast | Shuffle | Lookup)
```


#### Parameters

- `t1 t2 ... tn` - tables involved in the join
- Algorithm:

  - `Broadcast` - select the BroadcastJoin algorithm
  - `Shuffle`: choose the ShuffleJoin algorithm
  - `Lookup`: choose the LookupJoin algorithm

#### How it works

If the query plan contains a [join operator](../../concepts/glossary.md#operator) that joins only the tables listed in the list, the optimizer will select the specified join algorithm if it is applicable (for example, the LookupJoin algorithm cannot be applied to [columnar tables](../../concepts/datamodel/table.md#column-oriented-table)). If the algorithm cannot be applied, the user will be notified via a warning.

#### Examples


```sql
-- Use Broadcast for joining tables nation, region
JoinType(nation region Broadcast)

-- Use ShuffleJoin for the join whose subtree contains only tables customers, orders, products
JoinType(customers orders products Shuffle)

-- Use LookupJoin for joining tables nation, region
JoinType(nation region Lookup)
```


Apply join algorithm hints to the following query:


```sql
PRAGMA ydb.OptimizerHints =
'
    JoinType(R S Shuffle)
    JoinType(R S T Broadcast)
    JoinType(R S T U Shuffle)
    JoinType(R S T U V Broadcast)
';

SELECT * FROM
    R   INNER JOIN  S   on  R.id = S.id
        INNER JOIN  T   on  R.id = T.id
        INNER JOIN  U   on  T.id = U.id
        INNER JOIN  V   on  U.id = V.id;
```


You can view the query execution plan using the [CLI](../../reference/ydb-cli/commands/explain-plan.md) command:


```bash
 ydb -p <profile_name> sql --explain -f query.sql
```


```text
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│ Operation                                                                               │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│ ┌> ResultSet                                                                            │
│ └─┬> InnerJoin (MapJoin) (U.id = V.id)                                                  │
│   ├─┬> InnerJoin (Grace) (T.id = U.id)                                                  │
│   │ ├─┬> HashShuffle (KeyColumns: ["T.id"], HashFunc: "HashV2")                         │
│   │ │ └─┬> InnerJoin (MapJoin) (R.id = T.id)                                            │
│   │ │   ├─┬> InnerJoin (Grace) (R.id = S.id)                                            │
│   │ │   │ ├─┬> HashShuffle (KeyColumns: ["id"], HashFunc: "HashV2")                     │
│   │ │   │ │ └──> TableFullScan (Table: R, ReadColumns: ["id (-∞, +∞)","payload1","ts"]) │
│   │ │   │ └─┬> HashShuffle (KeyColumns: ["id"], HashFunc: "HashV2")                     │
│   │ │   │   └──> TableFullScan (Table: S, ReadColumns: ["id (-∞, +∞)","payload2"])      │
│   │ │   └──> TableFullScan (Table: T, ReadColumns: ["id (-∞, +∞)","payload3"])          │
│   │ └─┬> HashShuffle (KeyColumns: ["id"], HashFunc: "HashV2")                           │
│   │   └──> TableFullScan (Table: U, ReadColumns: ["id (-∞, +∞)","payload4"])            │
│   └──> TableFullScan (Table: V, ReadColumns: ["id (-∞, +∞)","payload5"])                │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```


Since the optimizer may change the join order during query optimization, the hint must reflect the exact list of tables being joined.
For example, this query assumes the join order will be: R with S, then T, and finally U. Specifying a different join algorithm may change the join order in the plan, and some hints will not be applied. In that case, you can add an additional join order hint.

### 2. Rows — [cardinality](../../concepts/glossary.md#cardinality) hints

Allows you to change the expected number of rows (the optimizer's estimate) for a join or individual tables.

#### How it works

The optimizer will change its row count estimate for the join operation that joins only the tables listed in the list.

#### Syntax


```text
Rows(t1 t2 ... tn (*|/|+|-|#) Number)
```


#### Parameters

- `t1 t2 ... tn` - tables
- Operation:

  - `*` - multiply by value
  - `/` - divide by value
  - `+` - add value
  - `-` - subtract value
  - `#` - replace value
- `Number` - numeric value

#### Examples


```sql
-- Multiply the expected number of rows by 2 for the join whose subtree contains only tables users orders yandex
Rows(users orders yandex * 2.0)

-- Replace the expected number of rows of table products with 1.3e6
Rows(products # 1.3e6)

-- Reduce the expected number of rows by a factor of 228
Rows(filtered_table / 228)

-- Add 5000 rows to the expected result
Rows(table1 table2 + 5000)
```


Let's run the query without [cardinality](../../concepts/glossary.md#cardinality) hints and then see how the hints change the query plan.


```sql
SELECT * FROM
    R   INNER JOIN  S   on  R.id = S.id
        INNER JOIN  T   on  R.id = T.id;
```


Without hints, the optimizer builds the following plan:


```text
┌────────┬────────┬────────┬───────────────────────────────────────────────────────────────────────────────┐
│ E-Cost │ E-Rows │ E-Size │ Operation                                                                     │
├────────┼────────┼────────┼───────────────────────────────────────────────────────────────────────────────┤
│        │        │        │ ┌> ResultSet                                                                  │
│ 114    │ 10     │ 300    │ └─┬> InnerJoin (MapJoin) (S.id = R.id)                                        │
│ 57     │ 10     │ 200    │   ├─┬> InnerJoin (MapJoin) (S.id = T.id)                                      │
│ 0      │ 10     │ 100    │   │ ├──> TableFullScan (Table: S, ReadColumns: ["id (-∞, +∞)","payload2"])    │
│ 0      │ 10     │ 100    │   │ └──> TableFullScan (Table: T, ReadColumns: ["id (-∞, +∞)","payload3"])    │
│ 0      │ 10     │ 100    │   └──> TableFullScan (Table: R, ReadColumns: ["id (-∞, +∞)","payload1","ts"]) │
└────────┴────────┴────────┴───────────────────────────────────────────────────────────────────────────────┘
```


If you apply the following hints:


```sql
PRAGMA ydb.OptimizerHints =
'
    Rows(R # 10e8)
    Rows(T # 1)
    Rows(S # 10e8)
    Rows(R T # 1)
    Rows(R S # 10e8)
';
SELECT * FROM
    R   INNER JOIN  S   on  R.id = S.id
        INNER JOIN  T   on  R.id = T.id;
```


The result is the following plan:


```text
┌───────────┬────────┬────────┬─────────────────────────────────────────────────────────────────────────────────┐
│ E-Cost    │ E-Rows │ E-Size │ Operation                                                                       │
├───────────┼────────┼────────┼─────────────────────────────────────────────────────────────────────────────────┤
│           │        │        │ ┌> ResultSet                                                                    │
│ 3.000e+09 │ 1      │ 100    │ └─┬> InnerJoin (MapJoin) (S.id = R.id)                                          │
│ 0         │ 1e+09  │ 100    │   ├──> TableFullScan (Table: S, ReadColumns: ["id (-∞, +∞)","payload2"])        │
│ 1.500e+09 │ 1      │ 100    │   └─┬> InnerJoin (MapJoin) (R.id = T.id)                                        │
│ 0         │ 1e+09  │ 100    │     ├──> TableFullScan (Table: R, ReadColumns: ["id (-∞, +∞)","payload1","ts"]) │
│ 0         │ 10     │ 100    │     └──> TableFullScan (Table: T, ReadColumns: ["id (-∞, +∞)","payload3"])      │
└───────────┴────────┴────────┴─────────────────────────────────────────────────────────────────────────────────┘
```


Warnings will also be returned:


```text
Warning: Unapplied hint: Rows(R S # 10e8)
```


Here you can see that after applying the [cardinality](../../concepts/glossary.md#cardinality) hints of the base tables, the join order changed, and one of the hints could not be applied because such a join is not present in the plan.

### 3. Bytes — data size hints

Allows you to change the expected data size in bytes for a join or individual tables.

#### Syntax


```text
Bytes(t1 t2 ... tn (*|/|+|-|#) Number)
```


#### Parameters are similar to Rows, but apply to the data size in bytes

#### Examples


```sql
-- Multiply the expected data size by 1.5
Bytes(large_table * 1.5)

-- Replace the data size for the join with 1GB
Bytes(table1 table2 # 1073741824)

-- Reduce the expected size by a factor of 2
Bytes(compressed_table / 2)

-- Add 100MB to the expected size
Bytes(temp_table + 104857600)
```


### 4. JoinOrder — join order

Allows you to fix a specific join subtree in the overall join tree.

#### Syntax


```text
JoinOrder((t1 t2) (t3 (t4 ...)))
```


#### Parameters

- The nested structure of parentheses determines the join order
- `(t1 t2)` means that t1 and t2 must be joined first
- You can set an arbitrary nesting depth

#### How it works

The optimizer will only consider plans that contain the specified partial or full join order.

#### Examples


```sql
-- Force join users with orders first, then with products
JoinOrder((users orders) products)

-- More complex join order
JoinOrder(((customers orders) products) shipping)

-- Grouping of joins
JoinOrder((table1 table2) (table3 table4))

-- Multi-level structure
JoinOrder((users (orders products)) (addresses phones))
```


Apply the join order hint to the following query:


```sql
SELECT * FROM
    R   INNER JOIN  S   on  R.id = S.id
        INNER JOIN  T   on  R.id = T.id;
```


The query plan without hints looks like this:


```text
┌────────┬────────┬────────┬───────────────────────────────────────────────────────────────────────────────┐
│ E-Cost │ E-Rows │ E-Size │ Operation                                                                     │
├────────┼────────┼────────┼───────────────────────────────────────────────────────────────────────────────┤
│        │        │        │ ┌> ResultSet                                                                  │
│ 114    │ 10     │ 300    │ └─┬> InnerJoin (MapJoin) (S.id = R.id)                                        │
│ 57     │ 10     │ 200    │   ├─┬> InnerJoin (MapJoin) (S.id = T.id)                                      │
│ 0      │ 10     │ 100    │   │ ├──> TableFullScan (Table: S, ReadColumns: ["id (-∞, +∞)","payload2"])    │
│ 0      │ 10     │ 100    │   │ └──> TableFullScan (Table: T, ReadColumns: ["id (-∞, +∞)","payload3"])    │
│ 0      │ 10     │ 100    │   └──> TableFullScan (Table: R, ReadColumns: ["id (-∞, +∞)","payload1","ts"]) │
└────────┴────────┴────────┴───────────────────────────────────────────────────────────────────────────────┘
```


Applying the following join order hint:


```sql
PRAGMA ydb.OptimizerHints =
'
    JoinOrder(T (R S))
';
SELECT * FROM
    R   INNER JOIN  S   on  R.id = S.id
        INNER JOIN  T   on  R.id = T.id;
```


We get the following plan:


```text
┌────────┬────────┬────────┬─────────────────────────────────────────────────────────────────────────────────┐
│ E-Cost │ E-Rows │ E-Size │ Operation                                                                       │
├────────┼────────┼────────┼─────────────────────────────────────────────────────────────────────────────────┤
│        │        │        │ ┌> ResultSet                                                                    │
│ 114    │ 10     │ 300    │ └─┬> InnerJoin (MapJoin) (T.id = R.id)                                          │
│ 0      │ 10     │ 100    │   ├──> TableFullScan (Table: T, ReadColumns: ["id (-∞, +∞)","payload3"])        │
│ 57     │ 10     │ 200    │   └─┬> InnerJoin (MapJoin) (R.id = S.id)                                        │
│ 0      │ 10     │ 100    │     ├──> TableFullScan (Table: R, ReadColumns: ["id (-∞, +∞)","payload1","ts"]) │
│ 0      │ 10     │ 100    │     └──> TableFullScan (Table: S, ReadColumns: ["id (-∞, +∞)","payload2"])      │
└────────┴────────┴────────┴─────────────────────────────────────────────────────────────────────────────────┘
```


Here you can see that the join order has changed to the one specified in the hint.

## Combining hints

You can use multiple types of hints simultaneously within a single pragma:


```sql
PRAGMA ydb.OptimizerHints =
'
    JoinType(users orders Broadcast)
    Rows(users orders * 0.5)
    JoinOrder((users orders) products)
    Bytes(products # 1073741824)
';
```
