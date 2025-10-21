# Optimizer Hints

Optimizer hints allow you to influence the behavior of the cost-based optimizer when planning the execution of SQL queries. {{ydb-short-name}} supports four types of hints for managing joins and statistics.

## Usage

Hints are specified via the `PRAGMA ydb.OptimizerHints` pragma at the beginning of the SQL query.
If the optimizer was unable to apply at least one of the specified hints to the query, the user will be notified via a warning.

## Syntax

Hints are specified as a string containing an array of expressions of one of four types:

```text
JoinType(TableList JoinType) 
Rows(TableList Op Value) 
Bytes(TableList Op Value) 
JoinOrder(JoinTree)

where:
TableList - list of table names or aliases from the query
Op - operation:
- `#` - set absolute value
- `*` - multiply by value
- `/` - divide by value
- `+` - add value
- `-` - subtract value
- `Number` - absolute numeric value
Value - numeric value
JoinTree - representation of binary tree using parentheses, for example: (R S) (T U)
```

For example, the following query uses 3 cardinality (number of records) hints `Rows`, a total join order hint `JoinOrder`, and a join algorithm selection hint `JoinType`:

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

All hints (`Rows`, `Bytes`, `JoinOrder`) work only with **enabled** [cost based optimizer](../concepts/optimizer.md), except `JoinType` - it can be specified even if CBO is disabled.

{% endnote %}

## Hint types

### JoinType - Join algorithm

Allows you to force the join algorithm for specific tables.

There are currently three types of join algorithms supported in {{ ydb-short-name }}:

- BroadcastJoin - is a join type where one of the data sets is small enough to be copied (broadcasted) to all required nodes in the cluster. This allows each node to perform the join locally, without transmitting the data over the network.

{% note info %}

If the join order is not fixed by a separate hint, the optimizer will build both variants of the plan: where the left and right input of the join is broadcasted. If the join order is fixed by a hint, the right side of the join will be broadcasted.

{% endnote %}

- ShuffleJoin is a type of join in which the data is divided (shuffled) by the join key so that records with the same key are processed on the same processing node. After such a redistribution of data, each node performs a local join of the tables. The results are combined into one common data set.
- LookupJoin - for each row of one input, a query is made to the table or index of the other input, currently supported only for [row tables](../concepts/datamodel/table.md#row-oriented-table).

{% note info %}

If the join order is not fixed by a separate hint, the optimizer will build both variants of the plan: where queries are made to the left and right inputs of the join. If the join order is fixed by a hint, queries will be made to the right side of the join.

{% endnote %}

#### Syntax

```text
JoinType(t1 t2 ... tn Broadcast | Shuffle | Lookup)
```

#### Parameters

- `t1 t2 ... tn` - tables involved in the join
- Algorithm:
- `Broadcast` - select the BroadcastJoin algorithm
- `Shuffle` - select the ShuffleJoin algorithm
- `Lookup` - select the LookupJoin algorithm

#### How it works

If the query plan contains a join operator that joins only those tables listed in the hint, the optimizer will choose the specified join algorithm if it is applicable (for example, you cannot apply the LookupJoin algorithm to [column-oriented tables](../concepts/datamodel/table.md#column-oriented-table)). If the algorithm cannot be applied, the user will be notified via a warning.

#### Examples

```sql
-- Use Broadcast to join tables nation, region
JoinType(nation region Broadcast)

-- Use HashJoin for a join whose subtree will contain only tables customers, orders, products
JoinType(customers orders products Shuffle)

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
R INNER JOIN S on R.id = S.id
INNER JOIN T on R.id = T.id
INNER JOIN U on T.id = U.id
INNER JOIN V on U.id = V.id;
```

You can view the query plan using the [CLI](../reference/ydb-cli/commands/explain-plan.md) command:

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

Since the optimizer may change the join order during query optimization, the join hint should reflect the exact list of tables that are being joined.
For example, in this query, the join order is assumed to be R with S, then T, and finally U. Specifying a different join algorithm may change the join order in the plan, and some join hints will not be applied. In this case, you can add an additional join order hint.

### 2. Rows - Cardinality Hints

Allows you to change the expected number of rows (optimizer estimate) for a join result or individual tables.

#### How it works

The optimizer will change its estimate of the cardinality (number of rows) for a join operation that joins only listed tables.

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
-- Multiply expected row count by 2 for join in subtree which contains only tables users orders yandex
Rows(users orders yandex * 2.0)

-- Replace cardinality of table products by 1.3e6
Rows(products # 1.3e6)

-- Reduce expected row count by 228 times
Rows(filtered_table / 228)

-- Add 5000 rows to the expected result
Rows(table1 table2 + 5000)
```

Let's run the query without cardinality hints, and then see how the hints change the query plan.

```sql
SELECT * FROM
R INNER JOIN S on R.id = S.id
INNER JOIN T on R.id = T.id;
```

Without hints, the optimizer builds the following plan:

```text
┌────────┬────────┬────────┬───────────────────────────────────────────────────────────────────────────────┐
│ E-Cost │ E-Rows │ E-Size │ Operation                                                                     │
├────────┼────────┼────────┼───────────────────────────────────────────────────────────────────────────────┤
|        │        │        │ ┌> ResultSet                                                                  │
│ 114    │ 10     │ 300    │ └─┬> InnerJoin (MapJoin) (S.id = R.id)                                        │
│ 57     │ 10     │ 200    │   ├─┬> InnerJoin (MapJoin) (S.id = T.id)                                      │
│ 0      │ 10     │ 100    │   │ ├──> TableFullScan (Table: S, ReadColumns: ["id (-∞, +∞)","payload2"])    │
│ 0      │ 10     │ 100    │   │ └──> TableFullScan (Table: T, ReadColumns: ["id (-∞, +∞)","payload3"])    │
│ 0      │ 10     │ 100    │   └──> TableFullScan (Table: R, ReadColumns: ["id (-∞, +∞)","payload1","ts"]) │
└────────┴────────┴────────┴───────────────────────────────────────────────────────────────────────────────┘
```

If we specify the following hints:

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

The resulting plan will look like:

```text
┌───────────┬────────┬────────┬─────────────────────────────────────────────────────────────────────────────────┐
│ E-Cost    │ E-Rows │ E-Size │ Operation                                                                       │
├───────────┼────────┼────────┼─────────────────────────────────────────────────────────────────────────────────┤
│           │        │        │ ┌> ResultSet                                                                    │
| 3.000e+09 │ 1      │ 100    │ └─┬> InnerJoin (MapJoin) (S.id = R.id)                                          │
│ 0         │ 1e+09  │ 100    │   ├──> TableFullScan (Table: S, ReadColumns: ["id (-∞, +∞)","payload2"])        │
│ 1.500e+09 │ 1      │ 100    │   └─┬> InnerJoin (MapJoin) (R.id = T.id)                                        │
│ 0         │ 1e+09  │ 100    │     ├──> TableFullScan (Table: R, ReadColumns: ["id (-∞, +∞)","payload1","ts"]) │
│ 0         │ 10     │ 100    │     └──> TableFullScan (Table: T, ReadColumns: ["id (-∞, +∞)","payload3"])      │
└───────────┴────────┴────────┴─────────────────────────────────────────────────────────────────────────────────┘
```

Also a warning will be issued:

```text
Warning: Unapplied hint: Rows(R S # 10e8)
```

Here you can see that after applying the cardinality hints of the base tables, the order of the joins changed, and one of the hints could not be applied, since there is no such join in the plan.

### 3. Bytes - Data Size Hints

Allows you to change the expected data size in bytes for a join or individual tables.

#### Syntax

```text
Bytes(t1 t2 ... tn (*|/|+|-|#) Number)
```

#### Parameters are similar to Rows, but apply to the size of the data in bytes

#### Examples

```sql
-- Multiply the expected data size by 1.5
Bytes(large_table * 1.5)

-- Replace the size of the data for the connection with 1GB
Bytes(table1 table2 # 1073741824)

-- Reduce the expected size by 2
Bytes(compressed_table / 2)

-- Add 100MB to the expected size
Bytes(temp_table + 104857600)
```

### 4. JoinOrder - Join order

Allows you to fix a specific subtree of joins in the overall join tree.

#### Syntax

```text
JoinOrder((t1 t2) (t3 (t4 ...)))
```

#### Parameters

- Nested parentheses structure defines the join order
- `(t1 t2)` means that t1 and t2 must be joined first
- You can set any nesting depth

#### How it works

The optimizer will only consider plans that have the specified partial or full join order.

#### Examples

```sql
-- Force users to join orders first, then products
JoinOrder((users orders) products)

-- More complex join order
JoinOrder(((customers orders) products) shipping)

-- Join grouping
JoinOrder((table1 table2) (table3 table4))

-- Multi-level structure
JoinOrder((users (orders products)) (addresses phones))
```

Let's apply the join order hint to the following query:

```sql
SELECT * FROM
R INNER JOIN S on R.id = S.id
INNER JOIN T on R.id = T.id;
```

The query plan without specifying hints is displayed as follows:

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

Here you can see that the order of connections has changed to the one specified in the hint.

## Combining hints

You can use several types of hints simultaneously within one pragma:

```sql
PRAGMA ydb.OptimizerHints =
'
    JoinType(users orders Broadcast)
    Rows(users orders * 0.5)
    JoinOrder((users orders) products)
    Bytes(products # 1073741824)
';
```