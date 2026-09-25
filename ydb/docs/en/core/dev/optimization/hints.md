# Optimizer Hints

Optimizer hints allow you to influence the behavior of the cost-based optimizer when planning the execution of SQL queries. {{ ydb-short-name }} supports four types of hints for managing joins and statistics.

## Usage

Hints are specified via the `PRAGMA ydb.OptimizerHints` pragma at the beginning of the SQL query.
If the optimizer fails to apply at least one of the specified hints to the query, the user will be notified via a warning.

## Syntax

Hints are specified as a string containing an array of expressions of one of four types:

```text
JoinType(TableList JoinType)
Rows(TableList Op Value)
Bytes(TableList Op Value)
JoinOrder(JoinTree)

where:
TableList - a list of table names or aliases from the query
Op - operation:
  - `#` - set an absolute value
  - `*` - multiply by a value
  - `/` - divide by a value
  - `+` - add a value
  - `-` - subtract a value
  - `Number` - numeric value
Value - numeric value
JoinTree - a representation of a binary tree using brackets, for example: (R S) (T U)
```

For example, the following query uses three hints `Rows` to specify [cardinality](../../concepts/glossary.md#cardinality), as well as a hint for the full join order `JoinOrder` and a hint for selecting the join algorithm `JoinType`:

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

All hints (`Rows`, `Bytes`, `JoinOrder`) work only with **enabled** [cost-based optimizer](../../concepts/query_execution/optimizer.md), except for `JoinType` — it can be specified even when CBO is disabled.

{% endnote %}

## Types of Hints

### 1. JoinType — join algorithm

Allows you to forcibly set the join algorithm for certain tables.

Currently, {{ ydb-short-name }} supports three types of join algorithms:

- BroadcastJoin is a type of join where one of the data sets is small enough to be copied (broadcast) to all necessary nodes in the cluster. This allows each node to perform the join locally without transferring data over the network.

{% note info %}

If the join order is not fixed by a separate hint, the optimizer will build both versions of the plans: where the left and right inputs of the join are transferred. If the join order is fixed by a hint, the right side of the join will be transferred.

{% endnote %}

- ShuffleJoin is a type of join where data is shuffled by the join key so that records with the same key are processed on the same processing node. After this data redistribution, each node performs a local join of the tables. The results are combined into a single data set.
- LookupJoin makes a request to the table or index of the other input for each row of one input; currently supported only for [string tables](../../concepts/datamodel/table.md#row-oriented-table).

#### Syntax

```text
JoinType(t1 t2 ... tn Broadcast | Shuffle | Lookup)
```

#### Parameters

- `t1 t2 ... tn` — tables involved in the join
- Algorithm:
  - `Broadcast` — select the BroadcastJoin algorithm
  - `Shuffle` — select the ShuffleJoin algorithm
  - `Lookup` — select the LookupJoin algorithm

#### Operating principle

If the query plan includes [a join operator](../../concepts/glossary.md#operator) that connects only the tables listed in the list, the optimizer will select the specified join algorithm if it is applicable (for example, the LookupJoin algorithm cannot be applied to [columnar tables](../../concepts/datamodel/table.md#column-oriented-table)). If the algorithm cannot be applied, the user will be notified via a warning.

#### Examples

```sql
-- Use Broadcast to join the nation, region tables
JoinType(nation region Broadcast)

-- Use ShuffleJoin for a join where the subtree will only contain the customers, orders, products tables
JoinType(customers orders products Shuffle)

-- Use LookupJoin to join the nation, region tables
JoinType(nation region Lookup)
```

Apply connection algorithm hints to the following query:

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

Since the optimizer can change the order of joins during query optimization, the hint should reflect the exact list of tables that are joined.
For example, this query assumes that the order of joins will be: R with S, then T, and finally U. Specifying a different join algorithm may change the order of joins in the plan, and some hints may not be applied. In this case, you can add an additional hint for the order of joins.

### 2. Rows — hints on [cardinality](../../concepts/glossary.md#cardinality)

Allows you to change the expected number of rows (optimizer estimate) for a join or individual tables.

#### How it works

The optimizer will change its estimate of the number of rows for the join operation that connects only the tables listed in the list.

#### Syntax

```text
Rows(t1 t2 ... tn (*|/|+|-|#) Number)
```

#### Parameters

- `t1 t2 ... tn` - tables
- Operation:
  - `*` - multiply by the value
  - `/` - divide by the value
  - `+` - add the value
  - `-` - subtract the value
  - `#` - replace the value
- `Number` - numerical value

#### Examples

```sql
-- Multiply the expected number of rows by 2 for a join where the subtree contains only the users, orders, and yandex tables
Rows(users orders yandex * 2.0)

-- Replace the expected number of rows for the products table with 1.3e6
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

If we apply the following hints:

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

The following plan will be obtained:

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

Alerts will also return:

```text
Warning: Unapplied hint: Rows(R S # 10e8)
```

Here you can see that after applying the hints [cardinality](../../concepts/glossary.md#cardinality) of the base tables, the order of joins has changed, and one of the hints could not be applied because there is no such join in the plan.

### 3. Bytes — data size hints

Allows you to change the expected data size in bytes for a connection or individual tables.

#### Syntax

```text
Bytes(t1 t2 ... tn (*|/|+|-|#) Number)
```

#### The parameters are similar to Rows, but they apply to the size of the data in bytes

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

Allows you to fix a certain subtree of joins in the overall join tree.

#### Syntax

```text
JoinOrder((t1 t2) (t3 (t4 ...)))
```

#### Parameters

- The nested bracket structure determines the order of joins
- `(t1 t2)` means that t1 and t2 should be joined first
- You can set an arbitrary nesting depth

#### Operating principle

The optimizer will only consider those plans that include a specified partial or complete join order.

#### Examples

```sql
-- Force joining users with orders first, then with products
JoinOrder((users orders) products)

-- More complex join order
JoinOrder(((customers orders) products) shipping)

-- Grouping joins
JoinOrder((table1 table2) (table3 table4))

-- Multi-level structure
JoinOrder((users (orders products)) (addresses phones))
```

Let's apply the join order hint to the following query:

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

By applying the following join order hint:

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