# Optimizer Hints

Optimizer hints allow you to influence the behavior of the cost-based optimizer when planning the execution of SQL queries. {{ ydb-short-name }} supports four types of hints for managing joins and statistics.

## Usage

Hints are specified via the `PRAGMA ydb.OptimizerHints` pragma at the beginning of the SQL query. 
 If the optimizer is unable to apply at least one of the specified hints to the query, the user will be notified via a warning.

## Syntax

Hints are specified as a string containing an array of expressions of one of four types:

```text
JoinType(TableList JoinType)
Rows(TableList Op Value)
Bytes(TableList Op Value)
JoinOrder(JoinTree)

где:
TableList - перечисление названий таблиц или элиасов из запроса
Op - операция:
  - `#` - задать абсолютное значение
  - `*` - умножить на значение
  - `/` - разделить на значение
  - `+` - прибавить значение
  - `-` - вычесть значение
  - `Number` - числовое значение
Value - числовое значение
JoinTree - представление бинарного дерева с помощью скобок, например: (R S) (T U)
```

For example, the following query uses three hints `Rows` that specify [cardinality](../../concepts/glossary.md#cardinality), as well as a full join order hint `JoinOrder` and a join algorithm selection hint `JoinType`:

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

All hints (`Rows`, `Bytes`, `JoinOrder`) work only with the **enabled** [cost-based optimizer](../../concepts/query_execution/optimizer.md), except `JoinType` — it can be specified even when CBO is disabled.

{% endnote %}

## Types of Hints

### 1. JoinType — Join Algorithm

Allows you to forcefully set the join algorithm for certain tables.

{{ ydb-short-name }} currently supports three types of join algorithms:

- BroadcastJoin is a type of join where one of the datasets is small enough to be copied (broadcast) to all necessary nodes in the cluster. This allows each node to perform the join locally without transmitting data over the network.

{% note info %}

If the join order is not fixed by a separate hint, the optimizer will build both versions of the plans: where the left and right inputs of the join are sent. If the join order is fixed by a hint, the right side of the join will be sent.

{% endnote %}

- ShuffleJoin is a type of join where data is shuffled by the join key so that records with the same key are processed on the same processing node. After this data redistribution, each node performs a local join of the tables. The results are combined into a single dataset.
- LookupJoin — for each row of one input, a query is made to the table or index of the other input; currently supported only for [string tables](../../concepts/datamodel/table.md#row-oriented-table).

#### Syntax

```text
JoinType(t1 t2 ... tn Broadcast | Shuffle | Lookup)
```

#### Parameters

- `t1 t2 ... tn` — tables participating in the join
- Algorithm:
  - `Broadcast` — select the BroadcastJoin algorithm
  - `Shuffle` — select the ShuffleJoin algorithm
  - `Lookup` — select the LookupJoin algorithm

#### Principle of Operation

If the query plan includes a [join operator](../../concepts/glossary.md#operator) that joins only the tables listed in the list, the optimizer will select the specified join algorithm if it is applicable (for example, the LookupJoin algorithm cannot be applied to [columnar tables](../../concepts/datamodel/table.md#column-oriented-table)). If the algorithm cannot be applied, the user will be notified via a warning.

#### Examples

```sql
-- Использовать Broadcast для соединения таблиц nation, region
JoinType(nation region Broadcast)

-- Использовать ShuffleJoin для соединения, в поддереве которого будут только таблицы customers, orders, products
JoinType(customers orders products Shuffle)

-- Использовать LookupJoin для соединения таблиц nation, region
JoinType(nation region Lookup)
```

Let's apply join algorithm hints to the following query:

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

Since the optimizer may change the join order during query optimization, the hint should reflect the exact list of tables that are being joined. 
 For example, in this query, it is assumed that the join order will be: R with S, then T, and finally U. Specifying a different join algorithm may change the join order in the plan, and some hints may not be applied. In such a case, you can add an additional join order hint.

### 2. Rows — Cardinality Hints

Allows you to change the expected number of rows (optimizer estimate) for a join or individual tables.

#### Principle of Operation

The optimizer will change its estimate of the number of rows for the join operation that joins only the tables listed in the list.

#### Syntax

```text
Rows(t1 t2 ... tn (*|/|+|-|#) Number)
```

#### Parameters

- `t1 t2 ... tn` — tables
- Operation:
  - `*` — multiply by a value
  - `/` — divide by a value
  - `+` — add a value
  - `-` — subtract a value
  - `#` — replace the value
- `Number` — numeric value

#### Examples

```sql
-- Умножить ожидаемое количество строк на 2 для соединения, в поддереве которого есть только таблицы users orders yandex
Rows(users orders yandex * 2.0)

-- Заменить ожидаемое число строк таблицы products на 1.3e6
Rows(products # 1.3e6)

-- Уменьшить ожидаемое количество строк в 228 раз
Rows(filtered_table / 228)

-- Добавить 5000 строк к ожидаемому результату
Rows(table1 table2 + 5000)
```

Let's run a query without [cardinality](../../concepts/glossary.md#cardinality) hints and then see how the hints change the query plan.

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

We get the following plan:

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

Also, the following alerts will be returned:

```text
Warning: Unapplied hint: Rows(R S # 10e8)
```

Here we can see that after applying the [cardinality](../../concepts/glossary.md#cardinality) hints, the order of joins of the base tables changed, and one of the hints could not be applied because there is no such join in the plan.

### 3. Bytes — Data Size Hints

Allows you to change the expected data size in bytes for a join or individual tables.

#### Syntax

```text
Bytes(t1 t2 ... tn (*|/|+|-|#) Number)
```

#### Parameters are similar to Rows, but are applied to the data size in bytes

#### Examples

```sql
-- Умножить ожидаемый размер данных на 1.5
Bytes(large_table * 1.5)

-- Заменить размер данных для соединения на 1GB
Bytes(table1 table2 # 1073741824)

-- Уменьшить ожидаемый размер в 2 раза
Bytes(compressed_table / 2)

-- Добавить 100MB к ожидаемому размеру
Bytes(temp_table + 104857600)
```

### 4. JoinOrder — Join Order

Allows you to fix a certain subtree of joins in the overall join tree.

#### Syntax

```text
JoinOrder((t1 t2) (t3 (t4 ...)))
```

#### Parameters

- The nested structure of brackets defines the join order
- `(t1 t2)` means that t1 and t2 should be joined first
- You can specify an arbitrary depth of nesting

#### Principle of Operation

The optimizer will only consider plans that include the specified partial or full join order.

#### Examples

```sql
-- Принудительно соединить сначала users с orders, затем с products
JoinOrder((users orders) products)

-- Более сложный порядок соединений
JoinOrder(((customers orders) products) shipping)

-- Группировка соединений
JoinOrder((table1 table2) (table3 table4))

-- Многоуровневая структура
JoinOrder((users (orders products)) (addresses phones))
```

Let's apply a join order hint to the following query:

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

Here we can see that the join order has changed to the one specified in the hint.

## Combining Hints

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