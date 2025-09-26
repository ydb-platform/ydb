# Query execution plan and AST

{{ ydb-short-name }} provides two types of query plans: a logical plan and an execution plan. The logical plan is better suited for analyzing complex queries with many join operators. The execution plan is more detailed: it additionally shows the stages of the distributed plan and the connectors between them, which makes it more convenient for analyzing simple OLTP queries.

## Query execution plans

You can display the execution plan via {{ ydb-short-name }} CLI with the `explain` command:

```bash
ydb -p <profile_name> table query explain \
-q "SELECT season_id, episode_id, title
FROM episodes
WHERE series_id = 1
AND season_id > 1
ORDER BY season_id, episode_id
LIMIT 3"
```

The returned execution plan is shown below:

```text
Query Plan:
ResultSet
└──Limit (Limit: 3)
└──<Merge>
└──TopSort (Limit: 3, TopSortBy: )
└──Filter (Predicate: Exist(item.season_id) And Exist(item.series_id))
└──TablePointLookup (ReadRange: [series_id (1), season_id (1, +∞), episode_id (-∞, +∞)], ReadColumns: [episode_id, season_id, title, series_id], Table: episodes)
Tables: [episodes]
```

In addition to the query execution plan, you can get an AST (abstract syntax tree).
The AST section contains a representation in the internal miniKQL language.

To display the AST, call `explain` with the `--ast` flag:

```bash
ydb -p <profile_name> table query explain \
-q "SELECT season_id, episode_id, title
FROM episodes
WHERE series_id = 1
AND season_id > 1
ORDER BY season_id, episode_id
LIMIT 3" --ast
```

The resulting AST:

```clojure
Query AST:
(
(let $1 (KqpTable '"episodes" '"72075186224045943:83859" '"" '1))
(let $2 '('"episode_id" '"season_id" '"title" '"series_id"))
(let $3 (Uint64 '1))
(let $4 (KqpRowsSourceSettings $1 $2 '() '((KqlKeyExc $3 $3) (KqlKeyInc $3))))
(let $5 (Uint64 '"3"))
(let $6 (DqPhyStage '((DqSource (DataSource '"KqpReadRangesSource") $4)) (lambda '($12) (block '(
  (let $13 (Bool 'true))
  (return (FromFlow (TopSort (OrderedMap (OrderedFilter (ToFlow $12) (lambda '($14) (And (Exists (Member $14 '"season_id")) (Exists (Member $14 '"series_id"))))) (lambda '($15) (AsStruct '('"episode_id" (Member $15 '"episode_id")) '('"season_id" (Member $15 '"season_id")) '('"title" (Member $15 '"title"))))) $5 '($13 $13) (lambda '($16) '((Member $16 '"season_id") (Member $16 '"episode_id"))))))
))) '('('"_logical_id" '842) '('"_id" '"14388682-e03b28dd-60f91f84-d7cd002f"))))
(let $7 (DqCnMerge (TDqOutput $6 '0) '('('"season_id" '"Asc") '('"episode_id" '"Asc"))))
(let $8 (DqPhyStage '($7) (lambda '($17) (FromFlow (Take (ToFlow $17) $5))) '('('"_logical_id" '855) '('"_id" '"295c0459-34d4b026-b467e71f-35402430"))))
(let $9 '('"season_id" '"episode_id" '"title"))
(let $10 (DqCnResult (TDqOutput $8 '0) $9))
(let $11 (OptionalType (DataType 'Uint64)))
(return (KqpPhysicalQuery '((KqpPhysicalTx '($6 $8) '($10) '() '('('"type" '"data")))) '((KqpTxResultBinding (ListType (StructType '('"episode_id" $11) '('"season_id" $11) '('"title" (OptionalType (DataType 'String))))) '0 '0)) '('('"type" '"data_query"))))
)
```

## Logical query plan

Let's consider the analytical query Q18 from the TPCH benchmark:

```yql
$p = (
  select
    p.p_brand as p_brand,
    p.p_type as p_type,
    p.p_size as p_size,
    ps.ps_suppkey as ps_suppkey
  from
    part as p
  join
    partsupp as ps
on
    p.p_partkey = ps.ps_partkey
where
    p.p_brand <> 'Brand#45'
    and p.p_type not like 'MEDIUM POLISHED%'
    and (p.p_size = 49 or p.p_size = 14 or p.p_size = 23 or p.p_size = 45 or p.p_size = 19 or p.p_size = 3 or p.p_size = 36 or p.p_size = 9)
);

$s = (
  select
    s_suppkey
  from
    supplier
  where
    s_comment like "%Customer%Complaints%"
);

$j = (
  select
    p.p_brand as p_brand,
    p.p_type as p_type,
    p.p_size as p_size,
    p.ps_suppkey as ps_suppkey
  from
    $p as p
  left only join
    $s as s
  on
    p.ps_suppkey = s.s_suppkey
);

select
    j.p_brand as p_brand,
    j.p_type as p_type,
    j.p_size as p_size,
    count(distinct j.ps_suppkey) as supplier_cnt
from
    $j as j
group by
    j.p_brand,
    j.p_type,
    j.p_size
order by
    supplier_cnt desc,
    p_brand,
    p_type,
    p_size
;
```

You can get the logical execution plan with the command:


```bash
{{ ydb-cli }} -p <profile_name> sql --explain -f q18.sql
```

This will result in a logical query plan consisting of the plan's operators and various optimizer predictions:

```text
Query Plan:
┌───────────┬──────────┬───────────┬───────────────────────────────────────────────────────┐
│ E-Cost    │ E-Rows   │ E-Size    │ Operation                                             │
├───────────┼──────────┼───────────┼───────────────────────────────────────────────────────┤
│           │          │           │ ┌> ResultSet                                          │
│           │          │           │ └─┬> Sort ([row.supplier_cnt,row.p_brand,row.p_type,r │
│           │          │           │ ow.p_size])                                           │
│           │          │           │   └─┬> Aggregate (Phase: Final)                       │
│           │          │           │     └─┬> HashShuffle (KeyColumns: ["part.p_brand","pa │
│           │          │           │ rt.p_size","part.p_type"], HashFunc: "HashV2")        │
│           │          │           │       └─┬> Aggregate (GroupBy: [part.p_brand,part.p_s │
│           │          │           │ ize,part.p_type], Aggregation: {Inc(_yql_agg_0)}, Pha │
│           │          │           │ se: Intermediate)                                     │
│           │          │           │         └─┬> Aggregate (Phase: Final)                 │
│           │          │           │           └─┬> HashShuffle (KeyColumns: ["part.p_bran │
│           │          │           │ d","part.p_size","part.p_type","partsupp.ps_suppkey"] │
│           │          │           │ , HashFunc: "HashV2")                                 │
│           │          │           │             └─┬> Aggregate (GroupBy: [part.p_brand,pa │
│           │          │           │ rt.p_size,part.p_type,partsupp.ps_suppkey], Aggregati │
│           │          │           │ on: state, Phase: Intermediate)                       │
│ 4.350e+08 │ 40000000 │ 3.421e+09 │               └─┬> LeftOnlyJoin (Grace) (partsupp.ps_ │
│           │          │           │ suppkey = supplier.s_suppkey)                         │
│           │          │           │                 ├─┬> HashShuffle (KeyColumns: ["parts │
│           │          │           │ upp.ps_suppkey"], HashFunc: "ColumnShardHashV1")      │
│ 3.135e+08 │ 40000000 │ 2.331e+09 │                 │ └─┬> InnerJoin (Grace) (part.p_part │
│           │          │           │ key = partsupp.ps_partkey)                            │
│ 0         │ 9000000  │ 2.869e+08 │                 │   ├─┬> Filter (Contains)            │
│ 0         │ 9000000  │ 2.869e+08 │                 │   │ └─┬> Filter ((p_brand != "Brand │
│           │          │           │ #33") AND (NOT p_type LIKE "PROMO POLISHED%"), Pushdo │
│           │          │           │ wn: True)                                             │
│ 0         │ 20000000 │ 6.375e+08 │                 │   │   └──> TableFullScan (Table: pa │
│           │          │           │ rt, ReadColumns: ["p_partkey (-∞, +∞)","p_brand","p_s │
│           │          │           │ ize","p_type"])                                       │
│           │          │           │                 │   └─┬> HashShuffle (KeyColumns: ["p │
│           │          │           │ s_partkey"], HashFunc: "ColumnShardHashV1")           │
│ 0         │ 80000000 │ 2.113e+09 │                 │     └──> TableFullScan (Table: part │
│           │          │           │ supp, ReadColumns: ["ps_partkey (-∞, +∞)","ps_suppkey │
│           │          │           │  (-∞, +∞)"])                                          │
│ 0         │ 500000   │ 13621431  │                 └─┬> Filter (Apply, Pushdown: True)   │
│ 0         │ 1000000  │ 27242862  │                   └──> TableFullScan (Table: supplier │
│           │          │           │ , ReadColumns: ["s_suppkey (-∞, +∞)","s_comment"])    │
└───────────┴──────────┴───────────┴───────────────────────────────────────────────────────┘
```

This plan shows the operator tree {{ ydb-short-name }} and three optimizer predictions for each operator:

- `E-Cost`: estimated cost of the current operator and all its inputs;
- `E-Rows`: estimated number of records at the output of the current operator;
- `E-Size`: estimated size of the operator's result in bytes.

In addition to optimizer predictions, you can also get actual statistics ​​when executing the query:

- `A-Cpu`: total CPU time for the current operator;
- `A-Rows`: the actual number of records in the output of the current operator.

Execution statistics are not always available for each operator, as they are currently collected by execution stages. Several operators can be placed into a single stage.

To get a logical query plan with the execution statistics, run the following command:

```bash
{{ ydb-cli }} -p <profile_name> sql --explain-analyze --format pretty-table --analyze -f q18.sql
```

You will get the following logical query plan:

```text
┌───────┬──────────┬───────────┬──────────┬───────────┬──────────────────────────────────────────────────────────────────┐
│ A-Cpu │ A-Rows   │ E-Cost    │ E-Rows   │ E-Size    │ Operation                                                        │
├───────┼──────────┼───────────┼──────────┼───────────┼──────────────────────────────────────────────────────────────────┤
│       │          │           │          │           │ ┌> ResultSet                                                     │
│ 934   │ 27840    │           │          │           │ └─┬> Sort (A-SelfCpu: 15.706, A-Size: 936768, [row.supplier_cnt, │
│       │          │           │          │           │ row.p_brand,row.p_type,row.p_size])                              │
│       │ 27840    │           │          │           │   └─┬> Aggregate (Phase: Final)                                  │
│       │          │           │          │           │     └─┬> HashShuffle (KeyColumns: ["part.p_brand","part.p_size", │
│       │          │           │          │           │ "part.p_type"], HashFunc: "HashV2")                              │
│ 918   │ 3431671  │           │          │           │       └─┬> Aggregate (GroupBy: [part.p_brand,part.p_size,part.p_ │
│       │          │           │          │           │ type], Aggregation: {Inc(_yql_agg_0)}, A-SelfCpu: 139.139, Phase │
│       │          │           │          │           │ : Intermediate, A-Size: 112058009)                               │
│       │          │           │          │           │         └─┬> Aggregate (Phase: Final)                            │
│       │          │           │          │           │           └─┬> HashShuffle (KeyColumns: ["part.p_brand","part.p_ │
│       │          │           │          │           │ size","part.p_type","partsupp.ps_suppkey"], HashFunc: "HashV2")  │
│ 779   │ 11865156 │           │          │           │             └─┬> Aggregate (GroupBy: [part.p_brand,part.p_size,p │
│       │          │           │          │           │ art.p_type,partsupp.ps_suppkey], Aggregation: state, A-SelfCpu:  │
│       │          │           │          │           │ 194.29, Phase: Intermediate, A-Size: 410786277)                  │
│       │ 11867631 │ 4.350e+08 │ 40000000 │ 3.421e+09 │               └─┬> LeftOnlyJoin (Grace) (partsupp.ps_suppkey = s │
│       │          │           │          │           │ upplier.s_suppkey)                                               │
│       │          │           │          │           │                 ├─┬> HashShuffle (KeyColumns: ["partsupp.ps_supp │
│       │          │           │          │           │ key"], HashFunc: "ColumnShardHashV1")                            │
│ 584   │ 11873200 │ 3.135e+08 │ 40000000 │ 2.331e+09 │                 │ └─┬> InnerJoin (Grace) (A-SelfCpu: 308.14, par │
│       │          │           │          │           │ t.p_partkey = partsupp.ps_partkey, A-Size: 505055811)            │
│ 65    │ 2968300  │ 0         │ 9000000  │ 2.869e+08 │                 │   ├─┬> Filter (Contains, A-SelfCpu: 65.311, A- │
│       │          │           │          │           │ Size: 105580755)                                                 │
│       │          │ 0         │ 9000000  │ 2.869e+08 │                 │   │ └─┬> Filter ((p_brand != "Brand#33") AND ( │
│       │          │           │          │           │ NOT p_type LIKE "PROMO POLISHED%"), Pushdown: True)              │
│       │ 18560313 │ 0         │ 20000000 │ 6.375e+08 │                 │   │   └──> TableFullScan (Table: part, ReadCol │
│       │          │           │          │           │ umns: ["p_partkey (-∞, +∞)","p_brand","p_size","p_type"], A-Size │
│       │          │           │          │           │ : 1060176372)                                                    │
│       │          │           │          │           │                 │   └─┬> HashShuffle (KeyColumns: ["ps_partkey"] │
│       │          │           │          │           │ , HashFunc: "ColumnShardHashV1")                                 │
│ 211   │ 80000000 │ 0         │ 80000000 │ 2.113e+09 │                 │     └──> TableFullScan (Table: partsupp, A-Sel │
│       │          │           │          │           │ fCpu: 210.822, ReadColumns: ["ps_partkey (-∞, +∞)","ps_suppkey ( │
│       │          │           │          │           │ -∞, +∞)"], A-Size: 1283276800)                                   │
│ 0     │ 479      │ 0         │ 500000   │ 13621431  │                 └─┬> Filter (Apply, A-SelfCpu: 0.27, Pushdown: T │
│       │          │           │          │           │ rue, A-Size: 4937)                                               │
│       │ 479      │ 0         │ 1000000  │ 27242862  │                   └──> TableFullScan (Table: supplier, ReadColum │
│       │          │           │          │           │ ns: ["s_suppkey (-∞, +∞)","s_comment"], A-Size: 7885)            │
└───────┴──────────┴───────────┴──────────┴───────────┴──────────────────────────────────────────────────────────────────┘
```

This plan contains actual execution statistics - `A-Cpu` and `A-Rows` in addition to optimizer estimates.