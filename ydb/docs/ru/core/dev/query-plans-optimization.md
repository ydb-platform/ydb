# Использование планов при оптимизации запросов

При написании запроса важно проанализировать план его выполнения для своевременного обнаружения и устранения причин возможного чрезмерного потребления ресурсов кластера или аномально высокого времени выполнения. {{ydb-short-name}} предоставляет два типа планов запроса: логический план запроса и план исполнения. Логический план запроса больше подходит для анализа сложных запросов с большим количеством операторов join. План исполнения более детальный и в дополнение показывает стадии распределенного плана запроса и коннекторы меджу стадиями и больше подходит для анализа более простых OLTP запросов.

В данной статье мы рассмотрим два примера анализа планов запросов: аналитический запрос из бенчмарка TPCH и простой OLTP запрос.

## Логический план запроса

Рассмотрим аналитический запрос Q18 из бенчмарка TPCH:

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

Получить логический план выполнения можно командой:
```bash
  ydb -p <profile_name> sql --explain -f q18.sql
```
В результате получится логический план запроса, состоящий из операторов плана и различных предсказаний оптимизатора:
```
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
В этом плане показано дерево операторов YDB и выведены 3 предсказания оптмизатора для каждого оператора:
- E-Cost: оценка стоимости текущего оператора и всех его входов
- E-Rows: оценка количества записей на выходе текущего оператора
- E-Size: оценка количества байта результата оператора

Кроме предсказаний оптимизатора, также можно получить и реальные значения при выполнении запроса:
- A-Cpu: общее время CPU для текущего оператора
- A-Rows: настоящее количество записей на выходе текущего оператора

Статистика выполнения не всегда присутствует для каждого оператора, так как в текущий момент собирается по стадиям выполнения. В одной стадии может вычисляться сразу несколько операторов.

Чтобы получить логический план запроса со статистикой выполнения, можно запустить следующую команду:
```bash
  ydb -p <profile_name> sql --explain-analyze --format pretty-table --analyze -f q18.sql
```

Получится следующий логический план запроса:
```
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

В таком плане можно сопоставлять предсказания оптимизатора запросов со статистикой выполнения. Если предсказания значительно отличаются от того, что получилось на этапе исполнения, это может быть сигналом, что оптимизатор построил не самый эффективный план для текущего запроса. В таком случае, можно с помощью [подсказок оптимизатора](query-hints.md) построить более эффективный план. 

Самая надежная тактика исправлять неэффективный план из-за ошибки предсказания оптимизатора - это использовать хинт кардинальности (то есть количества записей в результате оператора). Можно ввести абсолютное значение, если нет ожиданий, что данные будут меняться в будущем. В противном случае, можно использовать относительные значения. 

## План исполнения запроса
Для иллюстрации работы с планом исполнения рассмотрим следующий OLTP запрос, выполняющий поиск серии по названию:

```yql
SELECT season_id, episode_id
  FROM episodes
  WHERE title = 'The Work Outing'
```

Схема строковой таблицы `episodes`:

![episodes](../_assets/episodes_scheme.png)

Построим план исполнения для данного запроса, в {{ ydb-short-name }} это можно сделать двумя способами:

{% list tabs group=tool %}

- {{ ydb-short-name }} CLI

  Получить план запроса через {{ ydb-short-name }} [CLI](../reference/ydb-cli/commands/explain-plan.md) можно с помощью следующей команды:

  ```bash
  ydb -p <profile_name> table query explain \
    -q "SELECT season_id, episode_id
    FROM episodes
    WHERE title = 'The Work Outing'"
  ```

  Результат:

  ```text
  Query Plan:
  ResultSet
  └──Limit (Limit: 1001)
     └──<UnionAll>
        └──Limit (Limit: 1001)
        └──Filter (Predicate: item.title == "The Work Outing")
        └──TableFullScan (ReadRanges: ["series_id (-∞, +∞)","season_id (-∞, +∞)","episode_id (-∞, +∞)"], ReadColumns: ["episode_id","season_id","title"], Table: episodes)
           Tables: ["episodes"]
  ```

- Embedded UI

  План запроса также можно получить через [Embedded UI](../reference/embedded-ui/ydb-monitoring.md). Для этого необходимо перейти на страницу базы данных в раздел `Query` и нажать `Explain`:

  ![explain_ui](../_assets/explain_ui.png)

  Результат:

  ![query_plan_ui](../_assets/query_plan_ui.png)

{% endlist %}

И в визуальном и в текстовом представлении видно, что в корне этого плана возвращение данных на клиент, в листьях работа со строковыми таблицами, а на промежуточных узлах — преобразования данных. Важно обратить внимание на узел, показывающий обращение к строковой таблице `episodes`. В данном случае это  `TableFullScan`, который означает выполнение полного сканирования строковой таблицы. А полное сканирование таблицы потребляет времени и ресурсов пропорционально её размеру, из-за чего по возможности их стараются избегать в таблицах, которые имеют тенденцию расти с течением времени или просто большие.

Одним из типовых способов избежать полного сканирования строковой таблицы является добавление [вторичного индекса](secondary-indexes.md). В данном случае имеет смысл добавить вторичный индекс для колонки `title`, для этого воспользуемся запросом:

```yql
ALTER TABLE episodes
  ADD INDEX title_index GLOBAL ON (title)
```

Стоит отметить, что в данном примере мы используем [синхронный вторичный индекс](../concepts/secondary_indexes.md#sync). Создание индекса в {{ ydb-short-name }} — асинхронная операция, поэтому даже если запрос на построение завершился успехом, стоит подождать какое-то время, так как фактически индекс может быть еще не готов к использованию. Управлять асинхронной операцией можно через [CLI](../reference/ydb-cli/commands/secondary_index.md#add).

Построим план того же запроса с использованием вторичного индекса `title_index`. Обратите внимание, что вторичный индекс надо явно указать в запросе через конструкцию `VIEW`.

{% list tabs group=tool %}

- {{ ydb-short-name }} CLI

  Команда:

  ```bash
  ydb -p <profile_name> table query explain \
    -q "SELECT season_id, episode_id
    FROM episodes VIEW title_index
    WHERE title = 'The Work Outing'"
  ```

  Результат:

  ```text
  Query Plan:
  ResultSet
  └──Limit (Limit: 1001)
     └──<UnionAll>
        └──Limit (Limit: 1001)
        └──Filter (Predicate: Exist(item.title))
        └──TablePointLookup (ReadRange: ["title (The Work Outing)","series_id (-∞, +∞)","season_id (-∞, +∞)","episode_id (-∞, +∞)"], ReadLimit: 1001, ReadColumns: ["episode_id","season_id","title"], Table: episodes/title_index/indexImplTable)
           Tables: ["episodes/title_index/indexImplTable"]
  ```

- Embedded UI

  ![explain_ui](../_assets/explain_with_index_ui.png)

  Результат:

  ![query_plan_ui](../_assets/query_plan_with_index_ui.png)

{% endlist %}

С использованием вторичного индекса запрос выполняется без полного сканирования основной таблицы. Вместо `TableFullScan` появился `TablePointLookup` - чтение индексной таблицы по ключу, а основную таблицу мы теперь совсем не читаем, так как все нужные нам колонки содержатся в индексной таблице.
