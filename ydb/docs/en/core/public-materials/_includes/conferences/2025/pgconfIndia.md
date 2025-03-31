## Sharded and Distributed Are Not the Same: What You Must Know When PostgreSQL Is Not Enough {#2025-conf-pgconf-India}

{% include notitle [testing_tag](../../tags.md#testing) %}

It's not a secret that PostgreSQL is extremely efficient and scales vertically well. At the same time, it’s not a secret that PostgreSQL scales only vertically, thus its performance is limited by the capabilities of a single server. Most Citus-like solutions allow to shard the database, but a sharded database is not distributed and does not provide ACID guarantees for distributed transactions. The common opinion about distributed DBMSs is diametrically opposed: they are believed to scale well horizontally, have ACID distributed transactions, but lower efficiency in smaller installations.

When comparing monolithic and distributed DBMSs, discussions often focus on architecture but rarely provide specific performance metrics. This presentation, on the other hand, is entirely based on an empirical study of this issue. Our approach is simple: [{{ team.ivanov.name }}]({{ team.ivanov.profile }}) ({{ team.ivanov.position }}) installed PostgreSQL and distributed DBMSs on identical clusters of three physical servers and compared them using the popular TPC-C benchmark.

@[YouTube](https://youtu.be/HR-vUI8mTVI?si=oenZT8mTr6czcZtS)

The presentation will be of interest to developers of high-load systems and platform developers for various purposes.

[Slides](https://presentations.ydb.tech/2025/en/pgconfin2025/sharded_and_distributed_are_not_the_same/presentation.pdf)
