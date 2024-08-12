### YDB vs. TPC-C: the Good, the Bad, and the Ugly behind High-Performance Benchmarking {#2024-conf-techinternals-highload}

{% include notitle [testing-tag](../../tags.md#database_internals) %}

Modern distributed databases scale horizontally with great efficiency, making them almost limitless in capacity. This implies that benchmarks should be able to run on multiple machines and be very efficient to minimize the number of machines required. This talk will focus on benchmarking high-performance databases, particularly emphasizing YDB and our implementation of the TPC-C benchmark, the de facto gold standard in the database field.

First, we will speak about benchmarking strategies from a user's perspective. We will dive into key details related to benchmark implementations, which could be useful when you create a custom benchmark to mirror your production scenarios. Throughout our performance journey, we have identified numerous anti-patterns: there are things you should unequivocally avoid in your benchmark implementations. We'll highlight these "bad" and "ugly" practices with illustrative examples.

Next, we'll briefly discuss the popular key-value benchmark YCSB, which is a prerequisite for robust performance in distributed transactions. We'll then explore the TPC-C benchmark in greater detail, sharing valuable insights derived from our own implementation.

We'll conclude our talk by presenting performance results from the TPC-C benchmark, comparing YDB and CockroachDB with PostgreSQL to illustrate situations where PostgreSQL might not be enough and when you might want to consider a distributed DBMS instead.

[{{ team.ivanov.name }}]({{ team.ivanov.profile }}) ({{ team.ivanov.position }}) discussed best high-performance benchmarking practices and some pitfalls found during TPC-C implementation, then demonstrated TPC-C results of PostgreSQL, CockroachDB, and YDB.

@[YouTube](https://youtu.be/LlqfqzPtLD0?si=YYDcXkUZFsLJRJEY)

The presentation will be of interest to developers of high-load systems and developers of platforms for various purposes.

[Slides](https://presentations.ydb.tech/2024/en/techinternals_cyprus/ydb_vs_tpcc/presentation.pdf)
