## Parallel asynchronous replication between YDB database instances {#2022-conf-hydra-parallel-async-rep}

{% include notitle [database_internals_tag](../../tags.md#database_internals) %}

In this talk, we present an approach to asynchronous replication in {{ ydb-short-name }} that provides the following characteristics: changefeed from the source database is sharded among multiple persistent queues, sharded changefeed is applied to the target database in a manner that guarantees the target database consistency.

@[YouTube](https://www.youtube.com/watch?v=Ga2Eg2rbPPc)

[Slides](https://presentations.ydb.tech/2022/en/hydra/presentation.pdf)

