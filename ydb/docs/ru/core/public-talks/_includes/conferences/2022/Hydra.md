### Интервью с Андреем Фомичевым {#2022-conf-hydra-interview}

{% include notitle [overview_tag](../../tags.md#overview) %}

[ {{ team.fomichev.name }} ]( {{ team.fomichev.profile }} ) ( {{ team.fomichev.position }} ) рассказал о том, как он пришел в разработку баз данных, с чего начиналась {{ ydb-short-name }}, что предшествовало её появлению.

@[YouTube](https://www.youtube.com/watch?v=TYvgLWDtTFY)

### Parallel Asynchronous Replication between YDB Database Instances (EN) {#2022-conf-hydra-parallel-async-rep}

{% include notitle [database_internals_tag](../../tags.md#database_internals) %}

In this talk we present an approach to asynchronous replication in {{ ydb-short-name }} that provides the following characteristics: changefeed from source database is sharded among multiple persistent queues, sharded changefeed is applied to the target database in a manner to guarantee target database consistency.

@[YouTube](https://www.youtube.com/watch?v=Ga2Eg2rbPPc)

[Slides](https://squidex.jugru.team/api/assets/srm/acbeabc7-56f1-4234-9e97-0e66c33be4ce/hydra-2022-fomichev-nizametdinov-1-.pdf)