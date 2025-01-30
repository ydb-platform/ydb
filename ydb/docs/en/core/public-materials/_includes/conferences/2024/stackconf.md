## An approach to unite tables and persistent queues in one system {#2024-stackconf}

<div class = "multi-tags-container">

{% include notitle [general_tag](../../tags.md#general) %}

{% include notitle [database_internals](../../tags.md#database_internals) %}

</div>

People need databases to store their data and persistent queues to transfer their data from one system to another. We’ve united tables and persisted queues within one data platform. Now you have a possibility to take your data from a queue, then process it and keep the result in a database within a single transaction. So your application developers don’t need to think about data inconsistency in cases of connection failures or other errors.

@[YouTube](https://youtu.be/LOpP47pNFGM?si=vAXyubijAA31QaTR)

[{{ team.kalinina.name }}]({{ team.kalinina.profile }}) ({{ team.kalinina.position }}) tell you about an open-source platform called YDB which allows you to work with tables and queues within a single transaction. Elena walk you through architecture decisions, possible scenarios, and performance aspects of this approach.

[Slides](https://presentations.ydb.tech/2024/en/stackconf/tables-and-queues/presentation.pdf)
