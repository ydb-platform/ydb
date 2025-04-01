## Designing YDB: Constructing a Distributed cloud-native DBMS for OLTP and OLAP from the Ground Up {#2025-conf-fosdem}

{% include notitle [database_internals_tag](../../tags.md#database_internals) %}

Distributed systems are great in multiple aspects: they are built to be fault-tolerant and reliable, can scale almost infinitely, provide low latency in geo-distributed scenarios, and, finally, they are geeky and fun to explore. YDB is a distributed SQL database that has been running in production for years. There are installations with thousands of servers storing petabytes of data. To provide these capabilities, any distributed DBMS must achieve consistency and consensus while tolerating unreliable networks, faulty hardware, and the absence of a global clock.

In this session, we will briefly introduce the problems, challenges, and fallacies of distributed computing, explaining why sharded systems like Citus are not always ACID and differ from truly distributed systems. Then, we will dive deep into the design decisions made by YDB to address these difficulties and outline YDB's architecture layer by layer, from the bare metal disks and distributed storage up to OLTP and OLAP functionalities. Ultimately, we will briefly compare our approach with Calvin's, which initially inspired YDB, and Spanner.

[{{ team.ivanov.name }}]({{ team.ivanov.profile }}) ({{ team.ivanov.position }}) discussed the architecture of YDB, focusing on building a unified platform for fault-tolerant and reliable OLTP and OLAP processing.

@[YouTube](https://youtu.be/kfI0r5OvYIk?si=ZVyS2OTtJxl3ZuWj)

The presentation will be of interest to developers of high-load systems and platform developers for various purposes.

[Slides](https://presentations.ydb.tech/2025/en/fossasia/designing_ydb/presentation.pdf)
