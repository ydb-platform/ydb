## Designing YDB: Constructing a Distributed cloud-native DBMS for OLTP and OLAP from the Ground Up {#2023-conf-hl-serbia-scale}

{% include notitle [database_internals_tag](../../tags.md#database_internals) %}

Distributed systems offer multiple advantages: they are built to be fault-tolerant and reliable, can scale almost infinitely, provide low latency in geo-distributed scenarios, and, finally, are geeky and fun to explore. YDB is an open-source distributed SQL database that has been running in production for years. Some installations include thousands of servers storing petabytes of data. To provide these capabilities, any distributed DBMS must achieve consistency and consensus while tolerating unreliable networks, faulty hardware, and the absence of a global clock.

In this session, we will provide a gentle introduction to the problems, challenges, and fallacies of distributed computing, explaining why sharded systems like Citus are not always ACID-compliant and how they differ from truly distributed systems. Then, we will dive deep into the design decisions made by YDB to address these difficulties and outline YDB's architecture layer by layer—from bare metal disks and distributed storage to OLTP and OLAP functionalities. Finally, we will briefly compare our approach with that of Calvin, which originally inspired YDB, and Spanner.

[{{ team.ivanov.name }}]({{ team.ivanov.profile }}) ({{ team.ivanov.position }}) discussed the architecture of YDB, focusing on building a unified platform for fault-tolerant and reliable OLTP and OLAP processing.

@[YouTube](https://youtu.be/fMR6zQVchgE?si=ru-xdaY8p1MpLus4)

The presentation will be of interest to developers of high-load systems and platform developers for various purposes.

[Slides](https://presentations.ydb.tech/2025/en/fosdem/designing_ydb/presentation.pdf)
