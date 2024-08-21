### {{ ydb-short-name }}: dealing with Big Data and moving towards AI {#2024-qcon}

{% include notitle [general_tag](../../tags.md#general) %}

{{ ydb-short-name }} is a versatile open-source Distributed SQL Database that combines high availability and scalability with strong consistency and ACID transactions. It provides services for ML-products and goes ahead of vector search.

**Outline**:

The database is used for industrial operations within Yandex. Among its clients, Yandex Market, Yandex Alice, and Yandex Taxi. They are some of the largest and heaviest AI-based applications.

The database provides true elastic scalability, capable of scaling up or down by several orders of magnitude.

At the same time, the database is fault-tolerant. It has been designed to work across three availability zones, ensuring that the database continues to function even if one of the zones becomes unavailable. The database automatically recovers after a disk failure, server failure, or datacenter failure, with minimal latency disruptions to the applications.

Currently, work is underway to implement accurate and approximate nearest neighbor search for machine learning purposes.

**Takeaways**:
* Architecture of a distributed fault-tolerant database.
* Approaches to the implementation of vector search on large amounts of data.

[Slides](https://presentations.ydb.tech/2024/en/qcon/ydb_vector_search/presentation.pdf)