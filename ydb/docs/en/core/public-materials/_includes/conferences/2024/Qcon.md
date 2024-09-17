## {{ ydb-short-name }}: dealing with Big Data and moving towards AI {#2024-qcon}

{% include notitle [general_tag](../../tags.md#general) %}

{{ ydb-short-name }} is a versatile, open-source Distributed SQL database management system that combines high availability and scalability with strong consistency and ACID transactions. It provides services for machine learning products and goes beyond traditional vector search capabilities.

{% include [no_video](../../no_video.md) %}

This database is used for industrial operations within Yandex. Among its clients are Yandex Market, Yandex Alice, and Yandex Taxi, which are some of the largest and most demanding AI-based applications.

The database offers true elastic scalability, capable of scaling up or down by several orders of magnitude.

Simultaneously, the database is fault-tolerant. It is designed to operate across three availability zones, ensuring continuous operation even if one of the zones becomes unavailable. The database automatically recovers from disk failures, server failures, or data center failures, with minimal latency disruptions to applications.

Currently, work is underway to implement accurate and approximate nearest neighbor searches for machine learning purposes.

Takeaways:

* Architecture of a distributed, fault-tolerant database.
* Approaches to implementing vector search on large datasets.

[Slides](https://presentations.ydb.tech/2024/en/qcon/ydb_vector_search/presentation.pdf)