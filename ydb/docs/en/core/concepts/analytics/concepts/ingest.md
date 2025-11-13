# Data Ingestion

{{ydb-short-name}} is designed to ingest both streaming and batch data. The absence of dedicated master nodes allows for parallel writes to all database nodes, enabling write throughput to scale linearly as the cluster grows. The choice of tool depends on the requirements for latency, delivery guarantees, and data volume.

## Streaming Ingestion (Real-time)

For scenarios requiring minimal latency, such as logs, metrics, and CDC streams.

* [Topics](../../../concepts/datamodel/topic.md) with [Kafka API](../../../reference/kafka-api/index.md): the primary and recommended method for streaming ingestion. Topics are the {{ydb-short-name}} built-in equivalent of Apache Kafka. Thanks to Kafka API support, you can use existing clients and systems (Apache Flink, Spark Streaming, Kafka Connect) without any changes. The key advantage is the ability to perform [transactional writes from a topic to a table](../../../concepts/datamodel/topic.md#topic-transactions), which guarantees `exactly-once` semantics at the database level.
* Plugins for Fluent Bit / Logstash: if you use [Fluent Bit](../../../integrations/ingestion/fluent-bit.md) or [Logstash](../../../integrations/ingestion/logstash.md) for log collection, specialized plugins allow you to write data directly to {{ydb-short-name}}, bypassing intermediate message brokers.
* Built-in data transfer (Transfer): the [Transfer](../../transfer.md) service allows you to transform and move data from topics to tables in streaming mode.

## Batch Ingestion (Batch)

For loading large volumes of historical data, exports from other systems, or the results of batch jobs.

* [BulkUpsert](../../../recipes/ydb-sdk/bulk-upsert.md) - the most performant method for batch inserts. BulkUpsert is a specialized API optimized for maximum throughput. It requires fewer resources compared to transactional operations, allowing you to load large datasets at maximum speed.
* [Federated queries](../../federated_query/index.md) to data in S3 / Data Lakes - {{ydb-short-name}} allows you to execute SQL queries directly against data stored in S3-compatible object storage or other external systems. This is a convenient way to load data without using separate ETL tools.
* [Apache Spark connector](../../../integrations/ingestion/spark.md) saves data directly to {{ydb-short-name}} tables in multi-threaded mode for the most performant writes.
* [JDBC driver](../../../reference/languages-and-apis/jdbc-driver/index.md) and [native SDKs](../../../reference/languages-and-apis/index.md)- with these, you can connect any applications or pipelines, including Apache Spark, Apache NiFi, and other solutions.