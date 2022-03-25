## Persistent queue {#persistent-queue}

A persistent queue consists of one or more partitions, where each partition is a [FIFO](https://en.wikipedia.org/wiki/FIFO_(computing_and_electronics)) [message queue](https://en.wikipedia.org/wiki/Message_queue) ensuring reliable delivery between two or more components. Data messages have no type and are data blobs. Partitioning is a parallel processing tool that helps ensure high queue bandwidth. Mechanisms are provided to implement the "at least once" and the "exactly once" persistent queue delivery guarantees. A persistent queue in {{ ydb-short-name }} is similar to a topic in [Apache Kafka](https://en.wikipedia.org/wiki/Apache_Kafka).

