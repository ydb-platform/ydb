{% note info %}

Currently, not all functionality of Flink is supported for reading and writing. The following limitations exist:

- Exactly-once functionality via Kafka API is not supported at the moment, because transaction support in Kafka API is still under development.

- Subscription to topics using a pattern is currently unavailable.

- Using message CreateTime as a watermark is not available at the moment, because the current read time is used instead of CreateTime (will be fixed in future versions).

{% endnote %}