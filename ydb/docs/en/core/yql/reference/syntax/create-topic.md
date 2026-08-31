# CREATE TOPIC

Using the `CREATE TOPIC` statement, you can create a [topic](../../../../concepts/datamodel/topic) and [readers](../../../../concepts/datamodel/topic#consumer) for it.

General command syntax:


```yql
CREATE TOPIC topic_path (
    CONSUMER consumer_name [WITH (consumer_option = value[, ...])]
    ) WITH (topic_option = value[, ...]);
```


* `consumer_option` — reader parameter.
* `topic_option` — topic parameter.

All command parameters except `topic_path` are optional. By default, a topic is created without readers. All
parameters not explicitly specified are also set to defaults (for both the topic and the reader).

{% include [x](_includes/topic_consumer_parameters.md) %}

## Topic parameters {#topic-parameters}

* `metering_mode` — resource metering method (`RESERVED_CAPACITY` — by dedicated resources or `REQUEST_UNITS` — by actual usage). Relevant for topics in serverless databases. Value type — `String`.
* `min_active_partitions` — the minimum number of active partitions of the topic. [Auto-partitioning](../../../../concepts/datamodel/topic#autopartitioning) will not reduce the number of active partitions below this number. The value type is `integer`, and the default value is `1`.
* `max_active_partitions` — the maximum number of active partitions of the topic. [Auto-partitioning](../../../../concepts/datamodel/topic#autopartitioning) will not increase the number of active partitions above this number. The value type is `integer`, and by default it equals `min_active_partitions`.
* `retention_period`: Data retention period in the topic. Value type: `Interval`, default value: `24h`.
* `retention_storage_mb`: Limit on the maximum disk space occupied by the topic data. When this value is exceeded, the older data is cleared, like under a retention policy. The consumed space may exceed the set value when autopartitioning is enabled. Value type: `integer`, default value: `0` (no limit).
* `partition_write_burst_bytes` — the size of the write quota reserve for a partition to handle write bursts. When set to `0`, the actual write_burst value is taken to be equal to the quota value (which allows write bursts of up to 1 second). Value type — `integer`, default value: `0`.
* `partition_write_speed_bytes_per_second`: Maximum allowed write speed per partition. If a write speed for a given partition exceeds this value, the write speed will be capped. Value type: `integer`, default value: `2097152` (2MB).
* `auto_partitioning_strategy` — [auto-partitioning mode](../../../../concepts/datamodel/topic#autopartitioning_modes).
  Allowed values: `disabled`, `paused`, `scale_up`, default value — `disabled`.
* `auto_partitioning_up_utilization_percent` — defines the partition load threshold as a percentage of the maximum write speed, upon reaching which an automatic **increase** in the number of partitions will be initiated. Value type — `integer`, default value — `80`.
* `auto_partitioning_stabilization_window` — defines the time interval during which the load level must remain above the set threshold (`auto_partitioning_up_utilization_percent`) before the number of partitions is automatically increased. Value type — `Interval`, default value — `5m`.

{% if feature_topic_codecs %}

* `supported_codecs` — a list of [codecs](../../../../concepts/datamodel/topic#message-codec) supported by the topic. The value type is `String`.

{% endif %}

{% note info %}

When choosing a name for a topic, consider the general [rules for naming schema objects](../../../concepts/datamodel/cluster-namespace.md#object-naming-rules).

{% endnote %}

The following command creates a topic without readers with default settings:


```yql
CREATE TOPIC `my_topic`;
```


{% if feature_topic_codecs %}

* `supported_codecs`: List of [codecs](../../../../concepts/datamodel/topic#message-codec) supported by the topic. Value type: `String`.

{% endif %}

To create a topic with an important reader and a data retention period of 1 day, run the command:


```yql
CREATE TOPIC `my_topic` (
    CONSUMER my_consumer WITH (important = true)
) WITH (
    retention_period = Interval('P1D')
);
```


To create a topic with a data retention period of 1 day and two readers, for one of which data can be stored for up to 2 days if necessary, run the command:


```yql
CREATE TOPIC `my_topic` (
    CONSUMER my_consumer1,
    CONSUMER my_consumer2 WITH (availability_period = Interval('P2D'))
) WITH (
    retention_period = Interval('P1D')
);
```


To create a topic with a shared (common) reader, run the command:


```yql
CREATE TOPIC `my_topic` (
    CONSUMER my_consumer WITH (
        type = 'shared',
        keep_messages_order = false,
        default_processing_timeout = Interval('PT30S'),
        max_processing_attempts = 3,
        dead_letter_policy = 'move',
        dead_letter_queue = 'my_dlq_topic'
    )
);
```
