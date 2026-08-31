# CREATE TOPIC

The `CREATE TOPIC` call creates a [topic](../../../concepts/datamodel/topic.md).

When creating a topic, you can add topic [consumers](../../../concepts/datamodel/topic.md#consumer) to it and topic settings.

```yql
CREATE TOPIC topic_path (
    CONSUMER consumer1,
    CONSUMER consumer2 WITH (setting1 = value1)
) WITH (
    topic_setting2 = value2
);
```

<<<<<<< HEAD
All the parameters except the topic name are optional. By default, a topic is created without consumers. All
the omitted settings are also set by default (both for the topic and its consumers).
=======

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

* `supported_codecs` — a list of [codecs](../../../concepts/datamodel/topic.md#message-codec) supported by the topic. The value type is `String`.

{% endif %}
>>>>>>> da93627b85b (DOCS: add min max values of retention period for topic (#50516))

{% note info %}

When choosing a name for the topic, please consider the common [schema objects naming rules](../../../concepts/datamodel/cluster-namespace.md#object-naming-rules)

{% endnote %}

## Examples

Creating a topic without consumers with default settings:

```yql
CREATE TOPIC `my_topic`;
```

Creating a topic with a single consumer and the important option enabled:

```yql
CREATE TOPIC `my_topic` (
    CONSUMER my_consumer WITH (important = true)
);
```

### Full list of available topic consumer settings

* `important`: Defines an important consumer. No data will be deleted from the topic until all the important consumers read them. Value type: `boolean`, default value: `false`.
* `read_from`: Sets up the message write time starting from which the consumer will receive data. Data written before this time will not be read. Value type: `Datetime` OR `Timestamp` OR `integer` (unix-timestamp in the numeric format). Default value: `0` (read from the earliest available message).

{% if feature_topic_codecs %}
* `supported_codecs`: List of [codecs](concepts/topic#message-codec) supported by the consumer.

{% endif %}

Creating a topic with the retention period of one day:

```yql
CREATE TOPIC `my_topic` WITH(
    retention_period = Interval('P1D')
);
```

### Full list of available topic settings

* `min_active_partitions`: Minimum number of topic partitions. During automatic load balancing, the number of active partitions will not decrease below this value. Value type: `integer`, default value: `1`.
* `partition_count_limit`: Maximum number of active partitions in the topic. `0` is interpreted as unlimited. Value type: `integer`, default value: `0`.
* `retention_period`: Data retention period in the topic. Value type: `Interval`, default value: `18h`.
* `retention_storage_mb`: Limit on the maximum disk space occupied by the topic data. When this value is exceeded, the older data is cleared, like under a retention policy. The consumed space may exceed the set value when autopartitioning is enabled. Value type: `integer`, default value: `0` (no limit).
* `partition_write_speed_bytes_per_second`: Maximum allowed write speed per partition. If a write speed for a given partition exceeds this value, the write speed will be capped. Value type: `integer`, default value: `2097152` (2MB).
* `partition_write_burst_bytes`: Write quota allocated for write bursts. When set to zero, the actual write_burst value is equalled to the quota value (this allows write bursts of up to one second). Value type: `integer`, default value: `0`.
* `metering_mode`: Resource metering mode (`RESERVED_CAPACITY` - based on the allocated resources or `REQUEST_UNITS` - based on actual usage). This option applies to topics in serverless databases. Value type: `String`.

{% if feature_topic_codecs %}

* `supported_codecs`: List of [codecs](concepts/topic#message-codec) supported by the topic. Value type: `String`.

{% endif %}
