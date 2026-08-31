# CREATE TOPIC

You can use the `CREATE TOPIC` statement to create a [topic](../../../concepts/datamodel/topic), as well as [consumers](../../../concepts/datamodel/topic#consumer) for it.

General command format:

```yql
CREATE TOPIC topic_path (
    CONSUMER consumer_name [WITH (consumer_option = value[, ...])]
    ) WITH (topic_option = value[, ...]);
```

* `consumer_option` — consumer parameter;
* `topic_option` — topic parameter.

All command parameters except `topic_path` are optional. By default, a topic is created without consumers. All
parameters that are not specified explicitly are also set to their defaults (both for the topic and for the consumer).

Consumer parameters:

* `important` — defines an important consumer. No data will be deleted from the topic until all important consumers have processed it. Value type — `boolean`, default value: `false`.
* `availability_period` — defines the message availability period for the consumer. This option extends the message retention time in the topic from [retention_period](#topic-parameters) up to `availability_period` if the consumer does not acknowledge processing. Value type — `Interval`. Incompatible with the `important` parameter. No default value.
* `read_from` — defines the message write timestamp starting from which the consumer will receive data. Data written before this timestamp will not be read. Value type: `Datetime` OR `Timestamp` or `integer` (unix-timestamp as a number). Default value — `0` (read from the earliest time available in the topic).

{% if feature_topic_codecs %}

* `supported_codecs` — list of [codecs](../../../concepts/datamodel/topic#message-codec) supported by the consumer.

{% endif %}

## Topic parameters {#topic-parameters}

<<<<<<< HEAD
* `metering_mode` — resource metering mode (`RESERVED_CAPACITY` - based on allocated resources or `REQUEST_UNITS` - based on actual usage). Applies to topics in serverless databases. Value type - `String`.
* `min_active_partitions` — minimum number of active topic partitions. [Autopartitioning](../../../concepts/datamodel/topic#autopartitioning) will not decrease the number of active partitions below this value. Value type — `integer`, default value — `1`.
* `max_active_partitions` — maximum number of active topic partitions. [Autopartitioning](../../../concepts/datamodel/topic#autopartitioning) will not increase the number of active partitions above this value. Value type — `integer`, by default equal to `min_active_partitions`.
* `retention_period` — data retention period in the topic. Value type — `Interval`, default value — `18h`.
* `retention_storage_mb` — limit on the maximum disk space occupied by topic data. When this value is exceeded, older data is deleted as under retention. With automatic partitioning enabled, the consumed space may exceed the set value. Value type — `integer`, default value — `0` (unlimited).
* `partition_write_burst_bytes` — size of the write quota reserve for a partition in case of write bursts. When set to `0`, the actual write_burst value is taken equal to the quota value (which allows write bursts of up to 1 second). Value type — `integer`, default value: `0`.
* `partition_write_speed_bytes_per_second` — maximum allowed write speed to 1 partition. If the write stream to a partition exceeds this value, writes will be throttled. Value type — `integer`, default value — `2097152` (2 MB).
* `auto_partitioning_strategy` — [autopartitioning mode](../../../concepts/datamodel/topic#autopartitioning_strategies).
Allowed values: `disabled`, `paused`, `scale_up`, default value — `disabled`.
* `auto_partitioning_up_utilization_percent` — defines the partition load threshold as a percentage of the maximum write speed at which an automatic **increase** in the number of partitions is initiated. Value type — `integer`, default value — `80`.
=======
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
>>>>>>> da93627b85b (DOCS: add min max values of retention period for topic (#50516))
* `auto_partitioning_stabilization_window` — defines the time interval during which the load level must remain above the set threshold (`auto_partitioning_up_utilization_percent`) before the number of partitions is automatically increased. Value type — `Interval`, default value — `5m`.

{% if feature_topic_codecs %}

* `supported_codecs` — list of [codecs](../../../concepts/datamodel/topic#message-codec) supported by the topic. Value type — `String`.

{% endif %}

{% note info %}

When choosing a name for the topic, consider the common [schema object naming rules](../../../concepts/datamodel/cluster-namespace.md#object-naming-rules).

{% endnote %}

The following command creates a topic without consumers with default settings:

```yql
CREATE TOPIC `my_topic`;
```

{% if feature_topic_codecs %}

* `supported_codecs` - list of [codecs](../../../concepts/datamodel/topic#message-codec) supported by the topic. Value type - `String`.

{% endif %}

To create a topic with an important consumer and a data retention period of 1 day, run the following command:

```yql
CREATE TOPIC `my_topic` (
    CONSUMER my_consumer WITH (important = true)
) WITH (
    retention_period = Interval('P1D')
);
```

To create a topic with a data retention period of 1 day and two consumers, for one of which data can be stored for up to 2 days if needed, run the following command:

```yql
CREATE TOPIC `my_topic` (
    CONSUMER my_consumer1,
    CONSUMER my_consumer2 WITH (availability_period = Interval('P2D'))
) WITH (
    retention_period = Interval('P1D')
);
```