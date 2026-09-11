# ALTER TOPIC

You can use the `ALTER TOPIC` command to change the [topic](../../../concepts/datamodel/topic) settings, as well as add, update, or delete its consumers.

Here is the general format of the `ALTER TOPIC` command:

```yql
ALTER TOPIC topic_path action1, action2, ..., actionN;
```

`action` is one of the alter actions described below.

## Updating a set of consumers

`ADD CONSUMER`: Adds a [consumer](../../../concepts/datamodel/topic#consumer) to a topic.

The following example will add a consumer with default settings to the topic.

```yql
ALTER TOPIC `my_topic` ADD CONSUMER new_consumer;
```

When adding consumers, you can specify their settings, for example:

```yql
ALTER TOPIC `my_topic` ADD CONSUMER new_consumer2 WITH (important = false);
```

### Full list of available topic consumer settings

<<<<<<< HEAD
* `important`: Defines an important consumer. No data will be deleted from the topic until all the important consumers read them. Value type: `boolean`, default value: `false`.
* `read_from`: Sets up the message write time starting from which the consumer will receive data. Data written before this time will not be read. Value type: `Datetime` OR `Timestamp` OR `integer` (unix-timestamp in the numeric format). Default value: `0` (read from the earliest available message).
=======
* `metering_mode` — resource metering method (`RESERVED_CAPACITY` — by dedicated resources or `REQUEST_UNITS` — by actual usage). Relevant for topics in serverless databases. Value type — `String`.
* `min_active_partitions` — the minimum number of active partitions of the topic. [Auto-partitioning](../../../../concepts/datamodel/topic#autopartitioning) will not reduce the number of active partitions below this value. Type — `integer`, default value — `1`.
* `max_active_partitions` — the maximum number of active partitions of the topic. [Auto-partitioning](../../../../concepts/datamodel/topic#autopartitioning) will not increase the number of active partitions above this value. Type — `integer`, default is `min_active_partitions`.
* `retention_period`: Data retention period in the topic. Value type: `Interval`.
* `retention_storage_mb`: Limit on the maximum disk space occupied by the topic data. When this value is exceeded, the older data is cleared, like under a retention policy. The consumed space may exceed the set value when autopartitioning is enabled. Value type: `integer`, default value: `0` (no limit).
* `partition_write_burst_bytes` — the size of the write quota reserve for a partition to handle write bursts. When set to `0`, the actual write_burst value is taken to be equal to the quota value (which allows write bursts of up to 1 second). Value type — `integer`, default value: `0`.
* `partition_write_speed_bytes_per_second`: Maximum allowed write speed per partition. If a write speed for a given partition exceeds this value, the write speed will be capped. Value type: `integer`, default value: `2097152` (2MB).
* `auto_partitioning_strategy` — [auto-partitioning mode](../../../../concepts/datamodel/topic#autopartitioning_modes).
  Allowed values: `paused`, `scale_up`, default value — `disabled`.
* `auto_partitioning_up_utilization_percent` — defines the partition load threshold as a percentage of the maximum write speed, upon reaching which an automatic **increase** in the number of partitions will be initiated. Value type — `integer`, default value — `80`.
* `auto_partitioning_stabilization_window` — defines the time interval during which the load level must remain above the set threshold (`auto_partitioning_up_utilization_percent`) before the number of partitions is automatically increased. Value type — `Interval`, default value — `5m`.
>>>>>>> da93627b85b (DOCS: add min max values of retention period for topic (#50516))

{% if feature_topic_codecs %}

* `supported_codecs`: List of [codecs](../../../concepts/datamodel/topic.md#message-codec) supported by the consumer.

{% endif %}

`DROP CONSUMER`: Deletes the consumer from the topic.

```yql
ALTER TOPIC `my_topic` DROP CONSUMER old_consumer;
```

## Updating consumer settings

`ALTER CONSUMER`: Adds a consumer for a topic.

Here is the general syntax for `ALTER CONSUMER`:

```yql
ALTER TOPIC `topic_name` ALTER CONSUMER consumer_name consumer_action;
```

Supports the following types of `consumer_action`:

* `SET`: Sets consumer settings

{% if feature_topic_settings_reset %}

* `RESET`: Resets consumer settings to defaults.

{% endif %}

The following example will assign the `important` parameter to the consumer.

```yql
ALTER TOPIC `my_topic` ALTER CONSUMER my_consumer SET (important = true);
```

{% if feature_topic_settings_reset %}

This example will reset `read_from` to default.

```yql
ALTER TOPIC `my_topic` ALTER CONSUMER my_consumer RESET (read_from);
```

{% endif %}

You can specify several `ALTER CONSUMER` statements for a consumer. However, the settings applied by them shouldn't
repeat.

This is a valid statement:

```yql
ALTER TOPIC `my_topic`
    ALTER CONSUMER my_consumer SET (important = true)
    ALTER CONSUMER my_consumer SET (read_from = 0);
```

But this statement will raise an error.

```yql
ALTER TOPIC `my_topic`
    ALTER CONSUMER my_consumer SET (important = true)
    ALTER CONSUMER my_consumer SET (important = false);
```

## Updating topic settings

Using the `SET (option = value[, ...])` action, you can update your topic settings.

The example below will change the retention period for the topic and the writing quota per partition:

```yql
ALTER TOPIC `my_topic` SET (
    retention_period = Interval('PT36H'),
    partition_write_speed_bytes_per_second = 3000000
);
```

{% if feature_topic_settings_reset %}

The `RESET (option[, option2, ...])` action enables you to reset the topic settings to defaults.

### Example

```yql
ALTER TOPIC `my_topic` RESET (
    min_active_partitions,
    partition_count_limit
);
```

{% endif %}

### Full list of available topic settings

* `min_active_partitions`: Minimum number of topic partitions. During automatic load balancing, the number of active partitions will not decrease below this value. Value type: `integer`, default value: `1`.
* `partition_count_limit`: Maximum number of active partitions in the topic. `0` is interpreted as unlimited. Value type: `integer`, default value: `0`.
* `retention_period`: Data retention period in the topic. Value type: `Interval`, default value: `18h`.
* `retention_storage_mb`: Limit on the maximum disk space occupied by the topic data. When this value is exceeded, the older data is cleared, like under a retention policy. The consumed space may exceed the set value when autopartitioning is enabled. Value type: `integer`, default value: `0` (no limit).
* `partition_write_speed_bytes_per_second`: Maximum allowed write speed per partition. If a write speed for a given partition exceeds this value, the write speed will be capped. Value type: `integer`, default value: `2097152` (2MB).
* `partition_write_burst_bytes`: Write quota allocated for write bursts. When set to zero, the actual write_burst value is equalled to the quota value (this allows write bursts of up to one second). Value type: `integer`, default value: `0`.
* `metering_mode`: Resource metering mode (`RESERVED_CAPACITY` - based on the allocated resources or `REQUEST_UNITS` - based on actual usage). This option applies to topics in serverless databases. Value type: `String`.

{% if feature_topic_codecs %}

* `supported_codecs`: List of [codecs](../../../concepts/datamodel/topic.md#message-codec) supported by the topic. Value type: `String`.

{% endif %}

### Change autopartitioning strategies for the topic {#autopartitioning}

The following command sets the [autopartitioning](../../../concepts/datamodel/topic.md#autopartitioning) strategy to `UP`:

```yql
ALTER TOPIC `my_topic` SET (
    min_active_partitions = 1,
    max_active_partitions = 5,
    auto_partitioning_strategy = 'scale_up'
);
```

The following command pauses the topic [autopartitioning](../../../concepts/datamodel/topic.md#autopartitioning):

```yql
ALTER TOPIC `my_topic` SET (
    auto_partitioning_strategy = 'paused'
);
```

The following command unpauses the topic [autopartitioning](../../../concepts/datamodel/topic.md#autopartitioning):

```yql
ALTER TOPIC `my_topic` SET (
    auto_partitioning_strategy = 'scale_up'
);
```
