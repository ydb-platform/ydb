# ALTER TOPIC

You can use the `ALTER TOPIC` command to change the [topic](../../../concepts/datamodel/topic) settings, as well as add, update, or delete its consumers.

Here is the general format of the `ALTER TOPIC` command:

```yql
ALTER TOPIC topic_path action1, action2, ..., actionN;
```

`action` is one of the alter actions described below.

## Updating a set of consumers

### Add consumer {#add-consumer}

`ADD CONSUMER`: Adds a [consumer](../../../concepts/datamodel/topic#consumer) to a topic.

When adding consumers, you can specify their settings.

{% include [x](_includes/topic_consumer_parameters.md) %}

The following example will add a consumer with default settings to the topic.

```yql
ALTER TOPIC `my_topic` ADD CONSUMER new_consumer;
```

The following example will add an important consumer to the topic:

```yql
ALTER TOPIC `my_topic` ADD CONSUMER new_consumer2 WITH (important = true);
```

The following example will add a shared consumer to the topic:

```yql
ALTER TOPIC `my_topic`
    ADD CONSUMER my_shared_consumer WITH (
        type = 'shared',
        keep_messages_order = false,
        default_processing_timeout = Interval('PT30S'),
        max_processing_attempts = 3,
        dead_letter_policy = 'move',
        dead_letter_queue = 'my_dlq_topic'
    );
```

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

* `supported_codecs`: List of [codecs](../../../concepts/datamodel/topic#message-codec) supported by the topic. Value type: `String`.

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
