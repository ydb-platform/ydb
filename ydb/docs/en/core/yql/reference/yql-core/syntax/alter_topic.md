# ALTER TOPIC

You can use the `ALTER TOPIC` command to change the [topic](../../../../concepts/topic) settings, as well as add, update, or delete its consumers.

Here is the general format of the `ALTER TOPIC` command:

```sql
ALTER TOPIC topic_path action1, action2, ..., actionN;
```

`action` is one of the alter actions described below.

## Updating a set of consumers

`ADD CONSUMER`: Adds a [consumer](../../../../concepts/topic#consumer) to a topic.

The following example will add a consumer with default settings to the topic.

```sql
ALTER TOPIC `my_topic` ADD CONSUMER new_consumer;
```

When adding consumers, you can specify their settings, for example:

```sql
ALTER TOPIC `my_topic` ADD CONSUMER new_consumer2 WITH (important = false);
```

### Full list of available topic consumer settings

* `important`: Defines an important consumer. No data will be deleted from the topic until all the important consumers read them. Value type: `boolean`, default value: `false`.
* `read_from`: Sets up the message write time starting from which the consumer will receive data. Data written before this time will not be read. Value type: `Datetime` OR `Timestamp` OR `integer` (unix-timestamp in the numeric format). Default value: `now`.

{% if feature_topic_codecs %}
* `supported_codecs`: List of [codecs](../../../../concepts/topic#message-codec) supported by the consumer.

{% endif %}

`DROP CONSUMER`: Deletes the consumer from the topic.

```sql
ALTER TOPIC `my_topic` DROP CONSUMER old_consumer;
```

## Updating consumer settings

`ALTER CONSUMER`: Adds a consumer for a topic.

Here is the general syntax for `ALTER CONSUMER`:

```sql
ALTER TOPIC `topic_name` ALTER CONSUMER consumer_name consumer_action;
```

Supports the following types of `consumer_action`:
* `SET`: Sets consumer settings

{% if feature_topic_settings_reset %}
* `RESET`: Resets consumer settings to defaults.

{% endif %}

The following example will assign the `important` parameter to the consumer.

```sql
ALTER TOPIC `my_topic` ALTER CONSUMER my_consumer SET (important = true);
```

{% if feature_topic_settings_reset %}
This example will reset `read_from` to default.

```sql
ALTER TOPIC `my_topic` ALTER CONSUMER my_consumer RESET (read_from);
```

{% endif %}

You can specify several `ALTER CONSUMER` statements for a consumer. However, the settings applied by them shouldn't
repeat.

This is a valid statement:

```sql
ALTER TOPIC `my_topic`
    ALTER CONSUMER my_consumer SET (important = true)
    ALTER CONSUMER my_consumer SET (read_from = 0);
```

But this statement will raise an error.

```sql
ALTER TOPIC `my_topic`
    ALTER CONSUMER my_consumer SET (important = true)
    ALTER CONSUMER my_consumer SET (important = false);
```

## Updating topic settings

Using the `SET (option = value[, ...])` action, you can update your topic settings.

The example below will change the retention period for the topic and the writing quota per partition:

```sql
ALTER TOPIC `my_topic` SET (
    retention_period = Interval('PT36H'),
    partition_write_speed_bytes_per_second = 3000000
);
```

{% if feature_topic_settings_reset %}

The `RESET (option[, option2, ...])` action enables you to reset the topic settings to defaults.

**Example**

```sql
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
* `retention_storage_mb`: Limit on the maximum disk space occupied by the topic data. When this value is exceeded, the older data is cleared, like under a retention policy. `0` is interpreted as unlimited. Value type: `integer`, default value: `0`
* `partition_write_speed_bytes_per_second`: Maximum allowed write speed per partition. If a write speed for a given partition exceeds this value, the write speed will be capped. Value type: `integer`, default value: `2097152` (2MB).
* `partition_write_burst_bytes`: Write quota allocated for write bursts. When set to zero, the actual write_burst value is equalled to the quota value (this allows write bursts of up to one second). Value type: `integer`, default value: `0`.
* `metering_mode`: Resource metering mode (`RESERVED_CAPACITY` - based on the allocated resources or `REQUEST_UNITS` - based on actual usage). This option applies to topics in serverless databases. Value type: `String`.

{% if feature_topic_codecs %}
* `supported_codecs`: List of [codecs](../../../../concepts/topic#message-codec) supported by the topic. Value type: `String`.

{% endif %}
