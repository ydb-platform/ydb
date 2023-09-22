# CREATE TOPIC

The `CREATE TOPIC` call creates a [topic](../../../../concepts/topic).

When creating a topic, you can add topic [consumers](../../../../concepts/topic#consumer) to it and topic settings.

```
    CREATE TOPIC topic_path (
        CONSUMER consumer1,
        CONSUMER consumer2 WITH (setting1 = value1)
    ) WITH (
        topic_setting2 = value2
    );
```

All the parameters except the topic name are optional. By default, a topic is created without consumers. All
the omitted settings are also set by default (both for the topic and its consumers).

## Examples

Creating a topic without consumers with default settings:

```sql
CREATE TOPIC `my_topic`;
```

Creating a topic with a single consumer and the important option enabled:

```sql
CREATE TOPIC `my_topic` (
    CONSUMER my_consumer WITH (important = true)
);
```

### Full list of available topic consumer settings

* `important`: Defines an important consumer. No data will be deleted from the topic until all the important consumers read them. Value type: `boolean`, default value: `false`.
* `read_from`: Sets up the message write time starting from which the consumer will receive data. Data written before this time will not be read. Value type: `Datetime` OR `Timestamp` OR `integer` (unix-timestamp in the numeric format). Default value: `now`.

{% if feature_topic_codecs %}
* `supported_codecs`: List of [codecs](../../../../concepts/topic#message-codec) supported by the consumer.

{% endif %}

Creating a topic with the retention period of one day:

```sql
CREATE TOPIC `my_topic` WITH(
    retention_period = Interval('P1D')
);
```

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
