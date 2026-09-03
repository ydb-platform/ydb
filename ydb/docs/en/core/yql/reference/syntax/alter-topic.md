# ALTER TOPIC

Using the `ALTER TOPIC` operator, you can change the settings of a [topic](../../../../concepts/datamodel/topic), as well as add, modify, or delete a [reader](../../../../concepts/datamodel/topic#consumer).

General command syntax:


```yql
ALTER TOPIC topic_path action1, action2, ..., actionN;
```


* You can specify several `action` statements for a consumer. However, the settings applied by them shouldn't
  repeat.

## Working with a topic {#topic}

### Set topic parameters {#alter-topic}

Using the `SET (option = value[, ...])` action, you can update your topic settings.

General command syntax:


```yql
ALTER TOPIC topic_path SET (option = value[, ...]);
```


* `option` and `value` — the topic parameter and its value.

## Topic parameters {#topic-parameters}

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

{% if feature_topic_codecs %}

* `supported_codecs` — a list of [codecs](../../../concepts/datamodel/topic.md#message-codec) supported by the topic. Value type: `String`.

{% endif %}

The following command will change the data retention time in the topic and the write speed quota for 1 partition:


```yql
ALTER TOPIC `my_topic` SET (
    retention_period = Interval('PT36H'),
    partition_write_speed_bytes_per_second = 3000000
);
```


### Change autopartitioning strategies for the topic {#autopartitioning}

The following command enables [auto-partitioning](../../../../concepts/datamodel/topic#autopartitioning):


```yql
ALTER TOPIC `my_topic` SET (
    min_active_partitions = 1,
    max_active_partitions = 5,
    auto_partitioning_strategy = 'scale_up'
);
```


The following command pauses the topic [autopartitioning](../../../../concepts/datamodel/topic#autopartitioning):


```yql
ALTER TOPIC `my_topic` SET (
    auto_partitioning_strategy = 'paused'
);
```


The following command unpauses the topic [autopartitioning](../../../../concepts/datamodel/topic#autopartitioning):


```yql
ALTER TOPIC `my_topic` SET (
    auto_partitioning_strategy = 'scale_up'
);
```


{% if feature_topic_settings_reset %}

### Reset topic parameters {#reset-topic}

`RESET (option[, option2, ...])` — the action resets the specified topic parameter to its default value.

General command syntax:


```yql
ALTER TOPIC topic_path RESET (option[, option2, ...]);
```


* `option` — the topic parameter.

The following command will reset the values of the *minimum number of active partitions* and *maximum number of active partitions* parameters to their default values:


```yql
ALTER TOPIC `my_topic` RESET (
    min_active_partitions,
    max_active_partitions
);
```

{% endif %}

## Working with a reader {#consumer}

### Add a reader {#add-consumer}

`ADD CONSUMER` — the action adds [readers](../../../../concepts/datamodel/topic#consumer) for the topic.

General command syntax:


```yql
ALTER TOPIC topic_path ADD CONSUMER consumer_name [WITH (option = value[, ...])];
```


* `option` and `value` — the reader parameter and its value.

{% include [x](_includes/topic_consumer_parameters.md) %}

The following command will add a reader with default settings to the topic:


```yql
ALTER TOPIC `my_topic` ADD CONSUMER my_consumer;
```


The following command will add an important reader to the topic:


```yql
ALTER TOPIC `my_topic` ADD CONSUMER my_consumer2 WITH (important = true);
```


The following command will add a shared (common) reader to the topic:


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


### Set reader parameters {#alter-consumer}

`ALTER CONSUMER consumer_name SET (option = value[, ...])` — the action sets the parameters of the topic reader.

General command syntax:


```yql
ALTER TOPIC topic_path ALTER CONSUMER consumer_name SET (option = value[, ...]);
```


* `option` and `value` — the reader parameter and its value.

The following command will make the reader important:


```yql
ALTER TOPIC `my_topic` ALTER CONSUMER my_consumer SET (important = true);
```


A single command can contain multiple `ALTER CONSUMER` actions, and their settings must not be duplicated:


```yql
ALTER TOPIC `my_topic`
    ALTER CONSUMER my_consumer SET (availability_period = Interval('PT48H'))
    ALTER CONSUMER my_consumer SET (read_from = 0);
```


{% if feature_topic_settings_reset %}

### Reset reader parameters {#reset-consumer}

`ALTER CONSUMER consumer_name RESET (option[, ...])` — the action resets the specified settings to their default values.

General command syntax:


```yql
ALTER TOPIC topic_path ALTER CONSUMER consumer_name RESET (option[, ...]);
```


* `option` — the reader parameter.

This example will reset the `read_from` and `availability_period` parameters to their default values:


```yql
ALTER TOPIC `my_topic` ALTER CONSUMER my_consumer RESET (read_from, availability_period);
```

{% endif %}

### Delete a reader {#drop-consumer}

`DROP CONSUMER` — the action deletes the topic reader.

General command syntax:


```yql
ALTER TOPIC topic_path DROP CONSUMER consumer_name;
```


The following command will delete the reader named `old_consumer`:


```yql
ALTER TOPIC `my_topic` DROP CONSUMER old_consumer;
```
