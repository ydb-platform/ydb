Consumer parameters:

* `type` — consumer type. Possible values: `STREAMING` and `SHARED`. Default value: `STREAMING`.
* `important` — defines an important consumer. No data will be deleted from the topic until all important consumers process them. Value type: `boolean`, default value: `false`.
* `availability_period` — defines the time during which messages are available to the consumer. This option allows you to extend message retention in the topic beyond [retention_period](#topic-parameters) up to `availability_period` if the consumer does not acknowledge processing. Value type: `Interval`. Incompatible with the `important` parameter. No default value.
* `read_from` — sets the message write time starting from which the consumer will receive data. Messages written before this time will not be received by the consumer. Value type: `Datetime`, `Timestamp`, or `integer` (Unix timestamp as a number). Default value: `0` (read from the earliest available time in the topic).
{% if feature_topic_codecs %}

* `supported_codecs` — list of [codecs](../../../../concepts/datamodel/topic#message-codec) supported by the consumer.

{% endif %}

Consumer parameters available only for a shared consumer:

* `keep_messages_order` — preserves message read order. If the value is `true`, message processing order is guaranteed within a single message group. Default value: `false`.
* `default_processing_timeout` — message processing time. If processing is not acknowledged and the processing time is not extended within this period, the message returns to the queue and is delivered for reprocessing. Default value: `Interval('PT30S')`.
* `max_processing_attempts` — maximum number of processing attempts for a single message (value type: `integer`). Supported only when `dead_letter_policy = move` or `dead_letter_policy = delete`. Default value: `1000`.
* `dead_letter_policy` — action to perform with a message if all processing attempts fail (value type: `String`). Possible values: `delete`, `move`, `none`. Default value: `none`.
* `dead_letter_queue` — DLQ topic name (value type: `String`). Required when `dead_letter_policy = move` and not supported when `dead_letter_policy = none`/`delete`.
