Reader parameters:

* `type` — reader type. Possible values: `STREAMING` and `SHARED`. Default value: `STREAMING`.
* `important` — important reader flag. Data from the topic will not be deleted until all important readers process it. Value type: `boolean`, default value: `false`.
* `availability_period` — determines the time messages are available to the reader. The option allows extending the message retention time in the topic beyond [retention_period](#topic-parameters), up to `availability_period`, if the reader does not confirm their processing. Value type: `Interval`. Incompatible with the `important` parameter. No default value.
* `read_from` — determines the point in time from which the reader will receive data. Messages written before this point will not be received by the reader. Value type: `Datetime`, `Timestamp`, or `integer` (Unix timestamp as a number). Default value: `0` (reading from the earliest available point in time in the topic).

{% if feature_topic_codecs %}

* `supported_codecs` — list of [codecs](../../../../concepts/datamodel/topic#message-codec) supported by the reader.

{% endif %}

Reader parameters available only for a shared (common) reader:

* `keep_messages_order` — preserves the order of message reading. If the value is `true`, the order of message processing within a single message group is guaranteed. Default value: `false`.
* `default_processing_timeout` — message processing time. If message processing is not confirmed within this time and the processing time is not extended, the message will return to the queue and be sent for reprocessing. Default value: `Interval('PT30S')`.
* `max_processing_attempts` — maximum number of processing attempts for a single message (value type: `integer`). The option is supported only with `dead_letter_policy = move` or `dead_letter_policy = delete`. Default value: `1000`.
* `dead_letter_policy` — action to take with the message if all processing attempts have failed (value type: `String`). Possible values: `delete`, `move`, `none`. Default value: `none`.
* `dead_letter_queue` — DLQ topic name (value type: `String`). Required for `dead_letter_policy = move` and not supported for `dead_letter_policy = none` or `delete`.
