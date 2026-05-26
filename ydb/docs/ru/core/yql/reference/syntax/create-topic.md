# CREATE TOPIC

С помощью оператора `CREATE TOPIC` можно создать [топик](../../../../concepts/datamodel/topic), а также [читателей](../../../../concepts/datamodel/topic#consumer) для него.

Общий вид команды:

```yql
CREATE TOPIC topic_path (
    CONSUMER consumer_name [WITH (consumer_option = value[, ...])]
    ) WITH (topic_option = value[, ...]);
```

* `consumer_option` — параметр читателя;
* `topic_option` — параметр топика.

Все параметры команды, кроме `topic_path` не обязательны. По умолчанию топик создается без читателей. Все
не указанные явно параметры также выставляются по умолчанию (и для топика, и для читателя).

{% include [x](_includes/topic_consumer_parameters.md) %}

## Параметры топика {#topic-parameters}

* `metering_mode` — способ метеринга ресурсов (`RESERVED_CAPACITY` - по выделенным ресурсам или `REQUEST_UNITS` - по фактическому использованию). Актуально для топиков в serverless базах данных. Тип значения - `String`.
* `min_active_partitions` — минимальное количество активных партиций топика. [Автопартиционирование](../../../../concepts/datamodel/topic#autopartitioning) не будет уменьшать количество активных партиций ниже этого количества. Тип значения — `integer`, значение по умолчанию — `1`.
* `max_active_partitions` — максимальное количество активных партиций топика. [Автопартиционирование](../../../../concepts/datamodel/topic#autopartitioning) не будет увеличивать количество активных партиций выше этого количества. Тип значения — `integer`, по умолчанию равно `min_active_partitions`.
* `retention_period` — время хранения данных в топике. Тип значения — `Interval`, значение по умолчанию — `18h`.
* `retention_storage_mb` — ограничение на максимальное место на диске, занимаемое данными топика. При превышении этого значения старые данные будут удаляться, как по retention. Тип значения — `integer`, значение по умолчанию — `0` (не ограничено).
* `partition_write_burst_bytes` — размер запаса квоты на запись в партицию на случай всплесков записи. При выставлении в `0` фактическое значение write_burst принимается равным значению квоты (что позволяет всплески записи длительностью до 1 секунды). Тип значения — `integer`, значение по умолчанию: `0`.
* `partition_write_speed_bytes_per_second` — максимальная разрешенная скорость записи в 1 партицию. Если поток записи в партицию превысит это значение, запись будет квотироваться. Тип значения — `integer`, значение по умолчанию — `2097152` (2 МБ).
* `auto_partitioning_strategy` — [режим автопартиционирования](../../../../concepts/datamodel/topic#autopartitioning_modes).
Допустимые значения: `disabled`, `paused`, `scale_up`, значение по умолчанию — `disabled`.
* `auto_partitioning_up_utilization_percent` — определяет порог загрузки партиции в процентах от максимальной скорости записи, при достижении которого будет инициировано автоматическое **увеличение** числа партиций. Тип значения — `integer`, значение по умолчанию — `80`.
* `auto_partitioning_stabilization_window` — определяет временной интервал, в течение которого уровень нагрузки должен оставаться выше установленного порога (`auto_partitioning_up_utilization_percent`), прежде чем будет выполнено автоматическое увеличение количества партиций. Тип значения — `Interval`, значение по умолчанию — `5m`.

{% if feature_topic_codecs %}

* `supported_codecs` — список [кодеков](../../../../concepts/datamodel/topic#message-codec), поддерживаемых топиком. Тип значения — `String`.

{% endif %}

{% note info %}

При выборе имени для топика учитывайте общие [правила именования схемных объектов](../../../concepts/datamodel/cluster-namespace.md#object-naming-rules).

{% endnote %}

Следующая команда создаст топик без читателей с настройками по умолчанию:

```yql
CREATE TOPIC `my_topic`;
```

{% if feature_topic_codecs %}

* `supported_codecs` - список [кодеков](../../../../concepts/datamodel/topic#message-codec), поддерживаемых топиком. Тип значения - `String`.

{% endif %}

Чтобы создать топик с важным читателем и временем хранения данных 1 сутки, выполните команду:

```yql
CREATE TOPIC `my_topic` (
    CONSUMER my_consumer WITH (important = true)
) WITH (
    retention_period = Interval('P1D')
);
```

Чтобы создать топик с временем хранения данных 1 сутки и двумя читателями, для одного из которых данные могут по необходимости храниться до 2-х суток, выполните команду:

```yql
CREATE TOPIC `my_topic` (
    CONSUMER my_consumer1,
    CONSUMER my_consumer2 WITH (availability_period = Interval('P2D'))
) WITH (
    retention_period = Interval('P1D')
);
```

Чтобы создать топик с shared-читателем выполните команду:

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
)
```