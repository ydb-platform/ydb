# ALTER TOPIC

С помощью оператора `ALTER TOPIC` можно изменить настройки [топика](../../../../concepts/datamodel/topic), а также добавить, изменить или удалить [читателя](../../../../concepts/datamodel/topic#consumer).

Общий вид команды:

```yql
ALTER TOPIC topic_path action1, action2, ..., actionN;
```

* `action` — действие по изменению. Возможные действия описаны ниже.

## Работа с топиком {#topic}

### Задать параметры топика {#alter-topic}

`SET (option = value[, ...])` — действие задает параметры топика.

Общий вид команды:

```yql
ALTER TOPIC topic_path SET (option = value[, ...]);
```

* `option` и `value` — параметр топика и его значение.

### Параметры топика: {#topic-parameters}

* `metering_mode` — способ метеринга ресурсов (`RESERVED_CAPACITY` - по выделенным ресурсам или `REQUEST_UNITS` - по фактическому использованию). Актуально для топиков в serverless базах данных. Тип значения - `String`.
* `min_active_partitions` — минимальное количество активных партиций топика. [Автопартиционирование](../../../../concepts/datamodel/topic#autopartitioning) не будет уменьшать количество активных партиций ниже этого значения. Тип — `integer`, значение по умолчанию — `1`.
* `max_active_partitions` — максимальное количество активных партиций топика. [Автопартиционирование](../../../../concepts/datamodel/topic#autopartitioning) не будет увеличивать количество активных партиций выше этого значения. Тип — `integer`, по умолчанию равно `min_active_partitions`.
* `retention_period` — время хранения данных в топике. Тип значения — `Interval`, значение по умолчанию — `18h`.
* `retention_storage_mb` — ограничение на максимальное место на диске, занимаемое данными топика. При превышении этого значения старые данные будут удаляться, как по retention. Тип значения — `integer`, значение по умолчанию — `0` (не ограничено).
* `partition_write_burst_bytes` — размер запаса квоты на запись в партицию на случай всплесков записи. При выставлении в `0` фактическое значение write_burst принимается равным значению квоты (что позволяет всплески записи длительностью до 1 секунды). Тип значения — `integer`, значение по умолчанию: `0`.
* `partition_write_speed_bytes_per_second` — максимальная разрешенная скорость записи в 1 партицию. Если поток записи в партицию превысит это значение, запись будет квотироваться. Тип значения — `integer`, значение по умолчанию — `2097152` (2 МБ).
* `auto_partitioning_strategy` — [режим автопартиционирования](../../../../concepts/datamodel/topic#autopartitioning_modes).
Допустимые значения: `paused`, `scale_up`, значение по умолчанию — `disabled`.
* `auto_partitioning_up_utilization_percent` — определяет порог загрузки партиции в процентах от максимальной скорости записи, при достижении которого будет инициировано автоматическое **увеличение** числа партиций. Тип значения — `integer`, значение по умолчанию — `80`.
* `auto_partitioning_stabilization_window` — определяет временной интервал, в течение которого уровень нагрузки должен оставаться выше установленного порога (`auto_partitioning_up_utilization_percent`), прежде чем будет выполнено автоматическое увеличение количества партиций. Тип значения — `Interval`, значение по умолчанию — `5m`.

{% if feature_topic_codecs %}

* `supported_codecs` — список [кодеков](../../../../concepts/datamodel/topic#message-codec), поддерживаемых топиком. Тип значения — `String`.

{% endif %}

Следующая команда изменит время хранения данных в топике и квоту на скорость записи в 1 партицию:

```yql
ALTER TOPIC `my_topic` SET (
    retention_period = Interval('PT36H'),
    partition_write_speed_bytes_per_second = 3000000
);
```

### Включение и приостановка автопартиционирования {#autopartitioning}

Следующая команда включает [автопартиционирование](../../../../concepts/datamodel/topic#autopartitioning):

```yql
ALTER TOPIC `my_topic` SET (
    min_active_partitions = 1,
    max_active_partitions = 5,
    auto_partitioning_strategy = 'scale_up'
);
```

Следующая команда ставит [автопартиционирование](../../../../concepts/datamodel/topic#autopartitioning) на паузу:

```yql
ALTER TOPIC `my_topic` SET (
    auto_partitioning_strategy = 'paused'
);
```

Следующая команда снимает [автопартиционирование](../../../../concepts/datamodel/topic#autopartitioning) с паузы:

```yql
ALTER TOPIC `my_topic` SET (
    auto_partitioning_strategy = 'scale_up'
);
```

{% if feature_topic_settings_reset %}

### Сбросить параметры топика {#reset-topic}

`RESET (option[, option2, ...])` — действие сбрасывает указанный параметр топика в значение по умолчанию.

Общий вид команды:

```yql
ALTER TOPIC topic_path RESET (option[, option2, ...]);
```

* `option` — параметр топика.

Следующая команда сбросит значения параметров *минимальное количество активных партиций* и *максимальное количество активных партиций* в значения по умолчанию:

```yql
ALTER TOPIC `my_topic` RESET (
    min_active_partitions,
    max_active_partitions
);
```

{% endif %}

## Работа с читателем {#consumer}

### Добавить читателя {#add-consumer}

`ADD CONSUMER` — действие добавляет [читателей](../../../../concepts/datamodel/topic#consumer) для топика.

Общий вид команды:

```yql
ALTER TOPIC topic_path ADD CONSUMER consumer_name [WITH (option = value[, ...])];
```

* `option` и `value` — параметр читателя и его значение.

Параметры читателя:

* `important` — определяет важного читателя. Никакие данные из топика не будут удалены, пока все важные читатели их не обработали. Тип значения — `boolean`, значение по умолчанию: `false`.
* `availability_period` — определяет время доступности сообщений для читателя. Опция позволяет продлить время хранения сообщений в топике с [retention_period](#topic-parameters) вплоть до `availability_period`, если читатель не подтверждает их обработку. Тип значения — `Interval`. Не совместим с параметром `important`. Значение по умолчанию отсутствует.
* `read_from` — определяет момент времени записи сообщений, начиная с которого читатель будет получать данные. Данные, записанные ранее этого момента, прочитаны не будут. Тип значения: `Datetime` ИЛИ `Timestamp` или `integer` (unix-timestamp в виде числа). Значение по умолчанию — `0` (чтение с самого раннего доступного в топике времени).
{% if feature_topic_codecs %}
* `supported_codecs` — список [кодеков](../../../../concepts/datamodel/topic#message-codec), поддерживаемых читателем.
{% endif %}

Следующая команда добавит к топику читателя с настройками по умолчанию:

```yql
ALTER TOPIC `my_topic` ADD CONSUMER my_consumer;
```

Следующая команда добавит к топику важного читателя:

```yql
ALTER TOPIC `my_topic` ADD CONSUMER my_consumer2 WITH (important = true);
```

### Задать параметры читателя {#alter-consumer}

`ALTER CONSUMER consumer_name SET (option = value[, ...])` — действие задает параметры читателя топика.

Общий вид команды:

```yql
ALTER TOPIC topic_path ALTER CONSUMER consumer_name SET (option = value[, ...]);
```

* `option` и `value` — параметр читателя и его значение.

Следующая команда сделает читателя важным:

```yql
ALTER TOPIC `my_topic` ALTER CONSUMER my_consumer SET (important = true);
```

В одной команде может быть указано несколько `ALTER CONSUMER` действий, настройки в них не должны повторяться:

```yql
ALTER TOPIC `my_topic`
    ALTER CONSUMER my_consumer SET (availability_period = Interval('PT48H'))
    ALTER CONSUMER my_consumer SET (read_from = 0);
```

{% if feature_topic_settings_reset %}

### Сбросить параметры читателя {#reset-consumer}

`ALTER CONSUMER consumer_name RESET (option[, ...])` — действие сбрасывает указанные настройки к значениям по умолчанию.

Общий вид команды:

```yql
ALTER TOPIC topic_path ALTER CONSUMER consumer_name RESET (option[, ...]);
```

* `option` — параметр читателя.

Данный пример сбросит параметры `read_from` и `availability_period` в значения по умолчанию:

```yql
ALTER TOPIC `my_topic` ALTER CONSUMER my_consumer RESET (read_from, availability_period);
```

{% endif %}

### Удалить читателя {#drop-consumer}

`DROP CONSUMER` — действие удаляет читателя топика.

Общий вид команды:

```yql
ALTER TOPIC topic_path DROP CONSUMER consumer_name;
```

Следующая команда удалит читателя с именем `old_consumer`:

```yql
ALTER TOPIC `my_topic` DROP CONSUMER old_consumer;
```
