# ALTER TOPIC

С помощью оператора `ALTER TOPIC` можно изменить настройки [топика](../../../../concepts/topic), а также добавить, изменить или удалить [читателя](../../../../concepts/topic#consumer).

Общий вид команды:

```sql
ALTER TOPIC topic_path action1, action2, ..., actionN;
```

* `action` — действие по изменению. Возможные действия описаны ниже.

## Работа с топиком {#topic}

### Задать параметры топика {#alter-topic}

`SET (option = value[, ...])` — действие задает параметры топика.

Общий вид команды:

```sql
ALTER TOPIC topic_path SET (option = value[, ...]);
```

* `option` и `value` — параметр топика и его значение.

Параметры топика:

* `metering_mode` — способ метеринга ресурсов (`RESERVED_CAPACITY` - по выделенным ресурсам или `REQUEST_UNITS` - по фактическому использованию). Актуально для топиков в serverless базах данных. Тип значения - `String`.
* `min_active_partitions` — минимальное количество активных партиций топика. Автоматическая балансировка нагрузки не будет уменьшать количество активных партиций ниже этого количества. Тип значения — `integer`, значение по умолчанию — `1`.
* `retention_period` — время хранения данных в топике. Тип значения — `Interval`, значение по умолчанию — `18h`.
* `retention_storage_mb` — ограничение на максимальное место на диске, занимаемое данными топика. При превышении этого значения старые данные будут удаляться, как по retention. Тип значения — `integer`, значение по умолчанию — `0` (не ограничено).
* `partition_count_limit` — максимальное количество активных партиций топика. Тип значения — `integer`, значение по умолчанию: `0` (не ограничено).
* `partition_write_burst_bytes` — размер запаса квоты на запись в партицию на случай всплесков записи. При выставлении в `0` фактическое значение write_burst принимается равным значению квоты (что позволяет всплески записи длительностью до 1 секунды). Тип значения — `integer`, значение по умолчанию: `0`.
* `partition_write_speed_bytes_per_second` — максимальная разрешенная скорость записи в 1 партицию. Если поток записи в партицию превысит это значение, запись будет квотироваться. Тип значения — `integer`, значение по умолчанию — `2097152` (2 МБ).
{% if feature_topic_codecs %}
* `supported_codecs` — список [кодеков](../../../../concepts/topic#message-codec), поддерживаемых топиком. Тип значения — `String`.
{% endif %}

Следующая команда изменит время хранения данных в топике и квоту на скорость записи в 1 партицию:

```sql
ALTER TOPIC `my_topic` SET (
    retention_period = Interval('PT36H'),
    partition_write_speed_bytes_per_second = 3000000
);
```

{% if feature_topic_settings_reset %}

### Сбросить параметры топика {#reset-topic}

`RESET (option[, option2, ...])` — действие сбрасывает указанный параметр топика в значение по умолчанию.

Общий вид команды:

```sql
ALTER TOPIC topic_path RESET (option[, option2, ...]);
```

* `option` — параметр топика.

Следующая команда сбросит значения параметров _минимальное количество активных партиций_ и _максимальное количество активных партиций_ в значения по умолчанию:

```sql
ALTER TOPIC `my_topic` RESET (
    min_active_partitions,
    partition_count_limit
);
```

{% endif %}

## Работа с читателем {#consumer}

### Добавить читателя {#add-consumer}

`ADD CONSUMER` — действие добавляет [читателей](../../../../concepts/topic#consumer) для топика.

Общий вид команды:

```sql
ALTER TOPIC topic_path ADD CONSUMER consumer_name [WITH (option = value[, ...])];
```

* `option` и `value` — параметр читателя и его значение.

Параметры читателя:

* `important`— определяет важного читателя. Никакие данные из топика не будут удалены, пока все важные читатели их не прочитали. Тип значения — `boolean`, значение по умолчанию: `false`.
* `read_from`— определяет момент времени записи сообщений, начиная с которого читатель будет получать данные. Данные, записанные ранее этого момента, прочитаны не будут. Тип значения: `Datetime` ИЛИ `Timestamp` или `integer` (unix-timestamp в виде числа). Значение по умолчанию — `now`
{% if feature_topic_codecs %}
* `supported_codecs` — список [кодеков](../../../../concepts/topic#message-codec), поддерживаемых читателем.
{% endif %}

Следующая команда добавит к топику читателя с настройками по умолчанию:

```sql
ALTER TOPIC `my_topic` ADD CONSUMER my_consumer;
```

Следующая команда добавит к топику важного читателя:

```sql
ALTER TOPIC `my_topic` ADD CONSUMER my_consumer2 WITH (important = true);
```

### Задать параметры читателя {#alter-consumer}

`ALTER CONSUMER consumer_name SET (option = value[, ...])` — действие задает параметры читателя топика.

Общий вид команды:

```sql
ALTER TOPIC topic_path ALTER CONSUMER consumer_name SET (option = value[, ...]);
```

* `option` и `value` — параметр читателя и его значение.

Следующая команда сделает читателя важным:

```sql
ALTER TOPIC `my_topic` ALTER CONSUMER my_consumer SET (important = true);
```

В одной команде может быть указано несколько `ALTER CONSUMER` действий, настройки в них не должны повторяться:

```sql
ALTER TOPIC `my_topic`
    ALTER CONSUMER my_consumer SET (important = true)
    ALTER CONSUMER my_consumer SET (read_from = 0);
```

{% if feature_topic_settings_reset %}

### Сбросить параметры читателя {#reset-consumer}

`ALTER CONSUMER consumer_name RESET (option[, ...])` — действие сбрасывает указанные настройки к значениям по умолчанию.

Общий вид команды:

```sql
ALTER TOPIC topic_path ALTER CONSUMER consumer_name RESET (option[, ...]);
```

* `option` — параметр читателя.

Данный пример сбросит параметр `read_from` в значение по умолчанию:

```sql
ALTER TOPIC `my_topic` ALTER CONSUMER my_consumer RESET (read_from);
```

{% endif %}

### Удалить читателя {#drop-consumer}

`DROP CONSUMER` — действие удаляет читателя топика.

Общий вид команды:

```sql
ALTER TOPIC topic_path DROP CONSUMER consumer_name;
```

Следующая команда удалит читателя с именем `old_consumer`:

```sql
ALTER TOPIC `my_topic` DROP CONSUMER old_consumer;
```
