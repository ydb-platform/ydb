# ALTER TOPIC

При помощи команды `ALTER TOPIC` можно изменить настройки [топика](../../../../concepts/topic), добавить, изменить или удалить читателей.

{% include [trunk](../../../_includes/trunk.md) %}

В общем случае команда `ALTER TOPIC` выглядит так:

```sql
ALTER TOPIC topic_path action1, action2, ..., actionN;
```

`action` — это действие по изменению , из описанных ниже.

## Изменение набора читателей

`ADD CONSUMER` — добавляет [читателей](../../../../concepts/topic#consumer) для топика.

Следующий пример добавит к топику читателя с настройками по умолчанию.

```sql
ALTER TOPIC `my_topic` ADD CONSUMER new_consumer;
```

При добавлении читателя можно указывать его настройки, например:

```sql
ALTER TOPIC `my_topic` ADD CONSUMER new_consumer2 WITH (important = false);
```

### Полный список доступных настроек читателей топика

* `important` - определяет важного читателя. Никакие данные из топика не будут удалены, пока все важные читатели их не прочитали. Тип значения - `boolean`, значение по умолчанию: `false`.
* `read_from` - определяет момент времени записи сообщений, начиная с которого читатель будет получать данные. Данные, записанные раньше этого момента прочитаны не будут. Тип значения: `Datetime` ИЛИ `Timestamp` или `integer` (unix-timestamp в виде числа). Значение по умолчанию - `now`

{% if feature_topic_codecs %}
* `supported_codecs` - список [кодеков](../../../../concepts/topic#message-codec), поддерживаемых читателем.

{% endif %}

`DROP CONSUMER` — удаляет читателя у топика.

```sql
ALTER TOPIC `my_topic` DROP CONSUMER old_consumer;
```

## Модификация настроек читателя

`ALTER CONSUMER` — добавляет читателя для топика.

Общий синтаксис действия `ALTER CONSUMER`:

```sql
ALTER TOPIC `topic_name` ALTER CONSUMER consumer_name consumer_action;
```

Поддерживаются следующие типы `consumer_action`:
* `SET` - выставляет значение настроек читателя

{% if feature_topic_settings_reset %}
* `RESET` сбрасывает указанные настройки к значениям по умолчению.

{% endif %}

Следующий пример установит читателю параметр `important`.

```sql
ALTER TOPIC `my_topic` ALTER CONSUMER my_consumer SET (important = true);
```

{% if feature_topic_settings_reset %}
Данный пример сбросит параметр `read_from` в default.

```sql
ALTER TOPIC `my_topic` ALTER CONSUMER my_consumer RESET (read_from);
```

{% endif %}

Для одного читателя может быть указано несколько `ALTER CONSUMER` действий, однако настройки в них не должны 
повторяться. 

Пример ниже - валидный запрос

```sql
ALTER TOPIC `my_topic`
    ALTER CONSUMER my_consumer SET (important = true)
    ALTER CONSUMER my_consumer SET (read_from = 0);
```

А следующий запрос приведет к ошибке.

```sql
ALTER TOPIC `my_topic`
    ALTER CONSUMER my_consumer SET (important = true)
    ALTER CONSUMER my_consumer SET (important = false);
```

## Изменение настроек топика

Действие `SET (option = value[, ...])` позволяет изменить настройки топика.

Пример ниже изменит время хранения данных в топике и квоту на скорость записи в 1 партицию: 

```sql
ALTER TOPIC `my_topic` SET (
    retention_period = Interval('PT36H'),
    partition_write_speed_bytes_per_second = 3000000
);
```

{% if feature_topic_settings_reset %}

Действие `RESET (option[, option2, ...])` позволяет сбросить настройки топика в значение по умолчанию.

**Пример**

```sql
ALTER TOPIC `my_topic` RESET (
    min_active_partitions,
    partition_count_limit    
);
```

{% endif %}

### Полный список доступных настроек топика

* `min_active_partitions` - минимальное количество активных партиций топика. Автоматическая балансировка нагрузки не будет уменьшать количество активных партиций ниже этого количества. Тип значения - `integer`, значение по умолчанию: `1`.
* `partition_count_limit` - максимальное количество активных партиций топика. `0` интерпретируется как unlimited. Тип значения - `integer`, значение по умолчанию: `0`.
* `retention_period` - время хранения данных в топике. Тип значения - `Interval`, значение по умолчанию: `18h`.
* `retention_storage_mb` - ограничение на максимальное место на диске, занимаемое данными топика. При превышении этого значения старые данные будут удаляться, как по retention. `0` интерпретируется как unlimited. Тип значения - `integer`, значение по умолчанию: `0`
* `partition_write_speed_bytes_per_second` - максимальная разрешенная скорость записи в 1 партицию. Если поток записи в партицию превысит это значение, запись будет квотироваться. Тип значения - `integer`, значение по умолчанию: `2097152` (2MB).
* `partition_write_burst_bytes` - размер "запаса" квоты на запись в партицию на случай всплесков записи. При выставлении в 0 фактическое значение write_burst принимается равным значению квоты (что позволяет всплески записи длительностью до 1 секунды). Тип значения - `integer`, значение по умолчанию: `0`.
* `metering_mode` - способ метеринга ресурсов (`RESERVED_CAPACITY` - по выделенным ресурсам или `REQUEST_UNITS` - по фактическому использованию). Актуально для топиков в serverless базах данных. Тип значения - `String`.

{% if feature_topic_codecs %}
* `supported_codecs` - список [кодеков](../../../../concepts/topic#message-codec), поддерживаемых топиком. Тип значения - `String`.

{% endif %}

