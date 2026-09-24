# DROP TOPIC

С помощью оператора `DROP TOPIC` можно удалить [топик](../../../concepts/datamodel/topic.md).

Обозначения в блоке синтаксиса описаны в [{#T}](syntax-conventions.md).

## Синтаксис

```yql
DROP TOPIC [IF EXISTS] <topic_path>
```

### Параметры

* `IF EXISTS` — если топика с указанным именем нет, команда не возвращает ошибку.
* `<topic_path>` — путь удаляемого топика. Допускается запись в виде абсолютного пути или имени в текущей базе данных.

## Примеры

Следующая команда удалит топик с именем `my_topic`:

```yql
DROP TOPIC `my_topic`;
```

Если топик может отсутствовать:

```yql
DROP TOPIC IF EXISTS `my_topic`;
```

## См. также

* [CREATE TOPIC](create-topic.md)
* [ALTER TOPIC](alter-topic.md)
