# Сброс позиции чтения

Каждый читатель топика обладает [позицией чтения](../../concepts/datamodel/topic.md#consumer-offset).

С помощью команды `topic consumer offset reset` можно сбросить сохранённую позицию чтения [добавленного ранее](topic-consumer-add.md) читателя **во всех** партициях топика, включая неактивные партиции после split или merge.

Команда поддерживается начиная с версии сервера {{ ydb-short-name }} **27.1**.

Общий вид команды:

```bash
{{ ydb-cli }} [global options...] topic consumer offset reset [options...] <topic-path>
```

* `global options` — [глобальные параметры](commands/global-options.md).
* `options` — [параметры подкоманды](#options).
* `topic-path` — путь топика.

Посмотреть описание команды:

```bash
{{ ydb-cli }} topic consumer offset reset --help
```

При успешном выполнении команда печатает `OK`. Если сбросить смещение удалось не во всех партициях, команда печатает issues со списком идентификаторов неуспешных партиций.

## Параметры подкоманды {#options}

Имя | Описание
---|---
`--consumer <значение>` | Имя читателя.
`--position <значение>` | Целевая позиция: `earliest`, `latest` или метка времени. Метка времени может быть задана в unix time (секунды с 1970.01.01) или в формате ISO-8601 (например, `2020-07-10T15:00:00Z`).

## Примеры {#examples}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

Сбросить читателя `my-consumer` на начало топика `my-topic`:

```bash
{{ ydb-cli }} -p db1 topic consumer offset reset \
  --consumer my-consumer \
  --position earliest \
  my-topic
```
