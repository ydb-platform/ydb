# Копирование таблиц

С помощью подкоманды `tools copy` вы можете создать копию таблицы или нескольких таблиц БД. При копировании исходная таблица остается на месте, копия содержит все данные исходной таблицы.

Общий вид команды:

```bash
{{ ydb-cli }} [global options...] tools copy [options...]
```

* `global options` — [глобальные параметры](commands/global-options.md).
* `options` — [параметры подкоманды](#options).

Посмотрите описание команды для копирования таблицы:

```bash
{{ ydb-cli }} tools copy --help
```

## Параметры подкоманды {#options}

Имя параметра | Описание параметра
---|---
`--timeout` | Время, в течение которого должна быть выполнена операция на сервере.
`--item <свойство>=<значение>,...` | Свойства операции. Параметр может быть указан несколько раз, если необходимо выполнить копирование нескольких таблиц в одной транзакции.<br/>Обязательные свойства:<ul><li>`destination`, `dst`, `d` —  путь таблицы-назначения. Если путь назначения содержит директории, они должны быть созданы заранее. Таблица с именем назначения не должна существовать.</li><li>`source`, `src`, `s` — путь таблицы-источника.</li></ul>

## Примеры {#examples}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

Создайте в базе данных директорию `backup`:

```bash
{{ ydb-cli }} -p quickstart scheme mkdir backup
```

Скопируйте таблицу `series` в таблицу `series-v1`, таблицу `seasons` в `seasons-v1`, таблицу `episodes` в `episodes-v1` директории `backup`:

```bash
{{ ydb-cli }} -p quickstart tools copy --item destination=backup/series-v1,source=series --item destination=backup/seasons-v1,source=seasons --item destination=backup/episodes-v1,source=episodes
```

Посмотрите листинг объектов директории `backup`:

```bash
{{ ydb-cli }} -p quickstart scheme ls backup
```

Результат:

```text
episodes-v1  seasons-v1  series-v1
```
