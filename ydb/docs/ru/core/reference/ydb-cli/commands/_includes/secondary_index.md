# Создание и удаление вторичных индексов

Команда `table index` позволяет создавать и удалять [вторичные индексы](../../../../concepts/secondary_indexes.md):

```bash
{{ ydb-cli }} [connection options] table index [subcommand] [options]
```

{% include [conn_options_ref.md](conn_options_ref.md) %}

Также добавить или удалить вторичный индекс можно с помощью директив [ADD INDEX и DROP INDEX](../../../../yql/reference/syntax/alter_table/secondary_index.md) операции YQL ALTER TABLE.

О назначении и применении вторичных индексов при разработке приложений можно прочитать в статье [Вторичные индексы](../../../../dev/secondary-indexes.md).

## Создание вторичного индекса {#add}

Создание вторичного индекса выполняется командой `table index add`:

```bash
{{ ydb-cli }} [connection options] table index add <sync-async> <table> \
  --index-name STR --columns STR [--cover STR]
```

Параметры:

`<sync-async>` : Тип вторичного индекса. Укажите `global-sync` для построения индекса [с синхронным обновлением](../../../../concepts/secondary_indexes.md#sync) или `global-async` для индекса [с асинхронным обновлением](../../../../concepts/secondary_indexes.md#async).

`<table>`: Путь и имя таблицы, для которой выполняется построение индекса

`--index-name STR`: Обязательный параметр, в котором задается имя индекса. Рекомендуется определять такие имена индексов, чтобы по ним можно было понять, какие колонки в них включены. Имена индексов уникальны в контексте таблицы.

`--columns STR`: Обязательный параметр, в котором определяются состав и порядок включения колонок в ключ индекса. Перечисляются имена колонок через запятую, без пробелов. Ключ индекса будет состоять из этих колонок с добавлением колонок первичного ключа таблицы.

`--cover STR`: Необязательный параметр, в котором определяется состав [покрывающих колонок](../../../../concepts/secondary_indexes.md#cover) индекса. Их значения не будут включены в состав ключа индекса, но будут скопированы в записи в индексе для получения их значений при поиске по индексу без необходимости обращения к таблице.

В результате успешного исполнения команды запускается фоновая операция построения индекса, и в выделенном псевдографикой поле `id` возвращается идентификатор операции для дальнейшего получения информации о ее статусе командой `operation get`. Незаконченное построение индекса может быть прервано командой `operation cancel`.

После того, как построение индекса завершено (успешно или прервано), запись об операции построения может быть удалена командой `operation forget`.

Получить информацию о статусе всех операций построения индекса можно командой `operation list buildindex`.

**Примеры**

{% include [ydb-cli-profile.md](../../../../_includes/ydb-cli-profile.md) %}

Добавление синхронного индекса по колонке `air_date` в [созданную ранее]({{ quickstart-path }}) таблицу `episodes`:

```bash
{{ ydb-cli }} -p quickstart table index add global-sync episodes \
  --index-name idx_aired --columns air_date
```

Добавление асинхронного индекса по колонкам `release_date` и `title`, и с копированием в индекс значения колонки `series_info`, для [созданной ранее]({{ quickstart-path }}) таблицы `series`:

```bash
{{ ydb-cli }} -p quickstart table index add global-async series \
  --index-name idx_rel_title --columns release_date,title --cover series_info
```

Вывод (id операции при фактическом запуске будет другой):

``` text
┌──────────────────────────────────┬───────┬────────┐
| id                               | ready | status |
├──────────────────────────────────┼───────┼────────┤
| ydb://buildindex/7?id=2814749869 | false |        |
└──────────────────────────────────┴───────┴────────┘
```

Получение информации о статусе операции (подставьте фактический id операции):

```bash
{{ ydb-cli }} -p quickstart operation get ydb://buildindex/7?id=281474976866869
```

Возвращаемое значение:

``` text
┌──────────────────────────────────┬───────┬─────────┬───────┬──────────┬─────────────────┬───────────┐
| id                               | ready | status  | state | progress | table           | index     |
├──────────────────────────────────┼───────┼─────────┼───────┼──────────┼─────────────────┼───────────┤
| ydb://buildindex/7?id=2814749869 | true  | SUCCESS | Done  | 100.00%  | /local/episodes | idx_aired |
└──────────────────────────────────┴───────┴─────────┴───────┴──────────┴─────────────────┴───────────┘
```

Удаление информации о построении индекса (подставьте фактический id операции):
```bash
{{ ydb-cli }} -p quickstart operation forget ydb://buildindex/7?id=2814749869
```

## Удаление вторичного индекса {#drop}

Удаление вторичного индекса выполняется командой `table index drop`:

```bash
{{ ydb-cli }} [connection options] table index drop <table> --index-name STR
```

**Пример**

{% include [ydb-cli-profile.md](../../../../_includes/ydb-cli-profile.md) %}

Удаление индекса `idx_aired` с таблицы episodes, построенного в примере создания индекса выше:

```bash
{{ ydb-cli }} -p quickstart table index drop episodes --index-name idx_aired
```

## Переименование вторичного индекса {#rename}

Переименование вторичного индекса выполняется командой `table index rename`:

```bash
{{ ydb-cli }} [connection options] table index rename <table> --index-name STR --to STR
```

Если индекс с новым именем существует, команда вернет ошибку.

Чтобы атомарно заменить существующий индекс, выполните команду переименования с параметром `--replace`:

```bash
{{ ydb-cli }} [connection options] table index rename <table> --index-name STR --to STR --replace
```

**Пример**

{% include [ydb-cli-profile.md](../../../../_includes/ydb-cli-profile.md) %}

Переименование индекса `idx_aired` с таблицы episodes, построенного в примере создания индекса выше:

```bash
{{ ydb-cli }} -p quickstart table index rename episodes --index-name idx_aired --to idx_aired_renamed
```
