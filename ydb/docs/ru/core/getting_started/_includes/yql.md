# YQL - начало работы

## Введение {#intro}

YQL - язык запросов к базе данных {{ ydb-short-name }}, диалект SQL. В частности, он обладает синтаксическими особенностями, рассчитанными на его применение при исполнении запросов на кластерах.

Полная информация по синтаксису YQL находится в [справочнике по YQL](../../yql/reference/index.md).

Приведенные ниже примеры формируют сценарий знакомства с YQL, и предполагают последовательное выполнение: запросы в разделе [Работа с данными](#dml) обращаются к данным в таблицах, созданным в разделе [Работа со схемой данных](#ddl). Выполняйте шаги последовательно, чтобы скопированные через буфер обмена примеры успешно исполнялись.

Базовый интерфейс {{ ydb-short-name }} YQL принимает на вход не одну команду, а скрипт, который может состоять из множества команд.

## Инструменты исполнения YQL {#tools}

{{ ydb-short-name }} предоставляет следующие инструменты для отправки запроса к базе данных на языке YQL:

{% include [yql/ui_prompt.md](yql/ui_prompt.md) %}

* [{{ ydb-short-name }} CLI](#cli)

* [{{ ydb-short-name }} SDK](../../reference/ydb-sdk/index.md)

{% include [yql/ui_execute.md](yql/ui_execute.md) %}

### {{ ydb-short-name }} CLI {#cli}

Для исполнения скриптов через {{ ydb-short-name }} CLI нужно предварительно:

1. [Установить {{ ydb-short-name }} CLI](../../reference/ydb-cli/install.md).
1. [Создать профиль](../../reference/ydb-cli/profile/create.md), настроенный на соединение с вашей БД.

{% include [ydb-cli-profile.md](../../_includes/ydb-cli-profile.md) %}

Текст приведенных ниже скриптов нужно сохранить в файл. Назовите его `script.yql`, чтобы команды в примерах можно было выполнить простым копированием через буфер обмена. Далее выполните команду `{{ ydb-cli }} yql` с указанием использования профиля `quickstart` и чтения скрипта из файла `script.yql`:

```bash
{{ ydb-cli }} --profile quickstart yql -f script.yql
```

## Работа со схемой данных {#ddl}

### Создание таблиц {#create-table}

Таблица с заданными колонками создается [командой YQL `CREATE TABLE`](../../yql/reference/syntax/create_table.md). В таблице обязательно должен быть определен первичный ключ. Типы данных для колонок приведены в статье [Типы данных YQL](../../yql/reference/types/index.md).

По умолчанию все колонки опциональные и могут содержать `NULL`. Для колонок, входящих в первичный ключ, можно указать ограничение `NOT NULL`. Ограничения `FOREIGN KEY` {{ ydb-short-name }} не поддерживает.

Создайте таблицы каталога сериалов: `series` (Сериалы), `seasons` (Сезоны), и `episodes` (Эпизоды), выполнив следующий скрипт:

```sql
CREATE TABLE series (
    series_id Uint64 NOT NULL,
    title Utf8,
    series_info Utf8,
    release_date Date,
    PRIMARY KEY (series_id)
);

CREATE TABLE seasons (
    series_id Uint64,
    season_id Uint64,
    title Utf8,
    first_aired Date,
    last_aired Date,
    PRIMARY KEY (series_id, season_id)
);

CREATE TABLE episodes (
    series_id Uint64,
    season_id Uint64,
    episode_id Uint64,
    title Utf8,
    air_date Date,
    PRIMARY KEY (series_id, season_id, episode_id)
);
```

Описание всех возможностей работы с таблицами приведены в разделах документации по YQL:

* [CREATE TABLE](../../yql/reference/syntax/create_table.md) — создание таблицы и определение начальных параметров.
* [ALTER TABLE](../../yql/reference/syntax/alter_table.md) — изменение состава колонок таблицы и ее параметров.
* [DROP TABLE](../../yql/reference/syntax/drop_table.md) — удаление таблицы.

Для исполнения скрипта через {{ ydb-short-name }} CLI выполните инструкции, приведенные в пункте [Исполнение в {{ ydb-short-name }} CLI](#cli) данной статьи.

### Создание колоночной таблицы {#create-olap-table}

Таблица с заданными колонками создается [командой YQL `CREATE TABLE`](../../yql/reference/syntax/create_table.md). В таблице обязательно должен быть определен первичный ключ и ключ партицирования. Допустимые для использования в аналитических таблицах типы данных описаны в разделе [Поддерживаемые типы данных колоночных таблиц](../../concepts/datamodel/table.md#olap-data-types).

Колонки, входящие в первичный ключ, должны быть описаны с ограничением `NOT NULL`. Остальные колонки по умолчанию опциональные и могут содержать `NULL`.  Ограничения `FOREIGN KEY` {{ ydb-short-name }} не поддерживает.

Создайте таблицу каталога сериалов `views` (просмотры), выполнив следующий скрипт:

```sql
CREATE TABLE views (
    series_id Uint64 NOT NULL,
    season_id Uint64,
    viewed_at Timestamp NOT NULL,
    person_id Uint64 NOT NULL,
    PRIMARY KEY (viewed_at, series_id, person_id)
)
PARTITION BY HASH(viewed_at, series_id)
WITH (
  STORE = COLUMN,
  AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 10
)
```

Описание всех возможностей работы с таблицами приведены в разделах документации по YQL:

* [CREATE TABLE](../../yql/reference/syntax/create_table.md) — создание таблицы и определение начальных параметров.
* [DROP TABLE](../../yql/reference/syntax/drop_table.md) — удаление таблицы.

Для исполнения скрипта через {{ ydb-short-name }} CLI выполните инструкции, приведенные в пункте [Исполнение в {{ ydb-short-name }} CLI](#cli) данной статьи.

### Получение перечня существующих таблиц в БД {#scheme-ls}

Проверьте, что таблицы фактически созданы в БД.

{% include [yql/ui_scheme_ls.md](yql/ui_scheme_ls.md) %}

Для получения перечня существующих таблиц в БД через {{ ydb-short-name }} CLI убедитесь, что выполнены предварительные требования пункта [Исполнение в {{ ydb-short-name }} CLI](#cli) данной статьи, и выполните команду `scheme ls`:

```bash
{{ ydb-cli }} --profile quickstart scheme ls
```

## Работа с данными {#dml}

Команды исполнения YQL запросов и скриптов в YDB CLI и web-интерфейсе работают в режиме Autocommit, то есть после успешного исполнения транзакция подтверждается автоматически.

### UPSERT — запись данных {#upsert}

Самым эффективным способом записи данных в {{ ydb-short-name }} является [команда `UPSERT`](../../yql/reference/syntax/upsert_into.md). Она выполняет запись новых данных по первичным ключам независимо от того, существовали ли данные по этим ключам ранее в таблице. В результате, в отличие от привычных `INSERT` и `UPDATE`, ее исполнение не требует на сервере предварительного чтения данных для проверки уникальности ключа. Всегда при работе с {{ ydb-short-name }} рассматривайте `UPSERT` как основной способ записи данных, используя другие команды только при необходимости.

Все команды записи данных в {{ ydb-short-name }} поддерживают работу как с выборками, так и со множеством записей, передаваемых непосредственно в запросе.

Добавим данные в созданные ранее таблицы:

```yql
UPSERT INTO series (series_id, title, release_date, series_info)
VALUES
    (
        1,
        "IT Crowd",
        Date("2006-02-03"),
        "The IT Crowd is a British sitcom produced by Channel 4, written by Graham Linehan, produced by Ash Atalla and starring Chris O'Dowd, Richard Ayoade, Katherine Parkinson, and Matt Berry."),
    (
        2,
        "Silicon Valley",
        Date("2014-04-06"),
        "Silicon Valley is an American comedy television series created by Mike Judge, John Altschuler and Dave Krinsky. The series focuses on five young men who founded a startup company in Silicon Valley."
    )
    ;

UPSERT INTO seasons (series_id, season_id, title, first_aired, last_aired)
VALUES
    (1, 1, "Season 1", Date("2006-02-03"), Date("2006-03-03")),
    (1, 2, "Season 2", Date("2007-08-24"), Date("2007-09-28")),
    (2, 1, "Season 1", Date("2014-04-06"), Date("2014-06-01")),
    (2, 2, "Season 2", Date("2015-04-12"), Date("2015-06-14"))
;

UPSERT INTO episodes (series_id, season_id, episode_id, title, air_date)
VALUES
    (1, 1, 1, "Yesterday's Jam", Date("2006-02-03")),
    (1, 1, 2, "Calamity Jen", Date("2006-02-03")),
    (2, 1, 1, "Minimum Viable Product", Date("2014-04-06")),
    (2, 1, 2, "The Cap Table", Date("2014-04-13"))
;
```

Для исполнения скрипта через {{ ydb-short-name }} CLI выполните инструкции, приведенные в пункте [Исполнение в {{ ydb-short-name }} CLI](#cli) данной статьи.

Вы можете дополнительно ознакомиться с командами записи данных в справочнике YQL:

* [INSERT](../../yql/reference/syntax/insert_into.md) — добавление записей.
* [REPLACE](../../yql/reference/syntax/replace_into.md) — добавление/изменение записей.
* [UPDATE](../../yql/reference/syntax/update.md) — изменение указанных полей.
* [UPSERT](../../yql/reference/syntax/upsert_into.md) — добавление записей/изменение указанных полей.

### SELECT — выборка данных {#select}

Запросите выборку записанных на предыдущем шаге данных:

```sql
SELECT 
    series_id,
    title AS series_title,
    release_date
FROM series;
```

или

```sql
SELECT * FROM episodes;
```

Если в скрипте YQL будет несколько команд `SELECT`, то в результате его исполнения будет возвращено несколько выборок, к каждой из которых можно обратиться отдельно. Выполните приведенные выше команды `SELECT`, объединенные в одном скрипте.

Для исполнения скрипта через {{ ydb-short-name }} CLI выполните инструкции, приведенные в пункте [Исполнение в {{ ydb-short-name }} CLI](#cli) данной статьи.

Вы можете дополнительно ознакомиться с полным описанием команд, связанных с выборкой данных, в справочнике YQL:

* [SELECT](../../yql/reference/syntax/select.md) — выполнение выборки данных.
* [SELECT ... JOIN](../../yql/reference/syntax/join.md) — соединение таблиц при выполнении выборки данных.
* [SELECT ... GROUP BY](../../yql/reference/syntax/group_by.md) — группировка данных при выполнении выборки.

### Параметризованные запросы {#param}

Для транзакционных приложений, работающих с базой данных, характерно исполнение множества однотипных запросов, отличающихся только параметрами. Как и большинство баз данных, {{ ydb-short-name }} будет работать эффективнее, если вы определите изменяемые параметры и их типы, а далее будете инициировать исполнение запроса, передавая значения параметров отдельно от его текста.

Для определения параметров в тексте запроса YQL применяется [команда DECLARE](../../yql/reference/syntax/declare.md).

Описание методов исполнения параметризованных запросов {{ ydb-short-name }} SDK доступно в разделе [Тестовый пример](../../reference/ydb-sdk/example/index.md), в секции Параметризованные запросы для нужного языка программирования.

При отладке параметризованного запроса в {{ ydb-short-name }} SDK вы можете проверить его работоспособность вызовом {{ ydb-short-name }} CLI, скопировав полный текст запроса без каких-либо корректировок, и задав значения параметров.

Сохраните скрипт выполнения параметризованного запроса в текстовом файле `script.yql`:

```sql
DECLARE $seriesId AS Uint64;
DECLARE $seasonId AS Uint64;

SELECT sa.title AS season_title, sr.title AS series_title
FROM   seasons AS sa
INNER JOIN series AS sr ON sa.series_id = sr.series_id
WHERE sa.series_id = $seriesId AND sa.season_id = $seasonId;
```

Для выполнения параметризованной выборки убедитесь, что выполнены предварительные требования пункта [Исполнение в {{ ydb-short-name }} CLI](#cli) данной статьи, и выполните следующую команду:

```bash
{{ ydb-cli }} --profile quickstart yql -f script.yql -p '$seriesId=1' -p '$seasonId=1'
```

Полное описание возможностей передачи параметров находится в [справочнике по {{ ydb-short-name }} CLI](../../reference/ydb-cli/index.md).

## Туториал YQL {#tutorial}

Вы можете изучить больше примеров использования YQL, выполняя задания из [Туториала YQL](../../yql/tutorial/index.md).
