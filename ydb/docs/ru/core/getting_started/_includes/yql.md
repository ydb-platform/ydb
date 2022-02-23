# YQL - Начало работы

## Введение {#intro}

YQL - язык запросов к базе данных YDB, диалект SQL. В частности, он обладает синтаксическими особенностями, рассчитанными на его применение при исполнении запросов на кластерах.

YDB предоставляет следующие способы отправки запроса к базе данных на языке YQL:

{% include [yql/ui_prompt.md](yql/ui_prompt.md) %}

- [YDB CLI](#cli)
- [YDB SDK](../sdk.md)

Полная информация по синтаксису YQL находится в [справочнике по YQL](../../yql/reference/index.md).

Приведенные ниже примеры формируют сценарий знакомства с YQL, и предполагают последовательное выполнение: запросы в разделе ["Работа с данными"](#dml) обращаются к данным в таблицах, созданным в разделе ["Работа со схемой данных"](#ddl). Выполняйте шаги последовательно, чтобы скопированные через буфер обмена примеры успешно исполнялись.

Базовый интерфейс YDB YQL принимает на вход не одну команду, а скрипт, который может состоять из множества команд.

{% include [yql/ui_execute.md](yql/ui_execute.md) %}

### Исполнение YQL в YDB CLI {#cli}

Для исполнения скриптов через YDB CLI нужно предварительно:
- Выполнить [установку CLI](../cli.md#install)
- Определить и проверить [параметры соединения с БД](../cli#scheme-ls)
- [Создать профиль `db1`](../cli.md#profile), настроенный на соединение с вашей БД.

Текст приведенных ниже скриптов нужно сохранить в файл. Назовите его `script.yql`, чтобы команды в примерах можно было выполнить простым копированием через буфер обмена. Далее выполните команду `scripting yql` с указанием использования профиля `db1` и чтения скрипта из файла `script.yql`:

``` bash
{{ ydb-cli }} --profile db1 scripting yql -f script.yql
```

## Работа со схемой данных {#ddl}

### Создание таблицы {#create-table}

Таблица с заданными колонками создается [командой YQL `CREATE TABLE`](../../yql/reference/syntax/create_table.md). В таблице обязательно должен быть определен первичный ключ. Типы данных для колонок приведены в статье [Типы данных YQL](../../yql/reference/types/index.md).

YDB в настоящее время не поддерживает ограничение `NOT NULL`, все колонки будут допускать отсутствие значений, включая колонки первичного ключа. YDB также не поддерживает ограничения `FOREIGN KEY`.

Создайте таблицы каталога сериалов: `series` (Сериалы), `seasons` (Сезоны), и `episodes` (Эпизоды), выполнив следующий скрипт:

```sql
CREATE TABLE series (
    series_id Uint64,
    title Utf8,
    series_info Utf8,
    release_date Uint64,
    PRIMARY KEY (series_id)
);

CREATE TABLE seasons (
    series_id Uint64,
    season_id Uint64,
    title Utf8,
    first_aired Uint64,
    last_aired Uint64,
    PRIMARY KEY (series_id, season_id)
);

CREATE TABLE episodes (
    series_id Uint64,
    season_id Uint64,
    episode_id Uint64,
    title Utf8,
    air_date Uint64,
    PRIMARY KEY (series_id, season_id, episode_id)
);
```

Описание всех возможностей работы с таблицами приведены в разделах документаци по YQL:

- [`CREATE TABLE`](../../yql/reference/syntax/create_table) - создание таблицы и определение начальных параметров
- [`ALTER TABLE`](../../yql/reference/syntax/alter_table) - изменение состава колонок таблицы и её параметров
- [`DROP TABLE`](../../yql/reference/syntax/drop_table) - удаление таблицы

Для исполнения скрипта через YDB CLI выполните инструкции, приведенные в пункте ["Исполнение в YDB CLI"](#cli) данной статьи.

### Получение перечня существующих таблиц в БД {#scheme-ls}

Проверьте, что таблицы фактически созданы в БД.

{% include [yql/ui_scheme_ls.md](yql/ui_scheme_ls.md) %}

Для получения перечня существующих таблиц в БД через YDB CLI убедитесь, что выполнены предварительные требования пункта ["Исполнение в YDB CLI"](#cli) данной статьи, и выполните [команду `scheme ls`](../cli.md#ping):

``` bash
{{ ydb-cli }} --profile db1 scheme ls
```

## Работа с данными {#dml}

{% include [yql/ui_dml_autocommit.md](yql/ui_dml_autocommit.md) %}

### UPSERT : Запись данных {#upsert}

Самым эффективным способом записи данных в YDB является нестандартная для SQL [команда `UPSERT`](../../yql/reference/syntax/upsert_into.md). Она выполняет запись новых данных по первичным ключам независимо от того, существовали ли данные по этим ключам ранее в таблице. В результате, в отличие от привычных `INSERT` и `UPDATE`, её исполнение не требует на сервере предварительного чтения данных для проверки уникальности ключа. Всегда при работе с YDB рассматривайте `UPSERT` как основной способ записи данных, используя другие команды только при необходимости.

Все команды записи данных в YDB поддерживают работу как с выборками, так и со множеством записей, передаваемых непосредственно с запросе.

Добавим данные в созданные ранее таблицы:

``` yql
UPSERT INTO series (series_id, title, release_date, series_info)
VALUES
    (
        1,
        "IT Crowd",
        CAST(Date("2006-02-03") AS Uint64),
        "The IT Crowd is a British sitcom produced by Channel 4, written by Graham Linehan, produced by Ash Atalla and starring Chris O'Dowd, Richard Ayoade, Katherine Parkinson, and Matt Berry."),
    (
        2,
        "Silicon Valley",
        CAST(Date("2014-04-06") AS Uint64),
        "Silicon Valley is an American comedy television series created by Mike Judge, John Altschuler and Dave Krinsky. The series focuses on five young men who founded a startup company in Silicon Valley."
    )
    ;

UPSERT INTO seasons (series_id, season_id, title, first_aired, last_aired)
VALUES
    (1, 1, "Season 1", CAST(Date("2006-02-03") AS Uint64), CAST(Date("2006-03-03") AS Uint64)),
    (1, 2, "Season 2", CAST(Date("2007-08-24") AS Uint64), CAST(Date("2007-09-28") AS Uint64)),
    (2, 1, "Season 1", CAST(Date("2014-04-06") AS Uint64), CAST(Date("2014-06-01") AS Uint64)),
    (2, 2, "Season 2", CAST(Date("2015-04-12") AS Uint64), CAST(Date("2015-06-14") AS Uint64))
;

UPSERT INTO episodes (series_id, season_id, episode_id, title, air_date)
VALUES
    (1, 1, 1, "Yesterday's Jam", CAST(Date("2006-02-03") AS Uint64)),
    (1, 1, 2, "Calamity Jen", CAST(Date("2006-02-03") AS Uint64)),
    (2, 1, 1, "Minimum Viable Product", CAST(Date("2014-04-06") AS Uint64)),
    (2, 1, 2, "The Cap Table", CAST(Date("2014-04-13") AS Uint64))
;
```
Для исполнения скрипта через YDB CLI выполните инструкции, приведенные в пункте ["Исполнение в YDB CLI"](#cli) данной статьи.

### SELECT : Выборка данных {#select}

Запросите выборку записанных на предыдущем шаге данных: 

```sql
SELECT 
    series_id,
    title AS series_title,
    DateTime::ToDate(DateTime::FromDays(release_date)) AS release_date
FROM series;
```
или
```sql
SELECT * FROM episodes;
```

Если в скрипте YQL будет несколько команд `SELECT`, то в результате его исполнения будет возвращено несколько выборок, к каждой из которых можно обратиться отдельно. Выполните приведенные выше команды `SELECT`, объединенные в одном скрипте.

Для исполнения скрипта через YDB CLI выполните инструкции, приведенные в пункте ["Исполнение в YDB CLI"](#cli) данной статьи.

### Параметризованные запросы {#param}

Для транзакционных приложений, работающих с базой данных, характерно исполнение множества однотипных запросов, отличающихся только параметрами. Как и большинство баз данных, YDB будет работать эффективнее, если вы определите изменяемые параметры и их типы, а далее будете инициировать исполнение запроса, передавая значения параметров отдельно от его текста.

Для определения параметров в тексте запроса YQL применяется [команда DECLARE](../../yql/reference/syntax/declare.md).

Описание методов исполнения параметризованных запросов YDB SDK доступно в разделе [Тестовый пример](../../reference/ydb-sdk/example/index.md), в секции "Параметризованные запросы" для нужного языка программирования.

При отладке параметризованного запроса в YDB SDK вы можете проверить его работоспособность вызовом YDB CLI, скопировав полный текст запроса без каких-либо корректировок, и задав значения параметров.

Сохраните скрипт выполнения параметрированного запроса в текстовом файле `script.yql`:

``` sql
DECLARE $seriesId AS Uint64;
DECLARE $seasonId AS Uint64;

SELECT sa.title AS season_title, sr.title AS series_title
FROM   seasons AS sa
INNER JOIN series AS sr ON sa.series_id = sr.series_id
WHERE sa.series_id = $seriesId AND sa.season_id = $seasonId;
```

Для выполнения параметризованной выборки убедитесь, что выполнены предварительные требования пункта ["Исполнение в YDB CLI"](#cli) данной статьи, и выполните следующую команду:

``` bash
$ {{ ydb-cli }} --profile db1 scripting yql -f script.yql -p '$seriesId=1' -p '$seasonId=1'
```

Полное описание возможностей передачи параметров находится в [справочнике по YDB CLI](../../reference/ydb-cli/index.md).

## Туториал YQL {#tutorial}

Вы можете изучить больше примеров использования YQL, выполняя задания из [Туториала YQL](../../yql/tutorial/index.md).

## Продолжение знакомства с YDB {#next}

Перейдите к статье [YDB SDK - Начало работы](../sdk.md) для продолжения знакомства с YDB.