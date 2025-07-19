# Расширенное партицирование

[Партицирование](partitioning.md) позволяет подсказать для {{ydb-full-name}} правила размещения данных в S3 ({{objstorage-full-name}}).

Предположим, что данные в S3 ({{objstorage-full-name}}) хранятся в следующей структуре каталогов:

```text
year=2021
    month=01
    month=02
    month=03
year=2022
    month=01
```

При выполнении запроса ниже {{ydb-full-name}} выполнит следующие действия:

1. Получит полный список подкаталогов внутри '/'.
1. Для каждого подкаталога выполнит попытку обработать имя подкаталога в формате `year=<DIGITS>`.
1. Для каждого подкаталога `year=<DIGITS>` получит список всех подкаталогов в формате `month=<DIGITS>`.
1. Обработает считанные данные.

```yql
SELECT
    *
FROM
    objectstorage.'/'
WITH
(
    FORMAT = "csv_with_names",
    SCHEMA =
    (
        data String,
        year Int32 NOT NULL,
        month Int32 NOT NULL
    ),
    PARTITIONED_BY = (year, month)
)
WHERE
    year=2021
    AND month=02
```

То есть при работе с партицированными данными выполняется полный листинг содержимого S3 ({{objstorage-full-name}}), что может занимать продолжительное время на бакетах большого размера.

Для оптимизации работы на больших объемах данных следует использовать "расширенное партицирование". В этом режиме не происходит сканирования каталогов S3 ({{ objstorage-full-name }}), вместо этого все пути вычисляются заранее и обращение происходит только к ним.

Для работы расширенного партицирования необходимо задать правила работы через специальный параметр - "projection". В этом параметре описываются все правила размещения данных в каталогах S3 ({{ objstorage-full-name}}).

## Синтаксис для внешних источников данных {#syntax-external-data-source}

Расширенное партицирование называется "partition projection" и на уровне [внешних источников данных](../../datamodel/external_data_source.md) задается через параметр `projection` в формате JSON. В общем виде настройка расширенного партицирования для [внешних источников данных](../../datamodel/external_data_source.md) выглядит следующим образом:

```yql
SELECT
    *
FROM
    <object_storage_external_datasource_name>.<path>
WITH
(
    SCHEMA =
    (
        <field1> <type1>,
        <field2> <type2> NOT NULL,
        <field3> <type3> NOT NULL
    ),
    PARTITIONED_BY = (field2, field3),
    PROJECTION = @@ {
        "projection.enabled": <"true"|"false">,

        "projection.<field2>.type": "<type>",
        "projection.<field2>....": "<extended_properties>",

        "projection.<field3>.type": "<type>",
        "projection.<field3>....": "<extended_properties>",

        "storage.location.template": ".../${<field3>}/${<field2>}/..."
    } @@,
    <format_settings>
)
WHERE
    <filter>
```

Пример указания расширенного партицирования:

```yql
SELECT
    *
FROM
    objectstorage.`/`
WITH
(
    FORMAT = "csv_with_names",
    SCHEMA =
    (
        data String,
        year Int32 NOT NULL,
        month Int32 NOT NULL
    ),
    PARTITIONED_BY = (year, month),
    PROJECTION = @@ {
        "projection.enabled": "true",

        "projection.year.type": "integer",
        "projection.year.min": "2010",
        "projection.year.max": "2022",
        "projection.year.interval": "1",

        "projection.month.type": "integer",
        "projection.month.min": "1",
        "projection.month.max": "12",
        "projection.month.interval": "1",
        "projection.month.digits": "2",

        "storage.location.template": "${year}/${month}"
    } @@
)
WHERE
    year=2021
    AND month=02
```

В примере выше указывается, что данные существуют за каждый год и каждый месяц с 2010 по 2022 годы, при этом в бакете данные размещены в каталогах вида `2022/12`. Если данные за какой-то период отсутствуют внутри бакета, то это не приводит к ошибкам, запрос выполнится успешно, а данные будут пропущены в расчетах.

## Синтаксис для внешних таблиц {#syntax-external-table}

Рекомендованным способом работы с расширенным партицированием данных является использование [внешних таблиц](../../datamodel/external_table.md). Для них можно перечислить список настроек «partition projection» при создании таблицы. В общем виде синтаксис создания внешних таблиц с настройками расширенного партицирования выглядит следующим образом:

```yql
CREATE EXTERNAL TABLE <external_table> (
    <field1> <type1>,
    <field2> <type2> NOT NULL,
    <field3> <type3> NOT NULL
) WITH (
    DATA_SOURCE = "<object_storage_external_datasource_name>",
    LOCATION = "<path>",
    PARTITIONED_BY = "['<field2>', '<field3>']",
    `projection.enabled` = <"true"|"false">,

    `projection.<field2>.type` = "<type>",
    `projection.<field2>....` = "<extended_properties>",

    `projection.<field3>.type` = "<type>",
    `projection.<field3>....` = "<extended_properties>",

    `storage.location.template` = ".../${<field3>}/${<field2>}/...",

    <format_settings>
);
```

Пример указания расширенного партицирования при создании [внешней таблицы](../../datamodel/external_table.md):

```yql
CREATE EXTERNAL TABLE `objectstorage_data` (
    data String,
    year Int32 NOT NULL,
    month Int32 NOT NULL
) WITH (
    DATA_SOURCE = "objectstorage",
    LOCATION = "/",
    FORMAT = "csv_with_names",
    PARTITIONED_BY = "['year', 'month']",
    `projection.enabled` = "true",

    `projection.year.type` = "integer",
    `projection.year.min` = "2010",
    `projection.year.max` = "2022",
    `projection.year.interval` = "1",

    `projection.month.type` = "integer",
    `projection.month.min` = "1",
    `projection.month.max` = "12",
    `projection.month.interval` = "1",
    `projection.month.digits` = "2",

    `storage.location.template` = "${year}/${month}"
);
```

Приведённая внешняя таблица задаёт расширенное партицирование так же, как внешний источник данных, описанный [выше](#syntax-external-data-source). Далее вы можете читать данные таблицы `objectstorage_data` с фильтрацией по колонкам `year` и `month`, аналогично случаю с [обычным партицированием](partitioning.md#syntax-external-table):

```yql
SELECT
    *
FROM
    `objectstorage_data`
WHERE
    year=2021
    AND month=02
```

## Описание полей {#field_types}

|Название поля|Описание поля|Допустимые значения|
|----|----|----|
|`projection.enabled`|Включено или нет расширенное партицирование| `true`, `false`|
|`projection.<field1_name>.type`|Тип данных поля|`integer`, `enum`, `date`|
|`projection.<field1_name>.XXX`|Свойства конкретного типа||

### Поле типа integer {#integer_type}

Используется для колонок, чьи значения можно представить в виде целых чисел диапазона 2^-63^ до 2^63^-1.

|Название поля|Обязательное|Описание поля|Пример значений|
|----|----|----|----|
|`projection.<field_name>.type`|Да|Тип данных поля|integer|
|`projection.<field_name>.min`|Да|Определяет минимально допустимое значение. Задается в виде целого числа|-100<br/>004|
|`projection.<field_name>.max`|Да|Определяет максимально допустимое значение. Задается в виде целого числа|-10<br/>5000|
|`projection.<field_name>.interval`|Нет, по умолчанию `1`|Определяет шаг между элементами внутри диапазона значений. Например, шаг 3 при диапазоне значений 2, 10, приведет к следующим значениям: 2, 5, 8|2<br/>11|
|`projection.<field_name>.digits`|Нет, по умолчанию `0`|Определяет количество цифр в числе. Если ненулевых цифр в числе меньше, чем указанное значение, то значение дополняется нулями спереди вплоть до числа указанного числа цифр. Например, если указано значение .digits=3, а передается число 2, то оно будет превращено в значение 002|2<br/>4|

### Поле типа enum {#enum_type}

Используется для колонок, чьи значения можно представить в виде перечисления значений.

|Название поля|Обязательное|Описание поля|Пример значений|
|----|----|----|----|
|`projection.<field_name>.type`|Да|Тип данных поля|enum|
|`projection.<field_name>.values`|Да|Определяет допустимые значения, указанные через запятую. Пробелы не игнорируются|1, 2<br/>A,B,C|

### Поле типа date {#date_type}

Используется для колонок, чьи значения можно представить в виде даты. Допустимый диапазон дат с 1970-01-01 до 2105-01-01.

|Название поля|Обязательное|Описание поля|Пример значений|
|----|----|----|----|
|`projection.<field_name>.type`|Да|Тип данных поля|date|
|`projection.<field_name>.min`|Да|Определяет минимально допустимую дату. Разрешены значения в формате `YYYY-MM-DD` или в виде выражения, содержащего специальную макроподстановку NOW|2018-01-01<br/>NOW-10DAYS<br/>NOW-3HOURS|
|`projection.<field_name>.max`|Да|Определяет максимально допустимую дату. Разрешены значения в формате `YYYY-MM-DD` или в виде выражения, содержащего специальную макроподстановку NOW|2020-01-01<br/>NOW-5DAYS<br/>NOW+3HOURS|
|`projection.<field_name>.format`|Да|Строка форматирования даты на основе [strptime](https://cplusplus.com/reference/ctime/strftime/)|%Y-%m-%d<br/>%D|
|`projection.<field_name>.unit`|Нет, по умолчанию `DAYS`|Единицы измерения интервалов времени. Допустимые значения: `YEARS`, `MONTHS`, `WEEKS`, `DAYS`, `HOURS`, `MINUTES`, `SECONDS`, `MILLISECONDS`|SECONDS<br/>YEARS|
|`projection.<field_name>.interval`|Нет, по умолчанию `1`|Определяет шаг между элементами внутри диапазона значений с размерностью, указанной в `projection.<field_name>.unit`. Например, для диапазона 2021-02-02, 2021-03-05 шаг 15 c размерностью DAYS приведет к значениям: 2021-02-17, 2021-03-04|2<br/>6|

## Работа с макроподстановкой NOW

1. Поддерживается ряд арифметических операция с макроподстановкой NOW: прибавление интервала времени и вычитание интервала времени. Например: `NOW-3DAYS`, `NOW+1MONTH`, `NOW-6YEARS`, `NOW + 4HOURS`, `NOW - 5MINUTES`, `NOW + 6SECONDS`. Возможные варианты использования макроподстановки описываются регулярным выражением: `^\s*(NOW)\s*(([\+\-])\s*([0-9]+)\s*(YEARS?|MONTHS?|WEEKS?|DAYS?|HOURS?|MINUTES?|SECONDS?)\s*)?$`
1. Допустимые размерности интервалов: YEARS, MONTHS, WEEKS, DAYS, HOURS, MINUTES, SECONDS, MILLISECONDS.
1. В выражениях возможно использовать только одно арифметическое действие, выражения вида `NOW-5MINUTES+6SECONDS` не поддерживаются.
1. Работа с интервалами всегда приводит к получению корректной даты, но в зависимости от размерности, итоговые результаты могут быть разными:

   - При прибавлении `MONTHS` к дате происходит добавление календарного месяца, а не фиксированного числа дней. Например, если текущая дата - `2023-01-31`, то прибавление `1 MONTHS` приведет к получению даты `2023-02-28`.
   - При прибавлении `30 DAYS` к дате происходит добавление фиксированного числа дней. Например, если текущая дата - `2023-01-31`, то прибавление `30 DAYS` приведет к получению даты `2023-03-02`.
   - Минимальная возможная дата - `1970-01-01` года (время 0 в [Unix time](https://en.wikipedia.org/wiki/Unix_time)). Если в результате вычислений возникает дата меньше минимальной, то весь запрос завершается с ошибкой.
   - Максимальная возможная дата - `2105-12-31` года (максимальная дата в [Unix time](https://en.wikipedia.org/wiki/Unix_time)). Если в результате вычислений возникает дата больше максимальной, то весь запрос завершается с ошибкой.

## Шаблоны путей {#storage_location_template}

Данные в бакетах S3 ({{ objstorage-full-name }}) могут быть размещены в каталогах с произвольными названиями. С помощью настройки `storage.location.template` можно указать правила именования каталогов, где хранятся данные.

|Название поля|Описание поля|Пример значений|
|----|----|----|
|`storage.location.template`|Шаблон пути имен каталогов. Путь задается в виде текстовой строки с макроподстановками параметров `...${<field_name>}...${<field_name>}...`|`root/a/${year}/b/${month}/d`<br/>`${year}/${month}`|

Если в пути находится знак `$`, `\` или знаки `{}`, то их необходимо экранировать с помощью символа `\`. Например, для работы с каталогом с именем `my$folder` необходимо указать путь следующим образом: `my\$folder`.
