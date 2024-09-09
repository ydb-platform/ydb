<!-- Этот файл не отслеживается автоматической системой перевода. Правки в EN-версию необходимо внести самостоятельно. -->

# Список изменений {{ ydb-short-name }} CLI

## Версия 2.10.0 {#2-10-0}

Дата выхода 24 июня 2024. Для обновления до версии **2.10.0** перейдите в раздел [Загрузки](downloads/index.md#ydb-cli).

**Функциональность:**

* Добавлена команда `ydb sql`, работающая поверх QueryService, позволяющая выполнять любые DML/DDL команды.
* Добавлен режим `notx` для опции `--tx-mode` в команде `ydb table query execute`.
* Добавлены времена начала и конца в описании длительных операций (export, import).
* Добавлена поддержка описания объектов типа replication в командах `ydb scheme describe` и `ydb scheme ls`.
* Добавлена поддержка типов big datetime: `Date32`, `Datetime64`, `Timestamp64`, `Interval64`.
* Переработана команда `ydb workload`:
  * Добавлена опция `--clear` в подкоманде `init`, позволяющая удалить все существующие таблицы перед созданием новых.
  * Добавлена команда `ydb workload * import` для заполнения таблиц начальным контентом перед началом нагрузки.

**Изменения с потерей обратной совместимости:**
* Переработана команда `ydb workload`:
  * Опция `--path` перемещена на уровень конкретного типа нагрузки. Например: `ydb workload tpch --path some/tables/path init ...`.
  * Значение опции `--store=s3` переименовано в `--store=external-s3` в подкоманде `init`.

**Исправления ошибок:**

* Исправлена работа с цветовыми схемами в формате `PrettyTable`

## Версия 2.9.0 {#2-9-0}

Дата выхода 25 апреля 2024. Для обновления до версии **2.9.0** перейдите в раздел [Загрузки](downloads/index.md#ydb-cli).

**Функциональность:**

* Улучшены таблицы с логическими планами запросов: стали информативнее, добавлены цвета, исправлены некоторые ошибки.
* Для команды `ydb workload` поддержана опция `-v`, включающая вывод отладочной информации.
* Добавлена возможность запустить `ydb workload tpch --store s3` с источником s3 для измерения производительности федеративных запросов.
* Добавлена опция `--rate` для команды `ydb workload` для ограничения количества транзакций (запросов) в секунду.
* Добавлена опция `--use-virtual-addressing` для импорта/экспорта s3, позволяющая переключить режим [virtual hosting of buckets](https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html) для схемы путей s3.
* Улучшена производительность команды `ydb scheme ls` параллельным запуском листингов директорий.

**Исправления ошибок:**

* Исправлено обрезание лишних символов при переносе строк в таблицах CLI
* Исправлена ошибка обращения к памяти в команде `tools restore`.
* Исправлено игнорирование опции `--timeout` в generic и scan запросах.
* Добавлен таймаут 60s на проверку версии и скачивание бинарного файла CLI для избежания бесконечного ожидания.
* Исправлен ряд незначительных ошибок: опечатки, обработка пустых файлов и т.д.

## Версия 2.8.0 {#2-8-0}

Дата выхода 12 января 2024. Для обновления до версии **2.8.0** перейдите в раздел [Загрузки](downloads/index.md#ydb-cli).

**Функциональность:**

* Добавлены команды управления конфигурациями кластера [ydb admin config](reference/ydb-cli/configs.md) и [ydb admin volatile-config](reference/ydb-cli/configs.md).

* Добавлена поддержка загрузки PostgreSQL-совместимых типов командой [ydb import file csv|tsv|json](reference/ydb-cli/export-import/import-file.md). Только для строковых таблиц.

* Добавлена поддержка загрузки директории из S3-совместимого хранилища в команде [ydb import s3](reference/ydb-cli/export-import/import-s3.md). Пока доступна только под Linux и Mac OS.

* Добавлена поддержка вывода результата выполнения команд [ydb table query execute](reference/ydb-cli/table-query-execute.md), [ydb yql](reference/ydb-cli/yql.md) и [ydb scripting yql](reference/ydb-cli/scripting-yql.md) в формате [Apache Parquet](https://parquet.apache.org/docs/).

* В командах [ydb workload](reference/ydb-cli/commands/workload/index.md) добавлена опция `--executer`, задающая используемый тип запросов.

* Добавлена колонка медианного времени выполнения бенчмарка в таблице статистики в команде [ydb workload clickbench](reference/ydb-cli/workload-click-bench.md).

* **_(Experimental)_** Добавлен тип запросов `generic` в команде [ydb table query execute](reference/ydb-cli/table-query-execute.md), позволяющий выполнять [DDL](https://ru.wikipedia.org/wiki/Data_Definition_Language) и [DML](https://ru.wikipedia.org/wiki/Data_Manipulation_Language) операции, с результатами произвольного размера и c поддержкой [MVCC](concepts/mvcc.md). Команда использует экспериментальное API, совместимость не гарантируется.

* **_(Experimental)_** В команде `ydb table query explain` добавлена опция `--collect-diagnostics` для сбора диагностики запроса и сохранения её в файл. Команда использует экспериментальное API, совместимость не гарантируется.

**Исправления ошибок:**

* Исправлена ошибка вывода таблиц в `pretty` формате с [Unicode](https://ru.wikipedia.org/wiki/Юникод) символами.

* Исправлена ошибка подстановки неправильного первичного ключа в команде [ydb tools pg-convert](postgresql/import.md#pg-convert).

## Версия 2.7.0 {#2-7-0}

Дата выхода 23 октября 2023. Для обновления до версии **2.7.0** перейдите в раздел [Загрузки](downloads/index.md#ydb-cli).

**Функциональность:**

* Добавлена команда [ydb tools pg-convert](postgresql/import.md#pg-convert), выполняющая подготовку дампа, полученного утилитой [pg_dump](https://www.postgresql.org/docs/current/app-pgdump.html), к загрузке в postgres-совместимую прослойку YDB.

* Добавлена команда нагрузочного тестирования `ydb workload query`, которая нагружает базу [запросами выполнения скрипта](reference/ydb-cli/yql.md) в несколько потоков.

* Добавлена команда для просмотра списка разрешений `ydb scheme permissions list`.

* В командах [ydb table query execute](reference/ydb-cli/table-query-execute.md), [ydb table query explain](reference/ydb-cli/commands/explain-plan.md), [ydb yql](reference/ydb-cli/yql.md) и [ydb scripting yql](reference/ydb-cli/scripting-yql.md) добавлена опция `--flame-graph`, задающая путь до файла, в котором необходимо сохранить визуализацию статистики выполнения запросов.

* [Специальные команды](reference/ydb-cli/interactive-cli.md#spec-commands) интерактивного режима выполнения запросов теперь не чувствительны к регистру.

* Добавлена валидация [специальных команд](reference/ydb-cli/interactive-cli.md#spec-commands) и их [параметров](reference/ydb-cli/interactive-cli.md#internal-vars).

* Добавлено чтение из таблицы в сценарии с транзакциями в команде [ydb workload transfer topic-to-table run](reference/ydb-cli/workload-transfer.md#run).

* Добавлена опция `--commit-messages` в команде [ydb workload transfer topic-to-table run](reference/ydb-cli/workload-transfer.md#run), задающая число сообщений в одной транзакции.

* Добавлены опции `--only-table-in-tx` и `--only-topic-in-tx` в команде [ydb workload transfer topic-to-table run](reference/ydb-cli/workload-transfer.md#run), задающие ограничения на виды запросов в одной транзакции.

* Добавлены новые колонки `Select time` и `Upsert time` в таблице статистики в команде [ydb workload transfer topic-to-table run](reference/ydb-cli/workload-transfer.md#run).

**Исправления ошибок:**

* Исправлена ошибка при загрузке пустого JSON списка командами [ydb table query execute](reference/ydb-cli/table-query-execute.md), [ydb scripting yql](reference/ydb-cli/scripting-yql.md) и [ydb yql](reference/ydb-cli/yql.md).

## Версия 2.6.0 {#2-6-0}

Дата выхода 7 сентября 2023. Для обновления до версии **2.6.0** перейдите в раздел [Загрузки](downloads/index.md#ydb-cli).

**Функциональность:**

* В [ydb workload tpch run](reference/ydb-cli/workload-tpch.md#run) добавлена опция `--path`, содержащая путь до директории с таблицами, созданными командой [ydb workload tpch init](reference/ydb-cli/workload-tpch.md#init).

* Добавлена команда [ydb workload transfer topic-to-table](reference/ydb-cli/workload-transfer.md), которая нагружает базу запросами на чтение из топиков и запись в таблицу.

* Добавлена опция `--consumer-prefix` в командax [ydb workload topic init](reference/ydb-cli/workload-topic.md#init), [ydb workload topic run read|full](reference/ydb-cli/workload-topic.md#run-read), задающая префиксы имен читателей.

* Добавлена опция `--partition-ids` в команде [ydb topic read](reference/ydb-cli/topic-read.md), задающая список id партиций топика для чтения, разделенных запятой.

* Добавлена поддержка форматов параметров CSV и TSV в командах исполнения [YQL запросов](reference/ydb-cli/parameterized-queries-cli.md).

* Переработан [интерактивный режим выполнения запросов](reference/ydb-cli/interactive-cli.md). Добавлены [новые специфичные команды интерактивного режима](reference/ydb-cli/interactive-cli.md#spec-commands): `SET stats`, `EXPLAIN`, `EXPLAIN AST`. Добавлены сохранение истории между запусками CLI и автодополнение YQL запросов.

* Добавлена команда [ydb config info](reference/ydb-cli/commands/config-info.md), которая выводит текущие параметры соединения без подключения к базе данных.

* Добавлена команда [ydb workload kv run mixed](reference/ydb-cli/workload-kv.md#mixed-kv), которая нагружает базу запросами на запись и чтение.

* Опция `--percentile` в командах [ydb workload topic run write|read|full](reference/ydb-cli/workload-topic.md#run-write) теперь может принимать вещественные значения.

* Увеличены значения по умолчанию для опций `--seconds` и `--warmup` в командах [ydb workload topic run write|read|full](reference/ydb-cli/workload-topic.md#run-write) до 60 секунд и 5 секунд соответственно.

* Изменено значение по умолчанию для опции `--supported-codecs` на `RAW` в командах [ydb topic create](reference/ydb-cli/topic-create.md) и [ydb topic consumer add](reference/ydb-cli/topic-consumer-add.md).

**Исправления ошибок:**

* Исправлена потеря строк при загрузке командой [ydb import file json](reference/ydb-cli/export-import/import-file.md).

* Исправлен неучет статистики во время прогрева команд [ydb workload topic run write|read|full](reference/ydb-cli/workload-topic.md#run-write).

* Исправлен неполный вывод статистики в командах [ydb scripting yql](reference/ydb-cli/scripting-yql.md) и [ydb yql](reference/ydb-cli/yql.md).

* Исправлен неправильный вывод progress bar'a в командах [ydb tools dump](reference/ydb-cli/export-import/tools-dump.md) и [ydb tools restore](reference/ydb-cli/export-import/tools-restore.md).

* Исправлена ошибка загрузки больших файлов с заголовком в команде [ydb import file csv|tsv](reference/ydb-cli/export-import/import-file.md).

* Исправлено зависание команды [ydb tools restore --import-data](reference/ydb-cli/export-import/tools-restore.md#optional).

* Исправлена ошибка `Unknown value Rejected` при выполнении команды [ydb operation list buildindex](reference/ydb-cli/operation-list.md).

## Версия 2.5.0 {#2-5-0}

Дата выхода 20 июня 2023. Для обновления до версии **2.5.0** перейдите в раздел [Загрузки](downloads/index.md#ydb-cli).

**Функциональность:**

* Для команды `ydb import file` добавлен параметр [--timeout](reference/ydb-cli/export-import/import-file.md#optional), задающий время, в течение которого должна быть выполнена операция на сервере.
* Добавлен индикатор прогресса в командах [ydb scheme rmdir --recursive](reference/ydb-cli/commands/dir.md#rmdir) и [ydb import file](reference/ydb-cli/export-import/import-file.md).
* Добавлена команда [ydb workload kv run read-rows](reference/ydb-cli/workload-kv.md#read-rows-kv), которая нагружает базу запросами на чтение строк, используя новый экспериментальный API вызов ReadRows (реализован только в ветке [main](https://github.com/ydb-platform/ydb)), выполняющий более быстрое чтение по ключу, чем [select](reference/ydb-cli/workload-kv.md#select-kv).
* В [ydb workload topic](reference/ydb-cli/workload-topic.md) добавлены новые параметры `--warmup-time`, `--percentile`, `--topic`, задающие время прогрева теста, процентиль в выводе статистики и имя топика соответственно.
* Добавлена команда [ydb workload tpch](reference/ydb-cli/workload-tpch.md) для запуска нагрузочного теста TPC-H.
* Добавлен флаг `--ordered` в команде [ydb tools dump](reference/ydb-cli/export-import/tools-dump.md), сохраняющий порядок по первичному ключу в таблицах.

**Производительность:**

* Увеличена скорость загрузки данных в команде `ydb import file` за счет добавления параллельной загрузки. Число потоков задается новым параметром [--threads](reference/ydb-cli/export-import/import-file.md#optional).
* Увеличена производительность команды [ydb import file json](reference/ydb-cli/export-import/import-file.md), за счет уменьшения числа копирований данных.

## Версия 2.4.0 {#2-4-0}

Дата выхода 24 мая 2023. Для обновления до версии **2.4.0** перейдите в раздел [Загрузки](downloads/index.md#ydb-cli).

**Функциональность:**

* Добавлена возможность загрузки нескольких файлов командой [ydb import file](reference/ydb-cli/export-import/import-file.md).
* Добавлена поддержка удаления колоночных таблиц для команды [ydb scheme rmdir --recursive](reference/ydb-cli/commands/dir.md#rmdir).
* Повышена стабильность работы команды [ydb workload topic](reference/ydb-cli/workload-topic.md).

## Версия 2.3.0 {#2-3-0}

Дата выхода 1 мая 2023. Для обновления до версии **2.3.0** перейдите в раздел [Загрузки](downloads/index.md#ydb-cli).

**Функциональность:**

* Добавлен интерактивный режим выполнения запросов. Для перехода в интерактивный режим выполните команду [ydb yql](reference/ydb-cli/yql.md) без аргументов. Режим экспериментальный, обратная совместимость пока не гарантируется.
* Добавлена команда [ydb index rename](reference/ydb-cli/commands/secondary_index.md#rename) для [атомарной замены](dev/secondary-indexes.md#atomic-index-replacement) или переименования вторичного индекса.
* Добавлена команда `ydb workload topic` для запуска нагрузки, которая читает и записывает сообщения в топики.
* Для команды `ydb scheme rmdir` добавлен параметр [--recursive](reference/ydb-cli/commands/dir.md#rmdir-options), который позволяет рекурсивно удалить директорию вместе со всем содержимым.
* Для команды [ydb scheme describe](reference/ydb-cli/commands/scheme-describe.md) добавлена поддержка типов `topic` и `coordination node`.
* Для команды `ydb topic consumer` добавлен параметр [--commit](reference/ydb-cli/topic-read.md#osnovnye-opcionalnye-parametry) для подтверждения прочитанных сообщений.
* Для команды `ydb import file csv|tsv` добавлен параметр [--columns](reference/ydb-cli/export-import/import-file.md#optional), с помощью которого можно указать список колонок вместо заголовка в файле.
* Для команды `ydb import file csv|tsv` добавлен параметр [--newline-delimited](reference/ydb-cli/export-import/import-file.md#optional), который подтверждает отсутствие символа переноса строки в данных. Использование этого параметра ускоряет импорт за счет параллельного чтения из нескольких секций файла.

**Исправления ошибок:**

* Исправлена ошибка, которая приводила к повышенному потреблению памяти и процессора при выполнении команды `ydb import file`.

## Версия 2.2.0 {#2-2-0}

Дата выхода 3 марта 2023. Для обновления до версии **2.2.0** перейдите в раздел [Загрузки](downloads/index.md#ydb-cli).

**Функциональность:**

* Исправлена ошибка, когда невозможно было указать поддерживаемые алгоритмы сжатия при добавлении читателя топика.
* Добавлена поддержка потокового выполнения YQL-скриптов и запросов на основе параметров, [передаваемых через `stdin`](reference/ydb-cli/parameterized-queries-cli.md).
* Значения параметров YQL-запросов теперь могут быть [переданы из файла](reference/ydb-cli/parameterized-queries-cli.md).
* Запрос на ввод пароля теперь выводится в `stderr` вместо `stdout`.
* Путь к корневому CA сертификату теперь может быть сохранен в [профиле](reference/ydb-cli/profile/index.md).
* Добавлен глобальный параметр [--profile-file](reference/ydb-cli/commands/global-options.md#service-options) для использования указанного файла в качестве хранилища для настроек профилей.
* Добавлен новый тип нагрузочного тестирования [ydb workload clickbench](reference/ydb-cli/workload-click-bench).

## Версия 2.1.1 {#2-1-1}

Дата выхода 30 декабря 2022. Для обновления до версии **2.1.1** перейдите в раздел [Загрузки](downloads/index.md#ydb-cli).

**Улучшения:**

* Добавлена поддержка параметра `--stats` команды [ydb scheme describe](reference/ydb-cli/commands/scheme-describe.md) для колоночных таблиц.
* Добавлена поддержка файлов в формате Parquet для импорта командой [ydb import](reference/ydb-cli/export-import/import-file.md).
* Поддержаны дополнительное логирование и ретраи для команды [ydb import](reference/ydb-cli/export-import/import-file.md).

## Версия 2.1.0 {#2-1-0}

Дата выхода 18 ноября 2022. Для обновления до версии **2.1.0** перейдите в раздел [Загрузки](downloads/index.md#ydb-cli).

**Функциональность:**

* Добавлена возможность [неинтерактивного создания профиля](reference/ydb-cli/profile/create.md#cmdline).
* Добавлены команды [ydb config profile update](reference/ydb-cli/profile/create.md#update) и [ydb config profile replace](reference/ydb-cli/profile/create.md#replace) для изменения и замены профилей.
* Для команды [ydb scheme ls](reference/ydb-cli/commands/scheme-ls.md) добавлен параметр `-1`, включающая режим вывода по одному объекту на строку.
* URL сервиса IAM теперь можно сохранять в профиле.
* Добавлена возможность использовать аутентификацию по логину и паролю без указания пароля.
* Добавлена поддержка профилей AWS в команде [ydb export s3](reference/ydb-cli/export-import/auth-s3.md#auth).
* Добавлена возможность создания профиля используя `stdin`. Например, можно передать вывод команды [YC CLI](https://cloud.yandex.ru/docs/cli/) `yc ydb database get information` на вход команде `ydb config profile create`.

**Исправления ошибок:**

* Исправлена ошибка, когда некорректно выводился результат запроса в формате JSON-array, если он состоял из нескольких ответов сервера.
* Исправлена ошибка, приводящая к невозможности изменить профиль, используя при этом некорректный профиль.

## Версия 2.0.0 {#2-0-0}

Дата выхода 20 сентября 2022. Для обновления до версии **2.0.0** перейдите в раздел [Загрузки](downloads/index.md#ydb-cli).

**Функциональность:**

* Добавлена возможность работы с топиками:
  * `ydb topic create` — создание топика;
  * `ydb topic alter` — изменение топика;
  * `ydb topic write` — запись данных в топик;
  * `ydb topic read` — чтение данных из топика;
  * `ydb topic drop` — удаление топика.

* Добавлен новый тип нагрузочного тестирования:
  * `ydb workload kv init` — создание таблицы для тестирования kv нагрузки;
  * `ydb workload kv run` — запуск одной из 3 видов нагрузки: запуск нескольких сессий вставки `UPSERT`, запуск нескольких сессий вставки `INSERT` или запуск нескольких сессий с GET-запросами по первичному ключу;
  * `ydb workload kv clean` — удаление тестовой таблицы.

* Добавлена возможность деактивировать текущий активный профиль (см. команду `ydb config profile deactivate`).
* Добавлена возможность неинтерактивного удаления профиля без подтверждения (см. параметр `--force` команды `ydb config profile remove`).
* Добавлена поддержка CDC для команды `ydb scheme describe`.
* Добавлена возможность просмотра текущего статуса БД (см. команду `ydb monitoring healthcheck`).
* Добавлена возможность просмотра аутентификационной информации (токена), с которой будут отправляться запросы к БД при текущих настройках аутентификации (см. команду `ydb auth get-token`).
* Добавлена возможность чтения данных из стандартного потока ввода для команды `ydb import`.
* Добавлена возможность импорта данных в формате JSON из файла или стандартного потока ввода (см. команду `ydb import file json`).

**Улучшения:**

* Улучшен процессинг команд. Парсинг и валидация пользовательского ввода теперь более точные.

## Версия 1.9.1 {#1-9-1}

Дата выхода 25 июня 2022. Для обновления до версии **1.9.1** перейдите в раздел [Загрузки](downloads/index.md#ydb-cli).

**Функциональность:**

* Добавлена возможность сжатия данных при экспорте в S3-совместимое хранилище (см. параметр `--compression` команды [ydb export s3](reference/ydb-cli/export-import/export-s3.md)).
* Добавлена возможность управления автоматической проверкой доступности новой версии {{ ydb-short-name }} CLI (см. параметры `--disable-checks` и `--enable-checks` команды [ydb version](reference/ydb-cli/version.md)).
