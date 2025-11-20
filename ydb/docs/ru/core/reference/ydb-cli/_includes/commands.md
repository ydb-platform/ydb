# Команды {{ ydb-short-name }} CLI

Общий синтаксис вызова команд {{ ydb-short-name }} CLI:

``` bash
{{ ydb-cli }} [global options] <command> [<subcommand> ...] [command options]
```

, где:

- `{{ ydb-cli}}` - команда запуска {{ ydb-short-name }} CLI из командной строки операционной системы
- `[global options]` - [глобальные опции](../commands/global-options.md), одинаковые для всех команд {{ ydb-short-name }} CLI
- `<command>` - команда
- `[<subcomand>  ...]` - подкоманды, указываемые в случае если выбранная команда содержит подкоманды
- `[command options]` - опции команды, специфичные для каждой команды и подкоманд

## Команды {#list}

Вы можете ознакомиться с нужными командами выбрав тематический раздел в меню слева, или воспользовавшись алфавитным перечнем ниже.

Любая команда может быть вызвана в командной строке с опцией `--help` для получения справки по ней. Перечень всех поддерживаемых {{ ydb-short-name }} CLI команд может быть получен запуском {{ ydb-short-name }} CLI с опцией `--help` [без указания команды](../commands/service.md).

Команда / подкоманда | Краткое описание
--- | ---
admin cluster bootstrap | Инициализация подготовленного [кластера](../../../concepts/glossary.md#cluster)
[admin cluster bridge failover](../commands/bridge/failover.md) | Аварийное переключение при недоступности [pile](../../../concepts/glossary.md#pile)
[admin cluster bridge list](../commands/bridge/list.md) | Список состояния каждого [pile](../../../concepts/glossary.md#pile) в [режиме bridge](../../../concepts/bridge.md)
[admin cluster bridge rejoin](../commands/bridge/rejoin.md) | Возвращение [pile](../../../concepts/glossary.md#pile) в [кластер](../../../concepts/glossary.md#cluster) после обслуживания или восстановления
[admin cluster bridge switchover](../commands/bridge/switchover.md) | Плановая смена `PRIMARY` [pile](../../../concepts/glossary.md#pile)
[admin cluster bridge takedown](../commands/bridge/takedown.md) | Вывод [pile](../../../concepts/glossary.md#pile) из [кластера](../../../concepts/glossary.md#cluster) для обслуживания
[admin cluster config fetch](../commands/configuration/cluster/fetch.md) | Получение текущей динамической конфигурации [кластера](../../../concepts/glossary.md#cluster)
[admin cluster config generate](../commands/configuration/cluster/generate.md) | Генерация динамической конфигурации из статической конфигурации запуска
[admin cluster config replace](../commands/configuration/cluster/replace.md) | Замена динамической конфигурации [кластера](../../../concepts/glossary.md#cluster)
admin cluster config resolve | Вычисление итоговой динамической конфигурации [кластера](../../../concepts/glossary.md#cluster) на основе базовой конфигурации и селекторов переопределения
admin cluster config version | Отображение версии конфигурации [кластера](../../../concepts/glossary.md#cluster) на узлах
[admin cluster dump](../export-import/tools-dump.md#cluster) | Выгрузка метаданных кластера в файловую систему
[admin cluster restore](../export-import/tools-restore.md#cluster) | Восстановление метаданных кластера из файловой системы
admin database config fetch | Получение текущей динамической конфигурации [базы данных](../../../concepts/glossary.md#database)
admin database config generate | Генерация динамической конфигурации [базы данных](../../../concepts/glossary.md#database) из статической конфигурации запуска
admin database config replace | Замена динамической конфигурации [базы данных](../../../concepts/glossary.md#database)
admin database config resolve | Вычисление итоговой динамической конфигурации [базы данных](../../../concepts/glossary.md#database) на основе базовой конфигурации и селекторов переопределения
admin database config version | Отображение версии конфигурации [базы данных](../../../concepts/glossary.md#database)
[admin database dump](../export-import/tools-dump.md#db) | Выгрузка метаданных и данных базы данных в файловую систему
[admin database restore](../export-import/tools-restore.md#db) | Восстановление метаданных и данных базы данных из файловой системы
[admin node config init](../commands/configuration/node/init.md) | Инициализация конфигурации [узла](../../../concepts/glossary.md#node)
auth get-token | Получение [аутентификационного токена](../../../concepts/glossary.md#auth-token) из параметров аутентификации
[config info](../commands/config-info.md) | Просмотр [параметров соединения](../connect.md)
[config profile activate](../profile/activate.md) | Активация [профиля](../profile/index.md)
[config profile create](../profile/create.md) | Создание [профиля](../profile/index.md)
[config profile delete](../profile/create.md) | Удаление [профиля](../profile/index.md)
[config profile deactivate](../profile/activate.md) | Деактивация текущего активного [профиля](../profile/index.md)
[config profile get](../profile/list-and-get.md) | Получение параметров [профиля](../profile/index.md)
[config profile list](../profile/list-and-get.md) | Список [профилей](../profile/index.md)
[config profile replace](../profile/create.md) | Создание или замена [профиля](../profile/index.md) с новыми значениями параметров
[config profile set](../profile/activate.md) | Активация [профиля](../profile/index.md)
[config profile update](../profile/create.md) | Обновление существующего [профиля](../profile/index.md)
debug latency | Проверка базовой задержки с переменным количеством параллельных запросов
debug ping | Проверка доступности {{ ydb-short-name }}
[discovery list](../commands/discovery-list.md) | Список эндпоинтов
[discovery whoami](../commands/discovery-whoami.md) | Проверка аутентификации
[export s3](../export-import/export-s3.md) | Экспорт данных в хранилище S3
[import file csv](../export-import/import-file.md) | Импорт данных из CSV-файла
[import file json](../export-import/import-file.md) | Импорт данных из JSON-файла
[import file parquet](../export-import/import-file.md) | Импорт данных из Parquet-файла
[import file tsv](../export-import/import-file.md) | Импорт данных из TSV-файла
[import s3](../export-import/import-s3.md) | Импорт данных из хранилища S3
[init](../profile/create.md) | Инициализация CLI, создание [профиля](../profile/index.md)
[operation cancel](../operation-cancel.md) | Прерывание исполнения фоновой операции
[operation forget](../operation-forget.md) | Удаление фоновой операции из списка
[operation get](../operation-get.md) | Статус фоновой операции
[operation list](../operation-list.md) | Список фоновых операций
[scheme describe](../commands/scheme-describe.md) | Описание объекта схемы данных
[scheme ls](../commands/scheme-ls.md) | Список объектов схемы данных
[scheme mkdir](../commands/dir.md#mkdir) | Создание директории
[scheme permissions chown](../commands/scheme-permissions.md#chown) | Изменение владельца объекта
[scheme permissions clear](../commands/scheme-permissions.md#clear) | Очистка разрешений
[scheme permissions grant](../commands/scheme-permissions.md#grant-revoke) | Предоставление разрешения
[scheme permissions revoke](../commands/scheme-permissions.md#grant-revoke) | Удаление разрешения
[scheme permissions set](../commands/scheme-permissions.md#set) | Установка разрешений
[scheme permissions list](../commands/scheme-permissions.md#list) | Просмотр разрешений
[scheme permissions clear-inheritance](../commands/scheme-permissions.md#clear-inheritance) | Запрет наследования разрешений
[scheme permissions set-inheritance](../commands/scheme-permissions.md#set-inheritance) | Установка наследования разрешений
[scheme rmdir](../commands/dir.md#rmdir) | Удаление директории
[scripting yql](../scripting-yql.md) | Выполнение YQL-скрипта (команда устарела, используйте [`ydb sql`](../sql.md))
[sql](../sql.md) | Выполнение любого запроса
[table attribute add](../table-attribute-add.md) | Добавление атрибута для строковой или колоночной [таблицы](../../../concepts/glossary.md#table)
[table attribute drop](../table-attribute-drop.md) | Удаление атрибута у строковой или колоночной [таблицы](../../../concepts/glossary.md#table)
[table drop](../table-drop.md) | Удаление строковой или колоночной [таблицы](../../../concepts/glossary.md#table)
[table index add global-async](../commands/secondary_index.md#add) | Добавление асинхронного [вторичного индекса](../../../concepts/glossary.md#secondary-index) для строковых [таблиц](../../../concepts/glossary.md#row-oriented-table)
[table index add global-sync](../commands/secondary_index.md#add) | Добавление синхронного [вторичного индекса](../../../concepts/glossary.md#secondary-index) для строковых [таблиц](../../../concepts/glossary.md#row-oriented-table)
[table index drop](../commands/secondary_index.md#drop) | Удаление [вторичного индекса](../../../concepts/glossary.md#secondary-index) у строковых [таблиц](../../../concepts/glossary.md#row-oriented-table)
[table index rename](../commands/secondary_index.md#rename) | Переименование [вторичного индекса](../../../concepts/glossary.md#secondary-index) для указанной [таблицы](../../../concepts/glossary.md#table)
[table query execute](../table-query-execute.md) | Исполнение YQL-запроса (команда устарела, используйте [`ydb sql`](../sql.md))
[table query explain](../commands/explain-plan.md) | Получение плана исполнения YQL-запроса (команда устарела, используйте [`ydb sql --explain`](../sql.md))
[table read](../commands/readtable.md) | Потоковое чтение строковой [таблицы](../../../concepts/glossary.md#row-oriented-table)
[table ttl set](../table-ttl-set.md) | Установка параметров [TTL](../../../concepts/glossary.md#ttl) для строковых и колоночных [таблиц](../../../concepts/glossary.md#table)
[table ttl reset](../table-ttl-reset.md) | Сброс параметров [TTL](../../../concepts/glossary.md#ttl) для строковых и колоночных [таблиц](../../../concepts/glossary.md#table)
[tools copy](../tools-copy.md) | Копирование [таблиц](../../../concepts/glossary.md#table)
[tools dump](../export-import/tools-dump.md#schema-objects) | Выгрузка отдельных схемных объектов в файловую систему
[tools infer csv](../tools-infer.md) | Генерация текста запроса `CREATE TABLE SQL` из CSV файла
{% if ydb-cli == "ydb" %}
[tools pg-convert](../../../postgresql/import.md#pg-convert) | Конвертация дампа PostgreSQL, полученного утилитой pg_dump, в формат, понятный YDB
{% endif %}
[tools rename](../commands/tools/rename.md) | Переименование строковых [таблиц](../../../concepts/glossary.md#row-oriented-table)
[tools restore](../export-import/tools-restore.md#schema-objects) | Восстановление отдельных схемных объектов из файловой системы
[topic create](../topic-create.md) | Создание [топика](../../../concepts/glossary.md#topic)
[topic alter](../topic-alter.md) | Модификация параметров [топика](../../../concepts/glossary.md#topic) и перечня [читателей](../../../concepts/glossary.md#consumer)
[topic drop](../topic-drop.md) | Удаление [топика](../../../concepts/glossary.md#topic)
[topic consumer add](../topic-consumer-add.md) | Добавление [читателя](../../../concepts/glossary.md#consumer) в [топик](../../../concepts/glossary.md#topic)
topic consumer describe | Описание [читателя](../../../concepts/glossary.md#consumer) [топика](../../../concepts/glossary.md#topic)
[topic consumer drop](../topic-consumer-drop.md) | Удаление [читателя](../../../concepts/glossary.md#consumer) из [топика](../../../concepts/glossary.md#topic)
[topic consumer offset commit](../topic-consumer-offset-commit.md) | Сохранение [смещения](../../../concepts/glossary.md#offset) чтения
[topic read](../topic-read.md) | Чтение сообщений из [топика](../../../concepts/glossary.md#topic)
[topic write](../topic-write.md) | Запись сообщений в [топик](../../../concepts/glossary.md#topic)
{% if ydb-cli == "ydb" %}
[update](../commands/service.md) | Обновление {{ ydb-short-name }} CLI
[version](../commands/service.md) | Вывод информации о версии {{ ydb-short-name }} CLI
{% endif %}
[workload clickbench init](../workload-click-bench.md#init) | Создание и инициализация [таблиц](../../../concepts/glossary.md#table) для нагрузки `Clickbench`
[workload clickbench import files](../workload-click-bench.md#load) | Загрузка набора данных `Clickbench` из файлов
[workload clickbench run](../workload-click-bench.md#run) | Выполнение бенчмарка `Clickbench`
[workload clickbench clean](../workload-click-bench.md#cleanup) | Удаление [таблиц](../../../concepts/glossary.md#table), созданных на этапе инициализации нагрузки `Clickbench`
[workload kv init](../workload-kv.md#init) | Создание и инициализация [таблиц](../../../concepts/glossary.md#table) для `Key-Value` нагрузки
[workload kv run upsert](../workload-kv.md#upsert-kv) | Вставка случайных кортежей в [таблицу](../../../concepts/glossary.md#table) при помощи конструкции `UPSERT` в `Key-Value` нагрузке
[workload kv run insert](../workload-kv.md#insert-kv) | Вставка случайных кортежей в [таблицу](../../../concepts/glossary.md#table) при помощи конструкции `INSERT` в `Key-Value` нагрузке
[workload kv run mixed](../workload-kv.md#mixed-kv) | Одновременная вставка и чтение кортежей с проверкой успешности чтения записанных данных в `Key-Value` нагрузке
[workload kv run read-rows](../workload-kv.md#read-rows-kv) | Выполнение ReadRows запросов, возвращающих строки по точному совпадению [первичного ключа](../../../concepts/glossary.md#primary-key) в `Key-Value` нагрузке
[workload kv run select](../workload-kv.md#select-kv) | Выборка данных, возвращающих строки по точному совпадению [первичного ключа](../../../concepts/glossary.md#primary-key) в `Key-Value` нагрузке
[workload kv clean](../workload-kv.md#clean) | Удаление [таблиц](../../../concepts/glossary.md#table), созданных на этапе инициализации `Key-Value` нагрузки
workload log init | Создание и инициализация [таблиц](../../../concepts/glossary.md#table) для `Log` нагрузки
workload log import generator | Генератор случайных данных в `Log` нагрузке
workload log run bulk_upsert | Массовая вставка случайных строк в [таблицу](../../../concepts/glossary.md#table) около текущего времени в `Log` нагрузке
workload log run delete | Удаление случайных строк из [таблицы](../../../concepts/glossary.md#table) около текущего времени в `Log` нагрузке
workload log run insert | Вставка случайных строк в [таблицу](../../../concepts/glossary.md#table) около текущего времени в `Log` нагрузке с помощью команды `INSERT`
workload log run upsert | Вставка случайных строк в [таблицу](../../../concepts/glossary.md#table) около текущего времени в `Log` нагрузке с помощью команды `UPSERT`
workload log run select | Выполнение набора аналитических запросов для анализа логов: подсчет записей, агрегация по уровням, сервисам и компонентам, анализ метаданных и временные диапазоны в `Log` нагрузке
workload log clean | Удаление [таблиц](../../../concepts/glossary.md#table), созданных на этапе инициализации `Log` нагрузки
workload mixed init | Создание и инициализация [таблиц](../../../concepts/glossary.md#table) для `Mixed` нагрузки
workload mixed run bulk_upsert | Массовая вставка случайных строк в [таблицу](../../../concepts/glossary.md#table) около текущего времени с помощью команды `BULK_UPSERT` в `Mixed` нагрузке
workload mixed run insert | Вставка случайных строк в [таблицу](../../../concepts/glossary.md#table) около текущего времени с помощью команды `INSERT` в `Mixed` нагрузке
workload mixed run upsert | Обновление случайных строк в [таблице](../../../concepts/glossary.md#table) около текущего времени с помощью команды `UPSERT` в `Mixed` нагрузке
workload mixed run select | Выборка случайных строк из [таблицы](../../../concepts/glossary.md#table) в `Mixed` нагрузке
workload mixed clean | Удаление [таблиц](../../../concepts/glossary.md#table), созданных на этапе инициализации `Mixed` нагрузки
workload query init | Инициализация [таблиц](../../../concepts/glossary.md#table) и их конфигураций для `Query` нагрузки
workload query import | Заполнение [таблиц](../../../concepts/glossary.md#table) данными для `Query` нагрузки
workload query run | Запуск нагрузочного тестирования `Query` нагрузки
workload query clean | Удаление [таблиц](../../../concepts/glossary.md#table), используемых для `Query` нагрузки
[workload stock init](../commands/workload/stock.md#init) | Создание и инициализация [таблиц](../../../concepts/glossary.md#table) для `Stock` нагрузки
[workload stock run add-rand-order](../commands/workload/stock.md#insert-random-order) | Вставка заказов со случайным ID без их обработки в `Stock` нагрузке
[workload stock run put-rand-order](../commands/workload/stock.md#submit-random-order) | Отправка случайных заказов с обработкой в `Stock` нагрузке
[workload stock run put-same-order](../commands/workload/stock.md#submit-same-order) | Отправка заказов с одинаковыми продуктами в `Stock` нагрузке
[workload stock run rand-user-hist](../commands/workload/stock.md#get-random-customer-history) | Выборка заказов случайного клиента в `Stock` нагрузке
[workload stock run user-hist](../commands/workload/stock.md#get-customer-history) | Выборка заказов 10000-го клиента в `Stock` нагрузке
[workload stock clean](../commands/workload/stock.md#clean) | Удаление [таблиц](../../../concepts/glossary.md#table), созданных на этапе инициализации `Stock` нагрузки
[workload topic init](../workload-topic.md#init) | Создание и инициализация [топика](../../../concepts/glossary.md#topic) для `Topic` нагрузки
[workload topic run full](../workload-topic.md#run-full) | Выполнение полной нагрузки на [топик](../../../concepts/glossary.md#topic) с одновременным чтением и записью сообщений в `Topic` нагрузке
[workload topic run read](../workload-topic.md#run-read) | Выполнение нагрузки на чтение сообщений из [топика](../../../concepts/glossary.md#topic) в `Topic` нагрузке
[workload topic run write](../workload-topic.md#run-write) | Выполнение нагрузки на запись сообщений в [топик](../../../concepts/glossary.md#topic) в `Topic` нагрузке
[workload topic clean](../workload-topic.md#clean) | Удаление [топика](../../../concepts/glossary.md#topic), созданного на этапе инициализации `Topic` нагрузки
[workload tpcc init](../workload-tpcc.md#init) | Создание и инициализация [таблиц](../../../concepts/glossary.md#table) для бенчмарка `TPC-C`
[workload tpcc import](../workload-tpcc.md#load) | Заполнение [таблиц](../../../concepts/glossary.md#table) начальными данными бенчмарка `TPC-C`
[workload tpcc check](../workload-tpcc.md#consistency_check) | Проверка согласованности данных `TPC-C`
[workload tpcc run](../workload-tpcc.md#run) | Запуск бенчмарка `TPC-C`
[workload tpcc clean](../workload-tpcc.md#cleanup) | Удаление [таблиц](../../../concepts/glossary.md#table), созданных бенчмарком `TPC-C`
[workload tpcds init](../workload-tpcds.md#init) | Создание и инициализация [таблиц](../../../concepts/glossary.md#table) для бенчмарка  `TPC-DS`
[workload tpcds import generator](../workload-tpcds.md#load) | Генерация набора данных `TPC-DS` с помощью встроенного генератора
[workload tpcds run](../workload-tpcds.md#run) | Выполнение бенчмарка `TPC-DS`
[workload tpcds clean](../workload-tpcds.md#cleanup) | Удаление [таблиц](../../../concepts/glossary.md#table), созданных на этапе инициализации `TPC-DS`
[workload tpch init](../workload-tpch.md#init) | Создание и инициализация [таблиц](../../../concepts/glossary.md#table) для бенчмарка `TPC-H`
[workload tpch import generator](../workload-tpch.md#load) | Генерация набора данных `TPC-H` с помощью встроенного генератора
[workload tpch run](../workload-tpch.md#run) | Выполнение бенчмарка `TPC-H`
[workload tpch clean](../workload-tpch.md#cleanup) | Удаление [таблиц](../../../concepts/glossary.md#table), созданных на этапе инициализации `TPC-H`
[workload transfer topic-to-table init](../workload-transfer.md#init) | Создание и инициализация [топика](../../../concepts/glossary.md#topic) с консьюмерами и [таблиц](../../../concepts/glossary.md#table) для нагрузки на передачу данных из топика в таблицу
[workload transfer topic-to-table run](../workload-transfer.md#run) | Запуск нагрузки с чтением сообщений из [топика](../../../concepts/glossary.md#topic) и записью в [таблицу](../../../concepts/glossary.md#table) в транзакциях
[workload transfer topic-to-table clean](../workload-transfer.md#clean) | Удаление [топика](../../../concepts/glossary.md#topic) и [таблиц](../../../concepts/glossary.md#table), созданных на этапе инициализации
workload vector init | Создание и инициализация [таблиц](../../../concepts/glossary.md#table) для `Vector` нагрузки
workload vector run select | Получение топ-K векторов в `Vector` нагрузке
workload vector run upsert | Upsert векторных строк в [таблицу](../../../concepts/glossary.md#table) в `Vector` нагрузке
workload vector clean | Удаление [таблиц](../../../concepts/glossary.md#table), созданных на этапе инициализации `Vector` нагрузки
[yql](../yql.md) | Выполнение YQL-скрипта с поддержкой стриминга (команда устарела, используйте [`ydb sql`](../sql.md))
