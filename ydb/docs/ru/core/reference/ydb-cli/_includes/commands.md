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
| [admin cluster dump](../export-import/tools-dump.md#cluster) | Выгрузка метаданных кластера в файловую систему |
| [admin cluster restore](../export-import/tools-restore.md#cluster) | Восстановление метаданных кластера из файловой системы |
| [admin database dump](../export-import/tools-dump.md#database) | Выгрузка метаданных и данных базы данных в файловую систему |
| [admin database restore](../export-import/tools-restore.md#database) | Восстановление метаданных и данных базы данных из файловой системы |
[config info](../commands/config-info.md) | Просмотр [параметров соединения](../connect.md)
[config profile activate](../profile/activate.md) | Активация [профиля](../profile/index.md)
[config profile create](../profile/create.md) | Создание [профиля](../profile/index.md)
[config profile delete](../profile/create.md) | Удаление [профиля](../profile/index.md)
[config profile get](../profile/list-and-get.md) | Получение параметров [профиля](../profile/index.md)
[config profile list](../profile/list-and-get.md) | Список [профилей](../profile/index.md)
[config profile set](../profile/activate.md) | Активация [профиля](../profile/index.md)
[discovery list](../commands/discovery-list.md) | Список эндпоинтов
[discovery whoami](../commands/discovery-whoami.md) | Проверка аутентификации
[export s3](../export-import/export-s3.md) | Экспорт данных в хранилище S3
[import file csv](../export-import/import-file.md) | Импорт данных из CSV-файла
[import file tsv](../export-import/import-file.md) | Импорт данных из TSV-файла
[import s3](../export-import/import-s3.md) | Импорт данных из хранилища S3
[init](../profile/create.md) | Инициализация CLI, создание [профиля](../profile/index.md)
[monitoring healthcheck](../commands/monitoring-healthcheck.md) | Проверка состояния базы
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
table attribute add | Добавление атрибута для строкой или колоночной таблицы
table attribute drop | Удаление атрибута у строковой или колоночной таблицы
[table drop](../table-drop.md) | Удаление строковой или колоночной таблицы
[table index add global-async](../commands/secondary_index.md#add) | Добавление асинхронного индекса для строковых таблиц
[table index add global-sync](../commands/secondary_index.md#add) | Добавление синхронного индекса для строковых таблиц
[table index drop](../commands/secondary_index.md#drop) | Удаление индекса у строковых таблиц
[table query execute](../table-query-execute.md) | Исполнение YQL-запроса (команда устарела, используйте [`ydb sql`](../sql.md))
[table query explain](../commands/explain-plan.md) | Получение плана исполнения YQL-запроса (команда устарела, используйте [`ydb sql --explain`](../sql.md))
[table read](../commands/readtable.md) | Потоковое чтение строковой таблицы
[table ttl set](../table-ttl-set.md) | Установка параметров TTL для строковых и колоночных таблиц
[table ttl reset](../table-ttl-reset.md) | Сброс параметров TTL для строковых и колоночных таблиц
[tools copy](../tools-copy.md) | Копирование таблиц
[tools dump](../export-import/tools-dump.md#schema-objects) | Выгрузка отдельных схемных объектов в файловую систему
{% if ydb-cli == "ydb" %}
[tools pg-convert](../../../postgresql/import.md#pg-convert) | Конвертация дампа PostgreSQL, полученного утилитой pg_dump, в формат, понятный YDB
{% endif %}
[tools rename](../commands/tools/rename.md) | Переименование строковых таблиц
[tools restore](../export-import/tools-restore.md#schema-objects) | Восстановление отдельных схемных объектов из файловой системы
[topic create](../topic-create.md) | Создание топика
[topic alter](../topic-alter.md) | Модификация параметров топика и перечня читателей
[topic drop](../topic-drop.md) | Удаление топика
[topic consumer add](../topic-consumer-add.md) | Добавление читателя в топик
[topic consumer drop](../topic-consumer-drop.md) | Удаление читателя из топика
[topic consumer offset commit](../topic-consumer-offset-commit.md) | Сохранение позиции чтения
[topic read](../topic-read.md) | Чтение сообщений из топика
[topic write](../topic-write.md) | Запись сообщений в топик
{% if ydb-cli == "ydb" %}
[update](../commands/service.md) | Обновление {{ ydb-short-name }} CLI
[version](../commands/service.md) | Вывод информации о версии {{ ydb-short-name }} CLI
{% endif %}
[workload](../commands/workload/index.md) | Генерация нагрузки
[yql](../yql.md) | Выполнение YQL-скрипта с поддержкой стриминга (команда устарела, используйте [`ydb sql`](../sql.md))
