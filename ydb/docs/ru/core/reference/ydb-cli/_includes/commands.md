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
[operation cancel](../operation-cancel.md) | Прерывание исполнения фоновой операции
[operation forget](../operation-forget.md) | Удаление фоновой операции из списка
[operation get](../operation-get.md) | Статус фоновой операции
[operation list](../operation-list.md) | Список фоновых операций
[scheme describe](../commands/scheme-describe.md) | Описание объекта схемы данных
[scheme ls](../commands/scheme-ls.md) | Список объектов схемы данных
[scheme mkdir](../commands/dir.md#mkdir) | Создание директории
scheme permissions add | Предоставление разрешения
scheme permissions chown | Изменение владельца объекта
scheme permissions clear | Очистка разрешений
scheme permissions grant | Предоставление разрешения
scheme permissions remove | Удаление разрешения
scheme permissions revoke | Удаление разрешения
scheme permissions set | Установка разрешений
[scheme rmdir](../commands/dir.md#rmdir) | Удаление директории
[scripting yql](../scripting-yql.md) | Выполнение YQL-скрипта
table attribute add | Добавление атрибута таблицы
table attribute drop | Удаление атрибута таблицы
[table drop](../table-drop.md) | Удаление таблицы
[table index add global-async](../commands/secondary_index.md#add) | Добавление асинхронного индекса
[table index add global-sync](../commands/secondary_index.md#add) | Добавление синхронного индекса
[table index drop](../commands/secondary_index.md#drop) | Удаление индекса
[table query execute](../table-query-execute.md) | Исполнение YQL-запроса
[table query explain](../commands/explain-plan.md) | План исполнения YQL-запроса
[table read](../commands/readtable.md) | Потоковое чтение таблицы
[table ttl set](../table-ttl-set.md) | Установка параметров TTL
[table ttl reset](../table-ttl-reset.md) | Сброс параметров TTL
[tools copy](../tools-copy.md) | Копирование таблиц
[tools dump](../export-import/tools-dump.md) | Выгрузка директории или таблицы в файловую систему
{% if ydb-cli == "ydb" %}
[tools pg-convert](../../../postgresql/pg-dump.md#pg-convert) | Конвертация дампа PostgreSQL, полученного утилитой pg_dump, в формат, понятный YDB
{% endif %}
[tools rename](../commands/tools/rename.md) | Переименование таблиц
[tools restore](../export-import/tools-restore.md) | Восстановление из файловой системы
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
[yql](../yql.md) | Выполнение YQL-скрипта (с поддержкой стриминга)
