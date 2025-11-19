# Аудитный лог

_Аудитный лог_ — это поток записей, фиксирующих работу кластера {{ ydb-short-name }}. В отличие от технических логов, которые помогают в обнаружении сбоев и устранении неполадок, аудитный лог предоставляет данные значимые для безопасности. Он служит источником информации, отвечая на вопросы: кто что сделал, когда и откуда.

Одна запись в аудитном логе может выглядеть так:

```json
{"@timestamp":"2025-11-03T18:07:39.056211Z","@log_type":"audit","operation":"ExecuteQueryRequest","database":"/my_dir/db1","status":"SUCCESS","subject":"serviceaccount@as","remote_address":"ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]"}
```

Примеры типичных событий аудитного лога:

* доступ к данным через DML-запросы;
* операции управления схемой или конфигурацией;
* изменения прав или настроек контроля доступа;
* административные действия пользователей.

Секция `audit_config` в [конфигурации кластера](../reference/configuration/index.md) определяет, какие аудит-логи собираются, как они сериализуются и куда доставляются. Подробнее см. в разделе [Конфигурация аудитного лога](#audit-log-configuration).

## Ключевые понятия {#audit-log-concepts}

### Аудитные события {#audit-events}

*Аудитное событие* — это запись в аудитном логе, которая фиксирует одно действие, важное для безопасности. Каждое событие содержит атрибуты, описывающие разные аспекты события. Общие атрибуты перечислены в разделе [Общие атрибуты](#common-attributes).

### Источники аудитных событий {#audit-event-sources}

*Источник аудитных событий* — это служба или подсистема {{ ydb-short-name }}, которая может генерировать аудитные события. Каждый источник имеет уникальный идентификатор (UID) и может предоставлять дополнительные атрибуты, характерные для этого источника. Некоторые источники требуют дополнительной настройки, например включения [функциональных флагов](../reference/configuration/feature_flags.md), чтобы начать генерировать события. Подробнее см. [Обзор источников аудитных событий](#audit-event-sources-overview).

### Классы логирования {#log-classes}

Аудитные события группируются в *классы логирования*, которые отражают основные категории операций. Вы можете включать или отключать запись для каждого класса в [конфигурации](#log-class-config) и при необходимости настраивать каждый класс отдельно. Доступные классы логирования:

#|
|| Класс логирования.   | Описание ||
|| `ClusterAdmin`       | Запросы на администрирование кластера. ||
|| `DatabaseAdmin`      | Запросы на администрирование баз данных. ||
|| `Login`              | Запросы на вход. ||
|| `NodeRegistration`   | Регистрация нод. ||
|| `Ddl`                | DDL-запросы. ||
|| `Dml`                | DML-запросы. ||
|| `Operations`         | Асинхронные операции RPC, для отслеживания результата которых требуется опрос. ||
|| `ExportImport`       | Операции экспорта и импорта данных. ||
|| `Acl`                | Операции с ACL (списками контроля доступа). ||
|| `AuditHeartbeat`     | Синтетические heartbeat-сообщения, подтверждающие работу аудитного логирования. ||
|| `Default`            | Настройки по умолчанию для любого компонента без собственной секции конфигурации. ||
|#

На данный момента не все источники аудитных событий группируют события по классам. Чаще всего для их записи будет достаточно [базовой конфигурации](#enabling-audit-log). Подробности см. в разделе [Обзор источников аудитных событий](#audit-event-sources-overview).

### Фазы логирования {#log-phases}

Некоторые источники аудитных событий разделяют обработку запроса на этапы. *Фазы логирования* указывают те этапы, на которых создаются события аудитного лога. Указание фаз логирования полезно, когда требуется детальная видимость выполнения запроса и нужно фиксировать события до и после критических этапов обработки. Доступные фазы логирования:

#|
|| Фаза логирования | Описание ||
|| `Received`       | Запрос получен, выполнены начальные проверки и аутентификация. Атрибут `status` устанавливается в значение `IN-PROCESS`. </br>Эта фаза отключена по умолчанию; чтобы включить её, добавьте `Received` в `log_class_config.log_phase`. ||
|| `Completed`      | Запрос полностью завершён. Атрибут `status` принимает значение `SUCCESS` или `ERROR`. Эта фаза включена по умолчанию, если `log_class_config.log_phase` не задан. ||
|#

### Направления аудитного лога {#audit-log-destinations}

*Направление аудитного лога* - получатель потока аудитного лога.

На данный момент вы можете настроить следующие направления для аудитного лога:

* файл на каждой ноде кластера {{ ydb-short-name }};
* агент, который доставляет метрики [Unified Agent](https://yandex.cloud/docs/monitoring/concepts/data-collection/unified-agent/);
* стандартный поток ошибок `stderr`.

Вы можете использовать любое из перечисленных направлений или их комбинации. Подробнее см. в разделе [Конфигурация аудитного лога](#audit-log-configuration).

Если поток перенаправлен в файл, доступ к аудитному логу определяется правами файловой системы. Сохранение аудитного лога в файл рекомендуется для продуктивных инсталляций.

Для тестовых инсталляций направляйте аудитный лог в стандартный поток ошибок (`stderr`). Дальнейшая обработка потока зависит от настроек [логирования](../devops/observability/logging.md) кластера {{ ydb-short-name }}.

## Обзор источников аудитных событий {#audit-event-sources-overview}

В таблице ниже приведены встроенные источники аудитных событий. Используйте её, чтобы определить, какой источник генерирует нужные события и как их включить.

#|
|| Источник / UID | Что фиксирует | Требования к настройке ||
|| [Schemeshard](#schemeshard) </br>`schemeshard` | Операции со схемой, изменения ACL и действия по управлению пользователями. | Входит в [базовую конфигурацию аудита](#enabling-audit-log). ||
|| [gRPC-сервисы](#grpc-proxy) </br>`grpc-proxy` | Внешние gRPC-запросы {{ ydb-short-name }}. | Включите соответствующие [классы логирования](#log-class-config) и при необходимости [фазы логирования](#log-phases). ||
|| [gRPC-соединение](#grpc-connection) </br>`grpc-conn` | События подключения и отключения клиентов. | Включите функциональный флаг [`enable_grpc_audit`](../reference/configuration/feature_flags.md). ||
|| [gRPC-аутентификация](#grpc-login) </br>`grpc-login` | Попытки аутентификации в gRPC. | Включите класс `Login` в [`log_class_config`](#log-class-config). ||
|| [Сервис мониторинга](#monitoring) </br>`monitoring` | HTTP-запросы, обрабатываемые [эндпоинтом мониторинга](../reference/configuration/tls.md#http). | Включите класс `ClusterAdmin` в [`log_class_config`](#log-class-config). ||
|| [Heartbeat](#heartbeat) </br>`audit` | Синтетические heartbeat-события, подтверждающие работоспособность аудитного логирования. | Включите класс `AuditHeartbeat` в [`log_class_config`](#log-class-config) и при необходимости настройте [параметры heartbeat](#heartbeat-settings). ||
|| [BlobStorage Controller](#bsc) </br>`bsc` | Изменения конфигурации BlobStorage Controller из консоли. | Входит в [базовую конфигурацию аудита](#enabling-audit-log). ||
|| [Distconf](#distconf) </br>`distconf` | Обновления распределённой конфигурации. | Входит в [базовую конфигурацию аудита](#enabling-audit-log). ||
|| [Web login](#web-login) </br>`web-login` | Взаимодействия с виджетом аутентификации веб-консоли. | Входит в [базовую конфигурацию аудита](#enabling-audit-log). ||
|| [Console](#console) </br>`console` | Операции жизненного цикла баз данных и изменения динамической конфигурации. | Входит в [базовую конфигурацию аудита](#enabling-audit-log). ||
|#

## Атрибуты аудитных событий {#audit-event-attributes}

Как упоминалось, атрибуты делятся на две группы:
* общие атрибуты. Присутствуют в большинстве *источников аудитных событий*. Они всегда несут одинаковый смысл;
* атрибуты, специфичные для *источника аудитных событий*.

В этом разделе представлен справочник по атрибутам событий аудитного лога: как общим, так и специфичных для источников. Для каждого источника также указаны его UID, регистрируемые операции и требования к настройке.

### Общие атрибуты {#common-attributes}

В таблице ниже перечислены общие атрибуты.

#|
|| Атрибут             | Описание ||
|| `subject`           | SID источника события (формат `<login>@<subsystem>`). Если обязательная аутентификация не включена, атрибут будет иметь значение `{none}`. ||
|| `sanitized_token`   | Частично маскированный токен аутентификации, использованный для выполнения запроса. Позволяет связывать события и при этом скрывать исходные учётные данные. Если аутентификация не выполнялась, значение будет `{none}`. ||
|| `operation`         | Название операции (например, `ALTER DATABASE`, `CREATE TABLE`). ||
|| `component`         | Уникальный идентификатор *источника аудитных событий*. ||
|| `status`            | Статус завершения операции.<br/>Возможные значения:<ul><li>`SUCCESS`: операция завершена успешно.</li><li>`ERROR`: операция завершилась с ошибкой.</li><li>`IN-PROCESS`: операция выполняется.</li></ul> ||
|| `reason`            | Сообщение об ошибке. ||
|| `request_id`        | Уникальный идентификатор запроса, вызвавшего операцию. По `request_id` можно отличать события разных операций и связывать их в единый аудитный контекст. ||
|| `remote_address`    | IP-адрес клиента, отправившего запрос. ||
|| `detailed_status`   | Статус, который передаёт *источник аудитных событий* {{ ydb-short-name }}. ||
|| `database`          | Путь базы данных (например, `/my_dir/db`). ||
|| `cloud_id`          | Идентификатор облака базы данных {{ ydb-short-name }}. ||
|| `folder_id`         | Идентификатор каталога кластера или базы данных {{ ydb-short-name }}. ||
|| `resource_id`       | Идентификатор ресурса базы данных {{ ydb-short-name }}. ||
|#

### Schemeshard {#schemeshard}

**UID:** `schemeshard`.
**Регистрируемые операции:** операции со схемой, инициированные DDL-запросами, изменения ACL и действия управления пользователями.
**Как включить:** требуется только [базовая конфигурация аудита](#enabling-audit-log).

Таблица ниже перечисляет дополнительные атрибуты, специфичные для источника `Schemeshard`.

#|
|| Атрибут                              | Описание ||
|| **Общие атрибуты Schemeshard**      | **>** ||
|| `tx_id`                              | Уникальный идентификатор транзакции. Этот идентификатор помогает различать события разных операций.</br>*Обязательный.* ||
|| `paths`                              | Список путей в базе данных, которые изменяет операция (например, `[/my_dir/db/table-a, /my_dir/db/table-b]`).</br>*Необязательный.* ||
|| **Атрибуты владения и прав доступа** | **>** ||
|| `new_owner`                          | SID нового владельца объекта при передаче владения. ||
|| `acl_add`                            | Список добавленных разрешений в [краткой записи](./short-access-control-notation.md) (например, `[+R:someuser]`). ||
|| `acl_remove`                         | Список отозванных разрешений в [краткой записи](./short-access-control-notation.md) (например, `[-R:someuser]`). ||
|| **Пользовательские атрибуты**        | **>** ||
|| `user_attrs_add`                     | Список пользовательских атрибутов, добавленных при создании объектов или обновлении атрибутов (например, `[attr_name1: A, attr_name2: B]`). ||
|| `user_attrs_remove`                  | Список пользовательских атрибутов, удалённых при создании объектов или обновлении атрибутов (например, `[attr_name1, attr_name2]`). ||
|| **Атрибуты входа/аутентификации**    | **>** ||
|| `login_user`                         | Имя пользователя, зафиксированное при операциях входа. ||
|| `login_group`                        | Имя группы, зафиксированное при операциях входа. ||
|| `login_member`                       | Изменения членства. ||
|| `login_user_change`                  | Изменения, применённые к настройкам пользователя. ||
|| `login_user_level`                   | Уровень привилегий пользователя, записанный в событиях аудита. Это поле принимает значение `admin`. ||
|| **Атрибуты операций импорта/экспорта** | **>** ||
|| `id`                                 | Уникальный идентификатор операции экспорта или импорта. ||
|| `uid`                                | Заданная пользователем метка операции. ||
|| `start_time`                         | Время начала операции в формате ISO 8601. ||
|| `end_time`                           | Время завершения операции в формате ISO 8601. ||
|| `last_login`                         | Время последнего успешного входа пользователя в формате ISO 8601. ||
|| **Атрибуты экспорта**                | **>** ||
|| `export_type`                        | Направление экспорта. Возможные значения: `yt`, `s3`. ||
|| `export_item_count`                  | Количество экспортированных элементов. ||
|| `export_yt_prefix`                   | Префикс пути для экспорта в YT. ||
|| `export_s3_bucket`                   | Имя S3-бакета, используемого для экспорта. ||
|| `export_s3_prefix`                   | Префикс пути для экспорта в S3. ||
|| **Атрибуты импорта**                 | **>** ||
|| `import_type`                        | Тип источника импорта. Всегда `s3`. ||
|| `import_item_count`                  | Количество импортированных элементов. ||
|| `import_s3_bucket`                   | Имя S3-бакета, используемого для импорта. ||
|| `import_s3_prefix`                   | Префикс источника в S3. ||
|#

### gRPC-сервисы {#grpc-proxy}

**UID:** `grpc-proxy`.
**Регистрируемые операции:** все внешние (не внутренние) gRPC-запросы.
**Как включить:** требуется указать классы логирования в конфигурации аудита.
**Классы логирования:** зависят от типа RPC-запроса: `Ddl`, `Dml`, `Operations`, `ClusterAdmin`, `DatabaseAdmin` и другие классы.
**Фазы логирования:** `Received`, `Completed`.

Таблица ниже перечисляет дополнительные атрибуты, специфичные для источника `gRPC services`.

#|
|| Атрибут                  | Описание ||
|| **Общие gRPC-атрибуты**  | **>** ||
|| `grpc_method`            | Имя RPC-метода.</br>*Необязательный.* ||
|| `request`                | Санитизированное представление входящего запроса.</br>*Необязательный.* ||
|| `start_time`             | Время начала операции в формате ISO 8601.</br>*Обязательный.* ||
|| `end_time`               | Время завершения операции в формате ISO 8601.</br>*Необязательный.* ||
|| **Атрибуты транзакций**  | **>** ||
|| `tx_id`                  | Идентификатор транзакции. ||
|| `begin_tx`               | Флаг со значением `1`, если запрос запускает новую транзакцию. ||
|| `commit_tx`              | Показывает, завершается ли транзакция этим запросом. Возможные значения: `true`, `false`. ||
|| **Поля запроса**         | **>** ||
|| `query_text`             | Санитизированный текст [YQL](../yql/reference/index.md)-запроса. ||
|| `prepared_query_id`      | Идентификатор подготовленного запроса. ||
|| `program_text`           | [MiniKQL-программа](../concepts/glossary.md#minikql), отправленная с запросом. ||
|| `schema_changes`         | Описание изменений схемы, запрошенных в операции. ||
|| `table`                  | Полный путь таблицы. ||
|| `row_count`              | Количество строк, обработанных [пакетной вставкой данных](../recipes/ydb-sdk/bulk-upsert.md). ||
|| `tablet_id`              | Идентификатор таблетки. ||
|#

### gRPC-соединение {#grpc-connection}

**UID:** `grpc-conn`.
**Регистрируемые операции:** изменения состояния соединения (подключение и отключение).
**Как включить:** активируйте функциональный флаг [`enable_grpc_audit`](../reference/configuration/feature_flags.md).

*Этот источник использует только общие атрибуты.*

### gRPC-аутентификация {#grpc-login}

**UID:** `grpc-login`.
**Регистрируемые операции:** аутентификация в gRPC.
**Как включить:** требуется указать классы логирования в [конфигурации аудита](#audit-log-configuration).
**Классы логирования:** `Login`.
**Фазы логирования:** `Completed`.

Таблица ниже перечисляет дополнительные атрибуты, специфичные для источника `gRPC authentication`.

#|
|| Атрибут            | Описание ||
|| `login_user`       | Имя пользователя. *Обязательный.* ||
|| `login_user_level` | Уровень привилегий пользователя, записанный в событиях аудита. Это поле принимает значение `admin`. *Необязательный.* ||
|#

### Сервис мониторинга {#monitoring}

**UID:** `monitoring`.
**Регистрируемые операции:** HTTP-запросы, обрабатываемые сервисом мониторинга.
**Как включить:** требуется указать классы логирования в [конфигурации аудита](#audit-log-configuration).
**Классы логирования:** `ClusterAdmin`.
**Фазы логирования:** `Received`, `Completed`.

Таблица ниже перечисляет дополнительные атрибуты, специфичные для источника `Monitoring service`.

#|
|| Атрибут  | Описание ||
|| `method` | HTTP-метод запроса, например `POST`, `GET`.</br>*Обязательный.* ||
|| `url`    | Путь запроса без параметров строки запроса.</br>*Обязательный.* ||
|| `params` | Параметры строки запроса в исходном виде.</br>*Необязательный.* ||
|| `body`   | Тело запроса (усечено до 2 МБ с добавлением суффикса `TRUNCATED_BY_YDB`).</br>*Необязательный.* ||
|#

### Heartbeat {#heartbeat}

**UID:** `audit`.
**Регистрируемые операции:** периодические [heartbeat-сообщения](#heartbeat-settings) аудита.
**Как включить:** укажите классы логирования в [конфигурации аудита](#audit-log-configuration).
**Классы логирования:** `AuditHeartbeat`.
**Фазы логирования:** `Completed`.

Таблица ниже перечисляет дополнительные атрибуты, специфичные для источника `Heartbeat`.

#|
|| Атрибут  | Описание ||
|| `node_id` | Идентификатор ноды, на которой произошло событие. *Обязательный.* ||
|#

### BlobStorage Controller {#bsc}

**UID:** `bsc`.
**Регистрируемые операции:** запросы замены конфигурации (`TEvControllerReplaceConfigRequest`), отправляемые консолью.
**Как включить:** требуется только [базовая конфигурация аудита](#enabling-audit-log).

Таблица ниже перечисляет дополнительные атрибуты, специфичные для источника `BlobStorage Controller`.

#|
|| Атрибут      | Описание ||
|| `old_config` | Снапшот предыдущей конфигурации BlobStorage Controller в формате YAML. </br>*Необязательный.* ||
|| `new_config` | Снапшот конфигурации, которая заменила предыдущую. </br>*Необязательный.* ||
|#

### Distconf {#distconf}

**UID:** `distconf`.
**Регистрируемые операции:** изменения распределённой конфигурации.
**Как включить:** требуется только [базовая конфигурация аудита](#enabling-audit-log).

Таблица ниже перечисляет дополнительные атрибуты, специфичные для источника `Distconf`.

#|
|| Атрибут      | Описание ||
|| `old_config` | Снапшот конфигурации, которая была активна до принятия распределённого обновления. Distconf сериализует его в YAML. *Обязательный.* ||
|| `new_config` | Снапшот конфигурации, который Distconf зафиксировал после изменения. *Обязательный.* ||
|#

### Web login {#web-login}

**UID:** `web-login`.
**Регистрируемые операции:** отслеживает взаимодействия с виджетом аутентификации веб-консоли {{ ydb-short-name }}.
**Как включить:** требуется только [базовая конфигурация аудита](#enabling-audit-log).

*Этот источник использует только общие атрибуты.*

### Console {#console}

**UID:** `console`.
**Регистрируемые операции:** операции жизненного цикла баз данных и изменения динамической конфигурации.
**Как включить:** требуется только [базовая конфигурация аудита](#enabling-audit-log).

Таблица ниже перечисляет дополнительные атрибуты, специфичные для источника `Console`.

#|
|| Атрибут      | Описание ||
|| `old_config` | Снапшот конфигурации, которая действовала до применения запроса из консоли. *Необязательный.* ||
|| `new_config` | Снапшот конфигурации, применённой консолью. *Необязательный.* ||
|#

## Конфигурация аудитного лога {#audit-log-configuration}

### Включение аудитного лога {#enabling-audit-log}

Аудитное логирование работает на уровне всего кластера. Для *базовой конфигурации* добавьте раздел `audit_config` в [конфигурацию кластера](../reference/configuration/index.md) и укажите одно или несколько направлений потока (`file_backend`, `unified_agent_backend`, `stderr_backend`):

```yaml
audit_config:
  file_backend:
    format: audit_log_format
    file_path: "path_to_log_file"
  unified_agent_backend:
    format: audit_log_format
    log_name: session_meta_log_name
  stderr_backend:
    format: audit_log_format
```

### Параметры audit_config {#audit-config}

Все поля являются необязательными.

#|
|| Ключ                    | Описание ||
|| `stderr_backend`        | Перенаправляет аудитный лог в стандартный поток ошибок (`stderr`). Для тестовых инсталляций направляйте аудитный лог в стандартный поток ошибок (`stderr`). Дальнейшая обработка потока зависит от настроек [логирования](../devops/observability/logging.md) кластера {{ ydb-short-name }}. См. структуру [настроек backend](#backend-settings). ||
|| `file_backend`          | Записывает аудитный лог в файл на каждой ноде кластера. Если поток перенаправлен в файл, доступ к аудитному логу определяется правами файловой системы. Сохранение аудитного лога в файл рекомендуется для продуктовых инсталляций. См. структуру [настроек backend](#backend-settings). ||
|| `unified_agent_backend` | Отправляет аудитный лог в Unified Agent. Дополнительно необходимо задать секцию `uaclient_config` в [конфигурации кластера](../reference/configuration/index.md). См. структуру [настроек backend](#backend-settings). ||
|| `log_class_config`      | Массив правил аудита для разных классов логирования. См. [конфигурацию классов логирования](#log-class-config). ||
|| `heartbeat`             | Необязательная конфигурация heartbeat. См. [настройки heartbeat](#heartbeat-settings). ||
|#

### Конфигурация направлений {#backend-settings}

Каждое направление поддерживает следующие поля:

#|
|| Поле                | Описание ||
|| `format`            | Формат аудитного лога. Значение по умолчанию — `JSON`. Подробнее см. раздел [Формат логов](#log-format).<br/>*Необязательный.* ||
|| `file_path`         | Путь к файлу, в который будет записываться аудитный лог. Если путь или файл отсутствуют, они будут созданы на каждой ноде при запуске кластера. Если файл существует, данные будут дозаписываться. Только для `file_backend`. <br/>*Обязательный.* ||
|| `log_name`          | Метаданные сессии, передаваемые вместе с сообщением. По ним можно перенаправлять поток логов в один или несколько дочерних каналов с условием `_log_name: "session_meta_log_name"`. Только для `unified_agent_backend`. <br/>*Необязательный.* ||
|| `log_json_envelope` | Шаблон JSON, который оборачивает каждую запись лога. Шаблон должен содержать плейсхолдер `%message%`, заменяемый сериализованной аудитной записью. См. раздел [Формат конверта](#envelope-format).</br>*Необязательный.* ||
|#

#### Формат логов {#log-format}

Поле `format` определяет формат сериализации аудитных событий. Поддерживаются следующие форматы:

#|
|| Формат               | Описание ||
|| `JSON`               | Каждое аудитное событие сериализуется в однострочный JSON-объект с префиксом из временной метки в формате ISO 8601.</br>Пример: `<time>: {"k1": "v1", "k2": "v2", ...}` </br>*`k1`, `k2`, …, `kn` — атрибуты аудитного лога; `v1`, `v2`, …, `vn` — их значения.* ||
|| `TXT`                | Каждое аудитное событие сериализуется в однострочную строку `key=value` с префиксом из временной метки в формате ISO 8601.</br>Пример: `<time>: k1=v1, k2=v2, ...` </br>*`k1`, `k2`, …, `kn` — атрибуты аудитного лога; `v1`, `v2`, …, `vn` — их значения.* ||
|| `JSON_LOG_COMPATIBLE` | Каждое аудитное событие сериализуется в однострочный JSON-объект, совместимый с целями, совместно используемыми с отладочными логами. Объект содержит поле `@timestamp` с временной меткой ISO 8601 и поле `@log_type` со значением `audit`.</br>Пример: `{"@timestamp": "<ISO 8601 time>", "@log_type": "audit", "k1": "v1", "k2": "v2", ...}` </br>*`@timestamp` хранит временную метку ISO 8601; `k1`, `k2`, …, `kn` — атрибуты аудитного лога; `v1`, `v2`, …, `vn` — их значения.* ||
|#

#### Формат конверта {#envelope-format}

Backend может оборачивать аудитные события пользовательским конвертом перед отправкой, если указать поле `log_json_envelope`. Шаблон должен содержать плейсхолдер `%message%`, который заменяется сериализованной аудитной записью в выбранном формате.

Например, следующая конфигурация выводит аудитные события в `stderr` в формате JSON, обёрнутые пользовательским конвертом:

```yaml
audit_config:
  stderr_backend:
    format: JSON
    log_json_envelope: '{"audit": %message%, "source": "ydb-audit-log"}'
```

Подробнее о выводе см. в разделе [Пример с JSON-конвертом](#examples).

### Настройка классов логирования {#log-class-config}

Каждый элемент `log_class_config` поддерживает следующие поля:

#|
|| Поле                   | Описание ||
|| `log_class`            | Имя настраиваемого класса. Использует значения из списка [классов логирования](#log-classes). В списке `log_class_config` не должно быть двух классов с одинаковым именем.<br/>*Обязательный.* ||
|| `enable_logging`       | Включает генерацию аудитных событий для выбранного класса. По умолчанию отключено.<br/>*Необязательный.* ||
|| `exclude_account_type` | Массив типов аккаунтов (`Anonymous`, `User`, `Service`, `ServiceImpersonatedFromUser`), для которых события исключаются, даже если логирование включено.<br/>*Необязательный.* ||
|| `log_phase`            | Массив фаз обработки запроса, которые нужно логировать. См. раздел [Фазы логирования](#log-phases).<br/>*Необязательный.* ||
|#

### Настройки heartbeat {#heartbeat-settings}

Heartbeat-события помогают контролировать состояние подсистемы аудитного логирования. Они позволяют настраивать оповещения об отсутствии аудитных событий без ложных срабатываний в периоды без активности.

Параметр `heartbeat.interval_seconds` задаёт частоту записи heartbeat-событий. Значение `0` отключает сообщения heartbeat.

### Примеры конфигураций {#config-samples}

**Простая конфигурация.** Ниже приведена простая конфигурация, сохраняющая аудитный лог в текстовом виде в файле с форматом `TXT`.

```yaml
audit_config:
  file_backend:
    format: TXT
    file_path: "/var/log/ydb-audit.log"
```

**Расширенная конфигурация.** Ниже приведён расширенный пример конфигурации аудитного лога:
* {{ ydb-short-name }} отправляет аудитные логи в Unified Agent в формате `TXT` с меткой `audit`, а также выводит его в `stderr` в формате `JSON`;
* настройки `Default` включают логирование всех классов в фазе `Completed`;
* дополнительно `ClusterAdmin` настроен для логирования фазы `Received`, а `DatabaseAdmin` — для исключения событий от анонимных пользователей.

```yaml
audit_config:
  unified_agent_backend:
    format: TXT
    log_name: audit
  stderr_backend:
    format: JSON
  log_class_config:
    - log_class: ClusterAdmin
      enable_logging: true
      log_phase: [Received, Completed]
    - log_class: DatabaseAdmin
      enable_logging: true
      log_phase: [Completed]
      exclude_account_type: [Anonymous]
    - log_class: Default
      enable_logging: true
  heartbeat:
    interval_seconds: 60
```

## Примеры {#examples}

Следующие вкладки демонстрируют одно и то же событие аудитного лога, записанное с разной [конфигурацией направления](#backend-settings).

{% list tabs %}

- JSON

    Формат `JSON` создаёт записи следующего вида:

    ```json
    2023-03-14T10:41:36.485788Z: {"paths":"[/my_dir/db1/some_dir]","tx_id":"281474976775658","database":"/my_dir/db1","remote_address":"ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]:xxxxx","status":"SUCCESS","subject":"{none}","sanitized_token":"{none}","detailed_status":"StatusAccepted","operation":"MODIFY ACL","component":"schemeshard","acl_add":"[+(ConnDB):subject:-]"}
    2023-03-13T20:07:30.927210Z: {"reason":"Check failed: path: '/my_dir/db1/some_dir', error: path exist, request accepts it (id: [OwnerId: 72075186224037889, LocalPathId: 3], type: EPathTypeDir, state: EPathStateNoChanges)","paths":"[/my_dir/db1/some_dir]","tx_id":"844424930216970","database":"/my_dir/db1","remote_address":"ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]:xxxxx","status":"SUCCESS","subject":"{none}","sanitized_token":"{none}","detailed_status":"StatusAlreadyExists","operation":"CREATE DIRECTORY","component":"schemeshard"}
    2025-11-03T18:07:39.056211Z: {"@log_type":"audit","begin_tx":1,"commit_tx":1,"component":"grpc-proxy","database":"/my_dir/db1","detailed_status":"SUCCESS","end_time":"2025-11-03T18:07:39.056204Z","grpc_method":"Ydb.Query.V1.QueryService/ExecuteQuery","operation":"ExecuteQueryRequest","query_text":"SELECT * FROM `my_row_table`;","remote_address":"ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]","sanitized_token":"xxxxxxxx.**","start_time":"2025-11-03T18:07:39.054863Z","status":"SUCCESS","subject":"serviceaccount@as"}
    2025-11-03T17:41:44.203214Z: {"component":"monitoring","remote_address":"ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]","operation":"HTTP REQUEST","method":"POST","url":"/viewer/query","params":"base64=false&schema=multipart","body":"{\"query\":\"SELECT * FROM `my_row_table`;\",\"database\":\"/local\",\"action\":\"execute-query\",\"syntax\":\"yql_v1\"}","status":"IN-PROCESS","reason":"Execute"}
    ```

- TXT

    Формат `TXT` создаёт записи следующего вида:

    ```txt
    2023-03-14T10:41:36.485788Z: component=schemeshard, tx_id=281474976775658, remote_address=ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]:xxxxx, subject={none}, database=/my_dir/db1, operation=MODIFY ACL, paths=[/my_dir/db1/some_dir], status=SUCCESS, detailed_status=StatusSuccess, acl_add=[+(ConnDB):subject:-]
    2023-03-13T20:07:30.927210Z: component=schemeshard, tx_id=281474976775657, remote_address=ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]:xxxxx, subject={none}, database=/my_dir/db1, operation=CREATE DIRECTORY, paths=[/my_dir/db1/some_dir], status=SUCCESS, detailed_status=StatusAlreadyExists, reason=Check failed: path: '/my_dir/db1/some_dir', error: path exist, request accepts it (id: [OwnerId: 72075186224037889, LocalPathId: 3], type: EPathTypeDir, state: EPathStateNoChanges)
    2025-11-03T18:07:39.056211Z: component=grpc-proxy, tx_id=281474976775656, remote_address=ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]:xxxxx, subject=serviceaccount@as, database=/my_dir/db1, operation=ExecuteQueryRequest, query_text=SELECT * FROM `my_row_table`; status=SUCCESS, detailed_status=StatusSuccess, begin_tx=1, commit_tx=1, end_time=2025-11-03T18:07:39.056204Z, grpc_method=Ydb.Query.V1.QueryService/ExecuteQuery, sanitized_token=xxxxxxxx.**, start_time=2025-11-03T18:07:39.054863Z
    2025-11-03T17:41:44.203214Z: component=monitoring, remote_address=ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx], operation=HTTP REQUEST, method=POST, url=/viewer/query, params=base64=false&schema=multipart, body={"query":"SELECT * FROM `my_row_table`;","database":"/local","action":"execute-query","syntax":"yql_v1"}, status=IN-PROCESS, reason=Execute
    ```

- JSON_LOG_COMPATIBLE

    Формат `JSON_LOG_COMPATIBLE` создаёт записи следующего вида:

    ```json
    {"@timestamp":"2023-03-14T10:41:36.485788Z","@log_type":"audit","paths":"[/my_dir/db1/some_dir]","tx_id":"281474976775658","database":"/my_dir/db1","remote_address":"ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]:xxxxx","status":"SUCCESS","subject":"{none}","detailed_status":"StatusAccepted","operation":"MODIFY ACL","component":"schemeshard","acl_add":"[+(ConnDB):subject:-]"}
    {"@timestamp":"2023-03-13T20:07:30.927210Z","@log_type":"audit","reason":"Check failed: path: '/my_dir/db1/some_dir', error: path exist, request accepts it (id: [OwnerId: 72075186224037889, LocalPathId: 3], type: EPathTypeDir, state: EPathStateNoChanges)","paths":"[/my_dir/db1/some_dir]","tx_id":"844424930216970","database":"/my_dir/db1","remote_address":"ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]:xxxxx","status":"SUCCESS","subject":"{none}","detailed_status":"StatusAlreadyExists","operation":"CREATE DIRECTORY","component":"schemeshard"}
    {"@timestamp":"2025-11-03T18:07:39.056211Z","@log_type":"audit","begin_tx":1,"commit_tx":1,"component":"grpc-proxy","database":"/my_dir/db1","detailed_status":"SUCCESS","end_time":"2025-11-03T18:07:39.056204Z","grpc_method":"Ydb.Query.V1.QueryService/ExecuteQuery","operation":"ExecuteQueryRequest","query_text":"SELECT * FROM `my_row_table`;","remote_address":"ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]","start_time":"2025-11-03T18:07:39.054863Z","status":"SUCCESS","subject":"serviceaccount@as","sanitized_token":"xxxxxxxx.**"}
    {"@timestamp":"2025-11-03T17:41:44.203214Z","@log_type":"audit","component":"monitoring","remote_address":"ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]","operation":"HTTP REQUEST","method":"POST","url":"/viewer/query","params":"base64=false&schema=multipart","body":"{\"query\":\"SELECT * FROM `my_row_table`;\",\"database\":\"/local\",\"action\":\"execute-query\",\"syntax\":\"yql_v1\"}","status":"IN-PROCESS","reason":"Execute"}
    ```

- JSON-конверт

    Шаблон JSON-конверта `{"message": %message%, "source": "ydb-audit-log"}` создаёт записи следующего вида:

    ```json
    {"message":"2023-03-14T10:41:36.485788Z: {\"paths\":\"[/my_dir/db1/some_dir]\",\"tx_id\":\"281474976775658\",\"database\":\"/my_dir/db1\",\"remote_address\":\"ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]:xxxxx\",\"status\":\"SUCCESS\",\"subject\":\"{none}\",\"detailed_status\":\"StatusAccepted\",\"operation\":\"MODIFY ACL\",\"component\":\"schemeshard\",\"acl_add\":\"[+(ConnDB):subject:-]\"}\n","source":"ydb-audit-log"}
    {"message":"2023-03-13T20:07:30.927210Z: {\"reason\":\"Check failed: path: '/my_dir/db1/some_dir', error: path exist, request accepts it (id: [OwnerId: 72075186224037889, LocalPathId: 3], type: EPathTypeDir, state: EPathStateNoChanges)\",\"paths\":\"[/my_dir/db1/some_dir]\",\"tx_id\":\"844424930216970\",\"database\":\"/my_dir/db1\",\"remote_address\":\"ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]:xxxxx\",\"status\":\"SUCCESS\",\"subject\":\"{none}\",\"detailed_status\":\"StatusAlreadyExists\",\"operation\":\"CREATE DIRECTORY\",\"component\":\"schemeshard\"}\n","source":"ydb-audit-log"}
    {"message":"2025-11-03T18:07:39.056211Z: {\"@log_type\":\"audit\",\"begin_tx\":1,\"commit_tx\":1,\"component\":\"grpc-proxy\",\"database\":\"/my_dir/db1\",\"detailed_status\":\"SUCCESS\",\"end_time\":\"2025-11-03T18:07:39.056204Z\",\"grpc_method\":\"Ydb.Query.V1.QueryService/ExecuteQuery\",\"operation\":\"ExecuteQueryRequest\",\"query_text\":\"SELECT * FROM `my_row_table`;\",\"remote_address\":\"ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]\",\"sanitized_token\":\"xxxxxxxx.**\",\"start_time\":\"2025-11-03T18:07:39.054863Z\",\"status\":\"SUCCESS\",\"subject\":\"serviceaccount@as\"}\n","source":"ydb-audit-log"}
    {"message":"2025-11-03T17:41:44.203214Z: {\"component\":\"monitoring\",\"remote_address\":\"ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]\",\"operation\":\"HTTP REQUEST\",\"method\":\"POST\",\"url\":\"/viewer/query\",\"params\":\"base64=false&schema=multipart\",\"body\":\"{\\\"query\\\":\\\"SELECT * FROM `my_row_table`;\\\",\\\"database\\\":\\\"/local\\\",\\\"action\\\":\\\"execute-query\\\",\\\"syntax\\\":\\\"yql_v1\\\"}\",\"status\":\"IN-PROCESS\",\"reason\":\"Execute\"}\n","source":"ydb-audit-log"}
    ```

- Форматированный JSON

    Та же запись аудитного лога, представленная для удобства чтения:

    ```json
    {
      "paths": "[/my_dir/db1/some_dir]",
      "tx_id": "281474976775658",
      "database": "/my_dir/db1",
      "remote_address": "ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]:xxxxx",
      "status": "SUCCESS",
      "subject": "{none}",
      "detailed_status": "StatusAccepted",
      "operation": "MODIFY ACL",
      "component": "schemeshard",
      "acl_add": "[+(ConnDB):subject:-]"
    }
    ```

{% endlist %}
