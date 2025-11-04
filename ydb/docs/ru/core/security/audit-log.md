# Audit log

_Аудитный лог_ — это поток, который содержит информацию обо всех операциях (успешных или не успешных) над объектами {{ ydb-short-name }}. Аудитный лог фиксирует, среди прочего:

* база данных — создание, изменение и удаление;
* директория — создание и удаление;
* таблица — создание, изменение схемы, изменение количества партиций, резервное копирование и восстановление, копирование и переименование, удаление;
* топик — создание, изменение и удаление;
* ACL — изменение;
* SQL-операции — запросы DDL и DML;
* Изменения конфигурации, административные события.

Данные потока аудитного лога могут быть направлены:

* в файл на каждой ноде кластера {{ ydb-short-name }};
* в агент для поставки метрик [Unified Agent](https://cloud.yandex.ru/docs/monitoring/concepts/data-collection/unified-agent/);
* в стандартный вывод ошибок `stderr`.

Можно использовать любое из перечисленных направлений или их комбинацию.

В случае направления в файл доступ к аудитному логу задается правами на уровне файловой системы. Сохранение аудитного лога в файл рекомендуется для использования в промышленных инсталляциях.

Направление аудитного лога в стандартный вывод ошибок `stderr` рекомендуется для тестовых инсталляций. Дальнейшая обработка данных потока определяется настройками [логирования](../devops/observability/logging.md) кластера {{ ydb-short-name }}.

## События аудитного лога {#events}

События аудитного лога генерируются *источниками аудитных событий* — службами или подсистемами {{ ydb-short-name }}, способными их производить. В общем случае, для включения аудита требуется указание одного или нескольких [направлений аудитного потока](#enabling-audit-log) в [конфигурации аудита](#audit-config), однако некоторые источники могут требовать дополнительных параметров или включения [функционального флага](../reference/configuration/feature_flags.md).

*Источник аудитных событий* может создавать отдельное событие аудита, содержащее набор атрибутов. Эти атрибуты делятся на две группы:
* Общие атрибуты, присутствующие у всех *источников аудитных событий*.
* Атрибуты, специфичные для источника, который генерирует событие.

### Общие атрибуты {#common-attributes}

#|
|| Атрибут                | Описание ||
|| `subject`              | SID источника события (формат `<login>@<subsystem>`). Если обязательная аутентификация не включена, атрибут будет иметь значение `{none}`.<br/>*Обязательный.* ||
|| `sanitized_token`      | Частично замаскированный токен аутентификации, использованный для выполнения запроса. Позволяет связывать события, сохраняя исходные учетные данные скрытыми. Если аутентификация не выполнялась, будет иметь значение `{none}`.<br/>*Обязательный.* ||
|| `operation`            | Название операции или действия (например, `ALTER DATABASE`, `CREATE TABLE`).<br/>*Обязательный.* ||
|| `component`            | Уникальный идентификатор *источника аудитных событий*.<br/>*Обязательный.* ||
|| `status`               | Статус завершения операции.<br/>Возможные значения:<ul><li>`SUCCESS` — операция завершена успешно;</li><li>`ERROR` — операция завершилась с ошибкой;</li><li>`IN-PROCESS` — операция выполняется.</li></ul>*Обязательный.* ||
|| `reason`               | Сообщение об ошибке.<br/>*Необязательный.* ||
|| `request_id`           | Уникальный идентификатор запроса, вызвавшего операцию. По `request_id` можно отличать события разных операций и связывать события в единый аудитный контекст операции.<br/>*Необязательный.* ||
|| `remote_address`       | IP-адрес клиента, приславшего запрос.<br/>*Необязательный.* ||
|| `detailed_status`      | Статус, который передает *источник аудитных событий* {{ ydb-short-name }}.<br/>*Необязательный.* ||
|| `database`             | Путь базы данных (например, `/my_dir/db`).<br/>*Необязательный.* ||
|| `cloud_id`             | Идентификатор облака базы данных {{ ydb-short-name }}.<br/>*Необязательный.* ||
|| `folder_id`            | Идентификатор каталога базы данных или кластера {{ ydb-short-name }}.<br/>*Необязательный.* ||
|| `resource_id`          | Идентификатор ресурса базы данных {{ ydb-short-name }}.<br/>*Необязательный.* ||
|#

### Schemeshard {#schemeshard}

* **UID:** `schemeshard`.
* **Регистрируемые операции:** операции со схемой, инициированные DDL-запросами, изменением ACL и управлением пользователями.
* **Как включить:** требуется только [базовая конфигурация аудита](#enabling-audit-log).

#|
|| Атрибут                                 | Описание ||
|| **Общие атрибуты Schemeshard**          | **>** ||
|| `tx_id`                                 | Уникальный идентификатор транзакции.</br>*Обязательный.* ||
|| `paths`                                 | Список путей внутри БД, которые изменяет операция (например, `[/my_dir/db/table-a, /my_dir/db/table-b]`).</br>*Необязательный.* ||
|| **Атрибуты владения и прав доступа**    | **>** ||
|| `new_owner`                             | SID нового владельца объекта при передаче владения. ||
|| `acl_add`                               | Список добавленных разрешений при создании объектов или изменении разрешений в [краткой записи](./short-access-control-notation.md) (например, `[+R:someuser]`). ||
|| `acl_remove`                            | Список удаленных разрешений при изменении разрешений в [краткой записи](./short-access-control-notation.md) (например, `[-R:somegroup]`). ||
|| **Пользовательские атрибуты**           | **>** ||
|| `user_attrs_add`                        | Список добавленных пользовательских атрибутов при создании объектов или изменении атрибутов (например, `[attr_name1: A, attr_name2: B]`). ||
|| `user_attrs_remove`                     | Список удаленных пользовательских атрибутов при изменении атрибутов (например, `[attr_name1, attr_name2]`). ||
|| **Атрибуты аутентификации**      | **>** ||
|| `login_user`                            | Имя пользователя, зафиксированное при операциях входа. ||
|| `login_group`                           | Имя группы, зафиксированное при операциях входа. ||
|| `login_member`                          | Изменения членства. ||
|| `login_user_change`                     | Изменения, применённые к настройкам пользователя. ||
|| `login_user_level`                      | Уровень привилегий пользователя, записанный в событиях аудита. Может принимать значение `admin`. ||
|| **Атрибуты операций импорта/экспорта**  | **>** ||
|| `id`                                    | Уникальный идентификатор операции импорта или экспорта. ||
|| `uid`                                   | Метка, заданная пользователем для операции. ||
|| `start_time`                            | Время начала операции в формате ISO 8601. ||
|| `end_time`                              | Время завершения операции в формате ISO 8601. ||
|| `last_login`                            | Время последнего успешного входа пользователя в формате ISO 8601. ||
|| **Экспорт**                             | **>** ||
|| `export_type`                           | Направление экспорта. Возможные значения: `yt`, `s3`. ||
|| `export_item_count`                     | Количество экспортированных элементов. ||
|| `export_yt_prefix`                      | Префикс пути для экспорта в YT. ||
|| `export_s3_bucket`                      | Имя S3-бакета, используемого для экспорта. ||
|| `export_s3_prefix`                      | Префикс пути для экспорта в S3. ||
|| **Импорт**                              | **>** ||
|| `import_type`                           | Тип источника импорта. Значение всегда `s3`. ||
|| `import_item_count`                     | Количество импортированных элементов. ||
|| `import_s3_bucket`                      | Имя S3-бакета, используемого для импорта. ||
|| `import_s3_prefix`                      | Префикс источника в S3. ||
|#

### GRPC сервисы {#grpc-proxy}

* **UID:** `grpc-proxy`.
* **Регистрируемые операции:** все внешние (не внутренние) gRPC-запросы.
* **Как включить:** требуется указание классов логирования в конфигурации аудита.
* **Классы логирования:** зависят от используемого API — `Ddl`, `Dml`, `Operations`, `ClusterAdmin`, `DatabaseAdmin` и другие классы.
* **Фазы логирования:** `Received`, `Completed`.

#|
|| Атрибут | Описание ||
|| **Общие gRPC-атрибуты** | **>** ||
|| `grpc_method`            | Имя RPC-метода.</br>*Обязательный.* ||
|| `request`                | Обезличенное (санитизированное) представление входящего запроса.</br>*Необязательный.* ||
|| `start_time`             | Время начала операции в формате ISO 8601.</br>*Обязательный.* ||
|| `end_time`               | Время завершения операции в формате ISO 8601.</br>*Необязательный.* ||
|| **Атрибуты транзакций**  | **>** ||
|| `tx_id`                  | Уникальный идентификатор транзакции. Как и `request_id`, может быть использован для различения событий разных операций. ||
|| `begin_tx`               | Флаг `1`, если запрос инициирует новую транзакцию. ||
|| `commit_tx`              | Показывает, коммитит ли запрос транзакцию. Возможные значения: `true`, `false`. ||
|| **Поля запроса**         | **>** ||
|| `query_text`             | Обезличенный текст YQL-запроса. ||
|| `prepared_query_id`      | Идентификатор подготовленного запроса. ||
|| `program_text`           | Содержимое `ExecuteTabletMiniKQL` из TabletService. ||
|| `schema_changes`         | Содержимое `ChangeTabletSchema` из TabletService. ||
|| `table`                  | Полный путь к таблице. ||
|| `row_count`              | Количество строк, затронутых `BulkUpsert`. ||
|| `tablet_id`              | Идентификатор таблетки. ||
|#

### GRPC подключение {#grpc-connection}

* **UID:** `grpc-conn`.
* **Регистрируемые операции:** изменения состояния соединения (подключение/отключение).
* **Как включить:** необходимо активировать [функциональный флаг](../reference/configuration/feature_flags.md) `enable_grpc_audit`.

*Этот источник использует только общие атрибуты.*

### GRPC аутентификация {#grpc-login}

* **UID:** `grpc-login`.
* **Регистрируемые операции:** аутентификация через gRPC.
* **Как включить:** требуется указание классов логирования в [конфигурации аудита](#audit-log-configuration).
* **Классы логирования:** `Login`.
* **Фазы логирования:** `Completed`.

#|
|| Атрибут            | Описание ||
|| `login_user`       | Имя пользователя. *Обязательный.* ||
|| `login_user_level` | Значение, фиксируемое при успешных входах администратора. Возможное значение — `admin`. *Необязательный.* ||
|#

### Сервис мониторинга {#monitoring}

* **UID:** `monitoring`.
* **Регистрируемые операции:** HTTP-запросы, обрабатываемые сервисом мониторинга.
* **Как включить:** требуется указание классов логирования в [конфигурации аудита](#audit-log-configuration).
* **Классы логирования:** `ClusterAdmin`.
* **Фазы логирования:** `Received`, `Completed`.

#|
|| Атрибут  | Описание ||
|| `method` | HTTP-метод запроса. Например `POST`, `GET`.</br>*Обязательный.* ||
|| `url`    | Url запроса без параметров строки запроса. </br>*Обязательный.* ||
|| `params` | Параметры строки запроса в исходном виде.</br>*Необязательный.* ||
|| `body`   | Тело запроса (усечено до 2 МБ с добавлением суффикса `TRUNCATED_BY_YDB`).</br>*Необязательный.* ||
|#

### Heartbeat {#heartbeat}

* **UID:** `audit`.
* **Регистрируемые операции:** периодические [heartbeat-сообщения](#heartbeat-settings) аудита.
* **Как включить:** требуется указание классов логирования в [конфигурации аудита](#audit-log-configuration).
* **Классы логирования:** `AuditHeartbeat`.
* **Фазы логирования:** `Completed`.

#|
|| Атрибут    | Описание ||
|| `node_id`  | Идентификатор ноды, на которой произошло событие. *Обязательный.* ||
|#

### BlobStorage Controller {#bsc}

* **UID:** `bsc`.
* **Регистрируемые операции:** запросы замены конфигурации (`TEvControllerReplaceConfigRequest`), отправляемые консолью.
* **Как включить:** требуется только [базовая конфигурация аудита](#enabling-audit-log).

#|
|| Атрибут      | Описание ||
|| `old_config` | Снапшот предыдущей конфигурации в формате YAML. Снапшот объединяет основную конфигурацию и конфигурацию стораджа. </br>*Необязательный.* ||
|| `new_config` | Снапшот конфигурации, который заменил предыдущий. </br>*Необязательный.* ||
|#

### Distconf {#distconf}

* **UID:** `distconf`.
* **Регистрируемые операции:** изменения распределённой конфигурации.
* **Как включить:** требуется только [базовая конфигурация аудита](#enabling-audit-log).

#|
|| Атрибут      | Описание ||
|| `old_config` | Снапшот конфигурации, которая была активна до принятия распределённого обновления. Distconf сериализует его в YAML. *Обязательный.* ||
|| `new_config` | Снапшот конфигурации, который Distconf зафиксировал после изменения. *Обязательный.* ||
|#

### Web login {#web-login}

* **UID:** `web-login`.
* **Регистрируемые операции:** события аутентификации в виджете веб-консоли {{ ydb-short-name }}.
* **Как включить:** требуется только [базовая конфигурация аудита](#enabling-audit-log).

*Этот источник использует только общие атрибуты.*

### Console {#console}

* **UID:** `console`.
* **Регистрируемые операции:** операции жизненного цикла базы данных и изменения динамической конфигурации.
* **Как включить:** требуется только [базовая конфигурация аудита](#enabling-audit-log).

#|
|| Атрибут      | Описание ||
|| `old_config` | Снапшот конфигурации, которая была активна до применения запроса консоли. *Необязательный.* ||
|| `new_config` | Снапшот конфигурации, который была применён консолью. *Необязательный.* ||
|#

## Конфигурация аудитного лога {#audit-log-configuration}

### Включение аудитного лога {#enabling-audit-log}

Отправка событий в поток аудитного лога включается целиком для кластера {{ ydb-short-name }}. Для *базовой конфигурации* необходимо в [конфигурацию кластера](../reference/configuration/index.md) добавить секцию `audit_config`, указать в ней одно из направлений для потока (`file_backend`, `unified_agent_backend`, `stderr_backend`) или их комбинацию:

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

### Параметры аудита {#audit-config}

Все поля необязательны.

#|
|| Ключ                     | Описание ||
|| `stderr_backend`         | Направить аудитный лог в стандартный вывод ошибок `stderr`. См. [конфигурацию направления аудитного лога](#backend-settings). ||
|| `file_backend`           | Направить аудитный лог в файл на каждой ноде кластера. См. [конфигурацию направления аудитного лога](#backend-settings). ||
|| `unified_agent_backend`  | Направить аудитный лог в Unified Agent. Необходимо также описать секцию `uaclient_config` в [конфигурации кластера](../reference/configuration/index.md). См. [конфигурацию направления аудитного лога](#backend-settings). ||
|| `log_class_config`       | Массив правил аудита для разных классов логов. См. [настройку классов логирования](#log-class-config). ||
|| `heartbeat`              | Конфигурация heartbeat-сообщений. См. [настройку Heartbeat](#heartbeat-settings). ||
|#

### Конфигурация направлений аудитного лога {#backend-settings}

Направления поддерживают следующие поля:

#|
|| Поле                 | Описание ||
|| `format`             | Формат аудитного лога. По умолчанию — `JSON`. См. [формат логов](#log-format).</br>*Необязательный.* ||
|| `file_path`          | Путь к файлу, в который будет направлен аудитный лог. Путь и файл в случае их отсутствия будут созданы на каждой ноде при старте кластера. Если файл существует, запись в него будет продолжена. Только для `file_backend`. <br/>*Обязательный.* ||
|| `log_name`           | Метаданные сессии, которые передаются вместе с сообщением. Позволяют направить логирующий поток в один или несколько дочерних каналов по условию `_log_name: "session_meta_log_name"`. Только для `unified_agent_backend`. <br/>*Необязательный.* ||
|| `log_json_envelope`  | Шаблон, который оборачивает каждую запись лога. Шаблон должен содержать плейсхолдер `%message%`, заменяемый сериализованным сообщением аудита. См. [формат обертки](#envelope).</br>*Необязательный.* ||
|#

#### Формат логов {#log-format}

Поле `format` определяет способ сериализации событий аудита. Поддерживаются следующие форматы:

#|
|| Формат                 | Описание ||
|| `JSON`                 | Каждое событие аудита записывается одной строкой JSON, предварённой временной меткой в формате ISO 8601. </br>Пример: `<time>: {"k1": "v1", "k2": "v2", ...}` </br>*k1, k2, … — поля сообщения; v1, v2, … — их значения.* ||
|| `TXT`                  | Каждое событие записывается как строка `key=value`, предварённая меткой времени ISO 8601. <br/>Пример: `<time>: k1=v1, k2=v2, ...` </br>*k1, k2, … — поля сообщения; v1, v2, … — их значения.* ||
|| `JSON_LOG_COMPATIBLE`  | Каждое событие сериализуется как JSON-объект, совместимый с форматами отладочных логов. Объект включает `@timestamp` (ISO 8601-время) и `@log_type` со значением `audit`. </br>Пример: `{"@timestamp": "<ISO 8601 time>", "@log_type": "audit", "k1": "v1", "k2": "v2", ...}` </br>*@timestamp is ISO 8601 format time string, k1, k2, ..., kn - fields of audit log message and v1, v2, ..., vn are their values* ||
|#

#### Формат обертки {#envelope}

Каждое направление аудитного лога может оборачивать события в пользовательскую обертку с помощью параметра `log_json_envelope`. Шаблон должен содержать метку `%message%`, вместо которой будет записана сериализованная запись события в выбранном формате.

Пример конфигурации, выводящей события аудита в `stderr` в формате JSON с пользовательской оберткой:

```yaml
audit_config:
  stderr_backend:
    format: JSON
    log_json_envelope: '{"audit": %message%, "source": "ydb-audit-log"}'
```

Подробнее см. раздел [Пример с JSON-оберткой](#examples).

### Конфигурация классов логирования {#log-class-config}

Каждая запись `log_class_config` задаёт параметры логирования для определённого класса запросов.

#|
|| Поле                   | Описание ||
|| `log_class`            | Имя класса, к которому применяются настройки. Используются значения из списка [классов логирования](#log-classes). `log_class_config` не должен содержать двух классов с одинаковым именем. <br/>*Обязательный.* ||
|| `enable_logging`       | Включает аудитное логирование для выбранного класса. По умолчанию - выключено.<br/>*Необязательный.* ||
|| `exclude_account_type` | Список типов аккаунтов (`Anonymous`, `User`, `Service`, `ServiceImpersonatedFromUser`), для которых события исключаются из логирования, даже если оно включено.<br/>*Необязательный.* ||
|| `log_phase`            | Список фаз обработки запроса, которые необходимо логировать. См. раздел [Фазы логирования](#log-phases).<br/>*Необязательный.* ||
|#

#### Классы логирования {#log-classes}

Поддерживаемые классы логирования охватывают различные API-интерфейсы.
Если класс не указан в `log_class_config`, используется `Default`.

#|
|| Класс логирования  | Описание ||
|| `ClusterAdmin`     | Запросы администрирования кластера. ||
|| `DatabaseAdmin`    | Запросы администрирования базы данных. ||
|| `Login`            | Запросы входа в систему. ||
|| `NodeRegistration` | Регистрация нод. ||
|| `Ddl`              | DDL-запросы. ||
|| `Dml`              | DML-запросы. ||
|| `Operations`       | Асинхронные RPC-операции, требующие периодического опроса для отслеживания результата. ||
|| `ExportImport`     | Операции импорта и экспорта данных. ||
|| `Acl`              | Операции управления доступом. ||
|| `AuditHeartbeat`   | Синтетические heartbeat-сообщения, подтверждающие активность аудита. ||
|| `Default`          | Настройки по умолчанию для компонентов без отдельной конфигурации. ||
|#

#### Фазы логирования {#log-phases}

Фазы логирования указывают этапы обработки запроса, на которых включена запись аудитных событий.

#|
|| Фаза логирования | Описание ||
|| `Received`       | Запрос получен и прошёл начальные проверки и аутентификацию. Атрибут `status` принимает значение `IN-PROCESS`.<br/> Эта фаза отключена по умолчанию; чтобы включить, добавьте `Received` в `log_class_config.log_phase`. ||
|| `Completed`      | Запрос завершён, атрибут `status` принимает значение `SUCCESS` или `ERROR`.<br/> Эта фаза включена по умолчанию, если `log_class_config.log_phase` не задан. ||
|#

### Настройки Heartbeat {#heartbeat-settings}

События Heartbeat позволяют отслеживать состояние подсистемы аудита. Они позволяют формировать оповещения об отсутствии событий аудита без ложных тревог в периоды, когда активность действительно отсутствует.

Параметр `heartbeat.interval_seconds` задаёт частоту генерации heartbeat-событий. Если указано значение `0`, heartbeat отключён.

### Примеры конфигурации {#config-samples}

Ниже приведён пример простой конфигурации, сохраняющей аудитный лог в файл в формате `TXT`:

```yaml
audit_config:
  file_backend:
    format: TXT
    file_path: "/var/log/ydb-audit.log"
```

Пример конфигурации, отправляющей аудитный лог в Unified Agent в формате `TXT` с меткой `audit`, а также выводящей его в стандартный поток ошибок (`stderr`) в формате `JSON`. Настройки `Default` включают логирование всех классов в фазе `Completed`. Дополнительно `ClusterAdmin` настроен на логирование фазы `Received`, а для `DatabaseAdmin` — отключено логирование для анонимных пользователей:

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

Следующие примеры показывают аудитный лог, записанный при использовании различных [настроек направления аудита](#backend-settings).

{% list tabs %}

- JSON

    Для формата `JSON` аудитный лог выглядит так:

    ```json
    2023-03-14T10:41:36.485788Z: {"paths":"[/my_dir/db1/some_dir]","tx_id":"281474976775658","database":"/my_dir/db1","remote_address":"ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]:xxxxx","status":"SUCCESS","subject":"{none}","sanitized_token":"{none}", "detailed_status":"StatusAccepted","operation":"MODIFY ACL","component":"schemeshard","acl_add":"[+(ConnDB):subject:-]"}
    2023-03-13T20:07:30.927210Z: {"reason":"Check failed: path: '/my_dir/db1/some_dir', error: path exist, request accepts it (id: [OwnerId: 72075186224037889, LocalPathId: 3], type: EPathTypeDir, state: EPathStateNoChanges)","paths":"[/my_dir/db1/some_dir]","tx_id":"844424930216970","database":"/my_dir/db1","remote_address":"ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]:xxxxx","status":"SUCCESS","subject":"{none}","sanitized_token":"{none}","detailed_status":"StatusAlreadyExists","operation":"CREATE DIRECTORY","component":"schemeshard"}
    2025-11-03T18:07:39.056211Z: {"@log_type":"audit","begin_tx":1,"commit_tx":1,"component":"grpc-proxy","database":"/my_dir/db1","detailed_status":"SUCCESS","end_time":"2025-11-03T18:07:39.056204Z","grpc_method":"Ydb.Query.V1.QueryService/ExecuteQuery","operation":"ExecuteQueryRequest","query_text":"SELECT * FROM `my_row_table`;","remote_address":"ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]","sanitized_token":"xxxxxxxx.**","start_time":"2025-11-03T18:07:39.054863Z","status":"SUCCESS","subject":"serviceaccount@as"}
    2025-11-03T17:41:44.203214Z: {"component":"monitoring","remote_address":"ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]","operation":"HTTP REQUEST","method":"POST","url":"/viewer/query","params":"base64=false&schema=multipart","body":"{\"query\":\"SELECT * FROM `my_row_table`;\",\"database\":\"/local\",\"action\":\"execute-query\",\"syntax\":\"yql_v1\"}","status":"IN-PROCESS","reason":"Execute"}
    ```

- TXT

    Для формата `TXT` аудитный лог выглядит так:

    ```txt
    2023-03-14T10:41:36.485788Z: component=schemeshard, tx_id=281474976775658, remote_address=ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]:xxxxx, subject={none}, database=/my_dir/db1, operation=MODIFY ACL, paths=[/my_dir/db1/some_dir], status=SUCCESS, detailed_status=StatusSuccess, acl_add=[+(ConnDB):subject:-]
    2023-03-13T20:07:30.927210Z: component=schemeshard, tx_id=281474976775657, remote_address=ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]:xxxxx, subject={none}, database=/my_dir/db1, operation=CREATE DIRECTORY, paths=[/my_dir/db1/some_dir], status=SUCCESS, detailed_status=StatusAlreadyExists, reason=Check failed: path: '/my_dir/db1/some_dir', error: path exist, request accepts it (id: [OwnerId: 72075186224037889, LocalPathId: 3], type: EPathTypeDir, state: EPathStateNoChanges)
    2025-11-03T18:07:39.056211Z: component=grpc-proxy, tx_id=281474976775656, remote_address=ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]:xxxxx, subject=serviceaccount@as, database=/my_dir/db1, operation=ExecuteQueryRequest, query_text=SELECT * FROM `my_row_table`; status=SUCCESS, detailed_status=StatusSuccess, begin_tx=1, commit_tx=1, end_time=2025-11-03T18:07:39.056204Z, grpc_method=Ydb.Query.V1.QueryService/ExecuteQuery, sanitized_token=xxxxxxxx.**, start_time=2025-11-03T18:07:39.054863Z
    2025-11-03T17:41:44.203214Z: component=monitoring, remote_address=ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx], operation=HTTP REQUEST, method=POST, url=/viewer/query, params=base64=false&schema=multipart, body={"query":"SELECT * FROM `my_row_table`;","database":"/local","action":"execute-query","syntax":"yql_v1"}, status=IN-PROCESS, reason=Execute
    ```

- JSON_LOG_COMPATIBLE

    Для формата `JSON_LOG_COMPATIBLE` аудитный лог выглядит так:

    ```json
    {"@timestamp":"2023-03-14T10:41:36.485788Z","@log_type":"audit","paths":"[/my_dir/db1/some_dir]","tx_id":"281474976775658","database":"/my_dir/db1","remote_address":"ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]:xxxxx","status":"SUCCESS","subject":"{none}","detailed_status":"StatusAccepted","operation":"MODIFY ACL","component":"schemeshard","acl_add":"[+(ConnDB):subject:-]"}
    {"@timestamp":"2023-03-13T20:07:30.927210Z","@log_type":"audit","reason":"Check failed: path: '/my_dir/db1/some_dir', error: path exist, request accepts it (id: [OwnerId: 72075186224037889, LocalPathId: 3], type: EPathTypeDir, state: EPathStateNoChanges)","paths":"[/my_dir/db1/some_dir]","tx_id":"844424930216970","database":"/my_dir/db1","remote_address":"ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]:xxxxx","status":"SUCCESS","subject":"{none}","detailed_status":"StatusAlreadyExists","operation":"CREATE DIRECTORY","component":"schemeshard"}
    {"@timestamp":"2025-11-03T18:07:39.056211Z","@log_type":"audit","begin_tx":1,"commit_tx":1,"component":"grpc-proxy","database":"/my_dir/db1","detailed_status":"SUCCESS","end_time":"2025-11-03T18:07:39.056204Z","grpc_method":"Ydb.Query.V1.QueryService/ExecuteQuery","operation":"ExecuteQueryRequest","query_text":"SELECT * FROM `my_row_table`;","remote_address":"ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]","start_time":"2025-11-03T18:07:39.054863Z","status":"SUCCESS","subject":"serviceaccount@as","sanitized_token":"xxxxxxxx.**"}
    {"@timestamp":"2025-11-03T17:41:44.203214Z","@log_type":"audit","component":"monitoring","remote_address":"ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]","operation":"HTTP REQUEST","method":"POST","url":"/viewer/query","params":"base64=false&schema=multipart","body":"{\"query\":\"SELECT * FROM `my_row_table`;\",\"database\":\"/local\",\"action\":\"execute-query\",\"syntax\":\"yql_v1\"}","status":"IN-PROCESS","reason":"Execute"}
    ```

- Envelope JSON

    Для шаблона JSON-обертки `{"message": %message%, "source": "ydb-audit-log"}` аудитный лог выглядит так:

    ```json
    {"message":"2023-03-14T10:41:36.485788Z: {\"paths\":\"[/my_dir/db1/some_dir]\",\"tx_id\":\"281474976775658\",\"database\":\"/my_dir/db1\",\"remote_address\":\"ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]:xxxxx\",\"status\":\"SUCCESS\",\"subject\":\"{none}\",\"detailed_status\":\"StatusAccepted\",\"operation\":\"MODIFY ACL\",\"component\":\"schemeshard\",\"acl_add\":\"[+(ConnDB):subject:-]\"}\n","source":"ydb-audit-log"}
    {"message":"2023-03-13T20:07:30.927210Z: {\"reason\":\"Check failed: path: '/my_dir/db1/some_dir', error: path exist, request accepts it (id: [OwnerId: 72075186224037889, LocalPathId: 3], type: EPathTypeDir, state: EPathStateNoChanges)\",\"paths\":\"[/my_dir/db1/some_dir]\",\"tx_id\":\"844424930216970\",\"database\":\"/my_dir/db1\",\"remote_address\":\"ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]:xxxxx\",\"status\":\"SUCCESS\",\"subject\":\"{none}\",\"detailed_status\":\"StatusAlreadyExists\",\"operation\":\"CREATE DIRECTORY\",\"component\":\"schemeshard\"}\n","source":"ydb-audit-log"}
    {"message":"2025-11-03T18:07:39.056211Z: {\"@log_type\":\"audit\",\"begin_tx\":1,\"commit_tx\":1,\"component\":\"grpc-proxy\",\"database\":\"/my_dir/db1\",\"detailed_status\":\"SUCCESS\",\"end_time\":\"2025-11-03T18:07:39.056204Z\",\"grpc_method\":\"Ydb.Query.V1.QueryService/ExecuteQuery\",\"operation\":\"ExecuteQueryRequest\",\"query_text\":\"SELECT * FROM `my_row_table`;\",\"remote_address\":\"ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]\",\"sanitized_token\":\"xxxxxxxx.**\",\"start_time\":\"2025-11-03T18:07:39.054863Z\",\"status\":\"SUCCESS\",\"subject\":\"serviceaccount@as\"}\n","source":"ydb-audit-log"}
    {"message":"2025-11-03T17:41:44.203214Z: {\"component\":\"monitoring\",\"remote_address\":\"ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]\",\"operation\":\"HTTP REQUEST\",\"method\":\"POST\",\"url\":\"/viewer/query\",\"params\":\"base64=false&schema=multipart\",\"body\":\"{\\\"query\\\":\\\"SELECT * FROM `my_row_table`;\\\",\\\"database\\\":\\\"/local\\\",\\\"action\\\":\\\"execute-query\\\",\\\"syntax\\\":\\\"yql_v1\\\"}\",\"status\":\"IN-PROCESS\",\"reason\":\"Execute\"}\n","source":"ydb-audit-log"}
    ```

- Pretty-JSON

    Ниже приведена та же запись аудита в более удобочитаемом формате:

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
