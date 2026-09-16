### Параметры аудитного лога (`AUDIT_PART`)

Справочник полей, которые прикрепляются к аудитным событиям через `AUDIT_PART` / `AddAuditLogPart`.

На уровне макроса все значения — `TString` (`TVector<std::pair<TString, TString>>`). Пустые значения макрос отбрасывает; вместо них часто пишут `{none}`. Ниже тип — семантический.

Макросы: `ydb/core/audit/audit_log.h` (`AUDIT_PART`, `AUDIT_PART_COND`, `AUDIT_PART_NO_COND`).

Официальные описания атрибутов: [audit-log.md](../docs/ru/core/security/audit-log.md). В коде есть поля, которых в доке нет (FS export/import, весь набор YMQ).

---

#### `ydb/core/tx/schemeshard/schemeshard_audit_log.cpp`

**UID:** `schemeshard`.

| Поле | Тип | Описание |
|---|---|---|
| `component` | Utf8 | Всегда `schemeshard` |
| `tx_id` | Utf8 (число) | Идентификатор транзакции схемы |
| `remote_address` | Utf8 | IP клиента |
| `subject` | Utf8 | SID субъекта; `{none}`, если нет |
| `sanitized_token` | Utf8 | Частично маскированный токен |
| `database` | Utf8 | Путь БД |
| `operation` | Utf8 | Имя операции схемы (`CREATE TABLE`, …) или `EXPORT START` / `EXPORT END` / `IMPORT START` / `IMPORT END` |
| `paths` | Utf8 (список) | Пути, которые меняет операция: `[/db/t1, /db/t2]` |
| `status` | Utf8 (enum) | `SUCCESS` / `ERROR` (для scheme-op `StatusAccepted` тоже `SUCCESS`) |
| `detailed_status` | Utf8 | `NKikimrScheme::EStatus` или `Ydb::StatusIds` |
| `reason` | Utf8 | Текст ошибки |
| `cloud_id` | Utf8 | Атрибут `cloud_id` домена |
| `folder_id` | Utf8 | Атрибут `folder_id` домена |
| `resource_id` | Utf8 | Атрибут `database_id` домена |
| `new_owner` | Utf8 | SID нового владельца |
| `acl_add` | Utf8 (список) | Добавленные права в краткой записи (`[+R:user]`) |
| `acl_remove` | Utf8 (список) | Отозванные права (`[-R:user]`) |
| `user_attrs_add` | Utf8 (список) | Добавленные пользовательские атрибуты |
| `user_attrs_remove` | Utf8 (список) | Удалённые пользовательские атрибуты |
| `login_user` | Utf8 | Имя пользователя (AlterLogin) |
| `login_group` | Utf8 | Имя группы |
| `login_member` | Utf8 | Изменение членства в группе |
| `login_user_change` | Utf8 (список) | Изменения настроек пользователя |
| `last_login` | Timestamp (ISO 8601) | Время последнего успешного входа (через `additionalParts`) |
| `id` | Utf8 (число) | Идентификатор export/import |
| `uid` | Utf8 | Пользовательская метка операции |
| `start_time` | Timestamp (ISO 8601) | Начало export/import |
| `end_time` | Timestamp (ISO 8601) | Конец export/import |
| `export_type` | Utf8 (enum) | `yt` / `s3` / `fs` |
| `export_item_count` | Utf8 (число) | Число элементов экспорта |
| `export_yt_prefix` | Utf8 | Префикс пути в YT |
| `export_s3_bucket` | Utf8 | S3-бакет экспорта |
| `export_s3_prefix` | Utf8 | Префикс в S3 |
| `export_fs_base_path` | Utf8 | Базовый путь FS-экспорта (в доке нет) |
| `import_type` | Utf8 (enum) | `s3` / `fs` |
| `import_item_count` | Utf8 (число) | Число элементов импорта |
| `import_s3_bucket` | Utf8 | S3-бакет импорта |
| `import_s3_prefix` | Utf8 | Префикс источника в S3 |
| `import_fs_base_path` | Utf8 | Базовый путь FS-импорта (в доке нет) |

---

#### `ydb/core/grpc_services/audit_log.cpp`

**UID:** `grpc-conn` (`AuditLogConn`) и `grpc-proxy` (`AuditLog`).

| Поле | Тип | Описание |
|---|---|---|
| `component` | Utf8 | `grpc-conn` или `grpc-proxy` |
| `remote_address` | Utf8 | IP клиента |
| `subject` | Utf8 | SID субъекта |
| `sanitized_token` | Utf8 | Частично маскированный токен |
| `database` | Utf8 | Путь БД |
| `operation` | Utf8 | Имя RPC (`GetRequestName`) |
| `status` | Utf8 (enum) | `SUCCESS` / `ERROR` / `IN-PROCESS` |
| `detailed_status` | Utf8 | `Ydb::StatusIds` или числовой код |
| `reason` | Utf8 | Например `No permission to connect to the database` |

Остальные поля `grpc-proxy` приходят через `AddAuditLogPart` и проходят `AUDIT_PART(name, value)`.

---

#### `ydb/core/grpc_services/audit_dml_operations.cpp`

Поля складываются в `AuditLogParts` и уходят в `grpc-proxy`.

| Поле | Тип | Описание |
|---|---|---|
| `remote_address` | Utf8 | IP клиента |
| `subject` | Utf8 | SID субъекта |
| `sanitized_token` | Utf8 | Частично маскированный токен |
| `database` | Utf8 | Путь БД |
| `operation` | Utf8 | Имя RPC |
| `start_time` | Timestamp (ISO 8601) | Начало обработки |
| `end_time` | Timestamp (ISO 8601) | Конец обработки |
| `cloud_id` | Utf8 | Атрибут БД |
| `folder_id` | Utf8 | Атрибут БД |
| `resource_id` | Utf8 | `database_id` |
| `tx_id` | Utf8 | Идентификатор транзакции |
| `begin_tx` | Utf8 (флаг) | `"1"`, если запрос открывает транзакцию |
| `commit_tx` | Utf8 (bool) | `true` / `false` |
| `query_text` | Utf8 | Санитизированный YQL (до 1024 символов, одна строка) |
| `prepared_query_id` | Utf8 | Идентификатор подготовленного запроса |
| `table` | Utf8 | Полный путь таблицы (BulkUpsert) |
| `row_count` | Utf8 (число) | Число строк пакетной вставки |
| `program_text` | Utf8 | MiniKQL-программа |
| `schema_changes` | Utf8 | Описание смены схемы таблетки |
| `tablet_id` | Utf8 (число) | Идентификатор таблетки |

---

#### `ydb/core/grpc_services/grpc_request_check_actor.h`

| Поле | Тип | Описание |
|---|---|---|
| `grpc_method` | Utf8 | Имя RPC-метода |

---

#### `ydb/core/grpc_services/legacy/rpc_legacy.cpp`

| Поле | Тип | Описание |
|---|---|---|
| `request` | Utf8 | Санитизированное однострочное представление protobuf-запроса |

---

#### `ydb/core/audit/login_op.cpp`

**UID:** обычно `grpc-login` (передаётся аргументом `componentName`).

| Поле | Тип | Описание |
|---|---|---|
| `component` | Utf8 | Имя источника (как правило `grpc-login`) |
| `remote_address` | Utf8 | IP клиента |
| `database` | Utf8 | Путь БД |
| `operation` | Utf8 | Всегда `LOGIN` |
| `status` | Utf8 (enum) | `SUCCESS` / `ERROR` |
| `detailed_status` | Utf8 | `Ydb::StatusIds` при ошибке |
| `reason` | Utf8 | Текст ошибки (+ `errorDetails`) |
| `login_user` | Utf8 | Имя пользователя |
| `sanitized_token` | Utf8 | Частично маскированный токен |
| `login_user_level` | Utf8 (enum) | `admin` при успешном входе администратора |

---

#### `ydb/core/security/login_page.cpp`

**UID:** `web-login`.

| Поле | Тип | Описание |
|---|---|---|
| `component` | Utf8 | Всегда `web-login` |
| `remote_address` | Utf8 | IP клиента |
| `subject` | Utf8 | SID субъекта |
| `sanitized_token` | Utf8 | Частично маскированный токен |
| `operation` | Utf8 | Logout |
| `status` | Utf8 | Всегда `SUCCESS` |

---

#### `ydb/core/mon/audit/audit.cpp`

**UID:** `monitoring`.

| Поле | Тип | Описание |
|---|---|---|
| `component` | Utf8 | Всегда `monitoring` |
| `remote_address` | Utf8 | IP из `X-Forwarded-For` или сокета |
| `operation` | Utf8 | По умолчанию `HTTP REQUEST` |
| `method` | Utf8 | HTTP-метод (`GET`, `POST`, …) |
| `url` | Utf8 | Путь без query-параметров |
| `params` | Utf8 | Строка запроса как есть |
| `body` | Utf8 | Тело, обрезается до 2 МБ + `**TRUNCATED_BY_YDB**` |
| `status` | Utf8 (enum) | `SUCCESS` / `IN-PROCESS` / `ERROR` |
| `reason` | Utf8 | `HTTP status + message` или `Execute` |
| `subject` | Utf8 | Из разрешённого списка доп. частей |
| `sanitized_token` | Utf8 | Из разрешённого списка доп. частей |
| `cloud_id` | Utf8 | Из разрешённого списка доп. частей |
| `folder_id` | Utf8 | Из разрешённого списка доп. частей |
| `resource_id` | Utf8 | Из разрешённого списка доп. частей |

---

#### `ydb/core/audit/heartbeat_actor/heartbeat_actor.cpp`

**UID в коде:** `audit-service` (в доке указан `audit`).

| Поле | Тип | Описание |
|---|---|---|
| `component` | Utf8 | Всегда `audit-service` |
| `subject` | Utf8 | Всегда `metadata@system` |
| `sanitized_token` | Utf8 | Всегда `{none}` |
| `operation` | Utf8 | Всегда `HEARTBEAT` |
| `status` | Utf8 | Всегда `SUCCESS` |
| `node_id` | Utf8 (число) | Нода, с которой ушёл heartbeat |

---

#### `ydb/core/mind/bscontroller/bsc_audit.cpp`

**UID:** `bsc`.

| Поле | Тип | Описание |
|---|---|---|
| `component` | Utf8 | Всегда `bsc` |
| `remote_address` | Utf8 | IP клиента |
| `subject` | Utf8 | SID субъекта |
| `sanitized_token` | Utf8 | Частично маскированный токен |
| `status` | Utf8 (enum) | `SUCCESS` / `ERROR` |
| `reason` | Utf8 | Текст ошибки |
| `operation` | Utf8 | Всегда `REPLACE CONFIG` |
| `old_config` | Utf8 (YAML) | Снапшот конфигурации до изменения |
| `new_config` | Utf8 (YAML) | Снапшот после изменения |

---

#### `ydb/core/blobstorage/nodewarden/distconf_invoke_common.cpp`

**UID:** `distconf`.

| Поле | Тип | Описание |
|---|---|---|
| `component` | Utf8 | Всегда `distconf` |
| `remote_address` | Utf8 | IP клиента |
| `subject` | Utf8 | SID субъекта |
| `sanitized_token` | Utf8 | Частично маскированный токен |
| `status` | Utf8 | Всегда `SUCCESS` |
| `reason` | Utf8 | Зарезервировано (условие `false`, не пишется) |
| `operation` | Utf8 | Всегда `REPLACE CONFIG` |
| `old_config` | Utf8 (YAML) | Снапшот до обновления |
| `new_config` | Utf8 (YAML) | Снапшот после обновления |

---

#### `ydb/core/cms/console/console_audit.cpp`

**UID:** `console`.

| Поле | Тип | Описание |
|---|---|---|
| `component` | Utf8 | Всегда `console` |
| `remote_address` | Utf8 | IP клиента |
| `subject` | Utf8 | SID субъекта |
| `sanitized_token` | Utf8 | Частично маскированный токен |
| `database` | Utf8 | Путь БД (кроме `REPLACE DYNCONFIG`) |
| `status` | Utf8 (enum) | `SUCCESS` / `ERROR` |
| `reason` | Utf8 | Текст ошибки |
| `operation` | Utf8 | `REPLACE DYNCONFIG`, `REPLACE DATABASE CONFIG`, `BEGIN INIT DATABASE CONFIG`, `END INIT DATABASE CONFIG`, `BEGIN REMOVE DATABASE`, `END REMOVE DATABASE` |
| `old_config` | Utf8 (YAML) | Снапшот до изменения (replace-операции) |
| `new_config` | Utf8 (YAML) | Снапшот после изменения (replace-операции) |

---

#### `ydb/core/ymq/actor/action.h`

**UID:** `ymq`. В публичной доке нет.

| Поле | Тип | Описание |
|---|---|---|
| `component` | Utf8 | Всегда `ymq` |
| `request_id` | Utf8 | Идентификатор запроса |
| `subject` | Utf8 | SID субъекта |
| `account` | Utf8 | Аккаунт YMQ |
| `cloud_id` | Utf8 | В cloud-режиме — то же, что `account` |
| `folder_id` | Utf8 | Каталог (только cloud-режим) |
| `resource_id` | Utf8 | Имя очереди (только cloud-режим) |
| `operation` | Utf8 | Cloud-имя действия |
| `queue` | Utf8 | Имя очереди |
| `status` | Utf8 (enum) | `SUCCESS` / `ERROR` |
| `reason` | Utf8 | Текст ошибки |
| `detailed_status` | Utf8 | Код ошибки YMQ |

---

#### `ydb/core/ymq/actor/cloud_events/cloud_events.cpp`

**UID:** `ymq`. В публичной доке нет.

| Поле | Тип | Описание |
|---|---|---|
| `component` | Utf8 | Всегда `ymq` |
| `id` | Utf8 | Идентификатор cloud-event |
| `operation` | Utf8 | Тип события (`CreateMessageQueue`, …) |
| `status` | Utf8 (enum) | `SUCCESS` / `ERROR` |
| `reason` | Utf8 | Текст ошибки |
| `remote_address` | Utf8 | IP клиента |
| `subject` | Utf8 | SID субъекта |
| `masked_token` | Utf8 | Маскированный IAM-токен (аналог `sanitized_token`) |
| `auth_type` | Utf8 | `service_account` / `federated_account` / `user_account` |
| `permission` | Utf8 | Проверяемое разрешение |
| `created_at` | Timestamp | Время создания cloud-event |
| `cloud_id` | Utf8 | Идентификатор облака |
| `folder_id` | Utf8 | Идентификатор каталога |
| `resource_id` | Utf8 | Идентификатор ресурса / очереди |
| `request_id` | Utf8 | Идентификатор запроса |
| `idempotency_id` | Utf8 | Идентификатор идемпотентности |
| `queue` | Utf8 | Имя очереди |
| `labels` | Utf8 (JSON) | Метки очереди |
