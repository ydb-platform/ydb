# Аудитное логирование

_Аудитный лог_ &mdash; это поток, в котором фиксируются действия пользователей или сервисов с объектами и данными в {{ ydb-short-name }}.

С помощью аудитного лога можно следить за изменениями [объектов схемы](audit-log-scheme.md), [данных в таблицах](audit-log-dml.md) и за [клиентскими соединениями](audit-log-conn.md). Базовая информация, которую даёт аудитный лог: какие действия были выполнены над какими объектами с какого ip-адреса от имени какого пользователя или сервиса. Аудитный лог помогает организациям создавать исторический отчёт о деятельности в целях обеспечения соответствия нормативным требованиям или применения других политик.

## Записи и атрибуты аудитного лога

Информация о действиях или операциях записывается в аудитный лог в виде записей. Каждая запись состоит из набора полей или атрибутов. Одни атрибуты являются одинаковыми для всех записей, другие определяются видом лога или типом описываемой операции.

[//]: # (Атрибуты, обязательно присутствующие в записях, отмечены явно как _Обязательные_.)
[//]: # (| `request_id`| Уникальный идентификатор запроса, вызвавшего операцию. По `request_id` можно различать разные операции и связывать записи в единый аудитный контекст операции.)

| __Атрибут__ | __Описание__ |
|:----|:----|
| __Общие атрибуты__ ||
| `component` | Имя компонента {{ ydb-short-name }} — источника записи (например, `schemeshard`, `grpc-conn`, `grpc-proxy`).
| `subject`| User SID пользователя, от имени которого выполняется операция. Формат `<login>@<subsystem>`). Если обязательная аутентификация не включена, атрибут будет иметь значение `{none}`.<br>_Обязательный_.
| `operation`| Название операции или действия.<br>_Обязательный_.
| `database` | Путь базы данных, в которой выполняется операция (например, `/root/db`).
| `status` | Общий статус операции.<br/>Возможные значения:<ul><li>`SUCCESS` — операция завершена успешно;</li><li>`ERROR` — операция завершена с ошибкой;</li></ul>_Обязательный_.
| `detailed_status` | Детальный статус операции. Cпецифичный для компонента.
| `reason` | Сообщение об ошибке, свободный формат.
| `remote_address` | IP-адрес клиента, приславшего запрос.
| `start_time` | Время начала операции.
| `end_time` | Время окончания операции.
| __Атрибуты схемных операций__ | (компонент `schemeshard`)
| `tx_id`| Уникальный идентификатор транзакции. Как и `request_id`, может быть использован для различения азных операций.<br>_Обязательный_.
| `paths` | Список путей внутри базы данных, которые изменяет операция (например, `[/root/db/table-a, /root/db/table-b]`).<br>_Обязательный_.
| _Владение и разрешения_ ||
| `new_owner` | User SID нового владельца объекта при передаче владения.
| `acl_add` | Список добавленных разрешений при создании объектов или изменении разрешений, в [краткой записи](./short-access-control-notation.md) (например, `[+R:someuser]`).
| `acl_remove` | Список удаленных разрешений при изменении разрешений, в [краткой записи](./short-access-control-notation.md) (например, `[-R:somegroup]`).
| _[Пользовательские атрибуты](../dev/custom-attributes.md)_ ||
| `user_attrs_add` | Список добавленных пользовательских атрибутов при создании объектов или изменении атрибутов (например, `[attr1: A, attr2: B]`).
| `user_attrs_remove` | Список удаленных пользовательских атрибутов при изменении атрибутов (например, `[attr1, attr2]`).
| __Атрибуты операций с [учётными данными](../concepts/auth#static-credentials)__ | (компонент `schemeshard`)
| `login_user` | Имя пользователя (при [создании или модификации пользователя](./access-management.md#users)).
| `login_group` | Имя группы (при [создании, модификации групп](./access-management.md#groups)).
| `login_member` | Имя пользователя (при [добавлении, удалении из группы](./access-management.md#groups)).
| __Атрибуты DML-операций__ | (компонент `grpc-proxy`)
| `query_text` | Текст запроса.
| `prepared_query_id` | Идентификатор подготовленного запроса.
| `tx_id` | Уникальный идентификатор транзакции.
| `begin_tx` | Запрос на неявный старт транзакции.
| `commit_tx` | Запрос на неявный commit транзакции по завершении запроса.
| `table` | Путь изменяемой таблицы.
| `row_count` | Количество изменённых строк.

## Настройка и включение

Подсистема аудитного логирования {{ ydb-short-name }} поддерживает запись потока в:

* файл на каждом узле кластера {{ ydb-short-name }};
* стандартный вывод ошибок `stderr`;
* [Unified Agent](https://cloud.yandex.ru/docs/monitoring/concepts/data-collection/unified-agent/) &mdash; агент сбора и доставки логов и метрик.

Возможно использовать любое из перечисленных назначений, а также их комбинацию.

Запись в файл или unified-agent рекомендуется для использования в промышленных инсталляциях. В случае записи в файл доступ к аудитному логу задается правами на уровне файловой системы.

Запись в `stderr` рекомендуется для тестовых инсталляций. На дальнейшую обработку `stderr` могут влиять настройки подсистемы [логирования](../devops/manual/logging.md) {{ ydb-short-name }}.

### Конфигурация

Аудитное логирование конфигурируется на уровне кластера {{ ydb-short-name }}.

Для этого необходимо добавить секцию `audit_config` со списком бекендов в [конфигурацию кластера](../deploy/configuration/config.md):
```yaml
audit_config:
  file_backend:
    format: OUTPUT_FORMAT
    file_path: "path-to-file"
  unified_agent_backend:
    format: OUTPUT_FORMAT
    log_name: session_meta_log_name
  stderr_backend:
    format: OUTPUT_FORMAT
```

Никакой бекенд `file_backend`, `unified_agent_backend`, `stderr_backend` не является обязательным, но должен быть указан хотя бы один. Секция `audit_config` не может быть пустой.


Ключ | Описание
--- | ---
`*.format` | Формат записей аудитного лога.<br>Возможные значения:<ul><li>`JSON` — [JSON](https://{{ lang }}.wikipedia.org/wiki/JSON) в сериализованном виде;</li><li>`TXT` — текст.</ul>_Необязательный_.<br>Значение по умолчанию: `JSON`.
`stderr_backend` | Бекенд записи аудитного лога в стандартный поток `stderr`.</ul>
`file_backend` | Бекенд записи аудитного лога в локальный файл.
`file_backend.file_path` | Путь к файлу, в который будет направлен аудитный лог. Путь и файл будут созданы на каждом узле при старте узла, в случае их отсутствия. Если файл существует, запись в него будет продолжена.<br>_Обязательный для `file_backend`_.
`unified_agent_backend` | Бекенд записи аудитного лога в Unified Agent.<br>В [конфигурации кластера](../deploy/configuration/config.md) требуется дополнительно настроить секцию `uaclient_config`.
`unified_agent_backend.log_name` | Поле в метаданных сессии, которое будет передаваться вместе с сообщениями. Позволяет направить логирующий поток в один или несколько дочерних каналов по условию `_log_name: "session_meta_log_name"`.<br>_Необязательный_.<br>По-умолчанию берётся значение `uaclient_config.log_name`.

### Включение {#enabling-audit-log}

Аудитное логирование по-умолчанию отключено.

Добавление секции `audit_config` в [конфигурацию кластера](../deploy/configuration/config.md) достаточно для включения аудитного логирования.

Дополнительные требования для включения логирования отдельных активностей описаны в соответствующих разделах: [Client connections](./audit-log-conn.md#включение), [Schema operations](./audit-log-scheme.md#включение), [DML operations](./audit-log-dml.md#включение-и-настройка) .

Пример конфигурации для сохранения аудитного лога в файл `/var/log/ydb-audit.log` в формате `TXT`:

```yaml
audit_config:
  file_backend:
    format: TXT
    file_path: "/var/log/ydb-audit.log"
```

Пример конфигурации для отправки аудитного лога в Unified Agent с меткой `audit` в формате `TXT`, а также вывода его в `stderr` в формате `JSON``

```yaml
audit_config:
  unified_agent_backend:
    format: TXT
    log_name: audit
  stderr_backend:
    format: JSON
```

## Примеры лога {#examples}

В формате `JSON`:

```json
2023-03-13T20:05:19.776132Z: {"paths":"[/root/db1/some_dir]","tx_id":"562949953476313","database":"/root/db1","remote_address":"xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx","status":"SUCCESS","subject":"{none}","detailed_status":"StatusAccepted","operation":"CREATE DIRECTORY","component":"schemeshard"}
2023-03-13T20:07:30.927210Z: {"reason":"Check failed: path: '/root/db1/some_dir', error: path exist, request accepts it (id: [OwnerId: 72075186224037889, LocalPathId: 3], type: EPathTypeDir, state: EPathStateNoChanges)","paths":"[/root/db1/some_dir]","tx_id":"844424930216970","database":"/root/db1","remote_address":"xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx","status":"SUCCESS","subject":"{none}","detailed_status":"StatusAlreadyExists","operation":"CREATE DIRECTORY","component":"schemeshard"}
2023-03-13T19:59:27.614731Z: {"paths":"[/root/db1/some_table]","tx_id":"562949953426315","database":"/root/db1","remote_address":"{none}","status":"SUCCESS","subject":"{none}","detailed_status":"StatusAccepted","operation":"CREATE TABLE","component":"schemeshard"}
2023-03-13T20:10:44.345767Z: {"paths":"[/root/db1/some_table, /root/db1/another_table]","tx_id":"562949953506313","database":"{none}","remote_address":"xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx","status":"SUCCESS","subject":"{none}","detailed_status":"StatusAccepted","operation":"ALTER TABLE RENAME","component":"schemeshard"}
2023-03-14T10:41:36.485788Z: {"paths":"[/root/db1/some_dir]","tx_id":"281474976775658","database":"/root/db1","remote_address":"xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx","status":"SUCCESS","subject":"{none}","detailed_status":"StatusAccepted","operation":"MODIFY ACL","component":"schemeshard","acl_add":"[+(ConnDB):subject:-]"}
```

В формате `TXT`:

```txt
2023-03-13T20:05:19.776132Z: component=schemeshard, tx_id=844424930186969, remote_address=xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx, subject={none}, database=/root/db1, operation=CREATE DIRECTORY, paths=[/root/db1/some_dir], status=SUCCESS, detailed_status=StatusAccepted
2023-03-13T20:07:30.927210Z: component=schemeshard, tx_id=281474976775657, remote_address=xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx, subject={none}, database=/root/db1, operation=CREATE DIRECTORY, paths=[/root/db1/some_dir], status=SUCCESS, detailed_status=StatusAlreadyExists, reason=Check failed: path: '/root/db1/some_dir', error: path exist, request accepts it (id: [OwnerId: 72075186224037889, LocalPathId: 3], type: EPathTypeDir, state: EPathStateNoChanges)
2023-03-13T19:59:27.614731Z: component=schemeshard, tx_id=562949953426315, remote_address={none}, subject={none}, database=/root/db1, operation=CREATE TABLE, paths=[/root/db1/some_table], status=SUCCESS, detailed_status=StatusAccepted
2023-03-13T20:10:44.345767Z: component=schemeshard, tx_id=562949953506313, remote_address=xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx, subject={none}, database={none}, operation=ALTER TABLE RENAME, paths=[/root/db1/some_table, /root/db1/another_table], status=SUCCESS, detailed_status=StatusAccepted
2023-03-14T10:41:36.485788Z: component=schemeshard, tx_id=281474976775658, remote_address=xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx, subject={none}, database=/root/db1, operation=MODIFY ACL, paths=[/root/db1/some_dir], status=SUCCESS, detailed_status=StatusSuccess, acl_add=[+(ConnDB):subject:-]
```
