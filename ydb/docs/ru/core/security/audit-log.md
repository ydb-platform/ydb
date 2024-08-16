# Аудитный лог

_Аудитный лог_ — это поток, который содержит информацию обо всех операциях (успешных или не успешных) над объектами {{ ydb-short-name }}:

* база данных — создание, изменение и удаление;
* директория — создание и удаление;
* таблица — создание, изменение схемы, изменение количества партиций, резервное копирование и восстановление, копирование и переименование, удаление;
* топик — создание, изменение и удаление;
* ACL — изменение.

Данные потока аудитного лога могут быть направлены:

* в файл на каждом узле кластера {{ ydb-short-name }};
* в агент для поставки метрик [Unified Agent](https://cloud.yandex.ru/docs/monitoring/concepts/data-collection/unified-agent/);
* в стандартный вывод ошибок `stderr`.

Можно использовать любое из перечисленных назначений или их комбинацию.

В случае направления в файл доступ к аудитному логу задается правами на уровне файловой системы. Сохранение аудитного лога в файл рекомендуется для использования в промышленных инсталляциях.

Направление аудитного лога в стандартный вывод ошибок `stderr` рекомендуется для тестовых инсталляций. Дальнейшая обработка данных потока определяется настройками [логирования](../devops/manual/logging.md) кластера {{ ydb-short-name }}.

## События аудитного лога {#events}

Информация о каждой операции записывается в аудитный лог в виде отдельного события. Каждое событие содержит набор атрибутов. Одни атрибуты являются общими для любых событий, другие определяются компонентом {{ ydb-short-name }}, в котором произошло событие.

| Атрибут | Описание |
|:----|:----|
| **Общие атрибуты** ||
| `subject`| SID источника события (формат `<login>@<subsystem>`). Если обязательная аутентификация не включена, атрибут будет иметь значение `{none}`.<br/>Обязательный.
| `operation`| Название операции или действия, сходны с синтаксисом YQL (например, `ALTER DATABASE`, `CREATE TABLE`).<br/>Обязательный.
| `status` | Статус завершения операции.<br/>Возможные значения:<ul><li>`SUCCESS` — операция завершена успешно;</li><li>`ERROR` — операция завершена с ошибкой;</li><li>`IN-PROCESS` — операция выполняется.</li></ul>Обязательный.
| `reason` | Сообщение об ошибке.<br/>Необязательный.
| `component` | Имя компонента {{ ydb-short-name }} — источника события (например, `schemeshard`).<br/>Необязательный.
| `request_id`| Уникальный идентификатор запроса, вызвавшего операцию. По `request_id` можно отличать события разных операций и связывать события в единый аудитный контекст операции.<br/>Необязательный.
| `remote_address` | IP-адрес клиента, приславшего запрос.<br/>Необязательный.
| `detailed_status` | Статус, который передает компонент {{ ydb-short-name }} (например `StatusAccepted`, `StatusInvalidParameter `, `StatusNameConflict`).<br/>Необязательный.
| **Атрибуты владения и разрешений** ||
| `new_owner` | SID нового владельца объекта при передаче владения.<br/>Необязательный.
| `acl_add` | Список добавленных разрешений при создании объектов или изменении разрешений в [краткой записи](./short-access-control-notation.md) (например, `[+R:someuser]`).<br/>Необязательный.
| `acl_remove` | Список удаленных разрешений при изменении разрешений в [краткой записи](./short-access-control-notation.md) (например, `[-R:somegroup]`).<br/>Необязательный.
| **Пользовательские атрибуты** ||
| `user_attrs_add` | Список добавленных пользовательских атрибутов при создании объектов или изменении атрибутов (например, `[attr_name1: A, attr_name2: B]`).<br/>Необязательный.
| `user_attrs_remove` | Список удаленных пользовательских атрибутов при изменении атрибутов (например, `[attr_name1, attr_name2]`).<br/>Необязательный.
| **Атрибуты компонента SchemeShard** ||
| `tx_id`| Уникальный идентификатор транзакции. Как и `request_id`, может быть использован для различения событий разных операций.<br/>Обязательный.
| `database` | Путь базы данных (например, `/my_dir/db`).<br/>Обязательный.
| `paths` | Список путей внутри БД, которые изменяет операция (например, `[/my_dir/db/table-a, /my_dir/db/table-b]`).<br/>Обязательный.

## Включение аудитного лога {#enabling-audit-log}

Отправка событий в поток аудитного лога включается целиком для кластера {{ ydb-short-name }}. Для включения необходимо в [конфигурацию кластера](../deploy/configuration/config.md) добавить секцию `audit_config`, указать в ней одно из назначений для потока (`file_backend`, `unified_agent_backend`, `stderr_backend`) или их комбинацию:

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

Ключ | Описание
--- | ---
`file_backend` | Сохранять аудитный лог в файл на каждом узле кластера.</ul>Необязательный.
`format` | Формат аудитного лога. Значение по умолчанию: `JSON`.<br/>Возможные значения:<ul><li>`JSON` — [JSON]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/JSON){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/JSON){% endif %} в сериализованном виде;</li><li>`TXT` — текст.</ul>Необязательный.
`file_path` | Путь к файлу, в который будет направлен аудитный лог. Путь и файл в случае их отсутствия будут созданы на каждом узле при старте кластера. Если файл существует, запись в него будет продолжена.<br/>Обязательный при использовании `file_backend`.
`unified_agent_backend` | Направить аудитный лог в Unified Agent. Необходимо также описать секцию `uaclient_config` в [конфигурации кластера](../deploy/configuration/config.md).</ul>Необязательный.
`log_name` | Метаданные сессии, которые передаются вместе с сообщением. Позволяют направить логирующий поток в один или несколько дочерних каналов по условию `_log_name: "session_meta_log_name"`.<br/>Необязательный.
`stderr_backend` | Направить аудитный лог в стандартный вывод ошибок `stderr`.</ul>Необязательный.

Пример конфигурации для сохранения аудитного лога в текстовом формате в файл `/var/log/ydb-audit.log`:

```yaml
audit_config:
  file_backend:
    format: TXT
    file_path: "/var/log/ydb-audit.log"
```

Пример конфигурации для отправки аудитного лога в текстовом формате в Yandex Unified Agent с меткой `audit`, а также вывода его в `stderr` в формате JSON:

```yaml
audit_config:
  unified_agent_backend:
    format: TXT
    log_name: audit
  stderr_backend:
    format: JSON
```

## Примеры {#examples}

Фрагмент файла аудитного лога в формате `JSON`:

```json
2023-03-13T20:05:19.776132Z: {"paths":"[/my_dir/db1/some_dir]","tx_id":"562949953476313","database":"/my_dir/db1","remote_address":"ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]:xxxxx","status":"SUCCESS","subject":"{none}","detailed_status":"StatusAccepted","operation":"CREATE DIRECTORY","component":"schemeshard"}
2023-03-13T20:07:30.927210Z: {"reason":"Check failed: path: '/my_dir/db1/some_dir', error: path exist, request accepts it (id: [OwnerId: 72075186224037889, LocalPathId: 3], type: EPathTypeDir, state: EPathStateNoChanges)","paths":"[/my_dir/db1/some_dir]","tx_id":"844424930216970","database":"/my_dir/db1","remote_address":"ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]:xxxxx","status":"SUCCESS","subject":"{none}","detailed_status":"StatusAlreadyExists","operation":"CREATE DIRECTORY","component":"schemeshard"}
2023-03-13T19:59:27.614731Z: {"paths":"[/my_dir/db1/some_table]","tx_id":"562949953426315","database":"/my_dir/db1","remote_address":"{none}","status":"SUCCESS","subject":"{none}","detailed_status":"StatusAccepted","operation":"CREATE TABLE","component":"schemeshard"}
2023-03-13T20:10:44.345767Z: {"paths":"[/my_dir/db1/some_table, /my_dir/db1/another_table]","tx_id":"562949953506313","database":"{none}","remote_address":"ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]:xxxxx","status":"SUCCESS","subject":"{none}","detailed_status":"StatusAccepted","operation":"ALTER TABLE RENAME","component":"schemeshard"}
2023-03-14T10:41:36.485788Z: {"paths":"[/my_dir/db1/some_dir]","tx_id":"281474976775658","database":"/my_dir/db1","remote_address":"ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]:xxxxx","status":"SUCCESS","subject":"{none}","detailed_status":"StatusAccepted","operation":"MODIFY ACL","component":"schemeshard","acl_add":"[+(ConnDB):subject:-]"}
```

Событие от `2023-03-13T20:05:19.776132Z` в JSON-pretty:

```json
{
  "paths": "[/my_dir/db1/some_dir]",
  "tx_id": "562949953476313",
  "database": "/my_dir/db1",
  "remote_address": "ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]:xxxxx",
  "status": "SUCCESS",
  "subject": "{none}",
  "detailed_status": "StatusAccepted",
  "operation": "CREATE DIRECTORY",
  "component": "schemeshard"
}
```

Те же события в формате `TXT`:

```txt
2023-03-13T20:05:19.776132Z: component=schemeshard, tx_id=844424930186969, remote_address=ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]:xxxxx, subject={none}, database=/my_dir/db1, operation=CREATE DIRECTORY, paths=[/my_dir/db1/some_dir], status=SUCCESS, detailed_status=StatusAccepted
2023-03-13T20:07:30.927210Z: component=schemeshard, tx_id=281474976775657, remote_address=ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]:xxxxx, subject={none}, database=/my_dir/db1, operation=CREATE DIRECTORY, paths=[/my_dir/db1/some_dir], status=SUCCESS, detailed_status=StatusAlreadyExists, reason=Check failed: path: '/my_dir/db1/some_dir', error: path exist, request accepts it (id: [OwnerId: 72075186224037889, LocalPathId: 3], type: EPathTypeDir, state: EPathStateNoChanges)
2023-03-13T19:59:27.614731Z: component=schemeshard, tx_id=562949953426315, remote_address={none}, subject={none}, database=/my_dir/db1, operation=CREATE TABLE, paths=[/my_dir/db1/some_table], status=SUCCESS, detailed_status=StatusAccepted
2023-03-13T20:10:44.345767Z: component=schemeshard, tx_id=562949953506313, remote_address=ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]:xxxxx, subject={none}, database={none}, operation=ALTER TABLE RENAME, paths=[/my_dir/db1/some_table, /my_dir/db1/another_table], status=SUCCESS, detailed_status=StatusAccepted
2023-03-14T10:41:36.485788Z: component=schemeshard, tx_id=281474976775658, remote_address=ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]:xxxxx, subject={none}, database=/my_dir/db1, operation=MODIFY ACL, paths=[/my_dir/db1/some_dir], status=SUCCESS, detailed_status=StatusSuccess, acl_add=[+(ConnDB):subject:-]
```
