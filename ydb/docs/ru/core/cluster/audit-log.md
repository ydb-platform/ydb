# Аудитный лог

{% include [release-candidate](../_includes/trunk.md) %}

_Аудитный лог_ — это файл, который содержит информацию обо всех операциях (успешных или не успешных) по изменению объектов {{ ydb-short-name }} и ACL, таких как:

* создание, изменение и удаление баз данных;
* создание и удаление директорий;
* создание, изменение схемы, изменение количества партиций, резервное копирование и восстановление, копирование и переименование, удаление таблиц;
* создание, изменение, удаление топиков;
* изменение ACL.

Файл пишется на каждом узле кластера {{ ydb-short-name }}. Доступ к аудитному логу ограничен правами доступа на уровне файловой системы.

## События аудитного лога {#events}

Информация о каждой операции записывается в аудитный лог в виде отдельного события. Событие содержит набор атрибутов, список которых определяется условиями:
* некоторые атрибуты являются общими для всех аудитных записей;
* в зависимости от компонента YDB, который создает аудитный лог, могут быть записаны параметры, специфичные для конкретного компонента YDB;
* для баз данных, функционирующих в составе облачного управляемого сервиса YDB, будут добавлены облачные идентификаторы объекта операции.

| Атрибут | Описание | Обязательный |
|:----|:----|:----:|
| **Общие атрибуты** ||
| `subject`| SID источника события (формат `<login>@<subsystem>`).  Если обязательная аутентификация не включена, ключ будет иметь значение `{none}` | Да 
| `operation`| название операции/действия. Пишутся в верхнем регистре, схожи с синтаксисом SQL: `ALTER DATABASE`, `CREATE TABLE` | Да 
| `status` | статус завершения операции<br/><ul><li>`SUCCESS` — операция завершена успешно</li><li>`ERROR` — операция завершена с ошибкой</li><li>`IN-PROCESS` — операция выполняется</li></ul> | Да
| `reason` | сообщение об ошибке |
| `component` | имя компонента YDB, сделавшего запись. список существующих имен: `schemeshard`, `ymq`, `serverless-proxy` |
| `request_id`| уникальный id запроса, вызвавшего операцию; по нему можно различать операции друг от друга и связывать частичные записи в логе в единый аудит контекст операции | 
| `remote_address` | ip-адрес клиента, приславшего запрос |
| `detailed_status` | один из статусов для конкретного компонента, строка без пробелов, например `StatusAccepted`, `StatusInvalidParameter ` или `StatusNameConflict` |
| **Атрибуты компонента schemeshard** ||
| `tx_id`| уникальный идентификатор транзакции. как и request_id может быть использован для отличия операций друг от друга и связывания записей | Да 
| `database` | имя базы данных (формат `/my_dir/db`) | Да 
| `paths` | список путей внутри базы, которые изменяет операция (формат `[/root/db/table-a, root/db/table-b]`) | Да 
| Атрибуты владения и разрешений ||
| `new_owner` | SID нового владельца объекта (только при передаче владения) |
| `acl_add` | список добавленных разрешений (при создании объектов или изменении разрешений) (формат `[+R:gvit@staff, -R:all@staff]`) |
| `acl_remove` | список убранных разрешений (при изменении разрешений) (формат `[+R:gvit@staff, -R:all@staff]`) |
| Пользовательские атрибуты ||
| `user_attrs_add` | список добавленных пользовательских атрибутов (при создании объектов или изменении атрибутов) (формат `[attr_name1: A, attr_name2: B]`) |
| `user_attrs_remove` | список удалённых пользовательских атрибутов (при изменении атрибутов) (формат `[attr_name1, attr_name2]`) |
| **Атрибуты компонента ymq** ||
| `account` | описание очередей и разрешений пользователя | Да
| `queue` | имя очереди | Да
| **Атрибуты Облака** ||
| `cloud_id`, `folder_id`, `resource_id` | облачные идентификаторы объекта операции |

## Включение аудитного лога {#enabling-audit-log}

Сохранение событий в аудитный лог включается целиком для кластера. Для включения необходимо в файл [конфигурации кластера](../deploy/configuration/config.md) добавить секцию `audit`:

```proto
audit:
  stderr_backend:
    format: JSON
  file_backend:
    format: JSON
    file_path: "path_to_log_file"
  unified_agent_backend:
    format: JSON
    log_name: session_meta_log_name
```

Существует несколько способов записи аудитного лога. Чтобы включить один из них, в секцию `audit` нужно добавить одну или несколько соответствующих дочерних секций. Если секции нет - запись данным способом не производится.

Секция | Способ записи
--- | ---
`stderr_backend` | Запись в стандартный поток вывода `stderr`
`file_backend` | Запись в файл
`unified_agent_backend` | Запись в Unified Agent. Для записи аудитных логов в Unified Agent в файле [конфигурации кластера](../deploy/configuration/config.md) должна быть описана секция `log` - `uaclient_config`

Для каждой секции необходимо определить дополнительные параметры

Параметр | Значение
--- | ---
`format` | Не обязательный. Формат аудитного лога.<br>Возможные значения:<ul><li>`JSON` — [JSON]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/JSON){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/JSON){% endif %} в сериализованном виде;</li><li>`TXT` — текст.</ul> По умолчанию `JSON`.
`file_path` | Обязательный. Путь к файлу, в который будет записываться аудитный лог. Путь и файл в случае их отсутствия будут созданы на каждом узле при старте кластера. Если файл существует, запись в него будет продолжена.
`log_name` | Не обязательный. Метаданные сессии, который передаются вместе с сообщением. позволяют направить логирующий поток в один или несколько дочерних каналов по условию `_log_name: "session_meta_log_name"`

## Формат записи {#format}
Формат записи событий определяется параметром `format` в [конфигурации кластера](#enabling-audit-log). Пример событий в формате `JSON`:

```json
2023-03-13T20:05:19.776132Z: {"paths":"[/my_dir/db1/some_dir]","tx_id":"562949953476313","database":"/my_dir/db1","remote_address":"ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]:xxxxx","status":"SUCCESS","subject":"{none}","detailed_status":"StatusAccepted","operation":"CREATE DIRECTORY","component":"schemeshard"}
2023-03-13T20:07:30.927210Z: {"reason":"Check failed: path: '/my_dir/db1/some_dir', error: path exist, request accepts it (id: [OwnerId: 72075186224037889, LocalPathId: 3], type: EPathTypeDir, state: EPathStateNoChanges)","paths":"[/my_dir/db1/some_dir]","tx_id":"844424930216970","database":"/my_dir/db1","remote_address":"ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]:xxxxx","status":"SUCCESS","subject":"{none}","detailed_status":"StatusAlreadyExists","operation":"CREATE DIRECTORY","component":"schemeshard"}
2023-03-13T19:59:27.614731Z: {"paths":"[/my_dir/db1/some_table]","tx_id":"562949953426315","database":"/my_dir/db1","remote_address":"{none}","status":"SUCCESS","subject":"{none}","detailed_status":"StatusAccepted","operation":"CREATE TABLE","component":"schemeshard"}
2023-03-13T20:10:44.345767Z: {"paths":"[/my_dir/db1/some_table, /my_dir/db1/another_table]","tx_id":"562949953506313","database":"{none}","remote_address":"ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]:xxxxx","status":"SUCCESS","subject":"{none}","detailed_status":"StatusAccepted","operation":"ALTER TABLE RENAME","component":"schemeshard"}
2023-03-14T10:41:36.485788Z: {"paths":"[/my_dir/db1/some_dir]","tx_id":"281474976775658","database":"/my_dir/db1","remote_address":"ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]:xxxxx","status":"SUCCESS","subject":"{none}","detailed_status":"StatusAccepted","operation":"MODIFY ACL","component":"schemeshard","acl_add":"[+(ConnDB):subject:-]"}
```

Те же события в формате `TXT`:

```txt
2023-03-13T20:05:19.776132Z: component=schemeshard, tx_id=844424930186969, remote_address=ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]:xxxxx, subject={none}, database=/my_dir/db1, operation=CREATE DIRECTORY, paths=[/my_dir/db1/some_dir], status=SUCCESS, detailed_status=StatusAccepted
2023-03-13T20:07:30.927210Z: component=schemeshard, tx_id=281474976775657, remote_address=ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]:xxxxx, subject={none}, database=/my_dir/db1, operation=CREATE DIRECTORY, paths=[/my_dir/db1/some_dir], status=SUCCESS, detailed_status=StatusAlreadyExists, reason=Check failed: path: '/my_dir/db1/some_dir', error: path exist, request accepts it (id: [OwnerId: 72075186224037889, LocalPathId: 3], type: EPathTypeDir, state: EPathStateNoChanges)
2023-03-13T19:59:27.614731Z: component=schemeshard, tx_id=562949953426315, remote_address={none}, subject={none}, database=/my_dir/db1, operation=CREATE TABLE, paths=[/my_dir/db1/some_table], status=SUCCESS, detailed_status=StatusAccepted
2023-03-13T20:10:44.345767Z: component=schemeshard, tx_id=562949953506313, remote_address=ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]:xxxxx, subject={none}, database={none}, operation=ALTER TABLE RENAME, paths=[/my_dir/db1/some_table, /my_dir/db1/another_table], status=SUCCESS, detailed_status=StatusAccepted
2023-03-14T10:41:36.485788Z: component=schemeshard, tx_id=281474976775658, remote_address=ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]:xxxxx, subject={none}, database=/my_dir/db1, operation=MODIFY ACL, paths=[/my_dir/db1/some_dir], status=SUCCESS, detailed_status=StatusSuccess, acl_add=[+(ConnDB):subject:-]
```
