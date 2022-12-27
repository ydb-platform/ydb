# Аудитный лог

_Аудитный лог_ — это файл, который содержит информацию обо всех операциях (успешных или не успешных) по изменению объектов {{ ydb-short-name }} и ACL, таких как:

* создание, изменение и удаление баз данных;
* создание и удаление директорий;
* создание, изменение схемы, изменение количества партиций, резервное копирование и восстановление, копирование и переименование, удаление таблиц;
* создание, изменение, удаление топиков;
* изменение ACL.

Файл пишется на каждом узле кластера {{ ydb-short-name }}. Доступ к аудитному логу возможен только через сессию терминала.

## События аудитного лога {#events}

Информация о каждой операции записывается в аудитный лог в виде отдельного события. Событие содержит набор атрибутов. Одни атрибуты описывают операцию, другие - транзакцию, в которой выполнялась операция. Если несколько операций выполнялись в одной транзакции, то атрибуты транзакции таких событий будут совпадать.

Атрибуты операции:

* `operation` — обязательный, имя операции.
* `path` — не обязательный, путь к объекту изменения.
* `src path` — не обязательный, путь к исходному объекту (для операций копирования и перемещения). Поле может содержать несколько значений.
* `dst path` — не обязательный, путь к конечному объекту (для операций копирования и перемещения). Поле может содержать несколько значений.
* `set owner` — не обязательный, новый владелец при изменении ACL.
* `add access` — не обязательный, добавление доступа при изменении ACL. Поле может содержать несколько значений.
* `remove access` — не обязательный, удаление доступа при изменении ACL. Поле может содержать несколько значений.
* `protobuf request` — не обязательный, описание изменения схемы или ACL в формате protobuf.

Атрибуты транзакции:

* `txId` — обязательный, уникальный идентификатор транзакции.
* `database` — не обязательный, путь к базе данных.
* `subject` — обязательный, SID источника события (формат `<login>@<subsystem>`). Если обязательная аутентификация не включена, ключ будет иметь значение `no subject`.
* `status` — обязательный, статус завершения транзакции.
* `reason` — не обязательный, сообщение об ошибке.

Формат записи событий определяется параметром `format` в [конфигурации кластера](#enabling-audit-log). Пример событий в формате `JSON`:

```json
2022-12-05T18:58:39.517833Z: {"protobuf request":"WorkingDir: \"/my_dir/db1\" OperationType: ESchemeOpCreateTable CreateTable { Name: \"my_table\" Columns { Name: \"id\" Type: \"Uint64\" NotNull: false } Columns { Name: \"name\" Type: \"String\" NotNull: false } KeyColumnNames: \"id\" PartitionConfig { PartitioningPolicy { SizeToSplit: 2147483648 } ColumnFamilies { StorageConfig { SysLog { PreferredPoolKind: \"ssd\" } Log { PreferredPoolKind: \"ssd\" } Data { PreferredPoolKind: \"ssd\" } } } } } FailOnExist: false","txId":"281474976720657","subject":"no subject","status":"StatusAccepted","operation":"CREATE TABLE","path":"/my_dir/db1/my_table","database":"/my_dir/db1"}

2022-12-05T19:01:22.309877Z: {"dst path":"{/my_dir/db1/my_table2}","database":"/my_dir/db1","txId":"281474976720658","protobuf request":"OperationType: ESchemeOpMoveTable MoveTable { SrcPath: \"/my_dir/db1/my_table\" DstPath: \"/my_dir/db1/my_table2\" }","status":"StatusAccepted","subject":"no subject","src path":"{/my_dir/db1/my_table}","operation":"ALTER TABLE RENAME"}
```

Те же события в формате `TXT`:

```txt
2022-12-05T18:58:39.517833Z: txId=281474976720657, database=/my_dir/db1, subject=no subject, status=StatusAccepted, operation=CREATE TABLE, path=/my_dir/db1/my_table, protobuf request=WorkingDir: "/my_dir/db1" OperationType: ESchemeOpCreateTable CreateTable { Name: "my_table" Columns { Name: "id" Type: "Uint64" NotNull: false } Columns { Name: "name" Type: "String" NotNull: false } KeyColumnNames: "id" PartitionConfig { PartitioningPolicy { SizeToSplit: 2147483648 } ColumnFamilies { StorageConfig { SysLog { PreferredPoolKind: "ssd" } Log { PreferredPoolKind: "ssd" } Data { PreferredPoolKind: "ssd" } } } } } FailOnExist: false

2022-12-05T19:01:22.309877Z: txId=281474976720658, database=/my_dir/db1, subject=no subject, status=StatusAccepted, operation=ALTER TABLE RENAME, src path={/my_dir/db1/my_table}, dst path={/my_dir/db1/my_table2}, protobuf request=OperationType: ESchemeOpMoveTable MoveTable { SrcPath: "/my_dir/db1/my_table" DstPath: "/my_dir/db1/my_table2" }
```

## Включение аудитного лога {#enabling-audit-log}

Сохранение событий в аудитный лог включается целиком для кластера. Для включения необходимо в файл [конфигурации кластера](../deploy/configuration/config.md) добавить секцию `audit`:

```proto
audit:
  audit_file_path: "path_to_log_file"
  format: JSON
```

Параметр | Значение
--- | ---
`audit_file_path` | Путь к файлу, в который будет записываться аудитный лог. Путь и файл в случае их отсутствия будут созданы на каждом узле при старте кластера. Если файл существует, запись в него будет продолжена.<br>Не обязательный. Должен быть указан один из параметров или оба: `audit_file_path` или `log_name`.
`format` | Формат аудитного лога.<br>Возможные значения:<ul><li>`JSON` — [JSON]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/JSON){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/JSON){% endif %} в сериализованном виде;</li><li>`TXT` — текст.</ul>
