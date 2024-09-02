# Версионирование схемы данных и миграции в YDB с использованием "goose"

## Введение

Goose – open-source инструмент, который помогает версионировать схему данных в БД и управлять миграциями. Goose поддерживает множество различных баз данных, включая YDB. Goose использует файлы миграций и хранит состояние миграций непосредственно в базе данных в специальной таблице.

## Установка goose

Варианты установки goose описаны в [документации](https://github.com/pressly/goose/blob/master/README.md#install).

## Аргументы запуска goose

Утилита `goose` вызывается командой:

```
$ goose <DB> <CONNECTION_STRING> <COMMAND> <COMMAND_ARGUMENTS>
```

где:
- `<DB>` - движок базы данных, в случае YDB следует писать `goose ydb`
- `<CONNECTION_STRING>` - строка подключения к базе данных.
- `<COMMAND>` - команда, которую требуется выполнить. Полный перечень команд доступен во встроенной справке (`goose help`).
- `<COMMAND_ARGUMENTS>` - аргументы команды.

## Строка подлкючения к YDB

Для подключения к YDB следует использовать строку подключения вида

```
<protocol>://<host>:<port>/<database_path>?go_query_mode=scripting&go_fake_tx=scripting&go_query_bind=declare,numeric
```

где:
- `<protocol>` - протокол подключения (`grpc` для незащищенного соединения или `grpcs` для защищенного (`TLS`) соединения). При этом, для защищенного подключения (с `TLS`) следует явно подключить сертификаты `YDB`, например так: `export YDB_SSL_ROOT_CERTIFICATES_FILE=/path/to/ydb/certs/CA.pem`.
- `<host>` - адрес подключения к YDB.
- `<port>` - порт подключения к YDB.
- `<database_path>` - путь к базе данных в кластере YDB.
- `go_query_mode=scripting` - специальный режим `scripting` выполнения запросов по умолчанию в драйвере YDB. В этом режиме все запросы от goose направляются в YDB сервис `scripting`, который позволяет обрабатывать как `DDL`, так и `DML` инструкции `SQL`.
- `go_fake_tx=scripting` - поддержка эмуляции транзакций в режиме выполнения запросов через сервис YDB `scripting`. Дело в том, что в YDB выполнение `DDL` инструкций `SQL` в транзакции невозможно (или несет значительные накладные расходы). В частности сервис `scripting` не позволяет делать интерактивные транзакции (с явными `Begin`+`Commit`/`Rollback`). Соответственно, режим эмуляции транзакций на деле не делает ничего (`nop`) на вызовах `Begin`+`Commit`/`Rollback` из `goose`. Этот трюк в редких случаях может привести к тому, что отдельный шаг миграции может оказаться в промежуточном состоянии. Команда YDB работает на новым сервисом `query`, который должен помочь убрать этот риск.
- `go_query_bind=declare,numeric` - поддержка биндингов авто-выведения типов YQL из параметров запросов (`declare`) и поддержка биндингов нумерованных параметров (`numeric`). Дело в том, что `YQL` - язык со строгой типизацией, требующий явным образом указывать типы параметров запросов в теле самого `SQL`-запроса с помощью специальной инструкции `DECLARE`. Также `YQL` поддерживает только именованные параметры запроса (напрмиер, `$my_arg`), в то время как ядро `goose` генерирует `SQL`-запросы с нумерованными параметрами (`$1`, `$2`, и т.д.). Биндинги `declare` и `numeric` модифицируют исходные запросы из `goose` на уровне драйвера YDB, что позволило в конечном счете встроиться в `goose`.

В случае подключения к ломальному докер-контейнеру YDB строка подключения должна иметь вид:

```
grpc://localhost:2136/local?go_query_mode=scripting&go_fake_tx=scripting&go_query_bind=declare,numeric
```

Давайте сохраним эту строку в переменную окружения для дальнейшего использования:

```
export YDB_CONNECTION_STRING="grpc://localhost:2136/local?go_query_mode=scripting&go_fake_tx=scripting&go_query_bind=declare,numeric"
```

Далее примеры вызова команд `goose` будут содержать именно эту строку подключения.

## Директория с файлами миграций

Создадим директорию migrations и далее все команды `goose` следует выполнять в этой директории:

```
$ mkdir migrations && cd migrations
```

## Управление миграциями с помощью goose

### Создание файлов миграций и применение их к базе

Файл миграции можно создать командой `goose create`:

```
$ goose ydb $YDB_CONNECTION_STRING create 00001_create_first_table sql
2024/01/12 11:52:29 Created new file: 20240112115229_00001_create_first_table.sql
```

В результате выполнения команды был создан файл `<timestamp>_00001_create_table_users.sql`:
```
$ ls
20231215052248_00001_create_table_users.sql
```

Файл `<timestamp>_00001_create_table_users.sql` был создан со следующим содержимым:

```
-- +goose Up
-- +goose StatementBegin
SELECT 'up SQL query';
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
SELECT 'down SQL query';
-- +goose StatementEnd
```

Такая структура файла миграции помогает держать в контексте внимания ровно одну миграцию - шаги, чтобы обновить состояние базы, и шаги, чтобы откатить назад измения.

Файл миграции состоит из двух секций. Первая секция `+goose Up` содержит SQL-команды обновления схемы. Вторая секция `+goose Down` отвечает за откат изменений, выполненных в секции `+goose Up`. `Goose` заботливо вставил плейсхолдеры:

```
SELECT 'up SQL query';
```

и

```
SELECT 'down SQL query';
```

Мы можем заменить эти выражения на необходимые нам SQL-команды создания таблицы `users` и удаления ее в случае отката миграции:

```
-- +goose Up
-- +goose StatementBegin
CREATE TABLE users (
     id Uint64,
     username Text,
     created_at Timestamp,
     PRIMARY KEY (id)
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE users;
-- +goose StatementEnd
```

Проверим статус миграций:

```
$ goose ydb $YDB_CONNECTION_STRING status
2024/01/12 11:53:50     Applied At                  Migration
2024/01/12 11:53:50     =======================================
2024/01/12 11:53:50     Pending                  -- 20240112115229_00001_create_first_table.sql
```

Статус `Pending` означает, что миграция еще не применена.

Применим миграцию с помощью команды `goose up`:
```
$ goose ydb $YDB_CONNECTION_STRING up
2024/01/12 11:55:18 OK   20240112115229_00001_create_first_table.sql (93.58ms)
2024/01/12 11:55:18 goose: successfully migrated database to version: 20240112115229
```

Проверим статус миграций `goose status`:
```
$ goose ydb $YDB_CONNECTION_STRING status
2024/01/12 11:56:00     Applied At                  Migration
2024/01/12 11:56:00     =======================================
2024/01/12 11:56:00     Fri Jan 12 11:55:18 2024 -- 20240112115229_00001_create_first_table.sql
```

Статус `Pending` заменился на временную отметку `Fri Jan 12 11:55:18 2024` - это означает, что миграция успешно применена. Мы также можем убедиться в этом и другими способами:

{% list tabs %}

- Используя YDB UI по адресу http://localhost:8765

  ![YDB UI after apply first migration](../_assets/goose-ydb-ui-after-first-migration.png =450x)

- Используя YDB CLI

  ```
  $ ydb -e grpc://localhost:2136 -d /local scheme describe users
  <table> users

  Columns:
  ┌────────────┬────────────┬────────┬─────┐
  │ Name       │ Type       │ Family │ Key │
  ├────────────┼────────────┼────────┼─────┤
  │ id         │ Uint64?    │        │ K0  │
  │ username   │ Utf8?      │        │     │
  │ created_at │ Timestamp? │        │     │
  └────────────┴────────────┴────────┴─────┘

  Storage settings:
  Store large values in "external blobs": false

  Column families:
  ┌─────────┬──────┬─────────────┬────────────────┐
  │ Name    │ Data │ Compression │ Keep in memory │
  ├─────────┼──────┼─────────────┼────────────────┤
  │ default │      │ None        │                │
  └─────────┴──────┴─────────────┴────────────────┘

  Auto partitioning settings:
  Partitioning by size: true
  Partitioning by load: false
  Preferred partition size (Mb): 2048
  Min partitions count: 1
  ```

{% endlist %}

Давайте создадим второй файл миграции с добавлением колонки `password_hash` в таблицу `users`:

```
$ goose ydb $YDB_CONNECTION_STRING create 00002_add_column_password_hash_into_table_users sql
2024/01/12 12:00:57 Created new file: 20240112120057_00002_add_column_password_hash_into_table_users.sql
```

Отредактируем файл `<timestamp>_00002_add_column_password_hash_into_table_users.sql` до следующего содержимого:

```
-- +goose Up
-- +goose StatementBegin
ALTER TABLE users ADD COLUMN password_hash Text;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE users DROP COLUMN password_hash;
-- +goose StatementEnd
```

Проверим статус миграций:

```
$ goose ydb $YDB_CONNECTION_STRING status
2024/01/12 12:02:40     Applied At                  Migration
2024/01/12 12:02:40     =======================================
2024/01/12 12:02:40     Fri Jan 12 11:55:18 2024 -- 20240112115229_00001_create_first_table.sql
2024/01/12 12:02:40     Pending                  -- 20240112120057_00002_add_column_password_hash_into_table_users.sql
```

Мы видим, что первая миграция применена, а вторая только запланирована (`Pending`).

Применим вторую миграцию с помощью команды `goose up-by-one` (в отличие от `goose uo` команда `goose up-by-one` применяет ровно одну "следующую" миграцию):
```
$ goose ydb $YDB_CONNECTION_STRING up-by-one
2024/01/12 12:04:56 OK   20240112120057_00002_add_column_password_hash_into_table_users.sql (59.93ms)
```

Проверим статус миграций:
```
$ goose ydb $YDB_CONNECTION_STRING status
2024/01/12 12:05:17     Applied At                  Migration
2024/01/12 12:05:17     =======================================
2024/01/12 12:05:17     Fri Jan 12 11:55:18 2024 -- 20240112115229_00001_create_first_table.sql
2024/01/12 12:05:17     Fri Jan 12 12:04:56 2024 -- 20240112120057_00002_add_column_password_hash_into_table_users.sql
```

Обе миграции успешно применены. Убедимся в этом альтернативными способами:

{% list tabs %}

- Используя YDB UI по адресу http://localhost:8765

  ![YDB UI after apply second migration](../_assets/goose-ydb-ui-after-second-migration.png =450x)

- Используя YDB CLI

  ```
  $ ydb -e grpc://localhost:2136 -d /local scheme describe users
  <table> users

  Columns:
  ┌───────────────┬────────────┬────────┬─────┐
  │ Name          │ Type       │ Family │ Key │
  ├───────────────┼────────────┼────────┼─────┤
  │ id            │ Uint64?    │        │ K0  │
  │ username      │ Utf8?      │        │     │
  │ created_at    │ Timestamp? │        │     │
  │ password_hash │ Utf8?      │        │     │
  └───────────────┴────────────┴────────┴─────┘

  Storage settings:
  Store large values in "external blobs": false

  Column families:
  ┌─────────┬──────┬─────────────┬────────────────┐
  │ Name    │ Data │ Compression │ Keep in memory │
  ├─────────┼──────┼─────────────┼────────────────┤
  │ default │      │ None        │                │
  └─────────┴──────┴─────────────┴────────────────┘

  Auto partitioning settings:
  Partitioning by size: true
  Partitioning by load: false
  Preferred partition size (Mb): 2048
  Min partitions count: 1
  ```

{% endlist %}

Все последующие миграции можно создавать аналогичным образом.

### Откат миграции

Откатим последнюю миграцию с помощью команды `goose down`:
```
$ goose ydb $YDB_CONNECTION_STRING down
2024/01/12 13:07:18 OK   20240112120057_00002_add_column_password_hash_into_table_users.sql (43ms)
```

Проверим статус миграций `goose status`:
```
$ goose ydb $YDB_CONNECTION_STRING status
2024/01/12 13:07:36     Applied At                  Migration
2024/01/12 13:07:36     =======================================
2024/01/12 13:07:36     Fri Jan 12 11:55:18 2024 -- 20240112115229_00001_create_first_table.sql
2024/01/12 13:07:36     Pending                  -- 20240112120057_00002_add_column_password_hash_into_table_users.sql
```

Статус `Fri Jan 12 12:04:56 2024` заменился на статус `Pending` - это означает, что последняя миграция успешно отменена. Мы также можем убедиться в этом и другими способами:

{% list tabs %}

- Используя YDB UI по адресу http://localhost:8765

  ![YDB UI after apply first migration](../_assets/goose-ydb-ui-after-first-migration.png =450x)

- Используя YDB CLI

  ```
  $ ydb -e grpc://localhost:2136 -d /local scheme describe users
  <table> users

  Columns:
  ┌────────────┬────────────┬────────┬─────┐
  │ Name       │ Type       │ Family │ Key │
  ├────────────┼────────────┼────────┼─────┤
  │ id         │ Uint64?    │        │ K0  │
  │ username   │ Utf8?      │        │     │
  │ created_at │ Timestamp? │        │     │
  └────────────┴────────────┴────────┴─────┘

  Storage settings:
  Store large values in "external blobs": false

  Column families:
  ┌─────────┬──────┬─────────────┬────────────────┐
  │ Name    │ Data │ Compression │ Keep in memory │
  ├─────────┼──────┼─────────────┼────────────────┤
  │ default │      │ None        │                │
  └─────────┴──────┴─────────────┴────────────────┘

  Auto partitioning settings:
  Partitioning by size: true
  Partitioning by load: false
  Preferred partition size (Mb): 2048
  Min partitions count: 1
  ```

{% endlist %}

## "Горячий" список команд "goose"

Утилита `goose` позволяет управлять миграциями через командную строку:
- `goose status` - посмотреть статус применения миграций. Например, `goose ydb $YDB_CONNECTION_STRING status`.
- `goose up` - применить все известные миграции. Например, `goose ydb $YDB_CONNECTION_STRING up`.
- `goose up-by-one` - применить ровно одну "следующую" миграцию. Например, `goose ydb $YDB_CONNECTION_STRING up-by-one`.
- `goose redo` - пере-применить последнюю миграцию. Например, `goose ydb $YDB_CONNECTION_STRING redo`.
- `goose down` - откатить последнюю миграцию. Например, `goose ydb $YDB_CONNECTION_STRING down`.
- `goose reset` - откатить все миграции. Например, `goose ydb $YDB_CONNECTION_STRING reset`.

{% note warning %}

Будьте осторожны: команда `goose reset` может удалить все ваши миграции, включая данные в таблицах. Это происходит за счет инструкций в блоке `+goose Down`. Регулярно делайте бекапы и проверяйте их на возможность восстановления, чтобы минимизировать этот риск.

{% endnote %}
