# Версионирование схемы данных и миграции в YDB с использованием "goose"

## Введение

Goose – open-source инструмент, который помогает версионировать схему данных в БД и управлять миграциями. Goose поддерживает множество различных баз данных, включая YDB. Goose использует файлы миграций и хранит состояние миграций непосредственно в базе данных в специальной таблице.

## Установка goose

Варианты установки goose описаны в [документации](https://github.com/pressly/goose/blob/master/README.md#install). 

## Аргументы запуска goose

Утилита `goose` вызывается командой:

```
$ goose <DB> <DSN> <COMMAND> <COMMAND_ARGUMENTS>
```

где:
- `<DB>` - движок базы данных, в случае YDB следует писать `goose ydb`
- `<DSN>` - строка подключения к базе данных. 
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

Далее примеры вызова команд `goose` будут содержать именно эту строку подключения.

## Директория с файлами миграций

Создадим директорию migrations и далее все команды `goose` следует выполнять в этой директории:

```
$ mkdir migrations && cd migrations
```

## Добавление файла миграции

Файл миграции можно сгенерировать с помощью команды `goose create`:

```
$ goose ydb "grpc://localhost:2136/local?go_query_mode=scripting&go_fake_tx=scripting&go_query_bind=declare,numeric" create 00001_create_first_table sql
2023/12/15 05:22:48 Created new file: 20231215052248_00001_create_table_users.sql
```

Это означает, что инструмент создал новый файл миграции `<timestamp>_00001_create_table_users.sql`, где мы можем записать шаги по изменению схемы для базы данных YDB, которая доступна через соответствующую строку подключения.  

Итак, после выполнения команды goose create будет создан файл миграции `<timestamp>_00001_create_table_users.sql` со следующим содержимым:

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

Такая структура файла миграции помогает держать в контексте внимания инструкции, приводящие к следующей версии базы данных. Также легко, не отвлекаясь на лишнее, можно написать инструкции, откатывающие изменение базы данных.

Файл миграции состоит из двух разделов. Первый - `+goose Up`, область, в которой мы можем записать шаги миграции. Вторая - `+goose Down`, область, в которой мы можем написать шаг для инвертирования изменений для шагов `+goose Up`. `Goose` заботливо вставил запросы-плейсхолдеры:

```
SELECT 'up SQL query';
```

и

```
SELECT 'down SQL query';
```

чтобы мы могли вместо них вписать по сути сами запросы миграции. 

Отредактируем файл миграции `<timestamp>_00001_create_table_users.sql` так, чтобы при применении миграции мы создавали таблицу нужной нам структуры, а при откате миграции - мы удаляли созданную таблицу:

```
-- +goose Up
-- +goose StatementBegin
CREATE TABLE users (
    id Uint64,
    username Text,
    created_at TzDatetime,
    PRIMARY KEY (id)
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE users;
-- +goose StatementEnd
```

Все последующие файлы миграций следует создавать аналогично.

## Управление миграциями

Утилита `goose` позволяет управлять миграциями через командную строку:
- `goose up` - применить все известные миграции. Например, `goose ydb "grpc://localhost:2136/local?go_query_mode=scripting&go_fake_tx=scripting&go_query_bind=declare,numeric" up`.
- `goose reset` - откатить все миграции. Например, `goose ydb "grpc://localhost:2136/local?go_query_mode=scripting&go_fake_tx=scripting&go_query_bind=declare,numeric" reset`.
- `goose up-by-one` - применить ровно одну "следующую" миграцию. Например, `goose ydb "grpc://localhost:2136/local?go_query_mode=scripting&go_fake_tx=scripting&go_query_bind=declare,numeric" up-by-one`.
- `goose redo` - пере-применить последнюю миграцию. Например, `goose ydb "grpc://localhost:2136/local?go_query_mode=scripting&go_fake_tx=scripting&go_query_bind=declare,numeric" redo`.
- `goose down` - откатить последнюю миграцию. Например, `goose ydb "grpc://localhost:2136/local?go_query_mode=scripting&go_fake_tx=scripting&go_query_bind=declare,numeric" down`.
- `goose status` - посмотреть статус применения миграций. Например, `goose ydb "grpc://localhost:2136/local?go_query_mode=scripting&go_fake_tx=scripting&go_query_bind=declare,numeric" status`.
