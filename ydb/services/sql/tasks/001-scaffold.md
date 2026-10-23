# Каркас сервиса и слушатель

status: done
commit: e04a61264f9a97fc3960d5f34abcb65bf7cd70d0
id: scaffold
depends: 

## Сделать

Собрать каталог `ydb/services/sql`. Корневой `ya.make` только собирает подмодули. `RECURSE` в `ydb/services/ya.make`, `PEERDIR` в `ydb/core/driver_lib/run/ya.make`. Первый подмодуль — `wire`.
Конфиг в `ydb/core/protos/config.proto`: включение, адрес, порт 5432. Инициализатор рядом с `TKafkaProxyServiceInitializer` слушает сырой TCP через `ydb/core/raw_socket`. gRPC `AddService` не используется.
Логи — уже существующие коды `PGWIRE` и `LOCAL_PGWIRE`. Файл — одна сущность, общий `sql.h` не заводить.

## Тест

Юнит на мини-кластере по образцу Kafka (`test_client.cpp`, `TServer`): порт принимает соединение.

## Готово, когда

Сервис стартует, сокет слушает, тест соединения зелёный.

## Не трогать

- `ydb/core/tx/datashard`, `ydb/core/tx/columnshard` и их протобуфы
- `yql/` и `ydb/core/kqp`: ни `PEERDIR`, ни вызовы
- remote `official`
- код datashard и columnshard в одном читателе

Пока этой возможности нет в коде, она отвечает SQLSTATE `0A000`, текст `not supported`, сессия жива.
## Перед коммитом

```bash
./ya make --build relwithdebinfo -tA ydb/services/sql
```

Коммит только после зелёного прогона. В прогон входят тесты `pg8000`, когда они уже есть.

## После коммита

В этом файле поставить `status: done` и хеш в `commit`. Правка отметки входит в следующий коммит.
