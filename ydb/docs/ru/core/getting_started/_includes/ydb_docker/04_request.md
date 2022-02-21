## Выполните запрос к базе данных {#request}

Скачайте [YDB CLI](../../../getting_started/cli.md) и выполните запрос к базе данных {{ ydb-short-name }} в Docker-контейнере:

```bash
ydb \
  -e grpcs://localhost:2135 \
  --ca-file $(pwd)/ydb_certs/ca.pem \
  -d /local table query execute -q 'select 1;'
```

Где:

* `-e` — эндпоинт базы данных.
* `--ca-file` — путь к TLS-сертификату.
* `-d` — имя базы данных и параметры запроса.

Результат выполнения:

```text
┌─────────┐
| column0 |
├─────────┤
| 1       |
└─────────┘
```

Также можно выполнить запрос к контейнеру через порт без TLS, например:

```bash
ydb \
  -e grpc://localhost:2136 \
  -d /local table query execute -q 'select 1;'
```


Предсобранная версия [YDB CLI](../../../reference/ydb-cli/index.md) также доступа внутри образа:

```bash
docker exec <CONTAINER-ID> /ydb -e localhost:2136 -d /local table query execute -q 'select 1;'
```

Результат выполнения:

```text
┌─────────┐
| column0 |
├─────────┤
| 1       |
└─────────┘
```
