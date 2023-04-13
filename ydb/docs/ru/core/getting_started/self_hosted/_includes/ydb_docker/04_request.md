## Выполнение запросов {#request}

[Установите](../../../../reference/ydb-cli/install.md) YDB CLI и выполните запрос, например:

```bash
ydb -e grpc://localhost:2136 -d /local scheme ls
```

Для успешного соединения с использованием TLS в параметры соединения нужно добавить имя файла с сертификатом. Запрос в примере ниже должен быть выполнен из той же рабочей директории, которую вы использовали для запуска контейнера:

```bash
ydb -e grpcs://localhost:2135 --ca-file ydb_certs/ca.pem -d /local scheme ls
```

Предсобранная версия [YDB CLI](../../../../reference/ydb-cli/index.md) также доступа внутри образа:

```bash
docker exec <container_id> /ydb -e grpc://localhost:2136 -d /local scheme ls
```

, где

`<container_id>`: идентификатор контейнера, выведенный при его [запуске](#start).