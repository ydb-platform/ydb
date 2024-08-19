# Running {{ ydb-short-name }} in Docker

## Before start

Create a folder for testing {{ ydb-short-name }} and use it as the current working directory:

```bash
mkdir ~/ydbd && cd ~/ydbd
mkdir ydb_data
mkdir ydb_certs
```

## Launching a container with {{ ydb-short-name }} in Docker

Example of the {{ ydb-short-name }} startup command in Docker with detailed comments:

```bash
docker run \
    -d \ # запуск в фоне
    --rm \ # автоматическое удаление после установки
    --name ydb-local \ # имя контейнера
    -h localhost \ # хостейм
    -p 2135:2135 \ # открытие внешнего доступа к grpcs порту
    -p 2136:2136 \ # открытие внешнего доступа к grpc порту
    -p 8765:8765 \ # открытие внешнего доступа к http порту
    -p 5432:5432 \ # открытие внешнего доступа к порту, обеспечивающему PostgreSQL-совместимость
    -v $(pwd)/ydb_certs:/ydb_certs \ # директория для TLS сертификатов
    -v $(pwd)/ydb_data:/ydb_data \ # рабочая директория
    -e GRPC_TLS_PORT=2135 \ # grpcs порт
    -e GRPC_PORT=2136 \ # grpc порт
    -e MON_PORT=8765 \ # http порт
    -e YDB_USE_IN_MEMORY_PDISKS=1 \ # хранение данных только в оперативной памяти
    ydbplatform/local-ydb:latest # имя и тег образа
```

For more information about environment variables used when running a Docker container with {{ ydb-short-name }}, see [{#T}](environment.md)

With the parameters specified in the example above and running Docker locally, [Embedded UI](../embedded-ui/index.md) {{ ydb-short-name }} will be available at [http://localhost:8765](http://localhost:8765).

For more information about stopping and deleting a Docker container with {{ ydb-short-name }}, see [{#T}](cleanup.md)
