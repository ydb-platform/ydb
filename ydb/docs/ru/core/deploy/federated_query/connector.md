Коннекторы - специальные микросервисы, предоставляющие {{ ydb-full-name }} универсальную абстракцию доступа ко внешним источникам данных. Коннекторы являются ключевым элементом системы обработки федеративных запросов {{ ydb-full-name }}. Более подробно о них можно почитать [здесь](../../concepts/federated_query/index.md).

## Реализации коннекторов ко внешним источникам данных {#fq-connector-implementations}

### fq-connector-go {#fq-connector-go}

Коннектор `fq-connector-go` реализован на языке Go. Исходный код коннектора размещён на [Github](https://github.com/ydb-platform/fq-connector-go). `fq-connector-go` обеспечивает доступ к следующим источникам данных:

* ClickHouse
* PostgreSQL

#### Запуск

Для запуска `fq-connector-go` используйте официальный [Docker-образ](https://github.com/ydb-platform/fq-connector-go/pkgs/container/fq-connector-go). Подготовьте конфигурационный файл [по образцу]({#fq-connector-go-config}) и примонтируйте его к контейнеру:

```bash
docker run -d \
    --name=fq-connector-go \
    -p 50051:50051 \
    -v /path/to/config.txt:/usr/local/etc/fq-connector-go.conf
    ghcr.io/ydb-platform/fq-connector-go:latest
```

Для организации шифрованных соединений между YDB и `fq-connector-go` создайте пару TLS-ключей и примонтируйте папку с ключами в контейнер, а также включите соответствующие настройки в секции конфигурационного файла `connector_server.tls`:

```bash
docker run -d \
    --name=fq-connector-go \
    -p 50051:50051 \
    -v /path/to/config.txt:/usr/local/etc/fq-connector-go.conf
    -v /path/to/tls/:/usr/local/etc/tls/
    ghcr.io/ydb-platform/fq-connector-go:latest
```

В случае, если внешние источники данных используют TLS-шифрование, причем TLS-ключи для них выпущены Certificate Authority, не входящим в перечень доверенных, необходимо добавить корневой сертификат этого CA в системные пути контейнера `fq-connector-go`. Сделать это можно, например, собрав собственный Docker-образ на основе имеющегося:

```Dockerfile
FROM ghcr.io/ydb-platform/fq-connector-go:latest

USER root

RUN apk --no-cache add ca-certificates openssl
COPY custom_root_ca.crt /usr/local/share/ca-certificates
RUN update-ca-certificates

```

#### Конфигурация 

Пример конфигурационного файла сервиса `fq-connector-go`:

```proto
connector_server {
  endpoint {
      host: "0.0.0.0"
      port: 50051
  }

  tls {
    cert: "/usr/local/etc/tls/tls.crt"
    key: "/usr/local/etc/tls/tls.key"
  }
}

logger {
  log_level: DEBUG
  enable_sql_query_logging: false
}

pprof_server {
  endpoint {
      host: "0.0.0.0"
      port: 50052
  }
}

metrics_server {
  endpoint {
      host: "0.0.0.0"
      port: 8766
  }
}

paging {
  bytes_per_page: 4194304
  prefetch_queue_capacity: 2
}

conversion {
  use_unsafe_converters: true
}
```

| Параметр | Назначение |
|----------|------------|
| `connector_server` | Обязательная секция. Содержит настройки основного GPRC-сервиса `fq-connector-go`. |
| `connector_server.endpoint.host` | Сетевой интерфейс, на котором запускается слушающий сокет сервиса. |
| `connector_server.endpoint.port` | Номер порта, на котором запускается слушающий сокет сервиса. |
| `connector_server.tls` | Опциональная секция. Заполняется, если требуется включение TLS-соединений для основного GRPC-сервиса `fq-connector-go`. |
| `connector_server.tls.key` | Полный путь до закрытого ключа шифрования. |
| `connector_server.tls.cert` | Полный путь до открытого ключа шифрования. |
| `logger` | Опциональная секция. Содержит настройки логирования. |
| `logger.log_level` | Уровень логгирования. Допустимые значения: `TRACE`, `DEBUG`, `INFO`, `WARN`, `ERROR`, `FATAL`. |
| `logger.enable_sql_query_logging` | Для реляционных источников данных включает логирование транслированных запросов. Допустимые значения: `true`, `false`. **ВАЖНО**: включение этой опции может привести к печати конфиденциальных пользовательских данных в логи. |
| `pprof_server` | Опциональная секция. Заполняется, если требуется запуск встроенного профилировщика Go. |
| `pprof_server.endpoint.host` | Сетевой интерфейс, на котором запускается слушающий сокет сервера профилировщика. |
| `pprof_server.endpoint.port` | Номер порта, на котором запускается слушающий сокет сервера профилировщика. |
| `metrics_server` | Опциональная секция. Заполняется, если требуется выгрузка статистики сервиса через отдельный сервис. |
| `metrics_server.endpoint.host` | Сетевой интерфейс, на котором запускается слушающий сокет сервера статистики. |
| `metrics_server.endpoint.port` | Номер порта, на котором запускается слушающий сокет сервера статистики. |
| `paging` | Опциональная секция. Содержит настройки алгоритма разбиения извлекаемого из источника потока данных на Arrow-блоки. |
| `paging.bytes_per_page` | Количество байт в одном блоке. |
| `paging.prefetch_queue_capacity` | Количество заранее вычитываемых блоков данных, которые хранятся в адресном пространстве `fq-connector-go` до обращения YDB за очередным блоком данных. В некоторых сценариях бóльшие значения данной настройки могут увеличить пропускную способность, но одновременно приведут и к большему потреблению оперативной памяти процессом `fq-connector-go`. | 
