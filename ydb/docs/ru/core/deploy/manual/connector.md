# Развертывание коннекторов ко внешним источникам данных

[Коннекторы](../../concepts/federated_query/architecture.md#connectors) - специальные микросервисы, предоставляющие {{ ydb-full-name }} универсальную абстракцию доступа ко внешним источникам данных. Коннекторы являются ключевым элементом системы обработки [федеративных запросов](../../concepts/federated_query/index.md) {{ ydb-full-name }}. В данном руководстве мы рассмотрим особенности развертывания коннекторов в режиме on-premise.

## fq-connector-go {#fq-connector-go}

Коннектор `fq-connector-go` реализован на языке Go; его исходный код размещён на [Github](https://github.com/ydb-platform/fq-connector-go). Он обеспечивает доступ к следующим источникам данных:

* ClickHouse
* PostgreSQL

### Запуск {#fq-connector-go-launch}

Для запуска коннектора используйте официальный [Docker-образ](https://github.com/ydb-platform/fq-connector-go/pkgs/container/fq-connector-go). Он уже содержит [конфигурационный файл](https://github.com/ydb-platform/fq-connector-go/blob/main/example.conf) сервиса. Запустить сервис с настройками по умолчанию можно следующей командой: 

```bash
docker run -d \
    --name=fq-connector-go \
    -p 50051:50051 \
    ghcr.io/ydb-platform/fq-connector-go:latest
```

На порту 50051 публичного сетевого интерфейса вашего хоста запустится слушающий сокет GRPC-сервиса коннектора. В дальнейшем {{ ydb-short-name }} должно будет установить соединение именно с этим сетевым адресом.

При необходимости изменения конфигурации подготовьте конфигурационный файл [по образцу](#fq-connector-go-config) и примонтируйте его к контейнеру:

```bash
docker run -d \
    --name=fq-connector-go \
    -p 50051:50051 \
    -v /path/to/config.txt:/usr/local/etc/fq-connector-go.conf
    ghcr.io/ydb-platform/fq-connector-go:latest
```

Для организации шифрованных соединений между YDB и коннектором [подготовьте пару TLS-ключей](../manual/deploy-ydb-on-premises.md#tls-certificates) и примонтируйте папку с ключами в контейнер, а также включите соответствующие настройки в секции конфигурационного файла `connector_server.tls`:

```bash
docker run -d \
    --name=fq-connector-go \
    -p 50051:50051 \
    -v /path/to/config.txt:/usr/local/etc/fq-connector-go.conf
    -v /path/to/tls/:/usr/local/etc/tls/
    ghcr.io/ydb-platform/fq-connector-go:latest
```

В случае, если внешние источники данных используют TLS, для организации шифрованных соединений с ними коннектору потребуется корневой или промежуточный сертификат удостоверяющего центра (Certificate Authority, CA), которым были подписаны сертификаты источников. Docker-образ для коннектора базируется на образе дистрибутива Alpine Linux, который уже содержит некоторое количество сертификатов от доверенных CA. Проверить наличие нужного CA в списке предустановленных можно следующей командой:

```bash
docker run -it --rm ghcr.io/ydb-platform/fq-connector-go sh
apk add openssl
awk -v cmd='openssl x509 -noout -subject' ' /BEGIN/{close(cmd)};{print | cmd}' < /etc/ssl/certs/ca-certificates.crt
```

Если TLS-ключи для источников выпущены CA, не входящим в перечень доверенных, необходимо добавить сертификат этого CA в системные пути контейнера с коннектором. Сделать это можно, например, собрав собственный Docker-образ на основе имеющегося:

```Dockerfile
FROM ghcr.io/ydb-platform/fq-connector-go:latest

USER root

RUN apk --no-cache add ca-certificates openssl
COPY custom_root_ca.crt /usr/local/share/ca-certificates
RUN update-ca-certificates
```

Новый образ можно использовать для развертывания сервиса с помощью команд, приведённых выше.

### Конфигурация {#fq-connector-go-config}

Актуальный пример конфигурационного файла сервиса `fq-connector-go` можно найти в [репозитории](https://github.com/ydb-platform/fq-connector-go/blob/main/examples/config.prod.txt). 

| Параметр | Назначение |
|----------|------------|
| `connector_server` | Обязательная секция. Содержит настройки основного GPRC-сервиса. |
| `connector_server.endpoint.host` | Сетевой интерфейс, на котором запускается слушающий сокет сервиса. |
| `connector_server.endpoint.port` | Номер порта, на котором запускается слушающий сокет сервиса. |
| `connector_server.tls` | Опциональная секция. Заполняется, если требуется включение TLS-соединений для основного GRPC-сервиса `fq-connector-go`. |
| `connector_server.tls.key` | Полный путь до закрытого ключа шифрования. |
| `connector_server.tls.cert` | Полный путь до открытого ключа шифрования. |
| `logger` | Опциональная секция. Содержит настройки логирования. |
| `logger.log_level` | Уровень логгирования. Допустимые значения: `TRACE`, `DEBUG`, `INFO`, `WARN`, `ERROR`, `FATAL`. |
| `logger.enable_sql_query_logging` | Для реляционных источников данных включает логирование транслированных запросов. Допустимые значения: `true`, `false`. **ВАЖНО**: включение этой опции может привести к печати конфиденциальных пользовательских данных в логи. |
| `metrics_server` | Опциональная секция. Заполняется, если требуется выгрузка статистики сервиса через отдельный сервис. |
| `metrics_server.endpoint.host` | Сетевой интерфейс, на котором запускается слушающий сокет сервера статистики. |
| `metrics_server.endpoint.port` | Номер порта, на котором запускается слушающий сокет сервера статистики. |
| `paging` | Опциональная секция. Содержит настройки алгоритма разбиения извлекаемого из источника потока данных на Arrow-блоки. |
| `paging.bytes_per_page` | Количество байт в одном блоке. Рекомендуемые значения - от 4 до 8 МиБ, максимальное значение - 48 МиБ. |
| `paging.prefetch_queue_capacity` | Количество заранее вычитываемых блоков данных, которые хранятся в адресном пространстве коннектора до обращения YDB за очередным блоком данных. В некоторых сценариях бóльшие значения данной настройки могут увеличить пропускную способность, но одновременно приведут и к большему потреблению оперативной памяти процессом. Рекомендуемые значения - не менее 2. | 
