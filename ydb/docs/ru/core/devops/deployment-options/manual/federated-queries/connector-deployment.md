# Развёртывание коннекторов ко внешним источникам данных

{% note warning %}

Данная функциональность находится в режиме "Experimental".

{% endnote %}

[Коннекторы](../../../../concepts/federated_query/architecture.md#connectors) - специальные микросервисы, предоставляющие {{ ydb-full-name }} универсальную абстракцию доступа ко внешним источникам данных. Коннекторы выступают в качестве точек расширения системы обработки [федеративных запросов](../../../../concepts/federated_query/index.md) {{ ydb-full-name }}. В данном руководстве мы рассмотрим особенности развёртывания коннекторов в режиме on-premise.

## fq-connector-go {#fq-connector-go}

Коннектор `fq-connector-go` реализован на языке Go; его исходный код размещён на [GitHub](https://github.com/ydb-platform/fq-connector-go). Он обеспечивает доступ к следующим источникам данных:

* [ClickHouse](https://clickhouse.com/)
* [Greenplum](https://greenplum.org/)
* [Microsoft SQL Server](https://www.microsoft.com/en-us/sql-server)
* [MySQL](https://www.mysql.com/)
* [PostgreSQL](https://www.postgresql.org/)
* [{{ ydb-short-name }}](https://ydb.tech/)

Коннектор может быть установлен с помощью бинарного дистрибутива или с помощью Docker-образа.

### Запуск из бинарного дистрибутива {#fq-connector-go-binary}

Для установки коннектора на физический или виртуальный Linux-сервер без средств контейнерной виртуализации используйте бинарные дистрибутивы.

1. На [странице с релизами](https://github.com/ydb-platform/fq-connector-go/releases) коннектора выберите последний релиз, скачайте архив для подходящей вам платформы и архитектуры. Так выглядит команда для скачивания коннектора версии `v0.2.4` под платформу Linux и архитектуру процессора `amd64`:

    ```bash
    mkdir /tmp/connector && cd /tmp/connector
    wget https://github.com/ydb-platform/fq-connector-go/releases/download/v0.2.4/fq-connector-go-v0.2.4-linux-amd64.tar.gz
    tar -xzf fq-connector-go-v0.2.4-linux-amd64.tar.gz
    ```

1. Если на сервере ещё не были развёрнуты узлы {{ ydb-short-name }}, создайте директории для хранения исполняемых и конфигурационных файлов:

    ```bash
    sudo mkdir -p /opt/ydb/bin /opt/ydb/cfg
    ```

1. Разместите разархивированные исполняемый и конфигурационный файлы коннектора в только что созданные директории:

    ```bash
    sudo cp fq-connector-go /opt/ydb/bin
    sudo cp fq-connector-go.yaml /opt/ydb/cfg
    ```

2. В {% if oss %}[рекомендуемом режиме использования](index.md#general-scheme){% else %}рекомендуемом режиме использования{% endif %} коннектор развёртывается на тех же серверах, что и динамические узлы {{ ydb-short-name }}, следовательно, шифрование сетевых соединений между ними *не требуется*. Однако если вам всё же необходимо включить шифрование, [подготовьте пару TLS-ключей](../initial-deployment.md#tls-certificates) и пропишите пути до публичного и приватного ключа в поля `connector_server.tls.cert` и `connector_server.tls.key` конфигурационного файла `fq-connector-go.yaml`:

    ```yaml
    connector_server:
      # ...
      tls:
        cert: "/opt/ydb/certs/fq-connector-go.crt"
        key: "/opt/ydb/certs/fq-connector-go.key"
    ```

3. В случае, если внешние источники данных используют TLS, для организации шифрованных соединений с ними коннектору потребуется корневой или промежуточный сертификат удостоверяющего центра (Certificate Authority, CA), которым были подписаны сертификаты источников. На Linux-серверах обычно предустанавливается некоторое количество корневых сертификатов CA. Для ОС Ubuntu список поддерживаемых CA можно вывести следующей командой:

    ```bash
    awk -v cmd='openssl x509 -noout -subject' '/BEGIN/{close(cmd)};{print | cmd}' < /etc/ssl/certs/ca-certificates.crt
    ```

    Если на сервере отсутствует сертификат нужного CA, скопируйте его в специальную системную директорию и обновите список сертификатов:

    ```bash
    sudo cp root_ca.crt /usr/local/share/ca-certificates/
    sudo update-ca-certificates
    ```

4. Вы можете запустить сервис вручную или с помощью systemd.

    {% list tabs %}

    - Вручную

        Запустите сервис из консоли следующей командой:

        ```bash
        /opt/ydb/bin/fq-connector-go server -c /opt/ydb/cfg/fq-connector-go.yaml
        ```

    - С использованием systemd

        Вместе с бинарным дистрибутивом fq-connector-go распространяется [пример](https://github.com/ydb-platform/fq-connector-go/blob/main/examples/systemd/fq-connector-go.service) конфигурационного файла (юнита) для системы инициализации `systemd`. Скопируйте юнит в директорию `/etc/systemd/system`, активизируйте и запустите сервис:

        ```bash
        cd /tmp/connector
        sudo cp fq-connector-go.service /etc/systemd/system/
        sudo systemctl enable fq-connector-go.service
        sudo systemctl start fq-connector-go.service
        ```

        В случае успеха сервис должен перейти в состояние `active (running)`. Проверьте его следующей командой:

        ```bash
        sudo systemctl status fq-connector-go
        ● fq-connector-go.service - YDB FQ Connector Go
            Loaded: loaded (/etc/systemd/system/fq-connector-go.service; enabled; vendor preset: enabled)
            Active: active (running) since Thu 2024-02-29 17:51:42 MSK; 2s ago
        ```

        Логи сервиса можно прочитать с помощью команды:

        ```bash
        sudo journalctl -u fq-connector-go.service
        ```

    {% endlist %}

### Запуск в Docker {#fq-connector-go-docker}

1. Для запуска коннектора используйте официальный [Docker-образ](https://github.com/ydb-platform/fq-connector-go/pkgs/container/fq-connector-go). Он уже содержит [конфигурационный файл](https://github.com/ydb-platform/fq-connector-go/blob/main/app/server/config/config.prod.yaml) сервиса. Запустить сервис с настройками по умолчанию можно следующей командой:

    ```bash
    docker run -d \
        --name=fq-connector-go \
        -p 2130:2130 \
        ghcr.io/ydb-platform/fq-connector-go:latest
    ```

    На порту 2130 публичного сетевого интерфейса вашего хоста запустится слушающий сокет GRPC-сервиса коннектора. В дальнейшем сервер {{ ydb-short-name }} должен будет установить соединение именно с этим сетевым адресом.

1. При необходимости изменения конфигурации подготовьте конфигурационный файл [по образцу](#fq-connector-go-config) и примонтируйте его к контейнеру:

    ```bash
    docker run -d \
        --name=fq-connector-go \
        -p 2130:2130 \
        -v /path/to/config.yaml:/opt/ydb/cfg/fq-connector-go.yaml
        ghcr.io/ydb-platform/fq-connector-go:latest
    ```

2. В {% if oss %}[рекомендуемом режиме использования](./index.md#general-scheme){% else %}рекомендуемом режиме использования{% endif %}  коннектор развёртывается на тех же серверах, что и динамические узлы {{ ydb-short-name }}, следовательно, шифрование сетевых соединений между ними *не требуется*. Но если вам всё же необходимо включить шифрование между {{ ydb-short-name }} и коннектором, [подготовьте пару TLS-ключей](../initial-deployment.md#tls-certificates) и пропишите пути до публичного и приватного ключа в секции конфигурационного файла `connector_server.tls.cert` и `connector_server.tls.key` соответственно:

    ```yaml
    connector_server:
      # ...
      tls:
        cert: "/opt/ydb/certs/fq-connector-go.crt"
        key: "/opt/ydb/certs/fq-connector-go.key"
    ```

    При запуске контейнера примонтируйте внутрь него директорию с парой TLS-ключей так, чтобы они оказались доступны для процесса `fq-connector-go` по путям, указанным в конфигурационном файле:

    ```bash
    docker run -d \
        --name=fq-connector-go \
        -p 2130:2130 \
        -v /path/to/config.yaml:/opt/ydb/cfg/fq-connector-go.yaml
        -v /path/to/keys/:/opt/ydb/certs/
        ghcr.io/ydb-platform/fq-connector-go:latest
    ```

3. В случае, если внешние источники данных используют TLS, для организации шифрованных соединений с ними коннектору потребуется корневой или промежуточный сертификат удостоверяющего центра (Certificate Authority, CA), которым были подписаны сертификаты источников. Docker-образ для коннектора базируется на образе дистрибутива Alpine Linux, который уже содержит некоторое количество сертификатов от доверенных CA. Проверить наличие нужного CA в списке предустановленных можно следующей командой:

    ```bash
    docker run -it --rm ghcr.io/ydb-platform/fq-connector-go sh
    # далее в консоли внутри контейнера:
    apk add openssl
    awk -v cmd='openssl x509 -noout -subject' ' /BEGIN/{close(cmd)};{print | cmd}' < /etc/ssl/certs/ca-certificates.crt
    ```

    Если TLS-ключи для источников выпущены CA, не входящим в перечень доверенных, необходимо добавить сертификат этого CA в системные пути контейнера с коннектором. Сделать это можно, например, собрав собственный Docker-образ на основе имеющегося. Для этого подготовьте следующий `Dockerfile`:

    ```Dockerfile
    FROM ghcr.io/ydb-platform/fq-connector-go:latest

    USER root

    RUN apk --no-cache add ca-certificates openssl
    COPY root_ca.crt /usr/local/share/ca-certificates
    RUN update-ca-certificates
    ```

    Поместите `Dockerfile` и корневой сертификат CA в одной папке, зайдите в неё и соберите образ следующей командой:

    ```bash
    docker build -t fq-connector-go_custom_ca .
    ```

    Новый образ `fq-connector-go_custom_ca` можно использовать для развёртывания сервиса с помощью команд, приведённых выше.

### Конфигурация {#fq-connector-go-config}

Актуальный пример конфигурационного файла сервиса `fq-connector-go` можно найти в [репозитории](https://github.com/ydb-platform/fq-connector-go/blob/main/app/server/config/config.prod.yaml).

| Параметр | Назначение | Значение по умолчанию |
|----------|------------|-----------------------|
| `connector_server` | Обязательная секция. Содержит настройки основного GPRC-сервера, выполняющего доступ к данным. ||
| `connector_server.endpoint.host` | Хостнейм или IP-адрес, на котором запускается слушающий сокет сервиса. ||
| `connector_server.endpoint.port` | Номер порта, на котором запускается слушающий сокет сервиса. ||
| `connector_server.tls` | Опциональная секция. Заполняется, если требуется включение TLS-соединений для основного GRPC-сервиса `fq-connector-go`. | Сервер запускается без TLS. |
| `connector_server.tls.key` | Полный путь до закрытого ключа шифрования. ||
| `connector_server.tls.cert` | Полный путь до открытого ключа шифрования. ||
| `logger` | Опциональная секция. Содержит настройки логирования. ||
| `logger.log_level` | Уровень логгирования. Допустимые значения: `TRACE`, `DEBUG`, `INFO`, `WARN`, `ERROR`, `FATAL`. | `INFO` |
| `logger.enable_sql_query_logging` | Для источников данных, поддерживающих SQL, включает логирование транслированных запросов. Допустимые значения: `true`, `false`. **ВАЖНО**: Включение этой опции может привести к печати конфиденциальных пользовательских данных в логи. | `false` |
| `metrics_server` | Опциональная секция. Содержит настройки вспомогательного HTTP-сервера, выполняющего доступ к метрикам. | Сервер отключён. |
| `metrics_server.endpoint.host` | Хостнейм или IP-адрес, на котором запускается слушающий сокет сервиса. ||
| `metrics_server.endpoint.port` | Номер порта, на котором запускается слушающий сокет сервиса. ||
| `paging` | Опциональная секция. Содержит настройки алгоритма разбиения извлекаемого из источника потока данных на Arrow-блоки. На каждый запрос в коннекторе создаётся очередь из заранее подготовленных к отправке на сторону {{ ydb-short-name }} блоков данных. Аллокации Arrow-блоков формируют наиболее существенный вклад в потребление оперативной памяти процессом `fq-connector-go`. Минимальный объём памяти, необходимый коннектору для работы, можно приблизительно оценить по формуле $Mem = 2 \cdot Requests \cdot BPP \cdot PQC$, где $Requests$ — количество одновременно выполняемых запросов, $BPP$ — параметр `paging.bytes_per_page`, а $PQC$  — параметр `paging.prefetch_queue_capacity`. ||
| `paging.bytes_per_page` | Максимальное количество байт в одном блоке. Рекомендуемые значения - от 4 до 8 МиБ, максимальное значение - 48 МиБ. | 4 МиБ |
| `paging.prefetch_queue_capacity` | Количество заранее вычитываемых блоков данных, которые хранятся в адресном пространстве коннектора до обращения {{ ydb-short-name }} за очередным блоком данных. В некоторых сценариях бóльшие значения данной настройки могут увеличить пропускную способность, но одновременно приведут и к большему потреблению оперативной памяти процессом. Рекомендуемые значения - не менее 2. | 2 |
| `datasources` | Опциональная секция. Содержит настройки, специфичные для источников данных. ||
| `datasources.postgresql` | Опциональная секция. Содержит настройки, специфичные для источника данных PostgreSQL. ||
| `datasources.postgresql.open_connection_timeout` | Таймаут открытия соединения с PostgreSQL. Варианты допустимых значений: `100ms`, `5s`, `3h`. | `5s` |
| `datasources.postgresql.splitting` | Опциональная секция. Содержит различные настройки для процесса разбиения таблиц на сплиты. Её включение необходимо для массивно-параллельного чтения из PostgreSQL в {{ ydb-short-name }}. ||
| `datasources.postgresql.splitting.enabled` | Включает разбиение таблиц на сплиты. Допустимые значения: `true`, `false`. | `false` |
| `datasources.postgresql.splitting.table_physical_size_threshold_bytes` | Минимальное значение физического размера внешней таблицы для включения разбиения на сплиты. Все таблицы меньшего размера будут всегда читаться последовательно одним сплитом. ||
| `datasources.postgresql.pushdown` | Опциональная секция. Содержит различные настройки для пушдауна предикатов. ||
| `datasources.postgresql.pushdown.enable_timestamp_pushdown` | Включает пушдаун фильтров для столбцов типа YQL `Timestamp`. Допустимые значения: `true`, `false`. | `false` |
| `datasources.clickhouse` | Опциональная секция. Содержит настройки, специфичные для источника данных ClickHouse. ||
| `datasources.clickhouse.open_connection_timeout` | Таймаут открытия соединения с ClickHouse. Варианты допустимых значений: `100ms`, `5s`, `3h`. | `5s` |
| `datasources.clickhouse.ping_connection_timeout` | Таймаут проверки соединения с ClickHouse. Варианты допустимых значений: `100ms`, `5s`, `3h`. | `5s` |
| `datasources.clickhouse.pushdown` | Опциональная секция. Содержит различные настройки для пушдауна предикатов. ||
| `datasources.clickhouse.pushdown.enable_timestamp_pushdown` | Включает пушдаун фильтров для столбцов типа YQL `Timestamp`. Допустимые значения: `true`, `false`. | `false` |
| `datasources.mysql` | Опциональная секция. Содержит настройки, специфичные для источника данных MySQL. ||
| `datasources.mysql.result_chan_capacity` | Ёмкость буфера для извлечённых строк из сетевого соединения с базой. | `512` |
| `datasources.mysql.open_connection_timeout` | Таймаут открытия соединения с MySQL. Варианты допустимых значений: `100ms`, `5s`, `3h`. | `5s` |
| `datasources.mysql.pushdown` | Опциональная секция. Содержит различные настройки для пушдауна предикатов. ||
| `datasources.mysql.pushdown.enable_timestamp_pushdown` | Включает пушдаун фильтров для столбцов типа YQL `Timestamp`. Допустимые значения: `true`, `false`. | `false` |
| `datasources.ms_sql_server` | Опциональная секция. Содержит настройки, специфичные для источника данных MS SQL Server. ||
| `datasources.ms_sql_server.open_connection_timeout` | Таймаут открытия соединения с MS SQL Server. Варианты допустимых значений: `100ms`, `5s`, `3h`. | `5s` |
| `datasources.ms_sql_server.ping_connection_timeout` | Таймаут проверки соединения с MS SQL Server. Варианты допустимых значений: `100ms`, `5s`, `3h`. | `5s` |
| `datasources.ms_sql_server.pushdown` | Опциональная секция. Содержит различные настройки для пушдауна предикатов. ||
| `datasources.ms_sql_server.pushdown.enable_timestamp_pushdown` | Включает пушдаун фильтров для столбцов типа YQL `Timestamp`. Допустимые значения: `true`, `false`. | `false` |
| `datasources.greenplum` | Опциональная секция. Содержит настройки, специфичные для источника данных Greenplum. ||
| `datasources.greenplum.open_connection_timeout` | Таймаут открытия соединения с Greenplum. Варианты допустимых значений: `100ms`, `5s`, `3h`. | `5s` |
| `datasources.greenplum.pushdown` | Опциональная секция. Содержит различные настройки для пушдауна предикатов. ||
| `datasources.greenplum.pushdown.enable_timestamp_pushdown` | Включает пушадаун фильтров для столбцов типа YQL Timestamp. Допустимые значения: `true`, `false`. | `false` |
| `datasources.ydb` | Опциональная секция. Содержит настройки, специфичные для источника данных {{ ydb-short-name }}. ||
| `datasources.ydb.open_connection_timeout` | Таймаут открытия соединения с {{ ydb-short-name }}. Варианты допустимых значений: `100ms`, `5s`, `3h`. | `5s` |
| `datasources.ydb.ping_connection_timeout` | Таймаут проверки соединения с {{ ydb-short-name }}. Варианты допустимых значений: `100ms`, `5s`, `3h`. | `5s` |
| `datasources.ydb.mode` | Режим параметризует способ взаимодействия коннектора с сервером {{ ydb-short-name }}. Допустимые значения: `MODE_TABLE_SERVICE_STDLIB_SCAN_QUERIES` (использует Table Service {{ ydb-short-name }}), `MODE_QUERY_SERVICE_NATIVE` (использует Query Service {{ ydb-short-name }}). | `MODE_QUERY_SERVICE_NATIVE` |
| `datasources.ydb.splitting` | Опциональная секция. Содержит различные настройки для процесса разбиения таблиц на сплиты. Её включение необходимо для массивно-параллельного чтения из внешней {{ ydb-short-name }} в федеративную {{ ydb-short-name }}. ||
| `datasources.ydb.splitting.enabled_on_column_shards` | Включает разбиение на сплиты для колоночных таблиц. Допустимые значения: `true`, `false`. | `false` |
| `datasources.ydb.splitting.query_tablet_ids_timeout` | Таймаут для запроса, запрашивающего идентификаторы таблеток колоночной таблицы {{ ydb-short-name }}. Варианты допустимых значений: `100ms`, `5s`, `3h`. | `1m` |
| `datasources.ydb.resource_pool` | Название пула ресурсов, которое будет использоваться при выполнении запроса. Применимо только для режима `MODE_QUERY_SERVICE_NATIVE`. | `default` |
| `datasources.ydb.pushdown` | Опциональная секция. Содержит различные настройки для пушдауна предикатов. ||
| `datasources.ydb.pushdown.enable_timestamp_pushdown` | Включает проталкивание фильтров для столбцов типа YQL `Timestamp`. Допустимые значения: `true`, `false`. | `false` |
