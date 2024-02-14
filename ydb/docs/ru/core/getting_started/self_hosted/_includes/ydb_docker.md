# Запуск {{ ydb-short-name }} в Docker

Для отладки или тестирования вы можете запустить [Docker](https://docs.docker.com/get-docker/)-контейнер YDB.

## Параметры соединения {#conn}

В результате выполнения описанных ниже инструкций вы получите локальную базу данных YDB, к которой можно будет обратиться по следующим реквизитам:

{% list tabs %}

- gRPC

  - [Эндпоинт](../../../concepts/connect.md#endpoint): `grpc://localhost:2136`
  - [Путь базы данных](../../../concepts/connect.md#database): `/local`
  - [Аутентификация](../../../concepts/auth.md): Анонимная (без аутентификации)

- gRPCs/TLS

  - [Эндпоинт](../../../concepts/connect.md#endpoint): `grpcs://localhost:2135`
  - [Путь базы данных](../../../concepts/connect.md#database): `/local`
  - [Аутентификация](../../../concepts/auth.md): Анонимная (без аутентификации)

{% endlist %}

## Установка {#install}

Загрузите актуальную публичную версию Docker-образа:

```bash
docker pull {{ ydb_local_docker_image }}:{{ ydb_local_docker_image_tag }}
```

Проверьте, что Docker-образ успешно выгружен:

```bash
docker image list | grep {{ ydb_local_docker_image }}
```

Результат выполнения:

```bash
{{ ydb_local_docker_image }}           {{ ydb_local_docker_image_tag }}   b73c5c1441af   2 months ago   793MB
```

## Запуск {#start}

Docker-контейнер YDB использует ресурсы хост-системы (CPU, RAM) в пределах выделенных настройками Docker.

Docker-контейнер YDB хранит данные в файловой системе контейнера, разделы которой отражаются на директории в хост-системе. Приведенная ниже команда запуска контейнера создаст файлы в текущей директории, поэтому перед запуском создайте рабочую директорию, и выполняйте запуск из неё:

```bash
docker run -d --rm --name ydb-local -h localhost \
  -p 2135:2135 -p 8765:8765 -p 2136:2136 \
  -v $(pwd)/ydb_certs:/ydb_certs -v $(pwd)/ydb_data:/ydb_data \
  -e YDB_DEFAULT_LOG_LEVEL=NOTICE \
  -e GRPC_TLS_PORT=2135 -e GRPC_PORT=2136 -e MON_PORT=8765 \
  {{ ydb_local_docker_image}}:{{ ydb_local_docker_image_tag }}
```

{% list tabs %}

- Хранение данных на диске

    ```bash
    docker run -d --rm --name ydb-local -h localhost \
      -p 2135:2135 -p 8765:8765 -p 2136:2136 \
      -v $(pwd)/ydb_certs:/ydb_certs -v $(pwd)/ydb_data:/ydb_data \
      -e YDB_DEFAULT_LOG_LEVEL=NOTICE \
      -e GRPC_TLS_PORT=2135 -e GRPC_PORT=2136 -e MON_PORT=8765 \
      {{ ydb_local_docker_image}}:{{ ydb_local_docker_image_tag }}
    ```
    
    {% note warning %}

    На данный момент хранение данных на диске не поддерживается на Apple Silicon (M1 or M2). Используйте команду с вкладки "Хранение данных в памяти", если хотите попробовать {{ ydb-short-name }} на данных процессорах.

    {% endnote %}

- Хранение данных в памяти
    ```bash
    docker run -d --rm --name ydb-local -h localhost \
      -p 2135:2135 -p 8765:8765 -p 2136:2136 \
      -v $(pwd)/ydb_certs:/ydb_certs -v $(pwd)/ydb_data:/ydb_data \
      -e YDB_DEFAULT_LOG_LEVEL=NOTICE \
      -e GRPC_TLS_PORT=2135 -e GRPC_PORT=2136 -e MON_PORT=8765 \
      -e YDB_USE_IN_MEMORY_PDISKS=true \
      {{ ydb_local_docker_image}}:{{ ydb_local_docker_image_tag }}
    ```
{% endlist %}

При успешном запуске будет выведен идентификатор созданного контейнера.
### Параметры запуска {#start-pars}
`-d`: Запустить Docker-контейнер в фоновом режиме.
`--rm`: Удалить контейнер после завершения его работы.
`--name`: Имя контейнера. Укажите `ydb-local`, чтобы приведенные ниже инструкции по остановке контейнера можно было выполнить копированием текста через буфер обмена.
`-h`: Имя хоста контейнера. Должно быть обязательно передано значение `localhost`, иначе контейнер будет запущен со случайным именем хоста.
`-v`: Монтировать директории хост-системы в контейнер в виде `<директория хост-системы>:<директория монтирования в контейнере>`. Контейнер YDB использует следующие директории монтирования:
- `/ydb_data`: Размещение данных. Если данная директория не смонтирована, то контейнер будет запущен без сохранения данных на диск хост-системы.
- `/ydb_certs`: Размещение сертификатов для TLS соединения. Запущенный контейнер запишет туда сертификаты, которые вам нужно использовать для клиентского подключения с использованием TLS. Если данная директория не смонтирована, то вы не сможете подключиться по TLS, так как не будете обладать информацией о сертификате.
`-e`: Задать переменные окружения в виде `<имя>=<значение>`. Контейнер YDB использует следующие переменные окружения:
- `YDB_DEFAULT_LOG_LEVEL`: Уровень логирования. Допустимые значения: `CRIT`, `ERROR`, `WARN`, `NOTICE`, `INFO`. По умолчанию `NOTICE`.
- `GRPC_PORT`: Порт для нешифрованных соединений. По умолчанию 2136.
- `GRPC_TLS_PORT`: Порт для соединений с использованием TLS. По умолчанию 2135.
- `MON_PORT`: Порт для встроенного web-ui со средствами [мониторинга и интроспекции](../../../maintenance/embedded_monitoring/ydb_monitoring.md). По умолчанию 8765.
- `YDB_PDISK_SIZE`: Размер диска для хранения данных в формате `<NUM>GB` (например, `YDB_PDISK_SIZE=128GB`). Допустимые значения: от `80GB` и выше. По умолчанию 80GB.
- `YDB_USE_IN_MEMORY_PDISKS`: Использование дисков в памяти. Допустимые значения `true`, `false`, по умолчанию `false`. Во включенном состоянии не использует файловую систему контейнера для работы с данными, все данные хранятся только в памяти процесса, и теряются при его остановке. В настоящее время запуск контейнера на процессоре Apple Silicon (M1 или M2) возможен только в этом режиме.
- `YDB_FEATURE_FLAGS`: Флаги, позволяющие включить функции, отключенные по-умолчанию. Используется для функций, находящихся в разработке (по-умолчанию они выключены). Перечисляются через запятую.
- `POSTGRES_USER` - создать пользователя с указанным логином, используется для подключения через postgres-протокол.
- `POSTGRES_PASSWORD` - задать пароль пользователя для подключения через postgres-протокол.
- `YDB_TABLE_ENABLE_PREPARED_DDL` - временная опция, нужна для запуска Postgres-слоя совместимости, в будущем будет удалена.
- `FQ_CONNECTOR_ENDPOINT` - задать сетевой адрес коннектора ко внешним источникам данных для обработки [федеративных запросов](../../../concepts/federated_query/index.md). Формат строки `scheme://host:port`, где допустимыми значениями `scheme` могут быть `grpcs` (укаывает на подключение к коннектору по протоколу TLS) или `grpc` (подключение без шифрования).
`-p`: Опубликовать порты контейнера на хост-системе. Все применяемые порты должны быть явно перечислены, даже если используются значения по умолчанию.

{% include [_includes/storage-device-requirements.md](../../../_includes/storage-device-requirements.md) %}

{% note info %}

Инициализация Docker-контейнера, в зависимости от выделенных ресурсов, может занять несколько минут. До окончания инициализации база данных будет недоступна.

{% endnote %}

## Выполнение запросов {#request}

[Установите](../../../reference/ydb-cli/install.md) YDB CLI и выполните запрос, например:

```bash
ydb -e grpc://localhost:2136 -d /local scheme ls
```

Для успешного соединения с использованием TLS в параметры соединения нужно добавить имя файла с сертификатом. Запрос в примере ниже должен быть выполнен из той же рабочей директории, которую вы использовали для запуска контейнера:

```bash
ydb -e grpcs://localhost:2135 --ca-file ydb_certs/ca.pem -d /local scheme ls
```

Предсобранная версия [YDB CLI](../../../reference/ydb-cli/index.md) также доступа внутри образа:

```bash
docker exec <container_id> /ydb -e grpc://localhost:2136 -d /local scheme ls
```

, где

`<container_id>`: идентификатор контейнера, выведенный при его [запуске](#start).


## Остановка {#stop}

По окончании работы остановите Docker-контейнер:

```bash
docker kill ydb-local
```

## Лицензия и используемые компоненты {#license}

В корне Docker-контейнера расположены файл с текстом лицензионного соглашения (`LICENSE`) и список используемых компонентов и их лицензии (`THIRD_PARTY_LICENSES`).

Просмотрите текст лицензионного соглашения:

```bash
docker run --rm -it --entrypoint cat {{ ydb_local_docker_image }} LICENSE
```

Просмотрите все использованные при создании компоненты и их лицензии:

```bash
docker run --rm -it --entrypoint cat {{ ydb_local_docker_image }} THIRD_PARTY_LICENSES
```

## Запуск {{ ydb-short-name }} Federated Query в Docker

В данном разделе мы рассмотрим пример тестовой инсталляции {{ ydb-full-name }}, сконфигурированной для выполнения [федеративных запросов](../../../concepts/federated_query/index.md) к внешним источникам данных. Подключение {{ ydb-full-name }} к некоторым из источников требует развёртывания специального микросервиса - [коннектора](../../../concepts/federated_query/architecture.md#connectors). Приведённый ниже файл `docker-compose.yaml` описывает запуск контейнеров с тремя сервисами: 

* {{ ydb-short-name }} в одноузловой конфигурации;
* PostgreSQL (в качестве примера внешнего источника данных);
* Коннектор [fq-connector-go](../../../deploy/federated_query/connector.md#fq-connector-go).


```yaml
version: '3'

services:

  ydb:
    image: ghcr.io/ydb-platform/local-ydb:nightly
    container_name: fq-example-ydb
    hostname: localhost
    ports:
      - '2136:2136'
      - '8765:8765'
    environment:
      GRPC_TLS_PORT: 2135
      GRPC_PORT: 2136
      MON_PORT: 8765
      YDB_DEFAULT_LOG_LEVEL: NOTICE
      FQ_CONNECTOR_ENDPOINT: grpc://fq-connector-go:50051 

  postgresql:
    image: postgres
    container_name: fq-example-postgresql
    environment:
      POSTGRES_DB: fq
      POSTGRES_USER: admin
      POSTGRES_PASSWORD: password
      PGDATA: /var/lib/postgresql/data/pgdata

  fq-connector-go:
    image: ghcr.io/ydb-platform/fq-connector-go:latest
    container_name: fq-example-connector
```

Используя приведённый выше файл `docker-compose.yml`, разверните инсталляцию с помощью команд:
```bash
docker-compose pull
docker-compose up -d
```

Инициализируйте любым доступным вам способом данные внутри внешнего источника, например, подключившись к нему через CLI:
```bash
docker exec -it fq-example-postgresql psql -d fq --user admin -c "
    DROP TABLE IF EXISTS example_1;
    CREATE TABLE example_1 (id integer, col1 text, col2 integer);
    INSERT INTO example_1 VALUES (1, 'a', 10);
    INSERT INTO example_1 VALUES (2, 'b', 20);
    INSERT INTO example_1 VALUES (3, 'c', 30);
    INSERT INTO example_1 VALUES (4, 'd', 40);
    INSERT INTO example_1 VALUES (5, 'e', 50);
    INSERT INTO example_1 VALUES (6, NULL, 1)"
```

Откройте в браузере [страницу](http://localhost:8765/monitoring/tenant?schema=%2Flocal&name=%2Flocal) с веб-интерфейсом базы данных `/local` локально развёрнутого инстанса {{ ydb-short-name }}. В панели для запросов введите следующий код, регистрирующий базу данных `fq` из локального инстанса PostgreSQL в качестве внешнего источника данных в качестве внешнего источника данных для {{ ydb-short-name }}:

```sql
# Создаётся секрет, содержащий пароль пользователя admin базы данных PostgreSQL
CREATE OBJECT pg_local_password (TYPE SECRET) WITH (value = password);

CREATE EXTERNAL DATA SOURCE pg_local WITH (
    SOURCE_TYPE="PostgreSQL",                   # тип источника данных
    DATABASE_NAME="fq",                         # имя базы данных
    LOCATION="fq-example-postgresql:5432",      # сетевой адрес инстанса базы данных
    AUTH_METHOD="BASIC",                        # режим аутентификации по логину и паролю                 
    LOGIN="admin",                              # логин для доступа к внешнему источнику
    PASSWORD_SECRET_NAME="pg_local_password",   # имя секрета, содержащего пароль пользователя
    USE_TLS="FALSE",                            # признак, указывающий на то, использует ли внешний источник TLS-шифрование
    PROTOCOL="NATIVE"                           # протокол доступа к внешнему источнику данных
);
```

В селекторе типов запросов внизу страницы выберите `Query type: YQL Script` и нажмите кнопку `Run`. Запрос должен завершиться успешно.

Затем введите запрос, непосредственно извлекающий данные из таблицы `example_1` внешнего источника:

```sql
SELECT * FROM pg_local.example_1;
```

В селекторе типов запросов внизу страницы выберите `Query type: YQL - QueryService` и нажмите кнопку `Run`. На экране появятся данные таблицы, созданной во внешнем источнике несколькими шагами ранее.

{% note info %}

О дополнительных опциях запуска коннектора можно узнать в [руководстве по развёртыванию](../../../deploy/federated_query/connector.md#fq-connector-go-launch). В качестве внешнего источника данных можно использовать любое хранилище из перечня [поддерживаемых](../../../concepts/federated_query/architecture.md#supported-datasources).

{% endnote %}
