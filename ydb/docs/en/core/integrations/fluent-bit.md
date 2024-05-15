# Log records collection using FluentBit

This section describes the integration between {{ ydb-short-name }} and log capture tool [FluentBit](https://fluentbit.io ) to save and analyse the log records in {{ ydb-short-name }}.


## Введение

FluentBit is a tool that can collect text data, manipulate it (modify, transform, combine) and send it into various repositories for further processing. A custom plugin library for FluentBit has been developed to support saving the log records into {{ ydb-short-name }}. The source code of the library is available in the [repository](https://github.com/ydb-platform/fluent-bit-ydb).

Deploying a log delivery scheme using FluentBit and {{ ydb-short-name }} as the destination database includes the following steps:

1. Create {{ ydb-short-name }} tables for the log data storage
2. Deploy FluentBit and {{ ydb-short-name }} plugin for FluentBit
3. Configure FluentBit to collect and process the logs
4. Configure FluentBit to send the logs to {{ ydb-short-name }} tables


## Creating the tables for log data

Tables for log data storage must be created in the chosen {{ ydb-short-name }} database. The structure of the tables is determined by a set of fields of a specific log supplied using Fluent Bit. Different log types may be saved to different tables, depending on the requirements. Normally the table for log data contains the following fields:

* timestamp;
* log level;
* host name;
* service name;
* message text or its semi-structural representation as JSON document.

{{ ydb-short-name }} tables must have primary key, which uniquely identifies the specific row in the table. Timestamp does not always uniquely identify the message coming from the particular source. To enforce the uniqueness of the primary key, a hash value can be added to the table. The hash value is computed using the CityHash64 algorithm over the log record data.

Row-based and columnar tables can be both used for log data storage. Columnar tables are recommended, as they support more efficient data scans for log data retrieval.

Example of the command to create the row-based table for log data storage:

```sql
CREATE TABLE `fluent-bit/log` (
    `timestamp`         Timestamp NOT NULL,
    `hostname`          Text NOT NULL,
    `input`             Text NOT NULL,
    `datahash`          Uint64 NOT NULL,
    `level`             Text NULL,
    `message`           Text NULL,
    `other`             JsonDocument NULL,
    PRIMARY KEY (
         `timestamp`, `hostname`, `input`, `datahash`
    )
);
```

Example of the command to create the columnar table for log data storage:

```sql
CREATE TABLE `fluent-bit/log` (
    `timestamp`         Timestamp NOT NULL,
    `hostname`          Text NOT NULL,
    `input`             Text NOT NULL,
    `datahash`          Uint64 NOT NULL,
    `level`             Text NULL,
    `message`           Text NULL,
    `other`             JsonDocument NULL,
    PRIMARY KEY (
         `timestamp`, `hostname`, `input`, `datahash`
    )
) PARTITION BY HASH(`timestamp`, `hostname`, `input`)
  WITH (STORE = COLUMN);
```

The command which creates the columnar table differs in just two last lines, which specify the table's partitioning key and columnar storage type.

[TTL configuration](../concepts/ttl.md) can be optionally applied to the table, limiting the data storage period and enabling the automatical removal of obsolete data. Enabling TTL requires extra setting in the `WITH` section of the table creation command, for example `TTL = Interval(P14D) ON timestamp` sets the storage period to 14 days, based upon the `timestamp` field's value.


## FluentBit deployment and configuration

FluentBit deployment should be performed in accordance with [its own documentation](https://docs.fluentbit.io/manual/installation/getting-started-with-fluent-bit).

{{ ydb-short-name }} plugin for FluentBit is available in the source code form in the [repository](https://github.com/ydb-platform/fluent-bit-ydb), along with the build instructions. For container-based deployment the Docker image is provided: `ghcr.io/ydb-platform/fluent-bit-ydb`.

The general logic, configuration syntax and procedures to set up the receiving, processing and delivering logs in the FluentBit environment are defined in the [FluentBit documentation](https://docs.fluentbit.io/manual/concepts/key-concepts).


## Writing logs to {{ ydb-short-name }} tables

The list of the enabled FluentBit plugins is configured in a file (for example, `plugins.conf`), which is referenced through the `plugins_file` parameter in the `SERVICE` section of the main FluentBit configuration file. Below is the example of the {{ ydb-short-name }} plugin enabled (plugin library path may be different depending on your setup):

```text
# plugins.conf
[PLUGINS]
    Path /usr/lib/fluent-bit/out_ydb.so
```

The table below lists the configuration parameters supported by the {{ ydb-short-name }} plugin for FluentBit.

| **Ключ** | **Описание** |
|----------|--------------|
| `Name`   | Тип плагина, константа `ydb` |
| `Match`  | (опционально) [Выражение для отбора тегов](https://docs.fluentbit.io/manual/concepts/data-pipeline/router) записей, направляемых в {{ ydb-short-name }} |
| `ConnectionURL` | URL для подключения к БД, включая протокол, хост, номер порта и путь к базе данных (см. [документацию](../concepts/connect.md)) |
| `TablePath` | Путь к таблице, начиная от корня базы данных (пример: `fluent-bit/log`) |
| `Columns` | JSON-структура, состоящая из пар имен колонок, и устанавливающая соответствие между исходными колонками записи и целевыми колонками таблицы. Может включать перечисленные далее служебные псевдо-колонки |
| `CredentialsAnonymous` | Устанавливается в значение `1` для использования анонимной аутентификации |
| `CredentialsToken` | Значение токена аутентификации, для использования аутентификации непосредственно по значению токена |
| `CredentialsYcMetadata` | Устанавливается в значение `1` для использования аутентификации через метаданные виртуальной машины |
| `CredentialsStatic` | Логин и пароль для использования статического режима аутентификации, указанные в формате `Логин:Пароль@` |
| `CredentialsYcServiceAccountKeyFile` | Путь к файлу ключа сервисного аккаунта, для аутентификации по ключу сервисного аккаунта |
| `CredentialsYcServiceAccountKeyJson` | JSON-данные ключа сервисного аккаунта, для аутентификации по ключу сервисного аккаунта без указания имени файла (удобно в среде K8s) |
| `Certificates` | Путь к файлу с сертификатом CA, либо непосредственно содержимое сертификата CA |
| `LogLevel` | Уровень логирования для плагина, одно из значений `disabled` (по умолчанию), `trace`, `debug`, `info`, `warn`, `error`, `fatal` or `panic` |

Для использования в карте сопоставления колонок (параметр `Columns`), в дополнение к полям записи FluentBit, доступны следующие служебные псевдо-колонки:

* `.timestamp` - метка времени сообщения (обязательная)
* `.input` - имя входного потока сообщений (обязательная)
* `.hash` - uint64 хеш-код, вычисленный над всеми полями сообщения, кроме псевдо-колонок (опциональная)
* `.other` - документ JSON, содержащий все поля сообщения, которые не были явным образом сопоставлены с конкретной выходной колонкой (опциональная)

Пример значения параметра `Columns`:

```json
{".timestamp": "timestamp", ".input": "input", ".hash": "datahash", "log": "message", "level": "level", "host": "hostname", ".other": "other"}
```

## Сбор логов в кластере Kubernetes

FluentBit очень часто используется для сбора логов в кластерах Kubernetes. Принципиальная схема доставки логов запущенных приложений в Kubernetes с помощью FluentBit с последующим сохранением их в {{ ydb-short-name }} выглядит следующим образом:

![FluentBit in Kubernetes cluster](../_assets/fluent-bit-ydb-scheme.png)
<small>Рисунок 1 — Схема взаимодействия FluentBit и {{ ydb-short-name }} в Kubernetes кластере</small>

На этой схеме:

* Поды приложений пишут логи в stdout/stderr
* Текст из stdout/stderr сохраняется в виде файлов на рабочих узлах Kubernetes
* Под с FluentBit:
  * Монтирует каталог с лог-файлами рабочего узла Kubernetes
  * Вычитывает из лог-файлов содержимое
  * Обогащает записи дополнительными метаданными
  * Сохраняет записи в базу данных {{ ydb-short-name }}

### Таблица для хранения логов Kubernetes

Структура таблицы {{ ydb-short-name }} для хранения логов Kubernetes:

```sql
CREATE TABLE `fluent-bit/log` (
    `timestamp`         Timestamp NOT NULL,
    `file`              Text NOT NULL,
    `pipe`              Text NOT NULL,
    `message`           Text NULL,
    `datahash`          Uint64 NOT NULL,
    `message_parsed`    JSON NULL,
    `kubernetes`        JSON NULL,

    PRIMARY KEY (
         `timestamp`, `file`, `datahash`
    )
) PARTITION BY HASH(`timestamp`, `file`)
  WITH (STORE = COLUMN, TTL = Interval(P14D) ON `timestamp`);
```

Предназначение колонок:

* `timestamp` – временная метка лога;
* `file` – название источника, из которого прочитан лог. В случае Kubernetes это будет имя файла, на worker ноде, в который записываются логи определенного pod;
* `pipe` – stdout или stderr поток, куда была осуществлена запись на уровне приложения;
* `datahash` – хеш-код, вычисленный над сообщением лога;
* `message` – непосредственно само сообщение лога;
* `message_parsed` – структурированное сообщение лога, если его удалось разобрать с помощью механизма парсеров в fluent-bit
* `kubernetes` – информация о pod, например: название, неймспейс, логи и аннотации.

Опционально, можно установить TTL для строк таблицы, как показано в примере выше.

### Конфигурация FluentBit

Перед развертыванием FluentBit в среде Kubernetes необходимо подготовить файл настроек (обычно `values.yaml`), в котором указываются параметры сбора и обработки логов.

Необходимо указать репозиторий и версию образа контейнера FluentBit:

```yaml
image:
  repository: ghcr.io/ydb-platform/fluent-bit-ydb
  tag: latest
```

В данном образе, по сравнению со стандартным, добавлена библиотека-плагин для поддержки {{ ydb-short-name }}.

В следующих строках определены правила монтирования папок с логами в поды FluentBit:

```yaml
volumeMounts:
  - name: config
    mountPath: /fluent-bit/etc/conf

daemonSetVolumes:
  - name: varlog
    hostPath:
      path: /var/log
  - name: varlibcontainers
    hostPath:
      path: /var/lib/containerd/containers
  - name: etcmachineid
    hostPath:
      path: /etc/machine-id
      type: File

daemonSetVolumeMounts:
  - name: varlog
    mountPath: /var/log
  - name: varlibcontainers
    mountPath: /var/lib/containerd/containers
    readOnly: true
  - name: etcmachineid
    mountPath: /etc/machine-id
    readOnly: true
```

Также необходимо переопределить команду и аргументы запуска FluentBit:

```yaml
command:
  - /fluent-bit/bin/fluent-bit

args:
  - --workdir=/fluent-bit/etc
  - --plugin=/fluent-bit/lib/out_ydb.so
  - --config=/fluent-bit/etc/conf/fluent-bit.conf
```

Требуется настроить пайплайн сбора, преобразования и доставки логов:

```yaml
config:
  inputs: |
    [INPUT]
        Name tail
        Path /var/log/containers/*.log
        multiline.parser cri
        Tag kube.*
        Mem_Buf_Limit 5MB
        Skip_Long_Lines On

  filters: |
    [FILTER]
        Name kubernetes
        Match kube.*
        Keep_Log On
        Merge_Log On
        Merge_Log_Key log_parsed
        K8S-Logging.Parser On
        K8S-Logging.Exclude On

    [FILTER]
        Name modify
        Match kube.*
        Remove time
        Remove _p

  outputs: |
    [OUTPUT]
        Name ydb
        Match kube.*
        TablePath fluent-bit/log
        Columns {".timestamp":"timestamp",".input":"file",".hash":"datahash","log":"message","log_parsed":"message_structured","stream":"pipe","kubernetes":"metadata"}
        ConnectionURL ${OUTPUT_YDB_CONNECTION_URL}
        CredentialsToken ${OUTPUT_YDB_CREDENTIALS_TOKEN}
```

Описание конфигурационных блоков:

* `inputs` - в этом блоке указываются откуда считывать и как разбирать логи. В данном случае, будет осуществляться чтение файликов *.log  из папки /var/log/containers/ , которая была смонтирована с хоста
* `filters` - в этом блоке указывается как будет осуществляться обработка логов. В данном случае: для каждого лога будут найдены соответствующие метаданные (в помощью kubernetes фильтра), а также, вырезаны неиспользуемые поля (_p, time)
* `outputs` - в этом блоке указывается, куда будут отгружены логи. В данном случае в таблицу `fluent-bit/log` в базе данных {{ ydb-short-name }}. Параметры подключения к базе данных (в данном случае `ConnectionURL` и `CredentialsToken`) задаются с помощью переменных окружения – `OUTPUT_YDB_CONNECTION_URL`, `OUTPUT_YDB_CREDENTIALS_TOKEN`. При необходимости настройки аутентификации и состав используемых переменных окружения корректируются в зависимости от настроек используемого кластера {{ ydb-short-name }}.

Переменные окружения определяются следующим образом:

```yaml
env:
  - name: OUTPUT_YDB_CONNECTION_URL
    value: grpc://ydb-endpoint:2135/path/to/database
  - name: OUTPUT_YDB_CREDENTIALS_TOKEN
    valueFrom:
      secretKeyRef:
        key: token
        name: fluent-bit-ydb-plugin-token
```

Данные аутентификации необходимо сохранить в конфигурации кластера Kubernetes в виде секрета. Пример команды для создания секрета:

```sh
kubectl create secret -n ydb-fluent-bit-integration generic fluent-bit-ydb-plugin-token --from-literal=token=<YDB TOKEN>
```

### Развертывание FluentBit в кластере Kubernetes

[HELM](https://helm.sh) – способ пакетирования и установки приложений в кластере Kubernetes. Для развертывания FluentBit необходимо добавить репозиторий с соответствующим чартом (сценарием установки) с помощью команды:

```sh
helm repo add fluent https://fluent.github.io/helm-charts
```

После этого установка FluentBit в кластер Kubernetes выполняется с помощью следующей команды:

```sh
helm upgrade --install fluent-bit fluent/fluent-bit \
  --version 0.37.1 \
  --namespace ydb-fluent-bit-integration \
  --create-namespace \
  --values values.yaml
```

В команде выше в аргументе `--values` указывается ранее подготовленный файл с настройками FluentBit.

### Проверка установки

Проверяем что FluentBit запустился, читая его логи (должны отсутствовать записи уровня `[error]`):

```sh
kubectl logs -n ydb-fluent-bit-integration -l app.kubernetes.io/instance=fluent-bit
```

Проверяем, что записи в таблице {{ ydb-short-name }} есть (появятся спустя примерно несколько минут после запуска FluentBit):

```sql
SELECT * FROM `fluent-bit/log` LIMIT 10 ORDER BY `timestamp` DESC
```

### Очистка ресурсов

Для удаления FluentBit достаточно удалить Kubernetes namespace, в который была выполнена установка:

```sh
kubectl delete namespace ydb-fluent-bit-integration
```

Далее можно удалить таблицу с логами в базе данных {{ ydb-short-name }}:

```sql
DROP TABLE `fluent-bit/log`
```
