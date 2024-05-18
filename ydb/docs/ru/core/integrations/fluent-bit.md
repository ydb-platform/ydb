# Сбор логов с помощью FluentBit

В разделе представлена реализация интеграции между {{ ydb-short-name }} и инструментом захвата логов [FluentBit](https://fluentbit.io) с целью сохранения данных логов в {{ ydb-short-name }} для последующего просмотра или анализа.


## Введение

FluentBit – инструмент, который может собирать текстовые данные, манипулировать ими (изменять, преобразовывать, объединять) и отправлять в различные хранилища для дальнейшей обработки. Для поставки логов в таблицы {{ ydb-short-name }} с помощью FluentBit разработана библиотека-плагин. Исходный код библиотеки доступен [здесь](https://github.com/ydb-platform/fluent-bit-ydb).

Чтобы развернуть схему доставки логов с помощью FluentBit с последующим сохранением их в {{ ydb-short-name }}, необходимо:

1. Создать таблицы {{ ydb-short-name }} для хранения логов
2. Развернуть FluentBit и библиотеку-плагин для поддержки {{ ydb-short-name }}
3. Сконфигурировать FluentBit для сбора и обработки логов в соответствии с [документацией](https://docs.fluentbit.io/manual)
4. Настроить сохранение логов в таблицы {{ ydb-short-name }}


## Создание таблиц для хранения логов

Для сохранения логов необходимо создать таблицы в выбранной базе данных {{ ydb-short-name }}. Структура создаваемых таблиц определяется набором полей конкретного лога, поставляемого с помощью FluentBit. Типичный набор полей включает в себя:

* метку времени;
* уровень критичности сообщения;
* имя хоста, с которого получено сообщение;
* наименование сервиса;
* текст сообщения либо его структурированные данные в формате JSON.

В таблицах {{ ydb-short-name }} обязательно наличие первичного ключа, уникально идентифицирующего конкретную запись. Поскольку метка времени не обеспечивает уникальной идентификации записи лога, в состав хранимых полей обычно добавляют значение хеш-кода над данными сообщения, которое включают в состав первичного ключа.

Поддерживается хранение логов как в строковых, так и колоночных таблицах. Рекомендуется использование колоночных таблиц, как обеспечивающих более эффективное сканирование данных при выполнении запросов.

Пример команды создания строковой таблицы для хранения логов:

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
         `datahash`, `timestamp`, `hostname`, `input`
    )
);
```

Пример команды создания колоночной таблицы для хранения логов:

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

Команда создание колоночной таблицы отличается от команды создания строковой таблицы следующими деталями:

* указанием в двух последних строках колоночного режима хранения и отдельного ключа партиционирования;
* колонка `timestamp` указана первой в составе первичного ключа таблицы, что является оптимальным и рекомендованным для колоночных таблиц, и не рекомендовано для строчных таблиц. См. также рекомендации по выбору первичного ключа для [колоночных таблиц](../dev/primary-key/column-oriented.md) и для [строчных таблиц](../dev/primary-key/row-oriented.md).

Опционально можно [настроить TTL](../concepts/ttl.md) для строк таблицы, ограничив срок хранения и обеспечив автоматическое удаление устаревших данных. Для этого при создании таблицы в секции `WITH` указываются настройки TTL, например `TTL = Interval("P14D") ON timestamp` устанавливает срок хранения записей в 14 дней на основе значения поля `timestamp`.


## Развертывание FluentBit и настройка сбора логов

Развертывание FluentBit осуществляется в соответствии с [собственной документацией](https://docs.fluentbit.io/manual/installation/getting-started-with-fluent-bit).

Плагин для поддержки {{ ydb-short-name }} в FluentBit доступен в [репозитории](https://github.com/ydb-platform/fluent-bit-ydb) вместе с инструкциями по сборке. Для установки в формате контейнера предусмотрен Docker-образ `ghcr.io/ydb-platform/fluent-bit-ydb`.

Общая логика и порядок настройки процессов получения, обработки и поставки логов в среде FluentBit изложены в [документации FluentBit](https://docs.fluentbit.io/manual/concepts/key-concepts).


## Настройка записи логов в {{ ydb-short-name }}

Перед использованием выходного плагина для {{ ydb-short-name }}, его необходимо включить в настройках FluentBit. Перечень используемых плагинов FluentBit определяется в отдельном файле (например, `plugins.conf`), путь к которому устанавливается параметром `plugins_file` секции `SERVICE` основного файла настроек FluentBit. Пример файла плагинов со ссылкой на библиотеку плагина для записи в {{ ydb-short-name }} (путь к библиотеке в вашей системе может отличаться):

```text
# plugins.conf
[PLUGINS]
    Path /usr/lib/fluent-bit/out_ydb.so
```

Состав конфигурационных параметров, поддерживаемых выходным плагином {{ ydb-short-name }} для FluentBit, приведен в таблице ниже.

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
| `LogLevel` | Уровень логирования для плагина, одно из значений `disabled` (по умолчанию), `trace`, `debug`, `info`, `warn`, `error`, `fatal` либо `panic` |

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
  WITH (STORE = COLUMN, TTL = Interval("P14D") ON `timestamp`);
```

Предназначение колонок:

* `timestamp` – временная метка лога;
* `file` – название источника, из которого прочитан лог. В случае Kubernetes это будет имя файла, на worker ноде, в который записываются логи определенного pod;
* `pipe` – stdout или stderr поток, куда была осуществлена запись на уровне приложения;
* `datahash` – хеш-код, вычисленный над записью лога;
* `message` – текстовая часть записи лога;
* `message_parsed` – структурированное представление записи лога, если его удалось разобрать из текстовой части с помощью настроенных парсеров FluentBit;
* `kubernetes` – информация о pod, включая название, неймспейс и аннотации.

Опционально, можно установить TTL для строк таблицы, как показано в примере выше.

### Конфигурация FluentBit

Перед развертыванием FluentBit в среде Kubernetes необходимо подготовить файл настроек (обычно `values.yaml`), в котором указываются параметры сбора и обработки логов. В этом разделе представлены необходимые пояснения по заполнению этого файла с примерами.

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

* `inputs` - в этом блоке указываются откуда считывать и как разбирать логи. В данном случае, будет осуществляться чтение файликов `*.log`  из папки `/var/log/containers/`, которая была смонтирована с хоста
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

[HELM](https://helm.sh) – это инструмент пакетирования и установки приложений в кластере Kubernetes. Для развертывания FluentBit необходимо добавить репозиторий с соответствующим чартом (сценарием установки) с помощью команды:

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
