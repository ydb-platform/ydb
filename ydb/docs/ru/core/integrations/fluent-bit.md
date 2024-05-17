# Сбор логов в кластере Kubernetes с помощью FluentBit и YDB

В разделе представлена реализация интеграции между инструментом захвата логов кластера Kubernetes – FluentBit с последующим сохранением для просмотра или анализа в {{ ydb-short-name }}.

## Введение

FluentBit – инструмент, который может собирать текстовые данные, манипулировать ими (изменять, преобразовывать, объединять) и отправлять в различные хранилища для дальнейшей обработки.

Чтобы развернуть схему доставки логов запущенных приложений в Kubernetes с помощью FluentBit с последующим сохранением их в YDB, необходимо:

* Создать таблицу в YDB

* Сконфигурировать [FluentBit](https://fluentbit.io)

* Развернуть [FluentBit](https://fluentbit.io) в кластере через [HELM](https://helm.sh)

Принципиальная схема работы выглядит следующим образом:

![FluentBit in Kubernetes cluster](../_assets/fluent-bit-ydb-scheme.png)
<small>Рисунок 1 — Схема взаимодействия FluentBit и YDB в Kubernetes кластере</small>

На этой схеме:

* Поды приложений пишут логи в stdout/stderr

* Текст из stdout/stderr сохраняется в виде файлов на worker нодах Kubernetes

* Под с FluentBit

  * Монтирует под себя папку с лог-файлами

  * Вычитывает из них содержимое

  * Обогащает записи дополнительными метаданными

  * Сохраняет записи в YDB кластер

## Создание таблицы

В выбранном кластере YDB необходимо выполнить следующий запрос:

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
)
```

Предназначение колонок:

* timestamp – временная метка лога

* file – название источника, из которого прочитан лог. В случае Kubernetes это будет имя файла, на worker ноде, в который записываются логи определенного pod

* pipe – stdout или stderr поток, куда была осуществлена запись на уровне приложения

* message – непосредственно само сообщение лога

* datahash – Хеш-код CityHash64, вычисленный над сообщением лога (требуется для исключения перезаписи сообщений из одного и того же источника и с одинаковой меткой времени)

* message_parsed – структурированное сообщение лога, если его удалось разобрать с помощью механизма парсеров в fluent-bit

* kubernetes – некоторая информация о pod, например: название, неймспейс, логи и аннотации

Опционально, можно установить TTL для строк таблицы (т.к. данных будет достаточно много)

## Конфигурация FluentBit


Необходимо заменить репозиторий и версию образа:

```yaml
image:
  repository: ghcr.io/ydb-platform/fluent-bit-ydb
  tag: latest
```

В данном образе добавлена библиотека-плагин, реализующая поддержку YDB. Исходный код доступен [здесь](https://github.com/ydb-platform/fluent-bit-ydb)

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

Также, необходимо переопределить команду и аргументы запуска:

```yaml
command:
  - /fluent-bit/bin/fluent-bit

args:
  - --workdir=/fluent-bit/etc
  - --plugin=/fluent-bit/lib/out_ydb.so
  - --config=/fluent-bit/etc/conf/fluent-bit.conf
```

И сам пайплайн сбора, преобразования и доставки логов:

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

Описание ключей:

* Inputs. В этом блоке указываются откуда считывать и как разбирать логи. В данном случае, будет осуществляться чтение файликов *.log  из папки /var/log/containers/ , которая была смонтирована с хоста

* Filters. В этом блоке указывается как будет осуществляться обработка логов. В данном случае: для каждого лога будут найдены соответствующие метаданные (в помощью kubernetes фильтра), а также, вырезаны неиспользуемые поля (_p, time)

* Outputs. В этом блоке указывается куда будут отгружены логи. В данном случае в табличку `fluent-bit/log` в кластере {{ ydb-short-name }}. Параметры подключения к кластеру (ConnectionURL, CredentialsToken) задаются с помощью соответствующих переменных окружений – `OUTPUT_YDB_CONNECTION_URL`, `OUTPUT_YDB_CREDENTIALS_TOKEN`

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

Секретный авторизационный токен необходимо создать заранее в кластере. Например, с помощью команды:

```sh
kubectl create secret -n ydb-fluent-bit-integration generic fluent-bit-ydb-plugin-token --from-literal=token=<YDB TOKEN>
```

## Развертывание FluentBit

HELM – способ пакетирования и установки приложений в кластере Kubernetes. Для развертывания FluentBit необходимо добавить репозиторий с чартом с помощью команды:

```sh
helm repo add fluent https://fluent.github.io/helm-charts
```

Установка FluentBit в кластер Kubernetes выполняется с помощью следующей команды:

```sh
helm upgrade --install fluent-bit fluent/fluent-bit \
  --version 0.37.1 \
  --namespace ydb-fluent-bit-integration \
  --create-namespace \
  --values values.yaml
```

## Проверяем установку

Проверяем что fluent-bit запустился, читая его логи (должны отсутствовать записи уровня [error]):

```sh
kubectl logs -n ydb-fluent-bit-integration -l app.kubernetes.io/instance=fluent-bit
```

Проверяем что записи в таблице YDB есть (появятся спустя примерно несколько минут после запуска FluentBit):

```sql
SELECT * FROM `fluent-bit/log` LIMIT 10 ORDER BY `timestamp` DESC
```

## Очистка ресурсов

Достаточно удалить namespace с установленным fluent-bit:

```sh
kubectl delete namespace ydb-fluent-bit-integration
```

И таблицу с логами:

```sql
DROP TABLE `fluent-bit/log`
```
