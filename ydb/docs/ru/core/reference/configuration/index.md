# Параметры конфигурации кластера

Конфигурация кластера задается в YAML-файле, который передается в параметре `--yaml-config` при запуске узлов кластера. В данной статье приведено описание основных разделов конфигурации и ссылки на подробную документацию по каждому разделу.

Каждый раздел конфигурации служит определенной цели в настройке работы кластера {{ ydb-short-name }}, от распределения аппаратных ресурсов до настроек безопасности и функциональных флагов. Конфигурация организована в логические группы, соответствующие различным аспектам управления кластером и его работы.

## Разделы конфигурации

### Синтаксис

```yaml
host_configs:
- host_config_id: 1
  drive:
  - path: <path_to_device>
    type: <type>
  - path: ...
- host_config_id: 2
  ...
```

Атрибут `host_config_id` задает числовой идентификатор конфигурации. В атрибуте `drive` содержится коллекция описаний подключенных дисков. Каждое описание состоит из двух атрибутов:

- `path` : Путь к смонтированному блочному устройству, например `/dev/disk/by-partlabel/ydb_disk_ssd_01`
- `type` : Тип физического носителя устройства: `ssd`, `nvme` или `rot` (rotational - HDD)

### Примеры

Одна конфигурация с идентификатором 1, с одним диском типа SSD, доступным по пути `/dev/disk/by-partlabel/ydb_disk_ssd_01`:

``` yaml
host_configs:
- host_config_id: 1
  drive:
  - path: /dev/disk/by-partlabel/ydb_disk_ssd_01
    type: SSD
```

Две конфигурации с идентификаторами 1 (два SSD диска) и 2 (три SSD диска):

``` yaml
host_configs:
- host_config_id: 1
  drive:
  - path: /dev/disk/by-partlabel/ydb_disk_ssd_01
    type: SSD
  - path: /dev/disk/by-partlabel/ydb_disk_ssd_02
    type: SSD
- host_config_id: 2
  drive:
  - path: /dev/disk/by-partlabel/ydb_disk_ssd_01
    type: SSD
  - path: /dev/disk/by-partlabel/ydb_disk_ssd_02
    type: SSD
  - path: /dev/disk/by-partlabel/ydb_disk_ssd_03
    type: SSD
```

### Особенности Kubernetes {#host-configs-k8s}

{{ ydb-short-name }} Kubernetes operator монтирует NBS диски для Storage узлов на путь `/dev/kikimr_ssd_00`. Для их использования должна быть указана следующая конфигурация `host_configs`:

``` yaml
host_configs:
- host_config_id: 1
  drive:
  - path: /dev/kikimr_ssd_00
    type: SSD
```

Файлы с примерами конфигурации, поставляемые в составе {{ ydb-short-name }} Kubernetes operator, уже содержат такую секцию, и её не нужно менять.

## hosts — статические узлы кластера {#hosts}

В данной группе перечисляются статические узлы кластера, на которых запускаются процессы работы со Storage, и задаются их основные характеристики:

- Числовой идентификатор узла
- DNS-имя хоста и порт, по которым может быть установлено соединение с узлом в IP network
- Идентификатор [типовой конфигурации хоста](#host-configs)
- Размещение в определенной зоне доступности, стойке
- Инвентарный номер сервера (опционально)

### Синтаксис

``` yaml
hosts:
- host: <DNS-имя хоста>
  host_config_id: <числовой идентификатор типовой конфигурации хоста>
  port: <порт> # 19001 по умолчанию
  location:
    unit: <строка с инвентарным номером сервера>
    data_center: <строка с идентификатором зоны доступности>
    rack: <строка с идентификатором стойки>
- host: <DNS-имя хоста>
  # ...
```

### Примеры

``` yaml
hosts:
- host: hostname1
  host_config_id: 1
  node_id: 1
  port: 19001
  location:
    unit: '1'
    data_center: '1'
    rack: '1'
- host: hostname2
  host_config_id: 1
  node_id: 2
  port: 19001
  location:
    unit: '1'
    data_center: '1'
    rack: '1'
```

### Особенности Kubernetes {#hosts-k8s}

При развертывании {{ ydb-short-name }} с помощью оператора Kubernetes секция `hosts` полностью генерируется автоматически, заменяя любой указанный пользователем контент в передаваемой оператору конфигурации. Все Storage узлы используют `host_config_id` = `1`, для которого должна быть задана [корректная конфигурация](#host-configs-k8s).


## Конфигурация безопасности {#security}

В разделе `security_config` задаются режимы [аутентификации](../../security/authentication.md), первичная конфигурация локальных [пользователей](../../concepts/glossary.md#access-user) и [групп](../../concepts/glossary.md#access-group) и их [права](../../concepts/glossary.md#access-right).

```yaml
security_config:
  # настройка режима аутентификации
  enforce_user_token_requirement: false
  enforce_user_token_check_requirement: false
  default_user_sids: <аутентификационный токен для анонимных запросов>
  all_authenticated_users: <имя группы всех аутентифицированных пользователей>
  all_users_group: <имя группы всех пользователей>

  # первичные настройки безопасности
  default_users: <список пользователей по умолчанию>
  default_groups: <список групп по умолчанию>
  default_access: <список прав по умолчанию на корне кластера>

  # настройки привилегий
  viewer_allowed_sids: <список SID'ов с правами просмотра состояния кластера>
  monitoring_allowed_sids: <список SID'ов с правами просмотра и изменения состояния кластера>
  administration_allowed_sids: <список SID'ов с доступом администратора кластера>
  register_dynamic_node_allowed_sids: <список SID'ов с правами регистрации узлов баз данных в кластере>
```

Каждый клиент State Storage (например, таблетка DataShard) использует `nto_select` узлов для записи копий его данных в State Storage. Если State Storage состоит из большего количества узлов чем `nto_select`, то разные узлы могут быть использованы для разных клиентов, поэтому необходимо обеспечить, чтобы любое подмножество из `nto_select` узлов в пределах State Storage отвечало критериям отказоустойчивости.

Для `nto_select` должны использоваться нечетные числа, так как использование четных чисел не улучшает отказоустойчивость по сравнению с ближайшим меньшим нечетным числом.

### Конфигурация безопасности {#security}

В разделе `domains_config.security_config` задаются режимы [аутентификации](../../security/authentication.md), первичная конфигурация локальных [пользователей](../../concepts/glossary.md#access-user) и [групп](../../concepts/glossary.md#access-group) и их [права](../../concepts/glossary.md#access-right).

```yaml
domains_config:
  ...
  security_config:
    # настройка режима аутентификации
    enforce_user_token_requirement: false
    enforce_user_token_check_requirement: false
    default_user_sids: <аутентификационный токен для анонимных запросов>
    all_authenticated_users: <имя группы всех аутентифицированных пользователей>
    all_users_group: <имя группы всех пользователей>

    # первичные настройки безопасности
    default_users: <список пользователей по умолчанию>
    default_groups: <список групп по умолчанию>
    default_access: <список прав по умолчанию на корне кластера>

    # настройки привилегий
    viewer_allowed_sids: <список SID'ов с правами просмотра состояния кластера>
    monitoring_allowed_sids: <список SID'ов с правами просмотра и изменения состояния кластера>
    administration_allowed_sids: <список SID'ов с доступом администратора кластера>
```

[//]: # (TODO: wait for pull/9387, dynamic_node_registration to add info about "register_dynamic_node_allowed_sids: <список SID'ов с правами подключения динамических нод в кластер>")

#### Настройки режима аутентификации {#security-auth}

#|
|| **Раздел** | **Обязателен** | **Описание** ||
|| [{#T}](actor_system_config.md) | Да | Распределение CPU-ресурсов по пулам акторной системы ||
|| [{#T}](auth_config.md) | Нет | Настройки аутентификации и авторизации ||
|| [{#T}](blob_storage_config.md) | Нет | Конфигурация статической группы кластера для системных таблеток ||
|| [{#T}](bridge_config.md) | Нет | Конфигурация [режима bridge](../../concepts/bridge.md) ||
|| [{#T}](client_certificate_authorization.md) | Нет | Аутентификация с помощью клиентских сертификатов ||
|| [{#T}](domains_config.md) | Нет | Конфигурация домена кластера, включая Blob Storage и State Storage ||
|| [{#T}](feature_flags.md) | Нет | Функциональные флаги для включения или отключения определённых возможностей {{ ydb-short-name }} ||
|| [{#T}](healthcheck_config.md) | Нет | Пороговые значения и таймауты сервиса Health Check ||
|| [{#T}](hive_config.md) | Нет | Конфигурация запуска таблеток ||
|| [{#T}](host_configs.md) | Нет | Типовые конфигурации хостов для узлов кластера ||
|| [{#T}](hosts.md) | Да | Конфигурация статических узлов кластера ||
|| [{#T}](kafka_proxy_config.md) | Нет | Конфигурация [Kafka Proxy](../../reference/kafka-api/index.md) ||
|| [{#T}](log_config.md) | Нет | Конфигурация и параметры логирования ||
|| [{#T}](memory_controller_config.md) | Нет | Распределение памяти и лимиты для компонентов базы данных ||
|| [{#T}](node_broker_config.md) | Нет | Конфигурация стабильных имен узлов ||
|| [{#T}](query_service_config.md) | Нет | Конфигурация внешних источников для федеративных запросов ||
|| [{#T}](resource_broker_config.md) | Нет | Брокер ресурсов для контроля потребления CPU и памяти ||
|| [{#T}](security_config.md) | Нет | Настройки конфигурации безопасности ||
|| [{#T}](table_service_config.md) | Нет | Настройки конфигурации выполнения запросов||
|| [{#T}](tls.md) | Нет | Конфигурация TLS для безопасных соединений ||
|#

## Практические рекомендации

Этот раздел документации посвящён полному описанию доступных настроек, а практические рекомендации по тому, что и когда настраивать, можно найти в следующих местах:

- В рамках первоначального развёртывания кластера {{ ydb-short-name }}:

- [Ansible](../../devops/deployment-options/ansible/initial-deployment/index.md)
- [Kubernetes](../../devops/deployment-options/kubernetes/initial-deployment.md)
- [Вручную](../../devops/deployment-options/manual/initial-deployment/index.md)

- В рамках [поиска и устранения неисправностей](../../troubleshooting/index.md)
- В рамках [усиления безопасности](../../security/index.md)

## Примеры конфигураций кластеров

Модельные конфигурации кластера для развертывания можно найти в [репозитории](https://github.com/ydb-platform/ydb/tree/main/ydb/deploy/yaml_config_examples/). Изучите их перед развертыванием кластера.
