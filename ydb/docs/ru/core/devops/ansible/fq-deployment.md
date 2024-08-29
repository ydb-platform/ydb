# Развертывание {{ ydb-short-name }} кластера с коннектором для функции Federated Query с помощью Ansible

[Федеративные запросы](../../concepts/federated_query/index.md) - это способ получать информацию из различных источников данных без необходимости переноса данных этих источников внутрь {{ ydb-full-name }}. Для работы с большинством [внешних источников данных](../../concepts/datamodel/external_data_source.md) необходимо использование [коннектора](../../concepts/federated_query/architecture.md#connectors).

Эта инструкция описывает развертывание [коннектора fq-connector-go](../../deploy/manual/connector.md#fq-connector-go) в [новом](#new-cluster) или [существующем](#existing-cluster) кластере {{ ydb-short-name }} с помощью [Ansible](https://www.ansible.com)

## Развертывание нового {{ ydb-short-name }} кластера с Federated Query коннектором {#new-cluster}

Перед прочтением рекомендуется ознакомиться с [{#T}](./initial-deployment.md). Развертывание {{ ydb-short-name }} кластера с Federated Query коннектором похоже на развертывание любого другого {{ ydb-short-name }} кластера.

Шаги развертывания:

1. По [инструкции](./initial-deployment.md) подготовьте сервера, Ansible окружение, загрузите [репозиторий с шаблонами конфигурации](https://github.com/ydb-platform/ydb-ansible-examples).
2. Выберите один из шаблонов: `3-nodes-mirror-3-dc-fq`, `8-nodes-block-4-2-fq` или `9-nodes-mirror-3-dc-fq`. Конфигурационные файлы в этих шаблонах адаптированы под использование функции Federated Query.
3. Выполните [шаги по подготовке шаблона конфигурации](./initial-deployment.md#erasure-setup), но пока **НЕ** запускайте команду `ansible-playbook ydb_platform.ydb.initial_setup`.
4. Внесите дополнительные изменения в разделе `vars` инвентори-файла `files/50-inventory.yaml`:
    1. Выберите один из доступных вариантов развёртывания исполняемых файлов fq-connector-go:
        1. `ydb_fq_connector_archive`: локальный путь к архиву с дистрибутивом fq-connector-go, [загруженному](https://github.com/ydb-platform/fq-connector-go/releases) или подготовленному заранее.
        2. `ydb_fq_connector_binary`: локальный путь к исполняемому файлу fq-connector-go, [загруженному](https://github.com/ydb-platform/fq-connector-go/releases) или подготовленному заранее.
    2. Если необходимо, отредактируйте конфигурационный файл fq-connector-go `files/fq_config.yaml` ([документация по конфигурации](../../deploy/manual/connector.md#fq-connector-go-config)):
        1. `ydb_fq_connector_config`: укажите локальный путь до конфигурационного файла fq-connector-go.
    3. `ydb_fq_connector_dir`: укажите директорию, в которую fq-connector-go будет установлен на сервере.
5. Если необходимо, отредактируйте конфигурационный файл {{ ydb-short-name }} `files/config.yaml`
6. Если необходимо, [включите функцию multislot развертывания](#multislot) (функция работоспособна только для {{ ydb-short-name }} версии 24.3.3 или старше).
7. Выполните команду `ansible-playbook ydb_platform.ydb.initial_setup`, находясь в директории клонированного шаблона.

В результате выполнения плейбука будет создан кластер {{ ydb-short-name }}, на котором развернута тестовая база данных – `database`, создан `root` пользователь с максимальными правами доступа и запущен Embedded UI на порту 8765. Для подключения к Embedded UI можно настроить SSH-туннелирование. Для этого на локальной машине выполните команду `ssh -L 8765:localhost:8765 -i <ssh private key> <user>@<first ydb static node ip>`. После успешного установления соединения можно перейти по URL [localhost:8765](http://localhost:8765):

![ydb-web-ui](../../_assets/ydb-web-console.png)

К кластеру применимы инструкции:

* [Мониторинг состояния кластера](./initial-deployment.md#troubleshooting)
* [Тестирование кластера](./initial-deployment.md#testing)

Также в кластере будет развернут fq-connector-go, что позволит работать с [поддерживаемыми внешними источниками данных](../../concepts/federated_query/architecture.md#supported-datasources). Инструкции, как подключиться к внешнему источнику, можно найти [по этой ссылке](../../concepts/federated_query/index.md).

## Развертывание Federated Query коннектора в существующем {{ ydb-short-name }} кластере {#existing-cluster}

Предполагается, что кластер был развернут по инструкции [первоначального развёртывания](./initial-deployment.md).

Шаги, по добавлению fq-connector-go в такой кластер:

1. Перейдите в ту же директорию, которая использовалась для [первоначального развёртывания](./initial-deployment.md) кластера.
2. Добавьте дополнительные настройки в разделе `vars` инвентори-файла `files/50-inventory.yaml`:
    1. Выберите один из доступных вариантов развёртывания исполняемых файлов fq-connector-go:
        1. `ydb_fq_connector_archive`: локальный путь к архиву с дистрибутивом fq-connector-go, [загруженному](https://github.com/ydb-platform/fq-connector-go/releases) или подготовленному заранее.
        2. `ydb_fq_connector_binary`: локальный путь к исполняемому файлу fq-connector-go, [загруженному](https://github.com/ydb-platform/fq-connector-go/releases) или подготовленному заранее.
    2. Составьте конфигурационный файл fq-connector-go ([документация по конфигурации](../../deploy/manual/connector.md#fq-connector-go-config)):
        1. `ydb_fq_connector_config`: укажите локальный путь до конфигурационного файла fq-connector-go.
    3. `ydb_fq_connector_dir`: укажите директорию, в которую fq-connector-go будет установлен на сервере.
4. Отредактируйте конфигурационный файл {{ ydb-short-name }} `files/config.yaml`:
    1. в секции `query_service_config` добавьте подсекцию `generic` по образцу:
      ```yaml
        query_service_config:
            generic:
                connector:
                    endpoint:
                        host: localhost          # имя хоста, где развернут коннектор
                        port: 19102              # номер порта для слушающего сокета коннектора
                    use_ssl: false                      
                default_settings:
                    - name: DateTimeFormat
                      value: string
                    - name: UsePredicatePushdown
                      value: "true"
      ```
    2. в секции `feature_flags` включите следующие флаги:
      ```yaml
      feature_flags:
          enable_external_data_sources: true
        enable_script_execution_operations: true
    ```
5. Если необходимо, [включите функцию multislot развертывания](#multislot) (функция работоспособна только для {{ ydb-short-name }} версии 24.3.3 или старше).
6. Установите fq-connector-go командой `ansible-playbook ydb_platform.ydb.install_connector`.
7. Обновите конфигурацию {{ ydb-short-name }} по [инструкции](./update-config.md).

В результате в кластере будет развернут fq-connector-go, что позволит работать с [поддерживаемыми внешними источниками данных](../../concepts/federated_query/architecture.md#supported-datasources). Инструкции, как подключиться к внешнему источнику, можно найти [по этой ссылке](../../concepts/federated_query/index.md).

## Multislot развертывание {#multislot}

По умолчанию, плейбуки разворачивают по одному экземпляру fq-connector-go на каждом хосте с динамическими нодами. Благодаря этому, для каждой динамической ноды существует экземпляр коннектора, доступный по адресу `localhost`.

В простейшем случае, {{ ydb-short-name }} кластер с коннектором описывается схемой:

![Инсталляция {{ ydb-short-name }} FQ](../../deploy/manual/_images/ydb_fq_onprem.png "Инсталляция {{ ydb-short-name }} FQ" =1024x)

Однако [первоначальное развертывание в Ansible](./initial-deployment.md) поддерживает установку нескольких экземпляров динамических нод на одном хосте. При этом, схема хоста выглядит так:

![Хост {{ ydb-short-name }} с несколькими диннодами и одним коннектором](./_assets/ansible/multislot-dynnode-singleslot-fq-connector-host.png "Хост {{ ydb-short-name }} с несколькими диннодами и одним коннектором" =512x)

В такой схеме коннектор может стать узким местом. Чтобы решить эту проблему, в плейбуках предусмотрен режим multislot развертывания. В этом режиме для каждого экземпляра динамической ноды запускается свой экземпляр fq-connector-go:

![Хост {{ ydb-short-name }} с несколькими диннодами и несколькими коннекторами](./_assets/ansible/multislot-dynnode-multislot-fq-connector-host.png "Хост {{ ydb-short-name }} с несколькими диннодами и несколькими коннекторами" =512x)

Номера портов, которые используют экземпляры коннектора, вычисляются по формуле: `номер_порта_из_конфигурационного_файла_коннектора + ydb_dynnodes[*].offset`.

Динамические ноды {{ ydb-short-name }} могут использовать коннектор в таком режиме, начиная с версии 24.3.3. Для этого в конфигурационном файле {{ ydb-short-name }} предусмотрена специальная опция `query_service_config.generic.connector.offset_from_ic_port`:
  ```yaml
  query_service_config:
      generic:
          connector:
              endpoint:
                  host: localhost      # имя хоста, где развернут коннектор
              offset_from_ic_port: 100 # вычислить номер порта коннектора по формуле: номер_ic_порта_динноды + 100
              use_ssl: false
  ```

При использовании `query_service_config.generic.connector.offset_from_ic_port`, значение `query_service_config.generic.connector.endpoint.port` игнорируется.

{% note info %}

На момент написания этой инструкции, версия {{ ydb-short-name }} 24.3.3 была недоступна для [загрузки в предсобранном виде](../../downloads/index.md#ydb-server). Вы можете прибегнуть к установке {{ ydb-short-name }} из исходного кода. Для этого в разделе `vars` инвентори-файла `files/50-inventory.yaml`:
  * Удалите/закомментируйте строки `ydb_version`, `ydb_archive`, `ydbd_binary`, `ydb_cli_binary`
  * Используйте `ydb_git_version: 24.3.3`

{% endnote %}

Шаги, для включения функции multislot развертывания:

1. Внесите дополнительные изменения в разделе `vars` инвентори-файла `files/50-inventory.yaml`:
    1. `ydb_fq_connector_multislot`: установите в `true`
2. Отредактируйте конфигурационный файл {{ ydb-short-name }} `files/config.yaml`:
    1. отредактируйте `query_service_config.generic.connector` по образцу:
    ```yaml
    query_service_config:
        generic:
            connector:
                endpoint:
                    host: localhost      # имя хоста, где развернут коннектор
                offset_from_ic_port: 100 # вычислить номер порта коннектора по формуле: номер_ic_порта_динноды + 100
                use_ssl: false
    ```
3. Продолжите установку