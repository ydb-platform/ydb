# Развертывание {{ ydb-short-name }} кластера с коннектором для функции Federated Query с помощью Ansible

[Федеративные запросы](../../concepts/federated_query/index.md) - это способ получать информацию из [внешних источников данных](../../concepts/datamodel/external_data_source.md) без необходимости переноса данных этих источников непосредственно в {{ ydb-full-name }}. Для работы с большинством внешних источников данных необходимо использование [коннектора](../../concepts/federated_query/architecture.md#connectors) - отдельного процесса, в котором инкапсулируется логика взаимодействия YDB с внешним источником.

Эта инструкция описывает развертывание коннектора [fq-connector-go](../../deploy/manual/connector.md#fq-connector-go) в [новом](#new-cluster) или [существующем](#existing-cluster) кластере {{ ydb-short-name }} с помощью [Ansible](https://www.ansible.com)

## Развертывание нового кластера {{ ydb-short-name }} с функцией Federated Query и коннектором {#new-cluster}

Перед прочтением рекомендуется ознакомиться с [{#T}](./initial-deployment.md). Развертывание кластера {{ ydb-short-name }} с функцией Federated Query и коннектором [fq-connector-go](../../deploy/manual/connector.md#fq-connector-go) похоже на развертывание любого другого {{ ydb-short-name }} кластера.

Шаги развертывания:

1. По [инструкции](./initial-deployment.md) подготовьте сервера, Ansible окружение, загрузите [репозиторий с шаблонами конфигурации](https://github.com/ydb-platform/ydb-ansible-examples).
1. Выберите один из шаблонов: `3-nodes-mirror-3-dc-fq`, `8-nodes-block-4-2-fq` или `9-nodes-mirror-3-dc-fq`. Конфигурационные файлы в этих шаблонах адаптированы под использование функции Federated Query.
1. Выполните [шаги по подготовке шаблона конфигурации](./initial-deployment.md#erasure-setup), но пока **НЕ** запускайте команду `ansible-playbook ydb_platform.ydb.initial_setup`.
1. Внесите дополнительные изменения в разделе `vars` инвентори-файла `files/50-inventory.yaml`:
    1. Выберите один из доступных вариантов развёртывания исполняемых файлов fq-connector-go:
        1. `ydb_fq_connector_archive`: локальный путь к архиву с дистрибутивом fq-connector-go, [загруженному](https://github.com/ydb-platform/fq-connector-go/releases) или подготовленному заранее.
        1. `ydb_fq_connector_binary`: локальный путь к исполняемому файлу fq-connector-go, [загруженному](https://github.com/ydb-platform/fq-connector-go/releases) или подготовленному заранее.
    1. Если необходимо, отредактируйте конфигурационный файл fq-connector-go `files/fq_config.yaml` ([документация по конфигурации](../../deploy/manual/connector.md#fq-connector-go-config)):
        1. `ydb_fq_connector_config`: укажите локальный путь до конфигурационного файла fq-connector-go.
    1. `ydb_fq_connector_dir`: укажите директорию, в которую fq-connector-go будет установлен на сервере.
1. Если необходимо, отредактируйте конфигурационный файл {{ ydb-short-name }} `files/config.yaml`
1. Если необходимо, [включите функцию multislot развертывания](#multislot) (функция работоспособна только для {{ ydb-short-name }} версии 24.3.3 или старше).
1. Выполните команду `ansible-playbook ydb_platform.ydb.initial_setup`, находясь в директории клонированного шаблона.

В результате выполнения плейбука будет создан кластер {{ ydb-short-name }}, на котором развернута тестовая база данных – `database`, создан `root` пользователь с максимальными правами доступа и запущен Embedded UI на порту 8765. Для подключения к Embedded UI можно настроить SSH-туннелирование. Для этого на локальной машине выполните команду `ssh -L 8765:localhost:8765 -i <ssh private key> <user>@<first ydb static node ip>`. После успешного установления соединения можно перейти по URL [localhost:8765](http://localhost:8765):

![ydb-web-ui](../../_assets/ydb-web-console.png)

К кластеру применимы инструкции:

* [Мониторинг состояния кластера](./initial-deployment.md#troubleshooting)
* [Тестирование кластера](./initial-deployment.md#testing)

Также в кластере будет развернут fq-connector-go, что позволит работать с [поддерживаемыми внешними источниками данных](../../concepts/federated_query/architecture.md#supported-datasources). Примеры запросов для подключения к внешнему источнику можно найти в [инструкции](../../concepts/federated_query/index.md).

## Развертывание Federated Query коннектора в существующем {{ ydb-short-name }} кластере {#existing-cluster}

Предполагается, что кластер был развернут по инструкции [первоначального развёртывания](./initial-deployment.md).

Шаги, по добавлению fq-connector-go в такой кластер:

1. Перейдите в ту же директорию, которая использовалась для [первоначального развёртывания](./initial-deployment.md) кластера.
1. Добавьте дополнительные настройки в раздел `vars` инвентори-файла `files/50-inventory.yaml`:
    1. Выберите один из доступных вариантов развёртывания исполняемых файлов fq-connector-go:
        1. `ydb_fq_connector_archive`: локальный путь к архиву с дистрибутивом fq-connector-go, [загруженному](https://github.com/ydb-platform/fq-connector-go/releases) или подготовленному заранее.
        1. `ydb_fq_connector_binary`: локальный путь к исполняемому файлу fq-connector-go, [загруженному](https://github.com/ydb-platform/fq-connector-go/releases) или подготовленному заранее.
    1. Составьте конфигурационный файл fq-connector-go ([документация по конфигурации](../../deploy/manual/connector.md#fq-connector-go-config)):
        1. `ydb_fq_connector_config`: укажите локальный путь до конфигурационного файла fq-connector-go.
    1. `ydb_fq_connector_dir`: укажите директорию, в которую fq-connector-go будет установлен на сервере.
1. Отредактируйте конфигурационный файл {{ ydb-short-name }} `files/config.yaml` в соответствии с [инструкцией](../../deploy/manual/deploy-ydb-federated-query.md#guide)
1. Если необходимо, [включите функцию multislot развертывания](#multislot) (функция работоспособна только для {{ ydb-short-name }} версии 24.3.3 или старше).
1. Установите fq-connector-go командой `ansible-playbook ydb_platform.ydb.install_connector`.
1. Обновите конфигурацию {{ ydb-short-name }} по [инструкции](./update-config.md).

В результате в кластере будет развернут fq-connector-go, что позволит работать с [поддерживаемыми внешними источниками данных](../../concepts/federated_query/architecture.md#supported-datasources). Инструкции, как подключиться к внешнему источнику, можно найти [по этой ссылке](../../concepts/federated_query/index.md).

## Multislot развертывание {#multislot}

По умолчанию плейбуки разворачивают по одному экземпляру fq-connector-go на каждом хосте с динамическими нодами. Благодаря этому, для каждой динамической ноды существует экземпляр коннектора, доступный по адресу `localhost`.

В простейшем случае {{ ydb-short-name }} кластер с коннектором описывается схемой:

![Инсталляция {{ ydb-short-name }} FQ](../../deploy/manual/_images/ydb_fq_onprem.png "Инсталляция {{ ydb-short-name }} FQ" =1024x)

Однако [первоначальное развертывание в Ansible](./initial-deployment.md) поддерживает установку нескольких экземпляров динамических нод на одном хосте. При этом схема хоста выглядит так:

![Хост {{ ydb-short-name }} с несколькими диннодами и одним коннектором](./_assets/ansible/multislot-dynnode-singleslot-fq-connector-host.png "Хост {{ ydb-short-name }} с несколькими диннодами и одним коннектором" =512x)

В такой схеме коннектор может стать узким местом. Чтобы решить эту проблему, в плейбуках предусмотрен режим multislot развертывания. В этом режиме для каждого экземпляра динамической ноды запускается свой экземпляр fq-connector-go:

![Хост {{ ydb-short-name }} с несколькими диннодами и несколькими коннекторами](./_assets/ansible/multislot-dynnode-multislot-fq-connector-host.png "Хост {{ ydb-short-name }} с несколькими диннодами и несколькими коннекторами" =512x)

Номера портов, которые используют экземпляры коннектора, вычисляются по формуле: `номер_порта_из_конфигурационного_файла_коннектора + ydb_dynnodes[*].offset`.

Динамические ноды {{ ydb-short-name }} могут использовать коннектор в таком режиме, начиная с версии 24.3.3. Для этого в конфигурационном файле {{ ydb-short-name }} предусмотрена специальная опция `query_service_config.generic.connector.offset_from_ic_port`.

При использовании `query_service_config.generic.connector.offset_from_ic_port`, значение `query_service_config.generic.connector.endpoint.port` игнорируется. Вместо этого, номер порта коннектора определяется по формуле: `номер_порта_interconnect_динноды + query_service_config.generic.connector.offset_from_ic_port`

{% note info %}

Номер порта interconnect динноды определить по формуле: `19002 + ydb_dynnodes[*].offset`.

{% endnote %}

{% note info %}

Значение `query_service_config.generic.connector.offset_from_ic_port` можно определить по формуле: `номер_порта_из_конфигурационного_файла_коннектора - 19002`.

{% endnote %}

{% note info %}

На момент написания этой инструкции версия {{ ydb-short-name }} 24.3.3 была недоступна для [загрузки в предсобранном виде](../../downloads/index.md#ydb-server). Вы можете прибегнуть к установке {{ ydb-short-name }} из исходного кода. Для этого в разделе `vars` инвентори-файла `files/50-inventory.yaml`:
  * Удалите/закомментируйте строки `ydb_version`, `ydb_archive`, `ydbd_binary`, `ydb_cli_binary`
  * Используйте `ydb_git_version: 24.3.3`

{% endnote %}

Шаги, для включения функции multislot развертывания:

1. Внесите дополнительные изменения в раздел `vars` инвентори-файла `files/50-inventory.yaml`:
    1. `ydb_fq_connector_multislot`: установите в `true`
1. Отредактируйте конфигурационный файл {{ ydb-short-name }} `files/config.yaml`:
    1. отредактируйте `query_service_config.generic.connector` по образцу:
    ```yaml
    query_service_config:
        generic:
            connector:
                endpoint:
                    host: localhost      # имя хоста, где развернут коннектор
                offset_from_ic_port: 100 # если в конфигурационном файле коннектора указан порт 19102 (19102 - 19002 = 100)
                use_ssl: false
    ```
1. Продолжите установку