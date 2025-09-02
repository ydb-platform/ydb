# Развёртывание {{ ydb-short-name }} кластера с помощью Ansible

<!-- markdownlint-disable blanks-around-fences -->

В этом руководстве описывается процесс развёртывания {{ ydb-short-name }} кластера на группе серверов с помощью [Ansible](https://www.ansible.com/). Это рекомендуемый подход для сред с физическими серверами или виртуальными машинами.

## Предварительные требования

### Настройка серверов

Для начала работы рекомендуется настроить 3 сервера с 3 дисками для пользовательских данных на каждом. Для обеспечения отказоустойчивости каждый сервер должен иметь как можно более независимую инфраструктуру: желательно располагать их в отдельных дата-центрах или зонах доступности, или хотя бы в разных серверных стойках.

Для масштабных развёртываний рекомендуется использовать не менее 9 серверов для высокодоступных кластеров (`mirror-3-dc`) или 8 серверов для кластеров в одном дата-центре (`block-4-2`). В этих случаях серверам достаточно иметь один диск для пользовательских данных каждый, но желательно иметь дополнительный небольшой диск для операционной системы. Подробнее о доступных в {{ ydb-short-name }} моделях избыточности можно узнать из статьи [{#T}](../../../concepts/topology.md). Во время эксплуатации кластер может быть [расширен](../../configuration-management/configuration-v2/cluster-expansion.md) без приостановки доступа пользователей к данных.

{% note info %}

Рекомендуемые требования к серверам:

* 16 CPU (рассчитывается исходя из утилизации 8 CPU узлом хранения и 8 CPU динамическим узлом).
* 16 GB RAM (рекомендуемый минимальный объем RAM).
* Дополнительные SSD-диски для данных, не менее 120 GB каждый.
* Доступ по SSH.
* Сетевая связность между машинами в кластере.
* ОС: Ubuntu 18+, Debian 9+.
* Доступ в интернет для обновления репозиториев и скачивания необходимых пакетов.

Подробнее см. [{#T}](../../concepts/system-requirements.md).

{% endnote %}

Если вы планируете использовать виртуальные машины у публичного облачного провайдера, рекомендуется следовать [{#T}](preparing-vms-with-terraform.md).

### Настройка программного обеспечения

Для работы с проектом на локальной (промежуточной или установочной) машине потребуется:

- Python 3 версии 3.10+
- Ansible core версии 2.15.2 или выше
- Рабочая директория на сервере с SSH-доступом ко всем серверам кластера

{% note tip %}

Рекомендуется хранить рабочую директорию и все файлы, созданные в ходе этого руководства, в репозитории [системы управления версиями](https://ru.wikipedia.org/wiki/Система_управления_версиями), например [Git](https://git-scm.com/). Если несколько DevOps-инженеров будут работать с развёртываемым кластером, они должны взаимодействовать в общем репозитории.

{% endnote %}

Если Ansible уже установлен, можно перейти к разделу [«Настройка проекта Ansible»](#ansible-project-setup). Если Ansible ещё не установлен, установите его одним из следующих способов:

{% cut "Установка Ansible глобально" %}

На примере Ubuntu 22.04 LTS:

* Обновите список пакетов apt командой `sudo apt-get update`.
* Обновите пакеты командой `sudo apt-get upgrade`.
* Установите пакет `software-properties-common` для управления источниками программного обеспечения вашего дистрибутива — `sudo apt install software-properties-common`.
* Добавьте новый PPA в apt — `sudo add-apt-repository --yes --update ppa:ansible/ansible`.
* Установите Ansible — `sudo apt-get install ansible-core` (обратите внимание, что установка просто `ansible` приведёт к неподходящей устаревшей версии).
* Проверьте версию Ansible core — `ansible --version`

Подробнее см. [руководство по установке Ansible](https://docs.ansible.com/ansible/latest/installation_guide/index.html) для получения дополнительной информации и других вариантов установки.

{% endcut %}

{% cut "Установка Ansible в виртуальное окружение Python" %}

* Обновите список пакетов apt — `sudo apt-get update`.
* Установите пакет `venv` для Python3 — `sudo apt-get install python3-venv`
* Создайте директорию, где будет создано виртуальное окружение и куда будут загружены плейбуки. Например, `mkdir venv-ansible`.
* Создайте виртуальное окружение Python — `python3 -m venv venv-ansible`.
* Активируйте виртуальное окружение — `source venv-ansible/bin/activate`. Все дальнейшие действия с Ansible выполняются внутри виртуального окружения. Выйти из него можно командой `deactivate`.
* Установите рекомендуемую версию Ansible с помощью команды `pip3 install -r requirements.txt`, находясь в корневой директории загруженного репозитория.
* Проверьте версию Ansible core — `ansible --version`

{% endcut %}

## Настройка проекта Ansible {#ansible-project-setup}

### Установка {{ ydb-short-name }} Ansible плейбуков

{% list tabs %}

- Через requirements.yaml

  ```bash
  $ cat <<EOF > requirements.yaml
  roles: []
  collections:
    - name: git+https://github.com/ydb-platform/ydb-ansible
      type: git
      version: main
  EOF
  $ ansible-galaxy install -r requirements.yaml
  ```

- Однократно

  ```bash
  $ ansible-galaxy collection install git+https://github.com/ydb-platform/ydb-ansible.git
  ```

{% endlist %}

### Настройка Ansible

Создайте `ansible.cfg` с конфигурацией Ansible, подходящей для вашего окружения. Подробности см. в [справочнике по конфигурации Ansible](https://docs.ansible.com/ansible/latest/reference_appendices/config.html). Дальнейшее руководство предполагает, что поддиректория `./inventory` рабочей директории настроена для использования файлов инвентаризации.

{% cut "Пример стартового ansible.cfg" %}

```ini
[defaults]
conditional_bare_variables = False
force_handlers = True
gathering = explicit
interpreter_python = /usr/bin/python3
inventory = ./inventory
pipelining = True
private_role_vars = True
timeout = 5
vault_password_file = ./ansible_vault_password_file
verbosity = 1
log_path = ./ydb.log

[ssh_connection]
retries = 5
timeout = 60
```

{% endcut %}

### Создание основного файла инвентаризации {#inventory-create}

Создайте файл `inventory/50-inventory.yaml`, используя один из шаблонов ниже в зависимости от выбранной [топологии {{ ydb-short-name }} кластера](../../../concepts/topology.md):

{% list tabs %}

- Три узла

  ```yaml
  all:
    children:
      ydb:
        # Серверы
        hosts:
          static-node-1.ydb-cluster.com:
            location:
              data_center: 'zone-a'
          static-node-2.ydb-cluster.com:
            location:
              data_center: 'zone-b'
          static-node-3.ydb-cluster.com:
            location:
              data_center: 'zone-c'
        vars:
          # Ansible
          ansible_user: ubuntu
          ansible_ssh_private_key_file: "~/ydb"

          # Система
          system_timezone: UTC
          system_ntp_servers: pool.ntp.org

          # Узлы
          ydb_version: "25.1.1"
          ydb_storage_node_cores: 8
          ydb_database_node_cores: 8

          # Хранилище
          ydb_database_storage_groups: 8
          ydb_disks:
            - name: /dev/vdb
              label: ydb_disk_1
            - name: /dev/vdc
              label: ydb_disk_2
            - name: /dev/vdd
              label: ydb_disk_3
          ydb_allow_format_drives: true # замените на false после первоначальной настройки

          # База данных
          ydb_user: root
          ydb_domain: Root
          ydb_database_name: database
          ydb_config:
            erasure: mirror-3-dc
            fail_domain_type: disk
            default_disk_type: SSD
            security_config:
              enforce_user_token_requirement: true
  ```

- Три датацентра

  ```yaml
  all:
    children:
      ydb:
        # Серверы
        hosts:
          static-node-1.ydb-cluster.com:
            location:
              data_center: 'zone-a'
              rack: 'rack-1'
          static-node-2.ydb-cluster.com:
            location:
              data_center: 'zone-a'
              rack: 'rack-2'
          static-node-3.ydb-cluster.com:
            location:
              data_center: 'zone-a'
              rack: 'rack-3'
          static-node-4.ydb-cluster.com:
            location:
              data_center: 'zone-b'
              rack: 'rack-4'
          static-node-5.ydb-cluster.com:
            location:
              data_center: 'zone-b'
              rack: 'rack-5'
          static-node-6.ydb-cluster.com:
            location:
              data_center: 'zone-b'
              rack: 'rack-6'
          static-node-7.ydb-cluster.com:
            location:
              data_center: 'zone-c'
              rack: 'rack-7'
          static-node-8.ydb-cluster.com:
            location:
              data_center: 'zone-c'
              rack: 'rack-8'
          static-node-9.ydb-cluster.com:
            location:
              data_center: 'zone-c'
              rack: 'rack-9'
        vars:
          # Ansible
          ansible_user: ubuntu
          ansible_ssh_private_key_file: "~/ydb"

          # Система
          system_timezone: UTC
          system_ntp_servers: pool.ntp.org

          # Узлы
          ydb_version: "25.1.1"
          ydb_storage_node_cores: 8
          ydb_database_node_cores: 8

          # Хранилище
          ydb_database_storage_groups: 8
          ydb_disks:
            - name: /dev/vdb
              label: ydb_disk_1
          ydb_allow_format_drives: true # замените на false после первоначальной настройки

          # База данных
          ydb_user: root
          ydb_domain: Root
          ydb_database_name: database
          ydb_config:
            erasure: mirror-3-dc
            default_disk_type: SSD
            security_config:
              enforce_user_token_requirement: true
  ```

- Один датацентр

  ```yaml
  all:
    children:
      ydb:
        # Серверы
        hosts:
          static-node-1.ydb-cluster.com:
            location:
              rack: 'rack-1'
          static-node-2.ydb-cluster.com:
            location:
              rack: 'rack-2'
          static-node-3.ydb-cluster.com:
            location:
              rack: 'rack-3'
          static-node-4.ydb-cluster.com:
            location:
              rack: 'rack-4'
          static-node-5.ydb-cluster.com:
            location:
              rack: 'rack-5'
          static-node-6.ydb-cluster.com:
            location:
              rack: 'rack-6'
          static-node-7.ydb-cluster.com:
            location:
              rack: 'rack-7'
          static-node-8.ydb-cluster.com:
            location:
              rack: 'rack-8'
        vars:
          # Ansible
          ansible_user: ubuntu
          ansible_ssh_private_key_file: "~/ydb"

          # Система
          system_timezone: UTC
          system_ntp_servers: pool.ntp.org

          # Узлы
          ydb_version: "25.1.1"
          ydb_storage_node_cores: 8
          ydb_database_node_cores: 8

          # Хранилище
          ydb_database_storage_groups: 8
          ydb_disks:
            - name: /dev/vdb
              label: ydb_disk_1
          ydb_allow_format_drives: true # замените на false после первоначальной настройки

          # База данных
          ydb_user: root
          ydb_domain: Root
          ydb_database_name: database
          ydb_config:
            erasure: block-4-2
            default_disk_type: SSD
            security_config:
              enforce_user_token_requirement: true
  ```

{% endlist %}

Обязательные настройки, которые нужно адаптировать под ваше окружение в выбранном шаблоне:

1. **Имена серверов.** Замените `static-node-*.ydb-cluster.com` в `all.children.ydb.hosts` на реальные [FQDN](https://ru.wikipedia.org/wiki/FQDN).
2. **Расположение серверов.** Имена в `data_center` и `rack` в `all.children.ydb.hosts.location` произвольные, но они должны совпадать между серверами, только если они действительно находятся в одном датацентре (или зоне доступности) и стойке соответственно.
3. **Пути к блочным устройствам в файловой системе** в `all.children.ydb.vars.ydb_disks`. Шаблон предполагает, что `/dev/vda` предназначен для операционной системы, а следующие диски, такие как `/dev/vdb`, — для слоя хранения {{ ydb-short-name }}. Метки дисков создаются плейбуками автоматически, и их имена могут быть произвольными.
4. **Настройки, связанные с Ansible** с префиксом `all.children.ydb.ansible_`, такие как имя пользователя и приватный ключ для использования с `ssh`. Добавьте дополнительные по мере необходимости, например `ansible_ssh_common_args`.

Рекомендуемые настройки для адаптации:

* `ydb_domain`. Это будет первый компонент пути для всех [объектов схемы](../../../concepts/glossary.md#scheme-object) в кластере. Например, вы можете поместить туда название своей компании, регион кластера и т.д.
* `ydb_database_name`. Это будет второй компонент пути для всех [объектов схемы](../../../concepts/glossary.md#scheme-object) в базе данных. Например, вы можете поместить туда название сценария использования или проекта.
* `default_disk_type`. Если вы используете диски [NVMe](https://ru.wikipedia.org/wiki/NVM_Express) или вращающиеся [HDD](https://ru.wikipedia.org/wiki/Жёсткий_диск), измените эту настройку на `NVME` или `ROT` соответственно.
* `ydb_config`:
  * Любые настройки {{ ydb-short-name }} можно изменить через это поле, подробнее см. [{#T}](../../../reference/configuration/index.md).
  * Плейбуки {{ ydb-short-name }} автоматически устанавливают некоторые настройки {{ ydb-short-name }} на основе инвентаря Ansible (например, `hosts` или настройки, связанные с [TLS](../../../reference/configuration/index.md)), если вы настроите их явно в `ydb_config`, это будет иметь приоритет.
  * Если вы предпочитаете хранить специфичные для {{ ydb-short-name }} настройки отдельно от инвентаря Ansible, замените всю эту настройку строкой, содержащей путь к файлу с отдельным файлом конфигурации {{ ydb-short-name }} в формате [YAML](https://ru.wikipedia.org/wiki/YAML).
* `ydb_storage_node_cores` и `ydb_database_node_cores`. Если ваш сервер имеет более 16 ядер CPU, увеличьте эти значения так, чтобы их сумма равнялась фактически доступному количеству ядер. Если у вас более 64 ядер на сервер, рассмотрите возможность запуска нескольких узлов базы данных на сервер, используя `ydb_database_nodes_per_server`. Стремитесь к $ydb\_storage\_node\_cores + ydb\_database\_nodes\_per\_server \times ydb\_database\_node\_cores = available\_cores$.

{% cut "Дополнительные настройки" %}

Существует несколько вариантов указания того, какие именно исполняемые файлы {{ ydb-short-name }} вы хотите использовать для кластера:

* `ydb_version`: автоматически загрузить один из [официальных релизов {{ ydb-short-name }}](../../../downloads/index.md#ydb-server) по номеру версии. Например, `23.4.11`.
* `ydb_git_version`: автоматически скомпилировать исполняемые файлы {{ ydb-short-name }} из исходного кода, загруженного из [официального репозитория GitHub](https://github.com/ydb-platform/ydb). Значение настройки — это имя ветки, тега или коммита. Например, `main`.
* `ydb_archive`: локальный путь файловой системы к архиву дистрибутива {{ ydb-short-name }}, [загруженному](../../../downloads/index.md#ydb-server) или иным образом подготовленному заранее.
* `ydbd_binary` и `ydb_cli_binary`: локальные пути файловой системы к исполняемым файлам сервера и клиента {{ ydb-short-name }}, [загруженным](../../../downloads/index.md#ydb-server) или иным образом подготовленным заранее.

Для использования [федеративных запросов](../../../concepts/federated_query/index.md) может потребоваться установка [коннектора](../../../concepts/federated_query/architecture.md#connectors). Плейбук может развернуть [fq-connector-go](../manual/federated-queries/connector-deployment.md#fq-connector-go) на хостах с динамическими узлами. Используйте следующие настройки:

* `ydb_install_fq_connector` — установите в `true` для установки коннектора.
* Выберите один из доступных вариантов развёртывания исполняемых файлов fq-connector-go:
  * `ydb_fq_connector_version`: автоматически загрузить один из [официальных релизов fq-connector-go](https://github.com/ydb-platform/fq-connector-go/releases) по номеру версии. Например, `v0.7.1`.
  * `ydb_fq_connector_git_version`: автоматически скомпилировать исполняемый файл fq-connector-go из исходного кода, загруженного из [официального репозитория GitHub](https://github.com/ydb-platform/fq-connector-go). Значение настройки — это имя ветки, тега или коммита. Например, `main`.
  * `ydb_fq_connector_archive`: локальный путь файловой системы к архиву дистрибутива fq-connector-go, [загруженному](https://github.com/ydb-platform/fq-connector-go/releases) или иным образом подготовленному заранее.
  * `ydb_fq_connector_binary`: локальные пути файловой системы к исполняемому файлу fq-connector-go, [загруженному](https://github.com/ydb-platform/fq-connector-go/releases) или иным образом подготовленному заранее.

* `ydb_tls_dir` — укажите локальный путь к папке с TLS-сертификатами, подготовленными заранее. Она должна содержать файл `ca.crt` и подкаталоги с именами, соответствующими именам хостов узлов, содержащие сертификаты для данного узла. Если не указано, самоподписанные TLS-сертификаты будут сгенерированы автоматически для всего кластера {{ ydb-short-name }}.
* `ydb_brokers` — перечислите FQDN узлов брокеров. Например:

  ```yaml
  ydb_brokers:
      - static-node-1.ydb-cluster.com
      - static-node-2.ydb-cluster.com
      - static-node-3.ydb-cluster.com
  ```

Оптимальное значение настройки `ydb_database_storage_groups` в разделе `vars` зависит от доступных дисков. Предполагая только одну базу данных в кластере, используйте следующую логику:

* Для промышленных развёртываний используйте диски ёмкостью более 800 ГБ с высокой производительностью IOPS, затем выберите значение для этой настройки на основе топологии кластера:
  * Для `block-4-2` установите `ydb_database_storage_groups` на 95% от общего количества дисков, округляя вниз.
  * Для `mirror-3-dc` установите `ydb_database_storage_groups` на 84% от общего количества дисков, округляя вниз.
* Для тестирования {{ ydb-short-name }} на небольших дисках установите `ydb_database_storage_groups` в 1 независимо от топологии кластера.

Значения переменных `system_timezone` и `system_ntp_servers` зависят от свойств инфраструктуры, на которой развёртывается кластер {{ ydb-short-name }}. По умолчанию `system_ntp_servers` включает набор NTP-серверов без учёта географического расположения инфраструктуры, на которой будет развёрнут кластер {{ ydb-short-name }}. Мы настоятельно рекомендуем использовать локальный NTP-сервер для on-premise инфраструктуры и следующие NTP-серверы для облачных провайдеров:

{% list tabs %}

- AWS

  * `system_timezone`: USA/<region_name>
  * `system_ntp_servers`: [169.254.169.123, time.aws.com] [Подробнее](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/set-time.html#configure-time-sync) о настройках NTP-серверов AWS.

- Azure

  * О том, как настраивается синхронизация времени на виртуальных машинах Azure, можно прочитать в [этой](https://learn.microsoft.com/en-us/azure/virtual-machines/linux/time-sync) статье.

- Alibaba

  * Специфика подключения к NTP-серверам в Alibaba описана в [этой статье](https://www.alibabacloud.com/help/en/ecs/user-guide/alibaba-cloud-ntp-server).

- Yandex Cloud

  * `system_timezone`: Europe/Moscow
  * `system_ntp_servers`: [0.ru.pool.ntp.org, 1.ru.pool.ntp.org, ntp0.NL.net, ntp2.vniiftri.ru, ntp.ix.ru, ntps1-1.cs.tu-berlin.de] [Подробнее](https://yandex.cloud/ru/docs/tutorials/infrastructure-management/ntp) о настройках NTP-серверов Yandex Cloud.

{% endlist %}

{% endcut %}

### Изменение пароля пользователя root {#change-password}

Далее вы можете установить пароль для начального пользователя, указанного в настройке `ydb_user` (по умолчанию `root`). Этот пользователь изначально будет иметь полные права доступа в кластере, но при необходимости это можно изменить позже. Создайте `inventory/99-inventory-vault.yaml` со следующим содержимым (замените `<password>` на фактический пароль):

```yaml
all:
  children:
    ydb:
      vars:
        ydb_password: <password>
```

Зашифруйте этот файл с помощью команды `ansible-vault encrypt inventory/99-inventory-vault.yaml`. Это потребует либо вручную ввести пароль шифрования (который может отличаться), либо настроить параметр Ansible `vault_password_file`. Подробнее о том, как это работает, см. в [документации Ansible Vault](https://docs.ansible.com/ansible/latest/vault_guide/index.html).

### Подготовка конфигурационного файла {{ ydb-short-name }} {#ydb-config-prepare}

## Развёртывание кластера {{ ydb-short-name }}

После завершения всех описанных выше подготовительных действий фактическое первоначальное развёртывание кластера сводится к выполнению следующей команды из рабочей директории:

```bash
ansible-playbook ydb_platform.ydb.initial_setup
```

Вскоре после начала вас попросят подтвердить полную очистку настроенных дисков. Затем завершение развёртывания может занять десятки минут в зависимости от окружения и настроек. Этот плейбук выполняет примерно те же шаги, что описаны в инструкциях для [ручного развёртывания кластера {{ ydb-short-name }}](../manual/initial-deployment.md).

### Проверка состояния кластера {#cluster-state}

На последнем этапе плейбук выполнит несколько тестовых запросов, используя настоящие временные таблицы, чтобы перепроверить, что всё действительно работает штатно. При успехе вы увидите статус `ok`, `failed=0` для каждого сервера и результаты этих тестовых запросов (3 и 6), если вывод плейбука установлен достаточно подробным.

{% cut "Пример вывода" %}

```txt
...

TASK [ydb_platform.ydb.ydbd_dynamic : run test queries] *******************************************************************************************************************************************************
ok: [static-node-1.ydb-cluster.com] => (item={'instance': 'a'}) => {"ansible_loop_var": "item", "changed": false, "item": {"instance": "a"}, "msg": "all test queries were successful, details: {\"count\":3,\"sum\":6}\n"}
ok: [static-node-1.ydb-cluster.com] => (item={'instance': 'b'}) => {"ansible_loop_var": "item", "changed": false, "item": {"instance": "b"}, "msg": "all test queries were successful, details: {\"count\":3,\"sum\":6}\n"}
ok: [static-node-2.ydb-cluster.com] => (item={'instance': 'a'}) => {"ansible_loop_var": "item", "changed": false, "item": {"instance": "a"}, "msg": "all test queries were successful, details: {\"count\":3,\"sum\":6}\n"}
ok: [static-node-2.ydb-cluster.com] => (item={'instance': 'b'}) => {"ansible_loop_var": "item", "changed": false, "item": {"instance": "b"}, "msg": "all test queries were successful, details: {\"count\":3,\"sum\":6}\n"}
ok: [static-node-3.ydb-cluster.com] => (item={'instance': 'a'}) => {"ansible_loop_var": "item", "changed": false, "item": {"instance": "a"}, "msg": "all test queries were successful, details: {\"count\":3,\"sum\":6}\n"}
ok: [static-node-3.ydb-cluster.com] => (item={'instance': 'b'}) => {"ansible_loop_var": "item", "changed": false, "item": {"instance": "b"}, "msg": "all test queries were successful, details: {\"count\":3,\"sum\":6}\n"}
PLAY RECAP ****************************************************************************************************************************************************************************************************
static-node-1.ydb-cluster.com : ok=167  changed=80   unreachable=0    failed=0    skipped=167  rescued=0    ignored=0
static-node-2.ydb-cluster.com : ok=136  changed=69   unreachable=0    failed=0    skipped=113  rescued=0    ignored=0
static-node-3.ydb-cluster.com : ok=136  changed=69   unreachable=0    failed=0    skipped=113  rescued=0    ignored=0
```

{% endcut %}

В результате выполнения плейбука `ydb_platform.ydb.initial_setup` будет создан кластер {{ ydb-short-name }}. Он будет содержать [домен](../../../concepts/glossary.md#domain) с именем из настройки `ydb_domain` (по умолчанию `Root`), [базу данных](../../../concepts/glossary.md#database) с именем из настройки `ydb_database_name` (по умолчанию `database`) и начального [пользователя](../../../concepts/glossary.md#access-user) с именем из настройки `ydb_user` (по умолчанию `root`).

## Дополнительные шаги

Самый простой способ исследовать только что развёрнутый кластер — использовать [Embedded UI](../../../reference/embedded-ui/index.md), работающий на порту 8765 каждого сервера. В вероятном случае, когда у вас нет прямого доступа к этому порту из браузера, вы можете настроить SSH-туннелирование. Для этого выполните команду `ssh -L 8765:localhost:8765 -i <private-key> <user>@<any-ydb-server-hostname>` на локальной машине (при необходимости добавьте дополнительные опции). После успешного установления соединения вы можете перейти по URL [localhost:8765](http://localhost:8765) через браузер. Браузер может попросить принять исключение безопасности. Пример того, как это может выглядеть:

![ydb-web-ui](../../../_assets/ydb-web-console.png)

После успешного создания кластера {{ ydb-short-name }} вы можете проверить его состояние, используя следующую страницу Embedded UI: [http://localhost:8765/monitoring/cluster/tenants](http://localhost:8765/monitoring/cluster/tenants). Это может выглядеть так:

![ydb-cluster-check](../../../_assets/ydb-cluster-check.png)

В этом разделе отображаются следующие параметры кластера {{ ydb-short-name }}, отражающие его состояние:

* `Tablets` — список запущенных [таблеток](../../../concepts/glossary.md#tablet). Все индикаторы состояния таблеток должны быть зелёными.
* `Nodes` — количество и состояние узлов хранения и узлов базы данных, запущенных в кластере. Индикатор состояния узлов должен быть зелёным, а количество созданных и запущенных узлов должно быть равным. Например, 18/18 для кластера из девяти узлов с одним узлом базы данных на сервер.

Индикаторы `Load` (количество используемой RAM) и `Storage` (количество используемого дискового пространства) также должны быть зелёными.

Проверить состояние группы хранения можно в разделе `storage` — [http://localhost:8765/monitoring/cluster/storage](http://localhost:8765/monitoring/cluster/storage):

![ydb-storage-gr-check](../../../_assets/ydb-storage-gr-check.png)

Индикаторы `VDisks` должны быть зелёными, а статус `state` (находится в подсказке при наведении на индикатор Vdisk) должен быть `Ok`. Больше об индикаторах состояния кластера и мониторинге можно прочитать в статье [{#T}](../../../reference/embedded-ui/ydb-monitoring.md).

### Тестирование кластера {#testing}

Протестировать кластер можно с помощью встроенных нагрузочных тестов в {{ ydb-short-name }} CLI. Для этого [установите {{ ydb-short-name }} CLI](../../../reference/ydb-cli/install.md) и создайте профиль с параметрами подключения, заменив заполнители:

```shell
{{ ydb-cli }} \
  config profile create <profile-name> \
  -d /<ydb-domain>/<ydb-database> \
  -e grpcs://<any-ydb-cluster-hostname>:2135 \
  --ca-file $(pwd)/files/TLS/certs/ca.crt \
  --user root \
  --password-file <path-to-a-file-with-password>
```

Параметры команды и их значения:

* `config profile create` — эта команда используется для создания профиля подключения. Вы указываете имя профиля. Более подробную информацию о том, как создавать и изменять профили, можно найти в статье [{#T}](../../../reference/ydb-cli/profile/create.md).
* `-e` — конечная точка, строка в формате `protocol://host:port`. Вы можете указать FQDN любого узла кластера и опустить порт. По умолчанию используется порт 2135.
* `--ca-file` — путь к корневому сертификату для подключений к базе данных по `grpcs`.
* `--user` — пользователь для подключения к базе данных.
* `--password-file` — путь к файлу с паролем. Опустите это, чтобы ввести пароль вручную.

Проверить, создался ли профиль, можно с помощью команды `{{ ydb-cli }} config profile list`, которая отобразит список профилей. После создания профиля его нужно активировать командой `{{ ydb-cli }} config profile activate <profile-name>`. Чтобы убедиться, что профиль активирован, можно повторно выполнить команду `ydb config profile list` — активный профиль будет иметь отметку `(active)`.

Для выполнения [YQL](../../../yql/reference/index.md) запроса можно использовать команду `{{ ydb-cli }} sql -s 'SELECT 1;'`, которая вернёт результат запроса `SELECT 1` в табличной форме в терминал. После проверки соединения можно создать тестовую таблицу командой:
`{{ ydb-cli }} workload kv init --init-upserts 1000 --cols 4`. Это создаст тестовую таблицу `kv_test`, состоящую из 4 столбцов и 1000 строк. Проверить, что таблица `kv_test` создалась и заполнилась тестовыми данными, можно с помощью команды `{{ ydb-cli }} sql -s 'select * from kv_test limit 10;'`.

В терминал будет выведена таблица из 10 строк. Теперь можно выполнять тестирование производительности кластера. В статье [{#T}](../../../reference/ydb-cli/workload-kv.md) описаны несколько типов рабочих нагрузок (`upsert`, `insert`, `select`, `read-rows`, `mixed`) и параметры их выполнения. Пример выполнения тестовой рабочей нагрузки `upsert` с параметром для вывода времени выполнения `--print-timestamp` и стандартными параметрами выполнения: `{{ ydb-cli }} workload kv run upsert --print-timestamp`:

```text
Window Txs/Sec Retries Errors  p50(ms) p95(ms) p99(ms) pMax(ms)        Timestamp
1          727 0       0       11      27      71      116     2024-02-14T12:56:39Z
2          882 0       0       10      21      29      38      2024-02-14T12:56:40Z
3          848 0       0       10      22      30      105     2024-02-14T12:56:41Z
4          901 0       0       9       20      27      42      2024-02-14T12:56:42Z
5          879 0       0       10      22      31      59      2024-02-14T12:56:43Z
...
```

После завершения тестов таблицу `kv_test` можно удалить командой: `{{ ydb-cli }} workload kv clean`. Подробнее о параметрах создания тестовой таблицы и тестах можно прочитать в статье [{#T}](../../../reference/ydb-cli/workload-kv.md).

## См. также

* [Дополнительные примеры конфигурации Ansible](https://github.com/ydb-platform/ydb-ansible-examples)
* [{#T}](restart.md)
* [{#T}](update-config.md)
* [{#T}](update-executable.md)
