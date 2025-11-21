# Развёртывание {{ ydb-short-name }} кластера с помощью Ansible

<!-- markdownlint-disable blanks-around-fences -->
{% note warning %}

Данная инструкция предназначена только для развёртывания кластеров с [конфигурацией V1](../../configuration-management/configuration-v1/index.md). Развёртывание кластеров с [конфигурацией V2](../../configuration-management/configuration-v2/index.md) с помощью Ansible в настоящий момент находится в разработке.  

{% endnote %}

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
- Поддерживаются версии Ansible core начиная с 2.15.2 и до 2.18
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
* Установите Ansible — `sudo apt-get install ansible-core=2.16.3-0ubuntu2` (обратите внимание, что установка просто `ansible` приведёт к неподходящей устаревшей версии).
* Проверьте версию Ansible core — `ansible --version`

Подробнее см. [руководство по установке Ansible](https://docs.ansible.com/ansible/latest/installation_guide/index.html) для получения дополнительной информации и других вариантов установки.

{% endcut %}

## Создание директорий для работы {#prepare-directory}

```bash
mkdir deployment
cd deployment
mkdir inventory
mkdir files
```

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
  $ ansible-galaxy collection install git+https://github.com/ydb-platform/ydb-ansible.git,main
  ```

{% endlist %}

### Настройка Ansible

Создайте `ansible.cfg` с конфигурацией Ansible, подходящей для вашего окружения. Подробности см. в [справочнике по конфигурации Ansible](https://docs.ansible.com/ansible/latest/reference_appendices/config.html). Дальнейшее руководство предполагает, что поддиректория `./inventory` рабочей директории настроена для использования файлов инвентаризации.

{% cut "Пример стартового ansible.cfg" %}

{% note info %}

Использование параметра `StrictHostKeyChecking=no` в `ssh_args` повышает удобство автоматизации, но снижает уровень безопасности SSH-соединения (отключает проверку подлинности хоста). Для production-окружений рекомендуется не указывать этот аргумент и настроить доверенные ключи вручную. Используйте этот параметр только для тестовых и временных установок.

{% endnote %}

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
verbosity = 1
log_path = ./ydb.log

[ssh_connection]
retries = 5
timeout = 60
ssh_args = -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -o ControlMaster=auto -o ControlPersist=60s -o ControlPath=/tmp/ssh-%h-%p-%r -o ServerAliveCountMax=3 -o ServerAliveInterval=10
```

{% endcut %}

### Создание основного файла инвентаризации {#inventory-create}

Создайте файл `inventory/50-inventory.yaml` со следующим содержимым:

```yaml
  all:
  children:
    ydb:
    #Серверы
      hosts:
        static-node-1.ydb-cluster.com:
        static-node-2.ydb-cluster.com:
        static-node-3.ydb-cluster.com:

      vars:
        # Ansible
        ansible_user: имя_пользователя
        ansible_ssh_private_key_file: "/путь/к/вашему/id_rsa"

        # Система
        system_timezone: UTC
        system_ntp_servers: [time.cloudflare.com, time.google.com, ntp.ripe.net, pool.ntp.org]
        
        # Узлы
        ydb_config: "{{ ansible_config_file | dirname }}/files/config.yaml"
        ydb_version: "25.1.4.7"

        # Хранилище
        ydb_cores_static: 8
        ydb_disks:
          - name: /dev/vdb
            label: ydb_disk_1
          - name: /dev/vdc
            label: ydb_disk_2
          - name: /dev/vdd
            label: ydb_disk_3
        ydb_allow_format_drives: true
        ydb_skip_data_loss_confirmation_prompt: false
        ydb_enforce_user_token_requirement: true
        ydb_request_client_certificate: true
        ydb_pool_kind: ssd
        ydb_database_groups: 8
        ydb_cores_dynamic: 8
        ydb_dynnodes:
          - { instance: 'a', offset: 0 }
          - { instance: 'b', offset: 1 }
        ydb_brokers:
          - static-node-1.ydb-cluster.com
          - static-node-2.ydb-cluster.com
          - static-node-3.ydb-cluster.com
        
        # База данных
        ydb_user: root
        ydb_domain: Root
        ydb_dbname: database
  ```

Обязательные настройки, которые нужно адаптировать под ваше окружение в выбранном шаблоне:

1. **Имена серверов.** Замените `static-node-*.ydb-cluster.com` в `all.children.ydb.hosts` и `all.children. на реальные [FQDN](https://ru.wikipedia.org/wiki/FQDN).
2. **Настройка SSH доступа.** Укажите пользователя `ansible_user` и путь к приватному ключу `ansible_ssh_private_key_file`, которые Ansible будет использовать для подключения к вашим серверам.
3. **Пути к блочным устройствам в файловой системе** в `all.children.ydb.vars.ydb_disks`. Шаблон предполагает, что `/dev/vda` предназначен для операционной системы, а следующие диски, такие как `/dev/vdb`, — для слоя хранения {{ ydb-short-name }}. Метки дисков создаются плейбуками автоматически, и их имена могут быть произвольными.
4. **Версия системы**: В параметре `ydb_version` укажите номер версии {{ ydb-short-name }}, которую нужно установить. Список доступных версий вы найдёте на странице [загрузок](../../../downloads/ydb-open-source-database.md).

Рекомендуемые настройки для адаптации:

* `ydb_domain`. Это будет первый компонент пути для всех [объектов схемы](../../../concepts/glossary.md#scheme-object) в кластере. Например, вы можете поместить туда название своей компании, регион кластера и т.д.
* `ydb_database_name`. Это будет второй компонент пути для всех [объектов схемы](../../../concepts/glossary.md#scheme-object) в базе данных. Например, вы можете поместить туда название сценария использования или проекта.

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

Далее необходимо установить пароль для начального пользователя, указанного в настройке `ydb_user` (по умолчанию `root`). Этот пользователь изначально будет иметь полные права доступа в кластере, но при необходимости это можно изменить позже. Создайте `inventory/99-inventory-vault.yaml` со следующим содержимым (замените `<password>` на фактический пароль):

```yaml
all:
  children:
    ydb:
      vars:
        ydb_password: <password>
```

Зашифруйте этот файл с помощью команды `ansible-vault encrypt inventory/99-inventory-vault.yaml`. Это потребует либо вручную ввести пароль шифрования (который может отличаться), либо настроить параметр Ansible `vault_password_file`. Подробнее о том, как это работает, см. в [документации Ansible Vault](https://docs.ansible.com/ansible/latest/vault_guide/index.html).

### Подготовка конфигурационного файла {{ ydb-short-name }} {#ydb-config-prepare}

Создайте файл `files/config.yaml` со следующим содержимым:

```yaml
  storage_config_generation: 0
  static_erasure: mirror-3-dc
  host_configs:
  - drive:
    - path: /dev/disk/by-partlabel/ydb_disk_1
      type: SSD
    - path: /dev/disk/by-partlabel/ydb_disk_2
      type: SSD
    - path: /dev/disk/by-partlabel/ydb_disk_3
      type: SSD
    host_config_id: 1
  hosts:
  - host: static-node-1.ydb-cluster.com
    host_config_id: 1
    walle_location:
      body: 1
      data_center: 'zone-a'
      rack: '1'
  - host: static-node-2.ydb-cluster.com
    host_config_id: 1
    walle_location:
      body: 2
      data_center: 'zone-b'
      rack: '2'
  - host: static-node-3.ydb-cluster.com
    host_config_id: 1
    walle_location:
      body: 3
      data_center: 'zone-d'
      rack: '3'
  domains_config:
    domain:
    - name: Root
      storage_pool_types:
      - kind: ssd
        pool_config:
          box_id: 1
          erasure_species: mirror-3-dc
          kind: ssd
          geometry:
            realm_level_begin: 10
            realm_level_end: 20
            domain_level_begin: 10
            domain_level_end: 256
          pdisk_filter:
          - property:
            - type: SSD
          vdisk_kind: Default
    state_storage:
    - ring:
        node: [1, 2, 3]
        nto_select: 3
      ssid: 1
    security_config:
      enforce_user_token_requirement: true
      monitoring_allowed_sids:
      - "root"
      - "ADMINS"
      - "DATABASE-ADMINS"
      administration_allowed_sids:
      - "root"
      - "ADMINS"
      - "DATABASE-ADMINS"
      viewer_allowed_sids:
      - "root"
      - "ADMINS"
      - "DATABASE-ADMINS"
      register_dynamic_node_allowed_sids:
      - databaseNodes@cert
      - root@builtin
  blob_storage_config:
    service_set:
      groups:
      - erasure_species: mirror-3-dc
        rings:
        - fail_domains:
          - vdisk_locations:
            - node_id: static-node-1.ydb-cluster.com
              pdisk_category: SSD
              path: /dev/disk/by-partlabel/ydb_disk_1
          - vdisk_locations:
            - node_id: static-node-1.ydb-cluster.com
              pdisk_category: SSD
              path: /dev/disk/by-partlabel/ydb_disk_2
          - vdisk_locations:
            - node_id: static-node-1.ydb-cluster.com
              pdisk_category: SSD
              path: /dev/disk/by-partlabel/ydb_disk_3
        - fail_domains:
          - vdisk_locations:
            - node_id: static-node-2.ydb-cluster.com
              pdisk_category: SSD
              path: /dev/disk/by-partlabel/ydb_disk_1
          - vdisk_locations:
            - node_id: static-node-2.ydb-cluster.com
              pdisk_category: SSD
              path: /dev/disk/by-partlabel/ydb_disk_2
          - vdisk_locations:
            - node_id: static-node-2.ydb-cluster.com
              pdisk_category: SSD
              path: /dev/disk/by-partlabel/ydb_disk_3
        - fail_domains:
          - vdisk_locations:
            - node_id: static-node-3.ydb-cluster.com
              pdisk_category: SSD
              path: /dev/disk/by-partlabel/ydb_disk_1
          - vdisk_locations:
            - node_id: static-node-3.ydb-cluster.com
              pdisk_category: SSD
              path: /dev/disk/by-partlabel/ydb_disk_2
          - vdisk_locations:
            - node_id: static-node-3.ydb-cluster.com
              pdisk_category: SSD
              path: /dev/disk/by-partlabel/ydb_disk_3
  channel_profile_config:
    profile:
    - channel:
      - erasure_species: mirror-3-dc
        pdisk_category: 1   # 0=ROT, 1=SSD, 2=NVME
        storage_pool_kind: ssd
      - erasure_species: mirror-3-dc
        pdisk_category: 1
        storage_pool_kind: ssd
      - erasure_species: mirror-3-dc
        pdisk_category: 1
        storage_pool_kind: ssd
      profile_id: 0
  interconnect_config:
      start_tcp: true
      encryption_mode: OPTIONAL
      path_to_certificate_file: "/opt/ydb/certs/node.crt"
      path_to_private_key_file: "/opt/ydb/certs/node.key"
      path_to_ca_file: "/opt/ydb/certs/ca.crt"
  grpc_config:
      cert: "/opt/ydb/certs/node.crt"
      key: "/opt/ydb/certs/node.key"
      ca: "/opt/ydb/certs/ca.crt"
      services_enabled:
      - legacy
      - discovery
  auth_config:
    path_to_root_ca: /opt/ydb/certs/ca.crt    
  client_certificate_authorization:
    request_client_certificate: true
    client_certificate_definitions:
        - member_groups: ["databaseNodes@cert"]
          subject_terms:
          - short_name: "O"
            values: ["YDB"]
          - short_name: "CN"
            values: ["YDB CA"]
  query_service_config:
    generic:
      connector:
        endpoint:
          host: localhost
          port: 19102
        use_ssl: false
      default_settings:
        - name: DateTimeFormat
          value: string
        - name: UsePredicatePushdown
          value: "true"
  feature_flags:
    enable_external_data_sources: true
    enable_script_execution_operations: true
  ```

Для ускорения и упрощения первичного развёртывания {{ ydb-short-name }} конфигурационный файл уже содержит большинство настроек для установки кластера. Достаточно заменить стандартные хосты FQDN на актуальные в разделах `hosts` и `blob_storage_config`.

* Раздел `hosts`:

  ```yaml
  ...
  hosts:
    - host: static-node-1.ydb-cluster.com #FQDN ВМ
      host_config_id: 1
      walle_location:
        body: 1
        data_center: 'zone-a'
        rack: '1'
  ...
  ```

* Раздел `blob_storage_config`:

  ```yaml
  ...
  - fail_domains:
    - vdisk_locations:
      - node_id: static-node-1.ydb-cluster.com #FQDN ВМ
        pdisk_category: SSD
        path: /dev/disk/by-partlabel/ydb_disk_1
  ...
  ```

Остальные секции и настройки конфигурационного файла остаются без изменений.

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
