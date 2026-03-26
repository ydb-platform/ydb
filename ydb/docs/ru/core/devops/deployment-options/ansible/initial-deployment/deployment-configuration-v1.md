# Развёртывание кластера с использованием конфигурации V1

## Подготовьте окружение {# deployment-preparation}

Перед развёртыванием системы обязательно выполните подготовительные действия. Ознакомьтесь с документом [{#T}](deployment-preparation.md).

## Создайте директорию для работы {#prepare-directory}

```bash
mkdir deployment
cd deployment
mkdir inventory
mkdir files
```

## Создайте конфигурационный файл Ansible {# ansible-creat-config}

Создайте `ansible.cfg` с конфигурацией Ansible, подходящей для целевой среды развёртывания. Подробности в [справочнике по конфигурации Ansible](https://docs.ansible.com/ansible/latest/reference_appendices/config.html). Дальнейшее руководство предполагает, что поддиректория `./inventory` рабочей директории настроена для использования файлов инвентаризации.

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
vault_password_file = ./ansible_vault_password_file

[ssh_connection]
retries = 5
timeout = 60
ssh_args = -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -o ControlMaster=auto -o ControlPersist=60s -o ControlPath=/tmp/ssh-%h-%p-%r -o ServerAliveCountMax=3 -o ServerAliveInterval=10
```

{% endcut %}

## Создайте основной файл инвентаризации {#inventory-create}

Создайте файл `inventory/50-inventory.yaml` и заполните его в зависимости от выбранной топологии (подробнее: [выбор топологии](./deployment-preparation.md#topology-select)). Примеры для каждой поддерживаемой топологии приведены ниже во вкладках — выберите подходящий.

{% list tabs %}

- mirror-3-dc-3nodes

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
          ydb_version: "версия_системы"

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
          ydb_pool_kind: ssd
          ydb_database_groups: 8
          ydb_cores_dynamic: 8
          ydb_dynnodes:
            - { instance: 'a', offset: 1 }
            - { instance: 'b', offset: 2 }
          ydb_brokers:
            - static-node-1.ydb-cluster.com
            - static-node-2.ydb-cluster.com
            - static-node-3.ydb-cluster.com
          
          # База данных
          ydb_user: root
          ydb_domain: Root
          ydb_dbname: database

          #Настройки авторизации
          ydb_enforce_user_token_requirement: true
          ydb_request_client_certificate: true
    ```

- mirror-3-dc-9-nodes

  ```yaml
  all:
    children:
      ydb:
        #Серверы
        hosts:
          static-node-1.ydb-cluster.com:
          static-node-2.ydb-cluster.com:
          static-node-3.ydb-cluster.com:
          static-node-4.ydb-cluster.com:
          static-node-5.ydb-cluster.com:
          static-node-6.ydb-cluster.com:
          static-node-7.ydb-cluster.com:
          static-node-8.ydb-cluster.com:
          static-node-9.ydb-cluster.com:

        vars:
          # Ansible
          ansible_user: имя_пользователя
          ansible_ssh_private_key_file: "/путь/к/вашему/id_rsa"

          # Система
          system_timezone: UTC
          system_ntp_servers: [time.cloudflare.com, time.google.com, ntp.ripe.net, pool.ntp.org]
          
          # Узлы
          ydb_config: "{{ ansible_config_file | dirname }}/files/config.yaml"
          ydb_version: "версия_системы"

          # Хранилище
          ydb_cores_static: 8
          ydb_disks:
            - name: /dev/vdb
              label: ydb_disk_1
          ydb_allow_format_drives: true
          ydb_skip_data_loss_confirmation_prompt: false
          ydb_pool_kind: ssd
          ydb_database_groups: 8
          ydb_cores_dynamic: 8
          ydb_dynnodes:
            - { instance: 'a', offset: 1 }
            - { instance: 'b', offset: 2 }
          ydb_brokers:
            - static-node-1.ydb-cluster.com
            - static-node-2.ydb-cluster.com
            - static-node-3.ydb-cluster.com
          
          # База данных
          ydb_user: root
          ydb_domain: Root
          ydb_dbname: database

          #Настройки авторизации
          ydb_enforce_user_token_requirement: true
          ydb_request_client_certificate: true
    ```

- block-4-2

  ```yaml
  all:
    children:
      ydb:
        #Серверы
        hosts:
          static-node-1.ydb-cluster.com:
          static-node-2.ydb-cluster.com:
          static-node-3.ydb-cluster.com:
          static-node-4.ydb-cluster.com:
          static-node-5.ydb-cluster.com:
          static-node-6.ydb-cluster.com:
          static-node-7.ydb-cluster.com:
          static-node-8.ydb-cluster.com:

        vars:
          # Ansible
          ansible_user: имя_пользователя
          ansible_ssh_private_key_file: "/путь/к/вашему/id_rsa"

          # Система
          system_timezone: UTC
          system_ntp_servers: [time.cloudflare.com, time.google.com, ntp.ripe.net, pool.ntp.org]
          
          # Узлы
          ydb_config: "{{ ansible_config_file | dirname }}/files/config.yaml"
          ydb_version: "версия_системы"

          # Хранилище
          ydb_cores_static: 8
          ydb_disks:
            - name: /dev/vdb
              label: ydb_disk_1
          ydb_allow_format_drives: true
          ydb_skip_data_loss_confirmation_prompt: false
          ydb_pool_kind: ssd
          ydb_database_groups: 7
          ydb_cores_dynamic: 8
          ydb_dynnodes:
            - { instance: 'a', offset: 1 }
            - { instance: 'b', offset: 2 }
          ydb_brokers:
            - static-node-1.ydb-cluster.com
            - static-node-2.ydb-cluster.com
            - static-node-3.ydb-cluster.com
          
          # База данных
          ydb_user: root
          ydb_domain: Root
          ydb_dbname: database

          #Настройки авторизации
          ydb_enforce_user_token_requirement: true
          ydb_request_client_certificate: true
    ```

{% endlist %}

{% include notitle [_](./_includes/required-settings.md) %}

{% include notitle [_](./_includes/recommended-settings.md) %}

{% cut "Дополнительные настройки" %}

{% include notitle [_](./_includes/additional-settings.md) %}

{% endcut %}

## Измените пароль пользователя root {#change-password}

Создайте файл `ansible_vault_password_file` с содержимым:

```text
password
```

Этот файл содержит пароль, который Ansible будет использовать для автоматического шифрования и расшифровки конфиденциальных данных, например, файлов с паролями пользователей. Благодаря этому пароли не хранятся в открытом виде в репозитории. Подробнее о работе механизма Ansible Vault можно прочитать в [официальной документации](https://docs.ansible.com/ansible/latest/vault_guide/index.html).

Далее необходимо установить пароль для начального пользователя, указанного в настройке `ydb_user` (по умолчанию `root`). Этот пользователь изначально будет иметь полные права доступа в кластере, но при необходимости это можно изменить позже. Создайте `inventory/99-inventory-vault.yaml` со следующим содержимым (замените `<password>` на фактический пароль):

```yaml
all:
  children:
    ydb:
      vars:
        ydb_password: <password>
```

Зашифруйте этот файл с помощью команды `ansible-vault encrypt inventory/99-inventory-vault.yaml`.

## Подготовьте конфигурационный файл {{ ydb-short-name }} {#ydb-config-prepare}

Создайте файл `files/config.yaml` и заполните его в зависимости от выбранной топологии (подробнее: [выбор топологии](./deployment-preparation.md#topology-select)). Примеры для каждой поддерживаемой топологии приведены ниже во вкладках — выберите подходящий.

{% list tabs %}

- mirror-3-dc-3nodes

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

- mirror-3-dc-9-nodes

  ```yaml
  storage_config_generation: 0
  static_erasure: mirror-3-dc
  host_configs:
  - drive:
    - path: /dev/disk/by-partlabel/ydb_disk_1
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
      data_center: 'zone-a'
      rack: '2'
  - host: static-node-3.ydb-cluster.com
    host_config_id: 1
    walle_location:
      body: 3
      data_center: 'zone-a'
      rack: '3'
  - host: static-node-4.ydb-cluster.com
    host_config_id: 1
    walle_location:
      body: 4
      data_center: 'zone-b'
      rack: '4'
  - host: static-node-5.ydb-cluster.com
    host_config_id: 1
    walle_location:
      body: 5
      data_center: 'zone-b'
      rack: '5'
  - host: static-node-6.ydb-cluster.com
    host_config_id: 1
    walle_location:
      body: 6
      data_center: 'zone-b'
      rack: '6'
  - host: static-node-7.ydb-cluster.com
    host_config_id: 1
    walle_location:
      body: 7
      data_center: 'zone-d'
      rack: '7'
  - host: static-node-8.ydb-cluster.com
    host_config_id: 1
    walle_location:
      body: 8
      data_center: 'zone-d'
      rack: '8'
  - host: static-node-9.ydb-cluster.com
    host_config_id: 1
    walle_location:
      body: 9
      data_center: 'zone-d'
      rack: '9'
  domains_config:
    domain:
    - name: Root
      storage_pool_types:
      - kind: ssd
        pool_config:
          box_id: 1
          erasure_species: mirror-3-dc
          kind: ssd
          pdisk_filter:
          - property:
            - type: SSD
          vdisk_kind: Default
    state_storage:
    - ring:
        node: [1, 2, 3, 4, 5, 6, 7, 8, 9]
        nto_select: 9
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
            - node_id: static-node-2.ydb-cluster.com
              pdisk_category: SSD
              path: /dev/disk/by-partlabel/ydb_disk_1
          - vdisk_locations:
            - node_id: static-node-3.ydb-cluster.com
              pdisk_category: SSD
              path: /dev/disk/by-partlabel/ydb_disk_1
        - fail_domains:
          - vdisk_locations:
            - node_id: static-node-4.ydb-cluster.com
              pdisk_category: SSD
              path: /dev/disk/by-partlabel/ydb_disk_1
          - vdisk_locations:
            - node_id: static-node-5.ydb-cluster.com
              pdisk_category: SSD
              path: /dev/disk/by-partlabel/ydb_disk_1
          - vdisk_locations:
            - node_id: static-node-6.ydb-cluster.com
              pdisk_category: SSD
              path: /dev/disk/by-partlabel/ydb_disk_1
        - fail_domains:
          - vdisk_locations:
            - node_id: static-node-7.ydb-cluster.com
              pdisk_category: SSD
              path: /dev/disk/by-partlabel/ydb_disk_1
          - vdisk_locations:
            - node_id: static-node-8.ydb-cluster.com
              pdisk_category: SSD
              path: /dev/disk/by-partlabel/ydb_disk_1
          - vdisk_locations:
            - node_id: static-node-9.ydb-cluster.com
              pdisk_category: SSD
              path: /dev/disk/by-partlabel/ydb_disk_1
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
  auth_config:
    path_to_root_ca: /opt/ydb/certs/ca.crt    
  client_certificate_authorization:
    request_client_certificate: true
    client_certificate_definitions:
        - member_groups: ["databaseNodes@cert"]
          subject_terms:
          - short_name: "O"
            values: ["YDB"]
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

- block-4-2

  ```yaml
  storage_config_generation: 0
  static_erasure: block-4-2
  host_configs:
  - drive:
    - path: /dev/disk/by-partlabel/ydb_disk_1
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
      data_center: 'zone-a'
      rack: '2'
  - host: static-node-3.ydb-cluster.com
    host_config_id: 1
    walle_location:
      body: 3
      data_center: 'zone-a'
      rack: '3'
  - host: static-node-4.ydb-cluster.com
    host_config_id: 1
    walle_location:
      body: 4
      data_center: 'zone-a'
      rack: '4'
  - host: static-node-5.ydb-cluster.com
    host_config_id: 1
    walle_location:
      body: 5
      data_center: 'zone-a'
      rack: '5'
  - host: static-node-6.ydb-cluster.com
    host_config_id: 1
    walle_location:
      body: 6
      data_center: 'zone-a'
      rack: '6'
  - host: static-node-7.ydb-cluster.com
    host_config_id: 1
    walle_location:
      body: 7
      data_center: 'zone-a'
      rack: '7'
  - host: static-node-8.ydb-cluster.com
    host_config_id: 1
    walle_location:
      body: 8
      data_center: 'zone-a'
      rack: '8'
  domains_config:
    domain:
    - name: Root
      storage_pool_types:
      - kind: ssd
        pool_config:
          box_id: 1
          erasure_species: block-4-2
          kind: ssd
          pdisk_filter:
          - property:
            - type: SSD
          vdisk_kind: Default
    state_storage:
    - ring:
        node:
          - 1
          - 2
          - 3
          - 4
          - 5
          - 6
          - 7
          - 8
        nto_select: 8
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
      - erasure_species: block-4-2
        rings:
        - fail_domains:
          - vdisk_locations:
            - node_id: static-node-1.ydb-cluster.com
              pdisk_category: SSD
              path: /dev/disk/by-partlabel/ydb_disk_1
          - vdisk_locations:
            - node_id: static-node-2.ydb-cluster.com
              pdisk_category: SSD
              path: /dev/disk/by-partlabel/ydb_disk_1
          - vdisk_locations:
            - node_id: static-node-3.ydb-cluster.com
              pdisk_category: SSD
              path: /dev/disk/by-partlabel/ydb_disk_1
          - vdisk_locations:
            - node_id: static-node-4.ydb-cluster.com
              pdisk_category: SSD
              path: /dev/disk/by-partlabel/ydb_disk_1
          - vdisk_locations:
            - node_id: static-node-5.ydb-cluster.com
              pdisk_category: SSD
              path: /dev/disk/by-partlabel/ydb_disk_1
          - vdisk_locations:
            - node_id: static-node-6.ydb-cluster.com
              pdisk_category: SSD
              path: /dev/disk/by-partlabel/ydb_disk_1
          - vdisk_locations:
            - node_id: static-node-7.ydb-cluster.com
              pdisk_category: SSD
              path: /dev/disk/by-partlabel/ydb_disk_1
          - vdisk_locations:
            - node_id: static-node-8.ydb-cluster.com
              pdisk_category: SSD
              path: /dev/disk/by-partlabel/ydb_disk_1
  channel_profile_config:
    profile:
    - channel:
      - erasure_species: block-4-2
        pdisk_category: 1   # 0=ROT, 1=SSD, 2=NVME
        storage_pool_kind: ssd
      - erasure_species: block-4-2
        pdisk_category: 1
        storage_pool_kind: ssd
      - erasure_species: block-4-2
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

{% endlist %}

Для ускорения и упрощения первичного развёртывания {{ ydb-short-name }} конфигурационный файл уже содержит большинство настроек для установки кластера. Достаточно заменить стандартные хосты FQDN на актуальные в разделах `hosts` и `blob_storage_config`.

- Раздел `hosts`:

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

- Раздел `blob_storage_config`:

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

## Разверните кластер {{ ydb-short-name }} {# cluster-deployment}

После завершения всех описанных выше подготовительных действий фактическое первоначальное развёртывание кластера сводится к выполнению следующей команды из рабочей директории:

```bash
ansible-playbook ydb_platform.ydb.initial_setup
```

Вскоре после начала будет необходимо подтвердить полную очистку настроенных дисков. Затем завершение развёртывания может занять десятки минут в зависимости от окружения и настроек. Этот плейбук выполняет примерно те же шаги, которые описаны в инструкциях для [ручного развёртывания кластера {{ ydb-short-name }}](../../manual/initial-deployment/index.md).

### Проверьте состояние кластера {#cluster-state}

На последнем этапе плейбук выполнит несколько тестовых запросов с использованием настоящих временных таблиц для проверки корректной работы. При успешном выполнении для каждого сервера будут отображены статус ok, failed=0 и результаты тестовых запросов (3 и 6) при достаточно подробном выводе плейбука.

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

В результате выполнения плейбука `ydb_platform.ydb.initial_setup` будет создан кластер {{ ydb-short-name }}. Он будет содержать [домен](../../../../concepts/glossary.md#domain) с именем из настройки `ydb_domain` (по умолчанию `Root`), [базу данных](../../../../concepts/glossary.md#database) с именем из настройки `ydb_dbname` (по умолчанию `database`) и начального [пользователя](../../../../concepts/glossary.md#access-user) с именем из настройки `ydb_user` (по умолчанию `root`).

## Дополнительные шаги {# additional-steps}

Самый простой способ исследовать только что развёрнутый кластер — использовать [Embedded UI](../../../../reference/embedded-ui/index.md), работающий на порту 8765 каждого сервера. Если нет прямого доступа к порту из браузера, можно настроить SSH-туннелирование. Для этого выполните команду `ssh -L 8765:localhost:8765 -i <private-key> <user>@<any-ydb-server-hostname>` на локальной машине (при необходимости добавьте дополнительные опции). После успешного установления соединения можно перейти по URL [localhost:8765](http://localhost:8765) через браузер. Браузер может попросить принять исключение безопасности. Пример того, как это может выглядеть:

![ydb-web-ui](../../../../_assets/ydb-web-console.png)

После успешного создания кластера {{ ydb-short-name }} проверьте его состояние, используя следующую страницу Embedded UI: [http://localhost:8765/monitoring/cluster/tenants](http://localhost:8765/monitoring/cluster/tenants). Это может выглядеть так:

![ydb-cluster-check](../../../../_assets/ydb-cluster-check.png)

В этом разделе отображаются следующие параметры кластера {{ ydb-short-name }}, отражающие его состояние:

- `Tablets` — список запущенных [таблеток](../../../../concepts/glossary.md#tablet). Все индикаторы состояния таблеток должны быть зелёными.
- `Nodes` — количество и состояние узлов хранения и узлов базы данных, запущенных в кластере. Индикатор состояния узлов должен быть зелёным, а количество созданных и запущенных узлов должно быть равным. Например, 18/18 для кластера из девяти узлов с одним узлом базы данных на сервер.

Индикаторы `Load` (количество используемой RAM) и `Storage` (количество используемого дискового пространства) также должны быть зелёными.

Проверить состояние группы хранения можно в разделе `storage` — [http://localhost:8765/monitoring/cluster/storage](http://localhost:8765/monitoring/cluster/storage):

![ydb-storage-gr-check](../../../../_assets/ydb-storage-gr-check.png)

Индикаторы `VDisks` должны быть зелёными, а статус `state` (находится в подсказке при наведении на индикатор Vdisk) должен быть `Ok`. Больше об индикаторах состояния кластера и мониторинге можно прочитать в статье [{#T}](../../../../reference/embedded-ui/ydb-monitoring.md).

### Тестирование кластера {#testing}

Протестировать кластер можно с помощью встроенных нагрузочных тестов в {{ ydb-short-name }} CLI. Для этого [установите {{ ydb-short-name }} CLI](../../../../reference/ydb-cli/install.md) и создайте профиль с параметрами подключения, заменив заполнители:

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

- `config profile create` — эта команда используется для создания профиля подключения. Укажите имя профиля. Более подробную информацию о том, как создавать и изменять профили, можно найти в статье [{#T}](../../../../reference/ydb-cli/profile/create.md).
- `-e` — конечная точка, строка в формате `protocol://host:port`. Допускается указать FQDN любого узла кластера и опустить порт. По умолчанию используется порт 2135.
- `--ca-file` — путь к корневому сертификату для подключений к базе данных по `grpcs`.
- `--user` — пользователь для подключения к базе данных.
- `--password-file` — путь к файлу с паролем. Опустите это, чтобы ввести пароль вручную.

Проверить, создался ли профиль, можно с помощью команды `{{ ydb-cli }} config profile list`, которая отобразит список профилей. После создания профиля его нужно активировать командой `{{ ydb-cli }} config profile activate <profile-name>`. Чтобы убедиться, что профиль активирован, можно повторно выполнить команду `{{ ydb-cli }} config profile list` — активный профиль будет иметь отметку `(active)`.

Для выполнения [YQL](../../../../yql/reference/index.md) запроса можно использовать команду `{{ ydb-cli }} sql -s 'SELECT 1;'`, которая вернёт результат запроса `SELECT 1` в табличной форме в терминал. После проверки соединения можно создать тестовую таблицу командой:
`{{ ydb-cli }} workload kv init --init-upserts 1000 --cols 4`. Это создаст тестовую таблицу `kv_test`, состоящую из 4 столбцов и 1000 строк. Проверить, что таблица `kv_test` создалась и заполнилась тестовыми данными, можно с помощью команды `{{ ydb-cli }} sql -s 'select * from kv_test limit 10;'`.

В терминал будет выведена таблица из 10 строк. Теперь можно выполнять тестирование производительности кластера. В статье [{#T}](../../../../reference/ydb-cli/workload-kv.md) описаны несколько типов рабочих нагрузок (`upsert`, `insert`, `select`, `read-rows`, `mixed`) и параметры их выполнения. Пример выполнения тестовой рабочей нагрузки `upsert` с параметром для вывода времени выполнения `--print-timestamp` и стандартными параметрами выполнения: `{{ ydb-cli }} workload kv run upsert --print-timestamp`:

```text
Window Txs/Sec Retries Errors  p50(ms) p95(ms) p99(ms) pMax(ms)        Timestamp
1          727 0       0       11      27      71      116     2024-02-14T12:56:39Z
2          882 0       0       10      21      29      38      2024-02-14T12:56:40Z
3          848 0       0       10      22      30      105     2024-02-14T12:56:41Z
4          901 0       0       9       20      27      42      2024-02-14T12:56:42Z
5          879 0       0       10      22      31      59      2024-02-14T12:56:43Z
...
```

После завершения тестов таблицу `kv_test` можно удалить командой: `{{ ydb-cli }} workload kv clean`. Подробнее о параметрах создания тестовой таблицы и тестах можно прочитать в статье [{#T}](../../../../reference/ydb-cli/workload-kv.md).

## См. также

- [Дополнительные примеры конфигурации Ansible](https://github.com/ydb-platform/ydb-ansible-examples)
- [{#T}](../restart.md)
- [{#T}](../update-config.md)
- [{#T}](../update-executable.md)