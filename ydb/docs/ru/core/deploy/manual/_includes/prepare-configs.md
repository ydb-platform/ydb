Подготовьте конфигурационный файл {{ ydb-short-name }}:

1. Скачайте пример конфига для соответствующей модели отказа вашего кластера:

    * [block-4-2](https://github.com/ydb-platform/ydb/blob/stable-23-3/ydb/deploy/yaml_config_examples/block-4-2.yaml) - для однодатацентрового кластера.
    * [mirror-3dc](https://github.com/ydb-platform/ydb/blob/stable-23-3/ydb/deploy/yaml_config_examples/mirror-3dc-9-nodes.yaml) - для cross-DC кластера из 9 нод.
    * [mirror-3dc-3nodes](https://github.com/ydb-platform/ydb/blob/stable-23-3//ydb/deploy/yaml_config_examples/mirror-3dc-3-nodes.yaml) - для cross-DC кластера из 3 нод.

1. В секции `host_configs` укажите все диски и их тип на каждой из нод кластера. Возможные варианты типов дисков:
    * ROT: rotational, HDD диски.
    * SSD: SSD или NVMe диски.

    ```json
    host_configs:
    - drive:
      - path: /dev/disk/by-partlabel/ydb_disk_ssd_01
        type: SSD
      host_config_id: 1
    ```

1. В секции `hosts` укажите FQDN всех нод, их конфигурацию и расположение по датацентрам (`data_center`) и стойкам (`rack`):

    ```json
    hosts:
    - host: node1.ydb.tech
      host_config_id: 1
      walle_location:
        body: 1
        data_center: 'zone-a'
        rack: '1'
    - host: node2.ydb.tech
      host_config_id: 1
      walle_location:
        body: 2
        data_center: 'zone-b'
        rack: '1'
    - host: node3.ydb.tech
      host_config_id: 1
      walle_location:
        body: 3
        data_center: 'zone-c'
        rack: '1'
    ```

1. В секции `blob_storage_config` скорректируйте FQDN всех нод, используемых для размещения статической группы хранения:

    * для схемы `mirror-3-dc` необходимо указать FQDN для 9 нод;
    * для схемы `block-4-2` необходимо указать FQDN для 8 нод.

1. Включите аутентификацию пользователей (опционально).

    Если вы планируете использовать в кластере {{ ydb-short-name }} возможности аутентификации и разграничения доступа пользователей, добавьте в секцию `domains_config` следующие дополнительные параметры:

    ```json
    domains_config:
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
    ```
