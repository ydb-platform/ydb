Подготовьте конфигурационный файл {{ ydb-short-name }}:

1. Скачайте пример конфига для соответствующей модели отказа вашего кластера:

    * [block-4-2](https://github.com/ydb-platform/ydb/blob/main/ydb/deploy/yaml_config_examples/block-4-2.yaml) - для однодатацентрового кластера.
    * [mirror-3dc](https://github.com/ydb-platform/ydb/blob/main/ydb/deploy/yaml_config_examples/mirror-3dc-9-nodes.yaml) - для crossDC кластера из 9 нод.
    * [mirror-3dc](https://github.com/ydb-platform/ydb/blob/main/ydb/deploy/yaml_config_examples/mirror-3dc-3-nodes.yaml) - для crossDC кластера из 3 нод.

1. В секции host_configs укажите все диски и их тип на каждой из нод кластера. Возможные варианты типов дисков:

    ```text
    host_configs:
    - drive:
      - path: /dev/disk/by-partlabel/ydb_disk_ssd_01
        type: SSD
      host_config_id: 1
    ```

    * ROT (rotational) - HDD диски.
    * SSD - SSD или NVMe диски.

1. В секции `hosts` укажите FQDN всех нод, их конфигурацию и расположение по датацентрам (`data_center`) и стойкам (`rack`):

    ```text
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

    Более подробная информация по созданию конфигурации приведена в статье [Конфигурация кластера](../../configuration/config.md).