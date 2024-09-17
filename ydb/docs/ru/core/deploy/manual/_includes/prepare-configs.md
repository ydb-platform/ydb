Подготовьте конфигурационный файл {{ ydb-short-name }}:

1. Скачайте пример конфига для соответствующей модели отказа вашего кластера:

    * [block-4-2](https://github.com/ydb-platform/ydb/blob/stable-23-3/ydb/deploy/yaml_config_examples/block-4-2.yaml) - для однодатацентрового кластера.
    * [mirror-3dc](https://github.com/ydb-platform/ydb/blob/stable-23-3/ydb/deploy/yaml_config_examples/mirror-3dc-9-nodes.yaml) - для cross-DC кластера из 9 нод.
    * [mirror-3dc-3nodes](https://github.com/ydb-platform/ydb/blob/stable-23-3//ydb/deploy/yaml_config_examples/mirror-3dc-3-nodes.yaml) - для cross-DC кластера из 3 нод.

1. В секции `host_configs` укажите все диски и их тип на каждой из нод кластера. Возможные варианты типов дисков:

    * ROT: rotational, HDD диски.
    * SSD: SSD или NVMe диски.

    ```yaml
    host_configs:
    - drive:
      - path: /dev/disk/by-partlabel/ydb_disk_ssd_01
        type: SSD
      host_config_id: 1
    ```

1. В секции `hosts` укажите FQDN всех нод, их конфигурацию и расположение по датацентрам (`data_center`) и стойкам (`rack`):

    ```yaml
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

    ```yaml
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

1. Регистрация динамического узла (опционально).

    Если на шаге включения аутентификации пользователя был включен флаг `security_config.enforce_user_token_requirement: true`, то следует добавить в конфигурацию информацию для возможности зарегистрировать динамический узел.
    По скольку динамический узел, в процессе регистрации, делает запрос к статическому узлу кластера, то он должен предоставить некоторую аутентификационную информацию, а статический узел должен уметь ее проверить.

    Так как динамический узел выступает в качестве некоторого виртуального пользователя, то в качестве аутентификационной информации он должен предоставить SSL сертификат. Проверив этот сертификат, статический узел допускает такой динамический узел к регистрации.

    Настройки gRPC соединения:

    ```yaml
    grpc_config:
      services: [legacy, discovery]
      ca: "/opt/ydb/certs/ca.crt"
      cert: "/opt/ydb/certs/node.crt"
      key: "/opt/ydb/certs/node.key"
    ```

    Добавление проверочной аутентификационной информации статического узла. Статический узел должен проверить совпадение значений в конфигурации со значениями поля `Subject` предоставленного сертификата.

    ```yaml
    client_certificate_authorization:
    request_client_certificate: true
    client_certificate_definitions:
      - member_groups: ["registration-node@cert"]
        subject_terms:
        - short_name: "C"
          values: ["RU"]
        - short_name: "ST"
          values: ["Moscow"]
        - short_name: "L"
          values: ["Moscow"]
        - short_name: "O"
          values: ["My Organization"]
        - short_name: "OU"
          values: ["My organization unit"]
        - short_name: "CN"
          values: ["node2.ydb.tech"]
          suffixes: [".ydb.tech"]
    ```

    Так как динамический узел может представить в качестве аутентификационной информации только сертификат, то в системе такой узел будет известен под именем соответствующим полю `Subject` предоставленного сертификата.
    `member_groups` - имена групп пользователей. В эти группы попадают те пользователи (динамические узлы), предоставившие сертификаты, у которых поле `Subject` совпадает со значениями из секции `subject_terms`.

    Далее требуется добавить в группу `register_dynamic_node_allowed_sids` имена групп пользователей и групп. которым разрешено выполнить операцию регистрации динамического узла.

    ```yaml
    security_config:
    register_dynamic_node_allowed_sids:
    - registration-node@cert
    - root@builtin
    ```
