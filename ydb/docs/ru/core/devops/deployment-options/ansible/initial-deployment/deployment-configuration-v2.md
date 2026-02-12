# Развёртывание кластера с использованием конфигурации V2

{% note alert  %}

   Эта статья посвящена кластерам {{ ydb-short-name }}, в которых используется **конфигурация V2**. Данный способ конфигурирования пока является экспериментальным и доступен только для версий {{ ydb-short-name }} начиная с v25.1. Для использования в продакшене мы рекомендуем выбирать [конфигурацию V1](./deployment-configuration-v1.md) — она является основной и официально поддерживаемой для всех кластеров {{ ydb-short-name }}.

{% endnote %}

## Подготовьте окружение {# deployment-preparation}

Перед развёртыванием системы обязательно выполните подготовительные действия. Ознакомьтесь с документом [{#T}](deployment-preparation.md).

## Создайте директорию для работы {#prepare-directory}

```bash
mkdir deployment
cd deployment
mkdir -p inventory/group_vars/ydb
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
forks = 300
gathering = explicit
host_key_checking = False
interpreter_python = /usr/bin/python3
inventory = ./inventory
module_name = shell
pipelining = True
private_role_vars = True
retry_files_enabled = False
timeout = 5
vault_password_file = ./ansible_vault_password_file
verbosity = 1
log_path = ./ydb.log

[ssh_connection]
retries = 5
ssh_args = -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -o ControlMaster=auto -o ControlPersist=60s -o ControlPath=/tmp/ssh-%h-%p-%r -o ServerAliveCountMax=3 -o ServerAliveInterval=10 
```

{% endcut %}

## Создайте основной файл инвентаризации {#inventory-create}

Создайте файл `inventory/group_vars/ydb/all.yaml` и заполните его.

{% list tabs %}

- mirror-3-dc-3nodes

  ```yaml
  # Ansible
    ansible_user: имя_пользователя
    ansible_ssh_private_key_file: "/путь/к/вашему/id_rsa"
  
  # Система
    system_timezone: UTC
    system_ntp_servers: [time.cloudflare.com, time.google.com, ntp.ripe.net, pool.ntp.org]
  
  # База данных
    ydb_user: root
    ydb_dbname: db

  # Хранилище
    ydb_disks:
      - name: /dev/vdb
      label: ydb_disk_1
      - name: /dev/vdc
      label: ydb_disk_2
      - name: /dev/vdd
      label: ydb_disk_3
    ydb_pool_kind: ssd
    ydb_allow_format_drives: true
    ydb_skip_data_loss_confirmation_prompt: false
    ydbops_local: true
    ydb_cores_dynamic: 2
    ydb_dynnodes:
      - {"instance": "a", offset: 1}
    ydb_cores_static:  2
  
  # Настройки авторизации
    ydb_enforce_user_token_requirement: true

  # Узлы
    ydb_version: "версия_системы"
   ```

{% endlist %}

Обязательные настройки, которые нужно адаптировать под окружение в выбранном шаблоне:

1. **Настройка SSH доступа.** Укажите пользователя `ansible_user` и путь к приватному ключу `ansible_ssh_private_key_file`, которые Ansible будет использовать для подключения к вашим серверам.
2. **Пути к блочным устройствам в файловой системе.** В секции `ydb_disks` шаблон предполагает, что `/dev/vda` предназначен для операционной системы, а следующие диски, такие как `/dev/vdb`, — для слоя хранения {{ ydb-short-name }}. Метки дисков создаются плейбуками автоматически, и их имена могут быть произвольными.
3. **Версия системы.** В параметре `ydb_version` укажите номер версии {{ ydb-short-name }}, которую нужно установить. Список доступных версий вы найдёте на странице [загрузок](../../../../downloads/ydb-open-source-database.md).

{% include notitle [_](./_includes/recommended-settings.md) %}

{% cut "Дополнительные настройки" %}

{% include notitle [_](./_includes/additional-settings.md) %}

{% endcut %}

## Измените пароль пользователя root {#change-password}

Создайте файл `ansible_vault_password_file` с содержимым:

```bash
password
```

Этот файл содержит пароль, который Ansible будет использовать для автоматического шифрования и расшифровки конфиденциальных данных, например, файлов с паролями пользователей. Благодаря этому пароли не хранятся в открытом виде в репозитории. Подробнее о работе механизма Ansible Vault можно прочитать в [официальной документации](https://docs.ansible.com/ansible/latest/vault_guide/index.html).

Далее необходимо установить пароль для начального пользователя, указанного в настройке `ydb_user` (по умолчанию `root`). Этот пользователь изначально будет иметь полные права доступа в кластере, но при необходимости это можно изменить позже. Создайте `inventory/group_vars/ydb/vault.yaml` со следующим содержимым (замените `<password>` на фактический пароль):

```yaml
all:
  children:
    ydb:
      vars:
        ydb_password: <password>
```

Зашифруйте этот файл с помощью команды `ansible-vault encrypt inventory/group_vars/ydb/vault.yaml`.

## Подготовка конфигурационного файла {{ ydb-short-name }} {#ydb-config-prepare}

Создайте файл `files/config.yaml` и заполните его.

{% list tabs %}

- mirror-3-dc-3nodes

  ```yaml
  metadata:
    kind: MainConfig
    cluster: ""
    version: 0
  config:
    yaml_config_enabled: true
    erasure: mirror-3-dc
    fail_domain_type: disk
    self_management_config:
      enabled: true
    default_disk_type: SSD
  host_configs:
    - host_config_id: 1
    drive:
    - path: /dev/disk/by-partlabel/ydb_disk_ssd_01
      type: SSD
    - path: /dev/disk/by-partlabel/ydb_disk_ssd_02
      type: SSD
    - path: /dev/disk/by-partlabel/ydb_disk_ssd_03
      type: SSD
  hosts:
    - host: ydb-node-zone-a.local
      host_config_id: 1
      location:
        body: 1
        data_center: 'zone-a'
        rack: '1'
    - host: ydb-node-zone-b.local
      host_config_id: 1
      location:
        body: 2
        data_center: 'zone-b'
        rack: '1'
    - host: ydb-node-zone-d.local
      host_config_id: 1
      location:
        body: 3
        data_center: 'zone-d'
        rack: '1'
  actor_system_config:
    use_auto_config: true
    cpu_count: 1
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
  security_config:
    enforce_user_token_requirement: true
  client_certificate_authorization:
    request_client_certificate: true
    client_certificate_definitions:
    - member_groups: ["ADMINS"]
      subject_terms:
      - short_name: "O"
        values: ["YDB"]
  domains_config:
    security_config:
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
    ```

{% endlist %}

Для ускорения и упрощения первичного развёртывания {{ ydb-short-name }} конфигурационный файл уже содержит большинство настроек для установки кластера. Достаточно заменить стандартные хосты FQDN в разделе `hosts` и пути к дискам на актуальные в разделе `hosts_configs`.

- Раздел `hosts`:

  ```yaml
  ...
  hosts:
    - host: ydb-node-zone-a.local
    host_config_id: 1
    location:
      body: 1
      data_center: 'zone-a'
      rack: '1'
  ...
  ```

- Раздел `hosts_configs`:

  ```yaml
  ...
  host_configs:
  - host_config_id: 1
    drive:
    - path: /dev/disk/by-partlabel/ydb_disk_ssd_01
      type: SSD
    - path: /dev/disk/by-partlabel/ydb_disk_ssd_02
      type: SSD
    - path: /dev/disk/by-partlabel/ydb_disk_ssd_03
      type: SSD
  ...
  ```

Остальные секции и настройки конфигурационного файла остаются без изменений.

Создайте  файл `inventory/ydb_inventory.yaml` с содержимым:

```yaml
plugin: ydb_platform.ydb.ydb_inventory
ydb_config: "files/config.yaml"
  ```

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
