# Развёртывание кластера с использованием конфигурации V2

{% note alert  %}

   Эта статья посвящена кластерам {{ ydb-short-name }}, в которых используется **конфигурация V2**. Данный способ конфигурирования пока является экспериментальным и доступен только для версий {{ ydb-short-name }} начиная с v25.1. Для использования в продакшене мы рекомендуем выбирать [конфигурацию V1](./deployment-configuration-v1.md) — она является основной и официально поддерживаемой для всех кластеров {{ ydb-short-name }}.

{% endnote %}

## Подготовьте окружение {#deployment-preparation}

Перед развёртыванием системы обязательно выполните подготовительные действия. Ознакомьтесь с документом [{#T}](deployment-preparation.md).

## Подготовьте конфигурационные файлы {#config}

Подготовьте конфигурационный файл {{ ydb-short-name }}:

```yaml
metadata:
  kind: MainConfig
  cluster: ""
  version: 0
config:
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
      rack: '2'
  - host: ydb-node-zone-c.local
    host_config_id: 1
    location:
      body: 3
      data_center: 'zone-c'
      rack: '3'
  actor_system_config:
    use_auto_config: true
    cpu_count: 8
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
    bootstrap_allowed_sids:
    - "root"
    - "ADMINS"
    - "DATABASE-ADMINS"
  client_certificate_authorization:
    request_client_certificate: true
    client_certificate_definitions:
        - member_groups: ["ADMINS"]
          subject_terms:
          - short_name: "O"
            values: ["YDB"]
```

Для ускорения и упрощения первичного развёртывания {{ ydb-short-name }} конфигурационный файл уже содержит большинство настроек для установки кластера. Достаточно заменить стандартные хосты FQDN в разделе `hosts` и пути к дискам на актуальные в разделе `hosts_configs`.

* Раздел `hosts`:

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

* Раздел `hosts_configs`:

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

Сохраните конфигурационный файл YDB под именем `/tmp/config.yaml` на каждом сервере кластера.

Более подробная информация по созданию файла конфигурации приведена в разделе [{#T}](../../../../reference/configuration/index.md).

## Скопируйте ключи и сертификаты TLS на каждый сервер {#tls-copy-cert}

Подготовленные ключи и сертификаты TLS необходимо скопировать в защищенный каталог на каждом из узлов кластера {{ ydb-short-name }}. Ниже приведен пример команд для создания защищенного каталога и копирования файлов с ключами и сертификатами.

```bash
sudo mkdir -p /opt/ydb/certs
sudo cp -v ca.crt /opt/ydb/certs/
sudo cp -v node.crt /opt/ydb/certs/
sudo cp -v node.key /opt/ydb/certs/
sudo cp -v web.pem /opt/ydb/certs/
sudo chown -R ydb:ydb /opt/ydb/certs
sudo chmod 700 /opt/ydb/certs
```

## Подготовьте конфигурацию на статических узлах кластера

Создайте на каждой машине пустую директорию `opt/ydb/cfg` для работы кластера с конфигурацией. В случае запуска нескольких узлов кластера на одной машине создайте отдельные директории под каждый узел.

```bash
sudo mkdir -p /opt/ydb/cfg
sudo chown -R ydb:ydb /opt/ydb/cfg
```

Выполнив специальную команду на каждой машине, инициализируйте эту директорию файлом конфигурации.

```bash
sudo /opt/ydb/bin/ydb admin node config init --config-dir/opt/ydb/cfg --from-config /tmp/config.yaml
```

Исходный файл `/tmp/config.yaml` после выполнения этой команды больше не используется, его можно удалить.

## Запустите статические узлы {#start-storage}

{% list tabs group=manual-systemd %}

* Вручную

  Запустите сервис хранения данных {{ ydb-short-name }} на каждом статическом узле кластера:

  ```bash
  sudo su - ydb
  cd /opt/ydb
  export LD_LIBRARY_PATH=/opt/ydb/lib
  /opt/ydb/bin/ydbd server --log-level 3 --syslog --tcp --yaml-config  /opt/ydb/cfg/config.yaml \
      --grpcs-port 2135 --ic-port 19001 --mon-port 8765 --mon-cert /opt/ydb/certs/web.pem --node static
  ```

* С использованием systemd

  Создайте на каждом сервере, где будет размещен статический узел кластера, конфигурационный файл systemd `/etc/systemd/system/ydbd-storage.service` по приведенному ниже образцу. Образец файла также можно [скачать из репозитория](https://github.com/ydb-platform/ydb/blob/main/ydb/deploy/systemd_services/ydbd-storage.service).

  ```ini
  [Unit]
  Description=YDB storage node
  After=network-online.target rc-local.service
  Wants=network-online.target
  StartLimitInterval=10
  StartLimitBurst=15

  [Service]
  Restart=always
  RestartSec=1
  User=ydb
  PermissionsStartOnly=true
  StandardOutput=syslog
  StandardError=syslog
  SyslogIdentifier=ydbd
  SyslogFacility=daemon
  SyslogLevel=err
  Environment=LD_LIBRARY_PATH=/opt/ydb/lib
  ExecStart=/opt/ydb/bin/ydbd server --log-level 3 --syslog --tcp \
      --yaml-config  /opt/ydb/cfg/config.yaml \
      --grpcs-port 2135 --ic-port 19001 --mon-port 8765 \
      --mon-cert /opt/ydb/certs/web.pem --node static
  LimitNOFILE=65536
  LimitCORE=0
  LimitMEMLOCK=3221225472

  [Install]
  WantedBy=multi-user.target
  ```

  Запустите сервис на каждом статическом узле {{ ydb-short-name }}:

  ```bash
  sudo systemctl start ydbd-storage
  ```

{% endlist %}

После запуска статических узлов проверьте их работоспособность через встроенный веб-интерфейс {{ ydb-short-name }} (Embedded UI):

1. Откройте в браузере адрес `https://<node.ydb.tech>:8765`, где `<node.ydb.tech>` - FQDN сервера, на котором запущен любой статический узел;
2. Перейдите на вкладку **Nodes**;
3. Убедитесь, что в списке отображаются все 3 статических узла.

![Ручная установка, запущенные статические узлы](../../_assets/manual_installation_1.png)

## Инициализируйте кластер {#initialize-cluster}

Операция инициализации кластера осуществляет настройку набора статических узлов, перечисленных в конфигурационном файле кластера, для хранения данных {{ ydb-short-name }}.

Для инициализации кластера потребуется файл сертификата центра регистрации `ca.crt`, путь к которому должен быть указан при выполнении соответствующих команд. Перед выполнением соответствующих команд скопируйте файл `ca.crt` на сервер, на котором эти команды будут выполняться.

На одном из серверов хранения в составе кластера выполните команды:

Инициализируйте кластер используя полученный токен

```bash
export LD_LIBRARY_PATH=/opt/ydb/lib
/opt/ydb/bin/ydb --ca-file ca.crt \
    --client-cert-file node.crt \
    --client-cert-key-file node.key \
    -e grpcs://`hostname -f`:2135 \
    admin cluster bootstrap --uuid <строка>
echo $?
```

После инициализации кластера для выполнения дальнейших административных команд необходимо предварительно получить аутентификационный токен.

```bash
/opt/ydb/bin/ydb -e grpcs://`hostname -f`:2135 -d /Root --ca-file ca.crt \
--user root --no-password auth get-token --force > token-file
```

При успешном выполнении инициализации кластера выведенный на экран код завершения команды инициализации кластера должен быть нулевым.

## Создайте базу данных {#create-db}

Для работы со строковыми или колоночными таблицами необходимо создать как минимум одну базу данных и запустить процесс или процессы, обслуживающие эту базу данных (динамические узлы).

Для выполнения административной команды создания базы данных потребуется файл сертификата центра регистрации `ca.crt`, аналогично описанному выше порядку выполнения действий по инициализации кластера.

При создании базы данных устанавливается первоначальное количество используемых групп хранения, определяющее доступную пропускную способность ввода-вывода и максимальную емкость хранения. Количество групп хранения может быть при необходимости увеличено после создания базы данных.

На одном из серверов хранения в составе кластера выполните команды:

```bash
export LD_LIBRARY_PATH=/opt/ydb/lib
/opt/ydb/bin/ydbd --ca-file ca.crt -s grpcs://`hostname -f`:2135 -f auth_token \
    admin database /Root/testdb create ssd:8
echo $?
```

При успешном создании базы данных, выведенный на экран код завершения команды должен быть нулевым.

В приведенном выше примере команд используются следующие параметры:

* `/Root` - имя корневого домена, сгенерированного автоматически при инициализации кластера;
* `testdb` - имя создаваемой базы данных;
* `ssd:8` - задает пул хранения для базы данных и количество групп в нем. Имя пула (`ssd`) должно соответствовать типу диска, указанному в конфигурации кластера (например, в `default_disk_type`), и является регистронезависимым. Число после двоеточия — это количество выделяемых групп хранения.

## Запустите динамические узлы {#start-dynnode}

{% list tabs group=manual-systemd %}

* Вручную

  Запустите динамический узел {{ ydb-short-name }} для базы `/Root/testdb`:

  ```bash
  sudo su - ydb
  cd /opt/ydb
  export LD_LIBRARY_PATH=/opt/ydb/lib
  /opt/ydb/bin/ydbd server --grpcs-port 2136 --grpc-ca /opt/ydb/certs/ca.crt \
      --ic-port 19002 --ca /opt/ydb/certs/ca.crt \
      --mon-port 8766 --mon-cert /opt/ydb/certs/web.pem \
      --yaml-config  /opt/ydb/cfg/config.yaml \
      --tenant /Root/testdb \
      --grpc-cert /opt/ydb/certs/node.crt \
      --grpc-key /opt/ydb/certs/node.key \
      --node-broker grpcs://<ydb-static-node1>:2135 \
      --node-broker grpcs://<ydb-static-node2>:2135 \
      --node-broker grpcs://<ydb-static-node3>:2135
  ```

  В примере команды выше `<ydb-static-node1>` , `<ydb-static-node2>`, `<ydb-static-node3>`  - FQDN трех любых серверов, на которых запущены статические узлы кластера.

* С использованием systemd

  Создайте конфигурационный файл systemd `/etc/systemd/system/ydbd-testdb.service` по приведенному ниже образцу. Образец файла также можно [скачать из репозитория](https://github.com/ydb-platform/ydb/blob/main/ydb/deploy/systemd_services/ydbd-testdb.service).

  ```ini
  [Unit]
  Description=YDB testdb dynamic node
  After=network-online.target rc-local.service
  Wants=network-online.target
  StartLimitInterval=10
  StartLimitBurst=15

  [Service]
  Restart=always
  RestartSec=1
  User=ydb
  PermissionsStartOnly=true
  StandardOutput=syslog
  StandardError=syslog
  SyslogIdentifier=ydbd
  SyslogFacility=daemon
  SyslogLevel=err
  Environment=LD_LIBRARY_PATH=/opt/ydb/lib
  ExecStart=/opt/ydb/bin/ydbd server \
      --grpcs-port 2136 --grpc-ca /opt/ydb/certs/ca.crt \
      --ic-port 19002 --ca /opt/ydb/certs/ca.crt \
      --mon-port 8766 --mon-cert /opt/ydb/certs/web.pem \
      --yaml-config  /opt/ydb/cfg/config.yaml \
      --tenant /Root/testdb \
      --grpc-cert /opt/ydb/certs/node.crt \
      --grpc-key /opt/ydb/certs/node.key \
      --node-broker grpcs://<ydb-static-node1>:2135 \
      --node-broker grpcs://<ydb-static-node2>:2135 \
      --node-broker grpcs://<ydb-static-node3>:2135
  LimitNOFILE=65536
  LimitCORE=0
  LimitMEMLOCK=32212254720

  [Install]
  WantedBy=multi-user.target
  ```

  В примере команды выше `<ydb-static-node1>` , `<ydb-static-node2>`, `<ydb-static-node3>`  - FQDN трех любых серверов, на которых запущены статические узлы кластера.
  
  Запустите динамический узел {{ ydb-short-name }} для базы `/Root/testdb`:

  ```bash
  sudo systemctl start ydbd-testdb
  ```

{% endlist %}

Запустите дополнительные динамические узлы на других серверах для масштабирования и обеспечения отказоустойчивости базы данных.

## Настройка учетных записей {#security-setup}

1. Установите пароль для учетной записи `root`, используя полученный ранее токен:

    ```bash
    ydb --ca-file ca.crt -e grpcs://<node.ydb.tech>:2136 -d /Root/testdb --token-file auth_token \
        yql -s 'ALTER USER root PASSWORD "passw0rd"'
    ```

    Вместо значения `passw0rd` подставьте необходимый пароль. Сохраните пароль в отдельный файл. Последующие команды от имени пользователя `root` будут выполняться с использованием пароля, передаваемого с помощью ключа `--password-file <path_to_user_password>`. Также пароль можно сохранить в профиле подключения, как описано в [документации {{ ydb-short-name }} CLI](../../../../reference/ydb-cli/profile/index.md).

1. Создайте дополнительные учетные записи:

    ```bash
    ydb --ca-file ca.crt -e grpcs://<node.ydb.tech>:2136 -d /Root/testdb --user root --password-file <path_to_root_pass_file> \
        yql -s 'CREATE USER user1 PASSWORD "passw0rd"'
    ```

1. Установите права учетных записей, включив их во встроенные группы:

    ```bash
    ydb --ca-file ca.crt -e grpcs://<node.ydb.tech>:2136 -d /Root/testdb --user root --password-file <path_to_root_pass_file> \
        yql -s 'ALTER GROUP `ADMINS` ADD USER user1'
    ```

В перечисленных выше примерах команд `<node.ydb.tech>` — FQDN сервера, на котором запущен любой динамический узел, обслуживающий базу `/Root/testdb`. При подключении по SSH к динамическому узлу {{ ydb-short-name }} удобно использовать конструкцию `grpcs://$(hostname -f):2136` для получения FQDN.

При выполнении команд создания учётных записей и присвоения групп клиент {{ ydb-short-name }} CLI будет запрашивать ввод пароля пользователя `root`. Избежать многократного ввода пароля можно, создав профиль подключения, как описано в [документации {{ ydb-short-name }} CLI](../../../../reference/ydb-cli/profile/index.md).

## Протестируйте работу с созданной базой {#try-first-db}

1. Установите {{ ydb-short-name }} CLI, как описано в [документации](../../../../reference/ydb-cli/install.md).

1. Создайте тестовую строковую (`test_row_table`) или колоночную таблицу (`test_column_table`):

{% list tabs %}

* Создание строковой таблицы

    ```bash
    ydb --ca-file ca.crt -e grpcs://<node.ydb.tech>:2136 -d /Root/testdb --user root \
        yql -s 'CREATE TABLE `testdir/test_row_table` (id Uint64, title Utf8, PRIMARY KEY (id));'
    ```

* Создание колоночной таблицы

    ```bash
    ydb --ca-file ca.crt -e grpcs://<node.ydb.tech>:2136 -d /Root/testdb --user root \
        yql -s 'CREATE TABLE `testdir/test_column_table` (id Uint64 NOT NULL, title Utf8, PRIMARY KEY (id)) WITH (STORE = COLUMN);'
    ```

{% endlist %}

Где `<node.ydb.tech>` - FQDN сервера, на котором запущен динамический узел, обслуживающий базу `/Root/testdb`.

## Проверка доступа ко встроенному web-интерфейсу

Для проверки доступа ко встроенному web-интерфейсу {{ ydb-short-name }} достаточно открыть в Web-браузере страницу с адресом `https://<node.ydb.tech>:8765`, где `<node.ydb.tech>` - FQDN сервера, на котором запущен любой статический узел {{ ydb-short-name }}.

В Web-браузере должно быть настроено доверие в отношении центра регистрации, выпустившего сертификаты для кластера {{ ydb-short-name }}, в противном случае будет отображено предупреждение об использовании недоверенного сертификата.

Если в кластере включена аутентификация, в Web-браузере должен отобразиться запрос логина и пароля. После ввода верных данных аутентификации должна отобразиться начальная страница встроенного web-интерфейса. Описание доступных функций и пользовательского интерфейса приведено в разделе [{#T}](../../../../reference/embedded-ui/index.md).

{% note info %}

Обычно для обеспечения доступа ко встроенному web-интерфейсу {{ ydb-short-name }} настраивают отказоустойчивый HTTP-балансировщик на базе программного обеспечения `haproxy`, `nginx` или аналогов. Детали настройки HTTP-балансировщика выходят за рамки стандартной инструкции по установке {{ ydb-short-name }}.

{% endnote %}

## Особенности установки {{ ydb-short-name }} в незащищенном режиме

{% note warning %}

Мы не рекомендуем использовать незащищенный режим работы {{ ydb-short-name }} ни при эксплуатации, ни при разработке приложений.

{% endnote %}

Описанная выше процедура установки предусматривает развёртывание {{ ydb-short-name }} в стандартном защищенном режиме.

Незащищённый режим работы {{ ydb-short-name }} предназначен для решения тестовых задач, преимущественно связанных с разработкой и тестированием программного обеспечения {{ ydb-short-name }}. В незащищенном режиме:

* трафик между узлами кластера, а также между приложениями и кластером использует незашифрованные соединения;
* не используется аутентификация пользователей (включение аутентификации при отсутствии шифрования трафика не имеет смысла, поскольку логин и пароль в такой конфигурации передавались бы через сеть в открытом виде).

Установка {{ ydb-short-name }} для работы в незащищенном режиме производится в порядке, описанном выше, со следующими исключениями:

1. При подготовке к установке не требуется формировать сертификаты и ключи TLS, и не выполняется копирование сертификатов и ключей на узлы кластера.
1. Из конфигурационных файлов кластерных узлов исключаются секции `security_config`, `interconnect_config` и `grpc_config`.
1. Используются упрощенный вариант команд запуска статических и динамических узлов кластера: исключаются опции с именами файлов сертификатов и ключей, используется протокол `grpc` вместо `grpcs` при указании точек подключения.
1. Пропускается ненужный в незащищенном режиме шаг по получению токена аутентификации перед выполнением инициализации кластера и созданием базы данных.
1. Команда инициализации кластера выполняется в следующей форме:

    ```bash
    export LD_LIBRARY_PATH=/opt/ydb/lib
    ydb admin cluster bootstrap --uuid <строка>
    echo $?
    ```

6. Команда создания базы данных выполняется в следующей форме:

    ```bash
    export LD_LIBRARY_PATH=/opt/ydb/lib
    /opt/ydb/bin/ydbd admin database /Root/testdb create ssd:1
    ```

7. При обращении к базе данных из {{ ydb-short-name }} CLI и приложений используется протокол grpc вместо grpcs, и не используется аутентификация.
