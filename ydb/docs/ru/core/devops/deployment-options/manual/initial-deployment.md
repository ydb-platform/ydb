# Развёртывание {{ ydb-short-name }} кластера вручную

<!-- markdownlint-disable blanks-around-fences -->
{% note warning %}

Данная инструкция предназначена только для развёртывания кластеров с [конфигурацией V1](../../configuration-management/configuration-v1/index.md). Развёртывание кластеров с [конфигурацией V2](../../configuration-management/configuration-v2/index.md) в настоящий момент находится в разработке.

{% endnote %}

Этот документ описывает способ развернуть мультитенантный кластер {{ ydb-short-name }} на нескольких физических или виртуальных серверах.

## Перед началом работы {#before-start}

### Требования {#requirements}

Ознакомьтесь с [системными требованиями](../../../devops/concepts/system-requirements.md) и [топологией кластера](../../../concepts/topology.md).

У вас должен быть SSH доступ на все сервера. Это необходимо для установки артефактов и запуска исполняемого файла {{ ydb-short-name }}.

Сетевая конфигурация должна разрешать TCP соединения по следующим портам (по умолчанию, могут быть изменены настройками):

* 22: сервис SSH;
* 2135, 2136: GRPC для клиент-кластерного взаимодействия;
* 19001, 19002: Interconnect для внутрикластерного взаимодействия узлов;
* 8765, 8766: HTTP интерфейс {{ ydb-short-name }} Embedded UI.

При размещении нескольких динамических узлов на одном сервере потребуются отдельные порты для gRPC, Interconnect и HTTP интерфейса каждого динамического узла в рамках сервера.

Убедитесь в том, что системные часы на всех серверах в составе кластера синхронизированы с помощью инструментов `ntpd` или `chrony`. Желательно использовать единый источник времени для всех серверов кластера, чтобы обеспечить одинаковую обработку секунд координации (leap seconds).

Если применяемый на серверах кластера тип Linux использует `syslogd` для логирования, необходимо настроить ротацию файлов лога с использованием инструмента `logrotate` или его аналогов. Сервисы {{ ydb-short-name }}  могут генерировать значительный объем системных логов, в особенности при повышении уровня логирования для диагностических целей, поэтому важно включить ротацию файлов системного лога для исключения ситуаций переполнения файловой системы `/var`.

Выберите серверы и диски, которые будут использоваться для хранения данных:

* Используйте схему отказоустойчивости `block-4-2` для развертывания кластера в одной зоне доступности (AZ), задействуя не менее 8 серверов. Данная схема позволяет переживать отказ 2 серверов.
* Используйте схему отказоустойчивости `mirror-3-dc` для развертывания кластера в трех зонах доступности (AZ), задействуя не менее 9 серверов. Данная схема позволяет переживать отказ 1 AZ и 1 сервера в другой AZ. Количество задействованных серверов в каждой AZ должно быть одинаковым.

{% note info %}

Запускайте каждый статический узел (узел хранения данных) на отдельном сервере. Возможно совмещение статических и динамических узлов на одном сервере, а также размещение на одном сервере нескольких динамических узлов при наличии достаточных вычислительных ресурсов.

{% endnote %}

Подробнее требования к оборудованию описаны в разделе [{#T}](../../../devops/concepts/system-requirements.md).

### Подготовка ключей и сертификатов TLS {#tls-certificates}

Защита трафика и проверка подлинности серверных узлов {{ ydb-short-name }} осуществляется с использованием протокола TLS. Перед установкой кластера необходимо спланировать состав серверов, определиться со схемой именования узлов и конкретными именами, и подготовить ключи и сертификаты TLS.

Вы можете использовать существующие или сгенерировать новые сертификаты. Следующие файлы ключей и сертификатов TLS должны быть подготовлены в формате PEM:

* `ca.crt` - сертификат центра регистрации (Certification Authority, CA), которым подписаны остальные сертификаты TLS (одинаковые файлы на всех узлах кластера);
* `node.key` - секретные ключи TLS для каждого из узлов кластера (свой ключ на каждый сервер кластера);
* `node.crt` - сертификаты TLS для каждого из узлов кластера (соответствующий ключу сертификат);
* `web.pem` - конкатенация секретного ключа узла, сертификата узла и сертификата центра регистрации для работы HTTP интерфейса мониторинга (свой файл на каждый сервер кластера).

Необходимые параметры формирования сертификатов определяются политикой организации. Обычно сертификаты и ключи для {{ ydb-short-name }} формируются со следующими параметрами:

* ключи RSA длиною 2048 или 4096 бит;
* алгоритм подписи сертификатов SHA-256 с шифрованием RSA;
* срок действия сертификатов узлов не менее 1 года;
* срок действия сертификата центра регистрации не менее 3 лет.

Необходимо, чтобы сертификат центра регистрации был помечен соответствующим образом: должен быть установлен признак CA, а также включены виды использования "Digital Signature, Non Repudiation, Key Encipherment, Certificate Sign".

Для сертификатов узлов важно соответствие фактического имени хоста (или имён хостов) значениям, указанным в поле "Subject Alternative Name". Для сертификатов должны быть включены виды использования "Digital Signature, Key Encipherment" и расширенные виды использования "TLS Web Server Authentication, TLS Web Client Authentication". Необходимо, чтобы сертификаты узлов поддерживали как серверную, так и клиентскую аутентификацию (опция `extendedKeyUsage = serverAuth,clientAuth` в настройках OpenSSL).

Для пакетной генерации или обновления сертификатов кластера {{ ydb-short-name }} с помощью программного обеспечения OpenSSL можно воспользоваться [примером скрипта](https://github.com/ydb-platform/ydb/blob/main/ydb/deploy/tls_cert_gen/), размещённым в репозитории {{ ydb-short-name }} на GitHub. Скрипт позволяет автоматически сформировать необходимые файлы ключей и сертификатов для всего набора узлов кластера за одну операцию, облегчая подготовку к установке.

## Создайте системного пользователя и группу, от имени которых будет работать {{ ydb-short-name }} {#create-user}

На каждом сервере, где будет запущен {{ ydb-short-name }}, выполните:

```bash
sudo groupadd ydb
sudo useradd ydb -g ydb
```

Для того, чтобы сервис {{ ydb-short-name }} имел доступ к блочным дискам для работы, необходимо добавить пользователя, под которым будут запущены процессы {{ ydb-short-name }}, в группу `disk`:

```bash
sudo usermod -aG disk ydb
```

## Настройте лимиты файловых дескрипторов {#file-descriptors}

Для корректной работы {{ ydb-short-name }}, особенно при использовании [спиллинга](../../../concepts/query_execution/spilling.md) в многоузловых кластерах, рекомендуется увеличить лимит на количество одновременно открытых файловых дескрипторов.

Для изменения лимита файловых дескрипторов добавьте следующие строки в файл `/etc/security/limits.conf`:

```bash
ydb soft nofile 10000
ydb hard nofile 10000
```

Где `ydb` — имя пользователя, под которым запускается `ydbd`.

После изменения файла необходимо перезагрузить систему или заново залогиниться для применения новых лимитов.

{% note info %}

Для получения дополнительной информации о конфигурации спиллинга и его связи с файловыми дескрипторами см. раздел [«Конфигурация спиллинга»](../../../reference/configuration/table_service_config.md#file-system-requirements).

{% endnote %}

## Установите программное обеспечение {{ ydb-short-name }} на каждом сервере {#install-binaries}

1. Скачайте и распакуйте архив с исполняемым файлом `ydbd` и необходимыми для работы {{ ydb-short-name }} библиотеками:

    ```bash
    mkdir ydbd-stable-linux-amd64
    curl -L <binaries_url> | tar -xz --strip-component=1 -C ydbd-stable-linux-amd64
    ```
    где `binaries_url` ссылка на архив нужной вам версии со страницы [загрузок](../../../downloads/index.md)

1. Скопируйте исполняемый файл и библиотеки в соответствующие директории:

    ```bash
    sudo cp -iR ydbd-stable-linux-amd64/bin /opt/ydb/
    sudo cp -iR ydbd-stable-linux-amd64/lib /opt/ydb/
    ```

1. Установите владельца файлов и каталогов:

    ```bash
    sudo chown -R root:bin /opt/ydb
    ```

## Подготовьте и очистите диски на каждом сервере {#prepare-disks}

{% include [_includes/storage-device-requirements.md](../../../_includes/storage-device-requirements.md) %}

Получить список блочных устройств на сервере можно командой `lsblk`. Пример вывода:

```txt
NAME   MAJ:MIN RM   SIZE RO TYPE MOUNTPOINTS
loop0    7:0    0  63.3M  1 loop /snap/core20/1822
...
vda    252:0    0    40G  0 disk
├─vda1 252:1    0     1M  0 part
└─vda2 252:2    0    40G  0 part /
vdb    252:16   0   186G  0 disk
└─vdb1 252:17   0   186G  0 part
```

Названия блочных устройств зависят от настроек операционной системы, заданных базовым образом или настроенных вручную. Обычно имена устройств состоят из трех частей:

- Фиксированный префикс или префикс, указывающий на тип устройства
- Последовательный идентификатор устройства (может быть буквой или числом)
- Последовательный идентификатор раздела на данном устройстве (обычно число)

1. Создайте разделы на выбранных дисках:

    {% note alert %}

    Следующая операция удалит все разделы на указанном диске! Убедитесь, что вы указали диск, на котором нет других данных!

    {% endnote %}

    ```bash
    DISK=/dev/nvme0n1
    sudo parted ${DISK} mklabel gpt -s
    sudo parted -a optimal ${DISK} mkpart primary 0% 100%
    sudo parted ${DISK} name 1 ydb_disk_ssd_01
    sudo partx --u ${DISK}
    ```

    Выполните команду `ls -l /dev/disk/by-partlabel/`, чтобы убедиться что в системе появился диск с меткой `/dev/disk/by-partlabel/ydb_disk_ssd_01`.

    Если вы планируете использовать более одного диска на каждом сервере, укажите для каждого свою уникальную метку вместо `ydb_disk_ssd_01`. Метки дисков должны быть уникальны в рамках каждого сервера, и используются в конфигурационных файлах, как показано в последующих инструкциях.

    Для упрощения последующей настройки удобно использовать одинаковые метки дисков на серверах кластера, имеющих идентичную конфигурацию дисков.

2. Очистите диск встроенной в исполняемый файл `ydbd` командой:

    {% note warning %}

    После выполнения команды данные на диске сотрутся.

    {% endnote %}

    ```bash
    sudo LD_LIBRARY_PATH=/opt/ydb/lib /opt/ydb/bin/ydbd admin bs disk obliterate /dev/disk/by-partlabel/ydb_disk_ssd_01
    ```

    Проделайте данную операцию для каждого диска, который будет использоваться для хранения данных {{ ydb-short-name }}.

### Пример полной команды для разметки 3-х дисков

```bash
DISK=/dev/vdb
sudo parted ${DISK} mklabel gpt -s
sudo parted -a optimal ${DISK} mkpart primary 0% 100%
sudo parted ${DISK} name 1 ydb_disk_ssd_01
sudo partx --u ${DISK}
sleep 5
sudo LD_LIBRARY_PATH=/opt/ydb/lib /opt/ydb/bin/ydbd admin bs disk obliterate /dev/disk/by-partlabel/ydb_disk_ssd_01

DISK=/dev/vdc
sudo parted ${DISK} mklabel gpt -s
sudo parted -a optimal ${DISK} mkpart primary 0% 100%
sudo parted ${DISK} name 1 ydb_disk_ssd_02
sudo partx --u ${DISK}
sleep 5
sudo LD_LIBRARY_PATH=/opt/ydb/lib /opt/ydb/bin/ydbd admin bs disk obliterate /dev/disk/by-partlabel/ydb_disk_ssd_02

DISK=/dev/vdd
sudo parted ${DISK} mklabel gpt -s
sudo parted -a optimal ${DISK} mkpart primary 0% 100%
sudo parted ${DISK} name 1 ydb_disk_ssd_03
sudo partx --u ${DISK}
sleep 5
sudo LD_LIBRARY_PATH=/opt/ydb/lib /opt/ydb/bin/ydbd admin bs disk obliterate /dev/disk/by-partlabel/ydb_disk_ssd_03
```

### Проверьте подготовку дисков

Для проверки корректной разметки дисков выполните команду на каждом сервере кластера:

```bash
ls -al /dev/disk/by-partlabel/
```

В выводе команды должны быть созданные и размеченные вами диски

```bash
lrwxrwxrwx 1 root root    10 Nov 26 12:54 ydb_disk_ssd_01 -> ../../vdb1
lrwxrwxrwx 1 root root    10 Nov 26 12:54 ydb_disk_ssd_02 -> ../../vdc1
lrwxrwxrwx 1 root root    10 Nov 26 12:54 ydb_disk_ssd_03 -> ../../vdd1
```

## Подготовьте конфигурационные файлы {#config}

Подготовьте конфигурационный файл {{ ydb-short-name }}:
```yaml
static_erasure: mirror-3-dc
host_configs:
- drive:
  - path: /dev/disk/by-partlabel/ydb_disk_ssd_01
    type: SSD
  - path: /dev/disk/by-partlabel/ydb_disk_ssd_02
    type: SSD
  - path: /dev/disk/by-partlabel/ydb_disk_ssd_03
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
  security_config:
    enforce_user_token_requirement: true
    default_users:
      - name: "root"
        password: ""
    default_access:
      - "+(F):root"
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
table_service_config:
  sql_version: 1
actor_system_config:
  executor:
  - name: System
    threads: 2
    type: BASIC
  - name: User
    threads: 3
    type: BASIC
  - name: Batch
    threads: 2
    type: BASIC
  - name: IO
    threads: 1
    time_per_mailbox_micro_secs: 100
    type: IO
  - name: IC
    spin_threshold: 10
    threads: 1
    time_per_mailbox_micro_secs: 100
    type: BASIC
  scheduler:
    progress_threshold: 10000
    resolution: 256
    spin_threshold: 0
blob_storage_config:
  service_set:
    groups:
    - erasure_species: mirror-3-dc
      rings:
      - fail_domains:
        - vdisk_locations:
          - node_id: static-node-1.ydb-cluster.com
            pdisk_category: SSD
            path: /dev/disk/by-partlabel/ydb_disk_ssd_01
        - vdisk_locations:
          - node_id: static-node-1.ydb-cluster.com
            pdisk_category: SSD
            path: /dev/disk/by-partlabel/ydb_disk_ssd_02
        - vdisk_locations:
          - node_id: static-node-1.ydb-cluster.com
            pdisk_category: SSD
            path: /dev/disk/by-partlabel/ydb_disk_ssd_03
      - fail_domains:
        - vdisk_locations:
          - node_id: static-node-2.ydb-cluster.com
            pdisk_category: SSD
            path: /dev/disk/by-partlabel/ydb_disk_ssd_01
        - vdisk_locations:
          - node_id: static-node-2.ydb-cluster.com
            pdisk_category: SSD
            path: /dev/disk/by-partlabel/ydb_disk_ssd_02
        - vdisk_locations:
          - node_id: static-node-2.ydb-cluster.com
            pdisk_category: SSD
            path: /dev/disk/by-partlabel/ydb_disk_ssd_03
      - fail_domains:
        - vdisk_locations:
          - node_id: static-node-3.ydb-cluster.com
            pdisk_category: SSD
            path: /dev/disk/by-partlabel/ydb_disk_ssd_01
        - vdisk_locations:
          - node_id: static-node-3.ydb-cluster.com
            pdisk_category: SSD
            path: /dev/disk/by-partlabel/ydb_disk_ssd_02
        - vdisk_locations:
          - node_id: static-node-3.ydb-cluster.com
            pdisk_category: SSD
            path: /dev/disk/by-partlabel/ydb_disk_ssd_03
channel_profile_config:
  profile:
  - channel:
    - erasure_species: mirror-3-dc
      pdisk_category: 0
      storage_pool_kind: ssd
    - erasure_species: mirror-3-dc
      pdisk_category: 0
      storage_pool_kind: ssd
    - erasure_species: mirror-3-dc
      pdisk_category: 0
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
client_certificate_authorization:
  request_client_certificate: true
  client_certificate_definitions:
    - member_groups: ["registerNode@cert"]
      subject_terms:
      - short_name: "O"
        values: ["YDB"]
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

Сохраните конфигурационный файл YDB под именем `/opt/ydb/cfg/config.yaml` на каждом сервере кластера.

Более подробная информация по созданию файла конфигурации приведена в разделе [{#T}](../../../reference/configuration/index.md).

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

## Запустите статические узлы {#start-storage}

{% list tabs group=manual-systemd %}

- Вручную

  Запустите сервис хранения данных {{ ydb-short-name }} на каждом статическом узле кластера:

  ```bash
  sudo su - ydb
  cd /opt/ydb
  export LD_LIBRARY_PATH=/opt/ydb/lib
  /opt/ydb/bin/ydbd server --log-level 3 --syslog --tcp --config-dir /opt/ydb/cfg \
      --grpcs-port 2135 --ic-port 19001 --mon-port 8765 --mon-cert /opt/ydb/certs/web.pem --node static
  ```

- С использованием systemd

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
      --config-dir /opt/ydb/cfg \
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

![Ручная установка, запущенные статические узлы](../_assets/manual_installation_1.png)

## Инициализируйте кластер {#initialize-cluster}

Операция инициализации кластера осуществляет настройку набора статических узлов, перечисленных в конфигурационном файле кластера, для хранения данных {{ ydb-short-name }}.

Для инициализации кластера потребуется файл сертификата центра регистрации `ca.crt`, путь к которому должен быть указан при выполнении соответствующих команд. Перед выполнением соответствующих команд скопируйте файл `ca.crt` на сервер, на котором эти команды будут выполняться.

На одном из серверов хранения в составе кластера выполните команды:

Сначала получите авторизационный токен для регистрации запросов. Для этого выполните приведённую ниже команду.

```bash
/opt/ydb/bin/ydb --ca-file ca.crt -e grpcs://`hostname -f`:2135 -d /Root --user root --no-password auth get-token -f > auth_token
```

Инициализируйте кластер используя полученный токен

```bash
export LD_LIBRARY_PATH=/opt/ydb/lib
/opt/ydb/bin/ydbd --ca-file ca.crt -s grpcs://`hostname -f`:2135 -f auth_token \
    admin blobstorage config init --yaml-file  /opt/ydb/cfg/config.yaml
echo $?
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

- Вручную

  Запустите динамический узел {{ ydb-short-name }} для базы `/Root/testdb`:

  ```bash
  sudo su - ydb
  cd /opt/ydb
  export LD_LIBRARY_PATH=/opt/ydb/lib
  /opt/ydb/bin/ydbd server --grpcs-port 2136 --grpc-ca /opt/ydb/certs/ca.crt \
      --ic-port 19002 --ca /opt/ydb/certs/ca.crt \
      --mon-port 8766 --mon-cert /opt/ydb/certs/web.pem \
      --config-dir /opt/ydb/cfg \
      --tenant /Root/testdb \
      --grpc-cert /opt/ydb/certs/node.crt \
      --grpc-key /opt/ydb/certs/node.key \
      --node-broker grpcs://<ydb-static-node1>:2135 \
      --node-broker grpcs://<ydb-static-node2>:2135 \
      --node-broker grpcs://<ydb-static-node3>:2135
  ```

  В примере команды выше `<ydb-static-node1>` , `<ydb-static-node2>`, `<ydb-static-node3>`  - FQDN трех любых серверов, на которых запущены статические узлы кластера.

- С использованием systemd

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
      --config-dir /opt/ydb/cfg \
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

    Вместо значения `passw0rd` подставьте необходимый пароль. Сохраните пароль в отдельный файл. Последующие команды от имени пользователя `root` будут выполняться с использованием пароля, передаваемого с помощью ключа `--password-file <path_to_user_password>`. Также пароль можно сохранить в профиле подключения, как описано в [документации {{ ydb-short-name }} CLI](../../../reference/ydb-cli/profile/index.md).

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

При выполнении команд создания учётных записей и присвоения групп клиент {{ ydb-short-name }} CLI будет запрашивать ввод пароля пользователя `root`. Избежать многократного ввода пароля можно, создав профиль подключения, как описано в [документации {{ ydb-short-name }} CLI](../../../reference/ydb-cli/profile/index.md).

## Протестируйте работу с созданной базой {#try-first-db}

1. Установите {{ ydb-short-name }} CLI, как описано в [документации](../../../reference/ydb-cli/install.md).

1. Создайте тестовую строковую (`test_row_table`) или колоночную таблицу (`test_column_table`):

{% list tabs %}

- Создание строковой таблицы

    ```bash
    ydb --ca-file ca.crt -e grpcs://<node.ydb.tech>:2136 -d /Root/testdb --user root \
        yql -s 'CREATE TABLE `testdir/test_row_table` (id Uint64, title Utf8, PRIMARY KEY (id));'
    ```

- Создание колоночной таблицы

    ```bash
    ydb --ca-file ca.crt -e grpcs://<node.ydb.tech>:2136 -d /Root/testdb --user root \
        yql -s 'CREATE TABLE `testdir/test_column_table` (id Uint64 NOT NULL, title Utf8, PRIMARY KEY (id)) WITH (STORE = COLUMN);'
    ```

{% endlist %}

Где `<node.ydb.tech>` - FQDN сервера, на котором запущен динамический узел, обслуживающий базу `/Root/testdb`.

## Проверка доступа ко встроенному web-интерфейсу

Для проверки доступа ко встроенному web-интерфейсу {{ ydb-short-name }} достаточно открыть в Web-браузере страницу с адресом `https://<node.ydb.tech>:8765`, где `<node.ydb.tech>` - FQDN сервера, на котором запущен любой статический узел {{ ydb-short-name }}.

В Web-браузере должно быть настроено доверие в отношении центра регистрации, выпустившего сертификаты для кластера {{ ydb-short-name }}, в противном случае будет отображено предупреждение об использовании недоверенного сертификата.

Если в кластере включена аутентификация, в Web-браузере должен отобразиться запрос логина и пароля. После ввода верных данных аутентификации должна отобразиться начальная страница встроенного web-интерфейса. Описание доступных функций и пользовательского интерфейса приведено в разделе [{#T}](../../../reference/embedded-ui/index.md).

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

1. Команда создания базы данных выполняется в следующей форме:

    ```bash
    export LD_LIBRARY_PATH=/opt/ydb/lib
    /opt/ydb/bin/ydbd admin database /Root/testdb create ssd:1
    ```

1. При обращении к базе данных из {{ ydb-short-name }} CLI и приложений используется протокол grpc вместо grpcs, и не используется аутентификация.
