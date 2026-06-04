# Preparing for Deployment

## Before You Begin {#before-start}

### Requirements {#requirements}

Review the [system requirements](../../../../devops/concepts/system-requirements.md) and [cluster topology](../../../../concepts/topology.md).

You must have SSH access to all servers. This is required to install artifacts and run the {{ ydb-short-name }} executable.

The network configuration must allow TCP connections on the following ports (default values, can be changed by settings):

* 22: SSH service;
* 2135, 2136: gRPC for client-cluster interaction;
* 19001, 19002: Interconnect for intra-cluster node communication;
* 8765, 8766: HTTP interface of {{ ydb-short-name }} Embedded UI.
* 9092, 9093: ports for Kafka API.

When placing multiple dynamic nodes on the same server, separate ports will be required for gRPC, Interconnect, HTTP interface, and Kafka API of each dynamic node within the server.

Ensure that the system clocks on all servers in the cluster are synchronized using `ntpd` or `chrony` tools. It is advisable to use a single time source for all cluster servers to ensure consistent handling of leap seconds.

If the Linux distribution used on the cluster servers uses `syslogd` for logging, you need to configure log file rotation using `logrotate` or its equivalents. {{ ydb-short-name }} services can generate a significant amount of system logs, especially when the logging level is increased for diagnostic purposes, so it is important to enable system log file rotation to avoid `/var` file system overflow situations.

Select the servers and disks that will be used for data storage:

* Use the `block-4-2` fault tolerance scheme to deploy the cluster in a single availability zone (AZ), using at least 8 servers. This scheme can withstand the failure of 2 servers.
* Use the `mirror-3-dc` fault tolerance scheme to deploy the cluster in three availability zones (AZ), using at least 9 servers. This scheme can withstand the failure of 1 AZ and 1 server in another AZ. The number of servers used in each AZ must be the same.

{% note info %}

Run each static node (storage node) on a separate server. It is possible to co-locate static and dynamic nodes on the same server, as well as place multiple dynamic nodes on the same server if sufficient computing resources are available.

{% endnote %}

More details about hardware requirements are described in the [{#T}](../../../../devops/concepts/system-requirements.md) section.

### Preparing TLS Keys and Certificates {#tls-certificates}

Traffic protection and authentication of {{ ydb-short-name }} server nodes is performed using the TLS protocol. Before installing the cluster, you need to plan the server composition, decide on the node naming scheme and specific names, and prepare TLS keys and certificates.

You can use existing certificates or generate new ones. The following TLS key and certificate files must be prepared in PEM format:

* `ca.crt` - Certificate Authority (CA) certificate that signs the other TLS certificates (same file on all cluster nodes);
* `node.key` - TLS private keys for each cluster node (a separate key for each cluster server);
* `node.crt` - TLS certificates for each cluster node (the certificate corresponding to the key);
* `web.pem` - concatenation of the node's private key, node certificate, and Certificate Authority certificate for the monitoring HTTP interface (a separate file for each cluster server).

The required certificate generation parameters are determined by the organization's policy. Typically, certificates and keys for {{ ydb-short-name }} are generated with the following parameters:

* RSA keys of 2048 or 4096 bits;
* SHA-256 certificate signature algorithm with RSA encryption;
* node certificate validity period of at least 1 year;
* Certificate Authority certificate validity period of at least 3 years.

The Certificate Authority certificate must be marked appropriately: the CA flag must be set, and the key usages "Digital Signature, Non Repudiation, Key Encipherment, Certificate Sign" must be included.

Для сертификатов узлов важно соответствие фактического имени хоста (или имён хостов) значениям, указанным в поле "Subject Alternative Name". Для сертификатов должны быть включены виды использования "Digital Signature, Key Encipherment" и расширенные виды использования "TLS Web Server Authentication, TLS Web Client Authentication". Необходимо, чтобы сертификаты узлов поддерживали как серверную, так и клиентскую аутентификацию (опция `extendedKeyUsage = serverAuth,clientAuth` в настройках OpenSSL).

Для пакетной генерации или обновления сертификатов кластера {{ ydb-short-name }} с помощью программного обеспечения OpenSSL можно воспользоваться [примером скрипта](https://github.com/ydb-platform/ydb/blob/main/ydb/deploy/tls_cert_gen/), размещённым в репозитории {{ ydb-short-name }} на GitHub. Скрипт позволяет автоматически сформировать необходимые файлы ключей и сертификатов для всего набора узлов кластера за одну операцию, облегчая подготовку к установке.

## Создайте системного пользователя и группу, от имени которых будет работать {{ ydb-short-name }} {#create-user}

На каждом сервере, где будет запущен {{ ydb-short-name }}, выполните:


```bash
sudo groupadd ydb
sudo useradd -m -g ydb ydb
```


Для того, чтобы сервис {{ ydb-short-name }} имел доступ к блочным дискам для работы, необходимо добавить пользователя, под которым будут запущены процессы {{ ydb-short-name }}, в группу `disk`:


```bash
sudo usermod -aG disk ydb
```


## Настройте лимиты файловых дескрипторов {#file-descriptors}

Для корректной работы {{ ydb-short-name }}, особенно при использовании [спиллинга](../../../../concepts/query_execution/spilling.md) в многоузловых кластерах, рекомендуется увеличить лимит на количество одновременно открытых файловых дескрипторов.

Для изменения лимита файловых дескрипторов добавьте следующие строки в файл `/etc/security/limits.conf`:


```bash
ydb soft nofile 10000
ydb hard nofile 10000
```


Где `ydb` — имя пользователя, под которым запускается `ydbd`.

После изменения файла необходимо перезагрузить систему или заново залогиниться для применения новых лимитов.

{% note info %}

Для получения дополнительной информации о конфигурации спиллинга и его связи с файловыми дескрипторами см. раздел [«Конфигурация спиллинга»](../../../../reference/configuration/table_service_config.md#file-system-requirements).

{% endnote %}

## Установите программное обеспечение {{ ydb-short-name }} на каждом сервере {#install-binaries}

### Скачайте и распакуйте архив с исполняемым файлом `ydbd` и необходимыми для работы {{ ydb-short-name }} библиотеками

{% list tabs %}

- OSS

  ```bash
  mkdir ydbd-stable-linux-amd64
  curl -L <binaries_url> | tar -xz --strip-component=1 -C ydbd-stable-linux-amd64
  ```

- Enterprise

  ```bash
  mkdir ydbd-stable-linux-amd64
  curl -L <binaries_url> | tar -xJ --strip-component=1 -C ydbd-stable-linux-amd64
  ```

{% endlist %}

где `binaries_url` — ссылка на архив нужной вам версии со страницы [загрузок](../../../../downloads/index.md).

В командах выше: `-xz` — для архива `.tar.gz` (OSS), `-xJ` — для `.tar.xz` (Enterprise).

### Создайте на сервере директорию


```bash
sudo mkdir -p  /opt/ydb
```


### Скопируйте исполняемый файл и библиотеки в соответствующие директории


```bash
sudo cp -iR ydbd-stable-linux-amd64/bin /opt/ydb/
sudo cp -iR ydbd-stable-linux-amd64/lib /opt/ydb/
```


### Установите владельца файлов и каталогов


```bash
sudo chown -R root:bin /opt/ydb
```


## Подготовьте и очистите диски на каждом сервере {#prepare-disks}

{% include [_includes/storage-device-requirements.md](../../../../_includes/storage-device-requirements.md) %}

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

* фиксированный префикс, указывающий на тип устройства — например, `vd` в `vdb` (virtio), `sd` в `sda` (SATA/SCSI) или `nvme` в `nvme0n1` (NVMe);
* идентификатор устройства — буква или число (`b` в `vdb`, `a` в `sda`, `0n1` в `nvme0n1`);
* номер раздела — обычно число (`1` в `vdb1`).

В выводе `lsblk` выше системный диск — `vda`, диск для данных {{ ydb-short-name }} — `vdb`. В командах ниже используется `/dev/vdb`; на вашем сервере подставьте путь к целому диску без номера раздела (например, `/dev/sda` или `/dev/nvme0n1`).

1. Создайте разделы на выбранных дисках:

   {% note alert %}

   Следующая операция удалит все разделы на указанном диске! Убедитесь, что вы указали диск, на котором нет других данных!

   {% endnote %}


   ```bash
   DISK=/dev/vdb
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


The command output should contain the disks you created and partitioned.


```bash
lrwxrwxrwx 1 root root    10 Nov 26 12:54 ydb_disk_ssd_01 -> ../../vdb1
lrwxrwxrwx 1 root root    10 Nov 26 12:54 ydb_disk_ssd_02 -> ../../vdc1
lrwxrwxrwx 1 root root    10 Nov 26 12:54 ydb_disk_ssd_03 -> ../../vdd1
```


After completing the preparatory steps, you can proceed to deploying the system. Select the instructions according to your configuration:

* [Deploying a cluster using V1 configuration](deployment-configuration-v1.md)
* [Deploying a cluster using V2 configuration](deployment-configuration-v2.md)
