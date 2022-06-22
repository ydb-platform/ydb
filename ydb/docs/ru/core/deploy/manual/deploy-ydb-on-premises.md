# Развертывание кластера {{ ydb-short-name }} на виртуальных или железных серверах

Данный документ описывает способ развернуть мультитенантный кластер {{ ydb-short-name }} на нескольких серверах.

## Перед началом работы {#before-start}

### Требования {#requirements}

У вас должен быть ssh доступ на все сервера. Это необходимо для установки артефактов и запуска бинарника {{ ydb-short-name }}.
Ваша сетевая конфигурация должна разрешать TCP соединения по следующим портам (по умолчанию):
 * 2135, 2136 - grpc для клиент-кластерного взаимодействия;
 * 19001, 19002 - Interconnect для внутрикластерного взаимодействия нод;
 * 8765, 8766 - http интерфейс для мониторинга кластера.

<!--Ознакомьтесь с [Production checklist](../production_checklist.md) и рекомендованной топологией кластера;-->

Выберите сервера и диски, которые будут использоваться для хранения данных:
* Используйте схему отказоустойчивости `block-4-2` для деплоя кластера в одной зоне доступности (AZ). Чтобы переживать выпадения 2 нод используйте не менее 8 нод.
* Используйте схему отказоустойчивости `mirror-3-dc` для деплоя кластера в трех зонах доступности (AZ). Чтобы переживать выпадения 1 AZ и 1 ноды в другом AZ используйте не менее 9 нод. Количество нод в каждой AZ должно быть одинаковым.

Запускайте каждую статическую ноду на отдельном сервере.

## Создайте системного пользователя и группу, от имени которого будет работать {{ ydb-short-name }} {#create-user}
На каждом сервере, где будет запущен {{ ydb-short-name }} выполните:
```bash
sudo groupadd ydb
sudo useradd ydb -g ydb
```

Для того, чтобы {{ ydb-short-name }} server имел доступ к блочным дискам для работы, необходимо добавить пользователя, под которым будет запущен процесс, в группу disk
 ```bash
sudo usermod -aG disk ydb
```

## Подготовьте и отформатируйте диски на каждом сервере {#prepare-disks}

{% note warning %}

Мы не рекомендуем использовать для хранения данных диски, которые используются другими процессами (в т.ч. операционной системой)

{% endnote %}

1\. Создайте раздел на выбранном диске

{% note alert %}

Будьте внимательны! Следующая операция удалит все разделы на указанных дисках!
Убедитесь, что вы указали диски, на которых нет других данных!

{% endnote %}

```bash
sudo parted /dev/nvme0n1 mklabel gpt -s
sudo parted -a optimal /dev/nvme0n1 mkpart primary 0% 100%
sudo parted /dev/nvme0n1 name 1 ydb_disk_ssd_01
sudo partx --u /dev/nvme0n1
```
После выполнения в системе появится диск с лейблом `/dev/disk/by-partlabel/ydb_disk_ssd_01`


Если вы планируете использовать более одного диска на каждом сервере - укажите для каждого свой уникальный лейбл вместо `ydb_disk_ssd_01`. Эти диски необходимо будет использовать в конфигурационных файлах далее.

2\. Скачайте и распакуйте архив с исполняемым файлом `ydbd` и необходимыми для работы {{ ydb-short-name }} библиотеками:

```bash
curl https://binaries.ydb.tech/ydbd-main-linux-amd64.tar.gz | tar -xz
```

3\. Создайте директории для запуска {{ ydb-short-name }}:

```bash
sudo mkdir -p /opt/ydb/bin /opt/ydb/cfg /opt/ydb/lib
sudo chown -R ydb:ydb /opt/ydb
```

4\. Скопируйте бинарник, библиотеки и конфигурационный файл в соответствующие директории:
```bash
sudo cp -i ydbd-main-linux-amd64/bin/ydbd /opt/ydb/bin/
sudo cp -i ydbd-main-linux-amd64/lib/libaio.so /opt/ydb/lib/
sudo cp -i ydbd-main-linux-amd64/lib/libiconv.so /opt/ydb/lib/
sudo cp -i ydbd-main-linux-amd64/lib/libidn.so /opt/ydb/lib/
```

5\. Отформатируйте диск встроенной командой
```bash
sudo LD_LIBRARY_PATH=/opt/ydb/lib /opt/ydb/bin/ydbd admin bs disk obliterate /dev/disk/by-partlabel/ydb_disk_ssd_01
```
Проделайте данную операцию для каждого диска, который будет использоваться для хранения данных.

## Выберите желаемый режим сетевого взаимодействия

Подготовьте конфигурационные файлы, в зависимости от выбранного режима сетевого взаимодействия:

{% list tabs %}

- Незащищенный режим

  В данном режиме трафик между нодами кластера, а также между клиентом и кластером использует нешифрованное соединение. Используйте данный режим для тестовых задач.

  {% include [prepare-configs.md](_includes/prepare-configs.md) %}

  Сохраните конфигурационный файл {{ ydb-short-name }} под именем `/opt/ydb/cfg/config.yaml`

- Защищенный режим

  В данном режиме трафик между нодами кластера, а также между клиентом и кластером шифруется протоколом TLS.

  {% include [generate-ssl.md](_includes/generate-ssl.md) %}

  Создайте на каждой ноде директирии для сертификатов
  ```bash
  sudo mkdir /opt/ydb/certs
  sudo chown -R ydb:ydb /opt/ydb/certs
  sudo chmod 0750 /opt/ydb/certs
  ```

  Скопируйте сертификаты и ключи ноды
  ```bash
  sudo -u ydb cp certs/ca.crt certs/node.crt certs/node.key /opt/ydb/certs/
  ```

  {% include [prepare-configs.md](_includes/prepare-configs.md) %}

  3\. В секциях `interconnect_config` и `grpc_config` укажите путь до сертификата, ключа и CA сертификаты:
  ```text
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
  ```
  Сохраните конфигурационный файл под именем `/opt/ydb/cfg/config.yaml`

{% endlist %}


## Запустите статические ноды {# start-storage}

{% list tabs %}

- Вручную
  1. Запустите на каждой ноде {{ ydb-short-name }} storage:
      ```bash
      sudo su - ydb
      cd /opt/ydb
      export LD_LIBRARY_PATH=/opt/ydb/lib
      /opt/ydb/bin/ydbd server --log-level 3 --syslog --tcp --yaml-config /opt/ydb/cfg/config.yaml  \
          --grpc-port 2135 --ic-port 19001 --mon-port 8765 --node static
      ```

- С использованием systemd
  1. Создайте на каждой ноде конфигурационный файл `/etc/systemd/system/ydbd-storage.service` со следующим содержимым
      ```text
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
          --yaml-config /opt/ydb/cfg/config.yaml \
          --grpc-port 2135 --ic-port 19001 --mon-port 8765 --node static
      LimitNOFILE=65536
      LimitCORE=0
      LimitMEMLOCK=3221225472

      [Install]
      WantedBy=multi-user.target
      ```

  2. Запустите на каждой ноде {{ ydb-short-name }} storage:
      ```bash
      sudo systemctl start ydbd-storage
      ```

{% endlist %}

## Инициализируйте кластер {#initialize-cluster}

На одной из нод кластера выполните команды:

```bash
export LD_LIBRARY_PATH=/opt/ydb/lib
/opt/ydb/bin/ydbd admin blobstorage config init --yaml-file  /opt/ydb/cfg/config.yaml
echo $?
```

Код завершения команды должен быть нулевым.

## Создание первой базы данных {#create-fist-db}

Для работы с таблицами необходимо создать как минимум одну базу данных и поднять процесс, обслуживающий эту базу данных (динамическую ноду).
```bash
export LD_LIBRARY_PATH=/opt/ydb/lib 
/opt/ydb/bin/ydbd admin database /Root/testdb create ssd:1
```

## Запустите динамическую ноду базы {#start-dynnode}

{% list tabs %}
- Вручную

  1. Запустите динамическую ноду {{ ydb-short-name }} для базы /Root/testdb:
      ```bash
      sudo su - ydb
      cd /opt/ydb
      export LD_LIBRARY_PATH=/opt/ydb/lib
      /opt/ydb/bin/ydbd server --grpc-port 2136 --ic-port 19002 --mon-port 8766 \
          --yaml-config  /opt/ydb/cfg/config.yaml --tenant /Root/testdb \
          --node-broker <node1.ydb.tech>:2135 \
          --node-broker <node2.ydb.tech>:2135 \
          --node-broker <node3.ydb.tech>:2135
      ```
      Где `<nodeN.ydb.tech>` - FQDN серверов, на которых запущены статические ноды.

  2. Запустите дополнительные динноды на других серверах для обеспечения доступности базы данных.

- С использованием systemd

  1. Создайте конфигурационный файл `/etc/systemd/system/ydbd-testdb.service` со следующим содержимым:
      ```text
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
      ExecStart=/opt/ydb/bin/ydbd server --grpc-port 2136 --ic-port 19002 --mon-port 8766 \
          --yaml-config  /opt/ydb/cfg/config.yaml --tenant /Root/testdb \
          --node-broker <node1.ydb.tech>:2135 \
          --node-broker <node2.ydb.tech>:2135 \
          --node-broker <node3.ydb.tech>:2135
      LimitNOFILE=65536
      LimitCORE=0
      LimitMEMLOCK=32212254720

      [Install]
      WantedBy=multi-user.target
      ```
      Где `<nodeN.ydb.tech>` - FQDN серверов, на которых запущены статические ноды.

  2. Запустите динамическую ноду {{ ydb-short-name }} для базы /Root/testdb:
      ```bash
      sudo systemctl start ydbd-testdb
      ```
  3. Запустите дополнительные динноды на других серверах для обеспечения доступности базы данных.

{% endlist %}

## Протестируйте работу с созданной базой {#try-first-db}

1. Установите YDB CLI, как описано в статье [Установка YDB CLI](../../reference/ydb-cli/install.md)
2. Создайте тестовую таблицу `test_table`:

   ```bash
   ydb -e grpc://<node.ydb.tech>:2136 -d /Root/testdb scripting yql \
   --script 'CREATE TABLE `testdir/test_table` (id Uint64, title Utf8, PRIMARY KEY (id));'
   ```
   Где `<node.ydb.tech>` - FQDN сервера, на котором запущена динамическая нода, обслуживающая базу `/Root/testdb`
