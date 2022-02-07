# Локальный запуск кластера {{ ydb-full-name }}

В данном разделе описывается процесс разворачивания локального кластера {{ ydb-full-name }} c использованем конфигурации в YAML формате.

{% note warning %}

При работе локальной базы данных, в зависимости от задач, может использоваться значительная часть вычислительных ресурсов хост-системы.

{% endnote %}

## Запуск локального кластера

Cкачайте и распакуйте архив с исполняемым файлом `ydbd` и необходимыми для работы YDB библиотеками, после чего перейдите в директорию с артефактами:

```bash
curl https://binaries.ydb.tech/ydbd-master-linux-amd64.tar.gz | tar -xz
cd ydbd-master-linux-amd64/
sudo mkdir /lib/ydbd
sudo cp *.so /lib/ydbd/
```

Скопируйте конфигурацию:

```bash
wget https://raw.githubusercontent.com/ydb-platform/ydb/main/ydb/deploy/yaml_config_examples/single-node-in-memory.yaml -O config.yaml
```

Запустите статическую ноду кластера:

```bash
./ydbd server --yaml-config ./config.yaml --node 1 --grpc-port 2135 --ic-port 19001 --mon-port 8765
```

Проинициализируйте хранилище с помощью команды:

```bash
./ydbd admin blobstorage config init --yaml-file ./config.yaml
```

## Создание первой базы данных

Для работы с таблицами необходимо создать как минимум одну базу данных и поднять процесс, обслуживающий эту базу данных. Для этого необходимо выполнить набор команд:

Создайте базу данных:

```bash
./ydbd admin database /<domain-name>/<db-name> create <storage-pool-kind>:<storage-unit-count>
```

Например,

```bash
./ydbd admin database /Root/test create ssd:1
```

Запустите процесс, обслуживающий базу данных:

```bash
./ydbd server --yaml-config ./config.yaml --tenant /<domain-name>/<db-name> --node-broker <address>:<port> --grpc-port 31001 --ic-port 31003 --mon-port 31002
```
