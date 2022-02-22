# Локальный запуск кластера {{ ydb-full-name }}

В данном разделе описывается процесс разворачивания локального кластера {{ ydb-full-name }} c использованием конфигурации в YAML формате.

{% note warning %}

При работе локальной базы данных, в зависимости от задач, может использоваться значительная часть вычислительных ресурсов хост-системы.

{% endnote %}

## Скачайте {{ ydb-short-name }} {#download-ydb}

Скачайте и распакуйте архив с исполняемым файлом `ydbd` и необходимыми для работы {{ ydb-short-name }} библиотеками, после чего перейдите в директорию с артефактами:

```bash
curl https://binaries.ydb.tech/ydbd-master-linux-amd64.tar.gz | tar -xz
cd ydbd-master-linux-amd64/
export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:`pwd`/lib"
```

## Подготовьте конфигурацию локального кластера {#prepare-configuration}

Подготовьте конфигурацию локального кластера, которую хотите развернуть. Для того, чтобы поднять кластер с хранением данных в памяти достаточно скопировать конфигурацию. Для разворачивания кластера с хранением данных в файле, необходимо дополнительно создать файл для хранения данных размером 64GB и указать путь до него в конфигурации.

{% list tabs %}
- В пямяти

  ```bash
  wget https://raw.githubusercontent.com/ydb-platform/ydb/main/ydb/deploy/yaml_config_examples/single-node-in-memory.yaml -O config.yaml
  ```

- В файле

  ```bash
  wget https://raw.githubusercontent.com/ydb-platform/ydb/main/ydb/deploy/yaml_config_examples/single-node-with-file.yaml -O config.yaml
  dd if=/dev/zero of=/tmp/ydb-local-pdisk.data bs=1 count=0 seek=68719476736
  sed -i "s/\/tmp\/pdisk\.data/\/tmp\/ydb-local-pdisk\.data/g" config.yaml
  ```

{% endlist %}

## Запустите статическую ноду кластера {#start-static-node}

Запустите статическую ноду кластера, выполнив команду:

```bash
./bin/ydbd server --yaml-config ./config.yaml --node 1 --grpc-port 2135 --ic-port 19001 --mon-port 8765
```

Проинициализируйте хранилище с помощью команды:

```bash
./bin/ydbd admin blobstorage config init --yaml-file ./config.yaml
```

## Создание первой базы данных {#create-db}

Для работы с таблицами необходимо создать как минимум одну базу данных и поднять процесс, обслуживающий эту базу данных. Для этого необходимо выполнить набор команд:

Создайте базу данных:

```bash
./bin/ydbd admin database /<domain-name>/<db-name> create <storage-pool-kind>:<storage-unit-count>
```

Например,

```bash
./bin/ydbd admin database /Root/test create ssd:1
```

Запустите процесс, обслуживающий базу данных:

```bash
./bin/ydbd server --yaml-config ./config.yaml --tenant /<domain-name>/<db-name> --node-broker <address>:<port> --grpc-port 31001 --ic-port 31003 --mon-port 31002
```

Например, чтобы запустить процесс для созданной выше базы, следует выполнить приведенную ниже команду.

```bash
./bin/ydbd server --yaml-config ./config.yaml --tenant /Root/test --node-broker localhost:2135 --grpc-port 31001 --ic-port 31003 --mon-port 31002
```


## Работа с базой данных через Web UI {#web-ui}

Чтобы посмотреть на структуру базы данных и выполнить YQL-запрос, воспользуйтесь встроенным в процесс `ydbd` веб-интерфейсом. Для этого откройте браузер и перейдите по адресу `http://localhost:8765`. Подробней возможности встроенного веб-интерфейса описаны в разделе [Embedded UI](../maintenance/embedded_monitoring/ydb_monitoring.md).
