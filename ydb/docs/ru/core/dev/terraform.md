# Управление {{ ydb-short-name }} с помощью Terraform

С помощью [Terraform](https://www.terraform.io/) можно создавать, удалять и изменять следующие объекты внутри кластера {{ ydb-short-name }}:

* [строковые](../concepts/datamodel/table.md#row-oriented-tables) таблицы;
* [вторичные индексы](../concepts/secondary_indexes.md) для строковых таблиц;
* [потоки изменений](../concepts/cdc.md) строковых таблиц;
* [топики](../concepts/topic.md).

{% note warning %}

На данный момент {{ ydb-short-name }} провайдер для Terraform находится на стадии разработки и его функциональность будет расширяться.

{% endnote %}

Для начала работы необходимо:

1. Развернуть кластер [YDB](../devops/index.md)
2. Создать базу данных (описано в п.1 для соответствующего типа развертывания кластера)
3. Установить [Terraform](https://developer.hashicorp.com/terraform/install)
4. Установить и настроить [Terraform provider for {{ ydb-short-name }}](https://github.com/ydb-platform/terraform-provider-ydb/)

## Настройка Terraform провайдера для работы с {{ ydb-short-name }} {#setup}

1. Нужно скачать [код провайдера](https://github.com/ydb-platform/terraform-provider-ydb/)
2. Собрать провайдер, выполнив `$ make local-build` в корневой директории кода провайдера. Для этого вам необходимо дополнительно установить утилиту [make](https://www.gnu.org/software/make/) и [go](https://go.dev/)
Провайдер установится в папку плагинов Terraform - `~/.terraform.d/plugins/terraform.storage.ydb.tech/...`
3. Добавить провайдер в `~/.terraformrc`, дописав в секцию `provider_installation` следующее содержание (если такой секции ещё не было, то создать):

    ```tf
    provider_installation {
      direct {
        exclude = ["terraform.storage.ydb.tech/*/*"]
      }

      filesystem_mirror {
        path    = "/PATH_TO_HOME/.terraform.d/plugins"
        include = ["terraform.storage.ydb.tech/*/*"]
      }
    }
    ```

4. Далее настраиваем сам {{ ydb-short-name }} провайдер для работы (например в файле `provider.tf` в рабочей директории):

    ```tf
    terraform {
      required_providers {
        ydb = {
          source = "terraform.storage.ydb.tech/provider/ydb"
        }
      }
      required_version = ">= 0.13"
    }
    
    provider "ydb" {
      token = "<TOKEN>"
      //OR for static credentials
      user = "<USER>"
      password = "<PASSWORD>"
    }
    ```

Где:

* `token` - указывается токен доступа к БД, если используется аутентификация, например, с использованием стороннего [IAM](../concepts/auth.md#iam) провайдера.
* `user` - имя пользователя для доступа к базе данных в случае использования аутентификации по [логину и паролю](../concepts/auth.md#static-credentials)
* `password` - пароль для доступа к базе данных в случае использования аутентификации по [логину и паролю](../concepts/auth.md#static-credentials)

## Использование Terraform провайдера {{ ydb-short-name }} {#work-with-tf}

Для применения изменений в terraform ресурсах используются следующие команды:

1. `terraform init` - инициализация модуля terraform (выполняется в директории ресурсов terraform).
2. `terraform validate` - проверка синтаксиса конфигурационных файлов ресурсов terraform.
3. `terraform apply` - непосредственное применение конфигурации ресурсов terraform.

Для удобства использования файлы terraform рекомендуется именовать следующим образом:

1. `provider.tf` - содержит настройки самого terraform провайдера.
2. `main.tf` - содержит набор ресурсов для создания.

### Соединение с базой данных (БД) {#connection_string}

Для всех ресурсов, описывающих объекты схемы данных, необходимо задать реквизиты БД, в которой они размещаются. Для этого укажите один из двух аргументов:

* Строка соединения `connection_string` — выражение вида `grpc(s)://HOST:PORT/?database=/database/path`, где `grpc(s)://HOST:PORT/` эндпоинт, а `/database/path` — путь БД.
  Например, `grpcs://example.com:2135?database=/Root/testdb0`.
* `database_endpoint` - используется при работе с ресурсом [топиков](#topic_resource) (аналог `connection_string` при работе с ресурсами строковых таблиц).

{% note info %}

Если требуется, пользователь может передавать строку соединения с БД стандартными средствами Terraform - через [variables](https://developer.hashicorp.com/terraform/language/values/variables).

{% endnote %}

Если вы используете создание ресурсов `ydb_table_changefeed` или `ydb_topic` и на сервере {{ ydb-short-name }} не включена авторизация, то в конфиге БД [config.yaml](../deploy/configuration/config.md) нужно указать:

```yaml
...
pqconfig:
  require_credentials_in_new_protocol: false
  check_acl: false
```

### Пример использования всех типов ресурсов {{ ydb-short-name }} Terraform провайдера

Данный пример объединяет все типы ресурсов, которые доступны в {{ ydb-short-name }} Terraform провайдере:

```tf
variable "db-connect" {
  type = string
  default = "grpc(s)://HOST:PORT/?database=/database/path" # нужно указать путь к базе данных
}

resource "ydb_table" "table" {
  path        = "1/2/3/tftest"
  connection_string = var.db-connect
  column {
    name = "a"
    type = "Utf8"
  }
  column {
    name = "b"
    type = "String"
  }
  column {
    name = "ttlBase"
    type = "Uint32"
  }
  ttl {
    column_name = "ttlBase"
    expire_interval = "P7D"
    unit = "milliseconds"
  }

  primary_key = ["b", "a"]

  partitioning_settings {
    auto_partitioning_min_partitions_count = 5
    auto_partitioning_max_partitions_count = 8
    auto_partitioning_partition_size_mb    = 256
    auto_partitioning_by_load              = true
  }
}

resource "ydb_table_index" "table_index" {
  table_path        = ydb_table.table.path
  connection_string = ydb_table.table.connection_string
  name              = "my_index"
  type              = "global_sync" # "global_async"
  columns           = ["a", "b"]

  depends_on = [ydb_table.table] # ссылка на ресурс создания строковой таблицы
}

resource "ydb_table_changefeed" "table_changefeed" {
  table_id = ydb_table.table.id
  name     = "changefeed"
  mode     = "NEW_IMAGE"
  format   = "JSON"
  consumer {
    name = "test"
    supported_codecs = ["raw", "gzip"]
  }

  depends_on = [ydb_table.table] # ссылка на ресурс создания строковой таблицы
}

resource "ydb_topic" "test" {
  database_endpoint = ydb_table.table.connection_string
  name              = "1/2/test"
  supported_codecs  = ["zstd"]

  consumer {
    name             = "test-consumer3"
    starting_message_timestamp_ms = 0
    supported_codecs = ["zstd","raw"]
  }

  consumer {
    name             = "test-consumer1"
    starting_message_timestamp_ms = 2000
    supported_codecs = ["zstd"]
  }

  consumer {
    name             = "test-consumer2"
    starting_message_timestamp_ms = 0
    supported_codecs = ["zstd"]
  }
}
```

Ниже будут подробно расписаны все ресурсы {{ ydb-short-name }} Terraform провайдера

### Строковая таблица {#ydb-table}

{% include [not_allow_for_olap](../_includes/not_allow_for_olap_note_main.md) %}

Для работы со строковыми таблицами используется ресурс `ydb_table`.

Пример:

```tf
  resource "ydb_table" "ydb_table" {
    path = "path/to/table" # путь относительно корня базы
    connection_string = "grpc(s)://HOST:PORT/?database=/database/path" #пример подключения к БД
    column {
      name = "a"
      type = "Utf8"
      not_null = true
    }
    column {
      name = "b"
      type = "Uint32"
      not_null = true
    }
    column {
      name = "c"
      type = String
      not_null = false
    }
    column {
      name = "f"
      type = "Utf8"
    }
    column {
      name = "e"
      type = "String"
    }
    column {
      name = "d"
      type = "Timestamp"
    }
    primary_key = ["b", "a"]
  }
```

Поддерживаются следующие аргументы:

* `path` — (обязательный) путь строковой таблицы, относительно корня базы (пример - `/path/to/table`).
* `connection_string` — (обязательный) [строка соединения](#connection_string).

* `column` — (обязательный) свойства колонки (см. аргумент [column](#column)).
* `family` — (необязательный) группа колонок (см. аргумент [family](#family)).
* `primary_key` — (обязательный) [первичный ключ](../yql/reference/syntax/create_table.md#columns) строковой таблицы, содержит упорядоченный список имён колонок первичного ключа.
* `ttl` — (необязательный) TTL (см. аргумент [ttl](#ttl)).
* `partitioning_settings` — (необязательный) настройки партицирования (см. аргумент [partitioning_settings](#partitioning-settings)).
* `key_bloom_filter` — (необязательный) (bool) использовать [фильтра Блума для первичного ключа](../concepts/datamodel/table.md#bloom-filter), значение по умолчанию - false.
* `read_replicas_settings` — (необязательный) настройки [реплик для чтения](../concepts/datamodel/table.md#read_only_replicas).

#### column {#column}

Аргумент `column` описывает [свойства колонки](../yql/reference/syntax/create_table.md#columns) строковой таблицы.

{% note warning %}

При помощи Terraform нельзя удалить колонку, можно только добавить. Чтобы удалить колонку, используйте средства {{ ydb-short-name }}, затем удалите колонку из описания ресурса. При попытке "прокатки" изменений колонок строковой таблицы (смена типа, имени), Terraform не попытается их удалить, а попытается сделать update-in-place, но изменения применены не будут.

{% endnote %}

Пример:

```tf
column {
  name     = "column_name"
  type     = "Utf8"
  family   = "some_family"
  not_null = true
}
```

* `name` — (обязательный) имя колонки.
* `type` — (обязательный) [тип данных YQL](../yql/reference/types/primitive.md) колонки. Допускается использовать простые типы колонок. Как пример, контейнерные типы не могут быть использованы в качестве типов данных колонок строковых таблиц.
* `family` — (необязательный) имя группы колонок (см. аргумент [family](#family)).
* `not_null` — (необязательный) колонка не может содержать `NULL`. Значение по умолчанию: `false`.

#### family {#family}

Аргумент `family` описывает [свойства группы колонок](../yql/reference/syntax/create_table.md#column-family).

* `name` — (обязательный) имя группы колонок.
* `data` — (обязательный) [тип устройства хранения](../yql/reference/syntax/create_table#column-family) для данных колонок этой группы.
* `compression` — (обязательный) [кодек сжатия данных](../yql/reference/syntax/create_table#column-family).

Пример:

```tf
family {
  name        = "my_family"
  data        = "ssd"
  compression = "lz4"
}
```

#### partitioning_settings {#partitioning-settings}

Аргумент `partitioning_settings` описывает [настройки партицирования](../concepts/datamodel/table.md#partitioning).

Пример:

```tf
partitioning_settings {
  auto_partitioning_min_partitions_count = 5
  auto_partitioning_max_partitions_count = 8
  auto_partitioning_partition_size_mb    = 256
  auto_partitioning_by_load              = true
}
```

* `uniform_partitions` — (необязательный) количество [заранее аллоцированных партиций](../concepts/datamodel/table.md#uniform_partitions).
* `partition_at_keys` — (необязательный) [партицирование по первичному ключу](../concepts/datamodel/table.md#partition_at_keys).
* `auto_partitioning_min_partitions_count` — (необязательный) [минимально возможное количество партиций](../concepts/datamodel/table.md#auto_partitioning_min_partitions_count) при автопартицировании.
* `auto_partitioning_max_partitions_count` — (необязательный) [максимально возможное количество партиций](../concepts/datamodel/table.md#auto_partitioning_max_partitions_count) при автопартицировании.
* `auto_partitioning_partition_size_mb` — (необязательный) задание значения [автопартицирования по размеру](../concepts/datamodel/table.md#auto_partitioning_partition_size_mb) в мегабайтах.
* `auto_partitioning_by_size_enabled` — (необязательный) включение автопартиционирования по размеру (bool), по умолчанию - включено (true).
* `auto_partitioning_by_load` — (необязательный) включение [автопартицирования по нагрузке](../concepts/datamodel/table.md#auto_partitioning_by_load) (bool), по умолчанию - выключено (false).

Подробнее про параметры и их значения по умолчанию написано по ссылкам выше.

#### ttl {#ttl}

Аргумент `ttl` описывает настройки [Time To Live](../concepts/ttl.md).

Пример:

```tf
ttl {
  column_name     = "column_name"
  expire_interval = "PT1H" # 1 час
  unit            = "seconds" # для числовых типов колонок (non ISO 8601)
}
```

* `column_name` — (обязательный) имя колонки для TTL.
* `expire_interval` — (обязательный) интервал в формате [ISO 8601](https://ru.wikipedia.org/wiki/ISO_8601) (например, `P1D` — интервал длиной в 1 сутки, то есть 24 часа).
* `unit` — (необязательный) задается в случае, если колонка с ttl имеет [числовой тип](../yql/reference/types/primitive.md#numeric). Поддерживаемые значения:
  * `seconds`
  * `milliseconds`
  * `microseconds`
  * `nanoseconds`

### Вторичный индекс строковой таблицы {#ydb-table-index}

Для работы с индексом строковой таблицы используется ресурс [ydb_table_index](../concepts/secondary_indexes.md).

Пример:

```tf
resource "ydb_table_index" "ydb_table_index" {
  table_path        = "path/to/table" # путь относительно корня базы
  connection_string = "grpc(s)://HOST:PORT/?database=/database/path" #пример подключения к БД
  name              = "my_index"
  type              = "global_sync" # "global_async"
  columns           = ["a", "b"]
  cover             = ["c"]
}
```

Поддерживаются следующие аргументы:

* `table_path` — путь строковой таблицы. Указывается, если не задан `table_id`.
* `connection_string` — [строка соединения](#connection_string). Указывается, если не задан `table_id`.
* `table_id` - terraform-идентификатор строковой таблицы. Указывается, если не задан `table_path` или `connection_string`.

* `name` — (обязательный) имя индекса.
* `type` — (обязательный) тип индекса [global_sync | global_async](../yql/reference/syntax/create_table.md#secondary_index).
* `columns` — (обязательный) упорядоченный список имён колонок, участвующий в индексе.
* `cover` — (обязательный) список дополнительных колонок для покрывающего индекса.

### Поток изменений строковой таблицы {#ydb-table-changefeed}

{% include [not_allow_for_olap](../_includes/not_allow_for_olap_note_main.md) %}

Для работы с [потоком изменений](../concepts/cdc.md) строковой таблицы используется ресурс `ydb_table_changefeed`.

Пример:

```tf
resource "ydb_table_changefeed" "ydb_table_changefeed" {
  table_id = ydb_table.ydb_table.id
  name     = "changefeed"
  mode     = "NEW_IMAGE"
  format   = "JSON"
}
```

Поддерживаются следующие аргументы:

* `table_path` — путь строковой таблицы. Указывается, если не задан `table_id`.
* `connection_string` — [строка соединения](#connection_string). Указывается, если не задан `table_id`.
* `table_id` — terraform-идентификатор строковой таблицы. Указывается, если не задан `table_path` или `connection_string`.

* `name` — (обязательный) имя потока изменений.
* `mode` — (обязательный) режим работы [потока изменений](../yql/reference/syntax/alter_table#changefeed-options).
* `format` — (обязательный) формат [потока изменений](../yql/reference/syntax/alter_table#changefeed-options).
* `virtual_timestamps` — (необязательный) использование [виртуальных таймстеймпов](../concepts/cdc.md#virtual-timestamps).
* `retention_period` — (необязательный) время хранения данных в формате [ISO 8601](https://ru.wikipedia.org/wiki/ISO_8601).
* `consumer` — (необязательный) читатель потока изменений (см. аргумент [#consumer](#consumer)).

#### consumer {#consumer}

Аргумент `consumer` описывает [читателя](cdc.md#read) потока изменений.

* `name` — (обязательный) имя читателя.
* `supported_codecs` — (необязательный) поддерживаемые кодек данных.
* `starting_message_timestamp_ms` — (необязательный) временная метка в формате [UNIX timestamp](https://ru.wikipedia.org/wiki/Unix-время) в миллисекундах, с которой читатель начнет читать данные.

### Примеры использования {manage-examples}

#### Создание строковой таблицы в существующей БД {#example-with-connection-string}

```tf
resource "ydb_table" "ydb_table" {
  # Путь до строковой таблицы
  path = "path/to/table" # путь относительно корня базы

  connection_string = "grpc(s)://HOST:PORT/?database=/database/path" #пример подключения к БД

  column {
    name = "a"
    type = "Uint64"
    not_null = true
  }
  column {
    name     = "b"
    type     = "Uint32"
    not_null = true
  }
  column {
    name = "c"
    type = String
    not_null = false
  }
  column {
    name = "f"
    type = "Utf8"
  }
  column {
    name = "e"
    type = "String"
  }
  column {
    name = "d"
    type = "Timestamp"
  }
  # Первичный ключ
  primary_key = [
    "a", "b"
  ]
}
```

#### Создание строковой таблицы, индекса и потока изменений {#example-with-table}

```tf
resource "ydb_table" "ydb_table" {
  # Путь до строковой таблицы
  path = "path/to/table" # путь относительно корня базы
  
  # ConnectionString до базы данных.
  connection_string = "grpc(s)://HOST:PORT/?database=/database/path" #пример подключения к БД

  column {
    name = "a"
    type = "Uint64"
    not_null = true
  }
  column {
    name     = "b"
    type     = "Uint32"
    not_null = true
  }
  column {
    name = "c"
    type = "Utf8"
  }
  column {
    name = "f"
    type = "Utf8"
  }
  column {
    name = "e"
    type = "String"
  }
  column {
    name = "d"
    type = "Timestamp"
  }

  # Первичный ключ
  primary_key = [
    "a", "b"
  ]


  ttl {
    column_name     = "d"
    expire_interval = "PT5S"
  }

  partitioning_settings {
    auto_partitioning_by_load = false
    auto_partitioning_partition_size_mb    = 256
    auto_partitioning_min_partitions_count = 6
    auto_partitioning_max_partitions_count = 8
  }

  read_replicas_settings = "PER_AZ:1"

  key_bloom_filter = true # Default = false
}

resource "ydb_table_changefeed" "ydb_table_changefeed" {
  table_id = ydb_table.ydb_table.id
  name = "changefeed"
  mode = "NEW_IMAGE"
  format = "JSON"

  consumer {
    name = "test_consumer"
  }

  depends_on = [ydb_table.ydb_table] # ссылка на ресурс создания строковой таблицы
}

resource "ydb_table_index" "ydb_table_index" {
  table_id = ydb_table.ydb_table.id
  name = "some_index"
  columns = ["c", "d"]
  cover = ["e"]
  type = "global_sync"

  depends_on = [ydb_table.ydb_table] # ссылка на ресурс создания строковой таблицы
}
```

## Управление конфигурацией топиков {{ydb-short-name}} через Terraform

Для работы с [топиками](../concepts/topic.md) используется ресурс `ydb_topic`

{% note info %}

Топик не может быть создан в корне БД, нужно указать хотя бы один каталог в имени топика. При попытке создать топик в корне БД - провайдер выдаст ошибку.

{% endnote %}

### Описание ресурса `ydb_topic` {#topic_resource}

Пример:

```tf
resource "ydb_topic" "ydb_topic" {
  database_endpoint = "grpcs://example.com:2135/?database=/Root/testdb0" #пример подключения к БД
  name              = "test/test1"
  supported_codecs  = ["zstd"]

  consumer {
    name             = "test-consumer1"
    starting_message_timestamp_ms = 0
    supported_codecs = ["zstd","raw"]
  }

  consumer {
    name             = "test-consumer2"
    starting_message_timestamp_ms = 2000
    supported_codecs = ["zstd"]
  }

  consumer {
    name             = "test-consumer3"
    starting_message_timestamp_ms = 0
    supported_codecs = ["zstd"]
  }
}
```

Поддерживаются следующие аргументы:

* `name` - (обязательный) имя топика.
* `database_endpoint` - (обязательный) полный путь до базы данных, например: `"grpcs://example.com:2135/?database=/Root/testdb0"`; аналог `connection_string` для строковых таблиц.
* `retention_period_ms` - длительность хранения данных в миллисекундах, значение по умолчанию - `86400000` (сутки).
* `partitions_count` - количество партиций, значение по умолчанию - `2`.
* `supported_codecs` - поддерживаемые кодеки сжатия данных, значение по умолчанию - `"gzip", "raw", "zstd"`.
* `consumer` - читатели для топика.

Описание потребителя данных `consumer`:

* `name` - (обязательное) имя читателя.
* `supported_codecs` - поддерживаемые кодировки сжатия данных, по умолчанию - `"gzip", "raw", "zstd"`.
* `starting_message_timestamp_ms` - временная метка в формате [UNIX timestamp](https://ru.wikipedia.org/wiki/Unix-время) в миллисекундах, с которой читатель начнёт читать данные, по умаолчанию - 0, то есть с начала поставки.
