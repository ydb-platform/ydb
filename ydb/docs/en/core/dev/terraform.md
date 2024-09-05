# Managing {{ ydb-short-name }} using Terraform

[Terraform](https://www.terraform.io/) can create, delete, and modify the following objects inside a {{ ydb-short-name }} cluster:

* [tables](../concepts/datamodel/table.md)
* [indexes](../concepts/secondary_indexes.md) of tables
* [change data capture](../concepts/cdc.md) for tables
* [topics](../concepts/topic.md)

{% note warning %}

Currently, the {{ ydb-short-name }}  provider for Terraform is under development, and its functionality will be expanded.

{% endnote %}

To get started, you need to:

1. Deploy the [{{ ydb-short-name }}](../devops/index.md) cluster
2. Create a database (described in paragraph 1 for the appropriate type of cluster deployment)
3. Install [Terraform](https://developer.hashicorp.com/terraform/install)
4. Install and configure [Terraform provider for {{ ydb-short-name }}](https://github.com/ydb-platform/terraform-provider-ydb/)

## Configuring the Terraform provider to work with {{ ydb-short-name }} {#setup}

1. You need to download [provider code](https://github.com/ydb-platform/terraform-provider-ydb/)
2. Build the provider by executing `$make local-build` in the root directory of the provider's code. To do this, you need to additionally install the [make](https://www.gnu.org/software/make/) utility and [go](https://go.dev/)
The provider will be installed in the Terraform plugins folder - `~/.terraform.d/plugins/terraform.storage.ydb.tech/...`
3. Add the provider to `~/.terraformrc` by adding the following content to the `provider_installation` section (if there was no such section yet, then create):

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

4. Next, we configure the {{ ydb-short-name }} provider itself to work (for example, in the file `provider.tf` in the working directory):

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

Where:

* `token` - specifies the access token to the database if authentication is used, for example, using a third-party [IAM](../concepts/auth.md#iam) provider.
* `user` - the username for accessing the database in case of using authentication by [username and password](../concepts/auth.md#static-credentials)
* `password` - the password for accessing the database in case of using authentication by [username and password](../concepts/auth.md#static-credentials)

## Using the Terraform provider {{ ydb-short-name }} {#work-with-tf}

The following commands are used to apply changes to terraform resources:

1. `terraform init` - initialization of the terraform module (performed in the terraform resource directory).
2. `terraform validate` - checking the syntax of terraform resource configuration files.
3. `terraform apply` - direct application of the terraform resource configuration.

For ease of use, it is recommended to name terraform files as follows:

1. `provider.tf` - contains the settings of the terraform provider itself.
2. `main.tf` - contains a set of resources to create.

### Database connection {#connection_string}

For all resources describing data schema objects, you must specify the database details in which they are located. To do this, provide one of the two arguments:

* The connection string `connection_string` is an expression of the form `grpc(s)://HOST:PORT/?database=/database/path`, where `grpc(s)://HOST:PORT/` endpoint, and `/database/path` is the path of the database.
  For example, `grpcs://example.com:2135?database=/Root/testdb0`.
* `database_endpoint` - used when working with the [topics] resource (#topic_resource) (analogous to `connection_string` when working with table resources).

{% note info %}

The user can transfer the connection string to the database using standard Terraform tools - via [variables](https://developer.hashicorp.com/terraform/language/values/variables).

{% endnote %}

If you are using the creation of `ydb_table_changefeed` or `ydb_topic` resources and authorization is not enabled on the {{ ydb-short-name }} server, then in the DB config [config.yaml](../deploy/configuration/config.md) you need to specify:

```yaml
...
pqconfig:
  require_credentials_in_new_protocol: false
  check_acl: false
```

### Example of using all types of {{ ydb-short-name}} Terraform provider resources

This example combines all types of resources that are available in the {{ ydb-short-name }} Terraform provider:

```tf
variable "db-connect" {
  type = string
  default = "grpc(s)://HOST:PORT/?database=/database/path" # you need to specify the path to the database
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

  depends_on = [ydb_table.table] # link to the table creation resource
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

  depends_on = [ydb_table.table] # link to the table creation resource
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

All resources of the {{ ydb-short-name }} Terraform provider will be described in detail below.

### String table {#ydb-table}

{% note info %}

Working with column-oriented tables via Terraform is not yet available.

{% endnote %}

The `ydb_table` resource is used to work with tables.

Example:

```tf
  resource "ydb_table" "ydb_table" {
    path = "path/to/table" # path relative to the base root
    connection_string = "grpc(s)://HOST:PORT/?database=/database/path" #DB connection example
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

The following arguments are supported:

* `path` - (required) is the path of the table relative to the root of the database (example - `/path/to/table`).
* `connection_string` — (required) [connection string](#connection_string).

* `column` — (required) column properties (see the [column](#column) argument).
* `family` - (optional) is a column group (see the [family](#family) argument).
* `primary_key` — (required) [primary key](../yql/reference/syntax/create_table.md#columns) of the table that contains an ordered list of column names of the primary key.
* `ttl` — (optional) TTL (see the [ttl](#ttl) argument).
* `partitioning_settings` — (optional) partitioning settings (see the argument [partitioning_settings](#partitioning-settings)).
* `key_bloom_filter` — (optional) (bool) use [Bloom filter for primary key](../concepts/datamodel/table.md#bloom-filter), the default value is false.
* `read_replicas_settings` — (optional) settings for [read replicas](../concepts/datamodel/table.md#read_only_replicas).

#### column {#column}

The `column` argument describes the [column properties](../yql/reference/syntax/create_table.md#columns) of the table.

{% note warning %}

Using Terraform, you cannot only add columns but not delete them. To delete a column, use the {{ ydb-short-name }} tools, then delete the column from the resource description. When trying to apply changes to the table's columns (changing the data type or name), Terraform will not try to delete them but will try to do an update-in-place, though the changes will not be applied.

{% endnote %}

Example:

```tf
column {
  name     = "column_name"
  type     = "Utf8"
  family   = "some_family"
  not_null = true
}
```

* `name` - (required) is the column's name.
* `type` — (required) [YQL data type](../yql/reference/types/primitive.html) columns. Simple column types are allowed. However, container types cannot be used as data types of table columns.
* `family` - (optional) is the name of the column group (see the [family](#family) argument).
* `not_null` — (optional) column cannot contain `NULL`. The default value: `false`.

#### family {#family}

The `family` argument describes [column group properties](../yql/reference/syntax/create_table.md#column-family).

* `name` - (required) is the name of the column group.
* `data` — (required) [storage device type](../yql/reference/syntax/create_table#column-family) for column data of this group.
* `compression` — (required) [data compression codec](../yql/reference/syntax/create_table#column-family).

Example:

```tf
family {
  name        = "my_family"
  data        = "ssd"
  compression = "lz4"
}
```

#### partitioning_settings {#partitioning-settings}

The `partitioning_settings` argument describes [partitioning settings](../concepts/datamodel/table.md#partitioning).

Example:

```tf
partitioning_settings {
  auto_partitioning_min_partitions_count = 5
  auto_partitioning_max_partitions_count = 8
  auto_partitioning_partition_size_mb    = 256
  auto_partitioning_by_load              = true
}
```

* `uniform_partitions` — (optional) the number of [preallocated partitions](../concepts/datamodel/table.md#uniform_partitions).
* `partition_at_keys` — (optional) [partitioning by primary key](../concepts/datamodel/table.md#partition_at_keys).
* `auto_partitioning_min_partitions_count` — (optional) [minimum possible number of partitions](../concepts/datamodel/table.md#auto_partitioning_min_partitions_count) when auto-partitioning.
* `auto_partitioning_max_partitions_count` — (optional) [maximum possible number of partitions](../concepts/datamodel/table.md#auto_partitioning_max_partitions_count) when auto-partitioning.
* `auto_partitioning_partition_size_mb` — (optional) setting the value of [auto-partitioning by size](../concepts/datamodel/table.md#auto_partitioning_partition_size_mb) in megabytes.
* `auto_partitioning_by_size_enabled` — (optional) enabling auto-partitioning by size (bool), enabled by default (true).
* `auto_partitioning_by_load` — (optional) enabling [autopartition by load](../concepts/datamodel/table.md#auto_partitioning_by_load) (bool), disabled by default (false).

See the links above for more information about the parameters and their default values.

#### ttl {#ttl}

The `ttl` argument describes the [Time To Live](../concepts/ttl.md) settings.

Example:

```tf
ttl {
  column_name     = "column_name"
  expire_interval = "PT1H" # 1 hour
  unit = "seconds" # for numeric column types (non-ISO8601)
}
```

* `column_name` - (required) is the column name for TTL.
* `expire_interval` — (required) interval in [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) format (for example, `P1D` is an interval of 1 day, that is, 24 hours).
* `unit` — (optional) is set if the column with ttl has a [numeric type](../yql/reference/types/primitive.md#numeric). Supported values:
  * `seconds`
  * `milliseconds`
  * `microseconds`
  * `nanoseconds`

### Secondary index of the table {#ydb-table-index}

The [ydb_table_index](../concepts/secondary_indexes.md) resource is used to work with a table index.

Example:

```tf
resource "ydb_table_index" "ydb_table_index" {
  table_path = "path/to/table" # path relative to the base root
  connection_string = "grpc(s)://HOST:PORT/?database=/database/path" #DB connection example
  name              = "my_index"
  type              = "global_sync" # "global_async"
  columns           = ["a", "b"]
  cover             = ["c"]
}
```

The following arguments are supported:

* `table_path` - is the path of the table. Specified if `table_id` is not specified.
* `connection_string` — [connection string](#connection_string). Specified if `table_id` is not specified.
* `table_id` - terraform-table identifier. Specify if `table_path` or `connection_string` is not specified.

* `name` - (required) is the name of the index.
* `type` - (required) is the index type [global_sync | global_async](../yql/reference/syntax/create_table.md#secondary_index).
* `columns` - (required) is an ordered list of column names participating in the index.
* `cover` - (required) is a list of additional columns for the covering index.

### Change Data Capture {#ydb-table-changefeed}

The `ydb_table_changefeed` resource is used to work with the [change data capture](../concepts/cdc.md) of the table.

Example:

```tf
resource "ydb_table_changefeed" "ydb_table_changefeed" {
  table_id = ydb_table.ydb_table.id
  name     = "changefeed"
  mode     = "NEW_IMAGE"
  format   = "JSON"
}
```

The following arguments are supported:

* `table_path` - is the path of the table. Specified if `table_id` is not specified.
* `connection_string` — [connection string](#connection_string). Specified if `table_id` is not specified.
* `table_id` — terraform-table identifier. Specify if `table_path` or `connection_string` is not specified.

* `name` - (required) is the name of the change stream.
* `mode` - (required) is the mode of operation of the [change data capture](../yql/reference/syntax/alter_table#changefeed-options).
* `format` - (required) is the format of the [change data capture](../yql/reference/syntax/alter_table#changefeed-options).
* `virtual_timestamps` — (optional) using [virtual timestamps](../concepts/cdc.md#virtual-timestamps).
* `retention_period` — (optional) data storage time in [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) format.
* `consumer` - (optional) is a reader of the change data capture (see the argument [#consumer](#consumer)).

#### consumer {#consumer}

The `consumer` argument describes the [reader](cdc.md#read) of the change data capture.

* `name` - (required) is the reader's name.
* `supported_codecs` — (optional) supported data codec.
* `starting_message_timestamp_ms` — (optional) timestamp in [UNIX timestamp](https://en.wikipedia.org/wiki/Unix_time) format in milliseconds, from which the reader will start reading the data.

### Usage examples {manage-examples}

#### Creating a table in an existing database {#example-with-connection-string}

```tf
resource "ydb_table" "ydb_table" {
  # Path to the table
  path = "path/to/table" # path relative to the base root

  connection_string = "grpc(s)://HOST:PORT/?database=/database/path" #DB connection example

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
  # Primary key
  primary_key = [
    "a", "b"
  ]
}
```

#### Creating a table, index, and change stream {#example-with-table}

```tf
resource "ydb_table" "ydb_table" {
  # Path to the table
  path = "path/to/table" # path relative to the base root
  
  # ConnectionString to the database.
  connection_string = "grpc(s)://HOST:PORT/?database=/database/path" #DB connection example

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

  # Primary key
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

  depends_on = [ydb_table.ydb_table] # link to the table creation resource
}

resource "ydb_table_index" "ydb_table_index" {
  table_id = ydb_table.ydb_table.id
  name = "some_index"
  columns = ["c", "d"]
  cover = ["e"]
  type = "global_sync"

  depends_on = [ydb_table.ydb_table] # link to the table creation resource
}
```

## Topic configuration management {{ ydb-short-name }} via Terraform

The `ydb_topic` resource is used to work with [topics](../concepts/topic.md)

{% note info %}

The topic cannot be created in the root of the database; you need to specify at least one directory in the name of the topic. When trying to create a topic in the root of the database, the provider will return an error.

{% endnote %}

### Description of the `ydb_topic` resource {#topic_resource}

Example:

```tf
resource "ydb_topic" "ydb_topic" {
  database_endpoint = "grpcs://example.com:2135/?database=/Root/testdb0" #database connection example
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

The following arguments are supported:

* `name` - (required) is the name of the topic.
* `database_endpoint` - (required) is the full path to the database, for example: `"grpcs://example.com:2135/?database=/Root/testdb0"`; analogous to `connection_string` for tables.
* `retention_period_ms` - the duration of data storage in milliseconds; the default value is `86400000` (day).
* `partitions_count` - the number of partitions; the default value is `2`.
* `supported_codecs` - supported data compression codecs, the default value is `"gzip", "raw", "zstd"`.
* `consumer` - readers for the topic.

Description of the data consumer `consumer`:

* `name` - (required) is the reader's name.
* `supported_codecs` - supported data compression encodings, by default - `"gzip", "raw", "zstd"`.
* `starting_message_timestamp_ms` - timestamp in [UNIX timestamp](https://en.wikipedia.org/wiki/Unix_time) format in milliseconds, from which the reader will start reading the data; the default value is  0, which means "from the beginning".
