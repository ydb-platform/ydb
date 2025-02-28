# File structure of an export

The file structure outlined below is used to export data both to the file system and an S3-compatible object storage. When working with S3, the file path is added to the object key, and the key's prefix specifies the export directory.

## Cluster {#cluster}

{% note info %}

Cluster export is available only to the file system.

{% endnote %}

A cluster corresponds to a directory in the file structure, which contains:

- Directories describing [databases](#db) in the cluster, except:
  - Database schema objects.
  - Database users and groups that are not administrators.
- The file `permissions.pb`, which describes the cluster root ACL and its owner in [text protobuf](https://developers.google.com/protocol-buffers/docs/reference/cpp/google.protobuf.text_format) format.
- The file `create_user.sql`, which describes the cluster users in `SQL` format.
- The file `create_group.sql`, which describes the cluster groups in `SQL` format.
- The file `alter_group.sql`, which describes user membership in the cluster groups in `SQL` format.

## Database {#db}

{% note info %}

Export to an S3-compatible object storage exporting only the database schema objects.

{% endnote %}

A database corresponds to a directory in the file structure, which contains:

- Directories describing the database schema objects, for example, [tables](#tables).
- The file `database.pb`, which describes the database settings in [text protobuf](https://developers.google.com/protocol-buffers/docs/reference/cpp/google.protobuf.text_format) format.
- The file `permissions.pb`, which describes the database ACL and its owner in [text protobuf](https://developers.google.com/protocol-buffers/docs/reference/cpp/google.protobuf.text_format) format.
- The file `create_user.sql`, which describes the database users in `SQL` format.
- The file `create_group.sql`, which describes the database groups in `SQL` format.
- The file `alter_group.sql`, which describes user membership in the database groups in `SQL` format.

## Directories {#dir}

Each database directory has a corresponding directory in the file structure. Each of them includes a `permissions.pb` file, which describes the directory ACL and owner in the [text protobuf](https://developers.google.com/protocol-buffers/docs/reference/cpp/google.protobuf.text_format) format. The directory hierarchy in the file structure mirrors the hierarchy in the database. If a database directory contains no items (neither tables nor subdirectories), directory in the file structure includes an empty file named `empty_dir`.

## Tables {#tables}

For each table in the database, there's a same-name directory in the file structure's directory hierarchy that includes:

- The `scheme.pb` file describing the table structure and parameters in the [text protobuf](https://developers.google.com/protocol-buffers/docs/reference/cpp/google.protobuf.text_format) format.
- The `permissions.pb` file describes the table ACL and owner in the [text protobuf](https://developers.google.com/protocol-buffers/docs/reference/cpp/google.protobuf.text_format) format.
- One or more `data_XX.csv` files with the table data in `csv` format, where `XX` is the file's sequence number. The export starts with the `data_00.csv` file, with a next file created whenever the current file exceeds 100 MB.
- Directories describing the [changefeeds](https://ydb.tech/docs/en/concepts/cdc). Directory names match the names of the changefeeds. Each directory contains the following files:
  - The `changefeed_description.pb` file describing the changefeed in the [text protobuf](https://developers.google.com/protocol-buffers/docs/reference/cpp/google.protobuf.text_format) format.
  - The `topic_description.pb` file describing the underlying topic in the [text protobuf](https://developers.google.com/protocol-buffers/docs/reference/cpp/google.protobuf.text_format) format.


## Files with data {#datafiles}

The format of data files is `.csv`, where each row corresponds to a record in the table (except the row with column headings). The urlencoded format is used for rows. For example, the file row for the table with the uint64 and utf8 columns that includes the number 1 and the Russian string "Привет" (translates to English as "Hi"), would look like this:

```text
1,"%D0%9F%D1%80%D0%B8%D0%B2%D0%B5%D1%82"
```

## Checksums {#checksums}

{% note info %}

File checksums are only generated when exporting to S3-compatible object storage.

{% endnote %}

{{ ydb-short-name }} generates a checksum for each export file and saves it to a corresponding file with the `.sha256` suffix.

The file checksum can be validated using the `sha256sum` console utility:

```sh
$ sha256sum -c scheme.pb.sha256
scheme.pb: OK
```

## Examples {#example}

### Cluster {#example-cluster}

When you export a cluster with the nested database `/Root/db`, the system will create the following file structure:

```text
backup
├─ Root
│  └─ db
│     ├─ alter_group.sql
│     ├─ create_group.sql
│     ├─ create_user.sql
│     ├─ permissions.pb
│     └─ database.pb
├─ alter_group.sql
├─ create_group.sql
├─ create_user.sql
└─ permissions.pb
```

### Database {#example-db}

When you export a database with the nested table `table`, the system will create the following file structure:

```text
├─ table
│    ├─ data00.csv
│    ├─ scheme.pb
│    └─ permissions.pb
├─ alter_group.sql
├─ create_group.sql
├─ create_user.sql
├─ permissions.sql
└─ database.pb
```

### Tables {#example-table}

When you export the tables created under [{#T}]({{ quickstart-path }}) in Getting started, the system will create the following file structure:

```text
├── episodes
│   ├── data_00.csv
│   ├── permissions.pb
│   └── scheme.pb
├── seasons
│   ├── data_00.csv
│   ├── permissions.pb
│   └── scheme.pb
└── series
    ├── data_00.csv
    ├── permissions.pb
    └── scheme.pb
    └── updates_feed
        └── changefeed_description.pb
        └── topic_description.pb
```

Contents of the `series/scheme.pb` file:

```proto
columns {
  name: "series_id"
  type {
    optional_type {
      item {
        type_id: UINT64
      }
    }
  }
}
columns {
  name: "title"
  type {
    optional_type {
      item {
        type_id: UTF8
      }
    }
  }
}
columns {
  name: "series_info"
  type {
    optional_type {
      item {
        type_id: UTF8
      }
    }
  }
}
columns {
  name: "release_date"
  type {
    optional_type {
      item {
        type_id: UINT64
      }
    }
  }
}
primary_key: "series_id"
storage_settings {
  store_external_blobs: DISABLED
}
column_families {
  name: "default"
  compression: COMPRESSION_NONE
}
```

Contents of the `series/update_feed/topic_description.pb` file:

```proto
retention_period {
  seconds: 86400
}
consumers {
  name: "my_consumer"
  read_from {
  }
  attributes {
    key: "_service_type"
    value: "data-streams"
  }
}
```

### Directories {#example-directory}

When you export an empty directory `directory`, the system will create the following file structure:

```markdown
└── directory
    ├── permissions.pb
    └── empty_dir
```

When you export a directory `directory` with the nested table `table`, the system will create the following file structure:

```markdown
└── directory
    ├── permissions.pb
    └── table
        ├── data_00.csv
        ├── permissions.pb
        └── scheme.pb
```
