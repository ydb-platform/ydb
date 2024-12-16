# Файловая структура выгрузки

Описанная ниже файловая структура применяется для выгрузки как в файловую систему, так и в S3-совместимое объектное хранилище. При работе с S3 в ключ объекта записывается путь к файлу, а директория выгрузки является префиксом ключа.

## Директории {#dir}

Каждой директории в базе данных соответствует директория в файловой структуре. Иерархия директорий в файловой структуре соответствует иерархии директорий в базе данных. Если в некоторой директории базы данных нет никаких объектов (ни таблиц, ни поддиректорий), то в файловой структуре в такой директории присутствует один файл нулевого размера с именем `empty_dir`.

## Таблицы {#tables}

Каждой таблице в базе данных также соответствует одноименная директория в иерархии директорий файловой структуры, в которой находятся:

- Файл `scheme.pb`, содержащий информацию о структуре таблицы и её параметрах в формате [text protobuf](https://developers.google.com/protocol-buffers/docs/reference/cpp/google.protobuf.text_format)
- Один или несколько файлов `data_XX.csv`, содержащих данные таблицы в формате `csv`, где `XX` - порядковый номер файла. Выгрузка начинается с файла `data_00.csv`, каждый следующий файл создается при превышении размера текущего файла в 100MB.
- Директории для описания changefeed'ов. Имя директории соответствует названию changefeed'а. В директории находятся:
  - Файл `changefeed_description.pb` - содержит информацию о changefeed'е в формате protobuf
  - Файл `topic_description.pb` - содержит информацию о changefeed'е, как о топике в формате protobuf (`tools restore` берет информацию о consumer'ах из этого файла)

## Файлы с данными {#datafiles}

Формат файлов с данными - `.csv`, одна строка соответствует одной записи в таблице, без строки с заголовками колонок. Для строк применяется  представление в urlencoded формате. Например, строка файла для таблицы с колонками uint64 и utf8, содержащая число 1 и строку "Привет" соответственно, выглядит таким образом:

```text
1,"%D0%9F%D1%80%D0%B8%D0%B2%D0%B5%D1%82"
```

## Пример {#example}

При выгрузке таблиц, созданных в разделе [{#T}]({{ quickstart-path }}) Начала работы, будет создана следующая файловая структура:

```text
├── episodes
│   ├── data_00.csv
│   └── scheme.pb
├── seasons
│   ├── data_00.csv
│   └── scheme.pb
└── series
    ├── data_00.csv
    └── scheme.pb
    └── updates_feed
        └── changefeed_description.pb
        └── topic_description.pb
```

Содержимое файла `series/scheme.pb`:

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

Содержимое файла `series/update_feed/changefeed_description.pb`:

```proto
name: "update_feed"
mode: MODE_UPDATES
format: FORMAT_JSON
state: STATE_ENABLED
```

Содержимое файла `series/update_feed/topic_description.pb`:

```proto
self {
  name: "update_feed"
  owner: "root@builtin"
  type: TOPIC
  effective_permissions {
    subject: "USERS"
    permission_names: "ydb.database.connect"
  }
  effective_permissions {
    subject: "METADATA-READERS"
    permission_names: "ydb.generic.list"
  }
  effective_permissions {
    subject: "DATA-READERS"
    permission_names: "ydb.granular.select_row"
  }
  effective_permissions {
    subject: "DATA-WRITERS"
    permission_names: "ydb.tables.modify"
  }
  effective_permissions {
    subject: "DDL-ADMINS"
    permission_names: "ydb.granular.create_directory"
    permission_names: "ydb.granular.write_attributes"
    permission_names: "ydb.granular.create_table"
    permission_names: "ydb.granular.remove_schema"
    permission_names: "ydb.granular.alter_schema"
  }
  effective_permissions {
    subject: "ACCESS-ADMINS"
    permission_names: "ydb.access.grant"
  }
  effective_permissions {
    subject: "DATABASE-ADMINS"
    permission_names: "ydb.generic.manage"
  }
  created_at {
    plan_step: 1734362034420
    tx_id: 281474982949619
  }
}
partitioning_settings {
  min_active_partitions: 1
  max_active_partitions: 1
  auto_partitioning_settings {
    strategy: AUTO_PARTITIONING_STRATEGY_DISABLED
    partition_write_speed {
      stabilization_window {
        seconds: 300
      }
      up_utilization_percent: 80
      down_utilization_percent: 20
    }
  }
}
partitions {
  active: true
}
retention_period {
  seconds: 86400
}
partition_write_speed_bytes_per_second: 1048576
partition_write_burst_bytes: 1048576
attributes {
  key: "__max_partition_message_groups_seqno_stored"
  value: "6000000"
}
attributes {
  key: "_allow_unauthenticated_read"
  value: "true"
}
attributes {
  key: "_allow_unauthenticated_write"
  value: "true"
}
attributes {
  key: "_message_group_seqno_retention_period_ms"
  value: "1382400000"
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
