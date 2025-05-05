# Файловая структура выгрузки

Описанная ниже файловая структура применяется для выгрузки как в файловую систему, так и в S3-совместимое объектное хранилище. При работе с S3 в ключ объекта записывается путь к файлу, а директория выгрузки является префиксом ключа.

## Кластер {#cluster}

{% note info %}

Выгрузка кластера доступна только в файловую систему.

{% endnote %}

Кластеру соответствует директория в файловой структуре, в которой находятся:

- Директории, содержащие информацию о [базах данных](#db) в кластере, за исключением:
  - Схемных объектов базы данных
  - Пользователей и групп базы данных, не являющихся администраторами базы данных
- Файл `permissions.pb`, содержащий информацию об ACL корня кластера и его владельце в формате [text protobuf](https://developers.google.com/protocol-buffers/docs/reference/cpp/google.protobuf.text_format)
- Файл `create_user.sql`, содержащий информацию о пользователях кластера в формате YQL
- Файл `create_group.sql`, содержащий информацию о группах кластера в формате YQL
- Файл `alter_group.sql`, содержащий информацию о вхождении пользователей в группы кластера в формате YQL

## База данных {#db}

{% note info %}

Выгрузка в S3-совместимое объектное хранилище поддерживает только выгрузку схемных объекты базы данных.

{% endnote %}

Базе данных соответствует директория в файловой структуре, в которой находятся:

- Директории, содержащие информацию о схемных объектах базы данных, например, о [таблицах](#tables)
- Файл `database.pb`, содержащий информацию о настройках базы данных в формате [text protobuf](https://developers.google.com/protocol-buffers/docs/reference/cpp/google.protobuf.text_format)
- Файл `permissions.pb`, содержащий информацию об ACL базы данных и её владельце в формате [text protobuf](https://developers.google.com/protocol-buffers/docs/reference/cpp/google.protobuf.text_format)
- Файл `create_user.sql`, содержащий информацию о пользователях базы данных в формате YQL
- Файл `create_group.sql`, содержащий информацию о группах базы данных в формате YQL
- Файл `alter_group.sql`, содержащий информацию о вхождении пользователей в группы базы данных в формате YQL

## Директории {#dir}

Каждой директории в базе данных соответствует директория в файловой структуре. В каждой из них находится файл `permissions.pb`, содержащий информацию об ACL директории и её владельце в формате [text protobuf](https://developers.google.com/protocol-buffers/docs/reference/cpp/google.protobuf.text_format). Иерархия директорий в файловой структуре соответствует иерархии директорий в базе данных. Если в какой-либо директории базы данных отсутствуют объекты (таблицы или поддиректории), то в файловой структуре такая директория содержит файл нулевого размера с именем `empty_dir`.

## Таблицы {#tables}

Каждой таблице в базе данных также соответствует одноименная директория в иерархии директорий файловой структуры, в которой находятся:

- Файл `scheme.pb`, содержащий информацию о структуре таблицы и её параметрах в формате [text protobuf](https://developers.google.com/protocol-buffers/docs/reference/cpp/google.protobuf.text_format)
- Файл `permissions.pb`, содержащий информацию об ACL таблицы и её владельце в формате [text protobuf](https://developers.google.com/protocol-buffers/docs/reference/cpp/google.protobuf.text_format)
- Один или несколько файлов `data_XX.csv`, содержащих данные таблицы в формате `csv`, где `XX` - порядковый номер файла. Выгрузка начинается с файла `data_00.csv`, каждый следующий файл создается при превышении размера текущего файла в 100MB
- Директории для описания [потоков изменений](https://ydb.tech/docs/ru/concepts/cdc). Имя директории соответствует названию потока изменений. В директории находятся:
  - Файл `changefeed_description.pb`, содержащий информацию о потоке изменений в формате [text protobuf](https://developers.google.com/protocol-buffers/docs/reference/cpp/google.protobuf.text_format)
  - Файл `topic_description.pb`, содержащий информацию о нижележащем топике в формате [text protobuf](https://developers.google.com/protocol-buffers/docs/reference/cpp/google.protobuf.text_format)

## Файлы с данными {#datafiles}

Формат файлов с данными - `.csv`, одна строка соответствует одной записи в таблице, без строки с заголовками колонок. Для строк применяется  представление в urlencoded формате. Например, строка файла для таблицы с колонками uint64 и utf8, содержащая число 1 и строку "Привет" соответственно, выглядит таким образом:

```text
1,"%D0%9F%D1%80%D0%B8%D0%B2%D0%B5%D1%82"
```

## Контрольные суммы {#checksums}

{% note info %}

Контрольные суммы файлов создаются только при выгрузке в S3-совместимое объектное хранилище.

{% endnote %}

{{ ydb-short-name }} генерирует контрольную сумму для каждого файла выгрузки и сохраняет ее в соответствующем файле с суффиксом `.sha256`.

Контрольные суммы файлов можно проверить с помощью консольной утилиты `sha256sum`:

```sh
$ sha256sum -c scheme.pb.sha256
scheme.pb: OK
```

## Примеры {#example}

### Кластер {#example-cluster}

При выгрузке кластера с базыми данных `/Root/db1` и `/Root/db2` будет создана следующая структура файлов:

```text
backup
├─ Root
│  ├─ db1
│  │  ├─ alter_group.sql
│  │  ├─ create_group.sql
│  │  ├─ create_user.sql
│  │  ├─ permissions.pb
│  │  └─ database.pb
│  └─ db2
│    ├─ alter_group.sql
│    ├─ create_group.sql
│    ├─ create_user.sql
│    ├─ permissions.pb
│    └─ database.pb
├─ alter_group.sql
├─ create_group.sql
├─ create_user.sql
└─ permissions.pb
```

### Базы данных {#example-db}

При выгрузке базы данных c таблицей `episodes` будет создана следующая структура файлов:

```text
├─ episodes
│    ├─ data00.csv
│    ├─ scheme.pb
│    └─ permissions.pb
├─ alter_group.sql
├─ create_group.sql
├─ create_user.sql
├─ permissions.sql
└─ database.pb
```

### Таблицы {#example-table}

При выгрузке таблиц, созданных в разделе [{#T}]({{ quickstart-path }}) Начала работы, будет создана следующая файловая структура:

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

### Директории {#example-directory}

При выгрузке пустой директории `series` будет создана следующая структура файлов:

```markdown
└── series
    ├── permissions.pb
    └── empty_dir
```

При выгрузке директории `series` с вложенной таблицей `episodes` будет создана следующая структура файлов:

```markdown
└── series
    ├── permissions.pb
    └── episodes
        ├── data_00.csv
        ├── permissions.pb
        └── scheme.pb
```
