# File structure of an export

The file structure outlined below is used to export data both to the file system and an S3-compatible object storage. When working with S3, the file path is added to the object key, and the key's prefix specifies the export directory.

## Directories {#dir}

Each database directory has a counterpart directory in the file structure. The directory hierarchy in the file structure matches the directory hierarchy in the database. If a certain database directory includes no items (neither tables nor subdirectories), the first structure of such a directory includes one file of zero size named `empty_dir`.

## Tables {#tables}

For each table in the database, there's a same-name directory in the file structure's directory hierarchy that includes:

- The `scheme.pb` file describing the table structure and parameters in the [text protobuf](https://developers.google.com/protocol-buffers/docs/reference/cpp/google.protobuf.text_format) format
- One or more `data_XX.csv` files with the table data in `csv` format, where `XX` is the file's sequence number. The export starts with the `data_00.csv` file, with a next file created whenever the current file exceeds 100 MB.

## Files with data {#datafiles}

The format of data files is `.csv`, where each row corresponds to a record in the table (except the row with column headings). The urlencoded format is used for rows. For example, the file row for the table with the uint64 and utf8 columns that includes the number 1 and the Russian string "Привет" (translates to English as "Hi"), would look like this:

```
1,"%D0%9F%D1%80%D0%B8%D0%B2%D0%B5%D1%82"
```

## Example {#example}

When you export the tables created under [{#T}]({{ quickstart-path }}) in Getting started, the system will create the following file structure:

```
├── episodes
│   ├── data_00.csv
│   └── scheme.pb
├── seasons
│   ├── data_00.csv
│   └── scheme.pb
└── series
    ├── data_00.csv
    └── scheme.pb
```

Contents of the `series/scheme.pb` file:

```
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
