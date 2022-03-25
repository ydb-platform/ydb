# File structure of data export

The file structure described below is used for exporting data both to the file system and S3-compatible object storage.

## Directories {#dir}

Each directory in a database corresponds to a directory in the file structure. The directory hierarchy in the file structure corresponds the directory hierarchy in the database. If some DB directory contains no objects (neither tables nor subdirectories), this directory's file structure contains a single zero size file named `empty_dir`.

## Tables {#tables}

Each DB table also has a corresponding same-name directory in the file structure directory hierarchy, which contains:

- The `scheme.pb` file with information about the table structure and its parameters in [text protobuf](https://developers.google.com/protocol-buffers/docs/reference/cpp/google.protobuf.text_format) format.
- One or more `data_XX.csv` files with data in `CSV` format, where `XX` is the file sequence number. Data export starts with the `data_00.csv` file. Each subsequent file is created once the size of the current file exceeds 100 MB.

## Data files {#datafiles}

Data is stored in `.csv` files, one file line per table entry, without a row with column headers. URL-encoded format is used for string representation. For example, a file line for a table with uint64 and uft8 columns containing the number 1 and the string "Hello", respectively, looks like this:

```
1,"%D0%9F%D1%80%D0%B8%D0%B2%D0%B5%D1%82"
```

## Example {#example}

When exporting tables created within a tutorial when [Getting started with YQL](../../../../getting_started/yql.md#create-table) in the "Getting started" section, the following file structure is created:

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

