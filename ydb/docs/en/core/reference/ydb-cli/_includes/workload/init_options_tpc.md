### Available parameters {#init_options}

Option name | Option description
---|---
`--store <value>` | Table storage type. Possible values: `row`, `column`, `external-s3`. Default value is `row`.
`--external-s3-prefix <value>` | Only relevant for external tables. Root path to the dataset in S3 storage.
`--external-s3-endpoint <value>` or `-e <value>` | Only relevant for external tables. Link to S3 Bucket with data.
`--string` | Use `String` type for text fields. Default value is `Utf8`.
`--datetime` | Use for time-related fields of type `Date`, `Datetime`, and `Timestamp`. By default, `Date32`, `Datetime64` and `Timestamp64` are used.
`--float-mode <value>` | Which data type to use for float fields. Possible values ​​are `float`, `decimal` and `decimal_ydb`. `float` - use `Float` type, `decimal` - `Decimal` with the dimensions specified by the test standard, and `decimal_ydb` - use `Decimal(22,9)` type - the only one that YDB currently supports. By default, `float` is used.
`--clear` | If the table at the specified path already exists, it will be deleted.
