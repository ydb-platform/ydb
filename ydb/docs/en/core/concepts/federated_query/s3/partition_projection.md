# Advanced partitioning

[Partitioning](partitioning.md) allows you to specify  for {{ ydb-full-name }} rules for data placement in S3 ({{ objstorage-full-name }}).

Assume the data in S3 ({{ objstorage-full-name }}) is stored in the following directory structure:

```
year=2021
    month=01
    month=02
    month=03
year=2022
    month=01
```

When executing the query below, {{ ydb-full-name }} will perform the following actions:
1. Retrieve a full list of subdirectories within '/'.
2. Attempt to process the name of each subdirectory in the format `year=<DIGITS>`.
3. For each subdirectory `year=<DIGITS>`, retrieve a list of all subdirectories in the format `month=<DIGITS>`.
4. Process the read data.

```sql
SELECT
    *
FROM
    objectstorage.'/'
WITH
(
    SCHEMA =
    (
        data String,
        year Int32,
        month Int32
    ),
    PARTITIONED_BY = "['year', 'month']"
)
WHERE
    year=2021
    AND month=02
```

When working with partitioned data, a complete listing of the contents of S3 ({{objstorage-full-name}}) is performed, which can take a considerable amount of time on large buckets.

To optimize performance on large data volumes, use "advanced partitioning". In this mode, S3 ({{objstorage-full-name}}) directories are not scanned; instead, all paths are calculated in advance, and access is made only to these paths.

To enable advanced partitioning, specify the working rules through a special parameter - "projection". This parameter describes all the rules for data placement in the S3 ({{objstorage-full-name}}) directories.

## Syntax { #syntax }

Advanced partitioning is called "partition projection" and is specified through the `projection` parameter.

Example of specifying advanced partitioning:

```sql
SELECT
    *
FROM
    <connection>.`/`
WITH
(
    SCHEMA =
    (
        data String,
        year Int32,
        month Int32
    ),
    PARTITIONED_BY = "['year', 'month']",
    `projection.enabled` : "true",

    `projection.year.type` : "integer",
    `projection.year.min` : "2010",
    `projection.year.max` : "2022",
    `projection.year.interval` : "1",

    `projection.month.type` : "integer",
    `projection.month.min` : "1",
    `projection.month.max` : "12",
    `projection.month.interval` : "1",
    `projection.month.digits` : "2",

    `storage.location.template` : "${year}/${month}"
)
```

The example above specifies that data exists for each year and each month from 2010 to 2022, with data placed in directories like `2022/12` within the bucket. If data for a certain period is absent within the bucket, this does not cause errors; the query will execute successfully, and the data will be skipped in the calculations.

In general, the advanced partitioning setup looks as follows:

```sql
SELECT
    *
FROM
    <connection>.<path>
WITH
(
    SCHEMA = (<fields>, <field1>, <field2>),
    PARTITIONED_BY = "'['field1', 'field2']",
    `projection.enabled` : <"true"|"false">,

    `projection.<field1_name>.type` : "<type>",
    `projection.<field1_name>....` : "<extended_properties>",

    `projection.<field2_name>.type` : "<type>",
    `projection.<field2_name>....` : "<extended_properties>",

    `storage.location.template` : ".../${field2}/${field1}/..."
)
```

## Field descriptions { #field_types }

| Field name                 | Description                                        | Allowed values           |
|----------------------------|----------------------------------------------------|--------------------------|
| `projection.enabled`       | Whether advanced partitioning is enabled or not    | `true`, `false`          |
| `projection.<field1_name>.type` | Data type of the field                         | `integer`, `enum`, `date`|
| `projection.<field1_name>.XXX`  | Specific properties of the type                |                          |

### Integer field type { #integer_type }

It is used for columns whose values can be represented as integers ranging from 2^-63^ to 2^63^-1.

| Field name                 | Mandatory | Description                                            | Example values          |
|----------------------------|-----------|--------------------------------------------------------|-------------------------|
| `projection.<field_name>.type` | Yes   | Data type of the field                                 | integer                 |
| `projection.<field_name>.min`  | Yes   | Specifies the minimum allowable value as an integer    | -100<br/>004             |
| `projection.<field_name>.max`  | Yes   | Specifies the maximum allowable value as an integer    | -10<br/>5000             |
| `projection.<field_name>.interval` | No, default is `1` | Specifies the step between elements within the value range. For example, a step of 3 within the range 2 to 10 will result in the values: 2, 5, 8 | 2<br/>11             |
| `projection.<field_name>.digits` | No, default is `0` | Specifies the number of digits in the number. If the number of significant digits in the number is less than the specified value, the value is padded with leading zeros up to the specified number of digits. For example, if .digits=3 is specified and the number 2 is passed, it will be converted to 002 | 2<br/>4 |

### Enum field type { #enum_type }

It is used for columns whose values can be represented as a set of enumerated values.

| Field name                   | Mandatory | Description                                           | Example values          |
|------------------------------|-----------|-------------------------------------------------------|-------------------------|
| `projection.<field_name>.type` | Yes     | Data type of the field                                | enum                    |
| `projection.<field_name>.values` | Yes   | Specifies the allowable values, separated by commas. Spaces are not ignored | 1, 2<br/>A,B,C |

### Date field type { #date_type }

It is used for columns whose values can be represented as dates. The allowable date range is from 1970-01-01 to 2105-01-01.

| Field name                 | Mandatory | Description                                            | Example values          |
|----------------------------|-----------|--------------------------------------------------------|-------------------------|
| `projection.<field_name>.type` | Yes   | Data type of the field                                 | date                    |
| `projection.<field_name>.min`  | Yes   | Specifies the minimum allowable date. Allowed values in the format `YYYY-MM-DD` or as an expression containing the special macro substitution NOW |                        |
| `projection.<field_name>.max`  | Yes   | Specifies the maximum allowable date. Allowed values in the format `YYYY-MM-DD` or as an expression containing the special macro substitution NOW | 2020-01-01<br/>NOW-5DAYS<br/>NOW+3HOURS |
| `projection.<field_name>.format` | Yes | Date formatting string based on [strptime](https://cplusplus.com/reference/ctime/strftime/) | %Y-%m-%d<br/>%D         |
| `projection.<field_name>.unit` | No, default is `DAYS` | Time interval units. Allowed values: `YEARS`, `MONTHS`, `WEEKS`, `DAYS`, `HOURS`, `MINUTES`, `SECONDS`, `MILLISECONDS` | SECONDS<br/>YEARS |
| `projection.<field_name>.interval` | No, default is `1` | Specifies the step between elements within the value range with the specified dimension in `projection.<field_name>.unit`. For example, for the range 2021-02-02 to 2021-03-05 with a step of 15 and the dimension DAYS, the values will be: 2021-02-17, 2021-03-04 | 2<br/>6 |

## Working with the NOW macro substitution

1. A number of arithmetic operations with the NOW macro substitution are supported: adding and subtracting time intervals. For example: `NOW-3DAYS`, `NOW+1MONTH`, `NOW-6YEARS`, `NOW+4HOURS`, `NOW-5MINUTES`, `NOW+6SECONDS`. The possible usage options for the macro substitution are described by the regular expression: `^\s*(NOW)\s*(([\+\-])\s*([0-9]+)\s*(YEARS?|MONTHS?|WEEKS?|DAYS?|HOURS?|MINUTES?|SECONDS?)\s*)?$`
2. Allowed interval dimensions: YEARS, MONTHS, WEEKS, DAYS, HOURS, MINUTES, SECONDS, MILLISECONDS.
3. Only one arithmetic operation is allowed in expressions; expressions like `NOW-5MINUTES+6SECONDS` are not supported.
4. Working with intervals always results in obtaining a valid date, but depending on the dimension, the final results may vary:
   - Adding `MONTHS` to a date adds a calendar month, not a fixed number of days. For example, if the current date is `2023-01-31`, adding `1 MONTHS` will result in the date `2023-02-28`.
   - Adding `30 DAYS` to a date adds a fixed number of days. For example, if the current date is `2023-01-31`, adding `30 DAYS` will result in the date `2023-03-02`.
   - The earliest possible date is `1970-01-01` (time 0 in [Unix time](https://en.wikipedia.org/wiki/Unix_time)). If the result of calculations is a date earlier than the minimum, the entire query fails with an error.
   - The latest possible date is `2105-12-31` (the maximum date in [Unix time](https://en.wikipedia.org/wiki/Unix_time)). If the result of calculations is a date later than the maximum, the entire query fails with an error.

## Path templates { #storage_location_template }

Data in S3 ({{objstorage-full-name}}) buckets can be placed in directories with arbitrary names. The `storage.location.template` setting allows you to specify the naming rules for the directories where the data is stored.

| Field name                | Description                                        | Example values           |
|---------------------------|----------------------------------------------------|--------------------------|
| `storage.location.template` | Path template for directory names. The path is specified as a text string with parameter macro substitutions `...${<field_name>}...${<field_name>}...` | `root/a/${year}/b/${month}/d`<br/>`${year}/${month}` |

If the path contains the characters `$`, `\`, or the characters `{}`, they must be escaped with the `\` character. For example, to work with a directory named `my$folder`, it needs to be specified as`my\$folder`.
