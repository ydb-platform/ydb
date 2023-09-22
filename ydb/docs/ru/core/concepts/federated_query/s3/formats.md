# Форматы данных и алгоритмы сжатия

В данном разделе описываются поддерживаемые в {{ydb-full-name}} форматы данных, хранимых в S3, и поддерживаемые алгоритмы сжатия.

## Поддерживаемые форматы данных {#formats}

Список поддерживаемых в {{ ydb-short-name }} форматов данных приведен в таблице ниже.

|Формат|Чтение|Запись|
|----|-----|------|
|[`csv_with_names`](#csv_with_names)|✓|✓|
|[`tsv_with_names`](#tsv_with_names)|✓||
|[`json_list`](#json_list)|✓||
|[`json_each_row`](#json_each_row)|✓||
|[`json_as_string`](#json_as_string)|✓||
|[`parquet`](#parquet)|✓|✓|
|[`raw`](#raw)|✓||

### Формат csv_with_names {#csv_with_names}
Данный формат основан на формате [CSV](https://ru.wikipedia.org/wiki/CSV). Данные размещены в колонках, разделены запятыми, в первой строке файла находятся имена колонок.

Пример данных:
```
Year,Manufacturer,Model,Price
1997,Man_1,Model_1,3000.00
1999,Man_2,Model_2,4900.00
```

{% cut "Пример запроса" %}

```sql
SELECT
    AVG(Price)
FROM `connection`.`path`
WITH
(
    FORMAT = "csv_with_names",
    SCHEMA =
    (
        Year Int32,
        Manufacturer Utf8,
        Model Utf8,
        Price Double
    )
)
```

Результат выполнения запроса:

|#|Manufacturer|Model|Price|Year|
|-|-|-|-|-|
|1|Man_1|Model_1|3000|1997|
|2|Man_2|Model_2|4900|1999


{% endcut %}


### Формат tsv_with_names {#tsv_with_names}
Данный формат основан на формате [`TSV`](https://ru.wikipedia.org/wiki/TSV). Данные размещены в колонках, разделены символами табуляции (код `0x9`), в первой строке файла находятся имена колонок.

Пример данных:
```
Year    Manufacturer    Model   Price
1997    Man_1   Model_1    3000.00
1999    Man_2   Model_2    4900.00
```

{% cut "Пример запроса" %}

```sql
SELECT
    AVG(Price)
FROM `connection`.`path`
WITH
(
    FORMAT = "tsv_with_names",
    SCHEMA =
    (
        Year Int32,
        Manufacturer Utf8,
        Model Utf8,
        Price Double
    )
)
```

Результат выполнения запроса:

|#|Manufacturer|Model|Price|Year|
|-|-|-|-|-|
|1|Man_1|Model_1|3000|1997|
|2|Man_2|Model_2|4900|1999


{% endcut %}

### Формат json_list {#json_list}
Данный формат основан на [JSON-представлении](https://ru.wikipedia.org/wiki/JSON) данных. В этом формате внутри каждого файла должен находиться объект в корректном JSON-представлении.

Пример корректных данных (данные представлены в виде списка объектов JSON):
```json
[
    { "Year": 1997, "Manufacturer": "Man_1", "Model": "Model_1", "Price": 3000.0 },
    { "Year": 1999, "Manufacturer": "Man_2", "Model": "Model_2", "Price": 4900.00 }
]
```

Пример **НЕ**корректных данных (на каждой отдельной строке находится отдельный объект в формате JSON, но эти объекты не объединены в список):

```json
{ "Year": 1997, "Manufacturer": "Man_1", "Model": "Model_1", "Price": 3000.0 }
{ "Year": 1999, "Manufacturer": "Man_2", "Model": "Model_2", "Price": 4900.00 }
```

### Формат json_each_row {#json_each_row}
Данный формат основан на [JSON-представлении](https://ru.wikipedia.org/wiki/JSON) данных. В этом формате внутри каждого файла на каждой отдельной строке файла должен находиться объект в корректном JSON-представлении, но эти объекты не объединены в JSON-список. Такой формат используется при передаче данных через потоковые системы, например, Apache Kafka или [Топики {{ydb-full-name}}](../../topic.md).

Пример корректных данных (на каждой отдельной строке находится отдельный объект в формате JSON, но эти объекты не объединены в список):
```json
{ "Year": 1997, "Manufacturer": "Man_1", "Model": "Model_1", "Price": 3000.0 },
{ "Year": 1999, "Manufacturer": "Man_2", "Model": "Model_2", "Price": 4900.00 }
```


{% cut "Пример запроса" %}

```sql
SELECT
    AVG(Price)
FROM `connection`.`path`
WITH
(
    FORMAT = "json_each_row",
    SCHEMA =
    (
        Year Int32,
        Manufacturer Utf8,
        Model Utf8,
        Price Double
    )
)
```

Результат выполнения запроса:

|#|Manufacturer|Model|Price|Year|
|-|-|-|-|-|
|1|Man_1|Model_1|3000|1997|
|2|Man_2|Model_2|4900|1999


{% endcut %}

### Формат json_as_string {#json_as_string}
Данный формат основан на [JSON-представлении](https://ru.wikipedia.org/wiki/JSON) данных. Формат `json_as_string` не разбивает входной JSON-документ на поля, а представляет каждую строку файла в виде одного объекта JSON (или одной строки). Такой формат удобен, если список полей не одинаков во всех строках, а может изменяться.

В этом формате внутри каждого файла должен находиться:
- объект в корректном JSON-представлении в каждой отдельной строке файла;
- объекты в корректном JSON-представлении, объединенные в список.

Пример корректных данных (данные представлены в виде списка объектов JSON):
```json
{ "Year": 1997, "Manufacturer": "Man_1", "Model": "Model_1", "Price": 3000.0 }
{ "Year": 1999, "Manufacturer": "Man_2", "Model": "Model_2", "Price": 4900.00 }
```


{% cut "Пример запроса" %}

```sql
SELECT
    *
FROM `connection`.`path`
WITH
(
    FORMAT = "json_as_string",
    SCHEMA =
    (
        Data Json
    )
)
```

Результат выполнения запроса:

|#|Data|
|-|-|
|1|`{"Manufacturer": "Man_1", "Model": "Model_1", "Price": 3000, "Year": 1997}`|
|2|`{"Manufacturer": "Man_2", "Model": "Model_2", "Price": 4900, "Year": 1999}`|


{% endcut %}

### Формат parquet {#parquet}
Данный формат позволяет считывать содержимое файлов в формате [Apache Parquet](https://parquet.apache.org).

Поддерживаемые алгоритмы сжатия данных внутри файлов Parquet:
- Без сжатия
- SNAPPY
- GZIP
- LZO
- BROTLI
- LZ4
- ZSTD
- LZ4_RAW


{% cut "Пример запроса" %}

```sql
SELECT
    AVG(Price)
FROM `connection`.`path`
WITH
(
    FORMAT = "parquet",
    SCHEMA =
    (
        Year Int32,
        Manufacturer Utf8,
        Model Utf8,
        Price Double
    )
)
```

Результат выполнения запроса:

|#|Manufacturer|Model|Price|Year|
|-|-|-|-|-|
|1|Man_1|Model_1|3000|1997|
|2|Man_2|Model_2|4900|1999|


{% endcut %}

### Формат raw {#raw}
Данный формат позволяет считывать содержимое файлов как есть, в "сыром" виде. Считанные таким образом данные можно обработать средствами [YQL](../../../yql/reference/udf/list/string), разделив на строки и столбцы.

Этот формат стоит использовать, если встроенных возможностей парсинга исходных данных в {{ ydb-full-name }} не достаточно.

{% cut "Пример запроса" %}

```sql
SELECT
    *
FROM `connection`.`path`
WITH
(
    FORMAT = "raw",
    SCHEMA =
    (
        Data String
    )
)
```

Результат выполнения запроса:

```
Year,Manufacturer,Model,Price
1997,Man_1,Model_1,3000.00
1999,Man_2,Model_2,4900.00
```

{% endcut %}


## Поддерживаемые алгоритмы сжатия {#compression}

Использование алгоритмов сжатия зависит от форматов файлов. Для всех форматов файлов за исключением Parquet возможно использование следующих алгоритмов сжатия:

|Алгоритм|Название в {{ydb-full-name}}|Чтение|Запись|
|----|-----|------|------|
|[Gzip](https://ru.wikipedia.org/wiki/Gzip)|gzip|✓|✓|
|[Zstd](https://ru.wikipedia.org/wiki/Zstandard)|zstd|✓||
|[LZ4](https://ru.wikipedia.org/wiki/LZ4)|lz4|✓|✓|
|[Brotli](https://ru.wikipedia.org/wiki/Brotli)|brotli|✓||
|[Bzip2](https://ru.wikipedia.org/wiki/Bzip2)|bzip2|✓||
|[Xz](https://ru.wikipedia.org/wiki/XZ)|xz|✓||

Для формата файлов Parquet поддерживаются собственные внутренние алгоритмы сжатия:

|Формат сжатия|Название в {{ ydb-full-name }}|Чтение|Запись|
|--|--|----|-----|
|[Raw](https://ru.wikipedia.org/wiki/Gzip)|raw|✓||
|[Snappy](https://ru.wikipedia.org/wiki/Snappy_(библиотека))|snappy|✓|✓|

В {{ydb-full-name}} не поддерживается работа со сжатыми "снаружи" parquet-файлами, например, с файлами вида "<myfile>.parquet.gz" или аналогичными. Все файлы в формате Parquet должны быть без внешнего сжатия.
