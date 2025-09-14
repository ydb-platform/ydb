# Импорт и экспорт данных из колоночных таблиц

В колоночных таблицах {{ ydb-short-name }} отсутствует встроенный механизм резервного копирования и восстановления (backup/restore). Для миграции данных или их восстановления после сбоев рекомендуется использовать операции экспорта и импорта.

Доступны два основных подхода:

1.  [Экспорт и импорт через федеративные запросы](#objstorage) к объектным хранилищам (например, {{ objstorage-name }} или другим S3-совместимым).
2.  [Экспорт и импорт через Apache Spark™](#spark) — гибкий способ для работы с большими объемами данных.

## Экспорт и импорт через федеративные запросы к {{ objstorage-name }} {#objstorage}

[Федеративные запросы](../concepts/federated_query/index.md) позволяют {{ ydb-short-name }} напрямую читать и записывать данные в файлах форматов Parquet или CSV. Этот метод удобен для выполнения операций экспорта и импорта непосредственно средствами SQL без использования внешних инструментов.

### Создайте секрет для доступа к {{ objstorage-name }}

Для подключения к приватному бакету необходимо использовать аутентификацию по статическим ключам доступа. В {{ ydb-short-name }} эти ключи хранятся в виде [`SECRET`](../concepts/datamodel/secrets.md) объектов.

```sql
CREATE OBJECT aws_access_id (TYPE SECRET) WITH (value='<ID_ключа>');
CREATE OBJECT aws_access_key (TYPE SECRET) WITH (value='<секретный_ключ>');
```

Где:
-   `aws_access_id` — имя секрета, содержащего ID_ключа.
-   `<ID_ключа>` — идентификатор статического ключа доступа.
-   `aws_access_key` — имя секрета, содержащего секретный ключ.
-   `<секретный_ключ>` — секретная часть ключа доступа.

###  Настройка подключения

Далее необходимо настроить подключение к бакету, создав внешний источник данных и внешнюю таблицу со схемой, идентичной `lineitem`.

```sql
-- Создание источника данных, указывающего на бакет и использующего секрет
CREATE EXTERNAL DATA SOURCE `external/backup_datasource` WITH (
    SOURCE_TYPE="ObjectStorage",
    LOCATION="https://storage.yandexcloud.net/<bucket_name>/",
    AUTH_METHOD="AWS",
    AWS_ACCESS_KEY_ID_SECRET_NAME="aws_access_id",
    AWS_SECRET_ACCESS_KEY_SECRET_NAME="aws_access_key",
    AWS_REGION="ru-central1"
);
```

Где:
*   `external/backup_datasource` — имя создаваемого внешнего источника данных.
*   `LOCATION` — URL бакета, включая название бакета `<bucket_name>`
*   `AUTH_METHOD="AWS"` — метод аутентификации, совместимый с S3 API.
*   `AWS_ACCESS_KEY_ID_SECRET_NAME`, `AWS_SECRET_ACCESS_KEY_SECRET_NAME` — имена секретов, используемых для аутентификации в {{objstorage-name}}.

```sql
-- Создание внешней таблицы со схемой lineitem
CREATE EXTERNAL TABLE `external/backup/lineitem_sql` (
    l_orderkey Int64 NOT NULL,
    l_partkey Int32 NOT NULL,
    l_suppkey Int32 NOT NULL,
    l_linenumber Int32 NOT NULL,
    l_quantity Double NOT NULL,
    l_extendedprice Double NOT NULL,
    l_discount Double NOT NULL,
    l_tax Double NOT NULL,
    l_returnflag String NOT NULL,
    l_linestatus String NOT NULL,
    l_shipdate Date NOT NULL,
    l_commitdate Date NOT NULL,
    l_receiptdate Date NOT NULL,
    l_shipinstruct String NOT NULL,
    l_shipmode String NOT NULL,
    l_comment String NOT NULL
) WITH (
    DATA_SOURCE="external/backup_datasource",
    LOCATION="/ydb-dumps-sql/lineitem/",
    FORMAT="parquet"
);
```

Где:
*   `LOCATION` — путь к директории с данными внутри бакета.
*   `DATA_SOURCE` - название объекта `EXTERNAL DATA SOURCE`, содержащего параметры подключения.
*   `external/backup/lineitem_sql` — полное имя создаваемой внешней таблицы.

### Экспорт данных из {{ ydb-short-name }}

Для экспорта данных из таблицы `tpch/s10/lineitem` в {{ objstorage-name }} используется `INSERT INTO ... SELECT` во внешнюю таблицу.

```sql
INSERT INTO `external/backup/lineitem_sql`
SELECT * FROM `tpch/s10/lineitem`;
```

После выполнения этого запроса в бакете `your-bucket` по пути `/ydb-dumps-sql/lineitem/` появятся Parquet-файлы с данными.

### Импорт данных в {{ ydb-short-name }}

Для импорта данных из {{ objstorage-name }} в обратно в таблицу `tpch/s10/lineitem` используется `INSERT INTO ... SELECT` из внешней таблицы.

```sql
INSERT INTO `tpch/s10/lineitem`
SELECT * FROM `external/backup/lineitem_sql`;
```

Здесь `tpch/s10/lineitem` — это имя целевой таблицы в {{ ydb-short-name }}, в которую будут загружены данные.

## Экспорт и импорт с помощью Apache Spark™ {#spark}

Использование коннектора {{ ydb-short-name }} для Apache Spark™ является гибким и масштабируемым решением для экспорта и импорта больших объемов данных.

### Предварительные требования

*   Установленный драйвер {{ ydb-short-name }} для Spark.
*   Наличие gRPC-эндпоинта для подключения к базе данных {{ ydb-short-name }}.
*   Реквизиты доступа {{ ydb-short-name }} с правами на чтение/запись.
*   Статический ключ доступа к {{ objstorage-name }}.


### Подтоговка

Установите `PySpark`

```shell
pip3 install pyspark
```

### Экспорт данных из {{ ydb-short-name }} в Parquet

```python
from pyspark.sql import SparkSession

spark = (SparkSession.builder
    .appName("ydb-export-lineitem-to-parquet")
    .config("spark.jars.packages", "tech.ydb.spark:ydb-spark-connector-shaded:2.0.1,org.apache.hadoop:hadoop-aws:3.3.6,com.amazonaws:aws-java-sdk-bundle:1.12.662")
    # Конфигурация S3-коннектора
    .config("spark.hadoop.fs.s3a.endpoint", "https://storage.yandexcloud.net")
    .config("spark.hadoop.fs.s3a.access.key", "<ID_ключа_S3>")
    .config("spark.hadoop.fs.s3a.secret.key", "<секретный_ключ_S3>")
    .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60000")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "30000")   # 30s
    .config("spark.hadoop.fs.s3a.connection.timeout", "200000")            # 200s
    .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60000")          # 60s
    .config("spark.hadoop.fs.s3a.connection.ttl", "300000")                # 5m
    .config("spark.hadoop.fs.s3a.assumed.role.session.duration", "1800000")# 30m
    .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400000")         # 24h
    .config("spark.hadoop.fs.s3a.retry.interval", "500")                   # 500ms
    .config("spark.hadoop.fs.s3a.retry.throttle.interval", "100")          # 100ms
    .getOrCreate())

# Чтение данных из таблицы lineitem
df = (spark.read.format("ydb")
    .option("url", "grpcs://<endpoint>/<database>?<auth_info>")
    .load("tpch/s10/lineitem"))

# Запись данных в Parquet-файлы в S3
(df.repartition(64)
    .write.mode("overwrite")
    .option("compression", "snappy")
    .parquet("s3a://your-bucket/ydb-dumps-spark/lineitem/"))

spark.stop()
```

Где:
*   `spark.jars.packages` — зависимость Maven, которая загрузит коннектор {{ ydb-short-name }} для Spark, а также другие необходимые компоненты.
*   `spark.hadoop.fs.s3a.endpoint` — эндпоинт S3-совместимого хранилища.
*   `spark.hadoop.fs.s3a.access.key` (`<ID_ключа_S3>`) — ID статического ключа для доступа к S3.
*   `spark.hadoop.fs.s3a.secret.key` (`<секретный_ключ_S3>`) — секретная часть ключа для доступа к S3.
*   `url` — строка подключения к базе данных {{ ydb-short-name }}:
    *   `<endpoint>` — хост и порт gRPC-эндпоинта (например, `ydb.serverless.yandexcloud.net:2135`).
    *   `<database>` — путь к вашей базе данных (например, `/ru-central1/b1g.../etn...`).
    *   `<auth_info>` — параметры для аутентификации в {{ ydb-short-name }}, поддерживаемые [драйвером Apache Spark](https://github.com/ydb-platform/ydb-spark-connector?tab=readme-ov-file#connector-usage).
*   `load(...)` — путь к исходной таблице `tpch/s10/lineitem`.
*   `parquet(...)` — полный путь в S3-хранилище (`s3a://...`), куда будут сохранены данные.

### Импорт данных из Parquet в {{ ydb-short-name }}

```python
from pyspark.sql import SparkSession

spark = (SparkSession.builder
    .appName("ydb-import-lineitem-from-parquet")
    .config("spark.jars.packages", "tech.ydb.spark:ydb-spark-connector-shaded:2.0.1")
    # Конфигурация S3-коннектора (аналогично экспорту)
    .config("spark.hadoop.fs.s3a.endpoint", "https://storage.yandexcloud.net")
    .config("spark.hadoop.fs.s3a.access.key", "<ID_ключа_S3>")
    .config("spark.hadoop.fs.s3a.secret.key", "<секретный_ключ_S3>")
    .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60000")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "30000")   # 30s
    .config("spark.hadoop.fs.s3a.connection.timeout", "200000")            # 200s
    .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60000")          # 60s
    .config("spark.hadoop.fs.s3a.connection.ttl", "300000")                # 5m
    .config("spark.hadoop.fs.s3a.assumed.role.session.duration", "1800000")# 30m
    .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400000")         # 24h
    .config("spark.hadoop.fs.s3a.retry.interval", "500")                   # 500ms
    .config("spark.hadoop.fs.s3a.retry.throttle.interval", "100")          # 100ms
    .getOrCreate())

# Чтение данных из Parquet-файлов в S3, созданных на шаге экспорта
df = spark.read.parquet("s3a://your-bucket/ydb-dumps-spark/lineitem/")

# Запись данных в принимающую таблицу
(df.write.format("ydb")
    .option("url", "grpcs://<endpoint>/<database>?<!!!!auth_info>")
    .mode("append")
    .save("tpch/s10/lineitem"))

spark.stop()
```

Где:
*   Параметры `spark.hadoop.*` и `url` настраиваются аналогично примеру с экспортом.
*   `parquet(...)` — путь в S3, откуда будут читаться данные (тот же, что использовался для экспорта).
*   `save(...)` — путь к целевой таблице `tpch/s10/lineitem`, в которую будут импортированы данные.
