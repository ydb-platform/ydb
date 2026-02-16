# Импорт и экспорт данных в колоночные таблицы

В колоночных таблицах {{ ydb-short-name }} в настоящий момент отсутствует (точнее находится в разработке) встроенный механизм резервного копирования и восстановления (backup/restore). Для миграции данных или их восстановления после сбоев рекомендуется использовать операции экспорта и импорта.

Доступны два основных подхода:

1. [Экспорт и импорт через федеративные запросы](#objstorage) к объектным хранилищам (например, {{ objstorage-name }} любое другое S3-совместимое хранилище).
**Преимущества**: Использование встроенного функционала YDB, отсутствие необходимости в сторонних сервисах или инструментах.
**Ограничения:** Данный метод позволяет экспортировать данные только в объектные хранилища.

2. [Экспорт и импорт через Apache Spark™](#spark) — гибкий способ для работы с большими объемами данных.
**Преимущества**: Поддержка широкого спектра целевых хранилищ
**Ограничения:** Требуется установка и настройка дополнительного ПО (Apache Spark™).

## Экспорт и импорт через федеративные запросы к {{ objstorage-name }} {#objstorage}

[Федеративные запросы](../concepts/query_execution/federated_query/index.md) позволяют {{ ydb-short-name }} напрямую читать и записывать данные в файлах форматов Parquet или CSV. Этот метод удобен для выполнения операций экспорта и импорта непосредственно средствами SQL без использования внешних инструментов.

### Предварительные требования

- Объектное хранилище ({{ objstorage-name }}) и статический ключ доступа, например, [{{ objstorage-full-name }}](https://yandex.cloud/ru/docs/storage/) с заранее созданным бакетом (например, `your-bucket`).
- Настроенный сетевой доступ с узлов кластера {{ ydb-short-name }} к объектному хранилищу. В примере используется endpoint `storage.yandexcloud.net` — необходимо обеспечить доступ к нему по порту 443.
- В примерах используются данные теста производительности TPC-H. Инструкция по загрузке тестовых данных доступна в соответствующем [разделе](../reference/ydb-cli/workload-tpch.md) руководства.

### Создайте секрет для доступа к {{ objstorage-name }}

Для подключения к приватному бакету необходимо использовать аутентификацию по статическим ключам доступа. В {{ ydb-short-name }} эти ключи хранятся в виде [секретов](../concepts/datamodel/secrets.md).

```sql
CREATE SECRET aws_access_id WITH (value='<ID_ключа>');
CREATE SECRET aws_access_key WITH (value='<секретный_ключ>');
```

Где:

- `aws_access_id` — секрет, содержащий ID_ключа.
- `<ID_ключа>` — идентификатор статического ключа доступа.
- `aws_access_key` — секрет, содержащий секретный ключ.
- `<секретный_ключ>` — секретная часть ключа доступа.

### Настройка подключения

Далее необходимо настроить подключение к бакету, создав внешний источник данных и внешнюю таблицу со схемой, идентичной `lineitem`.

```sql
-- Создание источника данных, указывающего на бакет и использующего секрет
CREATE EXTERNAL DATA SOURCE `external/backup_datasource` WITH (
    SOURCE_TYPE="ObjectStorage",
    LOCATION="https://storage.yandexcloud.net/<bucket_name>/",
    AUTH_METHOD="AWS",
    AWS_ACCESS_KEY_ID_SECRET_PATH="aws_access_id",
    AWS_SECRET_ACCESS_KEY_SECRET_PATH="aws_access_key",
    AWS_REGION="ru-central1"
);
```

Где:

- `external/backup_datasource` — имя создаваемого внешнего источника данных.
- `LOCATION` — URL бакета, включая название бакета `<bucket_name>`
- `AUTH_METHOD="AWS"` — метод аутентификации, совместимый с S3 API.
- `AWS_ACCESS_KEY_ID_SECRET_PATH`, `AWS_SECRET_ACCESS_KEY_SECRET_PATH` — секреты, используемые для аутентификации в {{objstorage-name}}.

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

- `LOCATION` — путь к директории с данными внутри бакета.
- `DATA_SOURCE` — название объекта `EXTERNAL DATA SOURCE`, содержащего параметры подключения.
- `external/backup/lineitem_sql` — полное имя создаваемой внешней таблицы.

### Экспорт данных из {{ ydb-short-name }}

Для экспорта данных из таблицы `tpch/s10/lineitem` в {{ objstorage-name }} используется `INSERT INTO ... SELECT` во внешнюю таблицу.

```sql
INSERT INTO `external/backup/lineitem_sql`
SELECT * FROM `tpch/s10/lineitem`;
```

После выполнения этого запроса в бакете `your-bucket` по пути `/ydb-dumps-sql/lineitem/` появятся Parquet-файлы с данными.

### Импорт данных в {{ ydb-short-name }}

{% note info %}

Команда INSERT может завершиться с ошибкой, если таблица, куда вы восстанавливаете данные, уже содержит записи. В этом случае нужно очистить целевую таблицу и повторить команду INSERT.

{% endnote %}

Для импорта данных из {{ objstorage-name }} в обратно в таблицу `tpch/s10/lineitem` используется `INSERT INTO ... SELECT` из внешней таблицы.

```sql
INSERT INTO `tpch/s10/lineitem`
SELECT * FROM `external/backup/lineitem_sql`;
```

Здесь `tpch/s10/lineitem` — это имя целевой таблицы в {{ ydb-short-name }}, в которую будут загружены данные.

## Экспорт и импорт с помощью Apache Spark™ {#spark}

Использование [коннектора](../integrations/ingestion/spark.md) {{ ydb-short-name }} для Apache Spark™ является гибким и масштабируемым решением для экспорта и импорта больших объемов данных.

### Предварительные требования

- Установленный PySpark версии 4.0.1, установить можно по [инструкции](https://spark.apache.org/docs/latest/api/python/getting_started/install.html).
- Наличие [gRPC-эндпоинта](../concepts/connect.md#эндпоинт-endpoint) для подключения к базе данных {{ ydb-short-name }}.
- [Реквизиты доступа](../reference/ydb-cli/connect.md#command-line-pars) {{ ydb-short-name }} с правами на чтение/запись.
- Настроенный сетевой доступ с узлов кластера {{ ydb-short-name }} к объектному хранилищу. В примере используется endpoint `storage.yandexcloud.net` — необходимо обеспечить доступ к нему по порту 443.
- В примерах используются данные теста производительности TPC-H. Инструкция по загрузке тестовых данных доступна в соответствующем [разделе](../reference/ydb-cli/workload-tpch.md) руководства.

### Экспорт данных из {{ ydb-short-name }} в Parquet

Используемые параметры:

- `spark.jars.packages` — конфигурационный параметр Maven, который загрузит коннектор {{ ydb-short-name }} для Spark, а также другие необходимые компоненты.
- `S3_ENDPOINT` — эндпоинт S3-совместимого хранилища (для {{ objstorage-full-name }} используйте `https://storage.yandexcloud.net`).
- `S3_ACCESS_KEY` — ID статического ключа для доступа к S3.
- `S3_SECRET_KEY` — секретная часть ключа для доступа к S3.
- `YDB_HOSTNAME` — хост gRPC-эндпоинта (например, `ydb.serverless.yandexcloud.net`).
- `YDB_PORT` — порт gRPC-эндпоинта (например, `2135`).
- `YDB_DATABASE_NAME` — путь к вашей базе данных (например, `/ru-central1/b1g.../etn...`).
- `YDB_AUTH_TYPE` — параметры для аутентификации в {{ ydb-short-name }}, поддерживаемые [драйвером Apache Spark](https://github.com/ydb-platform/ydb-spark-connector?tab=readme-ov-file#connector-usage).
- `YDB_SOURCE_TABLE` — Путь к исходной таблице в базе источнике (например, `tpch/s1/lineitem`).

```python
from pyspark.sql import SparkSession

#Настройки источника
YDB_HOSTNAME = ""
YDB_PORT = ""
YDB_DATABASE_NAME = ""
YDB_AUTH_TYPE = ""
YDB_SOURCE_TABLE = ""

#Настройки назначения
S3_ENDPOINT = ""
S3_ACCESS_KEY = ""
S3_SECRET_KEY = ""
S3_BUCKET_NAME = ""

spark = (SparkSession.builder
    .appName("ydb-export-lineitem-to-parquet")
    .config("spark.jars.packages", "tech.ydb.spark:ydb-spark-connector-shaded:2.0.1,org.apache.hadoop:hadoop-aws:3.3.6,com.amazonaws:aws-java-sdk-bundle:1.12.662")
    # Конфигурация S3-коннектора
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
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
    .option("url", f"grpcs://{YDB_HOSTNAME}:{YDB_PORT}{YDB_DATABASE_NAME}?{YDB_AUTH_TYPE}")
    .load(YDB_SOURCE_TABLE))

# Запись данных в Parquet-файлы в S3
(df.repartition(64)
    .write.mode("overwrite")
    .option("compression", "snappy")
    .parquet(f"s3a://{S3_BUCKET_NAME}/ydb-dumps-spark/lineitem/"))

spark.stop()
```

### Импорт данных из Parquet в {{ ydb-short-name }}

- `spark.jars.packages` — конфигурационный параметр Maven, который загрузит коннектор {{ ydb-short-name }} для Spark, а также другие необходимые компоненты.
- `S3_ENDPOINT` — эндпоинт S3-совместимого хранилища (для {{ objstorage-full-name }} используйте `https://storage.yandexcloud.net`).
- `S3_ACCESS_KEY` — ID статического ключа для доступа к S3.
- `S3_SECRET_KEY` — секретная часть ключа для доступа к S3.
- `YDB_HOSTNAME` — хост gRPC-эндпоинта (например, `ydb.serverless.yandexcloud.net`).
- `YDB_PORT` — порт gRPC-эндпоинта (например, `2135`).
- `YDB_DATABASE_NAME` — путь к вашей базе данных (например, `/ru-central1/b1g.../etn...`).
- `YDB_AUTH_TYPE` — параметры для аутентификации в {{ ydb-short-name }}, поддерживаемые [драйвером Apache Spark](https://github.com/ydb-platform/ydb-spark-connector?tab=readme-ov-file#connector-usage).
- `YDB_TARGET_TABLE` — Путь к таблице в базе назначения (например, `tpch/s1/lineitem`).

```python
from pyspark.sql import SparkSession

#Настройки источника
S3_ENDPOINT = "https://storage.yandexcloud.net"
S3_ACCESS_KEY = ""
S3_SECRET_KEY = ""
S3_BUCKET_NAME = ""
S3_FOLDER_PATH = ""

#Настройки назначения
YDB_HOSTNAME = ""
YDB_PORT = ""
YDB_DATABASE_NAME = ""
YDB_AUTH_TYPE = ""
YDB_TARGET_TABLE = ""

spark = (SparkSession.builder
    .appName("ydb-import-lineitem-from-parquet")
    .config("spark.jars.packages", "tech.ydb.spark:ydb-spark-connector-shaded:2.0.1,org.apache.hadoop:hadoop-aws:3.3.6,com.amazonaws:aws-java-sdk-bundle:1.12.662")
    # Конфигурация S3-коннектора (аналогично экспорту)
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
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
df = spark.read.parquet(f"s3a://{S3_BUCKET_NAME}/{S3_FOLDER_PATH}")

# Запись данных в принимающую таблицу
(df.write.format("ydb")
    .option("url", f"grpcs://{YDB_HOSTNAME}:{YDB_PORT}{YDB_DATABASE_NAME}?{YDB_AUTH_TYPE}")
    .mode("append")
    .save(YDB_TARGET_TABLE))

spark.stop()
```