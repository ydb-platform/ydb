# App in Python

This page contains a detailed description of the code of a [test app](https://github.com/ydb-platform/ydb-python-sdk/tree/master/examples/basic_example_v1) that is available as part of the {{ ydb-short-name }} [Python SDK](https://github.com/ydb-platform/ydb-python-sdk).

## Downloading and starting {#download}

The following execution scenario is based on [git](https://git-scm.com/downloads) and [Python3](https://www.python.org/downloads/). Be sure to install the [YDB Python SDK](../../../reference/ydb-sdk/install.md).

Create a working directory and use it to run from the command line the command to clone the GitHub repository and install the necessary Python packages:

```bash
git clone https://github.com/ydb-platform/ydb-python-sdk.git
python3 -m pip install iso8601
```

Next, from the same working directory, run the command to start the test app. The command will differ depending on the database to connect to.

{% include [run_options.md](_includes/run_options.md) %}

{% include [init.md](../_includes/steps/01_init.md) %}

App code snippet for driver initialization:

```python
def run(endpoint, database, path):
    driver_config = ydb.DriverConfig(
        endpoint, database, credentials=ydb.credentials_from_env_variables(),
        root_certificates=ydb.load_ydb_root_certificate(),
    )
    with ydb.Driver(driver_config) as driver:
        try:
            driver.wait(timeout=5)
        except TimeoutError:
            print("Connect failed to YDB")
            print("Last reported errors by discovery:")
            print(driver.discovery_debug_details())
            exit(1)
```

App code snippet for creating a session:

```python
session = driver.table_client.session().create()
```

{% include [create_table.md](../_includes/steps/02_create_table.md) %}

To create tables, use the `session.create_table()` method:

```python
def create_tables(session, path):
    session.create_table(
        os.path.join(path, 'series'),
        ydb.TableDescription()
        .with_column(ydb.Column('series_id', ydb.PrimitiveType.Uint64))  # not null column
        .with_column(ydb.Column('title', ydb.OptionalType(ydb.PrimitiveType.Utf8)))
        .with_column(ydb.Column('series_info', ydb.OptionalType(ydb.PrimitiveType.Utf8)))
        .with_column(ydb.Column('release_date', ydb.OptionalType(ydb.PrimitiveType.Uint64)))
        .with_primary_key('series_id')
    )
```

The path parameter accepts the absolute path starting from the root:

```python
full_path = os.path.join(database, path)
```

You can use the `session.describe_table()` method to output information about the table structure and make sure that it was properly created:

```python
def describe_table(session, path, name):
    result = session.describe_table(os.path.join(path, name))
    print("\n> describe table: series")
    for column in result.columns:
        print("column, name:", column.name, ",", str(column.type.item).strip())
```

The given code snippet prints the following text to the console at startup:

```bash
> describe table: series
('column, name:', 'series_id', ',', 'type_id: UINT64')
('column, name:', 'title', ',', 'type_id: UTF8')
('column, name:', 'series_info', ',', 'type_id: UTF8')
('column, name:', 'release_date', ',', 'type_id: UINT64')
```
{% include [steps/03_write_queries.md](../_includes/steps/03_write_queries.md) %}

Code snippet for data insert/update:

```python
def upsert_simple(session, path):
    session.transaction().execute(
        """
        PRAGMA TablePathPrefix("{}");
        UPSERT INTO episodes (series_id, season_id, episode_id, title) VALUES
            (2, 6, 1, "TBD");
        """.format(path),
        commit_tx=True,
    )
```

{% include [pragmatablepathprefix.md](../_includes/auxilary/pragmatablepathprefix.md) %}

{% include [steps/04_query_processing.md](../_includes/steps/04_query_processing.md) %}

To execute YQL queries, use the `session.transaction().execute()` method.
The SDK lets you explicitly control the execution of transactions and configure the transaction execution mode using the `TxControl` class.

In the code snippet below, the transaction is executed using the `transaction().execute()` method. The transaction execution mode set is `ydb.SerializableReadWrite()`. When all the queries in the transaction are completed, the transaction is automatically committed by explicitly setting the flag `commit_tx=True`. The query body is described using YQL syntax and is passed to the `execute` method as a parameter.

```python
def select_simple(session, path):
    result_sets = session.transaction(ydb.SerializableReadWrite()).execute(
        """
        PRAGMA TablePathPrefix("{}");
        $format = DateTime::Format("%Y-%m-%d");
        SELECT
            series_id,
            title,
            $format(DateTime::FromSeconds(CAST(DateTime::ToSeconds(DateTime::IntervalFromDays(CAST(release_date AS Int16))) AS Uint32))) AS release_date
        FROM series
        WHERE series_id = 1;
        """.format(path),
        commit_tx=True,
    )
    print("\n> select_simple_transaction:")
    for row in result_sets[0].rows:
        print("series, id: ", row.series_id, ", title: ", row.title, ", release date: ", row.release_date)

    return result_sets[0]
```

When the query is executed, `result_set` is returned whose iteration outputs the following text to the console:

```bash
> SelectSimple:
series, Id: 1, title: IT Crowd, Release date: 2006-02-03
```


{% include [param_prep_queries.md](../_includes/steps/07_param_prep_queries.md) %}

```python
def select_prepared(session, path, series_id, season_id, episode_id):
    query = """
    PRAGMA TablePathPrefix("{}");
    DECLARE $seriesId AS Uint64;
    DECLARE $seasonId AS Uint64;
    DECLARE $episodeId AS Uint64;
    $format = DateTime::Format("%Y-%m-%d");
    SELECT
        title,
        $format(DateTime::FromSeconds(CAST(DateTime::ToSeconds(DateTime::IntervalFromDays(CAST(air_date AS Int16))) AS Uint32))) AS air_date
    FROM episodes
    WHERE series_id = $seriesId AND season_id = $seasonId AND episode_id = $episodeId;
    """.format(path)

    prepared_query = session.prepare(query)
    result_sets = session.transaction(ydb.SerializableReadWrite()).execute(
        prepared_query, {
            '$seriesId': series_id,
            '$seasonId': season_id,
            '$episodeId': episode_id,
        },
        commit_tx=True
    )
    print("\n> select_prepared_transaction:")
    for row in result_sets[0].rows:
        print("episode title:", row.title, ", air date:", row.air_date)

    return result_sets[0]
```

The given code snippet prints the following text to the console at startup:

```bash
> select_prepared_transaction:
('episode title:', u'To Build a Better Beta', ', air date:', '2016-06-05')
```

{% include [scan_query.md](../_includes/steps/08_scan_query.md) %}

```python
def executeScanQuery(driver):
  query = ydb.ScanQuery("""
    SELECT series_id, season_id, COUNT(*) AS episodes_count
    FROM episodes
    GROUP BY series_id, season_id
    ORDER BY series_id, season_id
  """, {})

  it = driver.table_client.scan_query(query)

  while True:
    try:
        result = next(it)
        print result.result_set.rows
    except StopIteration:
        break
```

{% include [transaction_control.md](../_includes/steps/10_transaction_control.md) %}

Code snippet for `transaction().begin()` and `tx.Commit()` calls:

```python
def explicit_tcl(session, path, series_id, season_id, episode_id):
    query = """
    PRAGMA TablePathPrefix("{}");

    DECLARE $seriesId AS Uint64;
    DECLARE $seasonId AS Uint64;
    DECLARE $episodeId AS Uint64;

    UPDATE episodes
    SET air_date = CAST(CurrentUtcDate() AS Uint64)
    WHERE series_id = $seriesId AND season_id = $seasonId AND episode_id = $episodeId;
    """.format(path)
    prepared_query = session.prepare(query)

    tx = session.transaction(ydb.SerializableReadWrite()).begin()

    tx.execute(
        prepared_query, {
            '$seriesId': series_id,
            '$seasonId': season_id,
            '$episodeId': episode_id
        }
    )

    print("\n> explicit TCL call")

    tx.commit()
```

