# App in Python

This page contains a detailed description of the code of a [test app](https://github.com/ydb-platform/ydb-python-sdk/tree/master/examples/basic_example_v2) that is available as part of the {{ ydb-short-name }} [Python SDK](https://github.com/ydb-platform/ydb-python-sdk).

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

{% list tabs %}

- Synchronous

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

- Asynchronous

   ```python
   async def run(endpoint, database, path):
       driver_config = ydb.DriverConfig(
           endpoint, database, credentials=ydb.credentials_from_env_variables(),
           root_certificates=ydb.load_ydb_root_certificate(),
       )
       async with ydb.aio.Driver(driver_config) as driver:
           try:
               await driver.wait(timeout=5)
           except TimeoutError:
               print("Connect failed to YDB")
               print("Last reported errors by discovery:")
               print(driver.discovery_debug_details())
               exit(1)
   ```

{% endlist %}

{% include [create_table.md](../_includes/steps/02_create_table.md) %}

To execute YQL queries, use the `pool.execute_with_retries()` method. For example, it is possible to create table:

{% list tabs %}

- Synchronous

   ```python
   def create_tables(pool: ydb.QuerySessionPool, path: str):
       print("\nCreating table series...")
       pool.execute_with_retries(
           f"""
            PRAGMA TablePathPrefix("{path}");
            CREATE table `series` (
                `series_id` Uint64,
                `title` Utf8,
                `series_info` Utf8,
                `release_date` Uint64,
                PRIMARY KEY (`series_id`)
            )
            """
       )

       print("\nCreating table seasons...")
       pool.execute_with_retries(
           f"""
            PRAGMA TablePathPrefix("{path}");
            CREATE table `seasons` (
                `series_id` Uint64,
                `season_id` Uint64,
                `title` Utf8,
                `first_aired` Uint64,
                `last_aired` Uint64,
                PRIMARY KEY (`series_id`, `season_id`)
            )
            """
       )

       print("\nCreating table episodes...")
       pool.execute_with_retries(
           f"""
            PRAGMA TablePathPrefix("{path}");
            CREATE table `episodes` (
                `series_id` Uint64,
                `season_id` Uint64,
                `episode_id` Uint64,
                `title` Utf8,
                `air_date` Uint64,
                PRIMARY KEY (`series_id`, `season_id`, `episode_id`)
            )
            """
       )
   ```

- Asynchronous

   ```python
   async def create_tables(pool: ydb.aio.QuerySessionPoolAsync, path: str):
       print("\nCreating table series...")
       await pool.execute_with_retries(
           f"""
            PRAGMA TablePathPrefix("{path}");
            CREATE table `series` (
                `series_id` Uint64,
                `title` Utf8,
                `series_info` Utf8,
                `release_date` Uint64,
                PRIMARY KEY (`series_id`)
            )
            """
       )

       print("\nCreating table seasons...")
       await pool.execute_with_retries(
           f"""
            PRAGMA TablePathPrefix("{path}");
            CREATE table `seasons` (
                `series_id` Uint64,
                `season_id` Uint64,
                `title` Utf8,
                `first_aired` Uint64,
                `last_aired` Uint64,
                PRIMARY KEY (`series_id`, `season_id`)
            )
            """
       )

       print("\nCreating table episodes...")
       await pool.execute_with_retries(
           f"""
            PRAGMA TablePathPrefix("{path}");
            CREATE table `episodes` (
                `series_id` Uint64,
                `season_id` Uint64,
                `episode_id` Uint64,
                `title` Utf8,
                `air_date` Uint64,
                PRIMARY KEY (`series_id`, `season_id`, `episode_id`)
            )
            """
       )
   ```

{% endlist %}

The path parameter accepts the absolute path starting from the root:

```python
full_path = os.path.join(database, path)
```

The function `pool.execute_with_retries(query)`, unlike `tx.execute()`, loads the result of the query into memory before returning it to the client. This eliminates the need to use special constructs to control the iterator, but it is necessary to use this method with caution for large `SELECT` queries. More information about streams will be discussed below.

{% include [steps/03_write_queries.md](../_includes/steps/03_write_queries.md) %}

Code snippet for data insert/update:

{% list tabs %}

- Synchronous

   ```python
   def upsert_simple(pool, path):
       print("\nPerforming UPSERT into episodes...")
       pool.execute_with_retries(
           f"""
           PRAGMA TablePathPrefix("{path}");
           UPSERT INTO episodes (series_id, season_id, episode_id, title) VALUES (2, 6, 1, "TBD");
           """
       )
   ```

- Asynchronous

   ```python
   async def upsert_simple(pool: ydb.aio.QuerySessionPoolAsync, path: str):
       print("\nPerforming UPSERT into episodes...")
       await pool.execute_with_retries(
           f"""
           PRAGMA TablePathPrefix("{path}");
           UPSERT INTO episodes (series_id, season_id, episode_id, title) VALUES (2, 6, 1, "TBD");
           """
       )
   ```

{% endlist %}

{% include [pragmatablepathprefix.md](../_includes/auxilary/pragmatablepathprefix.md) %}

{% include [steps/04_query_processing.md](../_includes/steps/04_query_processing.md) %}

To execute YQL queries, it is often enough to use the already familiar `pool.execute_with_retries()` method.

{% list tabs %}

- Synchronous

   ```python
   def select_simple(pool: ydb.QuerySessionPool, path: str):
       print("\nCheck series table...")
       result_sets = pool.execute_with_retries(
           f"""
           PRAGMA TablePathPrefix("{path}");
           SELECT
               series_id,
               title,
               release_date
           FROM series
           WHERE series_id = 1;
           """,
       )
       first_set = result_sets[0]
       for row in first_set.rows:
           print(
               "series, id: ",
               row.series_id,
               ", title: ",
               row.title,
               ", release date: ",
               row.release_date,
           )
       return first_set
   ```

- Asynchronous

   ```python
   async def select_simple(pool: ydb.aio.QuerySessionPoolAsync, path: str):
       print("\nCheck series table...")
       result_sets = await pool.execute_with_retries(
           f"""
           PRAGMA TablePathPrefix("{path}");
           SELECT
               series_id,
               title,
               release_date
           FROM series
           WHERE series_id = 1;
           """,
       )
       first_set = result_sets[0]
       for row in first_set.rows:
           print(
               "series, id: ",
               row.series_id,
               ", title: ",
               row.title,
               ", release date: ",
               row.release_date,
           )
       return first_set
   ```

{% endlist %}

As the result of executing the query, a `result_set` is returned, iterating on which the text is output to the console:

```bash
> SelectSimple:
series, Id: 1, title: IT Crowd, Release date: 2006-02-03
```

## Parameterized queries {#param-queries}

To execute parameterized queries in the `pool.execute_with_retries()` method (or `tx.execute()`, which will be shown in the next section) it is necessary to pass a dictionary with parameters of a special type, where the key is the parameter name, and the value can be one of the following:
1. The usual value;
2. Tuple with value and type;
3. A special type `ydb.TypedValue(value=value, value_type=value_type)`.

If you specify a value without a type, the conversion takes place according to the following rules:
* `int` -> `ydb.PrimitiveType.Int64`
* `float` -> `ydb.PrimitiveType.Double`
* `str` -> `ydb.PrimitiveType.Utf8`
* `bytes` -> `ydb.PrimitiveType.String`
* `bool` -> `ydb.PrimitiveType.Bool`
* `list` -> `ydb.ListType`
* `dict` -> `ydb.DictType`

Automatic conversion of lists and dictionaries is possible only in the case of homogeneous structures, the type of nested value will be calculated recursively according to the above rules.

A code snippet demonstrating the possibility of using parameterized queries:

{% list tabs %}

- Synchronous

   ```python
   def select_with_parameters(pool: ydb.QuerySessionPool, path: str, series_id, season_id, episode_id):
       result_sets = pool.execute_with_retries(
           f"""
           PRAGMA TablePathPrefix("{path}");
           SELECT
               title,
               air_date
           FROM episodes
           WHERE series_id = $seriesId AND season_id = $seasonId AND episode_id = $episodeId;
           """,
           {
               "$seriesId": series_id,  # data type could be defined implicitly
               "$seasonId": (season_id, ydb.PrimitiveType.Int64),  # could be defined via tuple
               "$episodeId": ydb.TypedValue(episode_id, ydb.PrimitiveType.Int64),  # could be defined via special class
           },
       )

       print("\n> select_with_parameters:")
       first_set = result_sets[0]
       for row in first_set.rows:
           print("episode title:", row.title, ", air date:", row.air_date)

       return first_set
   ```

- Asynchronous

   ```python
   async def select_with_parameters(pool: ydb.aio.QuerySessionPoolAsync, path: str, series_id, season_id, episode_id):
       result_sets = await pool.execute_with_retries(
           f"""
           PRAGMA TablePathPrefix("{path}");
           SELECT
               title,
               air_date
           FROM episodes
           WHERE series_id = $seriesId AND season_id = $seasonId AND episode_id = $episodeId;
           """,
           {
               "$seriesId": series_id,  # could be defined implicit
               "$seasonId": (season_id, ydb.PrimitiveType.Int64),  # could be defined via tuple
               "$episodeId": ydb.TypedValue(episode_id, ydb.PrimitiveType.Int64),  # could be defined via special class
           },
       )

       print("\n> select_with_parameters:")
       first_set = result_sets[0]
       for row in first_set.rows:
           print("episode title:", row.title, ", air date:", row.air_date)

       return first_set
   ```

{% endlist %}

The above code snippet outputs text to the console:

```bash
> select_prepared_transaction:
('episode title:', u'To Build a Better Beta', ', air date:', '2016-06-05')
```

{% include [transaction_control.md](../_includes/steps/10_transaction_control.md) %}

The `session.transaction().execute()` method can also be used to execute YQL queries.
This method, unlike `pool.execute_with_retries`, allows you to explicitly control the execution of transactions and configure the needed transaction mode using the `TxControl` class.

Available transaction modes:
* `ydb.QuerySerializableReadWrite()` (default);
* `ydb.QueryOnlineReadOnly(allow_inconsistent_reads=False)`;
* `ydb.QuerySnapshotReadOnly()`;
* `ydb.QueryStaleReadOnly()`.

For more information about transaction modes, see [YDB docs](https://ydb.tech/docs/en/concepts/transactions#modes)

The result of executing `tx.execute()` is an iterator. The iterator allows you to read an unlimited number of rows and a volume of data without loading the entire result into memory.
However, in order to correctly save the state of the transaction on the `ydb` side, the iterator must be read to the end after each request.
For convenience, the result of the `tx.execute()` function is presented as a context manager that scrolls through the iterator to the end after exiting.

{% list tabs %}

- Synchronous

   ```python
   with tx.execute(query) as _:
       pass
   ```

- Asynchronous

   ```python
   async with await tx.execute(query) as _:
       pass
   ```

{% endlist %}

In the code snippet below, the transaction is executed using the `transaction().execute()` method. The transaction mode is set to `ydb.QuerySerializableReadWrite()`.
The request body is described using YQL syntax and is passed to the `execute` method as the parameter.

A code snippet demonstrating the explicit use of `transaction().begin()` and `tx.commit()`:

{% list tabs %}

- Synchronous

   ```python
   def explicit_transaction_control(pool: ydb.QuerySessionPool, path: str, series_id, season_id, episode_id):
       def callee(session: ydb.QuerySessionSync):
           query = f"""
           PRAGMA TablePathPrefix("{path}");
           UPDATE episodes
           SET air_date = CurrentUtcDate()
           WHERE series_id = $seriesId AND season_id = $seasonId AND episode_id = $episodeId;
           """

           # Get newly created transaction id
           tx = session.transaction(ydb.QuerySerializableReadWrite()).begin()

           # Execute data query.
           # Transaction control settings continues active transaction (tx)
           with tx.execute(
               query,
               {
                   "$seriesId": (series_id, ydb.PrimitiveType.Int64),
                   "$seasonId": (season_id, ydb.PrimitiveType.Int64),
                   "$episodeId": (episode_id, ydb.PrimitiveType.Int64),
               },
           ) as _:
               pass

           print("\n> explicit TCL call")

           # Commit active transaction(tx)
           tx.commit()

       return pool.retry_operation_sync(callee)
   ```

- Asynchronous

   ```python
   async def explicit_transaction_control(
       pool: ydb.aio.QuerySessionPoolAsync, path: str, series_id, season_id, episode_id
   ):
       async def callee(session: ydb.aio.QuerySessionAsync):
           query = f"""
           PRAGMA TablePathPrefix("{path}");
           UPDATE episodes
           SET air_date = CurrentUtcDate()
           WHERE series_id = $seriesId AND season_id = $seasonId AND episode_id = $episodeId;
           """

           # Get newly created transaction id
           tx = await session.transaction(ydb.QuerySerializableReadWrite()).begin()

           # Execute data query.
           # Transaction control settings continues active transaction (tx)
           async with await tx.execute(
               query,
               {
                   "$seriesId": (series_id, ydb.PrimitiveType.Int64),
                   "$seasonId": (season_id, ydb.PrimitiveType.Int64),
                   "$episodeId": (episode_id, ydb.PrimitiveType.Int64),
               },
           ) as _:
               pass

           print("\n> explicit TCL call")

           # Commit active transaction(tx)
           await tx.commit()

       return await pool.retry_operation_async(callee)
   ```

{% endlist %}

However, it is worth remembering that a transaction can be opened implicitly at the first request. It could be commited automatically with the explicit indication of the `commit_tx=True` flag.
Implicit transaction management is preferable because it requires fewer server calls. An example of implicit control will be demonstrated in the next block.

## Huge Selects {#huge-selects}

To perform `SELECT` operations with an unlimited number of found rows, you must also use the `transaction().execute(query)` method. As mentioned above, the result of the work is an iterator - unlike `pool.execute_with_retries(query)`, it allows you to go through the selection without first loading it into memory.

Example of a `SELECT` with unlimited data and implicit transaction control:

{% list tabs %}

- Synchronous

   ```python
   def huge_select(pool: ydb.QuerySessionPool, path: str):
       def callee(session: ydb.QuerySessionSync):
           query = f"""
           PRAGMA TablePathPrefix("{path}");
           SELECT * from episodes;
           """

           with session.transaction().execute(
               query,
               commit_tx=True,
           ) as result_sets:
               print("\n> Huge SELECT call")
               for result_set in result_sets:
                   for row in result_set.rows:
                       print("episode title:", row.title, ", air date:", row.air_date)

       return pool.retry_operation_sync(callee)
   ```

- Asynchronous

   ```python
   async def huge_select(pool: ydb.aio.QuerySessionPoolAsync, path: str):
       async def callee(session: ydb.aio.QuerySessionAsync):
           query = f"""
           PRAGMA TablePathPrefix("{path}");
           SELECT * from episodes;
           """

           async with await session.transaction().execute(
               query,
               commit_tx=True,
           ) as result_sets:
               print("\n> Huge SELECT call")
               async for result_set in result_sets:
                   for row in result_set.rows:
                       print("episode title:", row.title, ", air date:", row.air_date)

       return await pool.retry_operation_async(callee)
   ```

{% endlist %}
