# Example app in Python

This page contains a detailed description of the code of a [test app](https://github.com/ydb-platform/ydb-python-sdk/tree/master/examples/basic_example_v2) that is available as part of the {{ ydb-short-name }} [Python SDK](https://github.com/ydb-platform/ydb-python-sdk).

## Downloading and starting {#download}

The following execution scenario is based on [git](https://git-scm.com/downloads) and [Python3](https://www.python.org/downloads/). Be sure to install the [YDB Python SDK](../../../reference/ydb-sdk/install.md).

Create a working directory and use it to run from the command line the command to clone the GitHub repository and install the necessary Python packages:

```bash
git clone https://github.com/ydb-platform/ydb-python-sdk.git
python3 -m pip install iso8601
```

Next, from the same working directory, run the following command to start the test app:

{% include [run_options.md](_includes/run_options.md) %}

{% include [init.md](../_includes/steps/01_init.md) %}

App code snippet for driver initialization:

{% list tabs %}

- Synchronous

   ```python
   def run(endpoint, database):
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
   async def run(endpoint, database):
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

App code snippet for session pool initialization:

{% list tabs %}

- Synchronous

   ```python
   with ydb.QuerySessionPool(driver) as pool:
       pass  # operations with pool here
   ```

- Asynchronous

   ```python
   async with ydb.aio.QuerySessionPool(driver) as pool:
       pass  # operations with pool here
   ```

{% endlist %}

## Executing queries

{{ ydb-short-name }} Python SDK supports queries described by YQL syntax.
There are two primary methods for executing queries, each with different properties and use cases:

* `pool.execute_with_retries`:

  * Buffers the entire result set in client memory.
  * Automatically retries execution in case of retriable issues.
  * Does not allow specifying a transaction execution mode.
  * Recommended for one-off queries that are expected to produce small result sets.

* `tx.execute`:

  * Returns an iterator over the query results, allowing processing of results that may not fit into client memory.
  * Retries must be handled manually via `pool.retry_operation_sync`.
  * Allows specifying a transaction execution mode.
  * Recommended for scenarios where `pool.execute_with_retries` is insufficient.

{% include [create_table.md](../_includes/steps/02_create_table.md) %}

To execute `CREATE TABLE` queries, use the `pool.execute_with_retries()` method:

{% list tabs %}

- Synchronous

   ```python
   def create_tables(pool: ydb.QuerySessionPool):
       print("\nCreating table series...")
       pool.execute_with_retries(
           """
           CREATE TABLE `series` (
               `series_id` Int64,
               `title` Utf8,
               `series_info` Utf8,
               `release_date` Date,
               PRIMARY KEY (`series_id`)
           )
           """
       )

       print("\nCreating table seasons...")
       pool.execute_with_retries(
           """
           CREATE TABLE `seasons` (
               `series_id` Int64,
               `season_id` Int64,
               `title` Utf8,
               `first_aired` Date,
               `last_aired` Date,
               PRIMARY KEY (`series_id`, `season_id`)
           )
           """
       )

       print("\nCreating table episodes...")
       pool.execute_with_retries(
           """
           CREATE TABLE `episodes` (
               `series_id` Int64,
               `season_id` Int64,
               `episode_id` Int64,
               `title` Utf8,
               `air_date` Date,
               PRIMARY KEY (`series_id`, `season_id`, `episode_id`)
           )
           """
       )
   ```

- Asynchronous

   ```python
   async def create_tables(pool: ydb.aio.QuerySessionPool):
       print("\nCreating table series...")
       await pool.execute_with_retries(
           """
           CREATE TABLE `series` (
               `series_id` Int64,
               `title` Utf8,
               `series_info` Utf8,
               `release_date` Date,
               PRIMARY KEY (`series_id`)
           )
           """
       )

       print("\nCreating table seasons...")
       await pool.execute_with_retries(
           """
           CREATE TABLE `seasons` (
               `series_id` Int64,
               `season_id` Int64,
               `title` Utf8,
               `first_aired` Date,
               `last_aired` Date,
               PRIMARY KEY (`series_id`, `season_id`)
           )
           """
       )

       print("\nCreating table episodes...")
       await pool.execute_with_retries(
           """
           CREATE TABLE `episodes` (
               `series_id` Int64,
               `season_id` Int64,
               `episode_id` Int64,
               `title` Utf8,
               `air_date` Date,
               PRIMARY KEY (`series_id`, `season_id`, `episode_id`)
           )
           """
       )
   ```

{% endlist %}

{% include [steps/03_write_queries.md](../_includes/steps/03_write_queries.md) %}

Code snippet for data insert/update:

{% list tabs %}

- Synchronous

   ```python
   def upsert_simple(pool: ydb.QuerySessionPool):
       print("\nPerforming UPSERT into episodes...")
       pool.execute_with_retries(
           """
           UPSERT INTO episodes (series_id, season_id, episode_id, title) VALUES (2, 6, 1, "TBD");
           """
       )
   ```

- Asynchronous

   ```python
   async def upsert_simple(pool: ydb.aio.QuerySessionPool):
       print("\nPerforming UPSERT into episodes...")
       await pool.execute_with_retries(
           """
           UPSERT INTO episodes (series_id, season_id, episode_id, title) VALUES (2, 6, 1, "TBD");
           """
       )
   ```

{% endlist %}

{% include [steps/04_query_processing.md](../_includes/steps/04_query_processing.md) %}

To execute YQL queries, the `pool.execute_with_retries()` method is often sufficient.

{% list tabs %}

- Synchronous

   ```python
   def select_simple(pool: ydb.QuerySessionPool):
       print("\nCheck series table...")
       result_sets = pool.execute_with_retries(
           """
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
   async def select_simple(pool: ydb.aio.QuerySessionPool):
       print("\nCheck series table...")
       result_sets = await pool.execute_with_retries(
           """
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

As the result of executing the query, a list of `result_set` is returned, iterating on which the text is output to the console:

```bash
> SelectSimple:
series, Id: 1, title: IT Crowd, Release date: 2006-02-03
```

## Parameterized queries {#param-queries}

For parameterized query execution, `pool.execute_with_retries()` and `tx.execute()` behave similarly. To execute parameterized queries, you need to pass a dictionary with parameters to one of these functions, where each key is the parameter name, and the value can be one of the following:

1. A value of a basic Python type
2. A tuple containing the value and its type
3. A special type, `ydb.TypedValue(value=value, value_type=value_type)`

If you specify a value without an explicit type, the conversion takes place according to the following rules:

| Python type | {{ ydb-short-name }} type                     |
|------------|------------------------------|
| `int`      | `ydb.PrimitiveType.Int64`    |
| `float`    | `ydb.PrimitiveType.Double`   |
| `str`      | `ydb.PrimitiveType.Utf8`     |
| `bytes`    | `ydb.PrimitiveType.String`   |
| `bool`     | `ydb.PrimitiveType.Bool`     |
| `list`     | `ydb.ListType`               |
| `dict`     | `ydb.DictType`               |

{% note warning %}

Automatic conversion of lists and dictionaries is possible only if the structures are homogeneous. The type of nested values will be determined recursively according to the rules explained above. In case of using heterogeneous structures, requests will raise `TypeError`.

{% endnote %}

A code snippet demonstrating the parameterized query execution:

{% list tabs %}

- Synchronous

   ```python
   def select_with_parameters(pool: ydb.QuerySessionPool, series_id, season_id, episode_id):
       result_sets = pool.execute_with_retries(
           """
           DECLARE $seriesId AS Int64;
           DECLARE $seasonId AS Int64;
           DECLARE $episodeId AS Int64;

           SELECT
               title,
               air_date
           FROM episodes
           WHERE series_id = $seriesId AND season_id = $seasonId AND episode_id = $episodeId;
           """,
           {
               "$seriesId": series_id,  # data type could be defined implicitly
               "$seasonId": (season_id, ydb.PrimitiveType.Int64),  # could be defined via a tuple
               "$episodeId": ydb.TypedValue(episode_id, ydb.PrimitiveType.Int64),  # could be defined via a special class
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
   async def select_with_parameters(pool: ydb.aio.QuerySessionPool, series_id, season_id, episode_id):
       result_sets = await pool.execute_with_retries(
           """
           DECLARE $seriesId AS Int64;
           DECLARE $seasonId AS Int64;
           DECLARE $episodeId AS Int64;

           SELECT
               title,
               air_date
           FROM episodes
           WHERE series_id = $seriesId AND season_id = $seasonId AND episode_id = $episodeId;
           """,
           {
               "$seriesId": series_id,  # could be defined implicitly
               "$seasonId": (season_id, ydb.PrimitiveType.Int64),  # could be defined via a tuple
               "$episodeId": ydb.TypedValue(episode_id, ydb.PrimitiveType.Int64),  # could be defined via a special class
           },
       )

       print("\n> select_with_parameters:")
       first_set = result_sets[0]
       for row in first_set.rows:
           print("episode title:", row.title, ", air date:", row.air_date)

       return first_set
   ```

{% endlist %}

The code snippet above outputs the following text to the console:

```bash
> select_prepared_transaction:
('episode title:', u'To Build a Better Beta', ', air date:', '2016-06-05')
```

{% include [transaction_control.md](../_includes/steps/10_transaction_control.md) %}

The `session.transaction().execute()` method can also be used to execute YQL queries. Unlike `pool.execute_with_retries`, this method allows explicit control of transaction execution by configuring the desired transaction mode using the `TxControl` class.

Available transaction modes:
* `ydb.QuerySerializableReadWrite()` (default);
* `ydb.QueryOnlineReadOnly(allow_inconsistent_reads=False)`;
* `ydb.QuerySnapshotReadOnly()`;
* `ydb.QueryStaleReadOnly()`.

For more information about transaction modes, see [{#T}](../../../concepts/transactions.md#modes).

The result of executing `tx.execute()` is an iterator. This iterator allows you to read result rows without loading the entire result set into memory. However, the iterator must be read to the end after each request to correctly maintain the transaction state on the {{ ydb-short-name }} server side. If this is not done, write queries could not be applied on the {{ ydb-short-name }} server side. For convenience, the result of the `tx.execute()` function can be used as a context manager that automatically iterates to the end upon exit.

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

The code snippet below demonstrates the explicit use of `transaction().begin()` and `tx.commit()`:

{% list tabs %}

- Synchronous

   ```python
   def explicit_transaction_control(pool: ydb.QuerySessionPool, series_id, season_id, episode_id):
       def callee(session: ydb.QuerySession):
           query = """
           DECLARE $seriesId AS Int64;
           DECLARE $seasonId AS Int64;
           DECLARE $episodeId AS Int64;

           UPDATE episodes
           SET air_date = CurrentUtcDate()
           WHERE series_id = $seriesId AND season_id = $seasonId AND episode_id = $episodeId;
           """

           # Get newly created transaction id
           tx = session.transaction().begin()

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
       pool: ydb.aio.QuerySessionPool, series_id, season_id, episode_id
   ):
       async def callee(session: ydb.aio.QuerySession):
           query = """
           DECLARE $seriesId AS Int64;
           DECLARE $seasonId AS Int64;
           DECLARE $episodeId AS Int64;

           UPDATE episodes
           SET air_date = CurrentUtcDate()
           WHERE series_id = $seriesId AND season_id = $seasonId AND episode_id = $episodeId;
           """

           # Get newly created transaction id
           tx = await session.transaction().begin()

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

However, a transaction can be opened implicitly with the first request and can be committed automatically by setting the `commit_tx=True` flag in arguments. Implicit transaction management is preferable because it requires fewer server calls.

## Iterating over query results {#iterating}

If a `SELECT` query is expected to return a potentially large number of rows, it is recommended to use the `tx.execute` method instead of `pool.execute_with_retries` to avoid excessive memory consumption on the client side. Instead of buffering the entire result set into memory, `tx.execute` returns an iterator for each top-level `SELECT` statement in the query.

Example of a `SELECT` with unlimited data and implicit transaction control:

{% list tabs %}

- Synchronous

   ```python
   def huge_select(pool: ydb.QuerySessionPool):
       def callee(session: ydb.QuerySession):
           query = """SELECT * from episodes;"""

           with session.transaction(ydb.QuerySnapshotReadOnly()).execute(
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
   async def huge_select(pool: ydb.aio.QuerySessionPool):
       async def callee(session: ydb.aio.QuerySession):
           query = """SELECT * from episodes;"""

           async with await session.transaction(ydb.QuerySnapshotReadOnly()).execute(
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
