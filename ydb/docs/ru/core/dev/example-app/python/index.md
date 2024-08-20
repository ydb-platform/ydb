# Приложение на Python

На этой странице подробно разбирается код [тестового приложения](https://github.com/ydb-platform/ydb-python-sdk/tree/master/examples/basic_example_v2), доступного в составе [Python SDK](https://github.com/ydb-platform/ydb-python-sdk) {{ ydb-short-name }}.

## Скачивание и запуск {#download}

Приведенный ниже сценарий запуска использует [git](https://git-scm.com/downloads) и [Python3](https://www.python.org/downloads/). Предварительно должен быть установлен [YDB Python SDK](../../../reference/ydb-sdk/install.md).

Создайте рабочую директорию и выполните в ней из командной строки команды клонирования репозитория с github.com и установки необходимых пакетов Python:

``` bash
git clone https://github.com/ydb-platform/ydb-python-sdk.git
python3 -m pip install iso8601
```

Далее из этой же рабочей директории выполните команду запуска тестового приложения, которая будет отличаться в зависимости от того, к какой базе данных необходимо подключиться.

{% include [run_options.md](_includes/run_options.md) %}

{% include [init.md](../_includes/steps/01_init.md) %}

Фрагмент кода приложения для инициализации драйвера:

{% list tabs %}

- Синхронный

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

- Асинхронный

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

Для выполнения YQL запросов используется метод `pool.execute_with_retries()`. Например, можно создать строковые таблицы:

{% list tabs %}

- Синхронный

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

- Асинхронный

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

В параметр path передаётся абсолютный путь от корня:

```python
full_path = os.path.join(database, path)
```

Функция `pool.execute_with_retries(query)`, в отличие от `tx.execute()`, загружает в память результат запроса перед его возвращением клиенту. Благодаря этому отпадает необходимость использования специальных конструкций для контроля над стримом, однако необходимо с осторожностью применять этот метод для больших запросов `SELECT`. Подробнее о стримах будет сказано ниже.

{% include [steps/03_write_queries.md](../_includes/steps/03_write_queries.md) %}

Фрагмент кода, демонстрирующий выполнение запроса на запись/изменение данных:

{% list tabs %}

- Синхронный

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

- Асинхронный

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

Для выполнения YQL-запросов чаще всего достаточно использования уже знакомого метода `pool.execute_with_retries()`.

{% list tabs %}

- Синхронный

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

- Асинхронный

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

В качестве результата выполнения запроса возвращается `result_set`, итерирование по которому выводит на консоль текст:

```bash
> SelectSimple:
series, Id: 1, title: IT Crowd, Release date: 2006-02-03
```

## Параметризованные запросы {#param-queries}

Для выполнения параметризованных запросов в метод `pool.execute_with_retries()` (или `tx.execute()`, работа с которым будет показана в следующей секции) необходимо передать словарь с параметрами специального вида, где ключом служит имя параметра, а значение может быть одним из следующих:
1. Обычное значение;
2. Кортеж со значением и типом;
3. Специальный тип `ydb.TypedValue(value=value, value_type=value_type)`.

В случае указания значения без типа, конвертация происходит по следующим правилам:
* `int` -> `ydb.PrimitiveType.Int64`
* `float` -> `ydb.PrimitiveType.Double`
* `str` -> `ydb.PrimitiveType.Utf8`
* `bytes` -> `ydb.PrimitiveType.String`
* `bool` -> `ydb.PrimitiveType.Bool`
* `list` -> `ydb.ListType`
* `dict` -> `ydb.DictType`

Автоматическая конвертация списков и словарей возможна только в случае однородных структур, тип вложенного значения будет вычисляться рекурсивно по вышеупомянутым правилам.

Фрагмент кода, демонстрирующий возможность использования параметризованных запросов:

{% list tabs %}

- Синхронный

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

- Асинхронный

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

Приведенный фрагмент кода при запуске выводит на консоль текст:

```bash
> select_prepared_transaction:
('episode title:', u'To Build a Better Beta', ', air date:', '2016-06-05')
```


{% include [transaction_control.md](../_includes/steps/10_transaction_control.md) %}

Для выполнения YQL-запросов также может использоваться метод `session.transaction().execute()`.
Данный способ, в отличие от `pool.execute_with_retries` позволяет в явном виде контролировать выполнение транзакций и настраивать необходимый режим выполнения транзакций с помощью класса `TxControl`.

Доступные режимы транзакции:
* `ydb.QuerySerializableReadWrite()` (по умолчанию);
* `ydb.QueryOnlineReadOnly(allow_inconsistent_reads=False)`;
* `ydb.QuerySnapshotReadOnly()`;
* `ydb.QueryStaleReadOnly()`.

Подробнее про режимы транзакций описано в [документации YDB](https://ydb.tech/docs/ru/concepts/transactions#modes).

Результатом выполнения `tx.execute()` является итератор. Итератор позволяет считать неограниченное количество строк и объем данных, не загружая в память весь результат.
Однако, для корректного сохранения состояния транзакции на стороне `ydb` итератор необходимо прочитывать до конца после каждого запроса.
Для удобства результат функции `tx.execute()` представлен в виде контекстного менеджера, который долистывает итератор до конца после выхода.

{% list tabs %}

- Синхронный

   ```python
   with tx.execute(query) as _:
       pass
   ```

- Асинхронный

   ```python
   async with await tx.execute(query) as _:
       pass
   ```

{% endlist %}

В фрагменте кода, приведенном ниже, транзакция выполняется с помощью метода `transaction().execute()`. Устанавливается режим выполнения транзакции `ydb.QuerySerializableReadWrite()`.
Тело запроса описано с помощью синтаксиса YQL и как параметр передается методу `execute`.

Фрагмент кода, демонстрирующий явное использование вызовов `transaction().begin()` и `tx.commit()`:

{% list tabs %}

- Синхронный

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

- Асинхронный

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

Однако стоит помнить, что транзакция может быть открыта неявно при первом запросе. Завершиться же она может автоматически с явным указанием флага `commit_tx=True`.
Неявное управление транзакцией предпочтительно, так как требует меньше обращений к серверу.
