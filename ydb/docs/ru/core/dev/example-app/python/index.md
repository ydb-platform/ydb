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

{% include [create_table.md](../_includes/steps/02_create_table.md) %}

Для создания таблиц используется метод `pool.execute_with_retries()`:

```python
def create_tables(pool, path):
    print("\nCreating table series...")
    pool.execute_with_retries(
        """
            PRAGMA TablePathPrefix("{}");
            CREATE table `series` (
                `series_id` Uint64,
                `title` Utf8,
                `series_info` Utf8,
                `release_date` Uint64,
                PRIMARY KEY (`series_id`)
            )
            """.format(
            path
        )
    )

    print("\nCreating table seasons...")
    pool.execute_with_retries(
        """
            PRAGMA TablePathPrefix("{}");
            CREATE table `seasons` (
                `series_id` Uint64,
                `season_id` Uint64,
                `title` Utf8,
                `first_aired` Uint64,
                `last_aired` Uint64,
                PRIMARY KEY (`series_id`, `season_id`)
            )
            """.format(
            path
        )
    )

    print("\nCreating table episodes...")
    pool.execute_with_retries(
        """
            PRAGMA TablePathPrefix("{}");
            CREATE table `episodes` (
                `series_id` Uint64,
                `season_id` Uint64,
                `episode_id` Uint64,
                `title` Utf8,
                `air_date` Uint64,
                PRIMARY KEY (`series_id`, `season_id`, `episode_id`)
            )
            """.format(
            path
        )
    )

```

В параметр path передаётся абсолютный путь от корня:

```python
full_path = os.path.join(database, path)
```

Функция `pool.execute_with_retries(query)`, в отличие от `tx.execute()`, загружает в память результат запроса до его возвращения клиенту.
Благодаря этому нет необходимости использовать специальные контрукции для контроля над стримом, однако нужно с осторожностью пользоваться данным методом с большими `SELECT` запросами.
Подробнее про стримы будет сказано ниже.

## Работа со стримами {#work-with-streams}

Результатом выполнения `tx.execute()` является стрим. Стрим позволяет считать неограниченное количество строк и объем данных, не загружая в память весь результат. Однако, для корректного сохранения состояния транзакции на стороне `ydb`
стрим необходимо прочитывать до конца после каждого запроса. Для удобства результат функции `tx.execute()` представлен в виде контекстного менеджера, который долистывает стрим до конца после выхода.

```python
with tx.execute(query) as _:
    pass
```

{% include [steps/03_write_queries.md](../_includes/steps/03_write_queries.md) %}

Фрагмент кода, демонстрирующий выполнение запроса на запись/изменение данных:

```python
def upsert_simple(pool, path):
    print("\nPerforming UPSERT into episodes...")

    def callee(session):
        with session.transaction().execute(
            """
            PRAGMA TablePathPrefix("{}");
            UPSERT INTO episodes (series_id, season_id, episode_id, title) VALUES (2, 6, 1, "TBD");
            """.format(
                path
            ),
            commit_tx=True,
        ) as _:
            pass

    return pool.retry_operation_sync(callee)
```

{% include [pragmatablepathprefix.md](../_includes/auxilary/pragmatablepathprefix.md) %}

{% include [steps/04_query_processing.md](../_includes/steps/04_query_processing.md) %}

Для выполнения YQL-запросов используется метод `session.transaction().execute()`.
SDK позволяет в явном виде контролировать выполнение транзакций и настраивать необходимый режим выполнения транзакций с помощью класса `TxControl`.

В фрагменте кода, приведенном ниже, транзакция выполняется с помощью метода `transaction().execute()`. Устанавливается режим выполнения транзакции `ydb.QuerySerializableReadWrite()`. После завершения всех запросов транзакции она будет автоматически завершена с помощью явного указания флага: `commit_tx=True`. Тело запроса описано с помощью синтаксиса YQL и как параметр передается методу `execute`.

```python
def select_simple(pool, path):
    print("\nCheck series table...")

    def callee(session):
        # new transaction in serializable read write mode
        # if query successfully completed you will get result sets.
        # otherwise exception will be raised
        with session.transaction(ydb.QuerySerializableReadWrite()).execute(
            """
            PRAGMA TablePathPrefix("{}");
            $format = DateTime::Format("%Y-%m-%d");
            SELECT
                series_id,
                title,
                $format(DateTime::FromSeconds(CAST(DateTime::ToSeconds(DateTime::IntervalFromDays(CAST(release_date AS Int16))) AS Uint32))) AS release_date
            FROM series
            WHERE series_id = 1;
            """.format(
                path
            ),
            commit_tx=True,
        ) as result_sets:
            first_set = next(result_sets)
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

    return pool.retry_operation_sync(callee)
```

В качестве результата выполнения запроса возвращается `result_set`, итерирование по которому выводит на консоль текст:

```bash
> SelectSimple:
series, Id: 1, title: IT Crowd, Release date: 2006-02-03
```

## Параметризованные запросы {#param-queries}

Для выполнения параметризованных запросов в метод `tx.execute()` необходимо передать словарь с параметрами специального вида, где ключом служит имя параметра, а значение может быть одним из следующих:
1. Обычное значение (без указывания типов допустимо использовать только int, str, bool);
2. Кортеж со значением и типом;
3. Специальный тип ydb.TypedValue(value=value, value_type=value_type).

Фрагмент кода, демонстрирующий возможность использования параметризованных запросов:

```python
def select_with_parameters(pool, path, series_id, season_id, episode_id):
    def callee(session):
        query = """
        PRAGMA TablePathPrefix("{}");
        $format = DateTime::Format("%Y-%m-%d");
        SELECT
            title,
            $format(DateTime::FromSeconds(CAST(DateTime::ToSeconds(DateTime::IntervalFromDays(CAST(air_date AS Int16))) AS Uint32))) AS air_date
        FROM episodes
        WHERE series_id = $seriesId AND season_id = $seasonId AND episode_id = $episodeId;
        """.format(
            path
        )

        with session.transaction(ydb.QuerySerializableReadWrite()).execute(
            query,
            {
                "$seriesId": (series_id, ydb.PrimitiveType.Uint64),
                "$seasonId": (season_id, ydb.PrimitiveType.Uint64),  # could be defined via tuple
                "$episodeId": ydb.TypedValue(
                    episode_id, ydb.PrimitiveType.Uint64
                ),  # could be defined via special class
            },
            commit_tx=True,
        ) as result_sets:
            print("\n> select_prepared_transaction:")
            first_set = next(result_sets)
            for row in first_set.rows:
                print("episode title:", row.title, ", air date:", row.air_date)

            return first_set

    return pool.retry_operation_sync(callee)
```

Приведенный фрагмент кода при запуске выводит на консоль текст:

```bash
> select_prepared_transaction:
('episode title:', u'To Build a Better Beta', ', air date:', '2016-06-05')
```


{% include [transaction_control.md](../_includes/steps/10_transaction_control.md) %}

Фрагмент кода, демонстрирующий явное использование вызовов `transaction().begin()` и `tx.commit()`:

```python
def explicit_tcl(pool, path, series_id, season_id, episode_id):
    def callee(session):
        query = """
        PRAGMA TablePathPrefix("{}");
        UPDATE episodes
        SET air_date = CAST(CurrentUtcDate() AS Uint64)
        WHERE series_id = $seriesId AND season_id = $seasonId AND episode_id = $episodeId;
        """.format(
            path
        )

        # Get newly created transaction id
        tx = session.transaction(ydb.QuerySerializableReadWrite()).begin()

        # Execute data query.
        # Transaction control settings continues active transaction (tx)
        with tx.execute(
            query,
            {
                "$seriesId": (series_id, ydb.PrimitiveType.Uint64),
                "$seasonId": (season_id, ydb.PrimitiveType.Uint64),
                "$episodeId": (episode_id, ydb.PrimitiveType.Uint64),
            },
        ) as _:
            pass

        print("\n> explicit TCL call")

        # Commit active transaction(tx)
        tx.commit()

    return pool.retry_operation_sync(callee)
```

