# {{ airflow-name }}

Для работы под управлением [{{ airflow-name }}](https://airflow.apache.org) {{ ydb-full-name }} предоставляет пакет [apache-airflow-providers-ydb](https://pypi.org/project/apache-airflow-providers-ydb/). [Задания {{ airflow-name }}](https://airflow.apache.org/docs/apache-airflow/stable/index.html) представляют собой приложения на языке Python, состоящие из набора [операторов {{ airflow-name }}](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/operators.html) и их [зависимостей](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html), определяющих порядок выполнения. Для выполнения запросов к {{ ydb-full-name }} в составе пакета содержится оператор {{ airflow-name }} [`YDBExecuteQueryOperator`](https://airflow.apache.org/docs/apache-airflow-providers-ydb/stable/_api/airflow/providers/ydb/operators/ydb/index.html) и хук [`YDBHook`](https://airflow.apache.org/docs/apache-airflow-providers-ydb/stable/_api/airflow/providers/ydb/hooks/ydb/index.html).

{% cut "Пример" %}

В примере ниже создается задание `create_pet_table`, создающее таблицу в {{ ydb-full-name }}. После успешного создания таблицы вызывается задание `populate_pet_table`, заполняющее таблицу данными с помощью команд `UPSERT` и задание `populate_pet_table_via_bulk_upsert`, заполняющее таблицу с помощью команд пакетной вставки данных [`bulk_upsert`](../recipes/ydb-sdk/bulk-upsert.md). После выполнения вставки данных выполняется операция чтения с помощью задания `get_all_pets` и задание для параметризованного чтения данных с помощью задания `get_birth_date`.

```python
from __future__ import annotations

import datetime
import os

import ydb
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.decorators import task
from airflow.providers.ydb.hooks.ydb import YDBHook
from airflow.providers.ydb.operators.ydb import YDBExecuteQueryOperator

@task
def populate_pet_table_via_bulk_upsert():
    hook = YDBHook(ydb_conn_id="test_ydb_connection")
    column_types = (
        ydb.BulkUpsertColumns()
        .add_column("pet_id", ydb.OptionalType(ydb.PrimitiveType.Int32))
        .add_column("name", ydb.PrimitiveType.Utf8)
        .add_column("pet_type", ydb.PrimitiveType.Utf8)
        .add_column("birth_date", ydb.PrimitiveType.Utf8)
        .add_column("owner", ydb.PrimitiveType.Utf8)
    )

    rows = [
        {"pet_id": 3, "name": "Lester", "pet_type": "Hamster", "birth_date": "2020-06-23", "owner": "Lily"},
        {"pet_id": 4, "name": "Quincy", "pet_type": "Parrot", "birth_date": "2013-08-11", "owner": "Anne"},
    ]
    hook.bulk_upsert("pet", rows=rows, column_types=column_types)


with DAG(
    dag_id="ydb_operator_dag",
    start_date=datetime.datetime(2020, 2, 2),
    schedule="@once",
    catchup=False,
) as dag:
    create_pet_table = YDBExecuteQueryOperator(
        task_id="create_pet_table",
        sql="""
            CREATE TABLE pet (
            pet_id INT,
            name TEXT NOT NULL,
            pet_type TEXT NOT NULL,
            birth_date TEXT NOT NULL,
            owner TEXT NOT NULL,
            PRIMARY KEY (pet_id)
            );
          """,
        is_ddl=True,  # must be specified for DDL queries
        ydb_conn_id="test_ydb_connection"
    )

    populate_pet_table = YDBExecuteQueryOperator(
        task_id="populate_pet_table",
        sql="""
              UPSERT INTO pet (pet_id, name, pet_type, birth_date, owner)
              VALUES (1, 'Max', 'Dog', '2018-07-05', 'Jane');

              UPSERT INTO pet (pet_id, name, pet_type, birth_date, owner)
              VALUES (2, 'Susie', 'Cat', '2019-05-01', 'Phil');
            """,
        ydb_conn_id="test_ydb_connection"
    )

    get_all_pets = YDBExecuteQueryOperator(task_id="get_all_pets", sql="SELECT * FROM pet;", ydb_conn_id="test_ydb_connection")

    get_birth_date = YDBExecuteQueryOperator(
        task_id="get_birth_date",
        sql="SELECT * FROM pet WHERE birth_date BETWEEN '{{params.begin_date}}' AND '{{params.end_date}}'",
        params={"begin_date": "2020-01-01", "end_date": "2020-12-31"},
        ydb_conn_id="test_ydb_connection"
    )

    (
        create_pet_table
        >> populate_pet_table
        >> populate_pet_table_via_bulk_upsert()
        >> get_all_pets
        >> get_birth_date
    )
```

{% endcut %}

## Установка {#setup}

Для установки необходимо выполнить следующие команды:

```shell
pip install ydb
pip install apache-airflow-providers-ydb
```

## Объектная модель {#object_model}

Пакет `airflow.providers.ydb` содержит набор компонентов для взаимодействия с {{ ydb-full-name }}:
- Оператор [YDBExecuteQueryOperator](#ydb_execute_query_operator) для интеграции задач в планировщик {{ airflow-name }}.
- Хук [YDBHook](#ydb_hook) для прямого взаимодействия с {{ ydb-name }}.

### YDBExecuteQueryOperator {#ydb_execute_query_operator}

Для выполнения запросов к {{ ydb-full-name }} используется {{ airflow-name }} оператор `YDBExecuteQueryOperator`.

Обязательные аргументы:
* `name` — название задания {{ airflow-name }}.
* `sql` — текст SQL-запроса, который необходимо выполнить в {{ ydb-full-name }}.

Опциональные аргументы:
* `ydb_conn_id` — идентификатор подключения с типом `YDB`, содержащий параметры соединения с {{ ydb-full-name }}. Если не указан, то используется соединение с именем `ydb_default`. Соединение `ydb_default` предустанавливается в составе {{ airflow-name }}, отдельно его заводить не нужно.
* `is_ddl` — признак, что выполняется [SQL DDL](https://en.wikipedia.org/wiki/Data_definition_language) запрос. Если аргумент не указан, или установлен в `False`, то будет выполняться [SQL DML](https://ru.wikipedia.org/wiki/Data_Manipulation_Language) запрос.
* `params` — словарь параметров

Пример:

```python
ydb_operator = YDBExecuteQueryOperator(task_id="ydb_operator", sql="SELECT 'Hello, world!'")
```

В данном примере создается задание {{ airflow-name }} с идентификатором `ydb_operator`, которое выполняет запрос `SELECT 'Hello, world!'`.


### YDBHook {#ydb_hook}

Для выполнения низкоуровневых команд в {{ ydb-full-name }} используется {{ airflow-name }} класс `YDBHook`.

Опциональные аргументы:
* `ydb_conn_id` — идентификатор подключения с типом `YDB`, содержащий параметры соединения с {{ ydb-full-name }}. Если не указан, то используется соединение с именем `ydb_default`. Соединение `ydb_default` предустанавливается в составе {{ airflow-name }}, отдельно его заводить не нужно.
* `is_ddl` — признак, что выполняется [SQL DDL](https://en.wikipedia.org/wiki/Data_definition_language) запрос. Если аргумент не указан, или установлен в `False`, то будет выполняться [SQL DML](https://ru.wikipedia.org/wiki/Data_Manipulation_Language) запрос.

`YDBHook` поддерживает следующие методы:
- [bulk_upsert](#bulk_upsert).
- [get_conn](#get_conn).

#### bulk_upsert {#bulk_upsert}

Выполняет [пакетную вставку данных](../recipes/ydb-sdk/bulk-upsert.md) в таблицы {{ ydb-full-name }}.

Обязательные аргументы:
* `rows` — массив строк для вставки.
* `column_types` — описание типов колонок.


Пример:

```python
hook = YDBHook(ydb_conn_id=...)
column_types = (
        ydb.BulkUpsertColumns()
        .add_column("pet_id", ydb.OptionalType(ydb.PrimitiveType.Int32))
        .add_column("name", ydb.PrimitiveType.Utf8)
        .add_column("pet_type", ydb.PrimitiveType.Utf8)
        .add_column("birth_date", ydb.PrimitiveType.Utf8)
        .add_column("owner", ydb.PrimitiveType.Utf8)
    )

rows = [
    {"pet_id": 3, "name": "Lester", "pet_type": "Hamster", "birth_date": "2020-06-23", "owner": "Lily"},
    {"pet_id": 4, "name": "Quincy", "pet_type": "Parrot", "birth_date": "2013-08-11", "owner": "Anne"},
]
hook.bulk_upsert("pet", rows=rows, column_types=column_types)
```

В данном примере создается объект `YDBHook`, через который выполняется операция пакетной вставки данных `bulk_upsert`.

#### get_conn {#get_conn}

Возвращает объект `YDBConnection`, реализующий интерфейс [`DbApiConnection`](https://peps.python.org/pep-0249/#connection-objects) для работы с данными. Класс `DbApiConnection` используется для создания стандартизированного интерфейса взаимодействия с базой данных, обеспечивающего выполнение операций, таких как подключение, выполнение SQL-запросов и управление транзакциями независимо от конкретной системы управления базами данных.

Пример:

```python
hook = YDBHook(ydb_conn_id=...)

# Выполняем SQL-запрос и получаем курсор
connection = hook.get_conn()
cursor = connection.cursor()
cursor.execute("SELECT * from pet;")

# Извлекаем результат и имена колонок
result = cursor.fetchall()
columns = [desc[0] for desc in cursor.description]

# Закрываем курсор и соединение
cursor.close()
connection.close()
```

В данном примере создается объект `YDBHook`, у созданного объекта запрашивается объект `DbApiConnection`, через который выполняется чтение данных и получение списка колонок.

## Соответствие YQL и Python-типов

Ниже приведены правила преобразования YQL-типов в Python-результаты.

### Скалярные типы {#scalars-types}

| YQL-тип | Python-тип | Пример в Python |
| --- | --- | --- |
| `Int8`, `Int16`, `Int32`, `Uint8`, `Uint16`, `Uint32`, `Int64`, `Uint64` | `int` | `647713` |
| `Bool` | `bool` | True |
| `Float`, `Double` | `double`<br/>NaN и Inf представляются в виде `None` | `7.88731023`<br/>`None` |
| `Decimal` | `Decimal` | `45.23410083` |
| `Utf8` | `str` | `Текст строки` |
| `String` | `str` <br/> `bytes`  | `Текст строки` |

### Сложные типы {#complex-types}

| YQL-тип | Python-тип | Пример в Python |
| --- | --- | --- |
| `Json`, `JsonDocument` | `str` (весь узел вставляется как строка) | `{"a":[1,2,3]}` |
| `Date`, `Datetime`, `Timestamp` | `datetime` | `2022-02-09` |

### Опциональные типы {#optional-types}

| YQL-тип | Python-тип | Пример в Python |
| --- | --- | --- |
| `Optional` | Оригинальный тип или None | ```1``` |

### Контейнеры {#containers}

| YQL-тип | Python-тип | Пример в Python |
| --- | --- | --- |
| `List<Type>` | `list` | `[1,2,3,4]` |
| `Dict<KeyType, ValueType>` | `dict` | ```{key1: value1, key2: value2}``` |
| `Set<KeyType>` | `set` | ```set(key_value1, key_value2)``` |
| `Tuple<Type1, Type2>` | `tuple` | ```(element1, element2, ..)``` |
| `Struct<Name:Utf8,Age:Int32>`| `dict` | `{ "Name": "John", "Age": 128 }` |
| `Variant<Type1, Type2>` с tuple | `list` | ```list[64563, 1]``` |
| `Variant<value:Int32,error:String>` со структурой | `dict` | ```{key1: value1, key2: value2}``` |

### Специальные типы {#special-types}

| YQL-тип | Python-тип |
| --- | --- |
| `Void`, `Null` | `None` |
| `EmptyList` | `[]` |
| `EmptyDict` | `{}` |


