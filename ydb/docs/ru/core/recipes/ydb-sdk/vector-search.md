# Векторный поиск

В данном разделе содержатся рецепты кода на разных языках программирования для решения задач векторного поиска с использованием {{ ydb-short-name }} SDK.

## Подключение к YDB

В данной секции описаны минимально необходимые действия для выполнения запросов в YDB.
Для получения более детальной информации по подключению к YDB, обратитесь к статье [Инициализация драйвера](./init.md)

{% list tabs %}

- Python

    Для выполнения запросов необходимо создать `ydb.QuerySessionPool`.

    ```python
    import ydb

    driver = ydb.Driver(
        endpoint=ydb_endpoint,
        database=ydb_database,
        credentials=ydb_credentials,
    )
    driver.wait(5, fail_fast=True)
    pool = ydb.QuerySessionPool(driver)
    ```

{% endlist %}


## Создание таблицы

В данном примере описан метод для создания таблицы для хранения документов и их векторных представлений.

Схема таблицы:

* `id` - идентификатор документа
* `document` - текст документа
* `embedding` - векторное представление документа

**Важно: хранения вектора используется тип `String`.**

Более подробно типы данных описаны в [соответствующей документации](../../yql/reference/udf/list/knn.md#data-types)

{% list tabs %}

- Python

    ```python
    def create_vector_table(pool: ydb.QuerySessionPool, table_name: str) -> None:
        query = f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            id Utf8,
            document Utf8,
            embedding String,
            PRIMARY KEY (id)
        );"""

        pool.execute_with_retries(query)

        print("Vector table created")
    ```

{% endlist %}


## Вставка векторов

В данном примере описан метод для вставки векторов в созданную ранее таблицу.
В готовом YQL запросе важно правильно преобразовать вектор в тип `String`, для этого используется [функция преобразования](../../yql/reference/udf/list/knn.md#functions-convert).

Формируемый запрос использует возможности контейнерных типов данных для возможности разовой передачи случайного количества объектов.

{% list tabs %}

- Python

    Метод принимает массив словарей `items`, где каждый словарь содержит поля `id` - идентификатор объекта, `document` - текст и `embedding` - векторное представление текста.

    Для использования структуры в примере ниже создается `items_struct_type = ydb.StructType()`, у которого указываются типы всех полей. Для передачи списка таких структур, необходимо обернуть в `ydb.ListType`: `ydb.ListType(items_struct_type)`.

    ```python
    def insert_items(
        pool: ydb.QuerySessionPool,
        table_name: str,
        items: list[dict],
    ) -> None:
        query = f"""
        DECLARE $items AS List<Struct<
            id: Utf8,
            document: Utf8,
            embedding: List<Float>
        >>;

        UPSERT INTO `{table_name}`
        (
        id,
        document,
        embedding
        )
        SELECT
            id,
            document,
            Untag(Knn::ToBinaryStringFloat(embedding), "FloatVector"),
        FROM AS_TABLE($items);
        """

        items_struct_type = ydb.StructType()
        items_struct_type.add_member("id", ydb.PrimitiveType.Utf8)
        items_struct_type.add_member("document", ydb.PrimitiveType.Utf8)
        items_struct_type.add_member("embedding", ydb.ListType(ydb.PrimitiveType.Float))

        pool.execute_with_retries(
            query, {"$items": (items, ydb.ListType(items_struct_type))}
        )

        print(f"{len(items)} items inserted")
    ```

{% endlist %}


## Добавление индекса

В данном примере описан метод для добавления векторного индекса. В методе делаются две вещи: создается временный индекс и производится атомарная замена. Данный подход позволяет создавать индекс и когда он уже существует (перестроение), и когда его еще нет.

Доступные стратегии:

* similarity=cosine
* similarity=inner_product
* distance=cosine
* distance=euclidean
* distance=manhattan

Каждая из стратегий соответствует функции, с которой будет производится дальнейший поиск. Подробнее функции описаны в [соответствующей документации](../../yql/reference/udf/list/knn.md#fuctions-distance).

Параметры, используемые при создании индекса типа `vector_kmeans_tree` описаны в [документации векторного индекса](../../dev/vector-indexes.md#kmeans-tree-type).


{% list tabs %}

- Python

    ```python
    def add_index(
        pool: ydb.QuerySessionPool,
        driver: ydb.Driver,
        table_name: str,
        index_name: str,
        strategy: str,
        dim: int,
        levels: int = 2,
        clusters: int = 128,
    ):
        query = f"""
        ALTER TABLE `{table_name}`
        ADD INDEX {index_name}__temp
        GLOBAL USING vector_kmeans_tree
        ON (embedding)
        WITH (
            {strategy},
            vector_type="Float",
            vector_dimension={dim},
            levels={levels},
            clusters={clusters}
        );
        """

        pool.execute_with_retries(query)
        driver.table_client.alter_table(
            f"{driver._driver_config.database}/{table_name}",
            rename_indexes=[
                ydb.RenameIndexItem(
                    source_name=f"{index_name}__temp",
                    destination_name=f"{index_name}",
                    replace_destination=True,
                ),
            ],
        )

        print(f"Table index {index_name} created.")
    ```

{% endlist %}

## Поиск по вектору

В данном примере описан метод для поиска документов по вектору. Метод возвращает словарь, включающий в себя значение колонок `id`, `document`, а также `score` - вывод функции поиска.

Метод требует указать стратегию (функцию) поиска. Возможные значения:

* CosineSimilarity
* InnerProductSimilarity
* CosineDistance
* ManhattanDistance
* EuclideanDistance

Подробнее функции описаны в [соответствующей документации](../../yql/reference/udf/list/knn.md#fuctions-distance).

Метод позволяет указать имя индекса - если имя указано, в запрос будет добавлено выражение вида `VIEW index_name`, которое позволит использовать преимущества векторных индексов.

{% list tabs %}

- Python

    ```python
    def search_items(
        pool: ydb.QuerySessionPool,
        table_name: str,
        embedding: list[float],
        strategy: str = "CosineSimilarity",
        limit: int = 1,
        index_name: str | None = None,
    ) -> list[dict]:
        view_index = "" if not index_name else f"VIEW {index_name}"

        sort_order = "DESC" if strategy.endswith("Similarity") else "ASC"

        query = f"""
        DECLARE $embedding as List<Float>;

        $TargetEmbedding = Knn::ToBinaryStringFloat($embedding);

        SELECT
            id,
            document,
            Knn::{strategy}(embedding, $TargetEmbedding) as score
        FROM {table_name} {view_index}
        ORDER BY score
        {sort_order}
        LIMIT {limit};
        """

        result = pool.execute_with_retries(
            query,
            {
                "$embedding": (embedding, ydb.ListType(ydb.PrimitiveType.Float)),
            },
        )

        items = []

        for result_set in result:
            for row in result_set.rows:
                items.append(
                    {
                        "id": row["id"],
                        "document": row["document"],
                        "score": row["score"],
                    }
                )

        return items
    ```

{% endlist %}

## Пример использования

В данном разделе указан пример использования вышеупомянутых методов, состоящий из следующих шагов:

1. Удаление существующей таблицы
2. Создание новой таблицы
3. Вставка объектов
4. Поиск ближайших векторов без использования индекса
5. Добавление векторного индекса
6. Поиск ближайших векторов с использованем индекса

- Python

    Пример использования

    ```python
    def print_results(items):
        if len(items) == 0:
            print("No items found")
            return

        for item in items:
            print(f"[score={item['score']}] {item['id']}: {item['document']}")

    def drop_vector_table_if_exists(pool: ydb.QuerySessionPool, table_name: str) -> None:
        pool.execute_with_retries(f"DROP TABLE IF EXISTS `{table_name}`")

        print("Vector table dropped")

    def main(
        ydb_endpoint: str,
        ydb_database: str,
        ydb_credentials: ydb.AbstractCredentials,
        table_name: str,
        index_name: str,
    ):
        driver = ydb.Driver(
            endpoint=ydb_endpoint,
            database=ydb_database,
            credentials=ydb_credentials,
        )
        driver.wait(5, fail_fast=True)
        pool = ydb.QuerySessionPool(driver)

        drop_vector_table_if_exists(pool, table_name)

        create_vector_table(pool, table_name)

        items = [
            {"id": "1", "document": "vector 1", "embedding": [0.98, 0.1, 0.01]},
            {"id": "2", "document": "vector 2", "embedding": [1.0, 0.05, 0.05]},
            {"id": "3", "document": "vector 3", "embedding": [0.9, 0.1, 0.1]},
            {"id": "4", "document": "vector 4", "embedding": [0.03, 0.0, 0.99]},
            {"id": "5", "document": "vector 5", "embedding": [0.0, 0.0, 0.99]},
            {"id": "6", "document": "vector 6", "embedding": [0.0, 0.02, 1.0]},
            {"id": "7", "document": "vector 7", "embedding": [0.0, 1.05, 0.05]},
            {"id": "8", "document": "vector 8", "embedding": [0.02, 0.98, 0.1]},
            {"id": "9", "document": "vector 9", "embedding": [0.0, 1.0, 0.05]},
        ]

        insert_items(pool, table_name, items)

        items = search_items(
            pool,
            table_name,
            embedding=[1, 0, 0],
            strategy="CosineSimilarity",
            limit=3,
        )
        print_results(items)

        add_vector_index(
            pool,
            driver,
            table_name,
            index_name=index_name,
            strategy="similarity=cosine",
            dim=3,
            levels=1,
            clusters=3,
        )

        items = search_items(
            pool,
            table_name,
            embedding=[1, 0, 0],
            index_name=index_name,
            strategy="CosineSimilarity",
            limit=3,
        )
        print_results(items)

        pool.stop()
        driver.stop()
    ```

    Вывод программы:

    ```bash
    Vector table dropped
    Vector table created
    9 items inserted
    [score=0.997509241104126] 2: vector 2
    [score=0.9947828650474548] 1: vector 1
    [score=0.9878783822059631] 3: vector 3
    Table index ydb_vector_index created.
    [score=0.997509241104126] 2: vector 2
    [score=0.9947828650474548] 1: vector 1
    [score=0.9878783822059631] 3: vector 3
    ```

    Полный код программы доступен по [ссылке](https://github.com/ydb-platform/ydb/blob/main/ydb/public/sdk/python/examples/vector_search/vector_search.py).

{% endlist %}
