# Векторный поиск

В данном разделе содержатся рецепты кода на разных языках программирования для решения задач [векторного поиска](../../concepts/query_execution/vector_search.md) с использованием {{ ydb-short-name }} SDK.

Подробно будут разобраны операции:

* [Подключение к YDB](#connect-ydb)
* [Создание таблицы для хранения векторов](#create-table)
* [Вставка векторов в таблицу](#insert-vectors)
* [Добавление векторного индекса](#add-vector-index)
* [Поиск ближайших векторов](#search-by-vector)

В данном рецепте будет создано хранилище текстов со следующей структурой:

|Поле|Пояснение|
|---|---|
|`id`|Идентификатор текста|
|`document`|Текст|
|`embedding`|Векторное представление текста|

В рецепте предполагается, что `embedding` уже имеется.

## Подключение к {{ ydb-short-name }} {#connect-ydb}

В данной секции описаны минимально необходимые действия для выполнения запросов в {{ ydb-short-name }}.
Для получения более подробной информации о подключении к {{ ydb-short-name }} обратитесь к статье [{#T}](./init.md).

{% list tabs %}

- Python

    Для выполнения запросов необходимо создать `ydb.QuerySessionPool`.

    ```python
    driver = ydb.Driver(
        endpoint=ydb_endpoint,
        database=ydb_database,
        credentials=ydb_credentials,
    )
    driver.wait(5, fail_fast=True)
    pool = ydb.QuerySessionPool(driver)
    ```

- C++

    ```cpp
    auto driverConfig = NYdb::CreateFromEnvironment(endpoint + "/?database=" + database);
    NYdb::TDriver driver(driverConfig);
    NYdb::NQuery::TQueryClient client(driver);
    ```

{% endlist %}


## Создание таблицы {#create-table}

Сначала необходимо создать таблицу для хранения документов и их векторных представлений.

Структура таблицы:

|Название столбца|Тип данных|Пояснение|
|---|----|------|
|`id`|`Utf8`|идентификатор документа|
|`document`|`Utf8`|текст документа|
|`embedding`|`String`|векторное представление документа|

{% note warning %}

Для хранения вектора используется тип `String`. Подробнее смотрите в документации по [точному векторному поиску](../../yql/reference/udf/list/knn.md#data-types).

{% endnote %}


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

        print(f"Vector table {table_name} created")
    ```

- C++

    ```cpp
    void CreateVectorTable(NYdb::NQuery::TQueryClient& client, const std::string& tableName)
    {
        std::string query = std::format(R"(
            CREATE TABLE IF NOT EXISTS `{}` (
                id Utf8,
                document Utf8,
                embedding String,
                PRIMARY KEY (id)
            ))", tableName);

        NYdb::NStatusHelpers::ThrowOnError(client.RetryQuerySync([&](NYdb::NQuery::TSession session) {
            return session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        }));

        std::cout << "Vector table created: " << tableName << std::endl;
    }
    ```

{% endlist %}


## Вставка векторов {#insert-vectors}

Для вставки векторов необходимо подготовить и выполнить правильный YQL-запрос. Для унификации вставки разных данных он параметризован.

Запрос оперирует контейнерным типом данных `List<Struct<...>>` (список структур), что позволяет передавать через параметры произвольное количество объектов за один раз.

В {{ ydb-short-name }} таблицах же вектора хранятся в виде сериализованной последовательности байт. Конвертацию в такое представление **рекомендуется выполнять на клиенте**. Альтернативный способ — делегировать конвертацию на сервер с помощью функции преобразования [Knn UDF](../../yql/reference/udf/list/knn.md#functions-convert). Ниже будут приведены примеры, демонстрирующие оба подхода.

{% list tabs %}

- Python

    Метод принимает массив словарей `items`, где каждый словарь содержит поля `id` - идентификатор, `document` - текст, `embedding` - векторное представление текста, заранее сериализованное в последовательность байт.

    Для использования структуры в примере ниже создается `items_struct_type = ydb.StructType()`, в котором задаются типы всех полей. Для передачи списка таких структур его необходимо обернуть в `ydb.ListType`: `ydb.ListType(items_struct_type)`.

    ```python
    import struct

    def convert_vector_to_bytes(vector: list[float]) -> bytes:
        b = struct.pack("f" * len(vector), *vector)
        return b + b"\x01"

    def insert_items_vector_as_bytes(
        pool: ydb.QuerySessionPool,
        table_name: str,
        items: list[dict],
    ) -> None:
        query = f"""
        DECLARE $items AS List<Struct<
            id: Utf8,
            document: Utf8,
            embedding: String
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
            embedding,
        FROM AS_TABLE($items);
        """

        items_struct_type = ydb.StructType()
        items_struct_type.add_member("id", ydb.PrimitiveType.Utf8)
        items_struct_type.add_member("document", ydb.PrimitiveType.Utf8)
        items_struct_type.add_member("embedding", ydb.PrimitiveType.String)

        for item in items:
            item["embedding"] = convert_vector_to_bytes(item["embedding"])

        pool.execute_with_retries(
            query, {"$items": (items, ydb.ListType(items_struct_type))}
        )

        print(f"{len(items)} items inserted")
    ```

- C++

    ```cpp
    std::string ConvertVectorToBytes(const std::vector<float>& vector)
    {
        std::string result;
        for (const auto& value : vector) {
            const char* bytes = reinterpret_cast<const char*>(&value);
            result += std::string(bytes, sizeof(float));
        }
        return result + "\x01";
    }

    void InsertItemsAsBytes(
        NYdb::NQuery::TQueryClient& client,
        const std::string& tableName,
        const std::vector<TItem>& items)
    {
        std::string query = std::format(R"(
            DECLARE $items AS List<Struct<
                id: Utf8,
                document: Utf8,
                embedding: String
            >>;
            UPSERT INTO `{0}`
            (
                id,
                document,
                embedding
            )
            SELECT
                id,
                document,
                embedding,
            FROM AS_TABLE($items);
        )", tableName);

        NYdb::TParamsBuilder paramsBuilder;
        auto& valueBuilder = paramsBuilder.AddParam("$items");
        valueBuilder.BeginList();
        for (const auto& item : items) {
            valueBuilder.AddListItem();
            valueBuilder.BeginStruct();
            valueBuilder.AddMember("id").Utf8(item.Id);
            valueBuilder.AddMember("document").Utf8(item.Document);
            valueBuilder.AddMember("embedding").String(ConvertVectorToBytes(item.Embedding));
            valueBuilder.EndStruct();
        }
        valueBuilder.EndList();
        valueBuilder.Build();

        NYdb::NStatusHelpers::ThrowOnError(client.RetryQuerySync([params = paramsBuilder.Build(), &query](NYdb::NQuery::TSession session) {
            return session.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx(NYdb::NQuery::TTxSettings::SerializableRW()).CommitTx(), params).ExtractValueSync();
        }));

        std::cout << items.size() << " items inserted" << std::endl;
    }
    ```

    {% note info %}

    В функции `ConvertVectorToBytes` подразумевается, что на клиенте используется процессор с [little-endian порядком байт](https://ru.wikipedia.org/wiki/Порядок_байтов), например x86\_64. Если используется другой порядок байт, функцию `ConvertVectorToBytes` необходимо адаптировать.

    {% endnote %}

- Python (альтернативный)

    Метод принимает массив словарей `items`, где каждый словарь содержит поля `id` - идентификатор, `document` - текст, `embedding` - векторное представление текста.

    Для использования структуры в примере ниже создается `items_struct_type = ydb.StructType()`, в котором задаются типы всех полей. Для передачи списка таких структур его необходимо обернуть в `ydb.ListType`: `ydb.ListType(items_struct_type)`.

    ```python
    def insert_items_vector_as_float_list(
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

- C++ (альтернативный)

    ```cpp
    void InsertItemsAsFloatList(
        NYdb::NQuery::TQueryClient& client,
        const std::string& tableName,
        const std::vector<TItem>& items)
    {
        std::string query = std::format(R"(
            DECLARE $items AS List<Struct<
                id: Utf8,
                document: Utf8,
                embedding: List<Float>
            >>;

            UPSERT INTO `{}`
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
        )", tableName);

        NYdb::TParamsBuilder paramsBuilder;
        auto& valueBuilder = paramsBuilder.AddParam("$items");
        valueBuilder.BeginList();
        for (const auto& item : items) {
            valueBuilder.AddListItem();
            valueBuilder.BeginStruct();
            valueBuilder.AddMember("id").Utf8(item.Id);
            valueBuilder.AddMember("document").Utf8(item.Document);
            valueBuilder.AddMember("embedding").BeginList();
            for (const auto& value : item.Embedding) {
                valueBuilder.AddListItem().Float(value);
            }
            valueBuilder.EndList();
            valueBuilder.EndStruct();
        }
        valueBuilder.EndList();
        valueBuilder.Build();

        NYdb::NStatusHelpers::ThrowOnError(client.RetryQuerySync([params = paramsBuilder.Build(), &query](NYdb::NQuery::TSession session) {
            return session.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx(NYdb::NQuery::TTxSettings::SerializableRW()).CommitTx(), params).ExtractValueSync();
        }));

        std::cout << items.size() << " items inserted" << std::endl;
    }
    ```

{% endlist %}


## Добавление индекса {#add-vector-index}

Использование векторного индекса позволяет эффективно решать задачу приближённого поиска ближайших векторов. Подробнее о преимуществах и особенностях использования описано в документации по [векторному индексу](../../dev/vector-indexes.md).

Для добавления индекса необходимо выполнить две операции:

1. Создать временный индекс;
2. Сохранить временный индекс как постоянный.

Такой подход позволяет создавать индекс как при его первоначальном создании, так и при перестроении (если индекс уже существует).

Доступные стратегии:

* `similarity=cosine`;
* `similarity=inner_product`;
* `distance=cosine`;
* `distance=euclidean`;
* `distance=manhattan`.

Каждая стратегия определяет функцию, которая будет использоваться для последующего поиска. Более подробно функции описаны в документации по [функциям расстояния и сходства](../../yql/reference/udf/list/knn.md#fuctions-distance).

Параметры, применяемые при создании индекса типа `vector_kmeans_tree`, описаны в документации [векторного индекса](../../dev/vector-indexes.md#kmeans-tree-type).


{% list tabs %}

- Python

    ```python
    def add_vector_index(
        pool: ydb.QuerySessionPool,
        driver: ydb.Driver,
        table_name: str,
        index_name: str,
        strategy: str,
        dimension: int,
        levels: int = 2,
        clusters: int = 128,
    ):
        temp_index_name = f"{index_name}__temp"
        query = f"""
        ALTER TABLE `{table_name}`
        ADD INDEX {temp_index_name}
        GLOBAL USING vector_kmeans_tree
        ON (embedding)
        WITH (
            {strategy},
            vector_type="Float",
            vector_dimension={dimension},
            levels={levels},
            clusters={clusters}
        );
        """

        pool.execute_with_retries(query)
        driver.table_client.alter_table(
            f"{driver._driver_config.database}/{table_name}",
            rename_indexes=[
                ydb.RenameIndexItem(
                    source_name=temp_index_name,
                    destination_name=f"{index_name}",
                    replace_destination=True,
                ),
            ],
        )

        print(f"Table index {index_name} created.")
    ```

- C++

    ```cpp
    void AddIndex(
        NYdb::TDriver& driver,
        NYdb::NQuery::TQueryClient& client,
        const std::string& database,
        const std::string& tableName,
        const std::string& indexName,
        const std::string& strategy,
        std::uint64_t dim,
        std::uint64_t levels,
        std::uint64_t clusters)
    {
        std::string query = std::format(R"(
            ALTER TABLE `{0}`
            ADD INDEX {1}__temp
            GLOBAL USING vector_kmeans_tree
            ON (embedding)
            WITH (
                {2},
                vector_type="Float",
                vector_dimension={3},
                levels={4},
                clusters={5}
            );
        )", tableName, indexName, strategy, dim, levels, clusters);

        NYdb::NStatusHelpers::ThrowOnError(client.RetryQuerySync([&](NYdb::NQuery::TSession session) {
            return session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        }));

        NYdb::NTable::TTableClient tableClient(driver);
        NYdb::NStatusHelpers::ThrowOnError(tableClient.RetryOperationSync([&](NYdb::NTable::TSession session) {
            return session.AlterTable(database + "/" + tableName, NYdb::NTable::TAlterTableSettings()
                .AppendRenameIndexes(NYdb::NTable::TRenameIndex{
                    .SourceName_ = indexName + "__temp",
                    .DestinationName_ = indexName,
                    .ReplaceDestination_ = true
                })
            ).ExtractValueSync();
        }));

        std::cout << "Table index `" << indexName << "` for table `" << tableName << "` added" << std::endl;
    }
    ```

{% endlist %}

## Поиск по вектору {#search-by-vector}

Для поиска документов по вектору используется специальный YQL‑запрос, в котором необходимо определить функцию сходства или расстояния.
Доступные значения:

* `CosineSimilarity`;
* `InnerProductSimilarity`;
* `CosineDistance`;
* `ManhattanDistance`;
* `EuclideanDistance`.

Подробнее функции описаны в документации по [функциям расстояния и сходства](../../yql/reference/udf/list/knn.md#fuctions-distance).

Метод позволяет указать имя индекса. Если оно задано, в запрос будет добавлено выражение `VIEW index_name`, что позволит использовать векторный индекс при поиске.

Метод возвращает список, состоящий из словарей с полями `id`, `document`, а также `score` — числом, отражающим степень сходства (или расстояния) с искомым вектором.

{% list tabs %}

- Python

    ```python
    def search_items_vector_as_bytes(
        pool: ydb.QuerySessionPool,
        table_name: str,
        embedding: list[float],
        strategy: str = "CosineSimilarity",
        limit: int = 1,
        index_name: str | None = None,
    ) -> list[dict]:
        view_index = f"VIEW {index_name}" if index_name else ""

        sort_order = "DESC" if strategy.endswith("Similarity") else "ASC"

        query = f"""
        DECLARE $embedding as String;

        SELECT
            id,
            document,
            Knn::{strategy}(embedding, $embedding) as score
        FROM {table_name} {view_index}
        ORDER BY score {sort_order}
        LIMIT {limit};
        """

        result = pool.execute_with_retries(
            query,
            {
                "$embedding": (
                    convert_vector_to_bytes(embedding),
                    ydb.PrimitiveType.String,
                ),
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

- C++

    ```cpp
    std::vector<TResultItem> SearchItemsAsBytes(
        NYdb::NQuery::TQueryClient& client,
        const std::string& tableName,
        const std::vector<float>& embedding,
        const std::string& strategy,
        std::uint64_t limit,
        const std::optional<std::string>& indexName)
    {
        std::string viewIndex = indexName ? "VIEW " + *indexName : "";
        std::string sortOrder = strategy.ends_with("Similarity") ? "DESC" : "ASC";

        std::string query = std::format(R"(
            DECLARE $embedding as String;
            SELECT
                id,
                document,
                Knn::{2}(embedding, $embedding) as score
            FROM {0} {1}
            ORDER BY score {3}
            LIMIT {4};
        )", tableName, viewIndex, strategy, sortOrder, limit);

        auto params = NYdb::TParamsBuilder()
            .AddParam("$embedding")
                .String(ConvertVectorToBytes(embedding))
                .Build()
            .Build();

        std::vector<TResultItem> result;

        NYdb::NStatusHelpers::ThrowOnError(client.RetryQuerySync([params, &query, &result](NYdb::NQuery::TSession session) {
            auto execResult = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx(NYdb::NQuery::TTxSettings::SerializableRW()).CommitTx(), params).ExtractValueSync();
            if (execResult.IsSuccess()) {
                auto parser = execResult.GetResultSetParser(0);
                while (parser.TryNextRow()) {
                    result.push_back({
                        .Id = *parser.ColumnParser(0).GetOptionalUtf8(),
                        .Document = *parser.ColumnParser(1).GetOptionalUtf8(),
                        .Score = *parser.ColumnParser(2).GetOptionalFloat()
                    });
                }
            }
            return execResult;
        }));

        return result;
    }
    ```

- Python (alternative)

    ```python
    def search_items_vector_as_float_list(
        pool: ydb.QuerySessionPool,
        table_name: str,
        embedding: list[float],
        strategy: str = "CosineSimilarity",
        limit: int = 1,
        index_name: str | None = None,
    ) -> list[dict]:
        view_index = f"VIEW {index_name}" if index_name else ""

        sort_order = "DESC" if strategy.endswith("Similarity") else "ASC"

        query = f"""
        DECLARE $embedding as List<Float>;

        $target_embedding = Knn::ToBinaryStringFloat($embedding);

        SELECT
            id,
            document,
            Knn::{strategy}(embedding, $target_embedding) as score
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

- C++ (alternative)

    ```cpp
    std::vector<TResultItem> SearchItemsAsFloatList(
        NYdb::NQuery::TQueryClient& client,
        const std::string& tableName,
        const std::vector<float>& embedding,
        const std::string& strategy,
        std::uint64_t limit,
        const std::optional<std::string>& indexName)
    {
        std::string viewIndex = indexName ? "VIEW " + *indexName : "";
        std::string sortOrder = strategy.ends_with("Similarity") ? "DESC" : "ASC";

        std::string query = std::format(R"(
            DECLARE $embedding as List<Float>;

            $TargetEmbedding = Knn::ToBinaryStringFloat($embedding);

            SELECT
                id,
                document,
                Knn::{2}(embedding, $TargetEmbedding) as score
            FROM {0} {1}
            ORDER BY score
            {3}
            LIMIT {4};
        )", tableName, viewIndex, strategy, sortOrder, limit);

        NYdb::TParamsBuilder paramsBuilder;
        auto& valueBuilder = paramsBuilder.AddParam("$embedding");
        valueBuilder.BeginList();
        for (auto value : embedding) {
            valueBuilder.AddListItem().Float(value);
        }
        valueBuilder.EndList().Build();

        std::vector<TResultItem> result;

        NYdb::NStatusHelpers::ThrowOnError(client.RetryQuerySync([params = paramsBuilder.Build(), &query, &result](NYdb::NQuery::TSession session) {
            auto execResult = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx(NYdb::NQuery::TTxSettings::SerializableRW()).CommitTx(), params).ExtractValueSync();
            if (execResult.IsSuccess()) {
                auto parser = execResult.GetResultSetParser(0);
                while (parser.TryNextRow()) {
                    result.push_back({
                        .Id = *parser.ColumnParser(0).GetOptionalUtf8(),
                        .Document = *parser.ColumnParser(1).GetOptionalUtf8(),
                        .Score = *parser.ColumnParser(2).GetOptionalFloat()
                    });
                }
            }
            return execResult;
        }));

        return result;
    }
    ```

{% endlist %}

## Итоговый пример {#full-example}

Объединим все вышеописанные методы в один пример, который включает следующие шаги:

1. Удаление существующей таблицы
2. Создание новой таблицы
3. Вставка объектов
4. Поиск ближайших векторов без использования индекса
5. Добавление векторного индекса
6. Поиск ближайших векторов с использованем индекса

{% list tabs %}

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

        insert_items_vector_as_bytes(pool, table_name, items)

        items = search_items_vector_as_bytes(
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
            dimension=3,
            levels=1,
            clusters=3,
        )

        items = search_items_vector_as_bytes(
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


    if __name__ == "__main__":
        main(
            ydb_endpoint=os.environ.get("YDB_ENDPOINT", "grpc://localhost:2136"),
            ydb_database=os.environ.get("YDB_DATABASE", "/local"),
            ydb_credentials=ydb.credentials_from_env_variables(),
            table_name="ydb_vector_search",
            index_name="ydb_vector_index",
        )
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

    В результате видно, что таблица была создана, добавлено 9 документов и успешно выполнен поиск по близости векторов — как до, так и после добавления векторного индекса.

    Полный код программы доступен по [ссылке](https://github.com/ydb-platform/ydb/blob/main/ydb/public/sdk/python/examples/vector_search/vector_search.py).

- C++

    ```cpp
    void PrintResults(const std::vector<TResultItem>& items)
    {
        if (items.empty()) {
            std::cout << "No items found" << std::endl;
            return;
        }

        for (const auto& item : items) {
            std::cout << "[score=" << item.Score << "] " << item.Id << ": " << item.Document << std::endl;
        }
    }

    void VectorExample(
        const std::string& endpoint,
        const std::string& database,
        const std::string& tableName,
        const std::string& indexName)
    {
        auto driverConfig = NYdb::CreateFromEnvironment(endpoint + "/?database=" + database);
        NYdb::TDriver driver(driverConfig);
        NYdb::NQuery::TQueryClient client(driver);

        try {
            DropVectorTable(client, tableName);
            CreateVectorTable(client, tableName);
            std::vector<TItem> items = {
                {.Id = "1", .Document = "document 1", .Embedding = {0.98, 0.1, 0.01}},
                {.Id = "2", .Document = "document 2", .Embedding = {1.0, 0.05, 0.05}},
                {.Id = "3", .Document = "document 3", .Embedding = {0.9, 0.1, 0.1}},
                {.Id = "4", .Document = "document 4", .Embedding = {0.03, 0.0, 0.99}},
                {.Id = "5", .Document = "document 5", .Embedding = {0.0, 0.0, 0.99}},
                {.Id = "6", .Document = "document 6", .Embedding = {0.0, 0.02, 1.0}},
                {.Id = "7", .Document = "document 7", .Embedding = {0.0, 1.05, 0.05}},
                {.Id = "8", .Document = "document 8", .Embedding = {0.02, 0.98, 0.1}},
                {.Id = "9", .Document = "document 9", .Embedding = {0.0, 1.0, 0.05}},
            };
            InsertItemsAsBytes(client, tableName, items);
            PrintResults(SearchItemsAsBytes(client, tableName, {1.0, 0.0, 0.0}, "CosineSimilarity", 3));
            AddIndex(driver, client, database, tableName, indexName, "similarity=cosine", 3, 1, 3);
            PrintResults(SearchItemsAsBytes(client, tableName, {1.0, 0.0, 0.0}, "CosineSimilarity", 3, indexName));
        } catch (const std::exception& e) {
            std::cerr << "Execution failed: " << e.what() << std::endl;
        }

        driver.Stop(true);
    }
    ```

    Полный код программы доступен по [ссылке](https://github.com/ydb-platform/ydb/tree/main/ydb/public/sdk/cpp/examples/vector_index_builtin).

{% endlist %}
