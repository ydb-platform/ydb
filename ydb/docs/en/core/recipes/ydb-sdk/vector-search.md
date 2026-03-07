# Vector search

This section contains code recipes in various programming languages for solving vector search tasks using the {{ ydb-short-name }} SDK.

The following operations are covered in detail:

* [Connecting to YDB](#connect-ydb)
* [Creating a table for storing vectors](#create-table)
* [Inserting vectors into the table](#insert-vectors)
* [Adding a vector index](#add-vector-index)
* [Searching for the nearest vectors](#search-by-vector)

This recipe creates a text store with the following structure:

|Field|Description|
|---|---|
|`id`|Text identifier|
|`document`|Text|
|`embedding`|Vector representation of the text|

The recipe assumes that `embedding` is already available.

## Connecting to {{ ydb-short-name }} {#connect-ydb}

This section describes the minimum steps required to execute queries in {{ ydb-short-name }}.
For more details on connecting to {{ ydb-short-name }}, refer to [{#T}](./init.md).

{% list tabs %}

- Python

    {% list tabs %}

    - Native SDK

      To execute queries, create a `ydb.QuerySessionPool`.

      ```python
      driver = ydb.Driver(
          endpoint=ydb_endpoint,
          database=ydb_database,
          credentials=ydb_credentials,
      )
      driver.wait(5, fail_fast=True)
      pool = ydb.QuerySessionPool(driver)
      ```

    - Native SDK (Asyncio)

      To execute queries, create a `ydb.aio.QuerySessionPool`:

      ```python
      import asyncio
      import ydb

      async def main():
          async with ydb.aio.Driver(
              endpoint=ydb_endpoint,
              database=ydb_database,
              credentials=ydb_credentials,
          ) as driver:
              await driver.wait(5, fail_fast=True)
              pool = ydb.aio.QuerySessionPool(driver)
              # ... use pool ...

      asyncio.run(main())
      ```

    {% endlist %}

- C++

    ```cpp
    auto driverConfig = NYdb::CreateFromEnvironment(endpoint + "/?database=" + database);
    NYdb::TDriver driver(driverConfig);
    NYdb::NQuery::TQueryClient client(driver);
    ```

{% endlist %}


## Creating a table {#create-table}

First, create a table for storing documents and their vector representations.

Table structure:

|Column name|Data type|Description|
|---|----|------|
|`id`|`Utf8`|document identifier|
|`document`|`Utf8`|document text|
|`embedding`|`String`|vector representation of the document|

{% note warning %}

The `String` type is used to store the vector. For more details, see the documentation on [exact vector search](../../yql/reference/udf/list/knn.md#data-types).

{% endnote %}


{% list tabs %}

- Python

    {% list tabs %}

    - Native SDK

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

    - Native SDK (Asyncio)

      ```python
      import ydb

      async def create_vector_table(pool: ydb.aio.QuerySessionPool, table_name: str) -> None:
          query = f"""
          CREATE TABLE IF NOT EXISTS `{table_name}` (
              id Utf8,
              document Utf8,
              embedding String,
              PRIMARY KEY (id)
          );"""

          await pool.execute_with_retries(query)

          print(f"Vector table {table_name} created")
      ```

    {% endlist %}

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


## Inserting vectors {#insert-vectors}

To insert vectors, prepare and execute the appropriate YQL query. It is parameterized to allow uniform insertion of different data.

The query uses the `List<Struct<...>>` container type (a list of structs), which lets you pass an arbitrary number of objects at once through parameters.

In {{ ydb-short-name }} tables, vectors are stored as a serialized byte sequence. Converting to this representation **is recommended to be done on the client side**. An alternative approach is to delegate the conversion to the server using the [Knn UDF](../../yql/reference/udf/list/knn.md#functions-convert) conversion function. Both approaches are demonstrated in the examples below.

{% list tabs %}

- Python

    The method accepts an array of dictionaries `items`, where each dictionary contains the fields `id` — identifier, `document` — text, `embedding` — the vector representation of the text, pre-serialized into a byte sequence.

    To use the struct in the example below, create `items_struct_type = ydb.StructType()` and define the types of all fields. To pass a list of such structs, wrap it in `ydb.ListType`: `ydb.ListType(items_struct_type)`.

    {% cut "asyncio" %}

    ```python
    import struct
    import ydb

    def convert_vector_to_bytes(vector: list[float]) -> bytes:
        b = struct.pack("f" * len(vector), *vector)
        return b + b"\x01"

    async def insert_items_vector_as_bytes(
        pool: ydb.aio.QuerySessionPool,
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

        await pool.execute_with_retries(
            query, {"$items": (items, ydb.ListType(items_struct_type))}
        )

        print(f"{len(items)} items inserted")
    ```

    {% endcut %}

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

    The `ConvertVectorToBytes` function assumes that the client processor uses [little-endian byte order](https://en.wikipedia.org/wiki/Endianness), such as x86\_64. If a different byte order is used, the `ConvertVectorToBytes` function must be adapted accordingly.

    {% endnote %}

- Python (alternative)

    The method accepts an array of dictionaries `items`, where each dictionary contains the fields `id` — identifier, `document` — text, `embedding` — the vector representation of the text.

    To use the struct in the example below, create `items_struct_type = ydb.StructType()` and define the types of all fields. To pass a list of such structs, wrap it in `ydb.ListType`: `ydb.ListType(items_struct_type)`.

    {% cut "asyncio" %}

    ```python
    import ydb

    async def insert_items_vector_as_float_list(
        pool: ydb.aio.QuerySessionPool,
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

        await pool.execute_with_retries(
            query, {"$items": (items, ydb.ListType(items_struct_type))}
        )

        print(f"{len(items)} items inserted")
    ```

    {% endcut %}

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

- C++ (alternative)

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


## Adding an index {#add-vector-index}

Using a vector index enables efficient approximate nearest vector search. For more details on the benefits and usage characteristics, see the [vector index](../../dev/vector-indexes.md) documentation.

Adding an index requires two operations:

1. Create a temporary index;
2. Rename the temporary index to make it permanent.

This approach supports both initial index creation and rebuilding (if the index already exists).

Available strategies:

* `similarity=cosine`;
* `similarity=inner_product`;
* `distance=cosine`;
* `distance=euclidean`;
* `distance=manhattan`.

Each strategy defines the function used for subsequent searches. The functions are described in detail in the [distance and similarity functions](../../yql/reference/udf/list/knn.md#fuctions-distance) documentation.

The parameters used when creating a `vector_kmeans_tree` index are described in the [vector index](../../dev/vector-indexes.md#kmeans-tree-type) documentation.


{% list tabs %}

- Python

    {% cut "asyncio" %}

    ```python
    import ydb

    async def add_vector_index(
        pool: ydb.aio.QuerySessionPool,
        driver: ydb.aio.Driver,
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
            clusters={clusters},
            overlap_clusters=3
        );
        """

        await pool.execute_with_retries(query)
        await driver.table_client.alter_table(
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

    {% endcut %}

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
            clusters={clusters},
            overlap_clusters=3
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
                clusters={5},
                overlap_clusters=3
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

## Searching by vector {#search-by-vector}

To search for documents by vector, use a special YQL query in which you define the similarity or distance function.
Available values:

* `CosineSimilarity`;
* `InnerProductSimilarity`;
* `CosineDistance`;
* `ManhattanDistance`;
* `EuclideanDistance`.

The functions are described in detail in the [distance and similarity functions](../../yql/reference/udf/list/knn.md#fuctions-distance) documentation.

The method allows specifying an index name. If provided, a `VIEW index_name` expression will be added to the query, enabling the vector index to be used during the search.

The method returns a list of dictionaries with fields `id`, `document`, and `score` — a number reflecting the degree of similarity (or distance) to the search vector.

{% list tabs %}

- Python

    {% cut "asyncio" %}

    ```python
    import ydb

    async def search_items_vector_as_bytes(
        pool: ydb.aio.QuerySessionPool,
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

        result = await pool.execute_with_retries(
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

    {% endcut %}

    ```python
    def search_items_vector_as_bytes(
        pool: ydb.QuerySessionPool,
        table_name: str,
        embedding: list[float],
        strategy: str = "CosineSimilarity",
        limit: int = 1,
        index_name: str | None = None,
        top_clusters: int = 10,
    ) -> list[dict]:
        view_index = f"VIEW {index_name}" if index_name else ""

        sort_order = "DESC" if strategy.endswith("Similarity") else "ASC"

        query = f"""
        PRAGMA ydb.KMeansTreeSearchTopSize = "{top_clusters}";
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
        std::uint64_t topClusters = 10,
        const std::optional<std::string>& indexName = std::nullopt)
    {
        std::string viewIndex = indexName ? "VIEW " + *indexName : "";
        std::string sortOrder = strategy.ends_with("Similarity") ? "DESC" : "ASC";

        std::string query = std::format(R"(
            PRAGMA ydb.KMeansTreeSearchTopSize = "{5}";
            DECLARE $embedding as String;
            SELECT
                id,
                document,
                Knn::{2}(embedding, $embedding) as score
            FROM {0} {1}
            ORDER BY score {3}
            LIMIT {4};
        )", tableName, viewIndex, strategy, sortOrder, limit, topClusters);

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

    {% cut "asyncio" %}

    ```python
    import ydb

    async def search_items_vector_as_float_list(
        pool: ydb.aio.QuerySessionPool,
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

        result = await pool.execute_with_retries(
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

    {% endcut %}

    ```python
    def search_items_vector_as_float_list(
        pool: ydb.QuerySessionPool,
        table_name: str,
        embedding: list[float],
        strategy: str = "CosineSimilarity",
        limit: int = 1,
        index_name: str | None = None,
        top_clusters: int = 10,
    ) -> list[dict]:
        view_index = f"VIEW {index_name}" if index_name else ""

        sort_order = "DESC" if strategy.endswith("Similarity") else "ASC"

        query = f"""
        PRAGMA ydb.KMeansTreeSearchTopSize = "{top_clusters}";
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
        std::uint64_t topClusters = 10,
        const std::optional<std::string>& indexName = std::nullopt)
    {
        std::string viewIndex = indexName ? "VIEW " + *indexName : "";
        std::string sortOrder = strategy.ends_with("Similarity") ? "DESC" : "ASC";

        std::string query = std::format(R"(
            PRAGMA ydb.KMeansTreeSearchTopSize = "{5}";
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
        )", tableName, viewIndex, strategy, sortOrder, limit, topClusters);

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

## Complete example {#full-example}

Combining all the methods described above into a single example with the following steps:

1. Drop the existing table
2. Create a new table
3. Insert items
4. Search for nearest vectors without an index
5. Add a vector index
6. Search for nearest vectors using the index

{% list tabs %}

- Python

    Example usage

    {% cut "asyncio" %}

    ```python
    import os
    import ydb
    import asyncio

    def print_results(items):
        if len(items) == 0:
            print("No items found")
            return

        for item in items:
            print(f"[score={item['score']}] {item['id']}: {item['document']}")

    async def drop_vector_table_if_exists(pool: ydb.aio.QuerySessionPool, table_name: str) -> None:
        await pool.execute_with_retries(f"DROP TABLE IF EXISTS `{table_name}`")

        print("Vector table dropped")

    async def main(
        ydb_endpoint: str,
        ydb_database: str,
        ydb_credentials: ydb.AbstractCredentials,
        table_name: str,
        index_name: str,
    ):
        async with ydb.aio.Driver(
            endpoint=ydb_endpoint,
            database=ydb_database,
            credentials=ydb_credentials,
        ) as driver:
            await driver.wait(5, fail_fast=True)
            pool = ydb.aio.QuerySessionPool(driver)

            await drop_vector_table_if_exists(pool, table_name)

            await create_vector_table(pool, table_name)

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

            await insert_items_vector_as_bytes(pool, table_name, items)

            items = await search_items_vector_as_bytes(
                pool,
                table_name,
                embedding=[1, 0, 0],
                strategy="CosineSimilarity",
                limit=3,
            )
            print_results(items)

            await add_vector_index(
                pool,
                driver,
                table_name,
                index_name=index_name,
                strategy="similarity=cosine",
                dimension=3,
                levels=1,
                clusters=3,
            )

            items = await search_items_vector_as_bytes(
                pool,
                table_name,
                embedding=[1, 0, 0],
                index_name=index_name,
                strategy="CosineSimilarity",
                limit=3,
            )
            print_results(items)

            await pool.stop()

    if __name__ == "__main__":
        asyncio.run(main(
            ydb_endpoint=os.environ.get("YDB_ENDPOINT", "grpc://localhost:2136"),
            ydb_database=os.environ.get("YDB_DATABASE", "/local"),
            ydb_credentials=ydb.credentials_from_env_variables(),
            table_name="ydb_vector_search",
            index_name="ydb_vector_index",
        ))
    ```

    {% endcut %}

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
            top_clusters=10,
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
            top_clusters=10,
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

    Program output:

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

    The output shows that the table was created, 9 documents were added, and vector similarity search was successfully performed both before and after adding the vector index.

    The full source code is available at [this link](https://github.com/ydb-platform/ydb/blob/main/ydb/public/sdk/python/examples/vector_search/vector_search.py).

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
            PrintResults(SearchItemsAsBytes(client, tableName, {1.0, 0.0, 0.0}, "CosineSimilarity", 3, 10, indexName));
        } catch (const std::exception& e) {
            std::cerr << "Execution failed: " << e.what() << std::endl;
        }

        driver.Stop(true);
    }
    ```

    The full source code is available at [this link](https://github.com/ydb-platform/ydb/tree/main/ydb/public/sdk/cpp/examples/vector_index_builtin).

{% endlist %}
