import os

import ydb


def drop_vector_table_if_exists(pool: ydb.QuerySessionPool, table_name: str) -> None:
    pool.execute_with_retries(f"DROP TABLE IF EXISTS `{table_name}`")

    print("Vector table dropped")


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


def insert_document(
    pool: ydb.QuerySessionPool,
    table_name: str,
    id: str,
    document: str,
    embedding: list[float],
) -> None:
    query = f"""
    DECLARE $documents AS List<Struct<
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
    FROM AS_TABLE($documents);
    """

    documents = [
        {
            "id": id,
            "document": document,
            "embedding": embedding,
        }
    ]

    document_struct_type = ydb.StructType()
    document_struct_type.add_member("id", ydb.PrimitiveType.Utf8)
    document_struct_type.add_member("document", ydb.PrimitiveType.Utf8)
    document_struct_type.add_member("embedding", ydb.ListType(ydb.PrimitiveType.Float))

    pool.execute_with_retries(
        query, {"$documents": (documents, ydb.ListType(document_struct_type))}
    )

    print(f"Document with id={id} inserted")


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


def search_documents(
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

    documents = []

    for result_set in result:
        for row in result_set.rows:
            documents.append(
                {
                    "id": row["id"],
                    "document": row["document"],
                    "score": row["score"],
                }
            )

    return documents


def print_results(docs):
    for document in docs:
        print(f"[score={document['score']}] {document['id']}: {document['document']}")


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

    insert_document(pool, table_name, "1", "vector 1", [0.98, 0.1, 0.01])
    insert_document(pool, table_name, "2", "vector 2", [1.0, 0.05, 0.05])
    insert_document(pool, table_name, "3", "vector 3", [0.9, 0.1, 0.1])
    insert_document(pool, table_name, "4", "vector 4", [0.03, 0.0, 0.99])
    insert_document(pool, table_name, "5", "vector 5", [0.0, 0.0, 0.99])
    insert_document(pool, table_name, "5", "vector 6", [0.0, 0.02, 1.0])
    insert_document(pool, table_name, "5", "vector 7", [0.0, 1.05, 0.05])
    insert_document(pool, table_name, "5", "vector 8", [0.02, 0.98, 0.1])
    insert_document(pool, table_name, "5", "vector 9", [0.0, 1.0, 0.05])

    docs = search_documents(
        pool,
        table_name,
        embedding=[1, 0, 0],
        strategy="CosineSimilarity",
        limit=3,
    )
    print_results(docs)

    add_index(
        pool,
        driver,
        table_name,
        index_name=index_name,
        strategy="similarity=cosine",
        dim=3,
        levels=1,
        clusters=3,
    )

    docs = search_documents(
        pool,
        table_name,
        embedding=[1, 0, 0],
        index_name=index_name,
        strategy="CosineSimilarity",
        limit=3,
    )
    print_results(docs)

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
