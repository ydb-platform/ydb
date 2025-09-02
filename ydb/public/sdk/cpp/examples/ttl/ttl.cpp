#include "ttl.h"
#include "util.h"

#include <format>

using namespace NExample;
using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYdb::NStatusHelpers;

constexpr uint32_t DOC_TABLE_PARTITION_COUNT = 4;
constexpr uint32_t EXPIRATION_QUEUE_COUNT = 4;

namespace {

template <class T>
std::string OptionalToString(const std::optional<T>& opt) {
    if (opt.has_value()) {
        return std::to_string(opt.value());
    }
    return "(NULL)";
}

template <>
std::string OptionalToString<std::string>(const std::optional<std::string>& opt) {
    if (opt.has_value()) {
        return opt.value();
    }
    return "(NULL)";
}

//! Creates Documents table and multiple ExpirationQueue tables
void CreateTables(TTableClient client, const std::string& path) {
    // Documents table stores the contents of web pages.
    // The table is partitioned by hash(Url) in order to evenly distribute the load.
    ThrowOnError(client.RetryOperationSync([path](TSession session) {
        auto documentsDesc = TTableBuilder()
            .AddNullableColumn("doc_id", EPrimitiveType::Uint64)
            .AddNullableColumn("url", EPrimitiveType::Utf8)
            .AddNullableColumn("html", EPrimitiveType::Utf8)
            .AddNullableColumn("timestamp", EPrimitiveType::Uint64)
            .SetPrimaryKeyColumn("doc_id")
            .Build();

        // Partition Documents table by DocId
        auto documnetsSettings = TCreateTableSettings();
        documnetsSettings.PartitioningPolicy(TPartitioningPolicy().UniformPartitions(DOC_TABLE_PARTITION_COUNT));

        return session.CreateTable(JoinPath(path, "documents"),
            std::move(documentsDesc), std::move(documnetsSettings)).GetValueSync();
    }));

    // Multiple ExpirationQueue tables allow to scale the load.
    // Each ExpirationQueue table can be handled by a dedicated worker.
    for (uint32_t i = 0; i < EXPIRATION_QUEUE_COUNT; ++i) {
        ThrowOnError(client.RetryOperationSync([path, i](TSession session) {
            auto expirationDesc = TTableBuilder()
                .AddNullableColumn("timestamp", EPrimitiveType::Uint64)
                .AddNullableColumn("doc_id", EPrimitiveType::Uint64)
                .SetPrimaryKeyColumns({"timestamp", "doc_id"})
                .Build();

            return session.CreateTable(JoinPath(path, std::format("expiration_queue_{}", i)),
                std::move(expirationDesc)).GetValueSync();
        }));
    }
}

///////////////////////////////////////////////////////////////////////////////

//! Insert or replaces a document.
TStatus AddDocumentTransaction(TSession session, const std::string& path,
    const std::string& url, const std::string& html, uint64_t timestamp)
{
    // Add an entry to a random expiration queue in order to evenly distribute the load
    uint32_t queue = rand() % EXPIRATION_QUEUE_COUNT;

    auto query = std::format(R"(
        --!syntax_v1
        PRAGMA TablePathPrefix("{}");

        DECLARE $url AS Utf8;
        DECLARE $html AS Utf8;
        DECLARE $timestamp AS Uint64;

        $doc_id = Digest::CityHash($url);

        REPLACE INTO documents
            (doc_id, url, html, `timestamp`)
        VALUES
            ($doc_id, $url, $html, $timestamp);

        REPLACE INTO expiration_queue_{}
            (`timestamp`, doc_id)
        VALUES
            ($timestamp, $doc_id);
    )", path, queue);

    auto params = session.GetParamsBuilder()
        .AddParam("$url").Utf8(url).Build()
        .AddParam("$html").Utf8(html).Build()
        .AddParam("$timestamp").Uint64(timestamp).Build()
        .Build();

    return session.ExecuteDataQuery(
        query,
        TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(),
        std::move(params)).GetValueSync();
}

//! Reads document contents.
TStatus ReadDocumentTransaction(TSession session, const std::string& path,
    const std::string& url, std::optional<TResultSet>& resultSet)
{
    auto query = std::format(R"(
        --!syntax_v1
        PRAGMA TablePathPrefix("{}");

        DECLARE $url AS Utf8;

        $doc_id = Digest::CityHash($url);

        SELECT doc_id, url, html, `timestamp`
        FROM documents
        WHERE doc_id = $doc_id;
    )", path);

    auto params = session.GetParamsBuilder()
        .AddParam("$url").Utf8(url).Build()
        .Build();

    auto result = session.ExecuteDataQuery(
        query,
        TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(),
        std::move(params)).GetValueSync();

    if (result.IsSuccess()) {
        resultSet = result.GetResultSet(0);
    }

    return result;
}

//! Reads a batch of entries from expiration queue
TStatus ReadExpiredBatchTransaction(TSession session, const std::string& path, const uint32_t queue,
    const uint64_t timestamp, const uint64_t prevTimestamp, const uint64_t prevDocId, std::optional<TResultSet>& resultSet)
{
    auto query = std::format(R"(
        --!syntax_v1
        PRAGMA TablePathPrefix("{0}");

        DECLARE $timestamp AS Uint64;
        DECLARE $prev_timestamp AS Uint64;
        DECLARE $prev_doc_id AS Uint64;

        $data = (
            (SELECT *
            FROM expiration_queue_{1}
            WHERE
                `timestamp` <= $timestamp
                AND
                `timestamp` > $prev_timestamp
            ORDER BY `timestamp`, doc_id
            LIMIT 100)

            UNION ALL

            (SELECT *
            FROM expiration_queue_{1}
            WHERE
                `timestamp` = $prev_timestamp AND doc_id > $prev_doc_id
            ORDER BY `timestamp`, doc_id
            LIMIT 100)
        );

        SELECT `timestamp`, doc_id
        FROM $data
        ORDER BY `timestamp`, doc_id
        LIMIT 100;
    )", path, queue);

    auto params = session.GetParamsBuilder()
        .AddParam("$timestamp").Uint64(timestamp).Build()
        .AddParam("$prev_timestamp").Uint64(prevTimestamp).Build()
        .AddParam("$prev_doc_id").Uint64(prevDocId).Build()
        .Build();

    auto result = session.ExecuteDataQuery(
        query,
        TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(),
        std::move(params)).GetValueSync();

    if (result.IsSuccess()) {
        resultSet = result.GetResultSet(0);
    }

    return result;
}

//! Deletes an expired document
TStatus DeleteDocumentWithTimestamp(TSession session, const std::string& path, const uint32_t queue,
    const uint64_t docId, const uint64_t timestamp)
{
    auto query = std::format(R"(
        --!syntax_v1
        PRAGMA TablePathPrefix("{}");

        DECLARE $doc_id AS Uint64;
        DECLARE $timestamp AS Uint64;

        DELETE FROM documents
        WHERE doc_id = $doc_id AND `timestamp` = $timestamp;

        DELETE FROM expiration_queue_{}
        WHERE `timestamp` = $timestamp AND doc_id = $doc_id;
    )", path, queue);

    auto params = session.GetParamsBuilder()
        .AddParam("$doc_id").Uint64(docId).Build()
        .AddParam("$timestamp").Uint64(timestamp).Build()
        .Build();

    auto result = session.ExecuteDataQuery(
        query,
        TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(),
        std::move(params)).GetValueSync();

    return result;
}

}

///////////////////////////////////////////////////////////////////////////////

void AddDocument(TTableClient client, const std::string& path, const std::string& url,
    const std::string& html, const uint64_t timestamp)
{
    std::cout << "> AddDocument:" << std::endl
         << " Url: " << url << std::endl
         << " Timestamp: " << timestamp << std::endl;

    ThrowOnError(client.RetryOperationSync([path, url, html, timestamp](TSession session) {
        return AddDocumentTransaction(session, path, url, html, timestamp);
    }));
    std::cout << std::endl;
}

void ReadDocument(TTableClient client, const std::string& path, const std::string& url) {
    std::cout << "> ReadDocument \"" << url << "\":" << std::endl;
    std::optional<TResultSet> resultSet;
    ThrowOnError(client.RetryOperationSync([path, url, &resultSet] (TSession session) {
        return ReadDocumentTransaction(session, path, url, resultSet);
    }));

    TResultSetParser parser(*resultSet);
    if (parser.TryNextRow()) {
        std::cout << " DocId: " << OptionalToString(parser.ColumnParser("doc_id").GetOptionalUint64()) << std::endl
            << " Url: " << OptionalToString(parser.ColumnParser("url").GetOptionalUtf8()) << std::endl
            << " Timestamp: " << OptionalToString(parser.ColumnParser("timestamp").GetOptionalUint64()) << std::endl
            << " Html: " << OptionalToString(parser.ColumnParser("html").GetOptionalUtf8()) << std::endl;
    } else {
        std::cout << " Not found" << std::endl;
    }
    std::cout << std::endl;
}

void DeleteExpired(TTableClient client, const std::string& path, const uint32_t queue, const uint64_t timestamp) {
    std::cout << "> DeleteExpired from queue #" << queue << ":" << std::endl;
    bool empty = false;
    uint64_t lastTimestamp = 0;
    uint64_t lastDocId = 0;
    while (!empty) {
        std::optional<TResultSet> resultSet;
        ThrowOnError(client.RetryOperationSync([path, queue, timestamp, lastDocId, lastTimestamp, &resultSet] (TSession session) {
            return ReadExpiredBatchTransaction(session, path, queue, timestamp, lastTimestamp, lastDocId, resultSet);
        }));

        empty = true;
        TResultSetParser parser(*resultSet);
        while (parser.TryNextRow()) {
            empty = false;
            lastDocId  = *parser.ColumnParser("doc_id").GetOptionalUint64();
            lastTimestamp = *parser.ColumnParser("timestamp").GetOptionalUint64();
            std::cout << " DocId: " << lastDocId << " Timestamp: " << lastTimestamp << std::endl;

            ThrowOnError(client.RetryOperationSync([path, queue, lastDocId, lastTimestamp] (TSession session) {
                return DeleteDocumentWithTimestamp(session, path, queue, lastDocId, lastTimestamp);
            }));
        }
    }
    std::cout << std::endl;
}
///////////////////////////////////////////////////////////////////////////////

bool Run(const TDriver& driver, const std::string& path) {
    TTableClient client(driver);

    try {
        CreateTables(client, path);

        AddDocument(client, path,
                    "https://yandex.ru/",
                    "<html><body><h1>Yandex</h1></body></html>",
                    1);

        AddDocument(client, path,
                    "https://ya.ru/",
                    "<html><body><h1>Yandex</h1></body></html>",
                    2);

        ReadDocument(client, path, "https://yandex.ru/");
        ReadDocument(client, path, "https://ya.ru/");

        for (uint32_t q = 0; q < EXPIRATION_QUEUE_COUNT; ++q) {
            DeleteExpired(client, path, q, 1);
        }

        ReadDocument(client, path, "https://ya.ru/");

        AddDocument(client, path,
                    "https://yandex.ru/",
                    "<html><body><h1>Yandex</h1></body></html>",
                    2);

        AddDocument(client, path,
                    "https://yandex.ru/",
                    "<html><body><h1>Yandex</h1></body></html>",
                    3);

        for (uint32_t q = 0; q < EXPIRATION_QUEUE_COUNT; ++q) {
            DeleteExpired(client, path, q, 2);
        }

        ReadDocument(client, path, "https://yandex.ru/");
        ReadDocument(client, path, "https://ya.ru/");
    }
    catch (const TYdbErrorException& e) {
        std::cerr << "Execution failed due to fatal error: " << e.what() << std::endl;
        return false;
    }

    return true;
}
