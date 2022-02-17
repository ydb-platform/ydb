#include "ttl.h"
#include "util.h"

#include <util/folder/pathsplit.h>
#include <util/string/printf.h>

using namespace NExample;
using namespace NYdb;
using namespace NYdb::NTable;

constexpr ui32 DOC_TABLE_PARTITION_COUNT = 4;
constexpr ui32 EXPIRATION_QUEUE_COUNT = 4;

//! Creates Documents table and multiple ExpirationQueue tables
static void CreateTables(TTableClient client, const TString& path) {
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
    for (ui32 i = 0; i < EXPIRATION_QUEUE_COUNT; ++i) {
        ThrowOnError(client.RetryOperationSync([path, i](TSession session) {
            auto expirationDesc = TTableBuilder()
                .AddNullableColumn("timestamp", EPrimitiveType::Uint64)
                .AddNullableColumn("doc_id", EPrimitiveType::Uint64)
                .SetPrimaryKeyColumns({"timestamp", "doc_id"})
                .Build();

            return session.CreateTable(JoinPath(path, Sprintf("expiration_queue_%" PRIu32, i)),
                std::move(expirationDesc)).GetValueSync();
        }));
    }
}

///////////////////////////////////////////////////////////////////////////////

//! Insert or replaces a document.
static TStatus AddDocumentTransaction(TSession session, const TString& path,
    const TString& url, const TString& html, ui64 timestamp)
{
    // Add an entry to a random expiration queue in order to evenly distribute the load
    ui32 queue = rand() % EXPIRATION_QUEUE_COUNT;

    auto query = Sprintf(R"(
        --!syntax_v1
        PRAGMA TablePathPrefix("%s");

        DECLARE $url AS Utf8;
        DECLARE $html AS Utf8;
        DECLARE $timestamp AS Uint64;

        $doc_id = Digest::CityHash($url);

        REPLACE INTO documents
            (doc_id, url, html, `timestamp`)
        VALUES
            ($doc_id, $url, $html, $timestamp);

        REPLACE INTO expiration_queue_%u
            (`timestamp`, doc_id)
        VALUES
            ($timestamp, $doc_id);
    )", path.c_str(), queue);

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
static TStatus ReadDocumentTransaction(TSession session, const TString& path,
    const TString& url, TMaybe<TResultSet>& resultSet)
{
    auto query = Sprintf(R"(
        --!syntax_v1
        PRAGMA TablePathPrefix("%s");

        DECLARE $url AS Utf8;

        $doc_id = Digest::CityHash($url);

        SELECT doc_id, url, html, `timestamp`
        FROM documents
        WHERE doc_id = $doc_id;
    )", path.c_str());

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
static TStatus ReadExpiredBatchTransaction(TSession session, const TString& path, const ui32 queue,
    const ui64 timestamp, const ui64 prevTimestamp, const ui64 prevDocId, TMaybe<TResultSet>& resultSet)
{
    auto query = Sprintf(R"(
        --!syntax_v1
        PRAGMA TablePathPrefix("%s");

        DECLARE $timestamp AS Uint64;
        DECLARE $prev_timestamp AS Uint64;
        DECLARE $prev_doc_id AS Uint64;

        $data = (
            SELECT *
            FROM expiration_queue_%u
            WHERE
                `timestamp` <= $timestamp
                AND
                `timestamp` > $prev_timestamp
            ORDER BY `timestamp`, doc_id
            LIMIT 100

            UNION ALL

            SELECT *
            FROM expiration_queue_%u
            WHERE
                `timestamp` = $prev_timestamp AND doc_id > $prev_doc_id
            ORDER BY `timestamp`, doc_id
            LIMIT 100
        );

        SELECT `timestamp`, doc_id
        FROM $data
        ORDER BY `timestamp`, doc_id
        LIMIT 100;
    )", path.c_str(), queue, queue);

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
static TStatus DeleteDocumentWithTimestamp(TSession session, const TString& path, const ui32 queue,
    const ui64 docId, const ui64 timestamp)
{
    auto query = Sprintf(R"(
        --!syntax_v1
        PRAGMA TablePathPrefix("%s");

        DECLARE $doc_id AS Uint64;
        DECLARE $timestamp AS Uint64;

        DELETE FROM documents
        WHERE doc_id = $doc_id AND `timestamp` = $timestamp;

        DELETE FROM expiration_queue_%u
        WHERE `timestamp` = $timestamp AND doc_id = $doc_id;
    )", path.c_str(), queue);

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
///////////////////////////////////////////////////////////////////////////////

void AddDocument(TTableClient client, const TString& path, const TString& url,
    const TString& html, const ui64 timestamp)
{
    Cout << "> AddDocument:" << Endl
         << " Url: " << url << Endl
         << " Timestamp: " << timestamp << Endl;

    ThrowOnError(client.RetryOperationSync([path, url, html, timestamp](TSession session) {
        return AddDocumentTransaction(session, path, url, html, timestamp);
    }));
    Cout << Endl;
}

void ReadDocument(TTableClient client, const TString& path, const TString& url) {
    Cout << "> ReadDocument \"" << url << "\":" << Endl;
    TMaybe<TResultSet> resultSet;
    ThrowOnError(client.RetryOperationSync([path, url, &resultSet] (TSession session) {
        return ReadDocumentTransaction(session, path, url, resultSet);
    }));

    TResultSetParser parser(*resultSet);
    if (parser.TryNextRow()) {
        Cout << " DocId: " << parser.ColumnParser("doc_id").GetOptionalUint64() << Endl
            << " Url: " << parser.ColumnParser("url").GetOptionalUtf8() << Endl
            << " Timestamp: " << parser.ColumnParser("timestamp").GetOptionalUint64() << Endl
            << " Html: " << parser.ColumnParser("html").GetOptionalUtf8() << Endl;
    } else {
        Cout << " Not found" << Endl;
    }
    Cout << Endl;
}

void DeleteExpired(TTableClient client, const TString& path, const ui32 queue, const ui64 timestamp) {
    Cout << "> DeleteExpired from queue #" << queue << ":" << Endl;
    bool empty = false;
    ui64 lastTimestamp = 0;
    ui64 lastDocId = 0;
    while (!empty) {
        TMaybe<TResultSet> resultSet;
        ThrowOnError(client.RetryOperationSync([path, queue, timestamp, lastDocId, lastTimestamp, &resultSet] (TSession session) {
            return ReadExpiredBatchTransaction(session, path, queue, timestamp, lastTimestamp, lastDocId, resultSet);
        }));

        empty = true;
        TResultSetParser parser(*resultSet);
        while (parser.TryNextRow()) {
            empty = false;
            lastDocId  = *parser.ColumnParser("doc_id").GetOptionalUint64();
            lastTimestamp = *parser.ColumnParser("timestamp").GetOptionalUint64();
            Cout << " DocId: " << lastDocId << " Timestamp: " << lastTimestamp << Endl;

            ThrowOnError(client.RetryOperationSync([path, queue, lastDocId, lastTimestamp] (TSession session) {
                return DeleteDocumentWithTimestamp(session, path, queue, lastDocId, lastTimestamp);
            }));
        }
    }
    Cout << Endl;
}
///////////////////////////////////////////////////////////////////////////////

bool Run(const TDriver& driver, const TString& path) {
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

        for (ui32 q = 0; q < EXPIRATION_QUEUE_COUNT; ++q) {
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

        for (ui32 q = 0; q < EXPIRATION_QUEUE_COUNT; ++q) {
            DeleteExpired(client, path, q, 2);
        }

        ReadDocument(client, path, "https://yandex.ru/");
        ReadDocument(client, path, "https://ya.ru/");
    }
    catch (const TYdbErrorException& e) {
        Cerr << "Execution failed due to fatal error:" << Endl;
        PrintStatus(e.Status);
        return false;
    }

    return true;
}
