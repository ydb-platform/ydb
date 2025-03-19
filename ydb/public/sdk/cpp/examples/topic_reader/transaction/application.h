#pragma once

#include "options.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <memory>
#include <optional>
#include <random>

class TApplication {
public:
    explicit TApplication(const TOptions& options);

    void Run();
    void Stop();
    void Finalize();

private:
    struct TRow {
        TRow() = default;
        TRow(uint64_t key, const std::string& value);

        uint64_t Key = 0;
        std::string Value;
    };

    void CreateTopicReadSession(const TOptions& options);
    void CreateTableSession();

    void BeginTransaction();
    void CommitTransaction();

    void TryCommitTransaction();

    void InsertRowsIntoTable();
    void AppendTableRow(const NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage& message);

    std::optional<NYdb::TDriver> Driver;
    std::optional<NYdb::NTopic::TTopicClient> TopicClient;
    std::optional<NYdb::NTable::TTableClient> TableClient;
    std::shared_ptr<NYdb::NTopic::IReadSession> ReadSession;
    std::optional<NYdb::NTable::TSession> TableSession;
    std::optional<NYdb::NTable::TTransaction> Transaction;
    std::vector<NYdb::NTopic::TReadSessionEvent::TStopPartitionSessionEvent> PendingStopEvents;
    std::vector<TRow> Rows;
    std::string TablePath;

    std::mt19937_64 MersenneEngine;
    std::uniform_int_distribution<uint64_t> Dist;
};
