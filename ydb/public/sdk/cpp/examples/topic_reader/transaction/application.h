#pragma once

#include "options.h"

#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

#include <memory>
#include <optional>

class TApplication {
public:
    explicit TApplication(const TOptions& options);

    void Run();
    void Stop();
    void Finalize();

private:
    struct TRow {
        TRow() = default;
        TRow(ui64 key, const TString& value);

        ui64 Key = 0;
        TString Value;
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
    TString TablePath;
};
