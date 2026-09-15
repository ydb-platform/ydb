#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

using namespace NYdb;
using namespace NYdb::NTable;

constexpr size_t BULK_UPSERT_BATCH_SIZE = 1000;

struct TRunArgs {
    TDriver Driver;
    std::string Path;
};

struct TLogMessage {
    struct TPrimaryKeyLogMessage {
        std::string App;
        std::string Host;
        TInstant Timestamp;
        uint64_t Id;
    };

    TPrimaryKeyLogMessage Pk;
    uint32_t HttpCode;
    std::string Message;
};

struct TStatistic {
    uint64_t SumApp;
    uint64_t SumHost;
    uint64_t RowCount;
};

TRunArgs GetRunArgs();
TStatus CreateTable(TTableClient& client, const std::string& table);
TStatistic GetLogBatch(uint64_t logOffset, std::vector<TLogMessage>& logBatch, uint32_t lastNumber,
    size_t batchSize = BULK_UPSERT_BATCH_SIZE);
TValue BuildLogBatchValue(const std::vector<TLogMessage>& logBatch);
TStatus WriteLogBatch(TTableClient& tableClient, const std::string& table, const std::vector<TLogMessage>& logBatch,
                   const TRetryOperationSettings& retrySettings);
TDuration MeasureBulkUpsertWallTime(
    TTableClient& tableClient,
    const std::string& table,
    uint64_t logOffset,
    uint32_t idOffset,
    const TBulkUpsertSettings& settings,
    size_t batchSize = BULK_UPSERT_BATCH_SIZE);
TStatistic Select(TTableClient& client, const std::string& path);
void DropTable(TTableClient& client, const std::string& path);
