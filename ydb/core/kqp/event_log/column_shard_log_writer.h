#pragma once

#include "base_event_log_writer.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/type_fwd.h>

#include <optional>

namespace NKikimr::NKqp {

class TKikimrRunner;

namespace NEventLog {

class TColumnShardLogWriter : public TBaseEventLogWriter {
public:
    struct TDatabaseSettings {
        TString Path;
        TString StoreName;
        TString TableName;
        ui32 StoreShardsCount = 5;
        ui32 TableShardsCount = 5;
        TDuration FlushTimeout;
        std::optional<ui32> FlushBatchSize;

        ui32 MaxBatchSize = 1000000;
        ui8 MaxWriteAttempts = 10;
        ui8 MaxActiveWrites = 10;

        NKikimrSchemeOp::TColumnTableSharding::THashSharding::EHashFunction ShardingMethod =
            NKikimrSchemeOp::TColumnTableSharding::THashSharding::HASH_FUNCTION_CONSISTENCY_64;
    };

    TColumnShardLogWriter(
        TDatabaseSettings settings,
        TVector<std::shared_ptr<TEventLogColumn>> columns);

    const TDatabaseSettings& GetDatabaseSettings() const {
        return Settings;
    }

    bool Write(const NActors::NStructuredLog::TLogMessage&) override;

protected:
    TString GetCreateStoreQuery();
    TString GetCreateTableQuery();

    TString GetStorePath() const;
    TString GetTablePath() const;

    void CreateSession();
    void ExecuteSchemeQuery(const TString& sessionId, const TString& query, std::function<void()> handle);
    void CreateStorage(const TString& sessionId);
    void CreateTable(const TString& sessionId);
    void CreateOrUpdateStorage() override;
    void SetCreateError();

    void WriteBatch(std::shared_ptr<arrow::RecordBatch> batch) override;

    // Write batch queue
    struct TWriteBatch {
        std::shared_ptr<arrow::RecordBatch> Data;
        ui8 AttemptNum {0};
    };
    std::atomic<unsigned> ActiveWriteCount{0};
    void ProcessWriteBatch(const TWriteBatch& batch);

    const TDatabaseSettings Settings;
};

} // namespace NEventLog
} // namespace NKikimr::NKqp
