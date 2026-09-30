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
        TString OptionalStorageId = "__MEMORY";
        ui32 StoreShardsCount = 4;
        ui32 TableShardsCount = 3;
        std::optional<ui32> MaxBatchSize;

        NKikimrSchemeOp::TColumnTableSharding::THashSharding::EHashFunction ShardingMethod =
            NKikimrSchemeOp::TColumnTableSharding::THashSharding::HASH_FUNCTION_CONSISTENCY_64;
    };

    TColumnShardLogWriter(
        TLogMessageFilter filter,
        TDatabaseSettings settings,
        TVector<std::shared_ptr<TSchematizedLogColumn>> columns);

    const TDatabaseSettings& GetDatabaseSettings() const {
        return Settings;
    }

    bool Write(const NActors::NStructuredLog::TLogMessage&) override;
    void Flush() override;

protected:
    TString GetCreateStoreQuery();
    TString GetCreateTableQuery();

    TString GetStorePath() const;
    TString GetTablePath() const;
    std::optional<TVector<TString>> GetTableColumnNames() const;
    void CreateStorage();
    bool ExecuteSchemeQuery(const TString& sessionId, const TString& query);
    bool CheckStorageExists();
    void CreateOrUpdateStorage() override;
    void WriteBatch(std::shared_ptr<arrow::RecordBatch> batch) override;

    const TDatabaseSettings Settings;
    ui32 CurrentBatchSize {0};
};

} // namespace NEventLog
} // namespace NKikimr::NKqp
