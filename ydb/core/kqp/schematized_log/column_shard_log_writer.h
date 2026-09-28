#pragma once

#include "base_schematized_log_writer.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/type_fwd.h>

#include <util/generic/string.h>
#include <util/system/yassert.h>

namespace NKikimr::NKqp {

class TKikimrRunner;

namespace NSchematizedLog {

class TColumnShardLogWriter : public TBaseSchematizedLogWriter {
public:
    struct TDatabaseSettings {
        TString OptionalStorageId = "__MEMORY";
        TString TableName{"olapTable"};
        TString StoreName{"olapStore"};
        ui32 StoreShardsCount = 4;
        ui32 TableShardsCount = 3;
        std::optional<ui32> MaxBatchSize;

        NKikimrSchemeOp::TColumnTableSharding::THashSharding::EHashFunction ShardingMethod =
            NKikimrSchemeOp::TColumnTableSharding::THashSharding::HASH_FUNCTION_CONSISTENCY_64;
    };

    TColumnShardLogWriter(
        TLogMessageFilter filter,
        TDatabaseSettings settings,
        TVector<std::shared_ptr<TSchematizedLogColumn>> columns,
        TKikimrRunner* runner = nullptr);

    TKikimrRunner& GetRunner() const {
        Y_DEBUG_ABORT_UNLESS(Runner);
        return *Runner;
    }

    const TDatabaseSettings& GetDatabaseSettings() const {
        return Settings;
    }

    bool Write(const NActors::NStructuredLog::TLogMessage&) override;
    void Flush() override;

protected:
    TString GetStoreDescription();
    TString GetTableDescription();

    void WaitForSchemeOperation(NActors::TActorId sender, ui64 txId);
    void ExecuteModifyScheme(NKikimrSchemeOp::TModifyScheme& modifyScheme);

    void CreateStorage();
    bool CheckStorageExists() const;
    void CreateOrUpdateStorage() override;
    void WriteBatch(std::shared_ptr<arrow::RecordBatch> batch) override;

    const TDatabaseSettings Settings;
    ui32 CurrentBatchSize {0};
    TKikimrRunner* Runner {nullptr};
};

} // namespace NSchematizedLog
} // namespace NKikimr::NKqp
