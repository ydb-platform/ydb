#pragma once

#include "write_log_schematized.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/type_fwd.h>

namespace NKikimr::NKqp::NSchematizedLog {

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

    TColumnShardLogWriter(TKikimrRunner& runner, TBaseSchematizedLogWriter::TLogMessageFilter filter, const TDatabaseSettings& settings, TVector<std::shared_ptr<TSchematizedLogColumn>> columns)
        : TBaseSchematizedLogWriter(runner, std::move(filter), std::move(columns))
        , Settings(settings)
    {
    }

    const TDatabaseSettings& GetDatabaseSettings() const {
        return Settings;
    }

    bool Write(const NActors::NStructuredLog::TLogMessage&) override;
    void Flush() override;

protected:
    TString GetStoreDescription();
    TString GetTableDescription();

    const TDatabaseSettings Settings;
    ui32 CurrentBatchSize{0};

    void WaitForSchemeOperation(TActorId sender, ui64 txId);
    void ExecuteModifyScheme(NKikimrSchemeOp::TModifyScheme& modifyScheme);

    void CreateOrUpdateStorage() override;
    void WriteBatch(std::shared_ptr<arrow::RecordBatch> batch) override;

};

}
