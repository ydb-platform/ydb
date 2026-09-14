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

        NKikimrSchemeOp::TColumnTableSharding::THashSharding::EHashFunction ShardingMethod =
            NKikimrSchemeOp::TColumnTableSharding::THashSharding::HASH_FUNCTION_CONSISTENCY_64;
    };

    TColumnShardLogWriter(TKikimrRunner& runner, NLog::EComponent component, const TDatabaseSettings& settings, TVector<std::shared_ptr<TSchematizedLogColumn>> columns)
        : TBaseSchematizedLogWriter(runner, component, std::move(columns))
        , Settings(settings)
    {
    }

    void CreateOrUpdateStorage() override;
    void DeleteStorageIfExists() override {}
    void CleanupStorageIfExists(TInstant) override {}

protected:
    TString GetStoreDescription();
    TString GetTableDescription();

    const TDatabaseSettings Settings;

    void WaitForSchemeOperation(TActorId sender, ui64 txId);
    void ExecuteModifyScheme(NKikimrSchemeOp::TModifyScheme& modifyScheme);

    void WriteBatch(std::shared_ptr<arrow::RecordBatch> batch) override;

};

}
