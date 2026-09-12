#pragma once
#include "write_log_column.h"

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/library/actors/struct_log/log_sink.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/buffer.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_binary.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type_fwd.h>

#include <memory>

namespace NKikimr::NKqp::NLogToDB {

class TBaseDBLogWriter : public NActors::NStructuredLog::ILogSink {
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

    TBaseDBLogWriter(TKikimrRunner& runner, NLog::EComponent component, const TDatabaseSettings& settings, TVector<std::shared_ptr<TBaseDBLogColumn>> columns)
        : Runner(runner)
        , Component(component)
        , Settings(settings)
        , Columns(std::move(columns))
    {
    }

    TKikimrRunner& GetRunner() const {
        return Runner;
    }

    void Write(const NActors::NStructuredLog::TLogMessage&) override;
    TString GetStoreDescription();
    TString GetTableDescription();
    std::shared_ptr<arrow::Schema> GetArrowSchema() const;

    TKikimrRunner& Runner;
    const NLog::EComponent Component;
    const TDatabaseSettings Settings;
    const TVector<std::shared_ptr<TBaseDBLogColumn>> Columns;
    bool TableExists {false};

    void WaitForSchemeOperation(TActorId sender, ui64 txId);
    void ExecuteModifyScheme(NKikimrSchemeOp::TModifyScheme& modifyScheme);
    void CreateStore();
    void CreateTable();
    void SendDataViaActorSystem(std::shared_ptr<arrow::RecordBatch> batch);

};

}
