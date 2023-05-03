#pragma once

#include <ydb/core/kqp/query_data/kqp_predictor.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>
#include <ydb/core/protos/kqp.pb.h>

#include <util/generic/vector.h>

#include <memory>
#include <vector>

namespace NKikimr {
namespace NMiniKQL {
class IFunctionRegistry;
class TScopedAlloc;
class TTypeEnvironment;
class TType;
}
}

namespace NKqpProto {
class TKqpPhyTx;
}

namespace NKikimr::NKqp {

class TPreparedQueryAllocHolder;

struct TPhyTxResultMetadata {
    NKikimr::NMiniKQL::TType* MkqlItemType;
    TVector<ui32> ColumnOrder;
};

class TKqpPhyTxHolder {
    std::shared_ptr<const NKikimrKqp::TPreparedQuery> PreparedQuery;
    const NKqpProto::TKqpPhyTx* Proto;
    bool LiteralTx = false;
    TVector<TPhyTxResultMetadata> TxResultsMeta;
    std::shared_ptr<TPreparedQueryAllocHolder> Alloc;
    std::vector<TStagePredictor> Predictors;
public:
    using TConstPtr = std::shared_ptr<const TKqpPhyTxHolder>;

    const TStagePredictor& GetCalculationPredictor(const size_t stageIdx) const;

    const TVector<TPhyTxResultMetadata>& GetTxResultsMeta() const { return TxResultsMeta; }

    const NKqpProto::TKqpPhyStage& GetStages(size_t index) const {
        return Proto->GetStages(index);
    }

    size_t StagesSize() const {
        return Proto->StagesSize();
    }

    NKqpProto::TKqpPhyTx_EType GetType() const {
        return Proto->GetType();
    }

    const TProtoStringType& GetPlan() const {
        return Proto->GetPlan();
    }

    size_t ResultsSize() const {
        return Proto->ResultsSize();
    }

    const NKqpProto::TKqpPhyResult& GetResults(size_t index) const {
        return Proto->GetResults(index);
    }

    const google::protobuf::RepeatedPtrField< ::NKqpProto::TKqpPhyStage>& GetStages() const {
        return Proto->GetStages();
    }

    bool GetHasEffects() const {
        return Proto->GetHasEffects();
    }

    const ::google::protobuf::RepeatedPtrField< ::NKqpProto::TKqpPhyParamBinding> & GetParamBindings() const {
        return Proto->GetParamBindings();
    }

    const google::protobuf::RepeatedPtrField< ::NKqpProto::TKqpPhyTable>& GetTables() const {
        return Proto->GetTables();
    }

    TProtoStringType DebugString() const {
        return Proto->ShortDebugString();
    }

    TKqpPhyTxHolder(const std::shared_ptr<const NKikimrKqp::TPreparedQuery>& pq, const NKqpProto::TKqpPhyTx* proto,
        const std::shared_ptr<TPreparedQueryAllocHolder>& alloc);

    bool IsLiteralTx() const;
};

class TLlvmSettings {
private:
    YDB_READONLY(bool, DisableLlvmForUdfStages, false);
    YDB_READONLY_DEF(std::optional<bool>, UseLlvmExternalDirective);
public:
    void Fill(NYql::TKikimrConfiguration::TPtr config, const NKikimrKqp::EQueryType qType);

    bool GetUseLlvm(const NYql::NDqProto::TProgram::TSettings& kqpSettingsProto) const;
};

class TPreparedQueryHolder {
private:
    YDB_ACCESSOR_DEF(TLlvmSettings, LlvmSettings);
    std::shared_ptr<const NKikimrKqp::TPreparedQuery> Proto;
    std::shared_ptr<TPreparedQueryAllocHolder> Alloc;
    TVector<TString> QueryTables;
    std::vector<TKqpPhyTxHolder::TConstPtr> Transactions;

public:

    TPreparedQueryHolder(NKikimrKqp::TPreparedQuery* proto, const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry);
    ~TPreparedQueryHolder();

    using TConstPtr = std::shared_ptr<const TPreparedQueryHolder>;

    const std::vector<TKqpPhyTxHolder::TConstPtr>& GetTransactions() const {
        return Transactions;
    }

    const ::google::protobuf::RepeatedPtrField< ::NKikimrKqp::TParameterDescription>& GetParameters() const {
        return Proto->GetParameters();
    }

    const TKqpPhyTxHolder::TConstPtr& GetPhyTx(ui32 idx) const;
    TKqpPhyTxHolder::TConstPtr GetPhyTxOrEmpty(ui32 idx) const;

    TString GetText() const;

    ui32 GetVersion() const {
        return Proto->GetVersion();
    }

    size_t ResultsSize() const {
        return Proto->ResultsSize();
    }

    const NKikimrKqp::TPreparedResult& GetResults(size_t index) const {
        return Proto->GetResults(index);
    }

    ui64 ByteSize() const {
        return Proto->ByteSize();
    }

    const TVector<TString>& GetQueryTables() const {
        return QueryTables;
    }

    const NKqpProto::TKqpPhyQuery& GetPhysicalQuery() const {
        return Proto->GetPhysicalQuery();
    }
};


} // namespace NKikimr::NKqp
