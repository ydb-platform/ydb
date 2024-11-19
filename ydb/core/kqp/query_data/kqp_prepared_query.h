#pragma once

#include <ydb/core/kqp/common/simple/temp_tables.h>
#include <ydb/core/kqp/query_data/kqp_predictor.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/protos/kqp.pb.h>
#include <ydb/core/protos/kqp_physical.pb.h>

#include <util/generic/vector.h>

#include <memory>
#include <vector>

namespace NYql {
    struct TKikimrConfiguration;
}

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

struct TTableConstInfo;

class TPreparedQueryAllocHolder;

struct TPhyTxResultMetadata {
    NKikimr::NMiniKQL::TType* MkqlItemType;
    TVector<ui32> ColumnOrder;
};

struct TTableConstInfoMap : public TAtomicRefCount<TTableConstInfoMap> {
    THashMap<NKikimr::TTableId, TIntrusivePtr<TTableConstInfo>> Map;
};

class TKqpPhyTxHolder {
    std::shared_ptr<const NKikimrKqp::TPreparedQuery> PreparedQuery;
    const NKqpProto::TKqpPhyTx* Proto;
    bool LiteralTx = false;
    TVector<TPhyTxResultMetadata> TxResultsMeta;
    std::shared_ptr<TPreparedQueryAllocHolder> Alloc;
    std::vector<TStagePredictor> Predictors;
    TIntrusivePtr<TTableConstInfoMap> TableConstInfoById;

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

    const NKqpProto::TKqpSchemeOperation& GetSchemeOperation() const {
        return Proto->GetSchemeOperation();
    }

    const google::protobuf::RepeatedPtrField<TProtoStringType>& GetSecretNames() const {
        return Proto->GetSecretNames();
    }

    TProtoStringType DebugString() const {
        return Proto->ShortDebugString();
    }

    TIntrusiveConstPtr<TTableConstInfoMap> GetTableConstInfoById() const;

    TKqpPhyTxHolder(const std::shared_ptr<const NKikimrKqp::TPreparedQuery>& pq, const NKqpProto::TKqpPhyTx* proto,
        const std::shared_ptr<TPreparedQueryAllocHolder>& alloc, TIntrusivePtr<TTableConstInfoMap> tableConstInfoById);

    bool IsLiteralTx() const;

    std::optional<std::pair<bool, std::pair<TString, TString>>>
    GetSchemeOpTempTablePath() const;
};

class TLlvmSettings {
private:
    YDB_READONLY(bool, DisableLlvmForUdfStages, false);
    YDB_READONLY_DEF(std::optional<bool>, UseLlvmExternalDirective);
public:
    void Fill(TIntrusivePtr<NYql::TKikimrConfiguration> config, const NKikimrKqp::EQueryType qType);

    bool GetUseLlvm(const NYql::NDqProto::TProgram::TSettings& kqpSettingsProto) const;
};

class TPreparedQueryHolder {
private:
    YDB_ACCESSOR_DEF(TLlvmSettings, LlvmSettings);
    std::shared_ptr<const NKikimrKqp::TPreparedQuery> Proto;
    std::shared_ptr<TPreparedQueryAllocHolder> Alloc;
    TVector<TString> QueryTables;
    std::vector<TKqpPhyTxHolder::TConstPtr> Transactions;
    TIntrusivePtr<TTableConstInfoMap> TableConstInfoById;

public:

    TPreparedQueryHolder(
        NKikimrKqp::TPreparedQuery* proto,
        const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
        bool noFillTables = false);
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

    TIntrusivePtr<TTableConstInfo>& GetInfo(const TTableId& tableId);

    const THashMap<TTableId, TIntrusivePtr<TTableConstInfo>>& GetTableConstInfo() const;

    void FillTable(const NKqpProto::TKqpPhyTable& phyTable);

    void FillTables(const google::protobuf::RepeatedPtrField< ::NKqpProto::TKqpPhyStage>& stages);

    bool HasTempTables(TKqpTempTablesState::TConstPtr tempTablesState, bool withSessionId) const;
};


} // namespace NKikimr::NKqp
