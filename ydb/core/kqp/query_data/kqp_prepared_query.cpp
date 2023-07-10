#include "kqp_prepared_query.h"

#include <ydb/library/mkql_proto/mkql_proto.h>
#include <ydb/library/yql/core/yql_data_provider.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/mkql_proto/mkql_proto.h>
#include <ydb/core/kqp/common/simple/helpers.h>
#include <ydb/core/protos/kqp_physical.pb.h>
#include <ydb/library/services/services.pb.h>

#include <library/cpp/actors/core/log.h>

namespace NKikimr::NKqp {

using namespace NKikimr::NMiniKQL;

namespace {

bool CalcIsLiteralTx(const NKqpProto::TKqpPhyTx* tx) {
    if (tx->GetType() != NKqpProto::TKqpPhyTx::TYPE_COMPUTE) {
        return false;
    }

    for (const auto& stage : tx->GetStages()) {
        if (stage.InputsSize() != 0) {
            return false;
        }
    }

    return true;
}

}

class TPreparedQueryAllocHolder {
public:
    NKikimr::NMiniKQL::TScopedAlloc Alloc;
    NKikimr::NMiniKQL::TTypeEnvironment TypeEnv;

    TPreparedQueryAllocHolder(const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry)
        : Alloc(__LOCATION__, NKikimr::TAlignedPagePoolCounters(), functionRegistry->SupportsSizedAllocators())
        , TypeEnv(Alloc)
    {
        Alloc.Release();
    }

    ~TPreparedQueryAllocHolder()
    {
        Alloc.Acquire();
    }
};

TKqpPhyTxHolder::TKqpPhyTxHolder(const std::shared_ptr<const NKikimrKqp::TPreparedQuery>& pq,
    const NKqpProto::TKqpPhyTx* proto, const std::shared_ptr<TPreparedQueryAllocHolder>& alloc)
    : PreparedQuery(pq)
    , Proto(proto)
    , LiteralTx(CalcIsLiteralTx(proto))
    , Alloc(alloc)
{
    TxResultsMeta.resize(Proto->GetResults().size());
    for (auto&& i : Proto->GetStages()) {
        TStagePredictor predictor;
        if (!predictor.DeserializeFromKqpSettings(i.GetProgram().GetSettings())) {
            ALS_ERROR(NKikimrServices::KQP_EXECUTER) << "cannot parse program settings for data prediction";
            Predictors.emplace_back();
        } else {
            Predictors.emplace_back(std::move(predictor));
        }
    }

    for (ui32 i = 0; i < Proto->ResultsSize(); ++i) {
        const auto& txResult = Proto->GetResults(i);
        auto& result = TxResultsMeta[i];

        result.MkqlItemType = ImportTypeFromProto(txResult.GetItemType(), Alloc->TypeEnv);
        if (txResult.ColumnHintsSize() > 0) {
            result.ColumnOrder.reserve(txResult.GetColumnHints().size());
            auto* structType = static_cast<NKikimr::NMiniKQL::TStructType*>(result.MkqlItemType);
            THashMap<TString, ui32> memberIndices;
            for(ui32 i = 0; i < structType->GetMembersCount(); ++i) {
                memberIndices[TString(structType->GetMemberName(i))] = i;
            }

            for(auto& name: txResult.GetColumnHints()) {
                auto it = memberIndices.find(name);
                YQL_ENSURE(it != memberIndices.end(), "undetermined column name: " << name);
                result.ColumnOrder.push_back(it->second);
            }
        }
    }
}

bool TKqpPhyTxHolder::IsLiteralTx() const {
    return LiteralTx;
}

const NKikimr::NKqp::TStagePredictor& TKqpPhyTxHolder::GetCalculationPredictor(const size_t stageIdx) const {
    YQL_ENSURE(stageIdx < Predictors.size(), "incorrect stage idx for predictor");
    return Predictors[stageIdx];
}

TPreparedQueryHolder::TPreparedQueryHolder(NKikimrKqp::TPreparedQuery* proto,
    const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry)
    : Proto(proto)
    , Alloc(std::move(std::make_shared<TPreparedQueryAllocHolder>(functionRegistry)))
{
    THashSet<TString> tablesSet;
    const auto& phyQuery = Proto->GetPhysicalQuery();
    Transactions.reserve(phyQuery.TransactionsSize());
    for (const auto& phyTx: phyQuery.GetTransactions()) {
        TKqpPhyTxHolder::TConstPtr txHolder = std::make_shared<const TKqpPhyTxHolder>(
            Proto, &phyTx, Alloc);
        Transactions.emplace_back(std::move(txHolder));
        for (const auto& stage: phyTx.GetStages()) {

            for (const auto& tableOp: stage.GetTableOps()) {
                tablesSet.insert(tableOp.GetTable().GetPath());
            }

            for (const auto& input : stage.GetInputs()) {
                if (input.GetTypeCase() == NKqpProto::TKqpPhyConnection::kStreamLookup) {
                    tablesSet.insert(input.GetStreamLookup().GetTable().GetPath());
                }

                if (input.GetTypeCase() == NKqpProto::TKqpPhyConnection::kSequencer) {
                    tablesSet.insert(input.GetSequencer().GetTable().GetPath());
                }
            }

            for (const auto& source : stage.GetSources()) {
                if (source.GetTypeCase() == NKqpProto::TKqpSource::kReadRangesSource) {
                    tablesSet.insert(source.GetReadRangesSource().GetTable().GetPath());
                }
            }
        }
    }

    QueryTables = TVector<TString>(tablesSet.begin(), tablesSet.end());
}

const TKqpPhyTxHolder::TConstPtr& TPreparedQueryHolder::GetPhyTx(ui32 txId) const {
    YQL_ENSURE(txId < Transactions.size());
    return Transactions[txId];
}

TKqpPhyTxHolder::TConstPtr TPreparedQueryHolder::GetPhyTxOrEmpty(ui32 txId) const {
    if (txId < Transactions.size()) {
        return Transactions[txId];
    }
    return nullptr;
}

TPreparedQueryHolder::~TPreparedQueryHolder() {
}

TString TPreparedQueryHolder::GetText() const {
    return Proto->GetText();
}

void TLlvmSettings::Fill(NYql::TKikimrConfiguration::TPtr config, const NKikimrKqp::EQueryType qType) {
    DisableLlvmForUdfStages = config->DisableLlvmForUdfStages();
    if (config->GetUseLlvm() == NYql::EOptionalFlag::Disabled) {
        UseLlvmExternalDirective = false;
    } else if (config->GetUseLlvm() == NYql::EOptionalFlag::Enabled) {
        UseLlvmExternalDirective = true;
    }
    if (!IsSqlQuery(qType)) {
        UseLlvmExternalDirective = false;
    }
}

bool TLlvmSettings::GetUseLlvm(const NYql::NDqProto::TProgram::TSettings& kqpSettingsProto) const {
    TStagePredictor stagePredictor;
    stagePredictor.DeserializeFromKqpSettings(kqpSettingsProto);
    if (DisableLlvmForUdfStages && stagePredictor.IsHasUdf()) {
        return false;
    } else if (UseLlvmExternalDirective) {
        return *UseLlvmExternalDirective;
    } else {
        return stagePredictor.NeedLLVM();
    }
}

} // namespace NKikimr::NKqp

