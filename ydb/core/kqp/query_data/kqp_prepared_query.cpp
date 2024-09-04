#include "kqp_prepared_query.h"

#include <ydb/core/base/path.h>
#include <ydb/core/kqp/common/kqp_resolve.h>
#include <ydb/library/mkql_proto/mkql_proto.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>
#include <ydb/library/yql/core/yql_data_provider.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/mkql_proto/mkql_proto.h>
#include <ydb/core/kqp/common/simple/helpers.h>
#include <ydb/core/protos/kqp_physical.pb.h>
#include <ydb/library/services/services.pb.h>

#include <ydb/library/actors/core/log.h>

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
    const NKqpProto::TKqpPhyTx* proto, const std::shared_ptr<TPreparedQueryAllocHolder>& alloc, TIntrusivePtr<TTableConstInfoMap> tableConstInfoById)
    : PreparedQuery(pq)
    , Proto(proto)
    , LiteralTx(CalcIsLiteralTx(proto))
    , Alloc(alloc)
    , TableConstInfoById(tableConstInfoById)
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

        YQL_ENSURE(Alloc);
        result.MkqlItemType = ImportTypeFromProto(txResult.GetItemType(), Alloc->TypeEnv);
        //Hack to prevent data race. Side effect of IsPresortSupported - fill cached value.
        //So no more concurent write subsequently
        result.MkqlItemType->IsPresortSupported();
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

TIntrusiveConstPtr<TTableConstInfoMap> TKqpPhyTxHolder::GetTableConstInfoById() const {
    return TableConstInfoById;
}

bool TKqpPhyTxHolder::IsLiteralTx() const {
    return LiteralTx;
}

std::optional<std::pair<bool, std::pair<TString, TString>>>
TKqpPhyTxHolder::GetSchemeOpTempTablePath() const {
    if (GetType() != NKqpProto::TKqpPhyTx::TYPE_SCHEME) {
        return std::nullopt;
    }
    auto& schemeOperation = GetSchemeOperation();
    switch (schemeOperation.GetOperationCase()) {
        case NKqpProto::TKqpSchemeOperation::kCreateTable: {
            const auto& modifyScheme = schemeOperation.GetCreateTable();
            const NKikimrSchemeOp::TTableDescription* tableDesc = nullptr;
            switch (modifyScheme.GetOperationType()) {
                case NKikimrSchemeOp::ESchemeOpCreateTable: {
                    tableDesc = &modifyScheme.GetCreateTable();
                    break;
                }
                case NKikimrSchemeOp::ESchemeOpCreateIndexedTable: {
                    tableDesc = &modifyScheme.GetCreateIndexedTable().GetTableDescription();
                    break;
                }
                default:
                    return std::nullopt;
            }
            if (tableDesc->HasTemporary()) {
                if (tableDesc->GetTemporary()) {
                    return {{true, {modifyScheme.GetWorkingDir(), tableDesc->GetName()}}};
                }
            }
            break;
        }
        case NKqpProto::TKqpSchemeOperation::kDropTable: {
            auto modifyScheme = schemeOperation.GetDropTable();
            auto* dropTable = modifyScheme.MutableDrop();

            return {{false, {modifyScheme.GetWorkingDir(), dropTable->GetName()}}};
        }
        default:
            return std::nullopt;
    }
    return std::nullopt;
}

const NKikimr::NKqp::TStagePredictor& TKqpPhyTxHolder::GetCalculationPredictor(const size_t stageIdx) const {
    YQL_ENSURE(stageIdx < Predictors.size(), "incorrect stage idx for predictor");
    return Predictors[stageIdx];
}

TPreparedQueryHolder::TPreparedQueryHolder(NKikimrKqp::TPreparedQuery* proto,
    const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry, bool noFillTables)
    : Proto(proto)
    , Alloc(nullptr)
    , TableConstInfoById(MakeIntrusive<TTableConstInfoMap>())
{

    if (functionRegistry) {
        Alloc = std::make_shared<TPreparedQueryAllocHolder>(functionRegistry);
    }

    // In case of some compilation failures filling tables may produce new problems which may replace original error messages.
    if (noFillTables) {
        return;
    }

    THashSet<TString> tablesSet;
    const auto& phyQuery = Proto->GetPhysicalQuery();
    Transactions.reserve(phyQuery.TransactionsSize());

    // Init TableConstInfoById
    for (const auto& phyTx: phyQuery.GetTransactions()) {
        for (const auto& phyTable : phyTx.GetTables()) {
            FillTable(phyTable);
        }
        FillTables(phyTx.GetStages());
    }

    for (const auto& phyTx: phyQuery.GetTransactions()) {
        TKqpPhyTxHolder::TConstPtr txHolder = std::make_shared<const TKqpPhyTxHolder>(
            Proto, &phyTx, Alloc, TableConstInfoById);
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
            for (const auto& sink : stage.GetSinks()) {
                if (sink.GetTypeCase() == NKqpProto::TKqpSink::kInternalSink && sink.GetInternalSink().GetSettings().Is<NKikimrKqp::TKqpTableSinkSettings>()) {
                    NKikimrKqp::TKqpTableSinkSettings settings;
                    YQL_ENSURE(sink.GetInternalSink().GetSettings().UnpackTo(&settings), "Failed to unpack settings");
                    tablesSet.insert(settings.GetTable().GetPath());
                }
            }
        }
    }

    QueryTables = TVector<TString>(tablesSet.begin(), tablesSet.end());
}

TIntrusivePtr<TTableConstInfo>& TPreparedQueryHolder::GetInfo(const TTableId& tableId) {
    auto info = TableConstInfoById->Map.FindPtr(tableId);
    MKQL_ENSURE_S(info);
    return *info;
}

const THashMap<TTableId, TIntrusivePtr<TTableConstInfo>>& TPreparedQueryHolder::GetTableConstInfo() const {
    return TableConstInfoById->Map;
}

void TPreparedQueryHolder::FillTable(const NKqpProto::TKqpPhyTable& phyTable) {
    auto tableId = MakeTableId(phyTable.GetId());

    auto infoPtr = TableConstInfoById->Map.FindPtr(tableId);
    if (!infoPtr) {
        auto infoPtr = MakeIntrusive<TTableConstInfo>(phyTable.GetId().GetPath());
        TableConstInfoById->Map[tableId] = infoPtr;
        infoPtr->FillTable(phyTable);
    } else {
        for (const auto& [_, phyColumn] : phyTable.GetColumns()) {
            (*infoPtr)->FillColumn(phyColumn);
        }
    }
}

void TPreparedQueryHolder::FillTables(const google::protobuf::RepeatedPtrField< ::NKqpProto::TKqpPhyStage>& stages) {
    for (auto& stage : stages) {
        for (auto& op : stage.GetTableOps()) {
            auto& info = GetInfo(MakeTableId(op.GetTable()));
            for (auto& column : op.GetColumns()) {
                info->AddColumn(column.GetName());
            }
        }

        for (auto& source : stage.GetSources()) {
            if (source.HasReadRangesSource()) {
                auto& info = GetInfo(MakeTableId(source.GetReadRangesSource().GetTable()));
                for (auto& column : source.GetReadRangesSource().GetColumns()) {
                    info->AddColumn(column.GetName());
                }
            }
        }

        for (const auto& sink : stage.GetSinks()) {
            if (sink.GetTypeCase() == NKqpProto::TKqpSink::kInternalSink && sink.GetInternalSink().GetSettings().Is<NKikimrKqp::TKqpTableSinkSettings>()) {
                NKikimrKqp::TKqpTableSinkSettings settings;
                YQL_ENSURE(sink.GetInternalSink().GetSettings().UnpackTo(&settings), "Failed to unpack settings");

                auto& info = GetInfo(MakeTableId(settings.GetTable()));
                for (auto& column : settings.GetColumns()) {
                    info->AddColumn(column.GetName());
                }
            }
        }

        for (const auto& input : stage.GetInputs()) {
            if (input.GetTypeCase() == NKqpProto::TKqpPhyConnection::kStreamLookup) {
                auto& info = GetInfo(MakeTableId(input.GetStreamLookup().GetTable()));
                for (auto& column : input.GetStreamLookup().GetColumns()) {
                    info->AddColumn(column);
                }
            }

            if (input.GetTypeCase() == NKqpProto::TKqpPhyConnection::kSequencer) {
                auto& info = GetInfo(MakeTableId(input.GetSequencer().GetTable()));
                for(auto& column: input.GetSequencer().GetColumns()) {
                    info->AddColumn(column);
                }
            }
        }
    }
}

bool TPreparedQueryHolder::HasTempTables(TKqpTempTablesState::TConstPtr tempTablesState, bool withSessionId) const {
    if (!tempTablesState) {
        return false;
    }
    for (const auto& table: QueryTables) {
        auto infoIt = tempTablesState->FindInfo(table, withSessionId);
        if (infoIt != tempTablesState->TempTables.end()) {
            return true;
        }
    }


    if (withSessionId) {
        for (const auto& tx: Transactions) {
            auto optPath = tx->GetSchemeOpTempTablePath();
            if (!optPath) {
                continue;
            } else {
                const auto& [isCreate, path] = *optPath;
                if (isCreate) {
                    return true;
                } else {
                    auto infoIt = tempTablesState->FindInfo(JoinPath({path.first, path.second}), withSessionId);
                    if (infoIt != tempTablesState->TempTables.end()) {
                        return true;
                    }
                }
            }
        }
    }
    return false;
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

