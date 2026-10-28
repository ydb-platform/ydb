#include "kqp_prepared_query.h"

#include <ydb/core/base/path.h>
#include <ydb/core/kqp/common/kqp_resolve.h>
#include <ydb/core/kqp/common/simple/helpers.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>
#include <ydb/core/protos/kqp_physical.pb.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/library/mkql_proto/mkql_proto.h>
#include <ydb/library/mkql_proto/mkql_proto.h>
#include <ydb/library/services/services.pb.h>
#include <yql/essentials/core/yql_data_provider.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/mkql_node.h>

#include <ydb/library/actors/core/log.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::KQP_EXECUTER

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

    TPreparedQueryAllocHolder()
        : Alloc(__LOCATION__)
        , TypeEnv(Alloc)
    {
        Alloc.Release();
    }

    ~TPreparedQueryAllocHolder()
    {
        Alloc.Acquire();
    }
};

const NKqpProto::TKqpInternalSink* FindTableTruncateSink(const NKqpProto::TKqpPhyTx& tx) {
    for (const auto& stage : tx.GetStages()) {
        for (const auto& sink : stage.GetSinks()) {
            if (sink.HasInternalSink()
                && sink.GetInternalSink().GetSettings().Is<NKikimrKqp::TKqpTableTruncateSettings>()) {
                return &sink.GetInternalSink();
            }
        }
    }
    return nullptr;
}

NKikimrKqp::TKqpTableTruncateSettings TKqpPhyTxHolder::GetUnsafeTruncate() const {
    const auto* sink = FindTableTruncateSink(*Proto);
    YQL_ENSURE(sink);
    YQL_ENSURE(Proto->GetType() == NKqpProto::TKqpPhyTx::TYPE_GENERIC);
    YQL_ENSURE(Proto->StagesSize() == 1 && Proto->ResultsSize() == 0 && Proto->ParamBindingsSize() == 0);
    const auto& stage = Proto->GetStages(0);
    YQL_ENSURE(stage.InputsSize() == 0 && stage.SourcesSize() == 0
        && stage.SinksSize() == 1 && stage.OutputTransformsSize() == 0);

    NKikimrKqp::TKqpTableTruncateSettings settings;
    YQL_ENSURE(sink->GetSettings().UnpackTo(&settings));
    YQL_ENSURE(!settings.GetTablePath().empty());
    return settings;
}

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
            YDB_LOG_ERROR("Cannot parse program settings for data prediction");
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
            NYql::TColumnOrder order;
            for (auto& name: txResult.GetColumnHints()) {
                order.AddColumn(name);
            }
            for (auto& [name, phy_name]: order) {
                auto it = memberIndices.find(phy_name);
                YQL_ENSURE(it != memberIndices.end(), "undetermined column name: " << name);
                result.ColumnOrder.push_back(it->second);
                result.ColumnHints.push_back(name);
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
                case NKikimrSchemeOp::ESchemeOpCreateColumnTable: {
                    if (modifyScheme.GetCreateColumnTable().HasTemporary() && modifyScheme.GetCreateColumnTable().GetTemporary()) {
                        return {{true, {modifyScheme.GetWorkingDir(), modifyScheme.GetCreateColumnTable().GetName()}}};
                    }
                    break;
                }
                default:
                    return std::nullopt;
            }
            if (tableDesc && tableDesc->HasTemporary()) {
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

namespace {

bool AreValuesKnownBeforeExecution(const NKqpProto::TKqpPhyKeyBound& bound, const THashSet<TString>& queryParams) {
    for (const auto& value : bound.GetValues()) {
        if (value.HasLiteralValue()) {
            continue;
        }
        TString name;
        if (value.HasParamValue()) {
            name = value.GetParamValue().GetParamName();
        } else if (value.HasParamElementValue()) {
            name = value.GetParamElementValue().GetParamName();
        } else {
            return false;
        }
        if (!queryParams.contains(name)) {
            return false;
        }
    }
    return true;
}

// A key range whose bounds are known before the query is executed: literals,
// or parameters supplied with the query rather than computed by it.
bool IsKeyRangeKnownBeforeExecution(const NKqpProto::TKqpPhyKeyRange& range, const THashSet<TString>& queryParams) {
    return range.HasFrom() && range.HasTo()
        && AreValuesKnownBeforeExecution(range.GetFrom(), queryParams)
        && AreValuesKnownBeforeExecution(range.GetTo(), queryParams);
}

} // namespace

TPreparedQueryHolder::TPreparedQueryHolder(NKikimrKqp::TPreparedQuery* proto,
    const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry, bool noFillTables)
    : Proto(proto)
    , Alloc(nullptr)
    , TableConstInfoById(MakeIntrusive<TTableConstInfoMap>())
{

    if (functionRegistry) {
        Alloc = std::make_shared<TPreparedQueryAllocHolder>();
    }

    // In case of some compilation failures filling tables may produce new problems which may replace original error messages.
    if (noFillTables) {
        return;
    }

    const auto& phyQuery = Proto->GetPhysicalQuery();
    Transactions.reserve(phyQuery.TransactionsSize());

    // Init TableConstInfoById
    for (const auto& phyTx: phyQuery.GetTransactions()) {
        for (const auto& phyTable : phyTx.GetTables()) {
            FillTable(phyTable);
        }
        FillTables(phyTx.GetStages());
    }

    struct TTableReads {
        TVector<const NKqpProto::TKqpPhyKeyRange*> KeyRanges;
        bool Unbounded = false;
    };
    THashMap<TString, TTableReads> tableReads;
    // A read (or write) that is not bounded by a key range.
    auto addUnbounded = [&](const TString& path) {
        tableReads[path].Unbounded = true;
    };
    // Only DataShard tables are snapshotted by key ranges.
    auto addRange = [&](const NKqpProto::TKqpPhyTableId& table, const NKqpProto::TKqpPhyKeyRange& range,
        const THashSet<TString>& queryParams)
    {
        auto info = TableConstInfoById->Map.FindPtr(MakeTableId(table));
        if (info && (*info)->TableKind == ETableKind::Datashard && IsKeyRangeKnownBeforeExecution(range, queryParams)) {
            tableReads[table.GetPath()].KeyRanges.push_back(&range);
        } else {
            addUnbounded(table.GetPath());
        }
    };

    for (const auto& phyTx: phyQuery.GetTransactions()) {
        TKqpPhyTxHolder::TConstPtr txHolder = std::make_shared<const TKqpPhyTxHolder>(
            Proto, &phyTx, Alloc, TableConstInfoById);
        Transactions.emplace_back(std::move(txHolder));

        // Parameters supplied with the query, as opposed to results of other
        // transactions of the query and internally generated values.
        THashSet<TString> queryParams;
        for (const auto& binding : phyTx.GetParamBindings()) {
            if (binding.HasExternalBinding()) {
                queryParams.insert(binding.GetName());
            }
        }

        for (const auto& stage: phyTx.GetStages()) {

            for (const auto& tableOp: stage.GetTableOps()) {
                if (tableOp.HasReadRange()) {
                    addRange(tableOp.GetTable(), tableOp.GetReadRange().GetKeyRange(), queryParams);
                } else {
                    addUnbounded(tableOp.GetTable().GetPath());
                }
            }

            for (const auto& input : stage.GetInputs()) {
                if (input.GetTypeCase() == NKqpProto::TKqpPhyConnection::kStreamLookup) {
                    addUnbounded(input.GetStreamLookup().GetTable().GetPath());
                }

                if (input.GetTypeCase() == NKqpProto::TKqpPhyConnection::kSequencer) {
                    addUnbounded(input.GetSequencer().GetTable().GetPath());
                }
            }

            for (const auto& source : stage.GetSources()) {
                if (source.GetTypeCase() == NKqpProto::TKqpSource::kReadRangesSource) {
                    const auto& read = source.GetReadRangesSource();
                    if (read.HasKeyRange()) {
                        addRange(read.GetTable(), read.GetKeyRange(), queryParams);
                    } else {
                        addUnbounded(read.GetTable().GetPath());
                    }
                }

                if (source.GetTypeCase() == NKqpProto::TKqpSource::kFullTextSource) {
                    addUnbounded(source.GetFullTextSource().GetTable().GetPath());
                }
            }

            auto fillFromSettings = [&](const NKikimrKqp::TKqpTableSinkSettings& settings) {
                addUnbounded(settings.GetTable().GetPath());
                for (const auto& indexSettings : settings.GetIndexes()) {
                    addUnbounded(indexSettings.GetTable().GetPath());
                    if (indexSettings.HasFulltextSettings()) {
                        addUnbounded(indexSettings.GetDocsTable().GetPath());
                        if (indexSettings.HasDictTable()) {
                            addUnbounded(indexSettings.GetDictTable().GetPath());
                        }
                        addUnbounded(indexSettings.GetStatsTable().GetPath());
                    }
                }
            };
            for (const auto& sink : stage.GetSinks()) {
                if (sink.GetTypeCase() == NKqpProto::TKqpSink::kInternalSink && sink.GetInternalSink().GetSettings().Is<NKikimrKqp::TKqpTableSinkSettings>()) {
                    NKikimrKqp::TKqpTableSinkSettings settings;
                    YQL_ENSURE(sink.GetInternalSink().GetSettings().UnpackTo(&settings), "Failed to unpack settings");
                    fillFromSettings(settings);
                }
            }
            for (const auto& transform : stage.GetOutputTransforms()) {
                if (transform.GetTypeCase() == NKqpProto::TKqpOutputTransform::kInternalSink && transform.GetInternalSink().GetSettings().Is<NKikimrKqp::TKqpTableSinkSettings>()) {
                    NKikimrKqp::TKqpTableSinkSettings settings;
                    YQL_ENSURE(transform.GetInternalSink().GetSettings().UnpackTo(&settings), "Failed to unpack settings");
                    fillFromSettings(settings);
                }
            }
        }
    }

    QueryTables.reserve(tableReads.size());
    for (auto& [path, reads] : tableReads) {
        auto& table = QueryTables.emplace_back();
        table.Path = path;
        if (!reads.Unbounded) {
            table.KeyRanges = std::move(reads.KeyRanges);
        }
    }
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

        auto fillFromSettings = [&](const NKikimrKqp::TKqpTableSinkSettings& settings) {
            if (settings.GetType() != NKikimrKqp::TKqpTableSinkSettings::MODE_FILL) {
                auto& info = GetInfo(MakeTableId(settings.GetTable()));
                for (const auto& column : settings.GetColumns()) {
                    info->AddColumn(column.GetName());
                }

                for (const auto& indexSettings : settings.GetIndexes()) {
                    if (indexSettings.GetIndexType() == NKqpProto::EKqpFullTextIndexType::EKqpFullTextCompact
                        || indexSettings.GetIndexType() == NKqpProto::EKqpFullTextIndexType::EKqpFullTextJsonCompact
                        || indexSettings.GetIndexType() == NKqpProto::EKqpFullTextIndexType::EKqpFullTextCompactRelevance) {
                        auto& info = GetInfo(MakeTableId(indexSettings.GetTable()));
                        for (const auto& column : {NTableIndex::NFulltext::TokenColumn, NTableIndex::NFulltext::MaxIdColumn,
                                NTableIndex::NFulltext::GenColumn, NTableIndex::NFulltext::AddedColumn,
                                NTableIndex::NFulltext::SegmentColumn}) {
                            info->AddColumn(column);
                        }
                    } else {
                        auto& info = GetInfo(MakeTableId(indexSettings.GetTable()));
                        for (const auto& column : indexSettings.GetColumns()) {
                            info->AddColumn(column.GetName());
                        }
                    }
                    if (indexSettings.HasFulltextSettings()) {
                        auto fillColumns = [&](const auto& indexImplTable, const auto& indexImplColumns) {
                            if (indexImplTable.GetPath().empty()) {
                                return;
                            }
                            auto& indexInfo = GetInfo(MakeTableId(indexImplTable));
                            for (const auto& column : indexImplColumns) {
                                indexInfo->AddColumn(column.GetName());
                            }
                        };
                        fillColumns(indexSettings.GetDocsTable(), indexSettings.GetDocsColumns());
                        if (indexSettings.HasDictTable()) {
                            fillColumns(indexSettings.GetDictTable(), indexSettings.GetDictColumns());
                        }
                        fillColumns(indexSettings.GetStatsTable(), indexSettings.GetStatsColumns());
                    }
                }
            }
        };

        for (const auto& sink : stage.GetSinks()) {
            if (sink.GetTypeCase() == NKqpProto::TKqpSink::kInternalSink && sink.GetInternalSink().GetSettings().Is<NKikimrKqp::TKqpTableSinkSettings>()) {
                NKikimrKqp::TKqpTableSinkSettings settings;
                YQL_ENSURE(sink.GetInternalSink().GetSettings().UnpackTo(&settings), "Failed to unpack settings");
                fillFromSettings(settings);
            }
        }

        for (const auto& transform : stage.GetOutputTransforms()) {
            if (transform.GetTypeCase() == NKqpProto::TKqpOutputTransform::kInternalSink && transform.GetInternalSink().GetSettings().Is<NKikimrKqp::TKqpTableSinkSettings>()) {
                NKikimrKqp::TKqpTableSinkSettings settings;
                YQL_ENSURE(transform.GetInternalSink().GetSettings().UnpackTo(&settings), "Failed to unpack settings");
                fillFromSettings(settings);
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
                    info->AddColumn(column.GetName());
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
        auto infoIt = tempTablesState->FindInfo(table.Path, withSessionId);
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

