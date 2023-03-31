#include "kqp_table_resolver.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/cputime.h>
#include <ydb/core/base/path.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>


namespace NKikimr::NKqp {

using namespace NActors;
using namespace NYql;
using namespace NYql::NDq;

namespace {

#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_EXECUTER, "TxId: " << TxId << ". " << stream)
#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::KQP_EXECUTER, "TxId: " << TxId << ". " << stream)
#define LOG_C(stream) LOG_CRIT_S(*TlsActivationContext, NKikimrServices::KQP_EXECUTER, "TxId: " << TxId << ". " << stream)
#define LOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::KQP_EXECUTER, "TxId: " << TxId << ". " << stream)

class TKqpTableResolver : public TActorBootstrapped<TKqpTableResolver> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_TABLE_RESOLVER;
    }

    TKqpTableResolver(const TActorId& owner, ui64 txId,
        const TIntrusiveConstPtr<NACLib::TUserToken>& userToken,
        const TVector<IKqpGateway::TPhysicalTxData>& transactions, TKqpTableKeys& tableKeys,
        TKqpTasksGraph& tasksGraph)
        : Owner(owner)
        , TxId(txId)
        , UserToken(userToken)
        , Transactions(transactions)
        , TableKeys(tableKeys)
        , TasksGraph(tasksGraph) {}

    void Bootstrap() {
        FillTables();

        ResolveKeys();
        Become(&TKqpTableResolver::ResolveKeysState);
    }

private:
    static void FillColumn(const NKqpProto::TKqpPhyColumn& phyColumn, TKqpTableKeys::TTable& table) {
        if (table.Columns.FindPtr(phyColumn.GetId().GetName())) {
            return;
        }

        TKqpTableKeys::TColumn column;
        column.Id = phyColumn.GetId().GetId();

        if (phyColumn.GetTypeId() != NScheme::NTypeIds::Pg) {
            column.Type = NScheme::TTypeInfo(phyColumn.GetTypeId());
        } else {
            column.Type = NScheme::TTypeInfo(phyColumn.GetTypeId(),
                NPg::TypeDescFromPgTypeName(phyColumn.GetPgTypeName()));
        }

        table.Columns.emplace(phyColumn.GetId().GetName(), std::move(column));
    }

    void FillTable(const NKqpProto::TKqpPhyTable& phyTable) {
        auto tableId = MakeTableId(phyTable.GetId());

        auto table = TableKeys.FindTablePtr(tableId);
        if (!table) {
            table = &TableKeys.GetOrAddTable(tableId, phyTable.GetId().GetPath());

            switch (phyTable.GetKind()) {
                case NKqpProto::TABLE_KIND_DS:
                    table->TableKind = ETableKind::Datashard;
                    break;
                case NKqpProto::TABLE_KIND_OLAP:
                    table->TableKind = ETableKind::Olap;
                    break;
                case NKqpProto::TABLE_KIND_SYS_VIEW:
                    table->TableKind = ETableKind::SysView;
                    break;
                default:
                    YQL_ENSURE(false, "Unexpected phy table kind: " << (i64) phyTable.GetKind());
            }

            for (const auto& [_, phyColumn] : phyTable.GetColumns()) {
                FillColumn(phyColumn, *table);
            }

            YQL_ENSURE(table->KeyColumns.empty());
            table->KeyColumns.reserve(phyTable.KeyColumnsSize());
            YQL_ENSURE(table->KeyColumnTypes.empty());
            table->KeyColumnTypes.reserve(phyTable.KeyColumnsSize());
            for (const auto& keyColumnId : phyTable.GetKeyColumns()) {
                const auto& column = table->Columns.FindPtr(keyColumnId.GetName());
                YQL_ENSURE(column);

                table->KeyColumns.push_back(keyColumnId.GetName());
                table->KeyColumnTypes.push_back(column->Type);
            }
        } else {
            for (const auto& [_, phyColumn] : phyTable.GetColumns()) {
                FillColumn(phyColumn, *table);
            }
        }
    }

    void FillTables() {
        auto addColumn = [](TKqpTableKeys::TTable& table, const TString& columnName) mutable {
            auto& sysColumns = GetSystemColumns();
            if (table.Columns.FindPtr(columnName)) {
                return;
            }

            auto* systemColumn = sysColumns.FindPtr(columnName);
            YQL_ENSURE(systemColumn, "Unknown table column"
                << ", table: " << table.Path
                << ", column: " << columnName);

            TKqpTableKeys::TColumn column;
            column.Id = systemColumn->ColumnId;
            column.Type = NScheme::TTypeInfo(systemColumn->TypeId);
            table.Columns.emplace(columnName, std::move(column));
        };

        for (auto& tx : Transactions) {
            for (const auto& phyTable : tx.Body->GetTables()) {
                FillTable(phyTable);
            }

            for (auto& stage : tx.Body->GetStages()) {
                for (auto& op : stage.GetTableOps()) {
                    auto& table = TableKeys.GetTable(MakeTableId(op.GetTable()));
                    for (auto& column : op.GetColumns()) {
                        addColumn(table, column.GetName());
                    }
                }

                for (auto& source : stage.GetSources()) {
                    if (source.HasReadRangesSource()) {
                        auto& table = TableKeys.GetTable(MakeTableId(source.GetReadRangesSource().GetTable()));
                        for (auto& column : source.GetReadRangesSource().GetColumns()) {
                            addColumn(table, column.GetName());
                        }
                    }
                }

                for (const auto& input : stage.GetInputs()) {
                    if (input.GetTypeCase() == NKqpProto::TKqpPhyConnection::kStreamLookup) {
                        auto& table = TableKeys.GetTable(MakeTableId(input.GetStreamLookup().GetTable()));
                        for (auto& column : input.GetStreamLookup().GetColumns()) {
                            addColumn(table, column);
                        }
                    }
                }
            }
        }
    }

    STATEFN(ResolveKeysState) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvResolveKeySetResult, HandleResolveKeys);
            hFunc(TEvents::TEvPoison, HandleResolveKeys);
            default: {
                LOG_C("ResolveKeysState: unexpected event " << ev->GetTypeRewrite());
                GotUnexpectedEvent = ev->GetTypeRewrite();
            }
        }
    }

    void HandleResolveKeys(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr &ev) {
        if (ShouldTerminate) {
            PassAway();
            return;
        }

        if (GotUnexpectedEvent) {
            ReplyErrorAndDie(Ydb::StatusIds::INTERNAL_ERROR, TIssue(TStringBuilder()
                << "Unexpected event: " << *GotUnexpectedEvent));
            return;
        }

        auto timer = std::make_unique<NCpuTime::TCpuTimer>(CpuTime);

        auto& results = ev->Get()->Request->ResultSet;
        LOG_D("Resolved key sets: " << results.size());

        for (auto& entry : results) {
            if (entry.Status != NSchemeCache::TSchemeCacheRequest::EStatus::OkData) {
                LOG_E("Error resolving keys for entry: " << entry.ToString(*AppData()->TypeRegistry));

                auto* table = TableKeys.FindTablePtr(entry.KeyDescription->TableId);
                TStringBuilder path;
                if (table) {
                    path << '`' << table->Path << '`';
                } else {
                    path << "unresolved `" << entry.KeyDescription->TableId << '`';
                }

                timer.reset();
                ReplyErrorAndDie(Ydb::StatusIds::SCHEME_ERROR,
                    YqlIssue({}, NYql::TIssuesIds::KIKIMR_SCHEME_MISMATCH, TStringBuilder()
                        << "Failed to resolve table " << path << " keys: " << entry.Status << '.'));
                return;
            }

            for (auto& partition : entry.KeyDescription->GetPartitions()) {
                YQL_ENSURE(partition.Range);
            }

            LOG_D("Resolved key: " << entry.ToString(*AppData()->TypeRegistry));

            auto& stageInfo = DecodeStageInfo(entry.UserData);
            stageInfo.Meta.ShardKey = std::move(entry.KeyDescription);
            stageInfo.Meta.ShardKind = std::move(entry.Kind);
        }

        timer.reset();

        auto replyEv = std::make_unique<TEvKqpExecuter::TEvTableResolveStatus>();
        replyEv->CpuTime = CpuTime;

        Send(Owner, replyEv.release());
        PassAway();
    }

    void HandleResolveKeys(TEvents::TEvPoison::TPtr&) {
        ShouldTerminate = true;
    }

private:
    void ResolveKeys() {
        FillKqpTasksGraphStages(TasksGraph, Transactions);

        auto request = MakeHolder<NSchemeCache::TSchemeCacheRequest>();
        request->ResultSet.reserve(TasksGraph.GetStagesInfo().size());
        if (UserToken && !UserToken->GetSerializedToken().empty()) {
            request->UserToken = UserToken;
        }

        for (auto& pair : TasksGraph.GetStagesInfo()) {
            auto& stageInfo = pair.second;
            if (!stageInfo.Meta.ShardOperations.empty()) {
                YQL_ENSURE(stageInfo.Meta.TableId);
                YQL_ENSURE(stageInfo.Meta.ShardOperations.size() == 1);
                auto operation = *stageInfo.Meta.ShardOperations.begin();

                const TKqpTableKeys::TTable* table = TableKeys.FindTablePtr(stageInfo.Meta.TableId);
                stageInfo.Meta.TableKind = table->TableKind;

                stageInfo.Meta.ShardKey = ExtractKey(stageInfo.Meta.TableId, operation);

                auto& entry = request->ResultSet.emplace_back(std::move(stageInfo.Meta.ShardKey));
                entry.UserData = EncodeStageInfo(stageInfo);
                switch (operation) {
                    case TKeyDesc::ERowOperation::Read:
                        entry.Access = NACLib::EAccessRights::SelectRow;
                        break;
                    case TKeyDesc::ERowOperation::Update:
                        entry.Access = NACLib::EAccessRights::UpdateRow;
                        break;
                    case TKeyDesc::ERowOperation::Erase:
                        entry.Access = NACLib::EAccessRights::EraseRow;
                        break;
                    default:
                        YQL_ENSURE(false, "Unsupported row operation mode: " << (ui32)operation);
                }
            }
        }

        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvResolveKeySet(request));
    }

private:
    THolder<TKeyDesc> ExtractKey(const TTableId& table, TKeyDesc::ERowOperation operation) {
        const auto& tableKey = TableKeys.GetTable(table);
        auto range = GetFullRange(tableKey.KeyColumnTypes.size());

        return MakeHolder<TKeyDesc>(table, range.ToTableRange(), operation, tableKey.KeyColumnTypes,
            TVector<TKeyDesc::TColumnOp>{});
    }

    static TSerializedTableRange GetFullRange(ui32 columnsCount) {
        TVector<TCell> fromValues(columnsCount);
        TVector<TCell> toValues;

        return TSerializedTableRange(fromValues, /* inclusiveFrom */ true, toValues, /* inclusiveTo */ false);
    }

    static uintptr_t EncodeStageInfo(TKqpTasksGraph::TStageInfoType& stageInfo) {
        return reinterpret_cast<uintptr_t>(&stageInfo);
    }

    static TKqpTasksGraph::TStageInfoType& DecodeStageInfo(uintptr_t userData) {
        return *reinterpret_cast<TKqpTasksGraph::TStageInfoType*>(userData);
    }

private:
    void UnexpectedEvent(const TString& state, ui32 eventType) {
        LOG_C("TKqpTableResolver, unexpected event: " << eventType << ", at state:" << state << ", self: " << SelfId());
        auto issue = NYql::YqlIssue({}, NYql::TIssuesIds::UNEXPECTED, "Internal error while executing transaction.");
        ReplyErrorAndDie(Ydb::StatusIds::INTERNAL_ERROR, std::move(issue));
    }

    void ReplyErrorAndDie(Ydb::StatusIds::StatusCode status, TIssue&& issue) {
        auto replyEv = std::make_unique<TEvKqpExecuter::TEvTableResolveStatus>();
        replyEv->Status = status;
        replyEv->Issues.AddIssue(std::move(issue));
        replyEv->CpuTime = CpuTime;
        Send(Owner, replyEv.release());
        PassAway();
    }

private:
    const TActorId Owner;
    const ui64 TxId;
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    const TVector<IKqpGateway::TPhysicalTxData>& Transactions;
    TKqpTableKeys& TableKeys;

    // TODO: TableResolver should not populate TasksGraph as it's not related to its job (bad API).
    TKqpTasksGraph& TasksGraph;

    bool ShouldTerminate = false;
    TMaybe<ui32> GotUnexpectedEvent;
    TDuration CpuTime;
};

} // anonymous namespace

NActors::IActor* CreateKqpTableResolver(const TActorId& owner, ui64 txId,
    const TIntrusiveConstPtr<NACLib::TUserToken>& userToken,
    const TVector<IKqpGateway::TPhysicalTxData>& transactions, TKqpTableKeys& tableKeys, TKqpTasksGraph& tasksGraph) {
    return new TKqpTableResolver(owner, txId, userToken, transactions, tableKeys, tasksGraph);
}

} // namespace NKikimr::NKqp
