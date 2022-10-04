#include "kqp_table_resolver.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/cputime.h>
#include <ydb/core/base/path.h>
#include <ydb/core/kqp/executer/kqp_executer.h>


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

    TKqpTableResolver(const TActorId& owner, ui64 txId, TMaybe<TString> userToken,
        const TVector<IKqpGateway::TPhysicalTxData>& transactions, TKqpTableKeys& tableKeys,
        TKqpTasksGraph& tasksGraph)
        : Owner(owner)
        , TxId(txId)
        , UserToken(userToken)
        , Transactions(transactions)
        , TableKeys(tableKeys)
        , TasksGraph(tasksGraph) {}

    void Bootstrap() {
        ResolveTables();
        Become(&TKqpTableResolver::ResolveTablesState);
    }

private:
    STATEFN(ResolveTablesState) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleResolveTables);
            hFunc(TEvents::TEvPoison, HandleResolveTables);
            default:
                UnexpectedEvent("ResolveTablesState", ev->GetTypeRewrite());
        }
    }

    void HandleResolveTables(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        auto timer = std::make_unique<NCpuTime::TCpuTimer>(CpuTime);

        const auto& entries = ev->Get()->Request->ResultSet;
        LOG_D("Resolved tables: " << entries.size());
        YQL_ENSURE(entries.size() == TableKeys.Size());

        for (auto& entry : entries) {
            switch (entry.Status) {
                case NSchemeCache::TSchemeCacheNavigate::EStatus::Ok:
                    break;

                case NSchemeCache::TSchemeCacheNavigate::EStatus::PathErrorUnknown:
                case NSchemeCache::TSchemeCacheNavigate::EStatus::PathNotTable:
                case NSchemeCache::TSchemeCacheNavigate::EStatus::TableCreationNotComplete:
                    ReplyErrorAndDie(Ydb::StatusIds::SCHEME_ERROR,
                        YqlIssue({}, NYql::TIssuesIds::KIKIMR_SCHEME_MISMATCH, TStringBuilder()
                            << "Table scheme error `" << JoinPath(entry.Path) << "`: " << entry.Status << '.'));
                    return;

                default:
                    ReplyErrorAndDie(Ydb::StatusIds::UNAVAILABLE,
                        YqlIssue({}, NYql::TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE, TStringBuilder()
                            << "Failed to resolve table `" << JoinPath(entry.Path) << "`: " << entry.Status << '.'));
                    return;

            }

            auto* table = TableKeys.FindTablePtr(entry.TableId);
            if (!table) {
                timer.reset();
                ReplyErrorAndDie(Ydb::StatusIds::UNAVAILABLE,
                    YqlIssue({}, NYql::TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE, TStringBuilder()
                        << "Unresolved table `" << JoinPath(entry.Path) << "` with tableId: " << entry.TableId << "."));
                return;
            }

            if (entry.Kind == NSchemeCache::TSchemeCacheNavigate::KindColumnTable) {
                YQL_ENSURE(entry.ColumnTableInfo || entry.OlapStoreInfo);
                // NOTE: entry.SysViewInfo might not be empty for OLAP stats virtual tables
                table->TableKind = ETableKind::Olap;
            } else if (entry.TableId.IsSystemView()) {
                table->TableKind = ETableKind::SysView;
            } else {
                table->TableKind = ETableKind::Datashard;
            }

            // TODO: Resolve columns by id
            TMap<TStringBuf, ui32> columnsMap;
            for (auto& [columnId, column] : entry.Columns) {
                auto ret = columnsMap.emplace(column.Name, columnId);
                YQL_ENSURE(ret.second, "" << column.Name);

                if (column.KeyOrder >= 0) {
                    table->Columns.emplace(column.Name, TKqpTableKeys::TColumn());

                    if (table->KeyColumns.size() <= (ui32) column.KeyOrder) {
                        table->KeyColumns.resize(column.KeyOrder + 1);
                        table->KeyColumnTypes.resize(column.KeyOrder + 1);
                    }
                    table->KeyColumns[column.KeyOrder] = column.Name;
                    table->KeyColumnTypes[column.KeyOrder] = column.PType;
                }
            }

            for (auto& keyColumn : table->KeyColumns) {
                YQL_ENSURE(!keyColumn.empty());
            }

            auto& sysColumns = GetSystemColumns();
            for (auto& [columnName, columnKey] : table->Columns) {
                if (auto* systemColumn = sysColumns.FindPtr(columnName)) {
                    columnKey.Id = systemColumn->ColumnId;
                    columnKey.Type = NScheme::TTypeInfo(systemColumn->TypeId);
                    continue;
                }

                auto* columnId = columnsMap.FindPtr(columnName);
                if (!columnId) {
                    timer.reset();
                    ReplyErrorAndDie(Ydb::StatusIds::SCHEME_ERROR,
                        YqlIssue({}, NYql::TIssuesIds::KIKIMR_SCHEME_MISMATCH, TStringBuilder()
                            << "Unknown column `" << columnName << "` at table `" << JoinPath(entry.Path) << "`."));
                    return;
                }

                auto* column = entry.Columns.FindPtr(*columnId);
                YQL_ENSURE(column);

                columnKey.Id = column->Id;
                columnKey.Type = column->PType;
            }
        }

        ResolveKeys();
        timer.reset();

        Become(&TKqpTableResolver::ResolveKeysState);
    }

    void HandleResolveTables(TEvents::TEvPoison::TPtr&) {
        PassAway();
    }

private:
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
    // TODO: Get rid of ResolveTables & TableKeys, get table information from phy tx proto.
    void ResolveTables() {
        for (auto& tx : Transactions) {
            for (auto& stage : tx.Body->GetStages()) {
                for (auto& op : stage.GetTableOps()) {
                    auto& table = TableKeys.GetOrAddTable(MakeTableId(op.GetTable()), op.GetTable().GetPath());
                    for (auto& column : op.GetColumns()) {
                        table.Columns.emplace(column.GetName(), TKqpTableKeys::TColumn());
                    }
                }

                for (const auto& input : stage.GetInputs()) {
                    if (input.GetTypeCase() == NKqpProto::TKqpPhyConnection::kStreamLookup) {
                        const auto& streamLookup = input.GetStreamLookup();
                        auto& table = TableKeys.GetOrAddTable(MakeTableId(streamLookup.GetTable()), streamLookup.GetTable().GetPath());
                        for (auto& column : input.GetStreamLookup().GetColumns()) {
                            table.Columns.emplace(column, TKqpTableKeys::TColumn());
                        }
                    }
                }
            }
        }

        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        request->ResultSet.reserve(TableKeys.Size());
        for (auto& [tableId, table] : TableKeys.Get()) {
            NSchemeCache::TSchemeCacheNavigate::TEntry entry;
            entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByTableId;
            entry.TableId = tableId;
            entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpTable;
            entry.ShowPrivatePath = true;

            request->ResultSet.emplace_back(std::move(entry));
        }

        auto ev = MakeHolder<TEvTxProxySchemeCache::TEvNavigateKeySet>(request.Release());
        Send(MakeSchemeCacheID(), ev.Release());
    }

    void ResolveKeys() {
        FillKqpTasksGraphStages(TasksGraph, Transactions);

        auto request = MakeHolder<NSchemeCache::TSchemeCacheRequest>();
        request->ResultSet.reserve(TasksGraph.GetStagesInfo().size());
        if (UserToken) {
            request->UserToken = new NACLib::TUserToken(*UserToken);
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
    const TMaybe<TString> UserToken;
    const TVector<IKqpGateway::TPhysicalTxData>& Transactions;
    TKqpTableKeys& TableKeys;

    // TODO: TableResolver should not populate TasksGraph as it's not related to its job (bad API).
    TKqpTasksGraph& TasksGraph;

    bool ShouldTerminate = false;
    TMaybe<ui32> GotUnexpectedEvent;
    TDuration CpuTime;
};

} // anonymous namespace

NActors::IActor* CreateKqpTableResolver(const TActorId& owner, ui64 txId, TMaybe<TString> userToken,
    const TVector<IKqpGateway::TPhysicalTxData>& transactions, TKqpTableKeys& tableKeys, TKqpTasksGraph& tasksGraph) {
    return new TKqpTableResolver(owner, txId, userToken, transactions, tableKeys, tasksGraph);
}

} // namespace NKikimr::NKqp
