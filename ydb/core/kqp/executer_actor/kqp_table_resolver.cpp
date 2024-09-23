#include "kqp_table_resolver.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/cputime.h>
#include <ydb/core/base/path.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>



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
        const TVector<IKqpGateway::TPhysicalTxData>& transactions,
        TKqpTasksGraph& tasksGraph)
        : Owner(owner)
        , TxId(txId)
        , UserToken(userToken)
        , Transactions(transactions)
        , TasksGraph(tasksGraph) {}

    void Bootstrap() {
        ResolveKeys();
        Become(&TKqpTableResolver::ResolveKeysState);
    }

private:

    STATEFN(ResolveKeysState) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvResolveKeySetResult, HandleResolveKeys);
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleResolveKeys);
            hFunc(TEvents::TEvPoison, HandleResolveKeys);
            default: {
                LOG_C("ResolveKeysState: unexpected event " << ev->GetTypeRewrite());
                GotUnexpectedEvent = ev->GetTypeRewrite();
            }
        }
    }

    void HandleResolveKeys(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        if (ShouldTerminate) {
            PassAway();
            return;
        }
        auto& results = ev->Get()->Request->ResultSet;
        if (results.size() != TableRequestIds.size()) {
            ReplyErrorAndDie(Ydb::StatusIds::INTERNAL_ERROR, TIssue(TStringBuilder() << "navigation problems for tables"));
            return;
        }
        LOG_D("Navigated key sets: " << results.size());
        for (auto& entry : results) {
            auto iter = TableRequestIds.find(entry.TableId);
            if (iter == TableRequestIds.end()) {
                ReplyErrorAndDie(Ydb::StatusIds::SCHEME_ERROR,
                    YqlIssue({}, NYql::TIssuesIds::KIKIMR_SCHEME_MISMATCH, TStringBuilder()
                        << "Incorrect tableId in reply " << entry.TableId << '.'));
                return;
            }
            TVector<TStageId> stageIds(std::move(iter->second));
            TableRequestIds.erase(entry.TableId);
            if (entry.Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
                ReplyErrorAndDie(Ydb::StatusIds::SCHEME_ERROR,
                    YqlIssue({}, NYql::TIssuesIds::KIKIMR_SCHEME_MISMATCH, TStringBuilder()
                        << "Failed to resolve table " << entry.TableId << " keys: " << entry.Status << '.'));
                return;
            }

            for (auto stageId : stageIds) {
                TasksGraph.GetStageInfo(stageId).Meta.ColumnTableInfoPtr = entry.ColumnTableInfo;
            }
        }
        NavigationFinished = true;
        TryFinish();
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

                TStringBuilder path;
                path << "unresolved `" << entry.KeyDescription->TableId << '`';

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
        ResolvingFinished = true;
        TryFinish();
    }

    void HandleResolveKeys(TEvents::TEvPoison::TPtr&) {
        ShouldTerminate = true;
    }

private:
    void ResolveKeys() {
        FillKqpTasksGraphStages(TasksGraph, Transactions);

        auto requestNavigate = std::make_unique<NSchemeCache::TSchemeCacheNavigate>();
        auto request = MakeHolder<NSchemeCache::TSchemeCacheRequest>();
        request->ResultSet.reserve(TasksGraph.GetStagesInfo().size());
        if (UserToken && !UserToken->GetSerializedToken().empty()) {
            request->UserToken = UserToken;
        }

        for (auto& pair : TasksGraph.GetStagesInfo()) {
            auto& stageInfo = pair.second;

            if (!stageInfo.Meta.ShardOperations.empty()) {
                YQL_ENSURE(stageInfo.Meta.TableId);
                YQL_ENSURE(!stageInfo.Meta.ShardOperations.empty());

                for (const auto& operation : stageInfo.Meta.ShardOperations) {
                    const auto& tableInfo = stageInfo.Meta.TableConstInfo;
                    Y_ENSURE(tableInfo);
                    stageInfo.Meta.TableKind = tableInfo->TableKind;

                    stageInfo.Meta.ShardKey = ExtractKey(stageInfo.Meta.TableId, stageInfo.Meta.TableConstInfo, operation);

                    if (stageInfo.Meta.TableKind == ETableKind::Olap && TableRequestIds.find(stageInfo.Meta.TableId) == TableRequestIds.end()) {
                        TableRequestIds[stageInfo.Meta.TableId].emplace_back(pair.first);
                        auto& entry = requestNavigate->ResultSet.emplace_back();
                        entry.TableId = stageInfo.Meta.TableId;
                        entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByTableId;
                        entry.Operation = NSchemeCache::TSchemeCacheNavigate::EOp::OpTable;
                    }

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
        }
        if (requestNavigate->ResultSet.size()) {
            Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(requestNavigate.release()));
        } else {
            NavigationFinished = true;
        }
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvResolveKeySet(request));
    }

private:
    THolder<TKeyDesc> ExtractKey(const TTableId& table, const TIntrusiveConstPtr<TTableConstInfo>& tableInfo, TKeyDesc::ERowOperation operation) {
        auto range = GetFullRange(tableInfo->KeyColumnTypes.size());

        return MakeHolder<TKeyDesc>(table, range.ToTableRange(), operation, tableInfo->KeyColumnTypes,
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

    void TryFinish() {
        if (!NavigationFinished || !ResolvingFinished) {
            return;
        }
        auto replyEv = std::make_unique<TEvKqpExecuter::TEvTableResolveStatus>();
        replyEv->CpuTime = CpuTime;

        Send(Owner, replyEv.release());
        PassAway();
    }

private:
    const TActorId Owner;
    const ui64 TxId;
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    const TVector<IKqpGateway::TPhysicalTxData>& Transactions;
    THashMap<TTableId, TVector<TStageId>> TableRequestIds;
    bool NavigationFinished = false;
    bool ResolvingFinished = false;

    // TODO: TableResolver should not populate TasksGraph as it's not related to its job (bad API).
    TKqpTasksGraph& TasksGraph;

    bool ShouldTerminate = false;
    TMaybe<ui32> GotUnexpectedEvent;
    TDuration CpuTime;
};

} // anonymous namespace

NActors::IActor* CreateKqpTableResolver(const TActorId& owner, ui64 txId,
    const TIntrusiveConstPtr<NACLib::TUserToken>& userToken,
    const TVector<IKqpGateway::TPhysicalTxData>& transactions, TKqpTasksGraph& tasksGraph) {
    return new TKqpTableResolver(owner, txId, userToken, transactions, tasksGraph);
}

} // namespace NKikimr::NKqp
