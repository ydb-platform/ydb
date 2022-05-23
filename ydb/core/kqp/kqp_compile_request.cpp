#include "kqp_impl.h"

#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/log.h>

#include <util/string/escape.h>

namespace NKikimr {
namespace NKqp {

using namespace NSchemeCache;
using namespace NYql;

class TKqpCompileRequestActor : public TActorBootstrapped<TKqpCompileRequestActor> {
public:
    using TBase = TActorBootstrapped<TKqpCompileRequestActor>;

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_COMPILE_REQUEST;
    }

    TKqpCompileRequestActor(const TActorId& owner, const TString& userToken, const TMaybe<TString>& uid,
        TMaybe<TKqpQueryId>&& query, bool keepInCache, const TInstant& deadline, TKqpDbCountersPtr dbCounters)
        : Owner(owner)
        , UserToken(userToken)
        , Uid(uid)
        , Query(std::move(query))
        , KeepInCache(keepInCache)
        , Deadline(deadline)
        , DbCounters(dbCounters) {}

    void Bootstrap(const TActorContext& ctx) {
        TimeoutTimerId = CreateLongTimer(ctx, Deadline - TInstant::Now(),
            new IEventHandle(ctx.SelfID, ctx.SelfID, new TEvents::TEvWakeup()));

        TMaybe<TKqpQueryId> query;
        std::swap(Query, query);

        auto compileEv = MakeHolder<TEvKqp::TEvCompileRequest>(UserToken, Uid, std::move(query),
            KeepInCache, Deadline, DbCounters);
        ctx.Send(MakeKqpCompileServiceID(ctx.SelfID.NodeId()), compileEv.Release());

        Become(&TKqpCompileRequestActor::MainState);
    }

    void Handle(TEvKqp::TEvCompileResponse::TPtr& ev, const TActorContext &ctx) {
        auto compileResult = ev->Get()->CompileResult;
        const auto& stats = ev->Get()->Stats;

        if (compileResult->Status != Ydb::StatusIds::SUCCESS || !stats.GetFromCache()) {
            ctx.Send(Owner, ev->Release().Release());
            Die(ctx);
            return;
        }

        if (!NavigateTables(*compileResult->PreparedQuery, compileResult->Query->Database, ctx)) {
            ctx.Send(Owner, ev->Release().Release());
            Die(ctx);
            return;
        }

        DeferredResponse.Reset(ev->Release().Release());
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext &ctx) {
        if (ValidateTables(*ev->Get(), ctx)) {
            ctx.Send(Owner, DeferredResponse.Release());
            Die(ctx);
            return;
        }

        auto& compileResult = *DeferredResponse->CompileResult;

        LOG_INFO_S(ctx, NKikimrServices::KQP_COMPILE_REQUEST, "Recompiling query due to scheme error"
            << ", self: " << ctx.SelfID
            << ", queryUid: " << compileResult.Uid);

        auto recompileEv = MakeHolder<TEvKqp::TEvRecompileRequest>(UserToken, compileResult.Uid, compileResult.Query,
            Deadline, DbCounters);
        ctx.Send(MakeKqpCompileServiceID(ctx.SelfID.NodeId()), recompileEv.Release());

        DeferredResponse.Reset();
    }

    void HandleTimeout(const TActorContext& ctx) {
        LOG_NOTICE_S(ctx, NKikimrServices::KQP_COMPILE_REQUEST, "Compile request deadline exceeded"
            << ", self: " << ctx.SelfID);

        NYql::TIssue issue(NYql::TPosition(), "Deadline exceeded during query compilation.");
        return ReplyError(Ydb::StatusIds::TIMEOUT, {issue}, ctx);
    }

    void Die(const NActors::TActorContext& ctx) override {
        if (TimeoutTimerId) {
            ctx.Send(TimeoutTimerId, new TEvents::TEvPoisonPill());
        }

        TBase::Die(ctx);
    }

private:
    STFUNC(MainState) {
        try {
            switch (ev->GetTypeRewrite()) {
                HFunc(TEvKqp::TEvCompileResponse, Handle);
                HFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
                CFunc(TEvents::TSystem::Wakeup, HandleTimeout);
            default:
                UnexpectedEvent("MainState", ev->GetTypeRewrite(), ctx);
            }
        } catch (const yexception& e) {
            InternalError(e.what(), ctx);
        }
    }

private:
    void FillTables(const NKikimrKqp::TPreparedKql& kql) {
        for (const auto& tableInfo : kql.GetTableInfo()) {
            TTableId tableId(tableInfo.GetTableId().GetOwnerId(), tableInfo.GetTableId().GetTableId());
            auto it = TableVersions.find(tableId);
            if (it != TableVersions.end()) {
                Y_ENSURE(it->second == tableInfo.GetSchemaVersion());
            } else {
                TableVersions.emplace(tableId, tableInfo.GetSchemaVersion());
            }
        }
    }

    void FillTables(const NKqpProto::TKqpPhyTx& phyTx) {
        for (const auto& stage : phyTx.GetStages()) {
            for (const auto& tableOp : stage.GetTableOps()) {
                TTableId tableId(tableOp.GetTable().GetOwnerId(), tableOp.GetTable().GetTableId());
                auto it = TableVersions.find(tableId);
                if (it != TableVersions.end()) {
                    Y_ENSURE(it->second == tableOp.GetTable().GetVersion());
                } else {
                    TableVersions.emplace(tableId, tableOp.GetTable().GetVersion());
                }
            }
        }
    }

    bool NavigateTables(const NKikimrKqp::TPreparedQuery& query, const TString& database, const TActorContext& ctx) {
        TableVersions.clear();

        switch (query.GetVersion()) {
            case NKikimrKqp::TPreparedQuery::VERSION_V1:
                for (const auto& kql : query.GetKqls()) {
                    FillTables(kql);
                }
                break;

            case NKikimrKqp::TPreparedQuery::VERSION_PHYSICAL_V1:
                for (const auto& tx : query.GetPhysicalQuery().GetTransactions()) {
                    FillTables(tx);
                }
                break;

            default:
                LOG_ERROR_S(ctx, NKikimrServices::KQP_COMPILE_REQUEST,
                    "Unexpected prepared query version"
                    << ", self: " << ctx.SelfID
                    << ", version: " << (ui32)query.GetVersion());
                return false;
        }

        if (TableVersions.empty()) {
            return false;
        }

        auto navigate = MakeHolder<TSchemeCacheNavigate>();
        navigate->DatabaseName = database;
        if (!UserToken.empty()) {
            navigate->UserToken = new NACLib::TUserToken(UserToken);
        }

        for (const auto& [tableId, _] : TableVersions) {
            TSchemeCacheNavigate::TEntry entry;
            entry.TableId = tableId;
            entry.RequestType = TSchemeCacheNavigate::TEntry::ERequestType::ByTableId;
            entry.Operation = TSchemeCacheNavigate::EOp::OpTable;
            entry.SyncVersion = false;
            entry.ShowPrivatePath = true;

            LOG_DEBUG_S(ctx, NKikimrServices::KQP_COMPILE_REQUEST, "Query has dependency on table, check the table schema version"
                << ", self: " << ctx.SelfID
                << ", pathId: " << entry.TableId.PathId
                << ", version: " << entry.TableId.SchemaVersion);

            navigate->ResultSet.emplace_back(entry);
        }

        auto ev = MakeHolder<TEvTxProxySchemeCache::TEvNavigateKeySet>(navigate.Release());
        ctx.Send(MakeSchemeCacheID(), ev.Release());
        return true;
    }

    bool ValidateTables(const TEvTxProxySchemeCache::TEvNavigateKeySetResult& response, const TActorContext& ctx) {
        Y_ENSURE(response.Request);
        const auto& navigate = *response.Request;

        for (const auto& entry : navigate.ResultSet) {
            switch (entry.Status) {
                case TSchemeCacheNavigate::EStatus::Ok: {
                    auto expectedVersion = TableVersions.FindPtr(TTableId(entry.TableId.PathId));
                    if (!expectedVersion) {
                        LOG_WARN_S(ctx, NKikimrServices::KQP_COMPILE_REQUEST,
                            "Unexpected tableId in scheme cache navigate reply"
                            << ", self: " << ctx.SelfID
                            << ", tableId: " << entry.TableId);
                        continue;
                    }

                    if (!*expectedVersion) {
                        // Do not check tables with zero version.
                        continue;
                    }

                    if (entry.TableId.SchemaVersion && entry.TableId.SchemaVersion != *expectedVersion) {
                        LOG_INFO_S(ctx, NKikimrServices::KQP_COMPILE_REQUEST, "Scheme version mismatch"
                            << ", self: " << ctx.SelfID
                            << ", pathId: " << entry.TableId.PathId
                            << ", expected version: " << *expectedVersion
                            << ", actual version: " << entry.TableId.SchemaVersion);
                        return false;
                    }

                    break;
                }

                case TSchemeCacheNavigate::EStatus::PathErrorUnknown:
                case TSchemeCacheNavigate::EStatus::PathNotTable:
                case TSchemeCacheNavigate::EStatus::TableCreationNotComplete:
                    LOG_INFO_S(ctx, NKikimrServices::KQP_COMPILE_REQUEST, "Scheme error"
                        << ", self: " << ctx.SelfID
                        << ", pathId: " << entry.TableId.PathId
                        << ", status: " << entry.Status);
                    return false;

                case TSchemeCacheNavigate::EStatus::LookupError:
                case TSchemeCacheNavigate::EStatus::RedirectLookupError:
                    // Transient error, do not invalidate the query.
                    // Hard validation will be performed later during the query execution.
                    break;

                default:
                    // Unexpected reply, do not invalidate the query as it may block the query execution.
                    // Hard validation will be performed later during the query execution.
                    LOG_ERROR_S(ctx, NKikimrServices::KQP_COMPILE_REQUEST, "Unexpected reply from scheme cache"
                        << ", self: " << ctx.SelfID
                        << ", pathId: " << entry.TableId.PathId
                        << ", status: " << entry.Status);
                    break;
            }
        }

        return true;
    }

private:
    void UnexpectedEvent(const TString& state, ui32 eventType, const TActorContext &ctx) {
        InternalError(TStringBuilder() << "TKqpCompileRequestActor, unexpected event: " << eventType
            << ", at state:" << state, ctx);
    }

    void InternalError(const TString& message, const TActorContext &ctx) {
        LOG_ERROR_S(ctx, NKikimrServices::KQP_COMPILE_REQUEST, "Internal error"
            << ", self: " << ctx.SelfID
            << ", message: " << message);


        NYql::TIssue issue(NYql::TPosition(), "Internal error while proccessing query compilation request.");
        issue.AddSubIssue(MakeIntrusive<TIssue>(NYql::TPosition(), message));

        ReplyError(Ydb::StatusIds::INTERNAL_ERROR, {issue}, ctx);
    }

    void ReplyError(Ydb::StatusIds::StatusCode status, const TIssues& issues, const TActorContext& ctx) {
        auto responseEv = MakeHolder<TEvKqp::TEvCompileResponse>(TKqpCompileResult::Make({}, status, issues));
        ctx.Send(Owner, responseEv.Release());
        Die(ctx);
    }

private:
    TActorId Owner;
    TString UserToken;
    TMaybe<TString> Uid;
    TMaybe<TKqpQueryId> Query;
    bool KeepInCache = false;
    TInstant Deadline;
    TKqpDbCountersPtr DbCounters;
    TActorId TimeoutTimerId;
    THashMap<TTableId, ui64> TableVersions;
    THolder<TEvKqp::TEvCompileResponse> DeferredResponse;
};


IActor* CreateKqpCompileRequestActor(const TActorId& owner, const TString& userToken, const TMaybe<TString>& uid,
    TMaybe<TKqpQueryId>&& query, bool keepInCache, const TInstant& deadline, TKqpDbCountersPtr dbCounters)
{
    return new TKqpCompileRequestActor(owner, userToken, uid, std::move(query), keepInCache, deadline, dbCounters);
}

} // namespace NKqp
} // namespace NKikimr
