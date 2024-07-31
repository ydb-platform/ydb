#include "kqp_session_actor.h"

#include <ydb/core/base/path.h>
#include <ydb/core/kqp/gateway/actors/scheme.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/kqp/session_actor/kqp_worker_common.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/event_pb.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/yql/utils/actor_log/log.h>

namespace NKikimr::NKqp {

#define LOG_C(stream) LOG_CRIT_S(*TlsActivationContext, NKikimrServices::KQP_SESSION, stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_SESSION, stream)
#define LOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::KQP_SESSION, stream)
#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::KQP_SESSION, stream)
#define LOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::KQP_SESSION, stream)
#define LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::KQP_SESSION, stream)

using namespace NThreading;

namespace {

using TPath = TVector<TString>;

class TKqpTempTablesManager : public TActorBootstrapped<TKqpTempTablesManager> {
    struct TEvPrivate {
        enum EEv {
            EvDropTableResult = EventSpaceBegin(TEvents::ES_PRIVATE),
            EvRemoveDirResult
        };

        struct TEvDropTableResult : public TEventLocal<TEvDropTableResult, EEv::EvDropTableResult> {
            IKqpGateway::TGenericResult Result;
        };

        struct TEvRemoveDirResult : public TEventLocal<TEvRemoveDirResult, EEv::EvRemoveDirResult> {
            IKqpGateway::TGenericResult Result;
        };
    };
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_SESSION_ACTOR;
    }

    TKqpTempTablesManager(
            TKqpTempTablesState tempTablesState,
            TIntrusiveConstPtr<NACLib::TUserToken> userToken,
            const TActorId& target,
            const TString& database)
        : TempTablesState(std::move(tempTablesState))
        , UserToken(std::move(userToken))
        , Target(target)
        , Database(database)
    {}

    void Bootstrap() {
        if (TempTablesState.TempTables.empty()) {
            Finish();
            return;
        }

        PathsToTraverse.push_back(
            NKikimr::SplitPath(GetSessionDirPath(Database, TempTablesState.SessionId)));
        TraverseNext();
        Become(&TKqpTempTablesManager::PathSearchState);
    }


    STATEFN(PathSearchState) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleNavigate);
            default:
                Y_ABORT("Unexpected event 0x%x at TKqpTempTablesManager::PathSearchState", ev->GetTypeRewrite());
        }
    }

    STATEFN(DropTablesState) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvDropTableResult, HandleDropTable);
            default:
                Y_ABORT("Unexpected event 0x%x at TKqpTempTablesManager::DropTablesState", ev->GetTypeRewrite());
        }
    }

    STATEFN(RmDirsState) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvRemoveDirResult, HandleRemoveDir);
            default:
                Y_ABORT("Unexpected event 0x%x at TKqpTempTablesManager::RmDirsState", ev->GetTypeRewrite());
        }
    }

private:
    void TraverseNext() {
        auto schemeCacheRequest = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();

        schemeCacheRequest->UserToken = UserToken;
        schemeCacheRequest->ResultSet.resize(PathsToTraverse.size());

        for (size_t i = 0; i < PathsToTraverse.size(); ++i) {
            DirsToDrop.push_back(PathsToTraverse[i]);
            schemeCacheRequest->ResultSet[i].Path = PathsToTraverse[i];
            schemeCacheRequest->ResultSet[i].Operation = NSchemeCache::TSchemeCacheNavigate::OpList;
            schemeCacheRequest->ResultSet[i].SyncVersion = false;
            schemeCacheRequest->ResultSet[i].ShowPrivatePath = true;
        }

        PathsToTraverse.clear();

        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(schemeCacheRequest.Release()));
    }

    void HandleNavigate(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const NSchemeCache::TSchemeCacheNavigate* navigate = ev->Get()->Request.Get();
        if (navigate->ErrorCount != 0) {
            Finish();
        }

        for (const auto& entry : navigate->ResultSet) {
            if (entry.ListNodeEntry) {
                for (const auto& child : entry.ListNodeEntry->Children) {
                    if (child.Kind == NSchemeCache::TSchemeCacheNavigate::KindPath) {
                        PathsToTraverse.push_back(entry.Path);
                        PathsToTraverse.back().push_back(child.Name);
                    } else if (child.Kind == NSchemeCache::TSchemeCacheNavigate::KindTable) {
                        TablesToDrop.push_back(entry.Path);
                        TablesToDrop.back().push_back(child.Name);
                    }
                }
            }
        }

        if (!PathsToTraverse.empty()) {
            TraverseNext();
        } else {
            DropTables();
        }
    }

    void DropTables() {
        if (TablesToDrop.empty()) {
            RemoveDirs();
            return;
        }

        for (const auto& path : TablesToDrop) {
            auto ev = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
            auto& record = ev->Record;

            record.SetDatabaseName(Database);
            if (UserToken) {
                record.SetUserToken(UserToken->GetSerializedToken());
            }

            auto* modifyScheme = record.MutableTransaction()->MutableModifyScheme();
            modifyScheme->SetWorkingDir(NKikimr::JoinPath(TVector<TString>(std::begin(path), std::prev(std::end(path)))));
            modifyScheme->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpDropTable);
            auto* drop = modifyScheme->MutableDrop();
            drop->SetName(path.back());

            auto promise = NewPromise<IKqpGateway::TGenericResult>();
            IActor* requestHandler = new TSchemeOpRequestHandler(ev.Release(), promise, true);
            RegisterWithSameMailbox(requestHandler);

            auto actorSystem = TlsActivationContext->AsActorContext().ExecutorThread.ActorSystem;
            auto selfId = SelfId();
            promise.GetFuture().Subscribe([actorSystem, selfId](const TFuture<IKqpGateway::TGenericResult>& future) {
                auto ev = MakeHolder<TEvPrivate::TEvDropTableResult>();
                ev->Result = future.GetValue();
                actorSystem->Send(selfId, ev.Release());
            });
        }

        Become(&TKqpTempTablesManager::DropTablesState);
    }

    void HandleDropTable(TEvPrivate::TEvDropTableResult::TPtr&) {
        if (++DroppedTablesCount == TablesToDrop.size()) {
            RemoveDirs();
        }
    }

    void RemoveDirs() {
        RemoveNextDir();
        Become(&TKqpTempTablesManager::RmDirsState);
    }

    void RemoveNextDir() {
        if (DirsToDrop.empty()) {
            Finish();
            return;
        }

        auto dirToDrop = DirsToDrop.back();
        DirsToDrop.pop_back();

        auto ev = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
        auto& record = ev->Record;

        record.SetDatabaseName(Database);
        if (UserToken) {
            record.SetUserToken(UserToken->GetSerializedToken());
        }

        auto* modifyScheme = record.MutableTransaction()->MutableModifyScheme();
        modifyScheme->SetWorkingDir(NKikimr::JoinPath(TVector<TString>(std::begin(dirToDrop), std::prev(std::end(dirToDrop)))));
        modifyScheme->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpRmDir);
        auto* drop = modifyScheme->MutableDrop();
        drop->SetName(dirToDrop.back());

        auto promise = NewPromise<IKqpGateway::TGenericResult>();
        IActor* requestHandler = new TSchemeOpRequestHandler(ev.Release(), promise, true);
        RegisterWithSameMailbox(requestHandler);

        auto actorSystem = TlsActivationContext->AsActorContext().ExecutorThread.ActorSystem;
        auto selfId = SelfId();
        promise.GetFuture().Subscribe([actorSystem, selfId](const TFuture<IKqpGateway::TGenericResult>& future) {
            auto ev = MakeHolder<TEvPrivate::TEvRemoveDirResult>();
            ev->Result = future.GetValue();
            actorSystem->Send(selfId, ev.Release());
        });
    }

    void HandleRemoveDir(TEvPrivate::TEvRemoveDirResult::TPtr& result) {
        if (!result->Get()->Result.Success()) {
            Finish();
        }

        RemoveNextDir();
    }

    void Finish() {
        Send(Target, new TEvents::TEvGone());
        PassAway();
    }

    TKqpTempTablesState TempTablesState;
    TVector<TPath> PathsToTraverse;

    TVector<TPath> TablesToDrop;
    size_t DroppedTablesCount = 0;

    TVector<TPath> DirsToDrop;

    const TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    const TActorId Target;
    const TString Database;
};

} // namespace

IActor* CreateKqpTempTablesManager(
        TKqpTempTablesState tempTablesState,
        TIntrusiveConstPtr<NACLib::TUserToken> userToken,
        const TActorId& target,
        const TString& database)
{
    return new TKqpTempTablesManager(tempTablesState, std::move(userToken), target, database);
}

} // namespace NKikimr::NKqp
