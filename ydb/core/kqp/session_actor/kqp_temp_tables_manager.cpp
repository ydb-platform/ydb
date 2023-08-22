#include "kqp_session_actor.h"

#include <ydb/core/base/path.h>
#include <ydb/core/kqp/gateway/actors/scheme.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/kqp/session_actor/kqp_worker_common.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/event_pb.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/log.h>
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

class TKqpTempTablesManager : public TActorBootstrapped<TKqpTempTablesManager> {
    struct TEvPrivate {
        enum EEv {
            EvResult = EventSpaceBegin(TEvents::ES_PRIVATE),
        };

        struct TEvResult : public TEventLocal<TEvResult, EEv::EvResult> {
            IKqpGateway::TGenericResult Result;
        };
    };
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_SESSION_ACTOR;
    }

    TKqpTempTablesManager(TKqpTempTablesState tempTablesState, const TActorId& target)
        : TempTablesState(std::move(tempTablesState))
        , Target(target)
    {}

    void Bootstrap() {
        using TRequest = TEvTxUserProxy::TEvProposeTransaction;

        for (const auto& [path, info] : TempTablesState.TempTables) {
            auto ev = MakeHolder<TRequest>();
            auto& record = ev->Record;

            record.SetDatabaseName(info.Database);
            if (info.UserToken) {
                record.SetUserToken(info.UserToken->GetSerializedToken());
            }

            auto* modifyScheme = record.MutableTransaction()->MutableModifyScheme();
            modifyScheme->SetWorkingDir(info.WorkingDir);
            modifyScheme->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpDropTable);
            auto* drop = modifyScheme->MutableDrop();
            if (TempTablesState.SessionId) {
                drop->SetName(info.Name + *TempTablesState.SessionId);
            }

            auto promise = NewPromise<IKqpGateway::TGenericResult>();
            IActor* requestHandler = new TSchemeOpRequestHandler(ev.Release(), promise, true);
            RegisterWithSameMailbox(requestHandler);

            auto actorSystem = TlsActivationContext->AsActorContext().ExecutorThread.ActorSystem;
            auto selfId = SelfId();
            promise.GetFuture().Subscribe([actorSystem, selfId](const TFuture<IKqpGateway::TGenericResult>& future) {
                auto ev = MakeHolder<TEvPrivate::TEvResult>();
                ev->Result = future.GetValue();
                actorSystem->Send(selfId, ev.Release());
            });
        }

        if (TempTablesState.TempTables.empty()) {
            Send(Target, new TEvents::TEvGone());
            PassAway();
            return;
        }

        Become(&TKqpTempTablesManager::WaitState);
    }

public:
    STATEFN(WaitState) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvResult, HandleResult);
            default:
                Y_FAIL("Unexpected event 0x%x at TKqpTempTablesManager::WaitState", ev->GetTypeRewrite());
        }
    }

    void HandleResult(TEvPrivate::TEvResult::TPtr&) {
        ResultsCount++;

        if (ResultsCount == TempTablesState.TempTables.size()) {
            Send(Target, new TEvents::TEvGone());
            PassAway();
        }
    }

private:
    TKqpTempTablesState TempTablesState;
    const TActorId Target;
    ui32 ResultsCount = 0;
};

} // namespace

IActor* CreateKqpTempTablesManager(TKqpTempTablesState tempTablesState, const TActorId& target)
{
    return new TKqpTempTablesManager(tempTablesState, target);
}

} // namespace NKikimr::NKqp
