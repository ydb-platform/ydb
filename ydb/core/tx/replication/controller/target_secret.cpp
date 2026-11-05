#include "target_secret.h"
#include "private_events.h"

#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NReplication::NController {

namespace {

class TSecretTopicRemover: public TActorBootstrapped<TSecretTopicRemover> {
    void Complete() {
        Send(Parent, new TEvPrivate::TEvDropStreamResult(ReplicationId, TargetId,
            NYdb::TStatus(NYdb::EStatus::SUCCESS, {})));
        PassAway();
    }

    void Retry() {
        Schedule(TDuration::Seconds(10), new TEvents::TEvWakeup());
    }

    void Handle(TEvYdbProxy::TEvDropTopicResponse::TPtr& ev) {
        const auto& status = ev->Get()->Result;
        if (status.IsSuccess()) {
            Complete();
        } else if (status.GetStatus() == NYdb::EStatus::SCHEME_ERROR) {
            Send(Proxy, new TEvYdbProxy::TEvDescribeTopicRequest(Path, NYdb::NTopic::TDescribeTopicSettings()));
        } else {
            Retry();
        }
    }

    void Handle(TEvYdbProxy::TEvDescribeTopicResponse::TPtr& ev) {
        if (ev->Get()->Result.GetStatus() == NYdb::EStatus::SCHEME_ERROR) {
            Complete();
        } else {
            Retry();
        }
    }

public:
    TSecretTopicRemover(const TActorId& parent, const TActorId& proxy, ui64 rid, ui64 tid, const TString& path)
        : Parent(parent)
        , Proxy(proxy)
        , ReplicationId(rid)
        , TargetId(tid)
        , Path(path)
    {}

    void Bootstrap() {
        Send(Proxy, new TEvYdbProxy::TEvDropTopicRequest(Path, NYdb::NTopic::TDropTopicSettings()));
        Become(&TThis::StateWork);
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvYdbProxy::TEvDropTopicResponse, Handle);
            hFunc(TEvYdbProxy::TEvDescribeTopicResponse, Handle);
            sFunc(TEvents::TEvWakeup, Bootstrap);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    const TActorId Parent;
    const TActorId Proxy;
    const ui64 ReplicationId;
    const ui64 TargetId;
    const TString Path;
};

}

void TTargetSecret::Progress(const TActorContext& ctx) {
    auto* replication = GetReplication();
    if (replication->GetState() != TReplication::EState::Removing) {
        return;
    }
    if (GetStreamState() == TReplication::EStreamState::Removing) {
        if (!TopicRemover && replication->GetYdbProxy()) {
            TopicRemover = ctx.Register(new TSecretTopicRemover(ctx.SelfID, replication->GetYdbProxy(),
                replication->GetId(), GetId(), GetStreamName()));
        }
    } else if (GetDstState() == TReplication::EDstState::Removing) {
        TTargetBase::Progress(ctx);
    }
}

void TTargetSecret::Shutdown(const TActorContext& ctx) {
    Reset(TopicRemover);
    TTargetBase::Shutdown(ctx);
}

}
