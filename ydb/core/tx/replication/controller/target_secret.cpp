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
        Schedule(TDuration::Seconds(10), new TEvents::TEvWakeup(ProxyGeneration));
    }

    void Handle(TEvYdbProxy::TEvDropTopicResponse::TPtr& ev) {
        if (ev->Cookie != ProxyGeneration) {
            return;
        }
        const auto& status = ev->Get()->Result;
        if (status.IsSuccess()) {
            Complete();
        } else if (status.GetStatus() == NYdb::EStatus::SCHEME_ERROR) {
            Send(Proxy, new TEvYdbProxy::TEvDescribeTopicRequest(Path, NYdb::NTopic::TDescribeTopicSettings()), 0, ProxyGeneration);
        } else {
            Retry();
        }
    }

    void Handle(TEvYdbProxy::TEvDescribeTopicResponse::TPtr& ev) {
        if (ev->Cookie != ProxyGeneration) {
            return;
        }
        if (ev->Get()->Result.GetStatus() == NYdb::EStatus::SCHEME_ERROR) {
            Complete();
        } else {
            Retry();
        }
    }

    void Handle(TEvPrivate::TEvUpdateYdbProxy::TPtr& ev) {
        if (Proxy != ev->Get()->YdbProxy) {
            Proxy = ev->Get()->YdbProxy;
            ++ProxyGeneration;
            Bootstrap();
        }
    }

    void Handle(TEvents::TEvWakeup::TPtr& ev) {
        if (ev->Get()->Tag == ProxyGeneration) {
            Bootstrap();
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
        Send(Proxy, new TEvYdbProxy::TEvDropTopicRequest(Path, NYdb::NTopic::TDropTopicSettings()), 0, ProxyGeneration);
        Become(&TThis::StateWork);
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvYdbProxy::TEvDropTopicResponse, Handle);
            hFunc(TEvYdbProxy::TEvDescribeTopicResponse, Handle);
            hFunc(TEvPrivate::TEvUpdateYdbProxy, Handle);
            hFunc(TEvents::TEvWakeup, Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    const TActorId Parent;
    TActorId Proxy;
    ui64 ProxyGeneration = 0;
    const ui64 ReplicationId;
    const ui64 TargetId;
    const TString Path;
};

}

void TTargetSecret::ProgressDestination(const TActorContext& ctx) {
    // TSecretCopier creates the destination during initial copy. Only its
    // removal belongs to the common destination lifecycle.
    if (GetReplication()->GetState() == TReplication::EState::Removing
        && GetDstState() == TReplication::EDstState::Removing)
    {
        TTargetBase::Progress(ctx);
    }
}

IActor* TTargetSecret::CreateStreamRemoverActor(const TActorContext& ctx) const {
    auto* replication = GetReplication();
    return new TSecretTopicRemover(ctx.SelfID, replication->GetYdbProxy(),
        replication->GetId(), GetId(), GetStreamName());
}

}
