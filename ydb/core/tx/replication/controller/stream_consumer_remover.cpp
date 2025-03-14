#include "logging.h"
#include "private_events.h"
#include "stream_consumer_remover.h"
#include "util.h"

#include <ydb/core/protos/replication.pb.h>
#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>

namespace NKikimr::NReplication::NController {

class TStreamConsumerRemover: public TActorBootstrapped<TStreamConsumerRemover> {
    void RequestPermission() {
        Send(Parent, new TEvPrivate::TEvRequestDropStream());
        Become(&TThis::StateRequestPermission);
    }

    STATEFN(StateRequestPermission) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvAllowDropStream, Handle);
        default:
            return StateBase(ev);
        }
    }

    void Handle(TEvPrivate::TEvAllowDropStream::TPtr& ev) {
        LOG_T("Handle " << ev->Get()->ToString());
        DropStreamConsumer();
    }

    void DropStreamConsumer() {
        Send(YdbProxy, new TEvYdbProxy::TEvAlterTopicRequest(SrcPath, NYdb::NTopic::TAlterTopicSettings()
            .AppendDropConsumers(ConsumerName)));

        Become(&TThis::StateWork);
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvYdbProxy::TEvAlterTopicResponse, Handle);
            sFunc(TEvents::TEvWakeup, DropStreamConsumer);
        default:
            return StateBase(ev);
        }
    }

    void Handle(TEvYdbProxy::TEvAlterTopicResponse::TPtr& ev) {
        LOG_T("Handle " << ev->Get()->ToString());
        auto& result = ev->Get()->Result;

        if (!result.IsSuccess()) {
            if (IsRetryableError(result)) {
                LOG_D("Retry");
                return Schedule(TDuration::Seconds(10), new TEvents::TEvWakeup);
            }

            LOG_E("Error"
                << ": status# " << result.GetStatus()
                << ", issues# " << result.GetIssues().ToOneLineString());
        } else {
            LOG_I("Success"
                << ": issues# " << result.GetIssues().ToOneLineString());
        }

        Send(Parent, new TEvPrivate::TEvDropStreamResult(ReplicationId, TargetId, std::move(result)));
        PassAway();
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::REPLICATION_CONTROLLER_STREAM_REMOVER;
    }

    explicit TStreamConsumerRemover(
            const TActorId& parent,
            const TActorId& proxy,
            ui64 rid,
            ui64 tid,
            TReplication::ETargetKind kind,
            const TString& srcPath,
            const TString& consumerName,
            const bool needDrop)
        : Parent(parent)
        , YdbProxy(proxy)
        , ReplicationId(rid)
        , TargetId(tid)
        , Kind(kind)
        , SrcPath(srcPath)
        , ConsumerName(consumerName)
        , NeedDrop(needDrop)
        , LogPrefix("StreamConsumerRemover", ReplicationId, TargetId)
    {
    }

    void Bootstrap() {
        switch (Kind) {
        case TReplication::ETargetKind::Table:
        case TReplication::ETargetKind::IndexTable:
            Y_ABORT("Unreachable");
        case TReplication::ETargetKind::Transfer:
            if (NeedDrop) {
                return RequestPermission();
            } else {
                Send(Parent, new TEvPrivate::TEvDropStreamResult(ReplicationId, TargetId, NYdb::TStatus(NYdb::EStatus::SUCCESS, {})));
                PassAway();
            }
        }
    }

    STATEFN(StateBase) {
        switch (ev->GetTypeRewrite()) {
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    const TActorId Parent;
    const TActorId YdbProxy;
    const ui64 ReplicationId;
    const ui64 TargetId;
    const TReplication::ETargetKind Kind;
    const TString SrcPath;
    const TString ConsumerName;
    const bool NeedDrop;
    const TActorLogPrefix LogPrefix;

}; // TStreamRemover

IActor* CreateStreamConsumerRemover(TReplication* replication, ui64 targetId, const TActorContext& ctx) {
    const auto* target = replication->FindTarget(targetId);
    Y_ABORT_UNLESS(target);
    auto needDrop = !replication->GetConfig().GetTransferSpecific().GetTargets(0).HasConsumerName();

    return CreateStreamConsumerRemover(ctx.SelfID, replication->GetYdbProxy(),
        replication->GetId(), target->GetId(), target->GetKind(), target->GetSrcPath(), target->GetStreamConsumerName(), needDrop);
}

IActor* CreateStreamConsumerRemover(const TActorId& parent, const TActorId& proxy, ui64 rid, ui64 tid,
        TReplication::ETargetKind kind, const TString& srcPath, const TString& consumerName, bool needDrop)
{
    return new TStreamConsumerRemover(parent, proxy, rid, tid, kind, srcPath, consumerName, needDrop);
}

}
