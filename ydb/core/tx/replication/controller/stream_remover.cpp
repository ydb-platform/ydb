#include "logging.h"
#include "private_events.h"
#include "stream_remover.h"
#include "util.h"

#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::REPLICATION_CONTROLLER

namespace NKikimr::NReplication::NController {

class TStreamRemover: public TActorBootstrapped<TStreamRemover> {
    void RequestPermission() {
        Send(Parent, new TEvPrivate::TEvRequestDropStream());
        Become(&TThis::StateRequestPermission);
    }

    STATEFN(StateRequestPermission) {
        YDB_LOG_CREATE_CONTEXT(LogPrefix,
            {"actorState", "StateRequestPermission"});
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvAllowDropStream, Handle);
        default:
            return StateBase(ev);
        }
    }

    void Handle(TEvPrivate::TEvAllowDropStream::TPtr& ev) {
        YDB_LOG_TRACE("Handle",
            {"ev", ev->Get()->ToString()});
        DropStream();
    }

    void DropStream() {
        switch (Kind) {
        case TReplication::ETargetKind::Table:
        case TReplication::ETargetKind::IndexTable:
            Send(YdbProxy, new TEvYdbProxy::TEvAlterTableRequest(SrcPath, NYdb::NTable::TAlterTableSettings()
                .AppendDropChangefeeds(StreamName)),
                0,
                ProxyGeneration);
            break;
        case TReplication::ETargetKind::Topic:
            Send(YdbProxy, new TEvYdbProxy::TEvAlterTopicRequest(SrcPath, NYdb::NTopic::TAlterTopicSettings()
                .AppendDropConsumers(StreamName)),
                0,
                ProxyGeneration);
            break;
        case TReplication::ETargetKind::Transfer:
            Y_ABORT("Unreachable");
        }

        Become(&TThis::StateWork);
    }

    STATEFN(StateWork) {
        YDB_LOG_CREATE_CONTEXT(LogPrefix,
            {"actorState", "StateWork"});
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvYdbProxy::TEvAlterTableResponse, HandleResult);
            hFunc(TEvYdbProxy::TEvAlterTopicResponse, HandleResult);
            hFunc(TEvents::TEvWakeup, Handle);
        default:
            return StateBase(ev);
        }
    }

    template <typename TEvent>
    void HandleResult(TEvent& ev) {
        if (ev->Cookie != ProxyGeneration) {
            return;
        }
        YDB_LOG_TRACE("Handle",
            {"ev", ev->Get()->ToString()});
        auto& result = ev->Get()->Result;
        if (Kind == TReplication::ETargetKind::Topic && result.GetStatus() == NYdb::EStatus::NOT_FOUND) {
            result = NYdb::TStatus(NYdb::EStatus::SUCCESS, {});
        }

        if (!result.IsSuccess()) {
            if (IsRetryableError(result)) {
                YDB_LOG_DEBUG("Retry");
                return Schedule(RetryInterval, new TEvents::TEvWakeup(ProxyGeneration));
            }

            YDB_LOG_ERROR("Error",
                {"status", result.GetStatus()},
                {"issues", result.GetIssues().ToOneLineString()});

            // Keep cleanup pending: the important consumer still retains source data.
            if (Kind == TReplication::ETargetKind::Topic) {
                return Schedule(RetryInterval, new TEvents::TEvWakeup(ProxyGeneration));
            }
        } else {
            YDB_LOG_INFO("Success",
                {"issues", result.GetIssues().ToOneLineString()});
        }

        Send(Parent, new TEvPrivate::TEvDropStreamResult(ReplicationId, TargetId, std::move(result)));
        PassAway();
    }

    void Handle(TEvents::TEvWakeup::TPtr& ev) {
        if (ev->Get()->Tag == ProxyGeneration) {
            DropStream();
        }
    }

    void Handle(TEvPrivate::TEvUpdateYdbProxy::TPtr& ev) {
        if (YdbProxy != ev->Get()->YdbProxy) {
            YdbProxy = ev->Get()->YdbProxy;
            ++ProxyGeneration;
            if (CurrentStateFunc() == &TThis::StateWork) {
                DropStream();
            }
        }
    }

    void Cancel() {
        Send(Parent, new TEvPrivate::TEvCancelDropStream());
        PassAway();
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::REPLICATION_CONTROLLER_STREAM_REMOVER;
    }

    explicit TStreamRemover(
            const TActorId& parent,
            const TActorId& proxy,
            ui64 rid,
            ui64 tid,
            TReplication::ETargetKind kind,
            const TString& srcPath,
            const TString& streamName)
        : Parent(parent)
        , YdbProxy(proxy)
        , ReplicationId(rid)
        , TargetId(tid)
        , Kind(kind)
        , SrcPath(srcPath)
        , StreamName(streamName)
        , LogPrefix(CreateActorLogPrefix("StreamRemover", ReplicationId, TargetId))
    {
    }

    void Bootstrap() {
        YDB_LOG_CREATE_CONTEXT(LogPrefix);
        switch (Kind) {
        case TReplication::ETargetKind::Table:
        case TReplication::ETargetKind::IndexTable:
        case TReplication::ETargetKind::Topic:
            return RequestPermission();
        case TReplication::ETargetKind::Transfer:
            Y_ABORT("Unreachable");
        }
    }

    STATEFN(StateBase) {
        YDB_LOG_CREATE_CONTEXT(LogPrefix,
            {"actorState", "StateBase"});
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvUpdateYdbProxy, Handle);
            sFunc(TEvents::TEvPoison, Cancel);
        }
    }

private:
    const TActorId Parent;
    TActorId YdbProxy;
    ui64 ProxyGeneration = 0;
    const ui64 ReplicationId;
    const ui64 TargetId;
    const TReplication::ETargetKind Kind;
    const TString SrcPath;
    const TString StreamName;
    const NActors::NStructuredLog::TStructuredMessage LogPrefix;

    static constexpr auto RetryInterval = TDuration::Seconds(10);

}; // TStreamRemover

IActor* CreateStreamRemover(TReplication* replication, ui64 targetId, const TActorContext& ctx) {
    const auto* target = replication->FindTarget(targetId);
    Y_ABORT_UNLESS(target);
    return CreateStreamRemover(ctx.SelfID, replication->GetYdbProxy(),
        replication->GetId(), target->GetId(), target->GetKind(), target->GetSrcPath(), target->GetStreamName());
}

IActor* CreateStreamRemover(const TActorId& parent, const TActorId& proxy, ui64 rid, ui64 tid,
        TReplication::ETargetKind kind, const TString& srcPath, const TString& streamName)
{
    return new TStreamRemover(parent, proxy, rid, tid, kind, srcPath, streamName);
}

}
