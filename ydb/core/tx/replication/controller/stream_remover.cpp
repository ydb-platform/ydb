#include "logging.h"
#include "private_events.h"
#include "stream_remover.h"
#include "target_cdc.h"
#include "util.h"

#include <ydb/core/base/path.h>
#include <ydb/core/protos/replication.pb.h>
#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>

#include <util/generic/algorithm.h>

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
        case TReplication::ETargetKind::CdcTopic:
        case TReplication::ETargetKind::Topic:
            Send(YdbProxy, new TEvYdbProxy::TEvAlterTopicRequest(SrcPath,
                NYdb::NTopic::TAlterTopicSettings().AppendDropConsumers(StreamName)), 0, ProxyGeneration);
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
            hFunc(TEvYdbProxy::TEvListDirectoryResponse, Handle);
            hFunc(TEvYdbProxy::TEvDescribeTableResponse, Handle);
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
        if (IsTopicTarget(Kind) && result.GetStatus() == NYdb::EStatus::NOT_FOUND) {
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
            if (IsTopicTarget(Kind)) {
                if (result.GetStatus() == NYdb::EStatus::SCHEME_ERROR) {
                    if (!CdcTablePath.empty()) {
                        return CheckSourceTable();
                    }
                    return CheckParentDirectory(SrcPath);
                }
                return Schedule(RetryInterval, new TEvents::TEvWakeup(ProxyGeneration));
            }
        } else {
            YDB_LOG_INFO("Success",
                {"issues", result.GetIssues().ToOneLineString()});
        }

        Reply(std::move(result));
    }

    void CheckSourceTable() {
        Send(YdbProxy, new TEvYdbProxy::TEvDescribeTableRequest(CdcTablePath, {}), 0, ProxyGeneration);
    }

    void Handle(TEvYdbProxy::TEvDescribeTableResponse::TPtr& ev) {
        if (ev->Cookie != ProxyGeneration) {
            return;
        }

        const auto& result = ev->Get()->Result;
        if (result.IsSuccess()) {
            const auto streams = result.GetTableDescription().GetChangefeedDescriptions();
            const auto name = ExtractBase(SrcPath);
            if (!AnyOf(streams, [&](const auto& stream) { return TStringBuf(stream.GetName()) == name; })) {
                return Reply(NYdb::TStatus(NYdb::EStatus::SUCCESS, {}));
            }
        } else if (EqualToOneOf(result.GetStatus(), NYdb::EStatus::SCHEME_ERROR, NYdb::EStatus::NOT_FOUND)) {
            // Check the table's directory, never enumerate the table as if its CDCs were ordinary topics.
            return CheckParentDirectory(CdcTablePath);
        }

        Schedule(RetryInterval, new TEvents::TEvWakeup(ProxyGeneration));
    }

    void CheckParentDirectory(TString childPath) {
        const TString parent(ExtractParent(childPath));
        if (parent.empty()) {
            return Schedule(RetryInterval, new TEvents::TEvWakeup(ProxyGeneration));
        }

        ChildPath = std::move(childPath);
        Send(YdbProxy, new TEvYdbProxy::TEvListDirectoryRequest(parent, {}), 0, ProxyGeneration);
    }

    bool SourceTopicIsGone(const NYdb::NScheme::TListDirectoryResult& result) const {
        using EType = NYdb::NScheme::ESchemeEntryType;
        const auto parentType = result.GetEntry().Type;
        if (!EqualToOneOf(parentType, EType::Directory, EType::SubDomain)) {
            // An ordinary source topic cannot be nested under a table or another topic.
            return EqualToOneOf(parentType, EType::Table, EType::Topic, EType::PqGroup);
        }

        // Directory listings include children regardless of their ACLs.
        const auto name = ExtractBase(ChildPath);
        const auto& children = result.GetChildren();
        const auto child = FindIf(children, [&](const auto& entry) {
            return TStringBuf(entry.Name) == name;
        });
        return child == children.end()
            || (ChildPath == SrcPath
                && !EqualToOneOf(child->Type, EType::Topic, EType::PqGroup, EType::Unknown));
    }

    void Handle(TEvYdbProxy::TEvListDirectoryResponse::TPtr& ev) {
        if (ev->Cookie != ProxyGeneration) {
            return;
        }

        const auto& result = ev->Get()->Result;
        if (result.IsSuccess()) {
            if (SourceTopicIsGone(result)) {
                return Reply(NYdb::TStatus(NYdb::EStatus::SUCCESS, {}));
            }
        } else if (EqualToOneOf(result.GetStatus(), NYdb::EStatus::SCHEME_ERROR, NYdb::EStatus::NOT_FOUND)) {
            return CheckParentDirectory(TString(ExtractParent(ChildPath)));
        } else {
            YDB_LOG_ERROR("Cannot confirm source topic absence",
                {"status", result.GetStatus()},
                {"issues", result.GetIssues().ToOneLineString()});
        }

        Schedule(RetryInterval, new TEvents::TEvWakeup(ProxyGeneration));
    }

    void Reply(NYdb::TStatus result) {
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
            const TString& streamName,
            const TString& cdcTablePath = {})
        : Parent(parent)
        , YdbProxy(proxy)
        , ReplicationId(rid)
        , TargetId(tid)
        , Kind(kind)
        , SrcPath(srcPath)
        , StreamName(streamName)
        , CdcTablePath(cdcTablePath)
        , LogPrefix(CreateActorLogPrefix("StreamRemover", ReplicationId, TargetId))
    {
    }

    void Bootstrap() {
        YDB_LOG_CREATE_CONTEXT(LogPrefix);
        switch (Kind) {
        case TReplication::ETargetKind::Table:
        case TReplication::ETargetKind::IndexTable:
        case TReplication::ETargetKind::CdcTopic:
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
    const TString CdcTablePath;
    TString ChildPath;
    const NActors::NStructuredLog::TStructuredMessage LogPrefix;

    static constexpr auto RetryInterval = TDuration::Seconds(10);

}; // TStreamRemover

IActor* CreateCdcStreamConsumerRemover(const TActorId& parent, const TActorId& proxy, ui64 rid, ui64 tid,
        const TString& sourceTablePath, const TString& streamName, const TString& consumerName)
{
    const TString streamPath = TStringBuilder() << sourceTablePath << "/" << streamName;
    return new TStreamRemover(parent, proxy, rid, tid, TReplication::ETargetKind::Topic,
        streamPath, consumerName, sourceTablePath);
}

IActor* CreateStreamRemover(TReplication* replication, ui64 targetId, const TActorContext& ctx) {
    const auto* target = replication->FindTarget(targetId);
    Y_ABORT_UNLESS(target);
    auto srcPath = target->GetSrcPath();
    if (const auto* cdc = GetCdcTarget(target)) {
        return CreateCdcStreamConsumerRemover(ctx.SelfID, replication->GetYdbProxy(),
            replication->GetId(), targetId, TString(ExtractParent(target->GetSrcPath())),
            cdc->GetCdc().GetDefinition().GetStreamDescription().GetName(), target->GetStreamConsumerName());
    }
    if (target->GetKind() == TReplication::ETargetKind::Topic) {
        srcPath = CanonizePath(IsStartWithSlash(srcPath) ? srcPath
            : replication->GetConfig().GetSrcConnectionParams().GetDatabase() + "/" + srcPath);
    }
    return CreateStreamRemover(ctx.SelfID, replication->GetYdbProxy(),
        replication->GetId(), target->GetId(), target->GetKind(), srcPath, target->GetStreamName());
}

IActor* CreateStreamRemover(const TActorId& parent, const TActorId& proxy, ui64 rid, ui64 tid,
        TReplication::ETargetKind kind, const TString& srcPath, const TString& streamName)
{
    return new TStreamRemover(parent, proxy, rid, tid, kind, srcPath, streamName);
}

}
