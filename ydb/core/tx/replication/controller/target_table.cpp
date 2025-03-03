#include "event_util.h"
#include "logging.h"
#include "stream_consumer_remover.h"
#include "target_table.h"
#include "util.h"

#include <ydb/core/base/path.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NReplication::NController {

class TTableWorkerRegistar: public TActorBootstrapped<TTableWorkerRegistar> {
    void Handle(TEvYdbProxy::TEvDescribeTopicResponse::TPtr& ev) {
        LOG_T("Handle " << ev->Get()->ToString());

        const auto& result = ev->Get()->Result;
        if (!result.IsSuccess()) {
            if (IsRetryableError(result)) {
                LOG_W("Error of resolving topic '" << SrcStreamPath << "': " << ev->Get()->ToString() << ". Retry.");
                return Retry();
            }

            LOG_E("Error of resolving topic '" << SrcStreamPath << "': " << ev->Get()->ToString() << ". Stop.");
            return; // TODO: hard error
        }

        for (const auto& partition : result.GetTopicDescription().GetPartitions()) {
            if (!partition.GetParentPartitionIds().empty()) {
                continue;
            }

            auto ev = MakeRunWorkerEv(
                ReplicationId, TargetId, Config, partition.GetPartitionId(),
                ConnectionParams, ConsistencySettings, SrcStreamPath, SrcStreamConsumerName, DstPathId);
            Send(Parent, std::move(ev));
        }

        PassAway();
    }

    void Retry() {
        LOG_D("Retry");
        Schedule(TDuration::Seconds(10), new TEvents::TEvWakeup());
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::REPLICATION_CONTROLLER_TABLE_WORKER_REGISTAR;
    }

    explicit TTableWorkerRegistar(
            const TActorId& parent,
            const TActorId& proxy,
            const NKikimrReplication::TConnectionParams& connectionParams,
            const NKikimrReplication::TConsistencySettings& consistencySettings,
            ui64 rid,
            ui64 tid,
            const TString& srcStreamPath,
            const TString& srcStreamConsumerName,
            const TPathId& dstPathId,
            const TReplication::ITarget::IConfig::TPtr& config)
        : Parent(parent)
        , YdbProxy(proxy)
        , ConnectionParams(connectionParams)
        , ConsistencySettings(consistencySettings)
        , ReplicationId(rid)
        , TargetId(tid)
        , SrcStreamPath(srcStreamPath)
        , SrcStreamConsumerName(srcStreamConsumerName)
        , DstPathId(dstPathId)
        , LogPrefix("TableWorkerRegistar", ReplicationId, TargetId)
        , Config(config)
    {
    }

    void Bootstrap() {
        Become(&TThis::StateWork);
        Send(YdbProxy, new TEvYdbProxy::TEvDescribeTopicRequest(SrcStreamPath, {}));
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvYdbProxy::TEvDescribeTopicResponse, Handle);
            sFunc(TEvents::TEvWakeup, Bootstrap);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    const TActorId Parent;
    const TActorId YdbProxy;
    const NKikimrReplication::TConnectionParams ConnectionParams;
    const NKikimrReplication::TConsistencySettings ConsistencySettings;
    const ui64 ReplicationId;
    const ui64 TargetId;
    const TString SrcStreamPath;
    const TString SrcStreamConsumerName;
    const TPathId DstPathId;
    const TActorLogPrefix LogPrefix;
    const TReplication::ITarget::IConfig::TPtr Config;

}; // TTableWorkerRegistar

TTargetTableBase::TTargetTableBase(TReplication* replication, ETargetKind finalKind,
        ui64 id, const IConfig::TPtr& config)
    : TTargetWithStream(replication, finalKind, id, config)
{
}

IActor* TTargetTableBase::CreateWorkerRegistar(const TActorContext& ctx) const {
    auto replication = GetReplication();
    const auto& config = replication->GetConfig();
    return new TTableWorkerRegistar(ctx.SelfID, replication->GetYdbProxy(),
        config.GetSrcConnectionParams(), config.GetConsistencySettings(),
        replication->GetId(), GetId(), BuildStreamPath(), GetStreamConsumerName(), GetDstPathId(), GetConfig());
}

TTargetTable::TTargetTable(TReplication* replication, ui64 id, const IConfig::TPtr& config)
    : TTargetTableBase(replication, ETargetKind::Table, id, config)
{
}

TString TTargetTable::BuildStreamPath() const {
    return CanonizePath(ChildPath(SplitPath(GetSrcPath()), GetStreamName()));
}

TString TTargetTableBase::GetStreamPath() const {
    return BuildStreamPath();
}

TTargetIndexTable::TTargetIndexTable(TReplication* replication, ui64 id, const IConfig::TPtr& config)
    : TTargetTableBase(replication, ETargetKind::IndexTable, id, config)
{
}

TString TTargetIndexTable::BuildStreamPath() const {
    return CanonizePath(ChildPath(SplitPath(GetSrcPath()), {"indexImplTable", GetStreamName()}));
}

TTargetTransfer::TTargetTransfer(TReplication* replication, ui64 id, const IConfig::TPtr& config)
    : TTargetTableBase(replication, ETargetKind::Transfer, id, config)
{
}

void TTargetTransfer::UpdateConfig(const NKikimrReplication::TReplicationConfig& cfg) {
    auto& t = cfg.GetTransferSpecific().GetTargets(0);
    Config = std::make_shared<TTargetTransfer::TTransferConfig>(
        GetConfig()->GetSrcPath(),
        GetConfig()->GetDstPath(),
        t.GetTransformLambda());
}

void TTargetTransfer::Progress(const TActorContext& ctx) {
    auto replication = GetReplication();

    switch (GetStreamState()) {
    case EStreamState::Removing:
        if (GetWorkers()) {
            RemoveWorkers(ctx);
        } else if (!StreamConsumerRemover) {
            StreamConsumerRemover = ctx.Register(CreateStreamConsumerRemover(replication, GetId(), ctx));
        }
        return;
    case EStreamState::Creating:
    case EStreamState::Ready:
    case EStreamState::Removed:
    case EStreamState::Error:
        break;
    }

    TTargetWithStream::Progress(ctx);
}

void TTargetTransfer::Shutdown(const TActorContext& ctx) {
    for (auto* x : TVector<TActorId*>{&StreamConsumerRemover}) {
        if (auto actorId = std::exchange(*x, {})) {
            ctx.Send(actorId, new TEvents::TEvPoison());
        }
    }

    TTargetWithStream::Shutdown(ctx);
}

TString TTargetTransfer::BuildStreamPath() const {
    return CanonizePath(GetSrcPath());
}

TTargetTransfer::TTransferConfig::TTransferConfig(const TString& srcPath, const TString& dstPath, const TString& transformLambda)
    : TConfigBase(ETargetKind::Transfer, srcPath, dstPath)
    , TransformLambda(transformLambda)
{
}

const TString& TTargetTransfer::TTransferConfig::GetTransformLambda() const {
    return TransformLambda;
}

}
