#include "event_util.h"
#include "logging.h"
#include "private_events.h"
#include "stream_creator.h"
#include "stream_remover.h"
#include "target_with_stream.h"
#include "util.h"

#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash_set.h>

namespace NKikimr::NReplication::NController {

const TString ReplicationConsumerName = "replicationConsumer";

namespace {

class TWorkerRegistar: public TActorBootstrapped<TWorkerRegistar> {
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

        Partitions = result.GetTopicDescription().GetPartitions();
        for (const auto& partition : Partitions) {
            if (partition.GetParentPartitionIds().empty()) {
                RunWorker(partition.GetPartitionId());
            }
        }

        // the partition graph is only returned by DescribeTopic, the offsets by DescribeConsumer
        DescribeConsumer();
    }

    void DescribeConsumer() {
        Send(YdbProxy, new TEvYdbProxy::TEvDescribeConsumerRequest(SrcStreamPath, SrcStreamConsumerName,
            NYdb::NTopic::TDescribeConsumerSettings().IncludeStats(true)));
    }

    // A child partition gets its worker on the parent's DataEnd, which is not persisted,
    // so after a restart it might run nowhere. Start the children whose parents are fully
    // consumed (the consumer offset is committed only after the data is applied, so the read
    // order is kept), unless the child and all its own children are fully consumed too.
    void Handle(TEvYdbProxy::TEvDescribeConsumerResponse::TPtr& ev) {
        LOG_T("Handle " << ev->Get()->ToString());

        const auto& result = ev->Get()->Result;
        if (result.GetStatus() == NYdb::EStatus::UNSUPPORTED || result.GetStatus() == NYdb::EStatus::CLIENT_CALL_UNIMPLEMENTED) {
            // e.g. the local proxy (transfers, local replications): children are started on DataEnd only
            LOG_W("Describing consumer '" << SrcStreamConsumerName << "' of '" << SrcStreamPath << "' is not supported"
                << ", child partitions are started on parent's DataEnd only: " << ev->Get()->ToString());
            return PassAway();
        }

        if (!result.IsSuccess()) {
            // never give up: otherwise children are silently left unread
            if (IsRetryableError(result)) {
                LOG_W("Error of describing consumer '" << SrcStreamConsumerName << "' of '" << SrcStreamPath << "': " << ev->Get()->ToString() << ". Retry.");
            } else {
                LOG_E("Error of describing consumer '" << SrcStreamConsumerName << "' of '" << SrcStreamPath << "': " << ev->Get()->ToString() << ". Retry.");
            }
            return (void)Schedule(TDuration::Seconds(10), new TEvents::TEvWakeup(DescribeConsumerTag));
        }

        THashSet<ui64> consumed;
        for (const auto& partition : result.GetConsumerDescription().GetPartitions()) {
            const auto& stats = partition.GetPartitionStats();
            const auto& consumerStats = partition.GetPartitionConsumerStats();
            if (stats && consumerStats && consumerStats->GetCommittedOffset() >= stats->GetEndOffset()) {
                consumed.insert(partition.GetPartitionId());
            }
        }

        THashSet<ui64> consumedInactive;
        for (const auto& partition : Partitions) {
            if (!partition.GetActive() && consumed.contains(partition.GetPartitionId())) {
                consumedInactive.insert(partition.GetPartitionId());
            }
        }

        auto isConsumed = [&](ui64 id) { return consumedInactive.contains(id); };
        for (const auto& partition : Partitions) {
            const auto& parents = partition.GetParentPartitionIds();
            if (parents.empty() || !AllOf(parents, isConsumed)) {
                continue;
            }
            if (isConsumed(partition.GetPartitionId()) && AllOf(partition.GetChildPartitionIds(), isConsumed)) {
                continue;
            }

            RunWorker(partition.GetPartitionId());
        }

        PassAway();
    }

    void RunWorker(ui64 partitionId) {
        auto ev = MakeRunWorkerEv(
            ReplicationId, TargetId, Config, partitionId,
            ConnectionParams, ConsistencySettings, SrcStreamPath, SrcStreamConsumerName, DstPathId,
            BatchingSettings, Database);
        Send(Parent, std::move(ev));
    }

    void Handle(TEvents::TEvWakeup::TPtr& ev) {
        if (ev->Get()->Tag == DescribeConsumerTag) {
            DescribeConsumer();
        } else {
            Bootstrap();
        }
    }

    void Retry() {
        LOG_D("Retry");
        Schedule(TDuration::Seconds(10), new TEvents::TEvWakeup());
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::REPLICATION_CONTROLLER_TABLE_WORKER_REGISTAR;
    }

    explicit TWorkerRegistar(
            const TActorId& parent,
            const TActorId& proxy,
            const NKikimrReplication::TConnectionParams& connectionParams,
            const NKikimrReplication::TConsistencySettings& consistencySettings,
            ui64 rid,
            ui64 tid,
            const TString& srcStreamPath,
            const TString& srcStreamConsumerName,
            const TPathId& dstPathId,
            const TReplication::ITarget::IConfig::TPtr& config,
            const NKikimrReplication::TBatchingSettings& batchingSettings,
            const TString& database)
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
        , BatchingSettings(batchingSettings)
        , Database(database)
    {
    }

    void Bootstrap() {
        Become(&TThis::StateWork);
        Send(YdbProxy, new TEvYdbProxy::TEvDescribeTopicRequest(SrcStreamPath, {}));
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvYdbProxy::TEvDescribeTopicResponse, Handle);
            hFunc(TEvYdbProxy::TEvDescribeConsumerResponse, Handle);
            hFunc(TEvents::TEvWakeup, Handle);
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
    const NKikimrReplication::TBatchingSettings BatchingSettings;
    const TString Database;
    std::vector<NYdb::NTopic::TPartitionInfo> Partitions;
    static constexpr ui64 DescribeConsumerTag = 1;

}; // TWorkerRegistar

} // namespace

void TTargetWithStream::Progress(const TActorContext& ctx) {
    auto replication = GetReplication();

    switch (GetStreamState()) {
    case EStreamState::Creating:
        if (GetStreamName().empty() && !NameAssignmentInProcess) {
            ctx.Send(ctx.SelfID, new TEvPrivate::TEvAssignStreamName(replication->GetId(), GetId()));
            NameAssignmentInProcess = true;
        } else if (!StreamCreator) {
            StreamCreator = ctx.Register(CreateStreamCreator(replication, GetId(), ctx));
        }
        return;
    case EStreamState::Removing:
        if (HasWorkers()) {
            RemoveWorkers(ctx);
        } else if (!StreamRemover) {
            StreamRemover = ctx.Register(CreateStreamRemover(replication, GetId(), ctx));
        }
        return;
    case EStreamState::Ready:
    case EStreamState::Removed:
    case EStreamState::Error:
        break;
    }

    TTargetBase::Progress(ctx);
}

void TTargetWithStream::Shutdown(const TActorContext& ctx) {
    for (auto* x : TVector<TActorId*>{&StreamCreator, &StreamRemover}) {
        if (auto actorId = std::exchange(*x, {})) {
            ctx.Send(actorId, new TEvents::TEvPoison());
        }
    }

    TTargetBase::Shutdown(ctx);
}

IActor* TTargetWithStream::CreateWorkerRegistar(const TActorContext& ctx) const {
    auto replication = GetReplication();
    const auto& config = replication->GetConfig();

    return new TWorkerRegistar(ctx.SelfID, replication->GetYdbProxy(),
        config.GetSrcConnectionParams(), config.GetConsistencySettings(),
        replication->GetId(), GetId(), GetStreamPath(), GetStreamConsumerName(), GetDstPathId(), GetConfig(),
        config.GetTransferSpecific().GetBatching(), replication->GetDatabase());
}

}
