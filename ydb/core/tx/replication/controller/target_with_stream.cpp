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

#include <ydb/public/api/protos/draft/ydb_replication.pb.h>

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

        for (const auto& partition : result.GetTopicDescription().GetPartitions()) {
            if (!partition.GetParentPartitionIds().empty()) {
                continue;
            }

            auto ev = MakeRunWorkerEv(
                ReplicationId, TargetId, Config, partition.GetPartitionId(),
                ConnectionParams, ConsistencySettings, SrcStreamPath, SrcStreamConsumerName, DstPathId,
                BatchingSettings, Database, MetricsLevel, Location);
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
            const TString& database,
            const TMetricsConfig& metricsConfig,
            const NKikimrReplication::TReplicationLocationConfig& location
    )
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
        , MetricsLevel(metricsConfig.GetLevel())
        , Location(location)
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
    const NKikimrReplication::TBatchingSettings BatchingSettings;
    const TString Database;
    TMetricsConfig::EMetricsLevel MetricsLevel;
    NKikimrReplication::TReplicationLocationConfig Location;

}; // TWorkerRegistar

} // namespace

TTargetWithStreamStats::TTargetWithStreamStats(TInstant startTime)
    : CollectionStartTime(startTime)
{
}

void TTargetWithStreamStats::RemoveWorker(ui64 workerId) {
    Y_UNUSED(workerId);
    // nop
}

bool TTargetWithStreamStats::UpdateWithSingleStatsItem(ui64 workerId, ui64 key, i64 value) {
    Y_UNUSED(workerId);

    const auto eKey = static_cast<NKikimrReplication::TWorkerStats::EStatsKeys>(key);
    switch (eKey) {
        case NKikimrReplication::TWorkerStats::READ_BYTES:
            ReadBytes.Add(value);
            break;

        case NKikimrReplication::TWorkerStats::READ_MESSAGES:
            ReadMessages.Add(value);
            break;

        case NKikimrReplication::TWorkerStats::WRITE_BYTES:
            WriteBytes.Add(value);
            break;

        case NKikimrReplication::TWorkerStats::WRITE_ROWS:
            WriteRows.Add(value);
            break;

        case NKikimrReplication::TWorkerStats::DECOMPRESS_ELAPSED_CPU:
            DecompressionCpuTime.Add(value);
            break;
        default:
            return false;
    };
    return true;
}

void TTargetWithStreamStats::Serialize(NKikimrReplication::TEvDescribeReplicationResult& destination, bool detailed) const {
    auto& dstStats = *destination.MutableStats();
    Y_UNUSED(detailed);
    ReadBytes.ToProto(*dstStats.MutableReadBytes(), 1);
    ReadMessages.ToProto(*dstStats.MutableReadMessages(), 1);
    WriteBytes.ToProto(*dstStats.MutableWriteBytes(), 1);
    WriteRows.ToProto(*dstStats.MutableWriteRows(), 1);
    DecompressionCpuTime.ToProto(*dstStats.MutableDecompressionCpuTime(), 1'000'000);
    dstStats.MutableStatsCollectionStart()->CopyFrom(NProtoInterop::CastToProto(CollectionStartTime));
}


bool TTragetWithStreamCounters::UpdateWithSingleStatsItem(ui64 workerId, ui64 key, i64 value) {
    if (!CountersGroup)
        return false;

    Y_UNUSED(workerId);

    const auto eKey = static_cast<NKikimrReplication::TWorkerStats::EStatsKeys>(key);
    switch (eKey) {
        case NKikimrReplication::TWorkerStats::READ_TIME:
            ReadTime->Add(value);
            break;

        case NKikimrReplication::TWorkerStats::WRITE_TIME:
            WriteTime->Add(value);
            break;

        case NKikimrReplication::TWorkerStats::DECOMPRESS_ELAPSED_CPU:
            DecompressionCpuTime->Add(value);
            break;

        case NKikimrReplication::TWorkerStats::WRITE_BYTES:
            WriteBytes->Add(value);
            break;

        case NKikimrReplication::TWorkerStats::WRITE_ROWS:
            WriteRows->Add(value);
            break;

        case NKikimrReplication::TWorkerStats::WRITE_ERRORS:
            WriteErrors->Add(value);
            break;

        default:
            return false;
    }
    return true;
}

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

void TTargetWithStream::WorkerStatusChanged(ui64, ui64) {
    // nop
}

void TTargetWithStream::UpdateStats(ui64 workerId, const NKikimrReplication::TWorkerStats& newStats) {
    if (!Stats && !Counters)
        return;

    if (!HasWorker(workerId)) {
        if (Stats) {
            Stats->RemoveWorker(workerId);
        }
        return;
    }

    for (const auto& item : newStats.GetValues()) {
        if (Stats) {
            Stats->UpdateWithSingleStatsItem(workerId, item.GetKey(), item.GetValue());
        }
        if (Counters) {
            Counters->UpdateWithSingleStatsItem(workerId, item.GetKey(), item.GetValue());
        }
    }
}

const TReplication::ITargetStats* TTargetWithStream::GetStats() const {
    return Stats.get();
}

IActor* TTargetWithStream::CreateWorkerRegistar(const TActorContext& ctx) const {
    auto replication = GetReplication();
    const auto& config = replication->GetConfig();

    return new TWorkerRegistar(ctx.SelfID, replication->GetYdbProxy(),
        config.GetSrcConnectionParams(), config.GetConsistencySettings(),
        replication->GetId(), GetId(), GetStreamPath(), GetStreamConsumerName(), GetDstPathId(), GetConfig(),
        config.GetTransferSpecific().GetBatching(), replication->GetDatabase(), config.GetMetricsConfig(),
        replication->GetLocation());
}

}
