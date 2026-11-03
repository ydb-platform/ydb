#include "event_util.h"
#include "logging.h"
#include "private_events.h"
#include "stream_creator.h"
#include "stream_remover.h"
#include "target_with_stream.h"
#include "util.h"

#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>
#include <ydb/core/protos/metrics_config.pb.h>
#include <ydb/core/protos/replication.pb.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/public/api/protos/draft/ydb_replication.pb.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash_set.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::REPLICATION_CONTROLLER

namespace NKikimr::NReplication::NController {

const TString ReplicationConsumerName = "replicationConsumer";

using TMetricsConfig = NKikimrProto::NMetricsConfig::TMetricsConfig;

namespace {

class TWorkerRegistar: public TActorBootstrapped<TWorkerRegistar> {
    void Handle(TEvYdbProxy::TEvDescribeTopicResponse::TPtr& ev) {
        YDB_LOG_TRACE("Handle",
            {"ev", ev->Get()->ToString()});

        const auto& result = ev->Get()->Result;
        if (!result.IsSuccess()) {
            if (IsRetryableError(result)) {
                YDB_LOG_WARN("Error of resolving topic",
                    {"streamPath", SrcStreamPath},
                    {"ev", ev->Get()->ToString()},
                    {"outcome", "retry"});
                return Retry();
            }

            YDB_LOG_ERROR("Error of resolving topic",
                {"streamPath", SrcStreamPath},
                {"ev", ev->Get()->ToString()},
                {"outcome", "stop"});
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
        YDB_LOG_TRACE("Handle",
            {"ev", ev->Get()->ToString()});

        const auto& result = ev->Get()->Result;
        if (result.GetStatus() == NYdb::EStatus::UNSUPPORTED || result.GetStatus() == NYdb::EStatus::CLIENT_CALL_UNIMPLEMENTED) {
            // e.g. the local proxy (transfers, local replications): children are started on DataEnd only
            YDB_LOG_WARN("Describing consumer is not supported, child partitions are started on parent's DataEnd only",
                {"streamPath", SrcStreamPath},
                {"consumer", SrcStreamConsumerName},
                {"ev", ev->Get()->ToString()});
            return PassAway();
        }

        if (!result.IsSuccess()) {
            // never give up: otherwise children are silently left unread
            if (IsRetryableError(result)) {
                YDB_LOG_WARN("Error of describing consumer",
                    {"streamPath", SrcStreamPath},
                    {"consumer", SrcStreamConsumerName},
                    {"ev", ev->Get()->ToString()},
                    {"outcome", "retry"});
            } else {
                YDB_LOG_ERROR("Error of describing consumer",
                    {"streamPath", SrcStreamPath},
                    {"consumer", SrcStreamConsumerName},
                    {"ev", ev->Get()->ToString()},
                    {"outcome", "retry"});
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
            BatchingSettings, Database, MetricsLevel, Location);
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
        YDB_LOG_DEBUG("Retry");
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
            const NKikimrReplication::TReplicationLocationConfig& location)
        : Parent(parent)
        , YdbProxy(proxy)
        , ConnectionParams(connectionParams)
        , ConsistencySettings(consistencySettings)
        , ReplicationId(rid)
        , TargetId(tid)
        , SrcStreamPath(srcStreamPath)
        , SrcStreamConsumerName(srcStreamConsumerName)
        , DstPathId(dstPathId)
        , LogPrefix(CreateActorLogPrefix("TableWorkerRegistar", ReplicationId, TargetId))
        , Config(config)
        , BatchingSettings(batchingSettings)
        , Database(database)
        , MetricsLevel(metricsConfig.GetLevel())
        , Location(location)
    {
    }

    void Bootstrap() {
        YDB_LOG_CREATE_CONTEXT(LogPrefix);
        Become(&TThis::StateWork);
        Send(YdbProxy, new TEvYdbProxy::TEvDescribeTopicRequest(SrcStreamPath, {}));
    }

    STATEFN(StateWork) {
        YDB_LOG_CREATE_CONTEXT(LogPrefix,
            {"actorState", "StateWork"});
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
    const NActors::NStructuredLog::TStructuredMessage LogPrefix;
    const TReplication::ITarget::IConfig::TPtr Config;
    const NKikimrReplication::TBatchingSettings BatchingSettings;
    const TString Database;
    TMetricsConfig::EMetricsLevel MetricsLevel;
    NKikimrReplication::TReplicationLocationConfig Location;
    std::vector<NYdb::NTopic::TPartitionInfo> Partitions;
    static constexpr ui64 DescribeConsumerTag = 1;

}; // TWorkerRegistar

} // namespace

TTargetWithStreamStats::TTargetWithStreamStats(TInstant startTime)
    : CollectionStartTime(startTime)
{
}

void TTargetWithStreamStats::RemoveWorker(ui64) {
    // nop
}

bool TTargetWithStreamStats::UpdateWithSingleStatsItem(ui64, ui64 key, i64 value) {
    switch (static_cast<NKikimrReplication::TWorkerStats::EStatsKeys>(key)) {
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
    }

    return true;
}

void TTargetWithStreamStats::Serialize(NKikimrReplication::TEvDescribeReplicationResult& destination, bool) const {
    auto& dstStats = *destination.MutableStats();
    ReadBytes.ToProto(*dstStats.MutableReadBytes(), 1);
    ReadMessages.ToProto(*dstStats.MutableReadMessages(), 1);
    WriteBytes.ToProto(*dstStats.MutableWriteBytes(), 1);
    WriteRows.ToProto(*dstStats.MutableWriteRows(), 1);
    DecompressionCpuTime.ToProto(*dstStats.MutableDecompressionCpuTime(), 1'000'000);
    dstStats.MutableStatsCollectionStart()->CopyFrom(NProtoInterop::CastToProto(CollectionStartTime));
}

bool TTargetWithStreamCounters::UpdateWithSingleStatsItem(ui64, ui64 key, i64 value) {
    if (!CountersGroup) {
        return false;
    }

    switch (static_cast<NKikimrReplication::TWorkerStats::EStatsKeys>(key)) {
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

bool TTargetWithStream::CanDetachWithoutStream() const {
    if (GetReplication()->GetDesiredState() != TReplication::EState::Done) {
        return false;
    }

    if (GetStreamState() == EStreamState::Removing) {
        return false;
    }

    switch (GetDstState()) {
    case EDstState::Attaching:
    case EDstState::Paused:
    case EDstState::Alter:
    case EDstState::Done:
        return true;
    default:
        return false;
    }
}

void TTargetWithStream::Progress(const TActorContext& ctx) {
    auto replication = GetReplication();

    if (CanDetachWithoutStream()) {
        Reset(StreamCreator);
        // Keep the stream metadata so a later DROP can clean up a stream whose
        // creation was already submitted when DONE was requested.
        TTargetBase::Progress(ctx);
        return;
    }

    // Source setup can return before destination progress, including while the
    // replication is initially PAUSED. Refresh retained source actors first.
    if (replication->GetYdbProxy()) {
        for (const auto& actor : {StreamCreator, StreamRemover}) {
            if (actor) {
                ctx.Send(actor, new TEvPrivate::TEvUpdateYdbProxy(replication->GetYdbProxy()));
            }
        }
    }

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

bool TTargetWithStream::IsExpectedStreamCreator(const TActorId& actorId) const {
    return StreamCreator && StreamCreator == actorId;
}

void TTargetWithStream::WorkerStatusChanged(ui64, ui64) {
    // nop
}

bool TTargetWithStream::UpdateStats(ui64 workerId, const NKikimrReplication::TWorkerStats& newStats) {
    auto* stats = GetStatsImpl();
    auto* counters = GetCountersImpl();
    if (!stats && !counters) {
        return false;
    }

    if (!HasWorker(workerId)) {
        if (stats) {
            stats->RemoveWorker(workerId);
        }

        return false;
    }

    for (const auto& item : newStats.GetValues()) {
        if (stats) {
            stats->UpdateWithSingleStatsItem(workerId, item.GetKey(), item.GetValue());
        }
        if (counters) {
            counters->UpdateWithSingleStatsItem(workerId, item.GetKey(), item.GetValue());
        }
    }

    return true;
}

const TReplication::ITargetStats* TTargetWithStream::GetStats() {
    return GetStatsImpl();
}

TTargetWithStreamStats* TTargetWithStream::GetStatsImpl() {
    return Stats.get();
}

TTargetWithStreamCounters* TTargetWithStream::GetCountersImpl() {
    return Counters.get();
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

void TTargetWithStream::SetLocation() {
    if (!Location) {
        Location = MakeHolder<NKikimrReplication::TReplicationLocationConfig>();
        Location->CopyFrom(GetReplication()->GetLocation());
    }
}

}
