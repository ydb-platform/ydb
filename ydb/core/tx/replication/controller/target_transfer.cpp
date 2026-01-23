#include "stream_consumer_remover.h"
#include "target_transfer.h"

#include <ydb/core/base/path.h>
#include <ydb/core/protos/replication.pb.h>
#include <ydb/core/tx/replication/service/service.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/public/api/protos/draft/ydb_replication.pb.h>

namespace NKikimr::NReplication::NController {

TTargetTransfer::TTargetTransfer(TReplication* replication, ui64 id, const IConfig::TPtr& config)
    : TTargetWithStream(replication, ETargetKind::Transfer, id, config)
    , Stats(std::make_unique<TTransferStats>(Now()))
    , Location(replication->GetLocation())
{
    MetricsLevel = replication->GetConfig().HasMetricsConfig() ? replication->GetConfig().GetMetricsConfig().GetLevel() : 0;
}

void TTargetTransfer::UpdateConfig(const NKikimrReplication::TReplicationConfig& cfg) {
    auto& t = cfg.GetTransferSpecific().GetTarget();
    Config = std::make_shared<TTargetTransfer::TTransferConfig>(
        GetConfig()->GetSrcPath(),
        GetConfig()->GetDstPath(),
        t.GetTransformLambda(),
        cfg.GetTransferSpecific().GetRunAsUser(),
        t.GetDirectoryPath());

    MetricsLevel = cfg.HasMetricsConfig() ? cfg.GetMetricsConfig().GetLevel() : 0;
    Location.CopyFrom(cfg.GetLocation());
}

void TTargetTransfer::Progress(const TActorContext& ctx) {
    auto replication = GetReplication();

    switch (GetStreamState()) {
    case EStreamState::Removing:
        if (HasWorkers()) {
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

TString TTargetTransfer::GetStreamPath() const {
    return CanonizePath(GetSrcPath());
}

TTargetTransfer::TTransferConfig::TTransferConfig(const TString& srcPath, const TString& dstPath, const TString& transformLambda, const TString& runAsUser, const TString& directoryPath)
    : TConfigBase(ETargetKind::Transfer, srcPath, dstPath)
    , TransformLambda(transformLambda)
    , RunAsUser(runAsUser)
    , DirectoryPath(directoryPath)
{
}

const TString& TTargetTransfer::TTransferConfig::GetTransformLambda() const {
    return TransformLambda;
}

const TString& TTargetTransfer::TTransferConfig::GetRunAsUser() const {
    return RunAsUser;
}

const TString& TTargetTransfer::TTransferConfig::GetDirectoryPath() const {
    return DirectoryPath;
}

void TTargetTransfer::RemoveWorker(ui64 id) {
    TBase::RemoveWorker(id);
    if (Stats) {
        Stats->WorkersStats.erase(id);
    }
}

#define ADD_STATS_VAL(KEY, VALUE)               \
    case NKikimrReplication::TWorkerStats::KEY: \
        workerStats.VALUE += value;             \
        Stats->VALUE += value;                  \
        break;

#define ADD_STATS_TO_WINDOW(KEY, WINDOW)                \
    case NKikimrReplication::TWorkerStats::KEY:         \
        workerStats.WINDOW.Minute.Update(value, Now()); \
        workerStats.WINDOW.Hour.Update(value, Now());   \
        Stats->WINDOW.Minute.Update(value, Now());      \
        Stats->WINDOW.Hour.Update(value, Now());        \
        break;

#define ADD_STATS_TO_WINDOW_AND_CNTR(KEY, WINDOW)       \
    case NKikimrReplication::TWorkerStats::KEY:         \
        workerStats.WINDOW.Minute.Update(value, Now()); \
        workerStats.WINDOW.Hour.Update(value, Now());   \
        Stats->WINDOW.Minute.Update(value, Now());      \
        Stats->WINDOW.Hour.Update(value, Now());        \
        if (Counters) {                                 \
            Counters->WINDOW->Add(value);               \
        }                                               \
        break;

#define ADD_STATS_TO_CNTR(KEY, CNTR)           \
    case NKikimrReplication::TWorkerStats::KEY: \
        if (Counters) {                         \
            Counters->CNTR->Add(value);         \
        }                                       \
        break;

void TTargetTransfer::EnsureCounters(NMonitoring::TDynamicCounterPtr counters) {

    auto metricsValEnumVal = static_cast<NKikimrReplication::TReplicationConfig::TMetricsConfig::EMetricsLevel>(MetricsLevel);
    switch (metricsValEnumVal) {
        case NKikimrReplication::TReplicationConfig::TMetricsConfig::LEVEL_OBJECT:
        case NKikimrReplication::TReplicationConfig::TMetricsConfig::LEVEL_DETAILED:
            if (!Counters) {
                Counters.ConstructInPlace(counters, Location);
            }
            break;
        default:
            Counters = Nothing();
            break;
    }
}

void TTargetTransfer::WorkerStatusChanged(ui64 workerId, ui64 status, NMonitoring::TDynamicCounterPtr counters) {
    EnsureCounters(counters);
    auto& workerStats = Stats->WorkersStats[workerId];
    switch (static_cast<NKikimrReplication::TEvWorkerStatus::EStatus>(status)) {
        case NKikimrReplication::TEvWorkerStatus::STATUS_RUNNING:
            workerStats.RestartsCount += 1;
            workerStats.Restarts.Hour.Update(1, Now());
            workerStats.Restarts.Minute.Update(1, Now());
            workerStats.StartTime = Now();
            if (Counters) {
                Counters->Restarts->Add(1);
            }
            break;
        case NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED:
            workerStats.StartTime = TInstant::Zero();
            Stats->LastWorkerStartTime = TInstant::Zero();
            break;
        default:
            break;
    }
    if (Counters) {
        Counters->MinWorkerUptime->Set(0);
    }
}

void TTargetTransfer::UpdateStats(ui64 workerId, const NKikimrReplication::TWorkerStats& newStats, NMonitoring::TDynamicCounterPtr counters) {
    if (!HasWorker(workerId)) {
        Stats->WorkersStats.erase(workerId);
        return;
    }
    EnsureCounters(counters);

    auto& workerStats = Stats->WorkersStats[workerId];
    if (newStats.HasStartTime()) {
        workerStats.StartTime = TInstant::Seconds(newStats.GetStartTime().seconds());
    }
    bool hasWorkerWithoutStartTime = false;
    if (Stats->LastWorkerStartTime == TInstant::Zero()) {
        for (const auto& [_, workerStats] : Stats->WorkersStats) {
            if (workerStats.StartTime == TInstant::Zero()) {
                hasWorkerWithoutStartTime = true;
                break;
            }
        }
    }
    if ((!hasWorkerWithoutStartTime && Stats->LastWorkerStartTime == TInstant::Zero()) || Stats->LastWorkerStartTime < workerStats.StartTime) {
        Stats->LastWorkerStartTime = workerStats.StartTime;
    }

    if (Counters) {
        if (Stats->LastWorkerStartTime) {
            Counters->MinWorkerUptime->Set((Now() - Stats->LastWorkerStartTime).MilliSeconds());
        } else {
            Counters->MinWorkerUptime->Set(0);
        }
    }

    for (const auto& item : newStats.GetValues()) {
        const auto key = static_cast<NKikimrReplication::TWorkerStats::EStatsKeys>(item.GetKey());
        const auto value = item.GetValue();
        switch(key) {
            ADD_STATS_TO_WINDOW(READ_BYTES, ReadBytes);
            ADD_STATS_TO_WINDOW(READ_MESSAGES, ReadMessages);
            ADD_STATS_TO_WINDOW_AND_CNTR(WRITE_BYTES, WriteBytes);
            ADD_STATS_TO_WINDOW_AND_CNTR(WRITE_ROWS, WriteRows);
            ADD_STATS_TO_WINDOW_AND_CNTR(DECOMPRESS_ELAPSED_CPU, DecompressionCpuTime)
            ADD_STATS_TO_WINDOW_AND_CNTR(PROCESSING_ELAPSED_CPU, ProcessingCpuTime)

            ADD_STATS_TO_CNTR(READ_TIME, ReadTime);
            ADD_STATS_TO_CNTR(PROCESSING_TIME, ProcessingTime);
            ADD_STATS_TO_CNTR(WRITE_TIME, WriteTime);
            ADD_STATS_TO_CNTR(WRITE_ERRORS, WriteErrors);
            ADD_STATS_TO_CNTR(PROCESSING_ERRORS, ProcessingErrors);
            case NKikimrReplication::TWorkerStats::READ_PARTITION:
                workerStats.Partition = value;
                break;
            case NKikimrReplication::TWorkerStats::READ_OFFSET:
                workerStats.ReadOffset = value;
                break;

            case NKikimrReplication::TWorkerStats::WORK_OPERATION: {
                workerStats.ChangeStateTime = Now();
                workerStats.Operation = static_cast<NKikimrReplication::EWorkOperation>(value);
                break;
            }
            default:
                break;
        };
    }
}
#undef ADD_STATS_VAL
#undef ADD_STATS_TO_WINDOW

const TReplication::ITargetStats* TTargetTransfer::GetStats() const {
    return Stats.get();
}

#define SET_PROTO_WINDOW(SRC_NAME, DST_STATS, PROTO_NAME, MULTIPLIER)                                               \
    DST_STATS.mutable_##PROTO_NAME()->mutable_per_minute()->set_seconds(SRC_NAME.Minute.GetValue() * MULTIPLIER); \
    DST_STATS.mutable_##PROTO_NAME()->mutable_per_hour()->set_seconds(SRC_NAME.Hour.GetValue() * MULTIPLIER);

void TTransferStats::FillToProto(NKikimrReplication::TEvDescribeReplicationResult& destination, bool includeDetailed) const {
    auto& dstStats = *destination.MutableStats()->MutableTransfer();
    SET_PROTO_WINDOW(ReadBytes, dstStats, read_bytes, 1);
    SET_PROTO_WINDOW(ReadMessages, dstStats, read_messages, 1);
    SET_PROTO_WINDOW(WriteBytes, dstStats, write_bytes, 1);
    SET_PROTO_WINDOW(WriteRows, dstStats, write_rows, 1);
    SET_PROTO_WINDOW(DecompressionCpuTime, dstStats, decompression_cpu_time, 1'000'000);
    SET_PROTO_WINDOW(ProcessingCpuTime, dstStats, processing_cpu_time, 1'000'000);

    if (LastWorkerStartTime) {
        dstStats.mutable_min_worker_uptime()->set_seconds((Now() - LastWorkerStartTime).Seconds());
    }

    dstStats.mutable_stats_collection_start()->set_seconds(CollectionStartTime.Seconds());

    if (includeDetailed) {
        for (const auto& [id, workerStats] : WorkersStats) {
            auto& dstWorker = *dstStats.add_workers_stats();
            dstWorker.set_worker_id(ToString(id));
            switch (workerStats.Operation) {
                case NKikimrReplication::EWorkOperation::UNSPECIFIED:
                    dstWorker.set_state(Ydb::Replication::DescribeTransferResult_Stats::STATE_UNSPECIFIED);
                    break;
                case NKikimrReplication::EWorkOperation::READ:
                    dstWorker.set_state(Ydb::Replication::DescribeTransferResult_Stats::STATE_READ);
                    break;
                case NKikimrReplication::EWorkOperation::DECOMPRESS:
                    dstWorker.set_state(Ydb::Replication::DescribeTransferResult_Stats::STATE_DECOMPRESS);
                    break;
                case NKikimrReplication::EWorkOperation::PROCESS:
                    dstWorker.set_state(Ydb::Replication::DescribeTransferResult_Stats::STATE_PROCESS);
                    break;
                case NKikimrReplication::EWorkOperation::WRITE:
                    dstWorker.set_state(Ydb::Replication::DescribeTransferResult_Stats::STATE_WRITE);
                    break;
            }
            dstWorker.set_partition_id(workerStats.Partition);
            dstWorker.set_read_offset(workerStats.ReadOffset);
            dstWorker.mutable_uptime()->set_seconds((Now() - workerStats.StartTime).Seconds());
            dstWorker.mutable_last_state_change()->set_seconds(workerStats.ChangeStateTime.Seconds());
            dstWorker.set_restart_count(workerStats.RestartsCount);
            SET_PROTO_WINDOW(workerStats.Restarts, dstWorker, restarts, 1);
            SET_PROTO_WINDOW(workerStats.ReadBytes, dstWorker, read_bytes, 1);
            SET_PROTO_WINDOW(workerStats.ReadMessages, dstWorker, read_messages, 1);
            SET_PROTO_WINDOW(workerStats.WriteBytes, dstWorker, write_bytes, 1);
            SET_PROTO_WINDOW(workerStats.WriteRows, dstWorker, write_rows, 1);
            SET_PROTO_WINDOW(workerStats.DecompressionCpuTime, dstWorker, decompression_cpu_time, 1'000'000);
            SET_PROTO_WINDOW(workerStats.ProcessingCpuTime, dstWorker, processing_cpu_time, 1'000'000);
        }
    }
}
#undef SET_PROTO_WINDOW

}
