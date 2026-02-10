#include "stream_consumer_remover.h"
#include "target_transfer.h"

#include <ydb/core/base/path.h>
#include <ydb/core/protos/replication.pb.h>
#include <ydb/core/tx/replication/service/service.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/public/api/protos/draft/ydb_replication.pb.h>

#include <library/cpp/protobuf/interop/cast.h>

namespace NKikimr::NReplication::NController {

class TTransferStats : public TTargetWithStreamStats {
    using TBase = TTargetWithStreamStats;
    using TBase::TBase;
    using TBase::TMultiSlidingWindow;

    struct TWorkerStats {
        NKikimrReplication::EWorkOperation Operation;
        TInstant LastChange = TInstant::Zero();
        ui32 ReadOffset;
        ui32 Partition;
        TInstant StartTime = TInstant::Zero();
        TInstant ChangeStateTime = TInstant::Zero();
        ui32 RestartsCount;
        TMultiSlidingWindow Restarts;
        TMultiSlidingWindow ReadBytes;
        TMultiSlidingWindow ReadMessages;
        TMultiSlidingWindow WriteBytes;
        TMultiSlidingWindow WriteRows;
        TMultiSlidingWindow DecompressionCpuTime;
        TMultiSlidingWindow ProcessingCpuTime;
    };

public:
    bool UpdateWithSingleStatsItem(ui64 workerId, ui64 key, i64 value) override {
        TBase::UpdateWithSingleStatsItem(workerId, key, value);
        const auto eKey = static_cast<NKikimrReplication::TWorkerStats::EStatsKeys>(key);
        auto& workerStats = WorkersStats[workerId];

        switch (eKey) {
            case NKikimrReplication::TWorkerStats::READ_BYTES:
                workerStats.ReadBytes.Add(value);
                break;

            case NKikimrReplication::TWorkerStats::READ_MESSAGES:
                workerStats.ReadMessages.Add(value);
                break;

            case NKikimrReplication::TWorkerStats::WRITE_BYTES:
                workerStats.WriteBytes.Add(value);
                break;

            case NKikimrReplication::TWorkerStats::WRITE_ROWS:
                workerStats.WriteRows.Add(value);
                break;

            case NKikimrReplication::TWorkerStats::DECOMPRESS_ELAPSED_CPU:
                workerStats.DecompressionCpuTime.Add(value);
                break;

            case NKikimrReplication::TWorkerStats::PROCESSING_ELAPSED_CPU:
                workerStats.ProcessingCpuTime.Add(value);
                ProcessingCpuTime.Add(value);
                break;

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

        return true;
    }

    void RemoveWorker(ui64 workerId) override {
        TBase::RemoveWorker(workerId);
        WorkersStats.erase(workerId);
    }

    void Serialize(NKikimrReplication::TEvDescribeReplicationResult& destination, bool detailed) const override {
        TBase::Serialize(destination, detailed);

        auto& dstStats = *destination.MutableStats()->MutableTransfer();
        ProcessingCpuTime.ToProto(*dstStats.MutableProcessingCpuTime(), 1'000'000);

        if (LastWorkerStartTime) {
            dstStats.MutableMinWorkerUptime()->CopyFrom(NProtoInterop::CastToProto(Now() - LastWorkerStartTime));
        }

        if (detailed) {
            for (const auto& [id, workerStats] : WorkersStats) {
                auto& dstWorker = *dstStats.AddWorkersStats();
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
                dstWorker.mutable_uptime()->CopyFrom(NProtoInterop::CastToProto(Now() - workerStats.StartTime));
                dstWorker.mutable_last_state_change()->CopyFrom(NProtoInterop::CastToProto(workerStats.ChangeStateTime));
                dstWorker.set_restart_count(workerStats.RestartsCount);
                workerStats.Restarts.ToProto(*dstWorker.mutable_restarts(), 1);
                workerStats.ReadBytes.ToProto(*dstWorker.mutable_read_bytes(), 1);
                workerStats.ReadMessages.ToProto(*dstWorker.mutable_read_messages(), 1);
                workerStats.WriteBytes.ToProto(*dstWorker.mutable_write_bytes(), 1);
                workerStats.WriteRows.ToProto(*dstWorker.mutable_write_rows(), 1);
                workerStats.DecompressionCpuTime.ToProto(*dstWorker.mutable_decompression_cpu_time(), 1'000'000);
                workerStats.ProcessingCpuTime.ToProto(*dstWorker.mutable_processing_cpu_time(), 1'000'000);
            }
        }
    }

public:
    THashMap<ui64, TWorkerStats> WorkersStats;
    TMultiSlidingWindow ProcessingCpuTime;
    TInstant LastWorkerStartTime = TInstant::Zero();
};

struct TTransferCounters: public TTragetWithStreamCounters {

    NMonitoring::TDynamicCounterPtr AggeregatedCounters;

    NMonitoring::TDynamicCounters::TCounterPtr ProcessingTime;
    NMonitoring::TDynamicCounters::TCounterPtr ProcessingCpuTime;
    NMonitoring::TDynamicCounters::TCounterPtr ProcessingErrors;
    NMonitoring::TDynamicCounters::TCounterPtr MinWorkerUptime;
    NMonitoring::TDynamicCounters::TCounterPtr Restarts;

    TTransferCounters(NMonitoring::TDynamicCounterPtr counters, const NKikimrReplication::TReplicationLocationConfig& location)
        : TTragetWithStreamCounters()
    {
        CountersGroup = counters
                            ->GetSubgroup("counters", "transfer")
                            ->GetSubgroup("host", "")
                            ->GetSubgroup("transfer_id", location.GetPath())
                            ->GetSubgroup("database_id", location.GetYdbDatabaseId())
                            ->GetSubgroup("folder_id", location.GetYcFolderId())
                            ->GetSubgroup("cloud_id", location.GetYcCloudId());

        ReadTime = CountersGroup->GetExpiringNamedCounter("name", "transfer.read.duration_milliseconds", true);
        ProcessingTime = CountersGroup->GetExpiringNamedCounter("name", "transfer.process.duration_milliseconds", true);
        WriteTime = CountersGroup->GetExpiringNamedCounter("name", "transfer.write.duration_milliseconds", true);
        DecompressionCpuTime = CountersGroup->GetExpiringNamedCounter("name", "transfer.decompress.cpu_elapsed_microseconds", true);
        ProcessingCpuTime = CountersGroup->GetExpiringNamedCounter("name", "transfer.process.cpu_elapsed_microseconds", true);
        WriteBytes = CountersGroup->GetExpiringNamedCounter("name", "transfer.write_bytes", true);
        WriteRows = CountersGroup->GetExpiringNamedCounter("name", "transfer.write_rows", true);
        ProcessingErrors = CountersGroup->GetExpiringNamedCounter("name", "transfer.processing_errors", true);
        WriteErrors = CountersGroup->GetExpiringNamedCounter("name", "transfer.write_errors", true);
        MinWorkerUptime = CountersGroup->GetExpiringNamedCounter("name", "transfer.worker_uptime_milliseconds_min", false);
        Restarts = CountersGroup->GetExpiringNamedCounter("name", "transfer.worker_restarts", true);
    }

    bool UpdateWithSingleStatsItem(ui64 workerId, ui64 key, i64 value) override {
        if (TTragetWithStreamCounters::UpdateWithSingleStatsItem(workerId, key, value)) {
            return true;
        }

        const auto eKey = static_cast<NKikimrReplication::TWorkerStats::EStatsKeys>(key);
        switch (eKey) {
            case NKikimrReplication::TWorkerStats::PROCESSING_ELAPSED_CPU:
                ProcessingCpuTime->Add(value);
                break;

            case NKikimrReplication::TWorkerStats::PROCESSING_TIME:
                ProcessingTime->Add(value);
                break;

            case NKikimrReplication::TWorkerStats::PROCESSING_ERRORS:
                ProcessingErrors->Add(value);
                break;

            default:
                return false;
        }
        return true;
    }
};

TTargetTransfer::TTargetTransfer(TReplication* replication, ui64 id, const IConfig::TPtr& config)
    : TTargetWithStream(replication, ETargetKind::Transfer, id, config)
{
    Stats.reset(new TTransferStats(Now()));
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
    auto* ptr = dynamic_cast<TTransferStats*>(Stats.get());
    if (ptr) {
        ptr->WorkersStats.erase(id);
    }
}

void TTargetTransfer::EnsureCounters() {
    auto metricsValEnumVal = static_cast<TMetricsConfig::EMetricsLevel>(MetricsLevel);
    switch (metricsValEnumVal) {
        case TMetricsConfig::LEVEL_OBJECT:
        case TMetricsConfig::LEVEL_DETAILED:
            if (!Counters) {
                Counters.reset(new TTransferCounters(AppData()->Counters, Location));
            }
            break;
        default:
            Counters.reset();
            break;
    }
}

void TTargetTransfer::WorkerStatusChanged(ui64 workerId, ui64 status) {
    EnsureCounters();
    auto* statsPtr = dynamic_cast<TTransferStats*>(Stats.get());
    auto* countersPtr = dynamic_cast<TTransferCounters*>(Counters.get());
    if (statsPtr) {
        auto& workerStats = statsPtr->WorkersStats[workerId];
        switch (static_cast<NKikimrReplication::TEvWorkerStatus::EStatus>(status)) {
            case NKikimrReplication::TEvWorkerStatus::STATUS_RUNNING:
                if (countersPtr) {
                    countersPtr->Restarts->Add(1);
                }
                workerStats.RestartsCount += 1;
                workerStats.Restarts.Hour.Update(1, Now());
                workerStats.Restarts.Minute.Update(1, Now());
                workerStats.StartTime = Now();

                break;
            case NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED:
                workerStats.StartTime = TInstant::Zero();
                statsPtr->LastWorkerStartTime = TInstant::Zero();
                break;
            default:
                break;
        }
    }

    if (countersPtr) {
        countersPtr->MinWorkerUptime->Set(0);
    }
}

void TTargetTransfer::UpdateStats(ui64 workerId, const NKikimrReplication::TWorkerStats& newStats) {
    TBase::UpdateStats(workerId, newStats);

    EnsureCounters();
    auto* statsPtr = dynamic_cast<TTransferStats*>(Stats.get());
    auto* countersPtr = dynamic_cast<TTransferCounters*>(Counters.get());
    Y_ABORT_UNLESS(statsPtr);

    auto& workerStats = statsPtr->WorkersStats[workerId];
    if (newStats.HasStartTime()) {
        workerStats.StartTime = TInstant::Seconds(newStats.GetStartTime().seconds());
    }
    bool allWorkersWithStartTime = true;
    if (statsPtr->LastWorkerStartTime == TInstant::Zero()) {
        for (const auto& [_, workerStats] : statsPtr->WorkersStats) {
            if (workerStats.StartTime == TInstant::Zero()) {
                allWorkersWithStartTime = false;
                break;
            }
        }
    }
    if (allWorkersWithStartTime) {
        statsPtr->LastWorkerStartTime = std::max(workerStats.StartTime, statsPtr->LastWorkerStartTime);
    }

    if (countersPtr) {
        if (statsPtr->LastWorkerStartTime) {
            countersPtr->MinWorkerUptime->Set((Now() - statsPtr->LastWorkerStartTime).MilliSeconds());
        } else {
            countersPtr->MinWorkerUptime->Set(0);
        }
    }
}


} // namespace NKikimr::NReplication::NController
