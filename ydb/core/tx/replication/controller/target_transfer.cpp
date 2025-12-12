#include "stream_consumer_remover.h"

#include <ydb/core/base/path.h>
#include <ydb/core/protos/replication.pb.h>
#include "target_transfer.h"
#include <ydb/core/tx/replication/service/service.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/public/api/protos/draft/ydb_replication.pb.h>

namespace NKikimr::NReplication::NController {

TTargetTransfer::TTargetTransfer(TReplication* replication, ui64 id, const IConfig::TPtr& config)
    : TTargetWithStream(replication, ETargetKind::Transfer, id, config)
    , Stats(std::make_unique<TTransferStats>(Now()))
{
}

void TTargetTransfer::UpdateConfig(const NKikimrReplication::TReplicationConfig& cfg) {
    auto& t = cfg.GetTransferSpecific().GetTarget();
    Config = std::make_shared<TTargetTransfer::TTransferConfig>(
        GetConfig()->GetSrcPath(),
        GetConfig()->GetDstPath(),
        t.GetTransformLambda(),
        cfg.GetTransferSpecific().GetRunAsUser(),
        t.GetDirectoryPath());
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

ui64 TTargetTransfer::TTransferConfig::GetCountersLevel() const {
    return CountersLevel;
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

void TTargetTransfer::UpdateStats(ui64 workerId, const NKikimrReplication::TWorkerStats& stats, NMonitoring::TDynamicCounterPtr counters) {
    if (!HasWorker(workerId)) {
        Stats->WorkersStats.erase(workerId);
        return;
    }
    if (GetConfig()->GetCountersLevel() > 2) {
        if (counters && !DynamicCounters) {
            DynamicCounters = counters->GetSubgroup("counters", "transfer")->GetSubgroup("host", "")->GetSubgroup("transfer_id", GetDstPathId().ToString());
        }
        if (DynamicCounters) {
            //ToDo:: populate counters.
        }
    }
    auto& workerStats = Stats->WorkersStats[workerId];
    for (const auto& item : stats.GetValues()) {
        const auto key = static_cast<NKikimrReplication::TWorkerStats::EStatsKeys>(item.GetKey());
        const auto value = item.GetValue();
        switch(key) {
            ADD_STATS_VAL(READ_TIME, ReadTime);
            ADD_STATS_VAL(READ_ELAPSED_CPU, ReadCpu);
            ADD_STATS_VAL(DECOMPRESS_TIME, DecompressTime);
            ADD_STATS_VAL(DECOMPRESS_ELAPSED_CPU, DecompressCpu);
            ADD_STATS_VAL(PROCESSING_TIME, ProcessingTime);
            ADD_STATS_VAL(PROCESSING_ELAPSED_CPU, ProcessingCpu);
            ADD_STATS_VAL(WRITE_TIME, WriteTime);
            ADD_STATS_VAL(WRITE_ELAPSED_CPU, WriteCpu);

            ADD_STATS_TO_WINDOW(READ_BYTES, ReadBytes);
            ADD_STATS_TO_WINDOW(READ_MESSAGES, ReadMessages);
            ADD_STATS_TO_WINDOW(WRITE_BYTES, WriteBytes);
            ADD_STATS_TO_WINDOW(WRITE_ROWS, WriteRows);

            case NKikimrReplication::TWorkerStats::READ_PARTITION:
                workerStats.Partition = value;
                break;
            case NKikimrReplication::TWorkerStats::READ_OFFSET:
                workerStats.ReadOffset = value;
                break;

            case NKikimrReplication::TWorkerStats::WORK_OPERATION:
                workerStats.Operation = static_cast<NKikimrReplication::EWorkOperation>(value);
                break;
            default:
                //ToDo: !! logging or error
                break;
        };
        Y_UNUSED(value);
    }
}
#undef ADD_STATS_VAL
#undef ADD_STATS_TO_WINDOW

const TReplication::ITargetStats* TTargetTransfer::GetStats() const {
    return Stats.get();
}

#define SET_PROTO_WINDOW(SRC_NAME, PROTO_NAME)                                  \
    dstStats.mutable_##PROTO_NAME()->set_per_minute(SRC_NAME.Minute.GetValue()); \
    dstStats.mutable_##PROTO_NAME()->set_per_hour(SRC_NAME.Hour.GetValue());

void TTransferStats::FillToProto(NKikimrReplication::TEvDescribeReplicationResult& destination, bool includeDetailed) const {
    auto& dstStats = *destination.MutableStats()->MutableTransfer();
    //dstStats.set_read_time(ReadTime);
    //dstStats.SetReadCpu(ReadCpu);
    SET_PROTO_WINDOW(ReadBytes, read_bytes);
    SET_PROTO_WINDOW(ReadMessages, read_messages);
    SET_PROTO_WINDOW(WriteBytes, write_bytes);
    SET_PROTO_WINDOW(WriteRows, write_rows);
    SET_PROTO_WINDOW(DecompressionCpuTime, decompression_cpu_time_ms);
    SET_PROTO_WINDOW(ProcessingCpuTime, processing_cpu_time_ms);
    dstStats.mutable_min_worker_uptime()->set_seconds(0); //ToDo: !! Calculate
    dstStats.mutable_stats_collection_start()->set_seconds(StartTime.Seconds());

    if (includeDetailed) {
        //ToDo: !! Fill detailed stats
    }
}
}
