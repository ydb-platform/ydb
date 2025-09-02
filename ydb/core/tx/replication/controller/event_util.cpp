#include "event_util.h"
#include "target_transfer.h"

namespace NKikimr::NReplication::NController {

THolder<TEvService::TEvRunWorker> MakeRunWorkerEv(
        const TReplication::TPtr replication,
        const TReplication::ITarget& target,
        ui64 workerId)
{
    return MakeRunWorkerEv(
        replication->GetId(),
        target.GetId(),
        target.GetConfig(),
        workerId,
        replication->GetConfig().GetSrcConnectionParams(),
        replication->GetConfig().GetConsistencySettings(),
        target.GetStreamPath(),
        target.GetStreamConsumerName(),
        target.GetDstPathId(),
        replication->GetConfig().GetTransferSpecific().GetBatching(),
        replication->GetDatabase());
}

THolder<TEvService::TEvRunWorker> MakeRunWorkerEv(
        ui64 replicationId,
        ui64 targetId,
        const TReplication::ITarget::IConfig::TPtr& config,
        ui64 workerId,
        const NKikimrReplication::TConnectionParams& connectionParams,
        const NKikimrReplication::TConsistencySettings& consistencySettings,
        const TString& srcStreamPath,
        const TString& srcStreamConsumerName,
        const TPathId& dstPathId,
        const NKikimrReplication::TBatchingSettings& batchingSettings,
        const TString& database)
{
    auto ev = MakeHolder<TEvService::TEvRunWorker>();
    auto& record = ev->Record;

    auto& worker = *record.MutableWorker();
    worker.SetReplicationId(replicationId);
    worker.SetTargetId(targetId);
    worker.SetWorkerId(workerId);

    record.MutableCommand()->SetDatabase(database);

    auto& readerSettings = *record.MutableCommand()->MutableRemoteTopicReader();
    readerSettings.MutableConnectionParams()->CopyFrom(connectionParams);
    readerSettings.SetTopicPath(srcStreamPath);
    readerSettings.SetTopicPartitionId(workerId);
    readerSettings.SetConsumerName(srcStreamConsumerName);

    switch(config->GetKind()) {
        case TReplication::ETargetKind::Table:
        case TReplication::ETargetKind::IndexTable: {
            auto& writerSettings = *record.MutableCommand()->MutableLocalTableWriter();
            dstPathId.ToProto(writerSettings.MutablePathId());
            break;
        }
        case TReplication::ETargetKind::Transfer: {
            auto p = std::dynamic_pointer_cast<const TTargetTransfer::TTransferConfig>(config);
            auto& writerSettings = *record.MutableCommand()->MutableTransferWriter();
            dstPathId.ToProto(writerSettings.MutablePathId());
            writerSettings.SetTransformLambda(p->GetTransformLambda());
            writerSettings.MutableBatching()->CopyFrom(batchingSettings);
            writerSettings.SetRunAsUser(p->GetRunAsUser());
            writerSettings.SetDirectoryPath(p->GetDirectoryPath());
            break;
        }
    }

    record.MutableCommand()->MutableConsistencySettings()->CopyFrom(consistencySettings);

    return ev;
}

}
