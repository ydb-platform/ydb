#include "event_util.h"
#include "target_with_stream.h"

namespace NKikimr::NReplication::NController {

THolder<TEvService::TEvRunWorker> MakeRunWorkerEv(
        const TReplication::TPtr replication,
        const TReplication::ITarget& target,
        ui64 workerId)
{
    return MakeRunWorkerEv(
        replication->GetId(),
        target.GetId(),
        workerId,
        replication->GetConfig().GetSrcConnectionParams(),
        replication->GetConfig().GetConsistencySettings(),
        target.GetStreamPath(),
        target.GetDstPathId());
}

THolder<TEvService::TEvRunWorker> MakeRunWorkerEv(
        ui64 replicationId,
        ui64 targetId,
        ui64 workerId,
        const NKikimrReplication::TConnectionParams& connectionParams,
        const NKikimrReplication::TConsistencySettings& consistencySettings,
        const TString& srcStreamPath,
        const TPathId& dstPathId)
{
    auto ev = MakeHolder<TEvService::TEvRunWorker>();
    auto& record = ev->Record;

    auto& worker = *record.MutableWorker();
    worker.SetReplicationId(replicationId);
    worker.SetTargetId(targetId);
    worker.SetWorkerId(workerId);

    auto& readerSettings = *record.MutableCommand()->MutableRemoteTopicReader();
    readerSettings.MutableConnectionParams()->CopyFrom(connectionParams);
    readerSettings.SetTopicPath(srcStreamPath);
    readerSettings.SetTopicPartitionId(workerId);
    readerSettings.SetConsumerName(ReplicationConsumerName);

    auto& writerSettings = *record.MutableCommand()->MutableLocalTableWriter();
    dstPathId.ToProto(writerSettings.MutablePathId());

    record.MutableCommand()->MutableConsistencySettings()->CopyFrom(consistencySettings);

    return ev;
}

}
