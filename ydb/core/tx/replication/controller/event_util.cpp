#include "event_util.h"
#include "target_with_stream.h"

namespace NKikimr::NReplication::NController {

THolder<TEvService::TEvRunWorker> MakeTEvRunWorker(const TReplication::TPtr replication, const TReplication::ITarget& target, ui64 partitionId) {
    return MakeTEvRunWorker(replication->GetId(), target.GetId(), partitionId, replication->GetConfig().GetSrcConnectionParams(), target.GetStreamPath(), target.GetDstPathId());
}

THolder<TEvService::TEvRunWorker> MakeTEvRunWorker(ui64 replicationId, ui64 targetId, ui64 partitionId, const NKikimrReplication::TConnectionParams& connectionParams,
        const TString& srcStreamPath, const TPathId& dstPathId)
{
    auto ev = MakeHolder<TEvService::TEvRunWorker>();
    auto& record = ev->Record;

    auto& worker = *record.MutableWorker();
    worker.SetReplicationId(replicationId);
    worker.SetTargetId(targetId);
    worker.SetWorkerId(partitionId);

    auto& readerSettings = *record.MutableCommand()->MutableRemoteTopicReader();
    readerSettings.MutableConnectionParams()->CopyFrom(connectionParams);
    readerSettings.SetTopicPath(srcStreamPath);
    readerSettings.SetTopicPartitionId(partitionId);
    readerSettings.SetConsumerName(ReplicationConsumerName);

    auto& writerSettings = *record.MutableCommand()->MutableLocalTableWriter();
    PathIdFromPathId(dstPathId, writerSettings.MutablePathId());

    return ev;
}

}
