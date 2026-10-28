#include "partition.h"

namespace NKikimr::NPQ {

void TPartition::SendOffsetsToReplicationConsumer(const TActorContext& ctx, TConstArrayRef<const TUserInfo*> userInfos) {
    NKikimrPQ::TReplicationDataPayload replicationData;
    if (userInfos.empty()) {
        for (const auto& [consumer, info] : UsersInfoStorage->ViewAll()) {
            (*replicationData.MutableOffsetsByConsumers())[consumer] = info.Offset;
        }
    }

    for (const auto* userInfo : userInfos) {
        (*replicationData.MutableOffsetsByConsumers())[userInfo->User] = userInfo->Offset;
    }
    replicationData.SetStartOffset(GetStartOffset());
    replicationData.SetEndOffset(GetEndOffset());
    replicationData.SetSourceTopicIsClosedForReadWrites(Config.GetWriteAndReadDisabled());
    const TString payload = replicationData.SerializeAsString();
    for (const auto& [consumer, info] : UsersInfoStorage->ViewAll()) {
        if (info.Type != NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_REPLICATION
            || info.Session.empty() || !info.PipeClient) {
            continue;
        }
        auto event = MakeHolder<TEvPersQueue::TEvReplicationData>();
        event->Record.SetPartitionId(Partition.OriginalPartitionId);
        event->Record.SetSession(info.Session);
        event->Record.SetGeneration(info.Generation);
        event->Record.SetStep(info.Step);
        event->Record.SetPayload(payload);
        event->Record.SetPartitionSessionId(info.PartitionSessionId);
        ActorIdToProto(info.PipeClient, event->Record.MutablePipeClient());
        ctx.Send(TabletActorId, event.Release());
    }
}

} // namespace NKikimr::NPQ
