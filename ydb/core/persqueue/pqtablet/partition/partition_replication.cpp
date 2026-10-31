#include "partition.h"

namespace NKikimr::NPQ {

void TPartition::Handle(TEvPQ::TEvReplicationData::TPtr& ev, const TActorContext& ctx) {
    if (PendingReplicationData) {
        return;
    }

    PendingReplicationData.emplace(std::move(ev));
    UpdatePendingUsersFromReplicationData();
    if (InitDone) {
        ProcessTxsAndUserActs(ctx);
    }
}

void TPartition::UpdatePendingUsersFromReplicationData() {
    if (!PendingReplicationData || KVWriteInProgress || !InitDone) {
        return;
    }

    const auto& config = ChangeConfig ? ChangeConfig->Config : Config;
    for (const auto& data : (*PendingReplicationData)->Get()->Data) {
        auto& startOffset = CompactionBlobEncoder.IsEmpty() ? BlobEncoder.StartOffset : CompactionBlobEncoder.StartOffset;
        startOffset = Max(GetStartOffset(), data.GetStartOffset());
        BlobEncoder.EndOffset = Max(GetEndOffset(), data.GetEndOffset());
        for (const auto& [consumer, offset] : data.GetOffsetsByConsumers()) {
            if (!GetConsumer(config, consumer) || offset > static_cast<ui64>(Max<i64>())) {
                continue;
            }
            auto& userInfo = GetOrCreatePendingUser(consumer);
            if (const auto* committed = UsersInfoStorage->GetIfExists(consumer)) {
                userInfo.Offset = Max(userInfo.Offset, committed->Offset);
            }
            userInfo.Offset = Max(userInfo.Offset, static_cast<i64>(offset));
        }
    }
}

void TPartition::CompleteReplicationDataWrite(const TActorContext& ctx) {
    if (!PendingReplicationData) {
        return;
    }

    // Replication may replay older snapshots; acknowledge the greatest valid offset.
    THashMap<TString, ui64> expectedOffsets;
    for (const auto& data : (*PendingReplicationData)->Get()->Data) {
        for (const auto& [consumer, offset] : data.GetOffsetsByConsumers()) {
            if (!GetConsumer(Config, consumer) || offset > static_cast<ui64>(Max<i64>())) {
                continue;
            }
            auto& expected = expectedOffsets[consumer];
            expected = Max(expected, offset);
        }
    }
    for (const auto& [consumer, offset] : expectedOffsets) {
        const auto* userInfo = UsersInfoStorage->GetIfExists(consumer);
        if (!userInfo || userInfo->Offset < 0 || static_cast<ui64>(userInfo->Offset) < offset) {
            UpdatePendingUsersFromReplicationData();
            return; // OnHandleWriteResponse starts the next persist cycle.
        }
    }

    const auto& event = *PendingReplicationData;
    ctx.Send(event->Sender, new TEvPQ::TEvReplicationDataAck(), 0, event->Cookie);
    PendingReplicationData.reset();
}

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
    replicationData.SetSourceTopicIsClosedForReadWrites(IsWriteAndReadDisabled(Config, *AppData(ctx)));
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
