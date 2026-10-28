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

    for (const auto& data : (*PendingReplicationData)->Get()->Data) {
        for (const auto& [consumer, offset] : data.GetOffsetsByConsumers()) {
            auto& userInfo = GetOrCreatePendingUser(consumer);
            userInfo.Offset = offset;
        }
    }
}

void TPartition::CompleteReplicationDataWrite(const TActorContext& ctx) {
    if (!PendingReplicationData) {
        return;
    }

    // A consumer may occur in several snapshots; the last offset is authoritative.
    THashMap<TString, ui64> expectedOffsets;
    for (const auto& data : (*PendingReplicationData)->Get()->Data) {
        for (const auto& [consumer, offset] : data.GetOffsetsByConsumers()) {
            expectedOffsets[consumer] = offset;
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
