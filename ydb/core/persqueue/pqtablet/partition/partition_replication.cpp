#include "partition.h"

namespace NKikimr::NPQ {

void TPartition::Handle(TEvPQ::TEvReplicationData::TPtr& ev, const TActorContext& ctx) {
    if (AppData(ctx)->FeatureFlags.GetEnableDstTopicReplicationDisabled()) {
        LOG_W("Replication is disabled for topic");

        DestroyMirrorer();

        return;
    }

    if (PendingReplicationData) {
        auto& pending = *(*PendingReplicationData)->Get();
        for (const auto& [consumer, offset] : ev->Get()->OffsetsByConsumers) {
            if (offset > static_cast<ui64>(Max<i64>())) {
                continue;
            }
            auto& pendingOffset = pending.OffsetsByConsumers[consumer];
            // An invalid offset in the earlier payload must not hide a valid update.
            pendingOffset = pendingOffset > static_cast<ui64>(Max<i64>()) ? offset : Max(pendingOffset, offset);
        }
        if (const auto& mirroredOffset = ev->Get()->MirroredOffset; mirroredOffset.Defined()) {
            pending.MirroredOffset = pending.MirroredOffset.Defined()
                ? Max(*pending.MirroredOffset, *mirroredOffset) : *mirroredOffset;
        }
    } else {
        PendingReplicationData.emplace(std::move(ev));
    }

    ReplicationDataNeedsApply = true;
    UpdatePendingUsersFromReplicationData();
    if (InitDone) {
        ProcessTxsAndUserActs(ctx);
    }
}

void TPartition::UpdatePendingUsersFromReplicationData() {
    if (!PendingReplicationData || KVWriteInProgress || !InitDone) {
        return;
    }

    ReplicationDataNeedsApply = false;
    const auto& config = ChangeConfig ? ChangeConfig->Config : Config;
    const auto mirroredOffset = (*PendingReplicationData)->Get()->MirroredOffset;
    if (mirroredOffset.Defined()) {
        BlobEncoder.MirroredOffset = Max(BlobEncoder.MirroredOffset, *mirroredOffset);
    }

    for (const auto& [consumer, offset] : (*PendingReplicationData)->Get()->OffsetsByConsumers) {
        auto* consumerConfig = GetConsumer(config, consumer);
        if (!consumerConfig || offset > static_cast<ui64>(Max<i64>())) {
            continue;
        }

        if (consumerConfig->GetType() == NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_REPLICATION) {
            continue;
        } else if (consumerConfig->GetType() == NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP) {
            if (auto it = MLPConsumers.find(consumer); it != MLPConsumers.end()) {
                auto request = MakeHolder<TEvPQ::TEvMLPPurgeRequest>(
                    TopicName(), consumer, Partition.OriginalPartitionId);
                request->Record.SetToOffset(offset);
                Send(it->second.ActorId, request.Release());
            }
            continue;
        }

        auto& userInfo = GetOrCreatePendingUser(consumer);
        if (const auto* committed = UsersInfoStorage->GetIfExists(consumer)) {
            userInfo.Offset = Max(userInfo.Offset, committed->Offset);
        }
        userInfo.Offset = Max(userInfo.Offset, static_cast<i64>(offset));
    }
}

void TPartition::CompleteReplicationDataWrite(const TActorContext&) {
    if (!PendingReplicationData) {
        return;
    }

    // Updates merged during the KV write have not been applied yet. In
    // particular, MLP offsets are not checked by expectedOffsets below.
    if (ReplicationDataNeedsApply) {
        UpdatePendingUsersFromReplicationData();
        return; // OnHandleWriteResponse starts the next persist cycle.
    }

    const auto mirroredOffset = (*PendingReplicationData)->Get()->MirroredOffset;
    // MirroredOffset is not updated while a KV write is in progress, so it still
    // reflects the completed write here, before retrying the pending payload.
    if (mirroredOffset.Defined() && BlobEncoder.MirroredOffset < *mirroredOffset) {
        UpdatePendingUsersFromReplicationData();
        return; // OnHandleWriteResponse starts the next persist cycle.
    }

    const auto& config = ChangeConfig ? ChangeConfig->Config : Config;

    // Replication may replay older snapshots; acknowledge the greatest valid offset.
    y_absl::flat_hash_map<TString, ui64> expectedOffsets;
    for (const auto& [consumer, offset] : (*PendingReplicationData)->Get()->OffsetsByConsumers) {
        const auto* consumerConfig = GetConsumer(config, consumer);
        // Replication consumers are excluded from applying incoming offsets above.
        // MLP progress is applied by TEvMLPPurgeRequest, not by the streaming commit offset.
        if (!consumerConfig
            || consumerConfig->GetType() == NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_REPLICATION
            || consumerConfig->GetType() == NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP
            || offset > static_cast<ui64>(Max<i64>())) {
            continue;
        }
        auto& expected = expectedOffsets[consumer];
        expected = Max(expected, offset);
    }
    for (const auto& [consumer, offset] : expectedOffsets) {
        const auto* userInfo = UsersInfoStorage->GetIfExists(consumer);
        if (!userInfo || userInfo->Offset < 0 || static_cast<ui64>(userInfo->Offset) < offset) {
            UpdatePendingUsersFromReplicationData();
            return; // OnHandleWriteResponse starts the next persist cycle.
        }
    }

    PendingReplicationData.reset();
}

void TPartition::RestoreReadSessionActor(TUserInfo& userInfo, const TString& sessionId,
                                       const TActorId& readSessionActor, const TActorContext& ctx) {
    if (userInfo.ReadSessionActor || !readSessionActor || userInfo.Session != sessionId) {
        return;
    }

    // A session change may already be waiting for its KV write to complete.
    if (auto* pending = GetPendingUserIfExists(userInfo.User)) {
        if (pending->Session != sessionId || pending->Generation != userInfo.Generation
            || pending->Step != userInfo.Step
            || (pending->ReadSessionActor && pending->ReadSessionActor != readSessionActor)) {
            return;
        }
        pending->ReadSessionActor = readSessionActor;
    }
    userInfo.ReadSessionActor = readSessionActor;

    LOG_D("Restored read session actor",
        {"consumer", userInfo.User},
        {"readSessionActor", readSessionActor});

    if (userInfo.Type == NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_REPLICATION) {
        SendOffsetsToReplicationConsumer(ctx);
    }
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
    const TString payload = replicationData.SerializeAsString();
    for (const auto& [consumer, info] : UsersInfoStorage->ViewAll()) {
        if (info.Type != NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_REPLICATION
            || info.Session.empty() || !info.ReadSessionActor) {
            continue;
        }
        auto event = MakeHolder<TEvPersQueue::TEvReplicationData>();
        event->Record.SetPartitionId(Partition.OriginalPartitionId);
        event->Record.SetSession(info.Session);
        event->Record.SetGeneration(info.Generation);
        event->Record.SetStep(info.Step);
        event->Record.SetPayload(payload);
        event->Record.SetPartitionSessionId(info.PartitionSessionId);
        ctx.Send(info.ReadSessionActor, event.Release());
    }
}

} // namespace NKikimr::NPQ
