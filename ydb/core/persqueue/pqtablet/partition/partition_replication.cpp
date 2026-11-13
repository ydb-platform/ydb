#include "partition.h"

#include <ydb/core/persqueue/public/utils.h>

namespace NKikimr::NPQ {

void TPartition::Handle(TEvPQ::TEvReplicationData::TPtr& ev, const TActorContext& ctx) {
    if (AppData(ctx)->FeatureFlags.GetEnableDstTopicReplicationDisabled()) {
        LOG_W("Replication is disabled for topic");

        DestroyMirrorer();

        return;
    }
    if (!Mirrorer || ev->Sender != Mirrorer) {
        LOG_D("Ignoring replication data from a retired mirrorer");
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
        for (const auto& [consumer, offset] : ev->Get()->VersionedOffsetsByConsumers) {
            auto it = pending.VersionedOffsetsByConsumers.find(consumer);
            if (it == pending.VersionedOffsetsByConsumers.end()) {
                pending.VersionedOffsetsByConsumers.emplace(consumer, offset);
                continue;
            }
            const auto comparison = CompareConsumerOffsetVersions(offset, it->second);
            if (comparison > 0) {
                it->second = offset;
            } else if (comparison == 0 && offset.GetOffset() != it->second.GetOffset()) {
                LOG_E("Conflicting offsets with the same source revision",
                    {"consumer", consumer},
                    {"readRuleGeneration", offset.GetReadRuleGeneration()},
                    {"offsetRevision", offset.GetOffsetRevision()},
                    {"pendingOffset", it->second.GetOffset()},
                    {"incomingOffset", offset.GetOffset()});
            }
        }
        if (const auto& mirroredOffset = ev->Get()->MirroredOffset; mirroredOffset.Defined()) {
            pending.MirroredOffset = pending.MirroredOffset.Defined()
                ? Max(*pending.MirroredOffset, *mirroredOffset) : *mirroredOffset;
        }
    } else {
        PendingReplicationData.emplace(std::move(ev));
    }

    ReplicationDataNeedsApply = true;
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

    const bool versioned = VersionedReplicationEnabled(config);
    const auto& versionedOffsets = (*PendingReplicationData)->Get()->VersionedOffsetsByConsumers;
    for (const auto& [consumer, offset] : (*PendingReplicationData)->Get()->OffsetsByConsumers) {
        if (versioned) {
            if (!versionedOffsets.contains(consumer)) {
                LOG_W("Source consumer has no versioned offset in opt-in mode; ignoring legacy offset",
                    {"consumer", consumer}, {"offset", offset});
            }
            continue;
        }
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

    if (!versioned) {
        return;
    }
    bool applied = false;
    for (const auto& [consumer, offset] : versionedOffsets) {
        const auto* consumerConfig = GetConsumer(config, consumer);
        if (!consumerConfig || offset.GetOffset() > static_cast<ui64>(Max<i64>())) {
            continue;
        }
        if (consumerConfig->GetType() == NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_REPLICATION) {
            continue;
        }
        if (consumerConfig->GetType() == NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP) {
            LOG_W("Versioned offset mirroring is not supported for MLP consumers",
                {"consumer", consumer});
            continue;
        }
        if (offset.GetOffset() < GetStartOffset() || offset.GetOffset() > GetEndOffset()) {
            LOG_W("Versioned source offset is outside the retained destination range; ignoring it",
                {"consumer", consumer},
                {"offset", offset.GetOffset()},
                {"startOffset", GetStartOffset()},
                {"endOffset", GetEndOffset()});
            continue;
        }

        const auto* pending = GetPendingUserIfExists(consumer);
        const auto* committed = UsersInfoStorage->GetIfExists(consumer);
        const auto* accepted = pending
            ? &pending->ReplicatedOffset
            : committed ? &committed->ReplicatedOffset : nullptr;
        if (accepted && *accepted) {
            const auto& replicated = **accepted;
            const auto comparison = CompareConsumerOffsetVersions(offset, replicated);
            if (comparison < 0) {
                continue;
            }
            if (comparison == 0) {
                if (offset.GetOffset() != replicated.GetOffset()) {
                    LOG_E("Conflicting offsets with the same accepted source revision",
                        {"consumer", consumer},
                        {"readRuleGeneration", offset.GetReadRuleGeneration()},
                        {"offsetRevision", offset.GetOffsetRevision()},
                        {"acceptedOffset", replicated.GetOffset()},
                        {"incomingOffset", offset.GetOffset()});
                }
                continue;
            }
        }

        auto& userInfo = GetOrCreatePendingUser(consumer);
        userInfo.Offset = offset.GetOffset();
        userInfo.ReplicatedOffset = offset;
        applied = true;
    }
    if (!applied && !mirroredOffset.Defined()) {
        PendingReplicationData.reset();
    }
}

void TPartition::CompleteReplicationDataWrite(const TActorContext&) {
    if (!PendingReplicationData || ReplicationDataNeedsApply) {
        return; // OnHandleWriteResponse starts the next persist cycle for new arrivals.
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
    auto addUserInfo = [&](const TUserInfo& info) {
        (*replicationData.MutableOffsetsByConsumers())[info.User] = info.Offset;
        auto& versioned = (*replicationData.MutableVersionedOffsetsByConsumers())[info.User];
        versioned.SetOffset(info.Offset);
        versioned.SetReadRuleGeneration(info.ReadRuleGeneration);
        versioned.SetOffsetRevision(info.OffsetRevision);
    };
    if (userInfos.empty()) {
        for (const auto& [consumer, info] : UsersInfoStorage->ViewAll()) {
            addUserInfo(info);
        }
    }

    for (const auto* userInfo : userInfos) {
        addUserInfo(*userInfo);
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
