#include "mlp_storage.h"

#include <ydb/core/persqueue/common/percentiles.h>
#include <ydb/library/actors/core/log.h>

#include <util/string/join.h>

#include <ranges>

namespace NKikimr::NPQ::NMLP {

namespace {

TInstant TrimToSeconds(TInstant time, bool up = true) {
    return TInstant::Seconds(time.Seconds() + (up && time.MilliSecondsOfSecond() > 0 ? 1 : 0));
}

}

TStorage::TStorage(TIntrusivePtr<ITimeProvider> timeProvider, const TStorageSettings& settings)
    : MaxMessages(settings.MaxMessages)
    , MinMessages(settings.MinMessages)
    , MaxFastMessages(MaxMessages - MaxMessages / 4)
    , MaxSlowMessages(MaxMessages / 4)
    , TimeProvider(timeProvider)
    , KeepMessageOrder(settings.KeepMessageOrder)
    , NextVacuumRun(TInstant::Zero())
    , Batch(this)
{
    Y_ASSERT(settings.ParentPartitionId.size() <= 2);
    if (KeepMessageOrder) {
        for (const ui32 parentPartitionId : settings.ParentPartitionId) {
            ParentPartitionExternalLockInfo.push_back(TParentPartitionExternalLockInfo{
                .PartitionId = parentPartitionId,
            });
        }
    }
    BaseDeadline = TrimToSeconds(timeProvider->Now(), false);
    Metrics.MessageLocks.Initialize(MLP_LOCKS_RANGES, std::size(MLP_LOCKS_RANGES), true);
    Metrics.MessageLockingDuration.Initialize(SLOW_LATENCY_RANGES, std::size(SLOW_LATENCY_RANGES), true);
    Metrics.WaitingLockingDuration.Initialize(SLOW_LATENCY_RANGES, std::size(SLOW_LATENCY_RANGES), true);
}

void TStorage::SetMaxMessageProcessingCount(ui32 maxMessageProcessingCount) {
    MaxMessageProcessingCount = maxMessageProcessingCount;
}

void TStorage::SetRetentionPeriod(std::optional<TDuration> retentionPeriod) {
    RetentionPeriod = retentionPeriod;
}

void TStorage::SetDeadLetterPolicy(std::optional<NKikimrPQ::TPQTabletConfig::EDeadLetterPolicy> deadLetterPolicy) {
    DeadLetterPolicy = deadLetterPolicy;

    auto policy = DeadLetterPolicy.value_or(NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_UNSPECIFIED);
    switch (policy) {
        case NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_MOVE:
            break;
        case NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_DELETE:
            for (auto [offset, _] : std::exchange(DLQMessages, {})) {
                size_t c = 0;
                DoCommit(offset, c);
                ++Metrics.TotalDeletedByDeadlinePolicyMessageCount;
            }
            break;
        case NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_UNSPECIFIED:
            WakeUpDLQ();
            break;
    }

}

NKikimrPQ::EReadWithKeepOrder TStorage::ReadWithKeepOrder() const {
    using enum NKikimrPQ::EReadWithKeepOrder;
    NKikimrPQ::EReadWithKeepOrder result = READ_WITH_KEEP_ORDER_ALLOW_ALL;
    for (const auto& parentPartitionExternalLockInfo : ParentPartitionExternalLockInfo) {
        switch (parentPartitionExternalLockInfo.ReadWithKeepOrder) {
            case READ_WITH_KEEP_ORDER_BLOCK_ALL:
            case READ_WITH_KEEP_ORDER_BLACKLIST:
            case READ_WITH_KEEP_ORDER_ALLOW_ALL:
                static_assert(READ_WITH_KEEP_ORDER_BLOCK_ALL < READ_WITH_KEEP_ORDER_BLACKLIST && READ_WITH_KEEP_ORDER_BLACKLIST < READ_WITH_KEEP_ORDER_ALLOW_ALL);
                result = std::min(result, parentPartitionExternalLockInfo.ReadWithKeepOrder);
                break;
            case READ_WITH_KEEP_ORDER_UNSPECIFIED:
                AFL_ENSURE(false)("ReadWithKeepOrder", NKikimrPQ::EReadWithKeepOrder_Name(parentPartitionExternalLockInfo.ReadWithKeepOrder));
                break;
            }
    }
    return result;
}

bool TStorage::GetKeepMessageOrder() const {
    return KeepMessageOrder;
}

bool TStorage::HasUnlockedMessageGroupsId() const {
    if (!KeepMessageOrder) {
        return true;
    }
    if (ReadWithKeepOrder() == NKikimrPQ::EReadWithKeepOrder::READ_WITH_KEEP_ORDER_BLOCK_ALL) {
        return false;
    }
    if (MessageGroups.UnlockedMessageGroupsId.size() > 0) {
        return true;
    }
    return MessageGroups.UnorderedOffsets.size() > 0;
}

std::optional<ui32> TStorage::GetRetentionDeadlineDelta() const {
    if (RetentionPeriod) {
        auto retentionDeadline = TrimToSeconds(TimeProvider->Now(), false) - RetentionPeriod.value();
        if (retentionDeadline >= BaseWriteTimestamp) {
            return (retentionDeadline - BaseWriteTimestamp).Seconds();
        }
    }

    return std::nullopt;
}

bool TStorage::CanReadMessageGroupIdHashFromParentPartition(const ui32 messageGroupIdHash) const {
    if (!KeepMessageOrder) {
        return true;
    }
    using enum NKikimrPQ::EReadWithKeepOrder;
    if (auto order = ReadWithKeepOrder(); EqualToOneOf(order, READ_WITH_KEEP_ORDER_BLOCK_ALL, READ_WITH_KEEP_ORDER_ALLOW_ALL)) {
        return order == READ_WITH_KEEP_ORDER_ALLOW_ALL;
    }
    for (const auto& parentPartitionExternalLockInfo : ParentPartitionExternalLockInfo) {
        switch (parentPartitionExternalLockInfo.ReadWithKeepOrder) {
            case READ_WITH_KEEP_ORDER_BLOCK_ALL:
                Y_ASSERT(false && "checked before");
                return false;
            case READ_WITH_KEEP_ORDER_BLACKLIST:
                if (parentPartitionExternalLockInfo.LockedMessageGroupsIdSet.contains(messageGroupIdHash)) {
                    return false;
                }
                break;
            case READ_WITH_KEEP_ORDER_ALLOW_ALL:
                break;
            case READ_WITH_KEEP_ORDER_UNSPECIFIED:
                AFL_ENSURE(false)("ReadWithKeepOrder", NKikimrPQ::EReadWithKeepOrder_Name(parentPartitionExternalLockInfo.ReadWithKeepOrder));
                break;
        }
    }
    return true;
}

bool TStorage::CanReadMessageGroupIdHash(const ui32 messageGroupIdHash) const {
    if (!KeepMessageOrder) {
        return true;
    }
    return MessageGroups.UnlockedMessageGroupsId.contains(messageGroupIdHash);
}

std::optional<TReadMessage> TStorage::Next(TInstant deadline, TPosition& position, const absl::flat_hash_set<ui32>& skipMessageGroups) {
    std::optional<ui64> retentionDeadlineDelta = GetRetentionDeadlineDelta();

    if (!position.SlowPosition) {
        position.SlowPosition = SlowMessages.begin();
    }

    auto retentionExpired = [&](const auto& message) {
        return retentionDeadlineDelta && message.WriteTimestampDelta <= retentionDeadlineDelta.value();
    };

    auto asResult = [&](auto offset, auto& message) {
        return TReadMessage{
            .Offset = offset,
            .ApproximateReceiveCount = message.ProcessingCount,
            .ApproximateFirstReceiveTimestamp = TimeProvider->Now(), // TODO: replace with persisted first-receive timestamp
        };
    };

    auto isMessageGroupLocked = [&](TMessage& message) {
        return message.HasMessageGroupId && (!CanReadMessageGroupIdHash(message.MessageGroupIdHash) || skipMessageGroups.contains(message.MessageGroupIdHash));
    };

    for(; position.SlowPosition != SlowMessages.end(); ++position.SlowPosition.value()) {
        auto offset = position.SlowPosition.value()->first;
        auto& message = position.SlowPosition.value()->second;
        if (message.GetStatus() == EMessageStatus::Unprocessed) {
            if (retentionExpired(message)) {
                continue;
            }

            if (isMessageGroupLocked(message)) {
                continue;
            }

            DoLock(offset, message, deadline);
            return asResult(offset, message);
        }
    }

    bool moveUnlockedOffset = position.FastPosition <= FirstUnlockedOffset;
    for (size_t i = std::max(position.FastPosition, FirstUnlockedOffset) - FirstOffset; i < Messages.size(); ++i) {
        auto& message = Messages[i];
        if (message.GetStatus() == EMessageStatus::Unprocessed) {
            if (retentionExpired(message)) {
                if (moveUnlockedOffset) {
                    ++FirstUnlockedOffset;
                }
                continue;
            }

            if (isMessageGroupLocked(message)) {
                moveUnlockedOffset = false;
                continue;
            }

            if (moveUnlockedOffset) {
                ++FirstUnlockedOffset;
            }

            ui64 offset = FirstOffset + i;
            position.FastPosition = offset + 1;

            DoLock(offset, message, deadline);
            return asResult(offset, message);
        } else if (moveUnlockedOffset) {
            ++FirstUnlockedOffset;
        }
    }

    position.FastPosition = FirstOffset + Messages.size();

    return std::nullopt;
}

bool TStorage::Commit(ui64 messageId) {
    return DoCommit(messageId, Metrics.TotalCommittedMessageCount);
}

bool TStorage::Unlock(ui64 messageId) {
    return DoUnlock(messageId);
}

bool TStorage::ChangeMessageDeadline(ui64 messageId, TInstant deadline) {
    auto [message, _] = GetMessageInt(messageId);
    if (!message) {
        return false;
    }

    switch (message->GetStatus()) {
        case EMessageStatus::Locked:
        case EMessageStatus::Delayed: {
            Batch.AddChange(messageId);

            auto newDeadlineDelta = NormalizeDeadline(deadline);
            message->DeadlineDelta = newDeadlineDelta;

            return true;
        }
        default:
            return false;
    }
}

bool TStorage::Purge(ui64 endOffset) {
    Metrics.TotalPurgedMessageCount += Metrics.UnprocessedMessageCount + Metrics.LockedMessageCount
        + Metrics.DelayedMessageCount + Metrics.DLQMessageCount
        + (endOffset - GetLastOffset());

    Metrics.InflightMessageCount = 0;
    Metrics.UnprocessedMessageCount = 0;
    Metrics.LockedMessageCount = 0;
    Metrics.LockedMessageGroupCount = 0;
    Metrics.DelayedMessageCount = 0;
    Metrics.CommittedMessageCount = 0;
    Metrics.DeadlineExpiredMessageCount = 0;
    Metrics.DLQMessageCount = 0;

    SlowMessages.clear();
    Messages.clear();
    DLQQueue.clear();
    DLQMessages.clear();
    MessageGroups.Clear();

    FirstOffset = endOffset;
    FirstUncommittedOffset = endOffset;
    FirstUnlockedOffset = endOffset;

    BaseDeadline = TrimToSeconds(TimeProvider->Now(), false);
    BaseWriteTimestamp = TrimToSeconds(TimeProvider->Now(), false);

    NextVacuumRun = TrimToSeconds(TimeProvider->Now(), false) + VACUUM_INTERVAL;

    Batch.SetPurged();

    return true;
}

TStorage::TUpdateExternalLockedMessageGroupsResult TStorage::UpdateExternalLockedMessageGroupsId(const NKikimrPQ::TExternalLockedMessageGroupsId& record) {
    return DoUpdateExternalLockedMessageGroupsId(record, false);
}

TStorage::TUpdateExternalLockedMessageGroupsResult TStorage::DoUpdateExternalLockedMessageGroupsId(const NKikimrPQ::TExternalLockedMessageGroupsId& record, bool loadState) {
    Y_ASSERT(KeepMessageOrder && "UpdateExternalLockedMessageGroupsId called for non-fifo queue");
    const ui32 parentPartitionId = record.GetParentPartitionId();
    TParentPartitionExternalLockInfo* info = FindIfPtr(ParentPartitionExternalLockInfo, [parentPartitionId](const auto& p) { return p.PartitionId == parentPartitionId; });
    if (!info) {
        Y_VERIFY_DEBUG(false, "UpdateExternalLockedMessageGroupsId for unknown partition");
        return TUpdateExternalLockedMessageGroupsResult{
            .Applied = false,
            .Invalid = true,
        };
    }
    const auto updateVersion = std::make_tuple(record.GetGeneration(), record.GetConsumerGeneration(), record.GetStep());
    auto currentVersion = std::tie(info->TabletGeneration, info->ConsumerGeneration, info->ConsumerStep);
    if (updateVersion < currentVersion) {
        Y_VERIFY_DEBUG(!loadState, "%s", (TStringBuilder() << "Loading obsolete state: "
                                                           << "(" << record.GetGeneration() << "," << record.GetConsumerGeneration() << "," << record.GetStep() << ") < "
                                                           << "(" << info->TabletGeneration << "," << info->ConsumerGeneration << "," << info->ConsumerStep << ")")
                                             .c_str());
        return TUpdateExternalLockedMessageGroupsResult{
            .Applied = false,
        };
    }
        if (record.GetMode() == NKikimrPQ::EReadWithKeepOrder::READ_WITH_KEEP_ORDER_UNSPECIFIED) {
        Y_VERIFY_DEBUG(false,
        "Unknown mode; mode: %s, newMode: %s",
        NKikimrPQ::EReadWithKeepOrder_Name(info->ReadWithKeepOrder).c_str(),
        NKikimrPQ::EReadWithKeepOrder_Name(record.GetMode()).c_str());
        return TUpdateExternalLockedMessageGroupsResult{
            .Applied = false,
            .Invalid = true,
        };
    }
    Y_VERIFY_DEBUG(
        info->ReadWithKeepOrder <= record.GetMode(),
        "mode: %s, newMode: %s",
        NKikimrPQ::EReadWithKeepOrder_Name(info->ReadWithKeepOrder).c_str(),
        NKikimrPQ::EReadWithKeepOrder_Name(record.GetMode()).c_str()
    );
    if (info->ReadWithKeepOrder == record.GetMode() && updateVersion == currentVersion) {
        return TUpdateExternalLockedMessageGroupsResult{
            .Applied = false,
        };
    }
    TUpdateExternalLockedMessageGroupsResult result{
        .Applied = true,
    };
    result.VersionChanged = true;
    currentVersion = updateVersion;

    result.ModeChanged = info->ReadWithKeepOrder < record.GetMode();
    if (info->ReadWithKeepOrder <= record.GetMode()) {
        info->ReadWithKeepOrder = record.GetMode();
    }
    if (info->ReadWithKeepOrder == NKikimrPQ::EReadWithKeepOrder::READ_WITH_KEEP_ORDER_BLACKLIST) {
        Y_ASSERT(record.HasFullBlacklist());
    }
    if (info->ReadWithKeepOrder == NKikimrPQ::EReadWithKeepOrder::READ_WITH_KEEP_ORDER_ALLOW_ALL) {
        info->LockedMessageGroupsIdSet.clear(); // free blacklist if any
    }
    if (record.HasFullBlacklist() || info->ReadWithKeepOrder == NKikimrPQ::EReadWithKeepOrder::READ_WITH_KEEP_ORDER_BLACKLIST) {
        const auto& list = record.GetFullBlacklist().GetParentLockedMessageGroupsIdHash();
        absl::flat_hash_set<ui32> lockedMessageGroupsIdSet(list.begin(), list.end());
        info->LockedMessageGroupsIdSet.swap(lockedMessageGroupsIdSet);
        UpdateMessageGroupsParentLocks(info->LockedMessageGroupsIdSet, lockedMessageGroupsIdSet, result.ModeChanged);
    } else if (result.ModeChanged) {
        UpdateMessageGroupsParentLocks(info->LockedMessageGroupsIdSet, info->LockedMessageGroupsIdSet, result.ModeChanged);
    }
    if (!loadState) {
        Batch.SetUpdateExternalLockedMessageGroupsId(parentPartitionId);
    }
    return result;
}

TInstant TStorage::GetMessageDeadline(ui64 messageId) {
    auto [message, _] = GetMessageInt(messageId);
    if (!message) {
        return TInstant::Zero();
    }

    switch (message->GetStatus()) {
        case EMessageStatus::Locked:
        case EMessageStatus::Delayed:
            return BaseDeadline + TDuration::Seconds(message->DeadlineDelta);
        default:
            return TInstant::Zero();
    }
}

size_t TStorage::ProccessDeadlines() {
    auto now = TimeProvider->Now();

    if (now < NextVacuumRun) {
        return 0;
    }
    NextVacuumRun = TrimToSeconds(now, false) + VACUUM_INTERVAL;

    auto deadlineDelta = (now - BaseDeadline).Seconds();
    size_t count = 0;

    auto unlockIfNeed = [&](auto offset, auto& message) {
        if (message.DeadlineDelta < deadlineDelta) {
            switch (message.GetStatus()) {
                case EMessageStatus::Locked:
                    ++Metrics.DeadlineExpiredMessageCount;
                    ++count;
                    DoUnlock(offset, message);
                    break;
                case EMessageStatus::Delayed:
                    ++count;
                    DoUndelay(offset);
                    break;
                default:
                    break;
            }
        }
    };
    IterateAllMessagesInOrder(unlockIfNeed);

    return count;
}

size_t TStorage::Compact() {
    AFL_ENSURE(FirstOffset <= FirstUncommittedOffset)("l", FirstOffset)("r", FirstUncommittedOffset);
    AFL_ENSURE(FirstOffset <= FirstUnlockedOffset)("l", FirstOffset)("r", FirstUnlockedOffset);
    AFL_ENSURE(FirstUncommittedOffset <= FirstUnlockedOffset)("l", FirstUncommittedOffset)("r", FirstUnlockedOffset);

    size_t removed = 0;

    // Remove messages by retention
    if (auto retentionDeadlineDelta = GetRetentionDeadlineDelta(); retentionDeadlineDelta.has_value()) {
        auto dieProcessingDelta = retentionDeadlineDelta.value() + 1;

        auto canRemove = [&](auto& message) {
            switch (message.GetStatus()) {
                case EMessageStatus::Locked:
                case EMessageStatus::DLQ:
                    return message.WriteTimestampDelta <= dieProcessingDelta;
                case EMessageStatus::Committed:
                case EMessageStatus::Delayed:
                case EMessageStatus::Unprocessed:
                    return message.WriteTimestampDelta <= retentionDeadlineDelta.value();
                default:
                    return false;
            }
        };

        for (auto it = SlowMessages.begin(); it != SlowMessages.end() && canRemove(it->second);) {
            it = RemoveMessageFromSlowZone(it);
            ++removed;
            ++Metrics.TotalDeletedByRetentionMessageCount;
        }

        while (!Messages.empty() && canRemove(Messages.front())) {
            auto& message = Messages.front();

            switch (message.GetStatus()) {
                case EMessageStatus::Unprocessed:
                case EMessageStatus::Locked:
                case EMessageStatus::Delayed:
                    ++Metrics.TotalDeletedByRetentionMessageCount;
                    break;
                case EMessageStatus::Committed:
                case EMessageStatus::DLQ:
                    break;
            }
            RemoveFirstMessageFromFastZone();
            ++removed;
        }
    }

    // Remove already committed messages
    while(!Messages.empty() && FirstOffset < FirstUncommittedOffset) {
        RemoveFirstMessageFromFastZone();
        ++removed;
    }

    while(!DLQQueue.empty()) {
        auto offset = DLQQueue.front().Offset;
        auto seqNo = DLQQueue.front().SeqNo;

        auto it = DLQMessages.find(offset);
        if (it != DLQMessages.end() && it->second == seqNo) {
            break;
        }

        DLQQueue.pop_front();
    }

    FirstUnlockedOffset = std::max(FirstUnlockedOffset, FirstOffset);
    FirstUncommittedOffset = std::max(FirstUncommittedOffset, FirstOffset);

    Batch.Compacted(removed);

    return removed;
}
static bool TrackMessageStatusInLockedGroups(const TStorage::EMessageStatus status) {
    return !EqualToOneOf(status, TStorage::EMessageStatus::Committed);
}

static bool TrackMessageStatusInLockedGroups(const TStorage::TMessage& message) {
    return TrackMessageStatusInLockedGroups(message.GetStatus());
}

static void UpdateLockedMaps(auto& messageGroups, const auto& locked, ui32 messageGroupIdHash) {
    if (locked.IsAccessible()) {
        messageGroups.UnlockedMessageGroupsId.insert(messageGroupIdHash);
        messageGroups.LockedMessageGroupsId.erase(messageGroupIdHash);
    } else {
        messageGroups.UnlockedMessageGroupsId.erase(messageGroupIdHash);
        if (locked.LockedSelf) {
            messageGroups.LockedMessageGroupsId.insert(messageGroupIdHash);
        } else {
            messageGroups.LockedMessageGroupsId.erase(messageGroupIdHash);
        }
    }
}

void TStorage::UpdateMessageGroupsParentLocks(const absl::flat_hash_set<ui32>& currLocked, const absl::flat_hash_set<ui32>& prevLocked, bool modeChanged) {
    auto update = [this](ui32 messageGroupIdHash, TSingleMessageGroupIdInfo& group) {
        bool newLockedParent = !CanReadMessageGroupIdHashFromParentPartition(messageGroupIdHash);
        if (group.Locked.LockedParent == newLockedParent) {
            return;
        }
        group.Locked.LockedParent = newLockedParent;
        UpdateLockedMaps(MessageGroups, group.Locked, messageGroupIdHash);
    };
    bool updateAll = currLocked.size() + prevLocked.size() >= MessageGroups.Groups.size();
    if (modeChanged || updateAll) {
        for (auto& [messageGroupIdHash, group] : MessageGroups.Groups) {
            update(messageGroupIdHash, group);
        }
    } else {
        for (auto messageGroupIdHash : currLocked) {
            if (prevLocked.contains(messageGroupIdHash)) {
                continue;
            }
            // unlocked -> locked
            Y_VERIFY_DEBUG_S(false, "Parent lock strictened; messageGroupIdHash=" << messageGroupIdHash);
            auto* group = MapFindPtr(MessageGroups.Groups, messageGroupIdHash);
            if (group) {
                update(messageGroupIdHash, *group);
            }
        }
        for (auto messageGroupIdHash : prevLocked) {
            if (currLocked.contains(messageGroupIdHash)) {
                continue;
            }
            // locked -> unlocked
            auto* group = MapFindPtr(MessageGroups.Groups, messageGroupIdHash);
            if (group) {
                update(messageGroupIdHash, *group);
            }
        }
    }
}

void TStorage::UpdateMessageGroupToNextMessage(ui64 offset, const TMessage& message) {
    auto messageGroupIterator = MessageGroups.Groups.find(message.MessageGroupIdHash);
    AFL_ENSURE(messageGroupIterator != MessageGroups.Groups.end());
    TSingleMessageGroupIdInfo* ptr = &messageGroupIterator->second;
    AFL_ENSURE(ptr->Size > 0);
    ptr->Size -= 1;
    AFL_ENSURE((ptr->Size == 0) == message.NextMessageGroupIdOffset().Empty())("size", ptr->Size)("next", message.NextMessageGroupIdOffset());
    AFL_ENSURE(ptr->FirstOffset <= offset)("first", ptr->FirstOffset)("offset", offset);

    auto nextOffset = message.NextMessageGroupIdOffset();
    if (nextOffset.Empty()) {
        MessageGroups.Groups.erase(messageGroupIterator);
        MessageGroups.LockedMessageGroupsId.erase(message.MessageGroupIdHash);
        MessageGroups.UnlockedMessageGroupsId.erase(message.MessageGroupIdHash);
        return;
    }
    ptr->FirstOffset = *nextOffset;
    auto [nextMessage, _] = GetMessageInt(*nextOffset);
    AFL_ENSURE(nextMessage != nullptr);
    ptr->Locked.FillFromStatus(nextMessage->GetStatus());
    UpdateLockedMaps(MessageGroups, ptr->Locked, message.MessageGroupIdHash);
}

void TStorage::UpdateMessageGroupOnMessageStatusChange(ui64 offset, const TMessage& message, EMessageStatus newStatus) {
    if (!KeepMessageOrder) {
        return;
    }
    if (!message.HasMessageGroupId) {
        if (EqualToOneOf(newStatus, EMessageStatus::Unprocessed)) {
            MessageGroups.UnorderedOffsets.insert(offset);
        } else {
            MessageGroups.UnorderedOffsets.erase(offset);
        }
        return;
    }
    if (!TrackMessageStatusInLockedGroups(message)) {
        return;
    }
    if (message.GetStatus() == newStatus) {
        return;
    }

    if (!TrackMessageStatusInLockedGroups(newStatus)) {
        UpdateMessageGroupToNextMessage(offset, message);
        return;
    }

    TSingleMessageGroupIdInfo* ptr = MapFindPtr(MessageGroups.Groups, message.MessageGroupIdHash);
    AFL_ENSURE(ptr != nullptr)("offset", offset)("messageGroupIdHash", message.MessageGroupIdHash);
    ptr->Locked.FillFromStatus(newStatus);
    UpdateLockedMaps(MessageGroups, ptr->Locked, message.MessageGroupIdHash);
}

void TStorage::UpdateMessageGroupForRemovedMessage(ui64 offset, const TMessage& message) {
    if (!KeepMessageOrder) {
        return;
    }
    if (!message.HasMessageGroupId) {
        MessageGroups.UnorderedOffsets.erase(offset);
        return;
    }
    if (!TrackMessageStatusInLockedGroups(message)) {
        return;
    }

    auto messageGroupIterator = MessageGroups.Groups.find(message.MessageGroupIdHash);
    TSingleMessageGroupIdInfo* ptr = (messageGroupIterator == MessageGroups.Groups.end()) ? nullptr : &messageGroupIterator->second;
    AFL_ENSURE(ptr != nullptr)("offset", offset)("messageGroupIdHash", message.MessageGroupIdHash);

    if (message.GetStatus() == EMessageStatus::Locked) {
        AFL_ENSURE(ptr->Locked.LockedSelf);
        ptr->Locked.LockedSelf = false;
    }
    if (message.GetStatus() == EMessageStatus::Delayed) {
        ptr->Locked.Delayed = false;
    }

    UpdateMessageGroupToNextMessage(offset, message);
}

void TStorage::UpdateMessageGroupForNewMessage(ui64 offset, TMessage& message) {
    if (!KeepMessageOrder) {
        return;
    }
    if (!message.HasMessageGroupId) {
        if (message.GetStatus() == EMessageStatus::Unprocessed) {
            MessageGroups.UnorderedOffsets.insert(offset);
        }
        return;
    }
    if (!TrackMessageStatusInLockedGroups(message)) {
        return;
    }

    const auto messageGroupIdHash = message.MessageGroupIdHash;
    auto [it, firstMessageInGroup] = MessageGroups.Groups.try_emplace(ui32(message.MessageGroupIdHash));
    bool firstReadableMessageInGroup = false;
    TSingleMessageGroupIdInfo& group = it->second;
    group.Size++;
    if (firstMessageInGroup) {
        group.FirstOffset = offset;
        firstReadableMessageInGroup = TrackMessageStatusInLockedGroups(message); // may be false on snapshot restore
    } else {
        auto [prev, _] = GetMessageInt(group.LastOffset);
        AFL_ENSURE(prev != nullptr);
        prev->SetNextMessageGroupIdOffset(offset);
        if (!TrackMessageStatusInLockedGroups(*prev)) {
            firstReadableMessageInGroup = true;
        }
    }
    if (firstReadableMessageInGroup) {
        group.FirstOffset = offset;
    }
    group.LastOffset = offset;
    if (message.GetStatus() == EMessageStatus::Locked) {
        AFL_ENSURE(firstReadableMessageInGroup)("offset", offset)("groupSize", group.Size);
    }
    if (firstReadableMessageInGroup) {
        group.Locked.LockedParent = !CanReadMessageGroupIdHashFromParentPartition(messageGroupIdHash);
        group.Locked.FillFromStatus(message.GetStatus());
        UpdateLockedMaps(MessageGroups, group.Locked, message.MessageGroupIdHash);
    }
}

void TStorage::RemoveMessage(ui64 offset, const TMessage& message) {
    AFL_ENSURE(Metrics.InflightMessageCount > 0);
    --Metrics.InflightMessageCount;

    UpdateMessageGroupForRemovedMessage(offset, message);

    switch(message.GetStatus()) {
        case EMessageStatus::Unprocessed:
            AFL_ENSURE(Metrics.UnprocessedMessageCount > 0);
            --Metrics.UnprocessedMessageCount;
            break;
        case EMessageStatus::Locked:
            AFL_ENSURE(Metrics.LockedMessageCount > 0);
            --Metrics.LockedMessageCount;
            if (KeepMessageOrder && message.HasMessageGroupId) {
                AFL_ENSURE(Metrics.LockedMessageGroupCount > 0);
                --Metrics.LockedMessageGroupCount;
            }
            break;
        case EMessageStatus::Delayed:
            AFL_ENSURE(Metrics.DelayedMessageCount > 0);
            --Metrics.DelayedMessageCount;
            break;
        case EMessageStatus::Committed:
            AFL_ENSURE(Metrics.CommittedMessageCount > 0);
            --Metrics.CommittedMessageCount;
            break;
        case EMessageStatus::DLQ:
            DLQMessages.erase(offset);

            AFL_ENSURE(Metrics.DLQMessageCount > 0);
            --Metrics.DLQMessageCount;
            break;
    }
}

void TStorage::RemoveFirstMessageFromFastZone() {
    AFL_ENSURE(!Messages.empty());
    auto& message = Messages.front();
    RemoveMessage(FirstOffset, message);
    Messages.pop_front();
    ++FirstOffset;
}

TStorage::TSlowMessagesMap::iterator TStorage::RemoveMessageFromSlowZone(TSlowMessagesMap::iterator it) {
    RemoveMessage(it->first, it->second);
    return SlowMessages.erase(it);
}

void TStorage::RemoveMessageFromSlowZone(ui64 offset) {
    auto it = SlowMessages.find(offset);
    AFL_ENSURE(it != SlowMessages.end())("offset", offset);
    RemoveMessageFromSlowZone(it);
}

bool TStorage::AddMessage(ui64 offset, bool hasMessagegroup, ui32 messageGroupIdHash, TInstant writeTimestamp, TDuration delay) {
    AFL_ENSURE(offset >= GetLastOffset())("l", offset)("r", GetLastOffset());

    while (!Messages.empty() && offset > GetLastOffset()) {
        RemoveFirstMessageFromFastZone();
    }

    if (Messages.size() >= MaxFastMessages) {
        // Move to slow zone
        for (size_t i = std::max<size_t>(std::min(std::min(Messages.size(), MaxMessages / 64), MaxSlowMessages - SlowMessages.size()), 1); i; --i) {
            auto& message = Messages.front();
            switch (message.GetStatus()) {
                case EMessageStatus::Unprocessed:
                case EMessageStatus::Locked:
                case EMessageStatus::Delayed:
                case EMessageStatus::DLQ:
                    SlowMessages.insert_or_assign(FirstOffset, message);
                    Batch.MoveToSlow(FirstOffset);
                    Messages.pop_front();
                    ++FirstOffset;
                    break;
                case EMessageStatus::Committed:
                    RemoveFirstMessageFromFastZone();
                    break;
            }
        }
    }

    if (Messages.empty()) {
        FirstOffset = offset;
    }

    if (Messages.size() >= MaxFastMessages) {
        return false;
    }

    FirstUnlockedOffset = std::max(FirstUnlockedOffset, FirstOffset);
    FirstUncommittedOffset = std::max(FirstUncommittedOffset, FirstOffset);

    writeTimestamp = TrimToSeconds(writeTimestamp);

    if (BaseWriteTimestamp == TInstant::Zero()) {
        BaseWriteTimestamp = writeTimestamp;
        Batch.MoveBaseTime(BaseDeadline, BaseWriteTimestamp);
    }

    auto writeTimestampDelta = (writeTimestamp - BaseWriteTimestamp).Seconds();
    if (auto retentionDeadlineDelta = GetRetentionDeadlineDelta(); retentionDeadlineDelta.has_value()) {
        auto removedByRetention = writeTimestampDelta <= retentionDeadlineDelta.value();
        // The message will be deleted by retention policy. Skip it.
        if (removedByRetention && Messages.empty()) {
            ++Metrics.TotalDeletedByRetentionMessageCount;
            Batch.AddNewMessage(offset);
            return true;
        }
    }

    ui32 deadlineDelta = delay == TDuration::Zero() ? 0 : NormalizeDeadline(writeTimestamp + delay);

    Messages.emplace_back(TMessageData{
        .Status = static_cast<ui32>(deadlineDelta ? EMessageStatus::Delayed : EMessageStatus::Unprocessed),
        .ProcessingCount = 0,
        .DeadlineDelta = deadlineDelta,
        .HasMessageGroupId = hasMessagegroup,
        .MessageGroupIdHash = messageGroupIdHash,
        .WriteTimestampDelta = ui32(writeTimestampDelta)
    });

    UpdateMessageGroupForNewMessage(offset, Messages.back());

    Batch.AddNewMessage(offset);

    ++Metrics.InflightMessageCount;
    if (deadlineDelta) {
        ++Metrics.DelayedMessageCount;
        Batch.AddChange(offset);
    } else {
        ++Metrics.UnprocessedMessageCount;
    }

    return true;
}

bool TStorage::MarkDLQMoved(TDLQMessage message) {
    while (!DLQQueue.empty() && message.SeqNo != DLQQueue.front().SeqNo) {
        auto frontOffset = DLQQueue.front().Offset;
        if (message.SeqNo < DLQQueue.front().SeqNo) {
            // The message was deleted by retention policy
            return true;
        }

        auto it = DLQMessages.find(frontOffset);
        if (it == DLQMessages.end()) {
            // The message queued second time
            DLQQueue.pop_front();
            continue;
        }

        auto [msg, _] = GetMessageInt(frontOffset, EMessageStatus::DLQ);
        if (!msg) {
            DLQMessages.erase(frontOffset);
            DLQQueue.pop_front();
            continue;
        }
        auto retentionDeadlineDelta = GetRetentionDeadlineDelta();
        if (retentionDeadlineDelta && msg->WriteTimestampDelta <= retentionDeadlineDelta.value()) {
            DLQMessages.erase(frontOffset);
            DLQQueue.pop_front();
            continue;
        }

        return false;
    }

    if (DLQQueue.empty()) {
        // DLQ policy changed for example
        return true;
    }

    DLQQueue.pop_front();

    ++Metrics.TotalMovedToDLQMessageCount;

    auto it = DLQMessages.find(message.Offset);
    if (it == DLQMessages.end()) {
        // message removed or message queued second time after changed dead letter policy
        return true;
    }

    if (it != DLQMessages.end() && it->second == message.SeqNo) {
        DoCommit(message.Offset, Metrics.TotalMovedToDLQMessageCount);
        return true;
    }

    return false;
}

bool TStorage::WakeUpDLQ() {
    for (auto [offset, _] : DLQMessages) {
        auto [message, slowZone] = GetMessageInt(offset, EMessageStatus::DLQ);
        if (message) {
            UpdateMessageGroupOnMessageStatusChange(offset, *message, EMessageStatus::Unprocessed);
            message->SetStatus(EMessageStatus::Unprocessed);
            if (!slowZone) {
                FirstUnlockedOffset = std::min(FirstUnlockedOffset, offset);
            }

            Batch.AddChange(offset);

            --Metrics.DLQMessageCount;
            ++Metrics.UnprocessedMessageCount;
        }
    }

    DLQMessages.clear();
    DLQQueue.clear();

    return true;
}

std::pair<const TStorage::TMessage*, bool> TStorage::GetMessageInt(ui64 offset) const {
    if (auto it = SlowMessages.find(offset); it != SlowMessages.end()) {
        return {&it->second,  true};
    }

    if (offset < FirstOffset) {
        return {nullptr, false};
    }

    auto offsetDelta = offset - FirstOffset;
    if (offsetDelta >= Messages.size()) {
        return {nullptr, false};
    }

    return {&Messages.at(offsetDelta), false};
}

std::pair<TStorage::TMessage*, bool> TStorage::GetMessageInt(ui64 offset) {
    if (auto it = SlowMessages.find(offset); it != SlowMessages.end()) {
        return {&it->second,  true};
    }

    if (offset < FirstOffset) {
        return {nullptr, false};
    }

    auto offsetDelta = offset - FirstOffset;
    if (offsetDelta >= Messages.size()) {
        return {nullptr, false};
    }

    return {&Messages.at(offsetDelta), false};
}

std::pair<const TStorage::TMessage*, bool> TStorage::GetMessage(ui64 message) {
    return GetMessageInt(message);
}

bool TStorage::DLQEmpty() const {
    return DLQQueue.empty();
}

std::deque<TDLQMessage> TStorage::GetDLQMessages() {
    static constexpr size_t MaxBatchSize = 1000;

    auto retentionDeadlineDelta = GetRetentionDeadlineDelta();

    std::deque<TDLQMessage> result;
    for (auto& [offset, seqNo] : DLQQueue) {
        auto [message, _] = GetMessageInt(offset, EMessageStatus::DLQ);
        if (!message) {
            continue;
        }
        if (retentionDeadlineDelta && message->WriteTimestampDelta <= retentionDeadlineDelta.value()) {
            continue;
        }

        auto it = DLQMessages.find(offset);
        if (it == DLQMessages.end() || seqNo < it->second) {
            // The message queued second time
            continue;
        }

        result.push_back({
            .Offset = offset,
            .SeqNo = seqNo
        });
        if (result.size() == MaxBatchSize) {
            break;
        }
    }

    return result;
}

const absl::flat_hash_set<ui32>& TStorage::GetLockedMessageGroupsId() const {
    return MessageGroups.LockedMessageGroupsId;
}

size_t TStorage::GetLockedMessageGroupsIdSize() const {
    return MessageGroups.LockedMessageGroupsId.size();
}

bool TStorage::HasRetentionExpiredMessages() const {
    if (Messages.empty()) {
        return false;
    }

    auto retentionDeadlineDelta = GetRetentionDeadlineDelta();
    if (!retentionDeadlineDelta.has_value()) {
        return false;
    }

    auto& message = Messages.front();
    return message.WriteTimestampDelta <= retentionDeadlineDelta.value();
}

std::pair<TStorage::TMessage*, bool> TStorage::GetMessageInt(ui64 offset, EMessageStatus expectedStatus) {
    auto [message, slowZone] = GetMessageInt(offset);
    if (!message) {
        return {nullptr, false};
    }

    if (message->GetStatus() != expectedStatus) {
        return {nullptr, slowZone};
    }

    return {message, slowZone};
}

ui64 TStorage::NormalizeDeadline(TInstant deadline) {
    auto now = TimeProvider->Now();
    if (deadline <= now) {
        return 0;
    }

    if (now < deadline && deadline - now > MaxDeadline) {
        deadline = now + MaxDeadline;
    }

    deadline = TrimToSeconds(deadline);

    auto deadlineDelta = (deadline - BaseDeadline).Seconds();
    if (deadlineDelta >= MaxDeadlineDelta) {
        MoveBaseDeadline();
        if (deadline <= BaseDeadline) {
            deadlineDelta = 0;
        } else {
            deadlineDelta = std::min((deadline - BaseDeadline).Seconds(), MaxDeadlineDelta - 1);
        }
    }

    return deadlineDelta;
}

ui64 TStorage::DoLock(ui64 offset, TMessage& message, const TInstant deadline) {
    auto now = TimeProvider->Now();

    AFL_VERIFY(message.GetStatus() == EMessageStatus::Unprocessed)("status", message.GetStatus());
    UpdateMessageGroupOnMessageStatusChange(offset, message, EMessageStatus::Locked);
    message.SetStatus(EMessageStatus::Locked);
    message.DeadlineDelta = NormalizeDeadline(deadline);
    if (message.ProcessingCount < MAX_PROCESSING_COUNT) {
        ++message.ProcessingCount;
        Metrics.MessageLocks.IncrementFor(message.ProcessingCount);
    }

    SetMessageLockingTime(message, now, BaseDeadline);

    Batch.AddChange(offset);
    if (KeepMessageOrder && message.HasMessageGroupId) {
        ++Metrics.LockedMessageGroupCount;
    }
    ++Metrics.LockedMessageCount;
    AFL_ENSURE(Metrics.UnprocessedMessageCount > 0)("o", offset);
    --Metrics.UnprocessedMessageCount;

    auto writeTimestamp = BaseWriteTimestamp + TDuration::Seconds(message.WriteTimestampDelta);
    auto waitingLockingDuration = now > writeTimestamp ? now - writeTimestamp : TDuration::Zero();
    Metrics.WaitingLockingDuration.IncrementFor(waitingLockingDuration.MilliSeconds());

    return offset;
}

TInstant TStorage::GetMessageLockingTime(const TMessage& message) const {
    if (message.GetStatus() != EMessageStatus::Locked) {
        return TInstant::Zero();
    }

    return message.LockingTimestampSign == 0 ?
        BaseDeadline + TDuration::MilliSeconds(message.LockingTimestampMilliSecondsDelta) :
        BaseDeadline - TDuration::MilliSeconds(message.LockingTimestampMilliSecondsDelta);
}

void TStorage::SetMessageLockingTime(TMessage& message, const TInstant& lockingTime, const TInstant& baseDeadline) const {
    const ui64 MaxLockingTimestampMilliSecondsDelta = (1 << 26) - 1;

    auto delta = std::abs(static_cast<i64>(baseDeadline.MilliSeconds()) - static_cast<i64>(lockingTime.MilliSeconds()));
    message.LockingTimestampMilliSecondsDelta = std::min<ui64>(delta, MaxLockingTimestampMilliSecondsDelta);
    message.LockingTimestampSign = baseDeadline > lockingTime;
}

void TStorage::UpdateMessageLockingDurationMetrics(const TMessage& message) {
    if (message.GetStatus() != EMessageStatus::Locked) {
        return;
    }
    auto lockingDuration = TimeProvider->Now() - GetMessageLockingTime(message);
    Metrics.MessageLockingDuration.IncrementFor(lockingDuration.MilliSeconds());
}

bool TStorage::DoCommit(ui64 offset, size_t& totalMetrics) {
    auto [message, slowZone] = GetMessageInt(offset);
    if (!message) {
        return false;
    }

    UpdateMessageGroupOnMessageStatusChange(offset, *message, EMessageStatus::Committed);
    switch(message->GetStatus()) {
        case EMessageStatus::Unprocessed:
            ++Metrics.CommittedMessageCount;
            AFL_ENSURE(Metrics.UnprocessedMessageCount > 0)("o", offset);
            --Metrics.UnprocessedMessageCount;
            ++totalMetrics;
            break;
        case EMessageStatus::Locked: {
            ++Metrics.CommittedMessageCount;
            AFL_ENSURE(Metrics.LockedMessageCount > 0)("o", offset);
            --Metrics.LockedMessageCount;
            if (KeepMessageOrder && message->HasMessageGroupId) {
                AFL_ENSURE(Metrics.LockedMessageGroupCount > 0)("o", offset);
                --Metrics.LockedMessageGroupCount;
            }

            ++totalMetrics;

            UpdateMessageLockingDurationMetrics(*message);
            message->LockingTimestampMilliSecondsDelta = 0;
            message->LockingTimestampSign = 0;

            break;
        }
        case EMessageStatus::Delayed:
            ++Metrics.CommittedMessageCount;
            AFL_ENSURE(Metrics.DelayedMessageCount > 0)("o", offset);
            --Metrics.DelayedMessageCount;
            ++totalMetrics;
            break;
        case EMessageStatus::Committed:
            return false;
        case EMessageStatus::DLQ:
            ++Metrics.CommittedMessageCount;
            DLQMessages.erase(offset);
            AFL_ENSURE(Metrics.DLQMessageCount > 0)("o", offset);
            --Metrics.DLQMessageCount;
            break;
    }
    message->SetStatus(EMessageStatus::Committed);
    message->DeadlineDelta = 0;

    if (slowZone) {
        RemoveMessage(offset, *message);
        SlowMessages.erase(offset);
        Batch.DeleteFromSlow(offset);
    } else {
        Batch.AddChange(offset);
    }

    UpdateFirstUncommittedOffset();

    return true;
}

bool TStorage::DoUnlock(ui64 offset) {
    auto [message, _] = GetMessageInt(offset, EMessageStatus::Locked);
    if (!message) {
        return false;
    }

    DoUnlock(offset, *message);

    return true;
}

void TStorage::DoUnlock(ui64 offset, TMessage& message) {
    UpdateMessageLockingDurationMetrics(message);
    UpdateMessageGroupOnMessageStatusChange(offset, message, EMessageStatus::Unprocessed);
    message.SetStatus(EMessageStatus::Unprocessed);
    message.DeadlineDelta = 0;
    message.LockingTimestampMilliSecondsDelta = 0;
    message.LockingTimestampSign = 0;

    Batch.AddChange(offset);

    ++Metrics.UnprocessedMessageCount;

    if (KeepMessageOrder && message.HasMessageGroupId) {
        AFL_ENSURE(Metrics.LockedMessageGroupCount > 0)("o", offset);
        --Metrics.LockedMessageGroupCount;
    }

    AFL_ENSURE(Metrics.LockedMessageCount > 0)("o", offset);
    --Metrics.LockedMessageCount;

    if (message.ProcessingCount >= MaxMessageProcessingCount && DeadLetterPolicy) {
        switch (DeadLetterPolicy.value()) {
            case NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_MOVE: {
                 UpdateMessageGroupOnMessageStatusChange(offset, message, EMessageStatus::DLQ);
                message.SetStatus(EMessageStatus::DLQ);

                auto seqNo = ++Metrics.TotalScheduledToDLQMessageCount;
                DLQMessages[offset] = seqNo;
                DLQQueue.push_back({
                    .Offset = offset,
                    .SeqNo = seqNo
                });
                Batch.AddToDLQ(offset, seqNo);

                AFL_ENSURE(Metrics.UnprocessedMessageCount > 0)("o", offset);
                --Metrics.UnprocessedMessageCount;
                ++Metrics.DLQMessageCount;
                return;
            }
            case NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_DELETE:
                DoCommit(offset, Metrics.TotalDeletedByDeadlinePolicyMessageCount);
                return;
            case NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_UNSPECIFIED:
                break;
        }
    }

    if (offset >= FirstOffset) {
        FirstUnlockedOffset = std::min(FirstUnlockedOffset, offset);
    }
}

bool TStorage::DoUndelay(ui64 offset) {
    auto [message, _] = GetMessageInt(offset, EMessageStatus::Delayed);
    if (!message) {
        return false;
    }

    UpdateMessageGroupOnMessageStatusChange(offset, *message, EMessageStatus::Unprocessed);
    message->SetStatus(EMessageStatus::Unprocessed);
    message->DeadlineDelta = 0;

    Batch.AddChange(offset);

    ++Metrics.UnprocessedMessageCount;
    AFL_ENSURE(Metrics.DelayedMessageCount > 0)("o", offset);
    --Metrics.DelayedMessageCount;

    if (offset >= FirstOffset) {
        FirstUnlockedOffset = std::min(FirstUnlockedOffset, offset);
    }

    return true;
}

void TStorage::MoveBaseDeadline() {
    if (Messages.empty() && SlowMessages.empty()) {
        return;
    }

    auto newBaseDeadline = TrimToSeconds(TimeProvider->Now(), false);
    auto newBaseWriteTimestamp = BaseWriteTimestamp +
        (SlowMessages.empty() ? TDuration::Seconds(Messages.front().WriteTimestampDelta)
            : TDuration::Seconds(SlowMessages.begin()->second.WriteTimestampDelta));

    MoveBaseDeadline(newBaseDeadline, newBaseWriteTimestamp);

    Batch.MoveBaseTime(BaseDeadline, BaseWriteTimestamp);
}

void TStorage::MoveBaseDeadline(TInstant newBaseDeadline, TInstant newBaseWriteTimestamp) {
    auto deadlineDiff = (newBaseDeadline - BaseDeadline).Seconds();
    auto writeTimestampDiff = (newBaseWriteTimestamp - BaseWriteTimestamp).Seconds();

    if (deadlineDiff == 0 && writeTimestampDiff == 0) {
        return;
    }

    auto doChange = [&](auto& message) {
        message.DeadlineDelta = message.DeadlineDelta > deadlineDiff ? message.DeadlineDelta - deadlineDiff : 0;
        message.WriteTimestampDelta = message.WriteTimestampDelta > writeTimestampDiff ? message.WriteTimestampDelta - writeTimestampDiff : 0;

        SetMessageLockingTime(message, GetMessageLockingTime(message), newBaseDeadline);
    };

    for (auto& [_, message] : SlowMessages) {
        doChange(message);
    }
    for (size_t i = 0; i < Messages.size(); ++i) {
        doChange(Messages[i]);
    }

    BaseDeadline = newBaseDeadline;
    BaseWriteTimestamp = newBaseWriteTimestamp;
}

void TStorage::UpdateFirstUncommittedOffset() {
    auto offsetDelta = FirstUncommittedOffset > FirstOffset ? FirstUncommittedOffset - FirstOffset : 0;
    while (offsetDelta < Messages.size() && Messages[offsetDelta].GetStatus() == EMessageStatus::Committed) {
        if (FirstUnlockedOffset == FirstUncommittedOffset) {
            ++FirstUnlockedOffset;
        }
        ++FirstUncommittedOffset;
        ++offsetDelta;
    }
}

TStorage::TBatch TStorage::ExtractBatch() {
    return std::exchange(Batch, {this});
}

bool TStorage::IsBatchEmpty() const {
    return Batch.Empty();
}

const TMetrics& TStorage::GetMetrics() const {
    return Metrics;
}

ui64 TStorage::GetFirstOffset() const {
    return FirstOffset;
}

size_t TStorage::GetMessageCount() const {
    return Messages.size() + SlowMessages.size();
}

ui64 TStorage::GetLastOffset() const {
    return FirstOffset + Messages.size();
}

ui64 TStorage::GetFirstUncommittedOffset() const {
    if (SlowMessages.empty()) {
        return FirstUncommittedOffset;
    }
    return SlowMessages.begin()->first;
}

ui64 TStorage::GetFirstUnlockedOffset() const {
    return FirstUnlockedOffset;
}

TInstant TStorage::GetBaseDeadline() const {
    return BaseDeadline;
}

TInstant TStorage::GetBaseWriteTimestamp() const {
    return BaseWriteTimestamp;
}

TString TStorage::DebugString() const {
    TStringBuilder sb;
    sb << "FirstOffset: " << FirstOffset
         << " FirstUncommittedOffset: " << FirstUncommittedOffset
         << " FirstUnlockedOffset: " << FirstUnlockedOffset
         << " BaseDeadline: " << BaseDeadline.ToString()
         << " BaseWriteTimestamp: " << BaseWriteTimestamp.ToString()
         << " Messages: [";

    auto dump = [&](const auto offset, const auto& message, auto zone) {
        sb << zone <<"{" << offset << ", "
            << static_cast<EMessageStatus>(message.Status) << ", "
            << message.DeadlineDelta << ", "
            << message.WriteTimestampDelta << ", "
            << GetMessageLockingTime(message).ToString() << ", "
            << message.MessageGroupIdHash << "} ";
    };

    for (auto& [offset, message] : SlowMessages) {
        dump(offset, message, 's');
    }
    for (size_t i = 0; i < Messages.size(); ++i) {
        dump(FirstOffset + i, Messages[i], 'f');
    }

    sb << "] LockedGroups [" << JoinSeq(", ", GetLockedMessageGroupsId()) << "]";
    sb << " DLQQueue [" << JoinSeq(", ", DLQQueue) << "]";
    sb << " DLQMessages [" << JoinSeq(", ", DLQMessages | std::views::transform(AsTDLQMessage)) << "]";
    sb << " Metrics {"
        << "Inflight: " << Metrics.InflightMessageCount << ", "
        << "Unprocessed: " << Metrics.UnprocessedMessageCount << ", "
        << "Locked: " << Metrics.LockedMessageCount << ", "
        << "LockedGroups: " << Metrics.LockedMessageGroupCount << ", "
        << "Committed: " << Metrics.CommittedMessageCount << ", "
        << "DLQ: " << Metrics.DLQMessageCount
        << "}";
    return sb;
}

TStorage::TBatch::TBatch(TStorage* storage)
    : Storage(storage)
{
}

void TStorage::TBatch::AddChange(ui64 offset) {
    ChangedMessages.push_back(offset);
}

void TStorage::TBatch::AddToDLQ(ui64 offset, ui64 seqNo) {
    AddedToDLQ.emplace_back(offset, seqNo);
}

void TStorage::TBatch::AddNewMessage(ui64 offset) {
    if (!FirstNewMessage) {
        FirstNewMessage = offset;
    }
    ++NewMessageCount;
}

void TStorage::TBatch::MoveToSlow(ui64 offset) {
    MovedToSlowZone.push_back(offset);
}

void TStorage::TBatch::DeleteFromSlow(ui64 offset) {
    DeletedFromSlowZone.push_back(offset);
}

void TStorage::TBatch::SetPurged() {
    Purged = true;
}

void TStorage::TBatch::SetUpdateExternalLockedMessageGroupsId(ui32 parentPartitionId) {
    UpdateExternalLockedMessageGroupsId.insert(parentPartitionId);
}

void TStorage::TBatch::Compacted(size_t count) {
    CompactedMessages += count;
}

void TStorage::TBatch::MoveBaseTime(TInstant baseDeadline, TInstant baseWriteTimestamp) {
    BaseDeadline = baseDeadline;
    BaseWriteTimestamp = baseWriteTimestamp;
}

bool TStorage::TBatch::Empty() const {
    return ChangedMessages.empty()
        && !FirstNewMessage.has_value()
        && AddedToDLQ.empty()
        && !BaseDeadline.has_value()
        && !BaseWriteTimestamp.has_value()
        && MovedToSlowZone.empty()
        && DeletedFromSlowZone.empty()
        && CompactedMessages == 0
        && !Purged
        && UpdateExternalLockedMessageGroupsId.empty();
}

size_t TStorage::TBatch::AddedMessageCount() const {
    return NewMessageCount;
}

size_t TStorage::TBatch::ChangedMessageCount() const {
    return ChangedMessages.size();
}

size_t TStorage::TBatch::DLQMessageCount() const {
    return AddedToDLQ.size();
}

bool TStorage::TBatch::GetPurged() const {
    return Purged;
}

TStorage::TMessageIterator::TMessageIterator(const TStorage& storage, std::map<ui64, TMessage>::const_iterator it, ui64 offset)
    : Storage(storage)
    , Iterator(it)
    , Offset(offset)
{
}

TStorage::TMessageIterator& TStorage::TMessageIterator::operator++() {
    if (Iterator != Storage.SlowMessages.end()) {
        ++Iterator;
    } else {
        ++Offset;
    }
    return *this;
}

TStorage::TMessageWrapper TStorage::TMessageIterator::operator*() const {
    ui64 offset;
    const TStorage::TMessage* message;
    if (Iterator != Storage.SlowMessages.end()) {
        offset = Iterator->first;
        message = &Iterator->second;
    } else {
        offset = Offset;
        auto [m, _] = Storage.GetMessageInt(Offset);
        message = m;
    }

    return TMessageWrapper{
        .SlowZone = Iterator != Storage.SlowMessages.end(),
        .Offset = offset,
        .Status = static_cast<EMessageStatus>(message->Status),
        .ProcessingCount = message->ProcessingCount,
        .ProcessingDeadline = static_cast<EMessageStatus>(message->Status) == EMessageStatus::Locked || static_cast<EMessageStatus>(message->Status) == EMessageStatus::Delayed ?
            Storage.BaseDeadline + TDuration::Seconds(message->DeadlineDelta) : TInstant::Zero(),
        .WriteTimestamp = Storage.BaseWriteTimestamp + TDuration::Seconds(message->WriteTimestampDelta),
        .LockingTimestamp = Storage.GetMessageLockingTime(*message),
        .MessageGroupIdHash = message->HasMessageGroupId ? std::optional<ui32>(message->MessageGroupIdHash) : std::nullopt,
        .MessageGroupIsLocked = Storage.KeepMessageOrder && message->HasMessageGroupId && !Storage.MessageGroups.UnlockedMessageGroupsId.contains(message->MessageGroupIdHash),
    };
}

bool TStorage::TMessageIterator::operator==(const TStorage::TMessageIterator& other) const {
    return Iterator == other.Iterator && Offset == other.Offset;
}

TStorage::TMessageIterator TStorage::begin() const {
    return TMessageIterator(*this, SlowMessages.begin(), FirstOffset);
}

TStorage::TMessageIterator TStorage::end() const {
    return TMessageIterator(*this, SlowMessages.end(), FirstOffset + Messages.size());
}

void TStorage::UpdateMessageMetrics(const TMessage& message) {
    switch (message.GetStatus()) {
        case EMessageStatus::Locked:
            ++Metrics.LockedMessageCount;
            if (KeepMessageOrder && message.HasMessageGroupId) {
                ++Metrics.LockedMessageGroupCount;
            }
            break;
        case EMessageStatus::Unprocessed:
            ++Metrics.UnprocessedMessageCount;
            break;
        case EMessageStatus::Delayed:
            ++Metrics.DelayedMessageCount;
            break;
        case EMessageStatus::Committed:
            ++Metrics.CommittedMessageCount;
            break;
        case EMessageStatus::DLQ:
            ++Metrics.DLQMessageCount;
            break;
    }
}

void TStorage::InitMetrics() {
    for (const auto& [_, message] : SlowMessages) {
        UpdateMessageMetrics(message);
    }

    for (const auto& message : Messages) {
        UpdateMessageMetrics(message);
    }

    Metrics.InflightMessageCount = Messages.size() + SlowMessages.size();
}

void TStorage::TMessageGroups::Clear() {
    Groups.clear();
    UnlockedMessageGroupsId.clear();
    LockedMessageGroupsId.clear();
    UnorderedOffsets.clear();
}

void TStorage::IterateMessageGroupsIdExclusiveFromParent(const std::function<void(ui32)>& callback) const {
    // handle case, when current partition is exhaused, but parent partition is still contains some unprocessed messages

    if (ParentPartitionExternalLockInfo.size() <= 1) [[likely]] {
        for (const auto& parentLockInfo : ParentPartitionExternalLockInfo) {
            for (const auto& messageGroupsId : parentLockInfo.LockedMessageGroupsIdSet) {
                if (MessageGroups.Groups.contains(messageGroupsId)) {
                    continue;
                }
                callback(messageGroupsId);
            }
        }
        return;
    }

    absl::flat_hash_set<ui32> processed;
    for (const auto& parentLockInfo : ParentPartitionExternalLockInfo) {
        for (const auto& messageGroupsId : parentLockInfo.LockedMessageGroupsIdSet) {
            if (MessageGroups.Groups.contains(messageGroupsId)) {
                continue;
            }
            if (!processed.insert(messageGroupsId).second) {
                continue;
            }
            callback(messageGroupsId);
        }
    }
}

size_t TStorage::GetEstimatedLockedMessageGroupsIdSizeFromSelfAndParents() const {
    size_t n = MessageGroups.Groups.size();
    for (const auto& parentLockInfo : ParentPartitionExternalLockInfo) {
        n += parentLockInfo.LockedMessageGroupsIdSet.size();
    }
    return n;
}

} // namespace NKikimr::NPQ::NMLP
