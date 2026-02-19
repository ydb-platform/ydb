#include "mlp_storage.h"

#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/persqueue/common/percentiles.h>
#include <ydb/library/actors/core/log.h>

#include <util/string/join.h>

namespace NKikimr::NPQ::NMLP {

namespace {

TInstant TrimToSeconds(TInstant time, bool up = true) {
    return TInstant::Seconds(time.Seconds() + (up && time.MilliSecondsOfSecond() > 0 ? 1 : 0));
}

}

TStorage::TStorage(TIntrusivePtr<ITimeProvider> timeProvider, size_t minMessages, size_t maxMessages)
    : MaxMessages(maxMessages)
    , MinMessages(minMessages)
    , MaxFastMessages(maxMessages - maxMessages / 4)
    , MaxSlowMessages(maxMessages / 4)
    , TimeProvider(timeProvider)
    , NextVacuumRun(TInstant::Zero())
    , Batch(this)
{
    BaseDeadline = TrimToSeconds(timeProvider->Now(), false);
    Metrics.MessageLocks.Initialize(MLP_LOCKS_RANGES, std::size(MLP_LOCKS_RANGES), true);
    Metrics.MessageLockingDuration.Initialize(SLOW_LATENCY_RANGES, std::size(SLOW_LATENCY_RANGES), true);
    Metrics.WaitingLockingDuration.Initialize(SLOW_LATENCY_RANGES, std::size(SLOW_LATENCY_RANGES), true);
}

void TStorage::SetKeepMessageOrder(bool keepMessageOrder) {
    KeepMessageOrder = keepMessageOrder;
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

bool TStorage::GetKeepMessageOrder() const {
    return KeepMessageOrder;
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

std::optional<ui64> TStorage::Next(TInstant deadline, TPosition& position) {
    std::optional<ui64> retentionDeadlineDelta = GetRetentionDeadlineDelta();

    if (!position.SlowPosition) {
        position.SlowPosition = SlowMessages.begin();
    }

    auto retentionExpired = [&](const auto& message) {
        return retentionDeadlineDelta && message.WriteTimestampDelta <= retentionDeadlineDelta.value();
    };

    for(; position.SlowPosition != SlowMessages.end(); ++position.SlowPosition.value()) {
        auto offset = position.SlowPosition.value()->first;
        auto& message = position.SlowPosition.value()->second;
        if (message.GetStatus() == EMessageStatus::Unprocessed) {
            if (retentionExpired(message)) {
                continue;
            }

            if (KeepMessageOrder && message.HasMessageGroupId && LockedMessageGroupsId.contains(message.MessageGroupIdHash)) {
                continue;
            }

            return DoLock(offset, message, deadline);
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

            if (KeepMessageOrder && message.HasMessageGroupId && LockedMessageGroupsId.contains(message.MessageGroupIdHash)) {
                moveUnlockedOffset = false;
                continue;
            }

            if (moveUnlockedOffset) {
                ++FirstUnlockedOffset;
            }

            ui64 offset = FirstOffset + i;
            position.FastPosition = offset + 1;
            return DoLock(offset, message, deadline);
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
    LockedMessageGroupsId.clear();

    FirstOffset = endOffset;
    FirstUncommittedOffset = endOffset;
    FirstUnlockedOffset = endOffset;

    BaseDeadline = TrimToSeconds(TimeProvider->Now(), false);
    BaseWriteTimestamp = TrimToSeconds(TimeProvider->Now(), false);

    NextVacuumRun = TrimToSeconds(TimeProvider->Now(), false) + VACUUM_INTERVAL;

    Batch.SetPurged();

    return true;
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

    for (auto& [offset, message] : SlowMessages) {
        unlockIfNeed(offset, message);
    }
    for (size_t i = 0; i < Messages.size(); ++i) {
        unlockIfNeed(FirstOffset + i, Messages[i]);
    }

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
            auto& message = it->second;
            RemoveMessage(it->first, message);
            it = SlowMessages.erase(it);
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

            RemoveMessage(FirstOffset, message);
            Messages.pop_front();
            ++FirstOffset;
            ++removed;
        }
    }

    // Remove already committed messages
    while(!Messages.empty() && FirstOffset < FirstUncommittedOffset) {
        auto& message = Messages.front();
        RemoveMessage(FirstOffset, message);
        Messages.pop_front();
        ++FirstOffset;
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

void TStorage::RemoveMessage(ui64 offset, const TMessage& message) {
    AFL_ENSURE(Metrics.InflightMessageCount > 0);
    --Metrics.InflightMessageCount;

    switch(message.GetStatus()) {
        case EMessageStatus::Unprocessed:
            AFL_ENSURE(Metrics.UnprocessedMessageCount > 0);
            --Metrics.UnprocessedMessageCount;
            break;
        case EMessageStatus::Locked:
            AFL_ENSURE(Metrics.LockedMessageCount > 0);
            --Metrics.LockedMessageCount;
            if (KeepMessageOrder && message.HasMessageGroupId && LockedMessageGroupsId.erase(message.MessageGroupIdHash)) {
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

bool TStorage::AddMessage(ui64 offset, bool hasMessagegroup, ui32 messageGroupIdHash, TInstant writeTimestamp, TDuration delay) {
    AFL_ENSURE(offset >= GetLastOffset())("l", offset)("r", GetLastOffset());

    while (!Messages.empty() && offset > GetLastOffset()) {
        auto message = Messages.front();
        RemoveMessage(FirstOffset, message);
        Messages.pop_front();
        ++FirstOffset;
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
                    SlowMessages[FirstOffset] = message;
                    Batch.MoveToSlow(FirstOffset);
                    break;
                case EMessageStatus::Committed:
                    RemoveMessage(FirstOffset, message);
                    break;
            }
            Messages.pop_front();
            ++FirstOffset;
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

    Messages.push_back({
        .Status = static_cast<ui32>(deadlineDelta ? EMessageStatus::Delayed : EMessageStatus::Unprocessed),
        .ProcessingCount = 0,
        .DeadlineDelta = deadlineDelta,
        .HasMessageGroupId = hasMessagegroup,
        .MessageGroupIdHash = messageGroupIdHash,
        .WriteTimestampDelta = ui32(writeTimestampDelta)
    });

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
    if (DLQQueue.empty()) {
        // DLQ policy changed for example
        return true;
    }

    if (message.SeqNo < DLQQueue.front().SeqNo) {
        // The message was deleted by retention policy
        return true;
    }

    if (message.SeqNo != DLQQueue.front().SeqNo || message.Offset != DLQQueue.front().Offset) {
        // the unexpected moved message
        return false;
    }

    DLQQueue.pop_front();

    ++Metrics.TotalMovedToDLQMessageCount;

    auto it = DLQMessages.find(message.Offset);
    if (it == DLQMessages.end() || it->second < message.SeqNo) {
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

std::deque<TDLQMessage> TStorage::GetDLQMessages() {
    static constexpr size_t MaxBatchSize = 1000;

    auto retentionDeadlineDelta = GetRetentionDeadlineDelta();

    std::deque<TDLQMessage> result;
    for (auto& [offset, seqNo] : DLQQueue) {
        auto [message, _] = GetMessageInt(offset, EMessageStatus::DLQ);
        if (!message) {
            continue;
        }
        if (retentionDeadlineDelta && message->DeadlineDelta <= retentionDeadlineDelta.value()) {
            continue;
        }

        auto it = DLQMessages.find(offset);
        if (it == DLQMessages.end() || seqNo != it->second) {
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

const std::unordered_set<ui32>& TStorage::GetLockedMessageGroupsId() const {
    return LockedMessageGroupsId;
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

ui64 TStorage::DoLock(ui64 offset, TMessage& message, TInstant& deadline) {
    auto now = TimeProvider->Now();
    
    AFL_VERIFY(message.GetStatus() == EMessageStatus::Unprocessed)("status", message.GetStatus());
    message.SetStatus(EMessageStatus::Locked);
    message.DeadlineDelta = NormalizeDeadline(deadline);
    if (message.ProcessingCount < MAX_PROCESSING_COUNT) {
        ++message.ProcessingCount;
        Metrics.MessageLocks.IncrementFor(message.ProcessingCount);
    }

    SetMessageLockingTime(message, now, BaseDeadline);

    Batch.AddChange(offset);

    if (KeepMessageOrder && message.HasMessageGroupId) {
        LockedMessageGroupsId.insert(message.MessageGroupIdHash);

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

    switch(message->GetStatus()) {
        case EMessageStatus::Unprocessed:
            if (!slowZone) {
                Batch.AddChange(offset);
                ++Metrics.CommittedMessageCount;
            }

            AFL_ENSURE(Metrics.UnprocessedMessageCount > 0)("o", offset);
            --Metrics.UnprocessedMessageCount;
            ++totalMetrics;
            break;
        case EMessageStatus::Locked: {
            if (!slowZone) {
                Batch.AddChange(offset);
                ++Metrics.CommittedMessageCount;
            }

            AFL_ENSURE(Metrics.LockedMessageCount > 0)("o", offset);
            --Metrics.LockedMessageCount;
            if (KeepMessageOrder && message->HasMessageGroupId && LockedMessageGroupsId.erase(message->MessageGroupIdHash)) {
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
            if (!slowZone) {
                Batch.AddChange(offset);
                ++Metrics.CommittedMessageCount;
            }

            AFL_ENSURE(Metrics.DelayedMessageCount > 0)("o", offset);
            --Metrics.DelayedMessageCount;
            ++totalMetrics;
            break;
        case EMessageStatus::Committed:
            return false;
        case EMessageStatus::DLQ:
            if (!slowZone) {
                Batch.AddChange(offset);
                ++Metrics.CommittedMessageCount;
            }

            AFL_ENSURE(Metrics.DLQMessageCount > 0)("o", offset);
            --Metrics.DLQMessageCount;
            break;
    }

    if (slowZone) {
        SlowMessages.erase(offset);
        Batch.DeleteFromSlow(offset);
        AFL_ENSURE(Metrics.InflightMessageCount > 0)("o", offset);
        --Metrics.InflightMessageCount;
    } else {
        message->SetStatus(EMessageStatus::Committed);
        message->DeadlineDelta = 0;
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

    message.SetStatus(EMessageStatus::Unprocessed);
    message.DeadlineDelta = 0;
    message.LockingTimestampMilliSecondsDelta = 0;
    message.LockingTimestampSign = 0;

    Batch.AddChange(offset);

    ++Metrics.UnprocessedMessageCount;

    if (KeepMessageOrder && message.HasMessageGroupId && LockedMessageGroupsId.erase(message.MessageGroupIdHash)) {
        AFL_ENSURE(Metrics.LockedMessageGroupCount > 0)("o", offset);
        --Metrics.LockedMessageGroupCount;
    }

    AFL_ENSURE(Metrics.LockedMessageCount > 0)("o", offset);
    --Metrics.LockedMessageCount;

    if (message.ProcessingCount >= MaxMessageProcessingCount && DeadLetterPolicy) {
        switch (DeadLetterPolicy.value()) {
            case NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_MOVE: {
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

TStorage::TBatch TStorage::GetBatch() {
    return std::exchange(Batch, {this});
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

    sb << "] LockedGroups [" << JoinRange(", ", LockedMessageGroupsId.begin(), LockedMessageGroupsId.end()) << "]";
    sb << " DLQQueue [" << JoinRange(", ", DLQQueue.begin(), DLQQueue.end()) << "]";
    sb << " DLQMessages [" << JoinRange(", ", DLQMessages.begin(), DLQMessages.end()) << "]";
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
        && !Purged;
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
        .MessageGroupIsLocked = Storage.KeepMessageOrder && message->HasMessageGroupId && Storage.LockedMessageGroupsId.contains(message->MessageGroupIdHash),
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

} // namespace NKikimr::NPQ::NMLP
