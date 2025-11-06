#include "mlp_storage.h"

#include <ydb/core/protos/pqconfig.pb.h>
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
    , Batch(this)
{
    BaseDeadline = TrimToSeconds(timeProvider->Now(), false);
}

void TStorage::SetKeepMessageOrder(bool keepMessageOrder) {
    KeepMessageOrder = keepMessageOrder;
}

void TStorage::SetMaxMessageProcessingCount(ui32 MaxMessageProcessingCount) {
    MaxMessageProcessingCount = MaxMessageProcessingCount;
}

void TStorage::SetRetentionPeriod(std::optional<TDuration> retentionPeriod) {
    RetentionPeriod = retentionPeriod;
}

std::optional<ui64> TStorage::Next(TInstant deadline, TPosition& position) {
    auto dieDelta = Max<ui64>();
    if (RetentionPeriod) {
        auto dieTime = TimeProvider->Now() - RetentionPeriod.value();
        dieDelta = dieTime > BaseWriteTimestamp ? (dieTime - BaseWriteTimestamp).Seconds() : 0;
    }

    if (!position.SlowPosition) {
        position.SlowPosition = SlowMessages.begin();
    }

    for(; position.SlowPosition != SlowMessages.end(); ++position.SlowPosition.value()) {
        auto offset = position.SlowPosition.value()->first;
        auto& message = position.SlowPosition.value()->second;
        if (message.Status == EMessageStatus::Unprocessed) {
            if (message.WriteTimestampDelta < dieDelta) {
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
        if (message.Status == EMessageStatus::Unprocessed) {
            if (message.WriteTimestampDelta < dieDelta) {
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

            return DoLock(offset, message, deadline);
        } else if (moveUnlockedOffset) {
            ++FirstUnlockedOffset;
        }
    }

    return std::nullopt;
}

bool TStorage::Commit(ui64 messageId) {
    return DoCommit(messageId);
}

bool TStorage::Unlock(ui64 messageId) {
    return DoUnlock(messageId);
}

bool TStorage::ChangeMessageDeadline(ui64 messageId, TInstant deadline) {
    auto [message, _] = GetMessageInt(messageId, EMessageStatus::Locked);
    if (!message) {
        return false;
    }

    Batch.AddChange(messageId);

    auto newDeadlineDelta = NormalizeDeadline(deadline);
    message->DeadlineDelta = newDeadlineDelta;

    return true;
}

TInstant TStorage::GetMessageDeadline(ui64 messageId) {
    auto [message, _] = GetMessageInt(messageId, EMessageStatus::Locked);
    if (!message) {
        return TInstant::Zero();
    }

    return BaseDeadline + TDuration::Seconds(message->DeadlineDelta);
}

size_t TStorage::ProccessDeadlines() {
    auto deadlineDelta = (TimeProvider->Now() - BaseDeadline).Seconds();
    size_t count = 0;

    auto unlockIfNeed = [&](auto offset, auto& message) {
        if (message.Status == EMessageStatus::Locked && message.DeadlineDelta < deadlineDelta) {
            DoUnlock(offset, message);
            ++count;
            ++Metrics.DeadlineExpiredMessageCount;
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
    if (RetentionPeriod && (TimeProvider->Now() - RetentionPeriod.value()) > BaseWriteTimestamp) {
        auto dieDelta = (TimeProvider->Now() - RetentionPeriod.value() - BaseWriteTimestamp).Seconds();
        auto dieProcessingDelta = dieDelta + 60;

        auto canRemove = [&](auto& message) {
            switch (message.Status) {
                case EMessageStatus::Locked:
                    return message.DeadlineDelta < dieProcessingDelta;
                case EMessageStatus::Unprocessed:
                case EMessageStatus::Committed:
                case EMessageStatus::DLQ:
                    return message.WriteTimestampDelta < dieDelta;
                default:
                    return false;
            }
        };

        for (auto it = SlowMessages.begin(); it != SlowMessages.end() && canRemove(it->second);) {
            auto& message = it->second;
            RemoveMessage(message);
            it = SlowMessages.erase(it);
            ++removed;
        }

        while (!Messages.empty() && canRemove(Messages.front())) {
            auto& message = Messages.front();
            RemoveMessage(message);
            Messages.pop_front();
            ++FirstOffset;
            ++removed;
        }
    }

    // Remove already committed messages
    while(!Messages.empty() && FirstOffset < FirstUncommittedOffset) {
        auto& message = Messages.front();
        RemoveMessage(message);
        Messages.pop_front();
        ++FirstOffset;
        ++removed;
    }

    FirstUnlockedOffset = std::max(FirstUnlockedOffset, FirstOffset);
    FirstUncommittedOffset = std::max(FirstUncommittedOffset, FirstOffset);

    Batch.Compacted(removed);

    return removed;
}

void TStorage::RemoveMessage(const TMessage& message) {
    --Metrics.InflyMessageCount;
    switch(message.Status) {
        case EMessageStatus::Unprocessed:
            --Metrics.UnprocessedMessageCount;
            break;
        case EMessageStatus::Locked:
            --Metrics.LockedMessageCount;
            if (KeepMessageOrder && message.HasMessageGroupId && LockedMessageGroupsId.erase(message.MessageGroupIdHash)) {
                --Metrics.LockedMessageGroupCount;
            }
            break;
        case EMessageStatus::Committed:
            --Metrics.CommittedMessageCount;
            break;
        case EMessageStatus::DLQ:
            --Metrics.DLQMessageCount;
            break;
    }
}

bool TStorage::AddMessage(ui64 offset, bool hasMessagegroup, ui32 messageGroupIdHash, TInstant writeTimestamp) {
    AFL_ENSURE(offset >= GetLastOffset())("l", offset)("r", GetLastOffset());

    while (!Messages.empty() && offset > GetLastOffset()) {
        auto message = Messages.front();
        RemoveMessage(message);
        Messages.pop_front();
        ++FirstOffset;
    }

    if (Messages.size() >= MaxFastMessages) {
        // Move to slow zone
        for (size_t i = std::max<size_t>(std::min(std::min(Messages.size(), MaxMessages / 64), MaxSlowMessages - SlowMessages.size()), 1); i; --i) {
            auto& message = Messages.front();
            switch (message.Status) {
                case EMessageStatus::Unprocessed:
                case EMessageStatus::Locked:
                case EMessageStatus::DLQ:
                    SlowMessages[FirstOffset] = message;
                    Batch.MoveToSlow(FirstOffset);
                    break;
                case EMessageStatus::Committed:
                    RemoveMessage(message);
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

    if (BaseWriteTimestamp == TInstant::Zero()) {
        BaseWriteTimestamp = TrimToSeconds(writeTimestamp);
        Batch.MoveBaseTime(TimeProvider->Now(), BaseWriteTimestamp);
    }

    Messages.push_back({
        .Status = EMessageStatus::Unprocessed,
        .ReceiveCount = 0,
        .DeadlineDelta = 0,
        .HasMessageGroupId = hasMessagegroup,
        .MessageGroupIdHash = messageGroupIdHash,
        .WriteTimestampDelta = (TrimToSeconds(writeTimestamp) - BaseWriteTimestamp).Seconds()
    });

    Batch.AddNewMessage(offset);

    ++Metrics.InflyMessageCount;
    ++Metrics.UnprocessedMessageCount;

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

const std::deque<ui64>& TStorage::GetDLQMessages() const {
    return DLQQueue;
}

std::pair<TStorage::TMessage*, bool> TStorage::GetMessageInt(ui64 offset, EMessageStatus expectedStatus) {
    auto [message, slowZone] = GetMessageInt(offset);
    if (!message) {
        return {nullptr, false};
    }

    if (message->Status != expectedStatus) {
        return {nullptr, slowZone};
    }

    return {message, slowZone};
}

ui64 TStorage::NormalizeDeadline(TInstant deadline) {
    auto now = TimeProvider->Now();
    if (deadline <= now) {
        return 0;
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
    AFL_VERIFY(message.Status == EMessageStatus::Unprocessed)("status", message.Status);
    message.Status = EMessageStatus::Locked;
    message.DeadlineDelta = NormalizeDeadline(deadline);
    ++message.ReceiveCount;

    Batch.AddChange(offset);

    if (KeepMessageOrder && message.HasMessageGroupId) {
        LockedMessageGroupsId.insert(message.MessageGroupIdHash);

        ++Metrics.LockedMessageGroupCount;
    }

    ++Metrics.LockedMessageCount;
    --Metrics.UnprocessedMessageCount;

    return offset;
}

bool TStorage::DoCommit(ui64 offset) {
    auto [message, slowZone] = GetMessageInt(offset);
    if (!message) {
        return false;
    }

    switch(message->Status) {
        case EMessageStatus::Unprocessed:
            if (!slowZone) {
                Batch.AddChange(offset);
                ++Metrics.CommittedMessageCount;
            }
            --Metrics.UnprocessedMessageCount;
            break;
        case EMessageStatus::Locked:
            if (!slowZone) {
                Batch.AddChange(offset);
                ++Metrics.CommittedMessageCount;
            }

            --Metrics.LockedMessageCount;
            if (KeepMessageOrder && message->HasMessageGroupId) {
                if (LockedMessageGroupsId.erase(message->MessageGroupIdHash)) {
                    --Metrics.LockedMessageGroupCount;
                }
            }

            break;
        case EMessageStatus::Committed:
            return false;
        case EMessageStatus::DLQ:
            if (!slowZone) {
                Batch.AddChange(offset);
                ++Metrics.CommittedMessageCount;
            }

            --Metrics.DLQMessageCount;
            break;
    }

    if (slowZone) {
        SlowMessages.erase(offset);
        Batch.DeleteFromSlow(offset);
        --Metrics.InflyMessageCount;
    } else {
        message->Status = EMessageStatus::Committed;
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

void TStorage::DoUnlock( ui64 offset, TMessage& message) {
    message.Status = EMessageStatus::Unprocessed;
    message.DeadlineDelta = 0;

    if (KeepMessageOrder && message.HasMessageGroupId) {
        if (LockedMessageGroupsId.erase(message.MessageGroupIdHash)) {
            --Metrics.LockedMessageGroupCount;
        }
    }

    Batch.AddChange(offset);

    --Metrics.LockedMessageCount;

    if (message.ReceiveCount >= MaxMessageProcessingCount) {
        // TODO Move to DLQ or remove message
        message.Status = EMessageStatus::DLQ;
        DLQQueue.push_back(offset);
        Batch.AddDLQ(offset);

        ++Metrics.DLQMessageCount;
    } else {
        if (offset >= FirstOffset) {
            FirstUnlockedOffset = std::min(FirstUnlockedOffset, offset);
        }
        
        ++Metrics.UnprocessedMessageCount;
    }
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
    while (offsetDelta < Messages.size() && Messages[offsetDelta].Status == EMessageStatus::Committed) {
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

const TStorage::TMetrics& TStorage::GetMetrics() const {
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
            << message.MessageGroupIdHash << "} ";
    };

    for (auto& [offset, message] : SlowMessages) {
        dump(offset, message, 's');
    }
    for (size_t i = 0; i < Messages.size(); ++i) {
        dump(FirstOffset + i, Messages[i], 'f');
    }

    sb << "] LockedGroups [" << JoinRange(", ", LockedMessageGroupsId.begin(), LockedMessageGroupsId.end()) << "]";
    return sb;
}

TStorage::TBatch::TBatch(TStorage* storage)
    : Storage(storage)
{
}

void TStorage::TBatch::AddChange(ui64 offset) {
    ChangedMessages.push_back(offset);
}

void TStorage::TBatch::AddDLQ(ui64 offset) {
    DLQ.push_back(offset);
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
        && DLQ.empty()
        && !BaseDeadline.has_value()
        && !BaseWriteTimestamp.has_value()
        && MovedToSlowZone.empty()
        && DeletedFromSlowZone.empty()
        && CompactedMessages == 0;
}

size_t TStorage::TBatch::AddedMessageCount() const {
    return NewMessageCount;
}

size_t TStorage::TBatch::ChangedMessageCount() const {
    return ChangedMessages.size();
}

size_t TStorage::TBatch::DLQMessageCount() const {
    return DLQ.size();
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
        .ProcessingCount = message->ReceiveCount,
        .ProcessingDeadline = static_cast<EMessageStatus>(message->Status) == EMessageStatus::Locked ?
            Storage.BaseDeadline + TDuration::Seconds(message->DeadlineDelta) : TInstant::Zero(),
        .WriteTimestamp = Storage.BaseWriteTimestamp + TDuration::Seconds(message->WriteTimestampDelta),
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

} // namespace NKikimr::NPQ::NMLP
