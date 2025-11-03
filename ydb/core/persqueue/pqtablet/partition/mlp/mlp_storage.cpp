#include "mlp_storage.h"

#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NPQ::NMLP {

namespace {

TInstant TrimToSeconds(TInstant time) {
    return TInstant::Seconds(time.Seconds() + (time.MilliSecondsOfSecond() > 0 ? 1 : 0));
}

}

TStorage::TStorage(TIntrusivePtr<ITimeProvider> timeProvider)
    : TimeProvider(timeProvider)
    , Batch(this)
{
    BaseDeadline = TrimToSeconds(timeProvider->Now());
}

void TStorage::SetKeepMessageOrder(bool keepMessageOrder) {
    KeepMessageOrder = keepMessageOrder;
}

void TStorage::SetMaxMessageReceiveCount(ui32 maxMessageReceiveCount) {
    MaxMessageReceiveCount = maxMessageReceiveCount;
}

void TStorage::SetReteintion(TDuration reteintion) {
    Reteintion = reteintion;
}

std::optional<TStorage::NextResult> TStorage::Next(TInstant deadline, ui64 fromOffset) {
    auto dieTime = TimeProvider->Now() - Reteintion;
    auto dieDelta = dieTime > BaseWriteTimestamp ? (dieTime - BaseWriteTimestamp).Seconds() : 0;

    bool moveUnlockedOffset = fromOffset <= FirstUnlockedOffset;
    for (size_t i = std::max(fromOffset, FirstUnlockedOffset) - FirstOffset; i < Messages.size(); ++i) {
        const auto& message = Messages[i];
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
            Batch.AddChange(offset);

            return NextResult{
                .Message = DoLock(i, deadline),
                .FromOffset = offset + 1
            };
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
    auto* message = GetMessageInt(messageId, EMessageStatus::Locked);
    if (!message) {
        return false;
    }

    Batch.AddChange(messageId);

    auto newDeadlineDelta = NormalizeDeadline(deadline);
    message->DeadlineDelta = newDeadlineDelta;

    return true;
}

TInstant TStorage::GetMessageDeadline(ui64 messageId) {
    auto* message = GetMessageInt(messageId, EMessageStatus::Locked);
    if (!message) {
        return TInstant::Zero();
    }

    return BaseDeadline + TDuration::Seconds(message->DeadlineDelta);
}

size_t TStorage::ProccessDeadlines() {
    auto deadlineDelta = (TimeProvider->Now() - BaseDeadline).Seconds();
    size_t count = 0;

    for (size_t i = 0; i < Messages.size(); ++i) {
        auto& message = Messages[i];
        if (message.Status == EMessageStatus::Locked && message.DeadlineDelta < deadlineDelta) {
            DoUnlock(message, FirstOffset + i);
            ++count;

            ++Metrics.DeadlineExpiredMessageCount;
        }
    }

    return count;
}

size_t TStorage::Compact() {
    AFL_ENSURE(FirstOffset <= FirstUncommittedOffset)("l", FirstOffset)("r", FirstUncommittedOffset);
    AFL_ENSURE(FirstOffset <= FirstUnlockedOffset)("l", FirstOffset)("r", FirstUnlockedOffset);
    AFL_ENSURE(FirstUncommittedOffset <= FirstUnlockedOffset)("l", FirstUncommittedOffset)("r", FirstUnlockedOffset);

    size_t removed = 0;

    auto dieTime = TimeProvider->Now() - Reteintion;
    if (dieTime > BaseWriteTimestamp) {
        auto dieDelta = (dieTime - BaseWriteTimestamp).Seconds();
        while (!Messages.empty() && dieDelta > Messages.front().WriteTimestampDelta && Messages.front().Status != EMessageStatus::Locked) {
            auto& message = Messages.front();

            ++FirstOffset;
            --Metrics.InflyMessageCount;

            switch (message.Status) {
                case EMessageStatus::Unprocessed:
                    --Metrics.UnprocessedMessageCount;
                    break;
                case EMessageStatus::Locked:
                    --Metrics.LockedMessageCount;
                    break;
                case EMessageStatus::Committed:
                    --Metrics.CommittedMessageCount;
                    break;
                case EMessageStatus::DLQ:
                    --Metrics.DLQMessageCount;
                    break;
            }

            ++removed;

           Messages.pop_front();
        }
    }

    while(!Messages.empty() && FirstOffset < FirstUncommittedOffset) {
        ++FirstOffset;
        --Metrics.InflyMessageCount;
        --Metrics.CommittedMessageCount;
        ++removed;

        Messages.pop_front();
    }

    FirstUnlockedOffset = std::max(FirstUnlockedOffset, FirstOffset);
    FirstUncommittedOffset = std::max(FirstUncommittedOffset, FirstOffset);

    return removed;
}

void TStorage::AddMessage(ui64 offset, bool hasMessagegroup, ui32 messageGroupIdHash, TInstant writeTimestamp) {
    AFL_ENSURE(offset >= GetLastOffset())("l", offset)("r", GetLastOffset());

    while (!Messages.empty() && offset > GetLastOffset()) {
        auto message = Messages.front();

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
                break;
        }

        Messages.pop_front();
        ++FirstOffset;
    }

    if (Messages.empty()) {
        FirstOffset = offset;
    }

    FirstUnlockedOffset = std::max(FirstUnlockedOffset, FirstOffset);
    FirstUncommittedOffset = std::max(FirstUncommittedOffset, FirstOffset);

    if (BaseWriteTimestamp == TInstant::Zero()) {
        BaseWriteTimestamp = TrimToSeconds(writeTimestamp);
        Batch.MoveBaseTime(BaseDeadline, BaseWriteTimestamp);
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
}

const TStorage::TMessage* TStorage::GetMessageInt(ui64 offset) const {
    if (offset < FirstOffset) {
        return nullptr;
    }

    auto offsetDelta = offset - FirstOffset;
    if (offsetDelta >= Messages.size()) {
        return nullptr;
    }

    return &Messages.at(offsetDelta);
}

TStorage::TMessage* TStorage::GetMessageInt(ui64 offset) {
    if (offset < FirstOffset) {
        return nullptr;
    }

    auto offsetDelta = offset - FirstOffset;
    if (offsetDelta >= Messages.size()) {
        return nullptr;
    }

    return &Messages.at(offsetDelta);
}

const TStorage::TMessage* TStorage::GetMessage(ui64 message) {
    return GetMessageInt(message);
}

const std::deque<ui64>& TStorage::GetDLQMessages() const {
    return DLQQueue;
}

TStorage::TMessage* TStorage::GetMessageInt(ui64 offset, EMessageStatus expectedStatus) {
    auto* message = GetMessageInt(offset);
    if (!message) {
        return nullptr;
    }

    if (message->Status != expectedStatus) {
        return nullptr;
    }

    return message;
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

ui64 TStorage::DoLock(ui64 offsetDelta, TInstant deadline) {
    auto& message = Messages[offsetDelta];
    AFL_VERIFY(message.Status == EMessageStatus::Unprocessed)("status", message.Status);
    message.Status = EMessageStatus::Locked;

    message.DeadlineDelta = NormalizeDeadline(deadline);
    ++message.ReceiveCount;

    if (KeepMessageOrder && message.HasMessageGroupId) {
        LockedMessageGroupsId.insert(message.MessageGroupIdHash);

        ++Metrics.LockedMessageGroupCount;
    }

    ++Metrics.LockedMessageCount;
    --Metrics.UnprocessedMessageCount;

    return FirstOffset + offsetDelta;
}

bool TStorage::DoCommit(ui64 offset) {
    auto* message = GetMessageInt(offset);
    if (!message) {
        return false;
    }

    switch(message->Status) {
        case EMessageStatus::Unprocessed:
            Batch.AddChange(offset);

            --Metrics.UnprocessedMessageCount;
            ++Metrics.CommittedMessageCount;
            break;
        case EMessageStatus::Locked:
            Batch.AddChange(offset);

            --Metrics.LockedMessageCount;
            if (KeepMessageOrder && message->HasMessageGroupId) {
                if (LockedMessageGroupsId.erase(message->MessageGroupIdHash)) {
                    --Metrics.LockedMessageGroupCount;
                }
            }
            ++Metrics.CommittedMessageCount;
            break;
        case EMessageStatus::Committed:
            return false;
        case EMessageStatus::DLQ:
            break;
    }

    message->Status = EMessageStatus::Committed;
    message->DeadlineDelta = 0;

    UpdateFirstUncommittedOffset();

    return true;
}

bool TStorage::DoUnlock(ui64 offset) {
    auto* message = GetMessageInt(offset, EMessageStatus::Locked);
    if (!message) {
        return false;
    }

    DoUnlock(*message, offset);

    return true;
}

void TStorage::DoUnlock(TMessage& message, ui64 offset) {
    message.Status = EMessageStatus::Unprocessed;
    message.DeadlineDelta = 0;

    if (KeepMessageOrder && message.HasMessageGroupId) {
        if (LockedMessageGroupsId.erase(message.MessageGroupIdHash)) {
            --Metrics.LockedMessageGroupCount;
        }
    }

    Batch.AddChange(offset);

    --Metrics.LockedMessageCount;

    if (message.ReceiveCount >= MaxMessageReceiveCount) {
        // TODO Move to DLQ
        message.Status = EMessageStatus::DLQ;
        DLQQueue.push_back(offset);
        Batch.AddDLQ(offset);

        ++Metrics.DLQMessageCount;
    } else {
        FirstUnlockedOffset = std::min(FirstUnlockedOffset, offset);
        
        ++Metrics.UnprocessedMessageCount;
    }
}

void TStorage::MoveBaseDeadline() {
    if (Messages.empty()) {
        return;
    }

    auto newBaseDeadline = TrimToSeconds(TimeProvider->Now());
    auto newBaseWriteTimestamp = BaseWriteTimestamp + TDuration::Seconds(Messages.front().WriteTimestampDelta);

    MoveBaseDeadline(newBaseDeadline, newBaseWriteTimestamp);

    Batch.MoveBaseTime(BaseDeadline, BaseWriteTimestamp);
}

void TStorage::MoveBaseDeadline(TInstant newBaseDeadline, TInstant newBaseWriteTimestamp) {
    auto deadlineDiff = (newBaseDeadline - BaseDeadline).Seconds();
    auto writeTimestampDiff = (newBaseWriteTimestamp - BaseWriteTimestamp).Seconds();

    for (size_t i = 0; i < Messages.size(); ++i) {
        auto& message = Messages[i];
        message.DeadlineDelta = message.DeadlineDelta > deadlineDiff ? message.DeadlineDelta - deadlineDiff : 0;
        message.WriteTimestampDelta = message.WriteTimestampDelta > writeTimestampDiff ? message.WriteTimestampDelta - writeTimestampDiff : 0;
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
    return Messages.size();
}

ui64 TStorage::GetLastOffset() const {
    return FirstOffset + Messages.size();
}

ui64 TStorage::GetFirstUncommittedOffset() const {
    return FirstUncommittedOffset;
}

ui64 TStorage::GetFirstUnlockedOffset() const {
    return FirstUnlockedOffset;
}

TInstant TStorage::GetBaseDeadline() const {
    return BaseDeadline;
}

TString TStorage::DebugString() const {
    TStringBuilder sb;
    sb << "FirstOffset: " << FirstOffset
         << " FirstUncommittedOffset: " << FirstUncommittedOffset
         << " FirstUnlockedOffset: " << FirstUnlockedOffset
         << " BaseDeadline: " << BaseDeadline.ToString()
         << " Messages: [";

    for (size_t i = 0; i < Messages.size(); ++i) {
        sb << "{" << (FirstOffset + i) << ", "
            << static_cast<EMessageStatus>(Messages[i].Status) << ", "
            << Messages[i].DeadlineDelta << ", "
            << Messages[i].WriteTimestampDelta << "} ";
    }

    sb << "]";
    return sb;
}

TStorage::TBatch::TBatch(TStorage* storage)
    : Storage(storage)
{
}

void TStorage::TBatch::AddChange(ui64 offset) {
    ChangedMessages.insert(offset);
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

void TStorage::TBatch::MoveBaseTime(TInstant baseDeadline, TInstant baseWriteTimestamp) {
    BaseDeadline = baseDeadline;
    BaseWriteTimestamp = baseWriteTimestamp;
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

TStorage::TMessageIterator::TMessageIterator(const TStorage& storage, ui64 offset)
    : Storage(storage)
    , Offset(offset)
{
}

TStorage::TMessageIterator& TStorage::TMessageIterator::operator++() {
    ++Offset;
    return *this;
}

TStorage::TMessageWrapper TStorage::TMessageIterator::operator*() const {
    auto *message = Storage.GetMessageInt(Offset);

    return TMessageWrapper{
        .Offset = Offset,
        .Status = static_cast<EMessageStatus>(message->Status),
        .ProcessingCount = message->ReceiveCount,
        .ProcessingDeadline = static_cast<EMessageStatus>(message->Status) == EMessageStatus::Locked ?
            Storage.BaseDeadline + TDuration::Seconds(message->DeadlineDelta) : TInstant::Zero(),
        .WriteTimestamp = Storage.BaseWriteTimestamp + TDuration::Seconds(message->WriteTimestampDelta),
    };
}

bool TStorage::TMessageIterator::operator==(const TStorage::TMessageIterator& other) const {
    return Offset == other.Offset;
}

TStorage::TMessageIterator TStorage::begin() const {
    return TMessageIterator(*this, FirstOffset);
}

TStorage::TMessageIterator TStorage::end() const {
    return TMessageIterator(*this, FirstOffset + Messages.size());
}

} // namespace NKikimr::NPQ::NMLP
