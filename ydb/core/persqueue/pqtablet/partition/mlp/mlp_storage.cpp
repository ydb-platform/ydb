#include "mlp_storage.h"

#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NPQ::NMLP {

TStorage::TStorage(TIntrusivePtr<ITimeProvider> timeProvider)
    : TimeProvider(timeProvider)
    , Batch(this)
{
    BaseDeadline = timeProvider->Now();
}

void TStorage::SetKeepMessageOrder(bool keepMessageOrder) {
    KeepMessageOrder = keepMessageOrder;
}

void TStorage::SetMaxMessageReceiveCount(ui32 maxMessageReceiveCount) {
    MaxMessageReceiveCount = maxMessageReceiveCount;
}

std::optional<TStorage::NextResult> TStorage::Next(TInstant deadline, ui64 fromOffset) {
    bool moveUnlockedOffset = fromOffset <= FirstUnlockedOffset;
    for (size_t i = std::max(fromOffset, FirstUnlockedOffset) - FirstOffset; i < Messages.size(); ++i) {
        const auto& message = Messages[i];
        if (message.Status == EMessageStatus::Unprocessed) {
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

bool TStorage::Commit(TMessageId messageId) {
    return DoCommit(messageId);
}

bool TStorage::Unlock(TMessageId messageId) {
    return DoUnlock(messageId);
}

bool TStorage::ChangeMessageDeadline(TMessageId messageId, TInstant deadline) {
    auto* message = GetMessageInt(messageId, EMessageStatus::Locked);
    if (!message) {
        return false;
    }

    Batch.AddChange(messageId);

    auto newDeadlineDelta = NormalizeDeadline(deadline);
    message->DeadlineDelta = newDeadlineDelta;

    return true;
}

TInstant TStorage::GetMessageDeadline(TMessageId messageId) {
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

    if (FirstOffset == FirstUncommittedOffset) {
        return 0;
    }

    size_t removed = 0;
    while(FirstOffset < FirstUncommittedOffset) {
        Messages.pop_front();
        ++FirstOffset;
        --Metrics.InflyMessageCount;
        --Metrics.CommittedMessageCount;
        ++removed;
    }

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
        BaseWriteTimestamp = writeTimestamp;
    }

    Messages.push_back({
        .Status = EMessageStatus::Unprocessed,
        .ReceiveCount = 0,
        .DeadlineDelta = 0,
        .HasMessageGroupId = hasMessagegroup,
        .MessageGroupIdHash = messageGroupIdHash,
        .WriteTimestampDelta = (writeTimestamp - BaseWriteTimestamp).Seconds()
    });

    Batch.AddNewMessage(offset);

    ++Metrics.InflyMessageCount;
    ++Metrics.UnprocessedMessageCount;
}

TStorage::TMessage* TStorage::GetMessageInt(ui64 offset) {
    if (offset < FirstOffset) {
        return nullptr;
    }

    auto offsetDelta = offset - FirstOffset;
    if (offsetDelta >= Messages.size()) {
        return nullptr;
    }

    return &Messages[offsetDelta];
}

const TStorage::TMessage* TStorage::GetMessage(TMessageId message) {
    return GetMessageInt(message);
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

    auto deadlineDuration = deadline - BaseDeadline;
    auto deadlineDelta = deadlineDuration.Seconds() + (deadlineDuration.MilliSecondsOfSecond() ? 1 : 0);
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

TMessageId TStorage::DoLock(ui64 offsetDelta, TInstant deadline) {
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

    auto now = TimeProvider->Now();
    auto deadlineDiff = (now - BaseDeadline).Seconds();

    if (!deadlineDiff) {
        return;
    }

    auto writeTimestampDiff = Messages.front().WriteTimestampDelta;
    BaseWriteTimestamp += TDuration::Seconds(writeTimestampDiff);

    Batch.RequiredSnapshot = true;

    for (size_t i = 0; i < Messages.size(); ++i) {
        auto& message = Messages[i];
        message.DeadlineDelta = message.DeadlineDelta > deadlineDiff ? message.DeadlineDelta - deadlineDiff : 0;
        message.WriteTimestampDelta = message.WriteTimestampDelta > writeTimestampDiff ? message.WriteTimestampDelta - writeTimestampDiff : 0;
    }

    BaseDeadline = now;
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
            << Messages[i].DeadlineDelta << "} ";
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

size_t TStorage::TBatch::AffectedMessageCount() const {
    return ChangedMessages.size() + DLQ.size() + NewMessageCount;
}

bool TStorage::TBatch::GetRequiredSnapshot() const {
    return RequiredSnapshot;
}

} // namespace NKikimr::NPQ::NMLP
