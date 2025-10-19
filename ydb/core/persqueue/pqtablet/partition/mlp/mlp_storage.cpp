#include "mlp_storage.h"

#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NPQ::NMLP {

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

            return NextResult{
                .Message = DoLock(i, deadline),
                .FromOffset = FirstOffset + i + 1
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
    auto* message = GetMessage(messageId, EMessageStatus::Locked);
    if (!message) {
        return false;
    }

    auto newDeadlineDelta = NormalizeDeadline(deadline);
    message->DeadlineDelta = newDeadlineDelta;

    return true;
}

TInstant TStorage::GetMessageDeadline(TMessageId messageId) {
    auto* message = GetMessage(messageId, EMessageStatus::Locked);
    if (!message) {
        return TInstant::Zero();
    }

    return BaseDeadline + TDuration::Seconds(message->DeadlineDelta);
}

size_t TStorage::ProccessDeadlines() {
    auto deadlineDelta = (TInstant::Now() - BaseDeadline).Seconds();
    size_t count = 0;

    for (size_t i = 0; i < Messages.size(); ++i) {
        auto& message = Messages[i];
        if (message.Status == EMessageStatus::Locked && message.DeadlineDelta > deadlineDelta) {
            DoUnlock(message, FirstOffset + i);
            ++count;

            ++Metrics.DeadlineExpiredMessageCount;
        }
    }

    return count;
}

bool TStorage::Compact() {
    AFL_ENSURE(FirstOffset <= FirstUncommittedOffset)("l", FirstOffset)("r", FirstUncommittedOffset);
    AFL_ENSURE(FirstOffset <= FirstUnlockedOffset)("l", FirstOffset)("r", FirstUnlockedOffset);
    AFL_ENSURE(FirstUncommittedOffset <= FirstUnlockedOffset)("l", FirstUncommittedOffset)("r", FirstUnlockedOffset);

    if (FirstOffset = FirstUncommittedOffset) {
        return false;
    }

    while(FirstOffset < FirstUncommittedOffset) {
        Messages.pop_front();
        ++FirstOffset;
        --Metrics.InflyMessageCount;
    }

    return true;
}

void TStorage::AddMessage(ui64 offset, bool hasMessagegroup, ui32 messageGroupIdHash) {
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

    Messages.push_back({
        .Status = EMessageStatus::Unprocessed,
        .HasMessageGroupId = hasMessagegroup,
        .ReceiveCount = 0,
        .DeadlineDelta = 0,
        .MessageGroupIdHash = messageGroupIdHash,
    });

    ++Metrics.InflyMessageCount;
    ++Metrics.UnprocessedMessageCount;
}

bool TStorage::InitializeFromSnapshot(const NKikimrPQ::TMLPStorageSnapshot& snapshot) {
    AFL_ENSURE(snapshot.GetFormatVersion() == 1)("v", snapshot.GetFormatVersion());

    Messages.resize(snapshot.GetMessages().length() / sizeof(TMessage));

    auto& meta = snapshot.GetMeta();
    FirstOffset = meta.GetFirstOffset();
    FirstUncommittedOffset = meta.GetFirstUncommittedOffset();
    BaseDeadline = TInstant::MilliSeconds(meta.GetBaseDeadlineMilliseconds());

    const TMessage* ptr = reinterpret_cast<const TMessage*>(snapshot.GetMessages().data());
    for (size_t i = 0; i < Messages.size(); ++i) {
        auto& message = Messages[i] = ptr[i];  // TODO BIGENDIAN/LOWENDIAN

        switch(message.Status) {
            case EMessageStatus::Locked:
                ++Metrics.LockedMessageCount;
                if (KeepMessageOrder && message.HasMessageGroupId) {
                    LockedMessageGroupsId.insert(message.MessageGroupIdHash);
                    ++Metrics.LockedMessageGroupCount;
                }
                break;
            case EMessageStatus::Committed:
                ++Metrics.CommittedMessageCount;
                break;
            case EMessageStatus::Unprocessed:
                ++Metrics.UnprocessedMessageCount;
                break;

        }
    }

    Metrics.InflyMessageCount = Messages.size();

    return true;
}

bool TStorage::CreateSnapshot(NKikimrPQ::TMLPStorageSnapshot& snapshot) {
    auto* meta = snapshot.MutableMeta();
    meta->SetFirstOffset(FirstOffset);
    meta->SetFirstUncommittedOffset(FirstUncommittedOffset);
    meta->SetBaseDeadlineMilliseconds(BaseDeadline.MilliSeconds());

    snapshot.SetFormatVersion(1);

    TString buffer;
    buffer.reserve(Messages.size() * sizeof(TMessage));
    for (auto& message : Messages) {
        void* ptr = &message;
        buffer.append(static_cast<char*>(ptr), sizeof(TMessage)); // TODO BIGENDIAN/LOWENDIAN
    }
    snapshot.SetMessages(std::move(buffer));

    return true;
}

TStorage::TMessage* TStorage::GetMessage(ui64 offset) {
    if (offset < FirstOffset) {
        return nullptr;
    }

    auto offsetDelta = offset - FirstOffset;
    if (offsetDelta >= Messages.size()) {
        return nullptr;
    }

    return &Messages[offsetDelta];
}

TStorage::TMessage* TStorage::GetMessage(ui64 offset, EMessageStatus expectedStatus) {
    auto* message = GetMessage(offset);
    if (!message) {
        return nullptr;
    }

    if (message->Status != expectedStatus) {
        return nullptr;
    }

    return message;
}

ui64 TStorage::NormalizeDeadline(TInstant deadline) {
    auto now = TInstant::Now();
    if (deadline <= now) {
        return 0;
    }

    auto deadlineDuration = deadline - BaseDeadline;
    auto deadlineDelta = deadlineDuration.Seconds() + (deadlineDuration.MilliSecondsOfSecond() ? 1 : 0);
    if (deadlineDelta >= MaxDeadlineDelta) {
        UpdateDeltas();
        deadlineDelta = std::min((deadline - BaseDeadline).Seconds(), MaxDeadlineDelta - 1);
    }

    return deadlineDelta;
}

TMessageId TStorage::DoLock(ui64 offsetDelta, TInstant deadline) {
    auto& message = Messages[offsetDelta];
    AFL_VERIFY(message.Status == EMessageStatus::Unprocessed)("status", message.Status);
    message.Status = EMessageStatus::Locked;

    auto deadlineDelta = NormalizeDeadline(deadline);
 
    message.DeadlineDelta = deadlineDelta;
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
    auto* message = GetMessage(offset);
    if (!message) {
        return false;
    }

    switch(message->Status) {
        case EMessageStatus::Unprocessed:
            --Metrics.UnprocessedMessageCount;
            ++Metrics.CommittedMessageCount;
            break;
        case EMessageStatus::Locked:
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
    auto* message = GetMessage(offset, EMessageStatus::Locked);
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

    --Metrics.LockedMessageCount;

    if (message.ReceiveCount > MaxMessageReceiveCount) {
        // TODO Move to DLQ
        message.Status = EMessageStatus::DLQ;

        ++Metrics.DLQMessageCount;
    } else {
        FirstUnlockedOffset = std::min(FirstUnlockedOffset, offset);
        
        ++Metrics.UnprocessedMessageCount;
    }
}

void TStorage::UpdateDeltas() {
    auto now = TInstant::Now();
    auto deadlineDiff = (now - BaseDeadline).Seconds();

    for (auto& message : Messages) {
        message.DeadlineDelta += message.DeadlineDelta > deadlineDiff ? message.DeadlineDelta - deadlineDiff : 0;
    }

    BaseDeadline = now;
}

void TStorage::UpdateFirstUncommittedOffset() {
    auto offsetDelta = FirstUncommittedOffset > FirstOffset ? FirstUncommittedOffset - FirstOffset : 0;
    while (offsetDelta < Messages.size() && Messages[offsetDelta].Status == EMessageStatus::Committed) {
        ++FirstUncommittedOffset;
        ++offsetDelta;
    }
}

const TStorage::TMetrics& TStorage::GetMetrics() const {
    return Metrics;
}

ui64 TStorage::GetFirstOffset() const {
    return FirstOffset;
}

ui64 TStorage::GetLastOffset() const {
    return FirstOffset + Messages.size();
}

}
