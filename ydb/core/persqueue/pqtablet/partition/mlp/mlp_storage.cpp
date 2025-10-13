#include "mlp_storage.h"

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NPQ::NMLP {


std::optional<TStorage::TMessageId> TStorage::Next(TInstant deadline, ui64 fromOffset) {
    if (!ReleasedMessages.empty()) {
        auto offset = ReleasedMessages.front();
        ReleasedMessages.pop_front();
        return DoLock(offset - FirstOffset, deadline);
    }

    bool moveUnlockedOffset = fromOffset <= FirstUnlockedOffset;
    for (size_t i = std::max(fromOffset, FirstUnlockedOffset) - FirstOffset; i < Messages.size(); ++i) {
        const auto& message = Messages[i];
        if (message.Status == EMessageStatus::Unprocessed) {
            if (message.HasMessageGroupId && LockedMessageGroupsId.contains(message.MessageGroupIdHash)) {
                moveUnlockedOffset = false;
                continue;
            }

            if (moveUnlockedOffset) {
                ++FirstUnlockedOffset;
            }

            return DoLock(i, deadline);
        } else if (moveUnlockedOffset) {
            ++FirstUnlockedOffset;
        }
    }

    return std::nullopt;
}

bool TStorage::Commit(TMessageId message) {
    return DoCommit(message.Offset);
}

bool TStorage::Unlock(TMessageId message) {
    return DoUnlock(message.Offset);
}

bool TStorage::ChangeMessageDeadline(TMessageId messageId, TInstant deadline) {
    auto [offsetDelta, message] = GetMessage(messageId.Offset, EMessageStatus::Locked);
    if (!message) {
        return false;
    }

    auto newDeadlineDelta = NormalizeDeadline(deadline);
    message->DeadlineDelta = newDeadlineDelta;

    return true;
}

bool TStorage::ProccessDeadlines() {
    auto deadlineDelta = (TInstant::Now() - BaseDeadline).Seconds();
    bool result = false;

    for (size_t i = 0; i < Messages.size(); ++i) {
        auto& message = Messages[i];
        if (message.Status == EMessageStatus::Locked && message.DeadlineDelta > deadlineDelta) {
            DoUnlock(message, FirstOffset + i);
            result = true;
        }
    }

    return result;
}

std::pair<ui64, TStorage::TMessage*> TStorage::GetMessage(ui64 offset, EMessageStatus expectedStatus) {
    if (offset < FirstOffset) {
        return {0, nullptr};
    }

    auto offsetDelta = offset - FirstOffset;
    if (offsetDelta >= Messages.size()) {
        return {0, nullptr};
    }

    auto& message = Messages[offsetDelta];
    if (message.Status != expectedStatus) {
        return {0, nullptr};
    }

    return {offsetDelta, &message};
}

ui64 TStorage::NormalizeDeadline(TInstant deadline) {
    auto now = TInstant::Now();
    if (deadline <= now) {
        return 0;
    }

    auto deadlineDelta = (deadline - BaseDeadline).Seconds();
    if (deadlineDelta >= MaxDeadlineDelta) {
        UpdateDeltas();
        deadlineDelta = std::min((deadline - BaseDeadline).Seconds(), MaxDeadlineDelta - 1);
    }

    return deadlineDelta;
}

TStorage::TMessageId TStorage::DoLock(ui64 offsetDelta, TInstant deadline) {
    auto& message = Messages[offsetDelta];
    AFL_VERIFY(message.Status == EMessageStatus::Unprocessed)("status", message.Status);
    message.Status = EMessageStatus::Locked;

    auto deadlineDelta = NormalizeDeadline(deadline);
 
    message.DeadlineDelta = deadlineDelta;
    ++message.ReceiveCount;

    if (message.HasMessageGroupId) {
        LockedMessageGroupsId.insert(message.MessageGroupIdHash);
    }

    return TMessageId{
        .Offset = FirstOffset + offsetDelta,
    };
}

bool TStorage::DoCommit(ui64 offset) {
    auto [offsetDelta, message] = GetMessage(offset, EMessageStatus::Locked);
    if (!message) {
        return false;
    }

    message->Status = EMessageStatus::Committed;
    message->DeadlineDelta = 0;

    if (message->HasMessageGroupId) {
        LockedMessageGroupsId.erase(message->MessageGroupIdHash);
    }

    UpdateFirstUncommittedOffset();

    return true;
}

bool TStorage::DoUnlock(ui64 offset) {
    auto [offsetDelta, message] = GetMessage(offset, EMessageStatus::Locked);
    if (!message) {
        return false;
    }

    DoUnlock(*message, offset);

    return true;
}

void TStorage::DoUnlock(TMessage& message, ui64 offset) {
    message.Status = EMessageStatus::Unprocessed;
    message.DeadlineDelta = 0;

    if (message.HasMessageGroupId) {
        LockedMessageGroupsId.erase(message.MessageGroupIdHash);
    }

    if (message.ReceiveCount > 1000 /* TODO Value from config */) {
        // TODO Move to DLQ
        message.Status = EMessageStatus::DLQ;
    } else if (ReleasedMessages.size() < MaxReleasedMessageSize) {
        ReleasedMessages.push_back(offset);
    } else {
        FirstUnlockedOffset = std::min(FirstUnlockedOffset, offset);
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
    }
}

}
