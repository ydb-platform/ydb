#include "mlp_storage.h"

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NPQ::NMLP {


std::optional<ui64> TStorage::Next(TDuration deadline) {
    if (!ReleasedMessages.empty()) {
        auto offset = ReleasedMessages.front();
        ReleasedMessages.pop_front();
        return DoLock(offset - FirstOffset, deadline);
    }

    bool moveUnlockedOffset = true;
    for (size_t i = FirstUnlockedOffset; i < Messages.size(); ++i) {
        const auto& message = Messages[i];
        const size_t offset = FirstOffset + i;
        if (message.Status == EMessageStatus::Unprocessed) {
            if (message.HasMessageGroupId && LockedMessageGroupsId.contains(message.MessageGroupIdHash)) {
                moveUnlockedOffset = false;
                continue;
            }

            if (moveUnlockedOffset) {
                FirstUnlockedOffset = offset + 1;
            }

            return DoLock(i, deadline);
        } else if (moveUnlockedOffset) {
            FirstUnlockedOffset = offset + 1;
        }
    }

    return std::nullopt;
}

bool TStorage::Commit(ui64 offset) {
    return DoCommit(offset);
}

bool TStorage::Unlock(ui64 offset) {
    return DoUnlock(offset);
}

bool TStorage::ProccessDeadlines() {
    auto deadlineDelta = (TInstant::Now() - BaseDeadline).Seconds();
    bool result = false;

    for (auto it = LockedMessages.begin(); it != LockedMessages.end(); ) {
        if (it->DeadlineDelta > deadlineDelta) {
            break;
        }

        auto offsetDelta = it->OffsetDelta;
        it = LockedMessages.erase(it);
        DoUnlock(Messages[offsetDelta], FirstOffset + offsetDelta);
        result = true;
    }

    return result;
}

ui64 TStorage::DoLock(ui64 offsetDelta, TDuration deadline) {
    auto& message = Messages[offsetDelta];
    AFL_VERIFY(message.Status == EMessageStatus::Unprocessed)("status", message.Status);
    message.Status = EMessageStatus::Locked;

    auto deadlineDelta = (deadline - BaseDeadline).Seconds();
    if (deadlineDelta >= MaxDeadlineDelta) {
        UpdateBaseDeadline();
        deadlineDelta = std::min((deadline - BaseDeadline).Seconds(), MaxDeadlineDelta - 1);
    }
 
    message.DeadlineDelta = deadlineDelta;
    message.Cookie = ++LockCookie;

    LockedMessages.emplace(deadlineDelta, offsetDelta);
    if (message.HasMessageGroupId) {
        LockedMessageGroupsId.insert(message.MessageGroupIdHash);
    }

    return FirstOffset + offsetDelta;
}

bool TStorage::DoCommit(ui64 offset) {
    if (offset < FirstOffset) {
        return false;
    }

    auto offsetDelta = offset - FirstOffset;
    if (offsetDelta >= Messages.size()) {
        return false;
    }
    
    auto& message = Messages[offsetDelta];
    if (message.Status != EMessageStatus::Locked) {
        return false;
    }

    message.Status = EMessageStatus::Committed;
    message.DeadlineDelta = 0;
    message.Cookie = 0;

    LockedMessages.erase(LockedMessage(offsetDelta, message.DeadlineDelta));
    if (message.HasMessageGroupId) {
        LockedMessageGroupsId.erase(message.MessageGroupIdHash);
    }

    UpdateFirstUncommittedOffset();

    return true;
}

bool TStorage::DoUnlock(ui64 offset) {
    if (offset < FirstOffset) {
        return false;
    }

    auto offsetDelta = offset - FirstOffset;
    if (offsetDelta >= Messages.size()) {
        return false;
    }
    
    auto& message = Messages[offsetDelta];
    if (message.Status != EMessageStatus::Locked) {
        return false;
    }

    LockedMessages.erase(LockedMessage(offsetDelta, message.DeadlineDelta));
    DoUnlock(message, offset);

    return true;
}

void TStorage::DoUnlock(TMessage& message, ui64 offset) {
    message.Status = EMessageStatus::Unprocessed;
    message.DeadlineDelta = 0;
    message.Cookie = 0;

    if (message.HasMessageGroupId) {
        LockedMessageGroupsId.erase(message.MessageGroupIdHash);
    }

    if (ReleasedMessages.size() < MaxReleasedSize) {
        ReleasedMessages.push_back(offset);
    } else {
        FirstUnlockedOffset = std::min(FirstUnlockedOffset, offset);
    }
}

void TStorage::UpdateBaseDeadline() {
    auto now = TInstant::Now();
    auto diff = (now - BaseDeadline).Seconds();

    std::set<LockedMessage> newLockedMessages;
    for (auto& message : LockedMessages) {
        auto newDeadlineDelta = message.DeadlineDelta > diff ? message.DeadlineDelta - diff : 0;
        Messages[message.OffsetDelta].DeadlineDelta = newDeadlineDelta;
        newLockedMessages.emplace(message.OffsetDelta, newDeadlineDelta);
    }

    LockedMessages = std::move(newLockedMessages);
}

void TStorage::UpdateFirstUncommittedOffset() {
    auto offsetDelta = FirstUncommittedOffset > FirstOffset ? FirstUncommittedOffset - FirstOffset : 0;
    while (offsetDelta < Messages.size() && Messages[offsetDelta].Status == EMessageStatus::Committed) {
        ++FirstUncommittedOffset;
    }
}

}
