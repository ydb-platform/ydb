#include "mlp_storage.h"

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NPQ::NMLP {


std::optional<ui64> TStorage::Next(TDuration deadline) {
    if (!ReleasedMessages.empty()) {
        auto offset = ReleasedMessages.front();
        ReleasedMessages.pop_front();
        return DoLock(offset - FirstOffset, deadline);
    }

    for (size_t i = 0; i < Messages.size(); ++i) {
        const auto& message = Messages[i];
        if (message.Status == EMessageStatus::Unprocessed) {
            if (message.HasMessageGroupId && LockedMessageGroupsId.contains(message.MessageGroupIdHash)) {
                continue;
            }

            return DoLock(i, deadline);
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

    LockedMessages.erase(LockedMessage(offsetDelta, message.DeadlineDelta, 0));
    if (message.HasMessageGroupId) {
        LockedMessageGroupsId.erase(message.MessageGroupIdHash);
    }

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

    LockedMessages.erase(LockedMessage(offsetDelta, message.DeadlineDelta, 0));
    DoUnlock(message, offset);

    return true;
}

void TStorage::DoUnlock(TMessage& message, ui64 offset) {
    message.Status = EMessageStatus::Unprocessed;
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
        newLockedMessages.emplace(message.OffsetDelta, message.DeadlineDelta > diff ? message.DeadlineDelta - diff : 0);
    }

    LockedMessages = std::move(newLockedMessages);
}

}
