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
    return DoCommit(message.Offset, message.Cookie);
}

bool TStorage::Unlock(TMessageId message) {
    return DoUnlock(message.Offset, message.Cookie);
}

bool TStorage::ChangeMessageDeadline(TMessageId messageId, TInstant deadline) {
    auto [offsetDelta, message] = GetMessage(messageId.Offset, messageId.Cookie, EMessageStatus::Locked);
    if (!message) {
        return false;
    }

    LockedMessages.erase(LockedMessage(message->DeadlineDelta, offsetDelta));

    auto newDeadlineDelta = NormalizeDeadline(deadline);
    message->DeadlineDelta = newDeadlineDelta;
    LockedMessages.insert(LockedMessage(newDeadlineDelta, offsetDelta));

    return true;
}

bool TStorage::ProccessDeadlines() {
    auto deadlineDelta = (TInstant::Now() - BaseDeadline).Seconds();
    bool result = false;

    for (auto it = LockedMessages.begin(); it != LockedMessages.end(); ) {
        if (it->DeadlineDelta >= deadlineDelta) {
            break;
        }

        auto offsetDelta = it->OffsetDelta;
        it = LockedMessages.erase(it);
        DoUnlock(Messages[offsetDelta], FirstOffset + offsetDelta);
        result = true;
    }

    return result;
}

std::pair<ui64, TStorage::TMessage*> TStorage::GetMessage(ui64 offset, ui64 cookie, EMessageStatus expectedStatus) {
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

    if (message.Cookie != cookie) {
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
        UpdateDeltas(FirstOffset);
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
    message.Cookie = ++LockCookie;

    LockedMessages.emplace(deadlineDelta, offsetDelta);
    if (message.HasMessageGroupId) {
        LockedMessageGroupsId.insert(message.MessageGroupIdHash);
    }

    return TMessageId{
        .Offset = FirstOffset + offsetDelta,
        .Cookie = message.Cookie
    };
}

bool TStorage::DoCommit(ui64 offset, ui64 cookie) {
    auto [offsetDelta, message] = GetMessage(offset, cookie, EMessageStatus::Locked);
    if (!message) {
        return false;
    }

    message->Status = EMessageStatus::Committed;
    message->DeadlineDelta = 0;
    message->Cookie = 0;

    LockedMessages.erase(LockedMessage(offsetDelta, message->DeadlineDelta));
    if (message->HasMessageGroupId) {
        LockedMessageGroupsId.erase(message->MessageGroupIdHash);
    }

    UpdateFirstUncommittedOffset();

    return true;
}

bool TStorage::DoUnlock(ui64 offset, ui64 cookie) {
    auto [offsetDelta, message] = GetMessage(offset, cookie, EMessageStatus::Locked);
    if (!message) {
        return false;
    }

    LockedMessages.erase(LockedMessage(offsetDelta, message->DeadlineDelta));
    DoUnlock(*message, offset);

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

void TStorage::UpdateDeltas(const ui64 oldFirstOffset) {
    auto now = TInstant::Now();
    auto deadlineDiff = (now - BaseDeadline).Seconds();
    AFL_ENSURE(FirstOffset >= oldFirstOffset)("old", oldFirstOffset)("new", FirstOffset);
    auto offsetDiff = FirstOffset - oldFirstOffset;

    std::set<LockedMessage> newLockedMessages;
    for (auto& message : LockedMessages) {
        auto newDeadlineDelta = message.DeadlineDelta > deadlineDiff ? message.DeadlineDelta - deadlineDiff : 0;
        auto newOffsetDelta = message.OffsetDelta > offsetDiff ? message.OffsetDelta - offsetDiff : 0;

        Messages[newOffsetDelta].DeadlineDelta = newDeadlineDelta;
        newLockedMessages.emplace(newOffsetDelta, newDeadlineDelta);
    }

    BaseDeadline = now;
    LockedMessages = std::move(newLockedMessages);
}

void TStorage::UpdateFirstUncommittedOffset() {
    auto offsetDelta = FirstUncommittedOffset > FirstOffset ? FirstUncommittedOffset - FirstOffset : 0;
    while (offsetDelta < Messages.size() && Messages[offsetDelta].Status == EMessageStatus::Committed) {
        ++FirstUncommittedOffset;
    }
}

}
