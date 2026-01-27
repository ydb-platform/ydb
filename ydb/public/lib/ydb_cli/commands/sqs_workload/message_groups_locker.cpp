#include "message_groups_locker.h"

namespace NYdb::NConsoleClient {

void TMessageGroupsLocker::Lock(const std::unordered_set<Aws::String>& messageGroups) {
    std::unique_lock lock(MessageGroupsMutex);
    std::unordered_map<Aws::String, ui64> messageGroupsTickets;
    for (const auto& messageGroupId : messageGroups) {
        auto [it, wasInserted] = MessageGroupsFreeTickets.try_emplace(messageGroupId, 0);
        if (wasInserted) {
            MessageGroupsCurrentTickets[messageGroupId] = 0;
        }

        messageGroupsTickets[messageGroupId] = it->second++;
    }

    for (auto& [messageGroupId, ticket] : messageGroupsTickets) {
        while (MessageGroupsCurrentTickets[messageGroupId] < ticket) {
            MessageGroupsCondition.wait(lock);
        }
    }
}

void TMessageGroupsLocker::Unlock(const std::unordered_set<Aws::String>& messageGroups) {
    std::unique_lock lock(MessageGroupsMutex);
    for (const auto& messageGroupId : messageGroups) {
        MessageGroupsCurrentTickets[messageGroupId]++;
    }
    MessageGroupsCondition.notify_all();
}

} // namespace NYdb::NConsoleClient
