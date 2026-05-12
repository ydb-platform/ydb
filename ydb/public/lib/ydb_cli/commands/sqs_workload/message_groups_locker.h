#pragma once
#include <aws/core/utils/memory/stl/AWSString.h>
#include <util/system/types.h>
#include <unordered_map>
#include<unordered_set>
#include <mutex>
#include <condition_variable>


namespace NYdb::NConsoleClient {

class TMessageGroupsLocker {
public:
    TMessageGroupsLocker() = default;

    void Lock(const std::unordered_set<Aws::String>& messageGroups);
    void Unlock(const std::unordered_set<Aws::String>& messageGroups);

private:
    std::unordered_map<Aws::String, ui64> MessageGroupsFreeTickets;
    std::unordered_map<Aws::String, ui64> MessageGroupsCurrentTickets;
    std::mutex MessageGroupsMutex;
    std::condition_variable MessageGroupsCondition;
};

} // namespace NYdb::NConsoleClient