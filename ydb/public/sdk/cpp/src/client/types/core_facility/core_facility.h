#pragma once

#include <ydb-cpp-sdk/client/types/status_codes.h>
#include <ydb-cpp-sdk/client/types/status/status.h>

namespace NYdb::inline V3 {
using TPeriodicCb = std::function<bool(NYdb::NIssue::TIssues&&, EStatus)>;

// !!!Experimental!!!
// Allows to communicate with sdk core
class ICoreFacility {
public:
    virtual ~ICoreFacility() = default;
    // Add task to execute periodicaly
    // Task should return false to stop execution
    virtual void AddPeriodicTask(TPeriodicCb&& cb, TDuration period) = 0;
};

} // namespace NYdb
