#pragma once

#include <ydb/public/sdk/cpp/client/ydb_types/status_codes.h>
#include <ydb/public/sdk/cpp/client/ydb_types/status/status.h>

namespace NYdb {
using TPeriodicCb = std::function<bool(NYql::TIssues&&, EStatus)>;

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
