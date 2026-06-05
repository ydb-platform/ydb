#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status_codes.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/time/time.h>

#include <memory>

namespace NYdb::inline Dev {

using TPeriodicCb = std::function<bool(NYdb::NIssue::TIssues&&, EStatus)>;
using TPostTaskCb = std::function<void()>;

// Allows to communicate with sdk core
class ICoreFacility {
public:
    virtual ~ICoreFacility() = default;
    // Add task to execute periodicaly
    // Task should return false to stop execution
    virtual void AddPeriodicTask(TPeriodicCb&& cb, TDeadline::Duration period) = 0;
    // Post task on SDK response executor.
    virtual void PostToResponseQueue(TPostTaskCb&& f) = 0;
};

// Self-contained single-threaded ICoreFacility for cases when the SDK core is not available
// (e.g. when a credentials provider factory's deprecated no-arg CreateProvider() is invoked
// directly without an enclosing TDriver).
std::shared_ptr<ICoreFacility> CreateSimpleCoreFacility();

} // namespace NYdb
