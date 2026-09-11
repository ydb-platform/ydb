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
    // Post task on SDK response executor, never inline.
    virtual void PostToResponseQueue(TPostTaskCb&& f) = 0;
};

// Shared background runtime adapter for use without an enclosing TDriver.
// Releasing the facility does not stop tasks; periodic callbacks return false to finish.
std::shared_ptr<ICoreFacility> CreateSimpleCoreFacility();

} // namespace NYdb
