#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/core_facility/core_facility.h>

namespace NYdb::inline Dev {

/// ICoreFacility adapter for the process-wide background runtime.
class TSimpleCoreFacility final : public ICoreFacility {
public:
    TSimpleCoreFacility() = default;

    void AddPeriodicTask(TPeriodicCb&& cb, TDeadline::Duration period) override;
    void PostToResponseQueue(TPostTaskCb&& f) override;

    TSimpleCoreFacility(const TSimpleCoreFacility&) = delete;
    TSimpleCoreFacility& operator=(const TSimpleCoreFacility&) = delete;
};

std::shared_ptr<ICoreFacility> CreateSimpleCoreFacility();

} // namespace NYdb::inline Dev
