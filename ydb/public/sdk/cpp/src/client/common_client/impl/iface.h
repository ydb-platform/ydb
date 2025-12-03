#pragma once

#include <ydb/public/sdk/cpp/src/library/time/time.h>

#include <functional>

namespace NYdb::inline Dev {

class IClientImplCommon {
public:
    virtual ~IClientImplCommon() = default;
    virtual void ScheduleTask(const std::function<void()>& fn, TDeadline::Duration timeout) = 0;
};

}
