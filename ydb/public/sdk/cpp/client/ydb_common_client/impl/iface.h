#pragma once

#include <functional>
#include <util/datetime/base.h>

namespace NYdb::inline V2 {

class IClientImplCommon {
public:
    virtual ~IClientImplCommon() = default;
    virtual void ScheduleTask(const std::function<void()>& fn, TDuration timeout) = 0;
};

}
