#pragma once

#include "task_queue.h"

namespace NYdb::NTPCC::NTest {

struct TTestResult {
    enum EStatus {
        E_OK,
        E_ERROR
    };

    TTestResult(EStatus status = E_OK)
        : Status(status)
    {
    }

    EStatus Status;
};

using TTestTask = TTask<TTestResult>;

} // namespace NYdb::NTPCC::NTest
