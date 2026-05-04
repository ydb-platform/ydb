#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/core_facility/core_facility.h>

#include <util/system/yassert.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>

namespace NYdb::inline Dev {

class TTestCoreFacility final : public ICoreFacility {
public:
    TTestCoreFacility();
    ~TTestCoreFacility() override;

    void AddPeriodicTask(TPeriodicCb&& cb, TDeadline::Duration period) override;
    void PostToResponseQueue(TPostTaskCb&& f) override;

    TTestCoreFacility(const TTestCoreFacility&) = delete;
    TTestCoreFacility& operator=(const TTestCoreFacility&) = delete;

private:
    void ResponseWorkerLoop();
    void PeriodicWorkerLoop(TDeadline::Duration period);

    std::mutex QueueMutex_;
    std::condition_variable QueueCv_;
    std::queue<TPostTaskCb> Queue_;
    bool StopQueue_ = false;

    std::mutex PeriodicMutex_;
    bool PeriodicStarted_ = false;
    TPeriodicCb PeriodicCb_;
    std::atomic<bool> StopPeriodic_{false};

    std::thread ResponseThread_;
    std::thread PeriodicThread_;
};

} // namespace NYdb::inline Dev
