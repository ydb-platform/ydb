#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/core_facility/core_facility.h>

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <thread>
#include <vector>

namespace NYdb::inline Dev {

/// Single-threaded ICoreFacility: one worker, priority queue by execution time
/// (immediate and delayed tasks, including periodic scheduling).
class TSimpleCoreFacility final : public ICoreFacility {
public:
    TSimpleCoreFacility();
    ~TSimpleCoreFacility() override;

    void AddPeriodicTask(TPeriodicCb&& cb, TDeadline::Duration period) override;
    void PostToResponseQueue(TPostTaskCb&& f) override;

    TSimpleCoreFacility(const TSimpleCoreFacility&) = delete;
    TSimpleCoreFacility& operator=(const TSimpleCoreFacility&) = delete;

private:
    using TClock = std::chrono::steady_clock;
    using TTimePoint = TClock::time_point;

    struct TScheduledTask {
        TTimePoint ExecuteAt;
        std::uint64_t SeqNo;
        TPostTaskCb Task;
    };

    struct TScheduledTaskLess {
        bool operator()(const TScheduledTask& l, const TScheduledTask& r) const;
    };

    void EnqueueTaskNoLock(TTimePoint executeAt, TPostTaskCb&& task);
    void SchedulePeriodic(std::shared_ptr<TPeriodicCb> periodicCb, TDeadline::Duration period);
    std::optional<TTimePoint> DrainReadyTasks(std::vector<TPostTaskCb>& ready);
    void WorkerLoop();

    std::mutex Mutex_;
    std::condition_variable Cv_;
    std::priority_queue<TScheduledTask, std::vector<TScheduledTask>, TScheduledTaskLess> Queue_;
    bool Stop_ = false;
    bool PeriodicStarted_ = false;
    std::uint64_t NextSeqNo_ = 0;

    std::thread WorkerThread_;
};

std::shared_ptr<ICoreFacility> CreateSimpleCoreFacility();

} // namespace NYdb::inline Dev
