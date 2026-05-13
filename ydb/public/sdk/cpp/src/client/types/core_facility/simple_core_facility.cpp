#include "simple_core_facility.h"

#include <util/system/yassert.h>

namespace NYdb::inline Dev {

bool TSimpleCoreFacility::TScheduledTaskLess::operator()(const TScheduledTask& l, const TScheduledTask& r) const {
    if (l.ExecuteAt != r.ExecuteAt) {
        return l.ExecuteAt > r.ExecuteAt;
    }
    return l.SeqNo > r.SeqNo;
}

TSimpleCoreFacility::TSimpleCoreFacility() {
    WorkerThread_ = std::thread([this] { WorkerLoop(); });
}

TSimpleCoreFacility::~TSimpleCoreFacility() {
    {
        std::lock_guard lock(Mutex_);
        Stop_ = true;
    }
    Cv_.notify_all();
    if (WorkerThread_.joinable()) {
        WorkerThread_.join();
    }
}

void TSimpleCoreFacility::AddPeriodicTask(TPeriodicCb&& cb, TDeadline::Duration period) {
    std::lock_guard lock(Mutex_);
    Y_ABORT_UNLESS(!PeriodicStarted_);
    PeriodicStarted_ = true;

    auto periodicCb = std::make_shared<TPeriodicCb>(std::move(cb));
    EnqueueTaskNoLock(
        TClock::now(),
        [this, periodicCb, period] {
            NYdb::NIssue::TIssues issues;
            const bool cont = (*periodicCb)(std::move(issues), EStatus::SUCCESS);
            if (cont) {
                SchedulePeriodic(periodicCb, period);
            }
        });
    Cv_.notify_one();
}

void TSimpleCoreFacility::PostToResponseQueue(TPostTaskCb&& f) {
    std::lock_guard lock(Mutex_);
    if (Stop_) {
        return;
    }
    EnqueueTaskNoLock(TClock::now(), std::move(f));

    Cv_.notify_one();
}

void TSimpleCoreFacility::EnqueueTaskNoLock(TTimePoint executeAt, TPostTaskCb&& task) {
    Queue_.push({
        .ExecuteAt = executeAt,
        .SeqNo = NextSeqNo_++,
        .Task = std::move(task),
    });
}

void TSimpleCoreFacility::SchedulePeriodic(std::shared_ptr<TPeriodicCb> periodicCb, TDeadline::Duration period) {
    std::lock_guard lock(Mutex_);
    if (Stop_) {
        return;
    }
    EnqueueTaskNoLock(
        TClock::now() + period,
        [this, periodicCb, period] {
            NYdb::NIssue::TIssues issues;
            const bool cont = (*periodicCb)(std::move(issues), EStatus::SUCCESS);
            if (cont) {
                SchedulePeriodic(periodicCb, period);
            }
        });

    Cv_.notify_one();
}

std::optional<TSimpleCoreFacility::TTimePoint> TSimpleCoreFacility::DrainReadyTasks(std::vector<TPostTaskCb>& ready) {
    const auto now = TClock::now();
    while (!Queue_.empty() && Queue_.top().ExecuteAt <= now) {
        ready.push_back(std::move(Queue_.top().Task));
        Queue_.pop();
    }
    if (Queue_.empty()) {
        return std::nullopt;
    }
    return Queue_.top().ExecuteAt;
}

void TSimpleCoreFacility::WorkerLoop() {
    for (;;) {
        std::vector<TPostTaskCb> ready;
        {
            std::unique_lock lock(Mutex_);
            while (ready.empty()) {
                if (Stop_) {
                    return;
                }
                const auto nextAt = DrainReadyTasks(ready);
                if (!ready.empty()) {
                    break;
                }
                if (!nextAt.has_value()) {
                    Cv_.wait(lock, [this] { return Stop_ || !Queue_.empty(); });
                } else {
                    Cv_.wait_until(lock, *nextAt, [this, deadline = *nextAt] {
                        return Stop_ || Queue_.empty() || Queue_.top().ExecuteAt < deadline;
                    });
                }
            }
        }

        for (auto& task : ready) {
            if (task) {
                task();
            }
        }
    }
}

std::shared_ptr<ICoreFacility> CreateSimpleCoreFacility() {
    return std::make_shared<TSimpleCoreFacility>();
}

} // namespace NYdb::inline Dev
