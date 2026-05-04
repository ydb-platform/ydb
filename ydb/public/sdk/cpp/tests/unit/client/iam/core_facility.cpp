#include "core_facility.h"

namespace NYdb::inline Dev {

TTestCoreFacility::TTestCoreFacility() {
    ResponseThread_ = std::thread([this] { ResponseWorkerLoop(); });
}

TTestCoreFacility::~TTestCoreFacility() {
    StopPeriodic_.store(true);
    if (PeriodicThread_.joinable()) {
        PeriodicThread_.join();
    }
    {
        std::lock_guard lock(QueueMutex_);
        StopQueue_ = true;
    }
    QueueCv_.notify_all();
    if (ResponseThread_.joinable()) {
        ResponseThread_.join();
    }
}

void TTestCoreFacility::AddPeriodicTask(TPeriodicCb&& cb, TDeadline::Duration period) {
    std::lock_guard lock(PeriodicMutex_);
    Y_ABORT_UNLESS(!PeriodicStarted_);
    PeriodicStarted_ = true;
    PeriodicCb_ = std::move(cb);
    PeriodicThread_ = std::thread([this, period] { PeriodicWorkerLoop(period); });
}

void TTestCoreFacility::PostToResponseQueue(TPostTaskCb&& f) {
    {
        std::lock_guard lock(QueueMutex_);
        if (StopQueue_) {
            return;
        }
        Queue_.push(std::move(f));
    }
    QueueCv_.notify_one();
}

void TTestCoreFacility::ResponseWorkerLoop() {
    for (;;) {
        TPostTaskCb job;
        {
            std::unique_lock lock(QueueMutex_);
            QueueCv_.wait(lock, [this] { return StopQueue_ || !Queue_.empty(); });
            if (StopQueue_ && Queue_.empty()) {
                return;
            }
            job = std::move(Queue_.front());
            Queue_.pop();
        }
        if (job) {
            job();
        }
    }
}

void TTestCoreFacility::PeriodicWorkerLoop(TDeadline::Duration period) {
    while (!StopPeriodic_.load()) {
        bool cont = false;
        {
            std::lock_guard lock(PeriodicMutex_);
            if (!PeriodicCb_) {
                return;
            }
            NYdb::NIssue::TIssues issues;
            cont = PeriodicCb_(std::move(issues), EStatus::SUCCESS);
        }
        if (!cont) {
            return;
        }
        std::this_thread::sleep_for(period);
    }
}    

} // namespace NYdb::inline Dev
