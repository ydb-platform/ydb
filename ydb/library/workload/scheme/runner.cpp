#include "runner.h"

#include <library/cpp/threading/local_executor/local_executor.h>
#include <library/cpp/time_provider/monotonic.h>

#include <util/generic/yexception.h>

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <exception>
#include <limits>
#include <mutex>
#include <vector>

namespace NYdbWorkload::NScheme {

const char* OperationName(EOperation operation) {
    static constexpr const char* Names[] = {"CreateTable", "CreateTopic", "AddColumns", "DropColumns", "AddConsumers", "DropConsumers"};
    return Names[static_cast<size_t>(operation)];
}

TRunner::TRunner(const TConfig& config, IClient& client, TCancelled cancelled)
    : Config_(config)
    , Client_(client)
    , Cancelled_(std::move(cancelled))
{}

void TRunner::Count(EOperation operation) {
    ++Counters_[static_cast<size_t>(operation)];
}

std::array<ui64, OperationCount> TRunner::Statistics() const {
    std::array<ui64, OperationCount> result;
    for (size_t i = 0; i < result.size(); ++i) {
        result[i] = Counters_[i].load();
    }
    return result;
}

void TRunner::Execute(ui64 concurrency, ui64 objects, const std::function<void(ui32, ui32, const TCancelled&)>& worker) {
    Y_ENSURE(!Cancelled_(), "Scheme workload interrupted");
    if (!objects) {
        return;
    }
    const ui64 workerCount = std::min(concurrency, objects);
    Y_ENSURE(workerCount > 0 && workerCount <= std::numeric_limits<int>::max(), "Invalid worker count");
    const ui32 workers = workerCount;
    std::atomic<bool> failed = false;
    std::mutex mutex;
    std::exception_ptr error;
    const TCancelled stop = [&] { return failed.load() || Cancelled_(); };
    NPar::TLocalExecutor executor;
    executor.RunAdditionalThreads(workers - 1);
    executor.ExecRange([&](int workerId) {
        try {
            worker(workerId, workers, stop);
        } catch (...) {
            failed = true;
            std::lock_guard guard(mutex);
            if (!error) {
                error = std::current_exception();
            }
        }
    }, 0, workers, NPar::TLocalExecutor::WAIT_COMPLETE);
    if (error) {
        std::rethrow_exception(error);
    }
    Y_ENSURE(!Cancelled_(), "Scheme workload interrupted; completed objects and alterations are preserved");
}

void TRunner::Init() {
    Y_ENSURE(!Cancelled_(), "Scheme workload interrupted");
    Client_.PrepareDirectory(Config_.Path);
    Execute(Config_.CreateConcurrency, Config_.InitialObjectCount(), [&](ui32 worker, ui32 workers, const TCancelled& stop) {
        for (ui64 index = worker; index < Config_.InitialObjectCount() && !stop(); index += workers) {
            const auto object = Config_.InitialObject(index);
            Client_.Create(object);
            Count(object.Type == EObjectType::Table ? EOperation::CreateTable : EOperation::CreateTopic);
        }
    });
}

void TRunner::Run(TDuration duration) {
    Y_ENSURE(duration > TDuration::Zero(), "Run duration must be positive");
    Y_ENSURE(!Cancelled_(), "Scheme workload interrupted");
    Client_.PrepareDirectory(Config_.Path);
    std::vector<EObjectState> states(Config_.ObjectCount());
    Execute(Config_.AlterConcurrency, Config_.ObjectCount(), [&](ui32 worker, ui32 workers, const TCancelled& stop) {
        for (ui64 index = worker; index < Config_.ObjectCount() && !stop(); index += workers) {
            states[index] = Client_.Inspect(Config_.Object(index));
        }
    });

    std::deque<ui64> missing;
    std::deque<ui64> ready;
    for (ui64 index = 0; index < Config_.ObjectCount(); ++index) {
        (states[index] == EObjectState::Missing ? missing : ready).push_back(index);
    }
    const ui64 concurrency = std::min<ui64>(Config_.CreateConcurrency, missing.size())
        + std::min<ui64>(Config_.AlterConcurrency, Config_.ObjectCount());
    std::mutex mutex;
    std::condition_variable changed;
    ui32 creating = 0;
    ui32 altering = 0;
    bool preferCreate = true;
    const auto deadline = TMonotonic::Now() + duration;
    Execute(concurrency, Config_.ObjectCount(), [&](ui32, ui32, const TCancelled& stop) {
        for (;;) {
            ui64 index;
            bool create;
            bool add;
            {
                std::unique_lock lock(mutex);
                for (;;) {
                    const auto now = TMonotonic::Now();
                    if (stop() || now >= deadline) {
                        return;
                    }
                    const bool canCreate = !missing.empty() && creating < Config_.CreateConcurrency;
                    const bool canAlter = !ready.empty() && altering < Config_.AlterConcurrency;
                    if (canCreate || canAlter) {
                        create = canCreate && (preferCreate || !canAlter);
                        preferCreate = !create;
                        auto& queue = create ? missing : ready;
                        index = queue.front();
                        queue.pop_front();
                        ++(create ? creating : altering);
                        add = states[index] == EObjectState::WithoutBatch;
                        break;
                    }
                    // Cancellation and failures must also wake idle workers, even if RPCs are still in flight.
                    const auto wait = std::min(deadline - now, TDuration::MilliSeconds(50));
                    changed.wait_for(lock, std::chrono::microseconds(wait.MicroSeconds()));
                }
            }
            const auto object = Config_.Object(index);
            if (create) {
                Client_.Create(object);
                Count(object.Type == EObjectType::Table ? EOperation::CreateTable : EOperation::CreateTopic);
            } else {
                Client_.Alter(object, add);
                Count(object.Type == EObjectType::Table
                    ? (add ? EOperation::AddColumns : EOperation::DropColumns)
                    : (add ? EOperation::AddConsumers : EOperation::DropConsumers));
            }
            {
                std::lock_guard lock(mutex);
                --(create ? creating : altering);
                states[index] = !create && add ? EObjectState::WithBatch : EObjectState::WithoutBatch;
                // Only completed objects enter this queue; popping an index reserves it for one worker.
                ready.push_back(index);
            }
            changed.notify_all();
        }
    });
}

} // namespace NYdbWorkload::NScheme
