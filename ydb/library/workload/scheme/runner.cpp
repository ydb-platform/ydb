#include "runner.h"

#include <library/cpp/threading/local_executor/local_executor.h>
#include <library/cpp/time_provider/monotonic.h>

#include <util/generic/yexception.h>

#include <algorithm>
#include <exception>
#include <mutex>
#include <vector>

namespace NYdbWorkload::NScheme {

const char* OperationName(EOperation operation) {
    static constexpr const char* Names[] = {"CreateTable", "CreateTopic", "AddColumns", "DropColumns", "AddConsumers", "DropConsumers"};
    return Names[static_cast<size_t>(operation)];
}

TRunner::TRunner(const TConfig& config, IClient& client, TCancelled cancelled)
    : Config(config)
    , Client(client)
    , Cancelled(std::move(cancelled))
{}

void TRunner::Count(EOperation operation) {
    ++Counters[static_cast<size_t>(operation)];
}

std::array<ui64, OperationCount> TRunner::Statistics() const {
    std::array<ui64, OperationCount> result;
    for (size_t i = 0; i < result.size(); ++i) {
        result[i] = Counters[i].load();
    }
    return result;
}

void TRunner::Execute(ui32 concurrency, const std::function<void(ui32, ui32, const TCancelled&)>& worker) {
    const ui32 workers = std::min<ui64>(concurrency, Config.ObjectCount());
    Y_ENSURE(workers > 0, "No workers or objects configured");
    std::atomic<bool> failed = false;
    std::mutex mutex;
    std::exception_ptr error;
    const TCancelled stop = [&] { return failed.load() || Cancelled(); };
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
    Y_ENSURE(!Cancelled(), "Scheme workload interrupted; completed objects and alterations are preserved");
}

void TRunner::Init() {
    Y_ENSURE(!Cancelled(), "Scheme workload interrupted");
    Client.MakeDirectory(Config.Path);
    Execute(Config.CreateConcurrency, [&](ui32 worker, ui32 workers, const TCancelled& stop) {
        for (ui64 index = worker; index < Config.ObjectCount() && !stop(); index += workers) {
            const auto object = Config.Object(index);
            Client.Create(object);
            Count(object.Type == EObjectType::Table ? EOperation::CreateTable : EOperation::CreateTopic);
        }
    });
}

void TRunner::Run(TDuration duration) {
    Y_ENSURE(duration > TDuration::Zero(), "Run duration must be positive");
    // Each object has one owning worker. Bytes, not vector<bool>, avoid shared-word writes.
    std::vector<ui8> present(Config.ObjectCount());
    Execute(Config.AlterConcurrency, [&](ui32 worker, ui32 workers, const TCancelled& stop) {
        for (ui64 index = worker; index < Config.ObjectCount() && !stop(); index += workers) {
            present[index] = Client.Inspect(Config.Object(index));
        }
    });
    const auto deadline = TMonotonic::Now() + duration;
    Execute(Config.AlterConcurrency, [&](ui32 worker, ui32 workers, const TCancelled& stop) {
        ui64 index = worker;
        while (!stop() && TMonotonic::Now() < deadline) {
            const auto object = Config.Object(index);
            const bool add = !present[index];
            Client.Alter(object, add);
            present[index] = add;
            Count(object.Type == EObjectType::Table
                ? (add ? EOperation::AddColumns : EOperation::DropColumns)
                : (add ? EOperation::AddConsumers : EOperation::DropConsumers));
            index += workers;
            if (index >= Config.ObjectCount()) {
                index = worker;
            }
        }
    });
}

} // namespace NYdbWorkload::NScheme
