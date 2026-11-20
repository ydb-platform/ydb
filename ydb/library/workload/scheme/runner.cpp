#include "runner.h"

#include <library/cpp/threading/future/async.h>
#include <library/cpp/time_provider/monotonic.h>

#include <util/generic/yexception.h>
#include <util/system/event.h>
#include <util/thread/pool.h>

#include <algorithm>
#include <deque>
#include <exception>
#include <vector>

namespace NYdbWorkload::NScheme {

namespace {

class TPendingOperations {
public:
    explicit TPendingOperations(IThreadPool& pool)
        : Pool_(pool)
    {
        Pending_.reserve(MAX_IN_FLIGHT_OPERATIONS);
    }

    ~TPendingOperations() {
        // The operation may still refer to the caller's client or state.
        for (const auto& operation : Pending_) {
            operation.Future.Wait();
        }
    }

    template <typename TStart>
    void Start(TStart&& start, std::function<void()> complete) {
        auto future = NThreading::Async(std::forward<TStart>(start), Pool_);
        Pending_.push_back({std::move(future), std::move(complete)});
        Pending_.back().Future.Subscribe([changed = Changed_](const auto&) mutable {
            changed.Signal();
        });
    }

    size_t Size() const {
        return Pending_.size();
    }

    void Collect() {
        std::erase_if(Pending_, [&](const TOperation& operation) {
            if (!operation.Future.IsReady()) {
                return false;
            }
            try {
                operation.Future.GetValue();
                operation.Complete();
            } catch (...) {
                Fail(std::current_exception());
            }
            return true;
        });
    }

    void Fail(std::exception_ptr error) {
        if (!Error_) {
            Error_ = std::move(error);
        }
    }

    bool Failed() const {
        return bool(Error_);
    }

    void Rethrow() const {
        if (Error_) {
            std::rethrow_exception(Error_);
        }
    }

    void Wait(TDuration timeout) {
        Changed_.WaitT(timeout);
    }

private:
    struct TOperation {
        NThreading::TFuture<void> Future;
        std::function<void()> Complete;
    };

    IThreadPool& Pool_;
    TAutoEvent Changed_;
    std::vector<TOperation> Pending_;
    std::exception_ptr Error_;
};

template <typename TSchedule>
void Execute(IThreadPool& pool, const TRunner::TCancelled& cancelled, TSchedule&& schedule,
        TMonotonic deadline = TMonotonic::Max()) {
    TPendingOperations pending(pool);
    for (;;) {
        pending.Collect();
        const auto now = TMonotonic::Now();
        const bool issuing = !pending.Failed() && !cancelled() && now < deadline;
        if (issuing) {
            try {
                if (schedule(pending)) {
                    continue;
                }
            } catch (...) {
                pending.Fail(std::current_exception());
            }
        }
        if (!pending.Size()) {
            break;
        }
        const auto interval = TDuration::MilliSeconds(50);
        pending.Wait(issuing ? std::min(interval, deadline - now) : interval);
    }

    pending.Rethrow();
    Y_ENSURE(!cancelled(), "Scheme workload interrupted; completed objects and alterations are preserved");
}

} // namespace

const char* OperationName(EOperation operation) {
    static constexpr const char* Names[] = {"CreateTable", "CreateTopic", "AddColumns", "DropColumns", "AddConsumers", "DropConsumers"};
    return Names[static_cast<size_t>(operation)];
}

TRunner::TRunner(const TConfig& config, IClient& client, TCancelled cancelled, ui32 threads)
    : Config_(config)
    , Client_(client)
    , Cancelled_(std::move(cancelled))
    , Threads_(threads)
{
    Y_ENSURE(Threads_ > 0 && Threads_ <= MAX_WORKLOAD_THREADS,
        "--threads must be in [1, " << MAX_WORKLOAD_THREADS << "]");
    Y_ENSURE(Config_.CreateConcurrency && Config_.AlterConcurrency
        && ui64(Config_.CreateConcurrency) + Config_.AlterConcurrency <= MAX_IN_FLIGHT_OPERATIONS,
        "Invalid scheme workload concurrency");
}

void TRunner::Count(EOperation operation) {
    ++Counters_[static_cast<size_t>(operation)];
}

std::array<ui64, OperationCount> TRunner::Statistics() const {
    return Counters_;
}

void TRunner::Init() {
    Y_ENSURE(!Cancelled_(), "Scheme workload interrupted");
    TThreadPool pool;
    pool.Start(std::min<ui64>(Threads_, Config_.InitialObjectCount()));
    Client_.PrepareDirectory(Config_.Path);
    ui64 next = 0;
    Execute(pool, Cancelled_, [&](TPendingOperations& pending) {
        if (next == Config_.InitialObjectCount() || pending.Size() >= Config_.CreateConcurrency) {
            return false;
        }
        const auto object = Config_.InitialObject(next++);
        pending.Start([this, object] { return Client_.Create(object); }, [this, type = object.Type] {
            Count(type == EObjectType::Table ? EOperation::CreateTable : EOperation::CreateTopic);
        });
        return true;
    });
}

void TRunner::Run(TDuration duration) {
    Y_ENSURE(duration > TDuration::Zero(), "Run duration must be positive");
    Y_ENSURE(!Cancelled_(), "Scheme workload interrupted");
    TThreadPool pool;
    pool.Start(std::min<ui64>(Threads_, Config_.ObjectCount()));
    Client_.PrepareDirectory(Config_.Path);
    std::vector<EObjectState> states(Config_.ObjectCount());
    ui64 next = 0;
    Execute(pool, Cancelled_, [&](TPendingOperations& pending) {
        if (next == Config_.ObjectCount() || pending.Size() >= Config_.AlterConcurrency) {
            return false;
        }
        const ui64 index = next++;
        pending.Start([&, index] {
            return Client_.Inspect(Config_.Object(index)).Apply([&, index](const auto& result) {
                states[index] = result.GetValue();
            });
        }, [] {});
        return true;
    });

    std::deque<ui64> missing;
    std::deque<ui64> ready;
    for (ui64 index = 0; index < Config_.ObjectCount(); ++index) {
        (states[index] == EObjectState::Missing ? missing : ready).push_back(index);
    }

    ui32 creating = 0;
    ui32 altering = 0;
    bool preferCreate = true;
    const auto deadline = TMonotonic::Now() + duration;
    // The scheduling thread owns both queues and updates them when futures complete.
    Execute(pool, Cancelled_, [&](TPendingOperations& pending) {
        const bool canCreate = !missing.empty() && creating < Config_.CreateConcurrency;
        const bool canAlter = !ready.empty() && altering < Config_.AlterConcurrency;
        if (!canCreate && !canAlter) {
            return false;
        }

        const bool create = canCreate && (preferCreate || !canAlter);
        preferCreate = !create;
        auto& queue = create ? missing : ready;
        const ui64 index = queue.front();
        queue.pop_front();
        ++(create ? creating : altering);
        const bool add = states[index] == EObjectState::WithoutBatch;
        const auto object = Config_.Object(index);
        pending.Start([this, object, create, add] {
            return create ? Client_.Create(object) : Client_.Alter(object, add);
        }, [&, index, create, add, type = object.Type] {
            if (create) {
                Count(type == EObjectType::Table ? EOperation::CreateTable : EOperation::CreateTopic);
            } else {
                Count(type == EObjectType::Table
                    ? (add ? EOperation::AddColumns : EOperation::DropColumns)
                    : (add ? EOperation::AddConsumers : EOperation::DropConsumers));
            }
            --(create ? creating : altering);
            states[index] = !create && add ? EObjectState::WithBatch : EObjectState::WithoutBatch;
            ready.push_back(index);
        });
        return true;
    }, deadline);
}

} // namespace NYdbWorkload::NScheme
