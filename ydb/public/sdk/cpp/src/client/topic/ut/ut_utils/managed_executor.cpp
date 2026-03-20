#include "managed_executor.h"

<<<<<<< HEAD:ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/managed_executor.cpp
namespace NYdb::NTopic::NTests {
=======
#include <util/random/random.h>

namespace NYdb::inline Dev::NTopic::NTests {
>>>>>>> 771638ae94f (YQ-5187 fixed hanging in PQ read session (#36220)):ydb/public/sdk/cpp/tests/integration/topic/utils/managed_executor.cpp

TManagedExecutor::TManagedExecutor(TExecutorPtr executor) :
    Executor{std::move(executor)}
{
}

bool TManagedExecutor::IsAsync() const
{
    return Executor->IsAsync();
}

void TManagedExecutor::Post(TFunction &&f)
{
    with_lock (Mutex) {
        Funcs.push_back(std::move(f));
        ++Planned;
    }
}

void TManagedExecutor::DoStart()
{
    Executor->Start();
}

auto TManagedExecutor::MakeTask(TFunction func) -> TFunction
{
    return [this, func = std::move(func)]() {
        ++Running;

        func();

        --Running;
        ++Executed;
    };
}

void TManagedExecutor::RunTask(TFunction&& func)
{
    Y_ABORT_UNLESS(Planned > 0);
    --Planned;
    Executor->Post(MakeTask(std::move(func)));
}

void TManagedExecutor::StartRandomFunc() {
    std::lock_guard lock(Mutex);

    Y_ABORT_UNLESS(Planned > 0);
    size_t index = RandomNumber<size_t>(Planned);

    for (size_t i = 0; i < Funcs.size(); ++i) {
        if (Funcs[i] != nullptr) {
            if (index == 0) {
                RunTask(std::move(Funcs[i]));
                Funcs[i] = nullptr;
                return;
            }
            --index;
        }
    }

    Y_ABORT("No functions to start");
}

void TManagedExecutor::StartFuncs(const std::vector<size_t>& indicies)
{
    with_lock (Mutex) {
        for (auto index : indicies) {
            Y_ABORT_UNLESS(index < Funcs.size());
            Y_ABORT_UNLESS(Funcs[index]);

            RunTask(std::move(Funcs[index]));
            Funcs[index] = nullptr;
        }
    }
}

size_t TManagedExecutor::GetFuncsCount() const
{
    with_lock (Mutex) {
        return Funcs.size();
    }
}

size_t TManagedExecutor::GetPlannedCount() const
{
    return Planned;
}

size_t TManagedExecutor::GetRunningCount() const
{
    return Running;
}

size_t TManagedExecutor::GetExecutedCount() const
{
    return Executed;
}

void TManagedExecutor::RunAllTasks()
{
    with_lock (Mutex) {
        for (auto& func : Funcs) {
            if (func) {
                RunTask(std::move(func));
                func = nullptr;
            }
        }
    }
}

TIntrusivePtr<TManagedExecutor> CreateThreadPoolManagedExecutor(size_t threads)
{
    return MakeIntrusive<TManagedExecutor>(NYdb::NTopic::CreateThreadPoolExecutor(threads));
}

TIntrusivePtr<TManagedExecutor> CreateSyncManagedExecutor()
{
    return MakeIntrusive<TManagedExecutor>(NYdb::NTopic::CreateSyncExecutor());
}

}
