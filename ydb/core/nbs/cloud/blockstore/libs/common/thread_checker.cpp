#include "thread_checker.h"

#include <ydb/core/nbs/cloud/storage/core/libs/coroutine/executor.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

bool TThreadChecker::TDelegate::Check() const
{
    return ThreadId == std::this_thread::get_id();
}

TThreadChecker::TThreadChecker()
    : ThreadId(std::make_shared<TThreadId>())
{}

TThreadChecker::TThreadChecker(TExecutorPtr executor)
    : TThreadChecker()
{
    BindToExecutor(std::move(executor));
}

TThreadChecker::~TThreadChecker() = default;

void TThreadChecker::BindToCurrent()
{
    ThreadId->store(std::this_thread::get_id());
}

void TThreadChecker::BindToExecutor(TExecutorPtr executor)
{
    std::weak_ptr<TThreadId> threadId = ThreadId;

    executor->ExecuteSimple(
        [threadId = std::move(threadId)]   //
        ()
        {
            if (auto t = threadId.lock()) {
                t->store(std::this_thread::get_id());
            }
        });
}

bool TThreadChecker::Check() const
{
    return *ThreadId == std::this_thread::get_id();
}

TThreadChecker::TDelegate TThreadChecker::CreateDelegate() const
{
    TDelegate result;
    result.ThreadId = ThreadId->load();
    return result;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
