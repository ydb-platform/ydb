#pragma once

#include <ydb/core/nbs/cloud/storage/core/libs/coroutine/public.h>

#include <thread>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

class TThreadChecker
{
public:
    class TDelegate
    {
    public:
        [[nodiscard]] bool Check() const;

    private:
        friend class TThreadChecker;
        std::thread::id ThreadId;
    };

    TThreadChecker();
    explicit TThreadChecker(TExecutorPtr executor);

    ~TThreadChecker();

    void BindToCurrent();
    void BindToExecutor(TExecutorPtr executor);
    [[nodiscard]] bool Check() const;

    [[nodiscard]] TDelegate CreateDelegate() const;

private:
    using TThreadId = std::atomic<std::thread::id>;
    std::shared_ptr<TThreadId> ThreadId;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
