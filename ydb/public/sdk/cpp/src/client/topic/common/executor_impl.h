#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/executor.h>
#include <ydb/public/sdk/cpp/src/client/common_client/impl/client.h>

#include <util/thread/pool.h>

#include <queue>

namespace NYdb::inline Dev::NTopic {

class IAsyncExecutor : public IExecutor {
private:
    virtual void PostImpl(std::vector<std::function<void()>>&&) = 0;
    virtual void PostImpl(std::function<void()>&&) = 0;

public:
    bool IsAsync() const override {
        return true;
    }
    // Post Implementation MUST NOT run f before it returns
    void Post(TFunction&& f) final;
};

class TSerialExecutor : public IAsyncExecutor, public std::enable_shared_from_this<TSerialExecutor> {
private:
    IAsyncExecutor::TPtr Executor; //!< Wrapped executor that is actually doing the job
    bool Busy = false; //!< Set if some closure was scheduled for execution and did not finish yet
    std::mutex Mutex = {};
    std::queue<TFunction> ExecutionQueue = {};

public:
    TSerialExecutor(IAsyncExecutor::TPtr executor);
    ~TSerialExecutor() = default;

private:
    void PostImpl(std::vector<TFunction>&& fs) override;
    void PostImpl(TFunction&& f) override;
    void PostNext();
};

class TSyncExecutor : public IExecutor {
public:
    void Post(TFunction&& f) final {
        return f();
    }
    bool IsAsync() const final {
        return false;
    }
    void DoStart() override {
    }
    void Stop() override {
    }
};

} // namespace NYdb::NTopic
