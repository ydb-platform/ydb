#pragma once

namespace NYql::NDqs {

struct ITaskRunnerInvoker: public TThrRefBase {
    using TPtr = TIntrusivePtr<ITaskRunnerInvoker>;
    virtual void Invoke(const std::function<void(void)>& f) = 0;
    virtual bool IsLocal() { return false; }
};

struct TTaskRunnerInvoker: public ITaskRunnerInvoker {
    void Invoke(const std::function<void(void)>& f) override {
        f();
    }

    bool IsLocal() override { return true; }
};

struct ITaskRunnerInvokerFactory: public TThrRefBase {
    using TPtr = TIntrusivePtr<ITaskRunnerInvokerFactory>;
    virtual ITaskRunnerInvoker::TPtr Create() = 0;
};

class TTaskRunnerInvokerFactory: public ITaskRunnerInvokerFactory {
    ITaskRunnerInvoker::TPtr Create() override {
        return new TTaskRunnerInvoker();
    }
};

} // namespace NYql::NDq
