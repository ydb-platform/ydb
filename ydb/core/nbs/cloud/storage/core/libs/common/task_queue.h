#pragma once

#include "public.h"

#include "startable.h"

#include <library/cpp/threading/future/future.h>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

struct ITask
{
    virtual ~ITask() = default;

    virtual void Execute() noexcept = 0;
};

////////////////////////////////////////////////////////////////////////////////

template <typename F>
class TFutureTask final
    : public ITask
{
public:
    using TResult = NThreading::TFutureType<TFunctionResult<F>>;

private:
    NThreading::TPromise<TResult> Result;
    F Func;

public:
    TFutureTask(NThreading::TPromise<TResult> result, F func)
        : Result(std::move(result))
        , Func(std::move(func))
    {}

    void Execute() noexcept override
    {
        NThreading::NImpl::SetValue(Result, Func);
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename F>
class TSimpleTask final
    : public ITask
{
private:
    F Func;

public:
    TSimpleTask(F func)
        : Func(std::move(func))
    {}

    void Execute() noexcept override
    {
        Func();
    }
};

////////////////////////////////////////////////////////////////////////////////

struct ITaskQueue
    : public IStartable
{
    virtual ~ITaskQueue() = default;

    virtual void Enqueue(ITaskPtr task) = 0;

    template <typename F>
    auto Execute(F func)
    {
        using TResult = typename TFutureTask<F>::TResult;
        auto result = NThreading::NewPromise<TResult>();
        Enqueue(std::make_unique<TFutureTask<F>>(result, std::move(func)));
        return result.GetFuture();
    }

    template <typename F>
    void ExecuteSimple(F func)
    {
        Enqueue(std::make_unique<TSimpleTask<F>>(std::move(func)));
    }
};

////////////////////////////////////////////////////////////////////////////////

ITaskQueuePtr CreateTaskQueueStub();

}   // namespace NYdb::NBS
