#pragma once

#include "public.h"

#include <util/generic/noncopyable.h>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

class TExecutorCounters
{
    struct TActivityCounters;
    struct TExecutor;
    struct TImpl;

    enum EActivity
    {
        WAIT,
        EXECUTE,
        MAX
    };

private:
    std::unique_ptr<TImpl> Impl;

public:
    TExecutorCounters();
    ~TExecutorCounters();

    void Register(NMonitoring::TDynamicCounters& counters);

    void UpdateStats();

    //
    // Activity
    //

    class TActivityScope: private TNonCopyable
    {
    private:
        TExecutorCounters* const Counters;
        TExecutor* const Executor;
        const int Index;

    public:
        TActivityScope(
            TExecutorCounters* counters,
            TExecutor* executor,
            int index)
            : Counters(counters)
            , Executor(executor)
            , Index(index)
        {
            Counters->ActivityStarted(Executor, Index);
        }

        ~TActivityScope()
        {
            Counters->ActivityCompleted(Executor, Index);
        }
    };

    //
    // Executor
    //

    class TExecutorScope: private TNonCopyable
    {
    private:
        // TExecutorCounters* const Counters;
        // TExecutor* const Executor;
        // TODO(umed): make members const once deprecated move constructor is
        // removed
        TExecutorCounters* Counters;
        TExecutor* Executor;

    public:
        TExecutorScope(TExecutorCounters* counters, TExecutor* executor)
            : Counters(counters)
            , Executor(executor)
        {}

        // TODO(umed): don't use it. It is workaround to make possible single
        // TExecutor for both filestore & blockstore, should be removed
        TExecutorScope(TExecutorScope&& other)
            : TExecutorScope(
                  std::exchange(other.Counters, nullptr),
                  std::exchange(other.Executor, nullptr))
        {}

        TExecutorScope& operator=(TExecutorScope&& other) = delete;

        ~TExecutorScope()
        {
            if (Counters) {
                Counters->ReleaseExecutor(Executor);
            }
        }

        TActivityScope StartWait()
        {
            return {Counters, Executor, WAIT};
        }

        TActivityScope StartExecute()
        {
            return {Counters, Executor, EXECUTE};
        }
    };

    TExecutorScope StartExecutor()
    {
        return {this, AllocExecutor()};
    }

private:
    TExecutor* AllocExecutor();
    void ReleaseExecutor(TExecutor* executor);

    void ActivityStarted(TExecutor* executor, int index);
    void ActivityCompleted(TExecutor* executor, int index);
};

}   // namespace NYdb::NBS
