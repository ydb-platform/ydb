#pragma once

#include <ydb/core/nbs/cloud/storage/core/libs/coroutine/executor.h>

#include <ydb/core/testlib/actors/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

#include <functional>
#include <memory>

namespace NYdb::NBS::NBlockStore::NStorage::NTransport::NTestLib {

////////////////////////////////////////////////////////////////////////////////

// Provides an actor runtime and a coroutine executor for storage transport
// tests and pumps both event loops while asynchronous requests are running.
class TStorageTransportTestFixture: public NUnitTest::TBaseFixture
{
public:
    static constexpr auto DefaultWaitFutureTimeout = TDuration::Seconds(10);
    static constexpr auto DefaultExecutorAndRuntimeDuration =
        TDuration::MilliSeconds(200);

    std::unique_ptr<NActors::TTestActorRuntime> Runtime;
    TVector<TExecutorPtr> Executors;

    // Initializes the actor runtime used by the transport under test.
    void SetUp(NUnitTest::TTestContext& context) override;

    // Stops coroutine executors and drains the actor runtime.
    void TearDown(NUnitTest::TTestContext& context) override;

    // Creates and starts a coroutine executor owned by the fixture.
    TExecutorPtr MakeExecutor();

    // Dispatches one batch of events queued in the runtime.
    bool DispatchRuntimeOnce(NActors::TDispatchOptions options = {}) const;

    // Dispatches everything currently queued in the runtime.
    void DrainRuntime() const;

    // Interleaves the simulated runtime and the coroutine executor until the
    // predicate succeeds or the timeout expires.
    bool DoExecutorAndRuntimeWorkWithPredicate(
        const TExecutorPtr& executor,
        std::function<bool()> predicate,
        TDuration timeout) const;

    // Pumps runtime and executor for a bounded time so in-flight asynchronous
    // work settles without waiting for a specific condition.
    void DoAllExecutorAndRuntimeWork(
        const TExecutorPtr& executor,
        TDuration duration = DefaultExecutorAndRuntimeDuration) const;

    // Pumps runtime and executor until the future is resolved and returns its
    // value.
    template <typename T>
    T WaitFuture(
        const TExecutorPtr& executor,
        NThreading::TFuture<T> future,
        TDuration timeout)
    {
        DoExecutorAndRuntimeWorkWithPredicate(
            executor,
            [&]() { return future.HasValue() || future.HasException(); },
            timeout);
        return future.GetValue(timeout);
    }

    // Waits for a future and asserts that it was resolved.
    void WaitReady(
        const NThreading::TFuture<void>& future,
        TDuration timeout = DefaultWaitFutureTimeout);

    // Pumps runtime and executor while waiting for a future and asserts that
    // it was resolved.
    void WaitReady(
        const TExecutorPtr& executor,
        const NThreading::TFuture<void>& future,
        TDuration timeout = DefaultWaitFutureTimeout);
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NTransport::NTestLib
