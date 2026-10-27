#include "storage_transport_test_fixture.h"

#include <ydb/core/nbs/cloud/storage/core/libs/coroutine/executor_ut.h>

#include <ydb/core/base/appdata_fwd.h>

namespace {

constexpr auto DispatchTimeout = TDuration::MilliSeconds(50);
constexpr auto DispatchStep = TDuration::MilliSeconds(10);
constexpr int MaxDrainIterations = 100;

}   // namespace

namespace NYdb::NBS::NBlockStore::NStorage::NTransport::NTestLib {

void TStorageTransportTestFixture::SetUp(NUnitTest::TTestContext& context)
{
    Y_UNUSED(context);
    Runtime = std::make_unique<NActors::TTestActorRuntime>();
    Runtime->Initialize(NActors::TTestActorRuntime::TEgg{
        .App0 = new NKikimr::
            TAppData(0, 0, 0, 0, {}, nullptr, nullptr, nullptr, nullptr),
        .Opaque = nullptr,
        .KeyConfigGenerator = nullptr,
        .Icb = {},
        .Dcb = {}});
    Runtime->SetLogPriority(
        NKikimrServices::NBS_PARTITION,
        NActors::NLog::PRI_DEBUG);
    Runtime->SetDispatchTimeout(DispatchTimeout);
}

void TStorageTransportTestFixture::TearDown(NUnitTest::TTestContext& context)
{
    Y_UNUSED(context);
    for (const auto& executor: Executors) {
        executor->Stop();
    }
    Executors.clear();

    if (Runtime) {
        DrainRuntime();
    }
}

TExecutorPtr TStorageTransportTestFixture::MakeExecutor()
{
    auto executor = TExecutor::Create("DBG_TEST");
    executor->Start();
    Executors.push_back(executor);
    return executor;
}

bool TStorageTransportTestFixture::DispatchRuntimeOnce(
    NActors::TDispatchOptions options) const
{
    // A non-empty FinalEvents list enables full simulation.
    options.FinalEvents.emplace_back([](NActors::IEventHandle&)
                                     { return false; });
    try {
        return Runtime->DispatchEvents(options, DispatchStep);
    } catch (const NActors::TEmptyEventQueueException&) {
        return false;
    }
}

void TStorageTransportTestFixture::DrainRuntime() const
{
    for (int i = 0; i < MaxDrainIterations && DispatchRuntimeOnce(); ++i) {
    }
}

bool TStorageTransportTestFixture::DoExecutorAndRuntimeWorkWithPredicate(
    const TExecutorPtr& executor,
    std::function<bool()> predicate,
    TDuration timeout) const
{
    const auto deadline = TInstant::Now() + timeout;
    for (;;) {
        if (predicate()) {
            return true;
        }
        if (TInstant::Now() >= deadline) {
            return false;
        }

        DrainExecutor(executor);

        NActors::TDispatchOptions options;
        options.CustomFinalCondition = [&]()
        {
            return predicate();
        };
        DispatchRuntimeOnce(std::move(options));

        DrainExecutor(executor);
    }
}

void TStorageTransportTestFixture::DoAllExecutorAndRuntimeWork(
    const TExecutorPtr& executor,
    TDuration duration) const
{
    DoExecutorAndRuntimeWorkWithPredicate(
        executor,
        []() { return false; },
        duration);
}

void TStorageTransportTestFixture::WaitReady(
    const NThreading::TFuture<void>& future,
    TDuration timeout)
{
    future.Wait(timeout);
    UNIT_ASSERT(future.HasValue());
}

void TStorageTransportTestFixture::WaitReady(
    const TExecutorPtr& executor,
    const NThreading::TFuture<void>& future,
    TDuration timeout)
{
    DoExecutorAndRuntimeWorkWithPredicate(
        executor,
        [&]() { return future.HasValue() || future.HasException(); },
        timeout);
    UNIT_ASSERT(future.HasValue());
}

}   // namespace NYdb::NBS::NBlockStore::NStorage::NTransport::NTestLib
