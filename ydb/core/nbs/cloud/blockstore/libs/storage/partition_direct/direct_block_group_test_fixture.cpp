#include "direct_block_group_test_fixture.h"

// #include <ydb/core/nbs/cloud/blockstore/config/config.h>
// #include <ydb/core/nbs/cloud/blockstore/config/protos/storage.pb.h>
#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/partition_direct_service_mock.h>

#include <ydb/core/nbs/cloud/storage/core/libs/coroutine/executor_ut.h>

namespace {

// Bound for a single Runtime->DispatchEvents() call. On an idle real runtime
// the simulated clock does not advance, so DispatchEvents would otherwise block
// until the (huge) default dispatch timeout. With a short bound it throws
// TEmptyEventQueueException once the queue drains, which the pumping helpers
// catch.
constexpr auto DispatchTimeout = TDuration::MilliSeconds(50);

// Duration of a single DispatchEvents step inside the pumping loops.
constexpr auto DispatchStep = TDuration::MilliSeconds(10);

// Upper bound on DispatchEvents iterations in DrainRuntime() so a misbehaving
// runtime cannot loop forever.
constexpr int MaxDrainIterations = 100;

}   // namespace

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

TVector<NKikimr::NBsController::TDDiskId> MakeDDiskIds(ui32 baseNodeId)
{
    TVector<NKikimr::NBsController::TDDiskId> ids;
    ids.reserve(DirectBlockGroupHostCount);
    for (ui32 i = 0; i < DirectBlockGroupHostCount; ++i) {
        ids.emplace_back(baseNodeId + i, 1, i);
    }
    return ids;
}

void TDBGFixture::SetUp(NUnitTest::TTestContext& context)
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

void TDBGFixture::TearDown(NUnitTest::TTestContext& context)
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

// Dispatches one batch of events queued in the runtime.
bool TDBGFixture::DispatchRuntimeOnce(NActors::TDispatchOptions options) const
{
    // Quirk: a non-empty FinalEvents list enables full simulation.
    options.FinalEvents.emplace_back([](NActors::IEventHandle&)
                                     { return false; });
    try {
        return Runtime->DispatchEvents(options, DispatchStep);
    } catch (const NActors::TEmptyEventQueueException&) {
        return false;
    }
}

// Dispatches everything currently queued in the runtime.
void TDBGFixture::DrainRuntime() const
{
    for (int i = 0; i < MaxDrainIterations && DispatchRuntimeOnce(); ++i) {
    }
}

TExecutorPtr TDBGFixture::MakeExecutor()
{
    auto executor = TExecutor::Create("DBG_TEST");
    executor->Start();
    Executors.push_back(executor);
    return executor;
}

[[nodiscard]] std::shared_ptr<TDirectBlockGroup>
TDBGFixture::MakeDirectBlockGroup(
    const TExecutorPtr& executor,
    std::unique_ptr<NStorage::NTransport::IStorageTransport> transport,
    const TVector<NKikimr::NBsController::TDDiskId>& ddisksIds,
    const TVector<NKikimr::NBsController::TDDiskId>& pbufferIds) const
{
    return std::make_shared<TDirectBlockGroup>(
        Runtime->GetActorSystem(0),
        std::make_shared<TStorageConfig>(NProto::TStorageServiceConfig()),
        executor,
        "disk-1",
        1,
        1,
        0,
        ddisksIds,
        pbufferIds,
        std::move(transport));
}

[[nodiscard]] std::shared_ptr<TDirectBlockGroup>
TDBGFixture::MakeDirectBlockGroup(
    const TExecutorPtr& executor,
    std::unique_ptr<NTransport::TStorageTransportMock> transport,
    ui32 baseNodeId) const
{
    return MakeDirectBlockGroup(
        executor,
        std::move(transport),
        MakeDDiskIds(baseNodeId),
        MakeDDiskIds(baseNodeId + DirectBlockGroupHostCount));
}

[[nodiscard]] std::shared_ptr<TDirectBlockGroup>
TDBGFixture::MakeDirectBlockGroup(
    const TExecutorPtr& executor,
    std::unique_ptr<NTransport::NTestLib::TICStorageTransportTestAdapter>
        transport) const
{
    auto ddisks = transport->GetDDiskIds();
    auto pbuffers = transport->GetPBufferIds();

    return MakeDirectBlockGroup(
        executor,
        std::move(transport),
        ddisks,
        pbuffers);
}

// Interleaves the simulated runtime and the coroutine executor: dispatches
// everything queued in the runtime and lets the executor run the resumed
// coroutines.
bool TDBGFixture::DoExecutorAndRuntimeWorkWithPredicate(
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

        // Push any work the coroutine has already produced into the
        // runtime.
        DrainExecutor(executor);

        // Dispatch the runtime, stopping early once the predicate holds.
        NActors::TDispatchOptions options;
        options.CustomFinalCondition = [&]()
        {
            return predicate();
        };
        DispatchRuntimeOnce(std::move(options));

        // Run the coroutines woken up by the resolved transport futures.
        DrainExecutor(executor);
    }
}

// Pumps runtime + executor for a bounded time so in-flight async work (sent
// requests, callbacks) settles, without waiting for a specific condition.
void TDBGFixture::DoAllExecutorAndRuntimeWork(
    const TExecutorPtr& executor,
    TDuration duration) const
{
    DoExecutorAndRuntimeWorkWithPredicate(
        executor,
        []() { return false; },
        duration);
}

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
