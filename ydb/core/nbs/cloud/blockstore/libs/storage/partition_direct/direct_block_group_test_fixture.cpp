#include "direct_block_group_test_fixture.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
<<<<<<< HEAD
#include <ydb/core/nbs/cloud/blockstore/libs/service/partition_direct_service_mock.h>
=======
#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/request.h>
>>>>>>> 3b287e3de3d (NBS-7459: own PBuffer cleanup in DirectBlockGroup (#52414))

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>
#include <ydb/core/nbs/cloud/storage/core/libs/coroutine/executor_ut.h>

namespace {

constexpr auto DispatchTimeout = TDuration::MilliSeconds(50);
constexpr auto DispatchStep = TDuration::MilliSeconds(10);
constexpr int MaxDrainIterations = 100;

}   // namespace

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

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

bool TDBGFixture::DispatchRuntimeOnce(NActors::TDispatchOptions options) const
{
    // TODO
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
        CreateArenaAllocator(),
        Runtime->GetActorSystem(0),
        std::make_shared<TStorageConfig>(StorageServiceConfig),
        executor,
        "disk-1",
        1,
        1,
        0,
        ddisksIds,
        pbufferIds,
<<<<<<< HEAD
        std::move(transport));
=======
        TVector(ddisksIds.size(), EHostHealth::Online),
        dbgConnectionsConfigGeneration,
        std::move(transport),
        nullptr);
>>>>>>> f5807c51a12 (NBS-7513: Add oracle's initial health load (#52831))
}

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

void TDBGFixture::DoAllExecutorAndRuntimeWork(
    const TExecutorPtr& executor,
    TDuration duration) const
{
    DoExecutorAndRuntimeWorkWithPredicate(
        executor,
        []() { return false; },
        duration);
}

NThreading::TFuture<void> TDBGFixture::RunAndGetInitialReady(
    const std::shared_ptr<TDirectBlockGroup>& dbg,
    bool dropScheduledCallbacks)
{
    auto service =
        std::make_shared<TPartitionDirectServiceMock>(dropScheduledCallbacks);
    Services.push_back(service);
    return dbg->Run(service.get());
}

void TDBGFixture::WaitReady(
    const NThreading::TFuture<void>& future,
    TDuration timeout)
{
    future.Wait(timeout);
    UNIT_ASSERT(future.HasValue());
}

void TDBGFixture::WaitReady(
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

<<<<<<< HEAD
=======
size_t TDBGFixture::ReplyUpdateRequests()
{
    auto requests = std::move(Service->UpdateConfigRequests);
    for (auto& r: requests) {
        r.Promise.SetValue(EPersistResult::Success);
    }
    return requests.size();
}

void TDBGFixture::WaitDirtyMapReady(
    const TExecutorPtr& executor,
    const std::shared_ptr<TVChunk>& vchunk)
{
    UNIT_ASSERT(DoExecutorAndRuntimeWorkWithPredicate(
        executor,
        [&] { return TBaseFixture::IsDirtyMapReady(*vchunk); },
        DefaultWaitFutureTimeout));
}

void TDBGFixture::WriteBlock(
    const TExecutorPtr& executor,
    const std::shared_ptr<TVChunk>& vchunk,
    ui64 blockIndex)
{
    TString buffer(DefaultBlockSize, 'w');
    auto request = std::make_shared<TWriteBlocksLocalRequest>(TRequestHeaders{
        .VolumeConfig = VolumeConfig,
        .RequestId = 1,
        .Range = TBlockRange64::WithLength(blockIndex, 1)});
    request->Sglist =
        TGuardedSgList(TSgList{TBlockDataRef{buffer.data(), buffer.size()}});

    const auto response = WaitFuture(
        executor,
        vchunk->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(),
            std::move(request),
            NWilson::TTraceId::NewTraceId(
                NWilson::TTraceId::MAX_VERBOSITY,
                NWilson::TTraceId::MAX_TIME_TO_LIVE)),
        DefaultWaitFutureTimeout);
    UNIT_ASSERT_C(!HasError(response.Error), FormatError(response.Error));
}

void TDBGFixture::WaitBarrierErases(
    const TExecutorPtr& executor,
    const std::shared_ptr<NTransport::TStorageTransportMock>& transport,
    size_t count)
{
    UNIT_ASSERT_C(
        DoExecutorAndRuntimeWorkWithPredicate(
            executor,
            [&] { return transport->BarrierErases.size() >= count; },
            DefaultWaitFutureTimeout),
        "barrier erases sent: " << transport->BarrierErases.size());
}

>>>>>>> 3b287e3de3d (NBS-7459: own PBuffer cleanup in DirectBlockGroup (#52414))
}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
