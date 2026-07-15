#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/service/partition_direct_service_mock.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/direct_block_group_impl.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport/storage_transport_mock.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport/testlib/ic_storage_transport_test_adapter.h>

#include <ydb/core/nbs/cloud/storage/core/libs/coroutine/executor.h>

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/testlib/actors/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

struct TDBGFixture: public NUnitTest::TBaseFixture
{
    static constexpr auto DefaultWaitFutureTimeout = TDuration::Seconds(10);
    static constexpr auto DefaultExecutorAndRuntimeDuration =
        TDuration::MilliSeconds(200);

    std::unique_ptr<NActors::TTestActorRuntime> Runtime;
    TVector<TExecutorPtr> Executors;

    // Mock services created by RunAndGetInitialReady(). Kept alive for the
    // whole test because TDirectBlockGroup::Run() stores a raw pointer to the
    // service.
    TVector<std::shared_ptr<TPartitionDirectServiceMock>> Services;

    void SetUp(NUnitTest::TTestContext& context) override;
    void TearDown(NUnitTest::TTestContext& context) override;

    // Creates a mock service and starts the dbg.
    NThreading::TFuture<void> RunAndGetInitialReady(
        const std::shared_ptr<TDirectBlockGroup>& dbg,
        bool dropScheduledCallbacks = true);

    TExecutorPtr MakeExecutor();
    // Dispatches one batch of events queued in the runtime.
    bool DispatchRuntimeOnce(NActors::TDispatchOptions options = {}) const;
    // Dispatches everything currently queued in the runtime.
    void DrainRuntime() const;

    [[nodiscard]] std::shared_ptr<TDirectBlockGroup> MakeDirectBlockGroup(
        const TExecutorPtr& executor,
        std::unique_ptr<NTransport::IStorageTransport> transport,
        const TVector<NKikimr::NBsController::TDDiskId>& ddisksIds,
        const TVector<NKikimr::NBsController::TDDiskId>& pbufferIds) const;

    template <typename TTransport>
        requires std::derived_from<TTransport, NTransport::IStorageTransport>
    [[nodiscard]] std::shared_ptr<TDirectBlockGroup> MakeDirectBlockGroup(
        const TExecutorPtr& executor,
        std::unique_ptr<TTransport> transport) const
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
    bool DoExecutorAndRuntimeWorkWithPredicate(
        const TExecutorPtr& executor,
        std::function<bool()> predicate,
        TDuration timeout) const;

    // Pumps runtime + executor for a bounded time so in-flight async work (sent
    // requests, callbacks) settles, without waiting for a specific condition.
    void DoAllExecutorAndRuntimeWork(
        const TExecutorPtr& executor,
        TDuration duration = DefaultExecutorAndRuntimeDuration) const;

    // Pumps runtime + executor until `future` is resolved, then returns its
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

    // Waits for a future and asserts it resolved.
    void WaitReady(
        const NThreading::TFuture<void>& future,
        TDuration timeout = DefaultWaitFutureTimeout);

    // Waits for a future by pumping the runtime + executor, then asserts it
    // resolved.
    void WaitReady(
        const TExecutorPtr& executor,
        const NThreading::TFuture<void>& future,
        TDuration timeout = DefaultWaitFutureTimeout);
};

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
