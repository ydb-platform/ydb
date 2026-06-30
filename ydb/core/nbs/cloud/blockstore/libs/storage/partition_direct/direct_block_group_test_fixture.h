#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/direct_block_group_impl.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport/storage_transport_mock.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport/testlib/ic_storage_transport_test_adapter.h>

#include <ydb/core/nbs/cloud/storage/core/libs/coroutine/executor.h>

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/testlib/actors/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

TVector<NKikimr::NBsController::TDDiskId> MakeDDiskIds(ui32 baseNodeId);

struct TDBGFixture: public NUnitTest::TBaseFixture
{
    std::unique_ptr<NActors::TTestActorRuntime> Runtime;
    TVector<TExecutorPtr> Executors;

    void SetUp(NUnitTest::TTestContext& context) override;

    void TearDown(NUnitTest::TTestContext& context) override;

    // Dispatches one batch of events queued in the runtime.
    bool DispatchRuntimeOnce(NActors::TDispatchOptions options = {}) const;

    // Dispatches everything currently queued in the runtime.
    void DrainRuntime() const;

    TExecutorPtr MakeExecutor();

    [[nodiscard]] std::shared_ptr<TDirectBlockGroup> MakeDirectBlockGroup(
        const TExecutorPtr& executor,
        std::unique_ptr<NTransport::IStorageTransport> transport,
        const TVector<NKikimr::NBsController::TDDiskId>& ddisksIds,
        const TVector<NKikimr::NBsController::TDDiskId>& pbufferIds) const;

    [[nodiscard]] std::shared_ptr<TDirectBlockGroup> MakeDirectBlockGroup(
        const TExecutorPtr& executor,
        std::unique_ptr<NTransport::TStorageTransportMock> transport,
        ui32 baseNodeId) const;

    [[nodiscard]] std::shared_ptr<TDirectBlockGroup> MakeDirectBlockGroup(
        const TExecutorPtr& executor,
        std::unique_ptr<NTransport::NTestLib::TICStorageTransportTestAdapter>
            transport) const;

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
        TDuration duration) const;

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
};

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
