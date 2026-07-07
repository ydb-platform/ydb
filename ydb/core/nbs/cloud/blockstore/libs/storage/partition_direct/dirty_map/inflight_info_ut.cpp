#include "inflight_info.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestReadyQueue: public IReadyQueue
{
    void Register(ui64 lsn, EQueueType queueType) override
    {
        switch (queueType) {
            case IReadyQueue::EQueueType::Clone: {
                ReadyToClone.insert(lsn);

                ReadyToFlush.erase(lsn);
                ReadyToErase.erase(lsn);
                break;
            }
            case IReadyQueue::EQueueType::Flush: {
                ReadyToFlush.insert(lsn);

                ReadyToClone.erase(lsn);
                ReadyToErase.erase(lsn);
                break;
            }
            case IReadyQueue::EQueueType::Erase: {
                ReadyToErase.insert(lsn);

                ReadyToClone.erase(lsn);
                ReadyToFlush.erase(lsn);
                break;
            }
        }
    }

    void UnRegister(ui64 lsn) override
    {
        ReadyToErase.erase(lsn);
        ReadyToClone.erase(lsn);
        ReadyToFlush.erase(lsn);
    }

    void DataToPBufferAdded(
        THostIndex host,
        EPBufferCounter counter,
        size_t size) override
    {
        PBufferCounters[host][counter] += size;
    }

    void DataFromPBufferReleased(
        THostIndex host,
        EPBufferCounter counter,
        size_t size) override
    {
        PBufferCounters[host][counter] -= size;
    }

    size_t GetTotalBytes(THostIndex host)
    {
        return PBufferCounters[host][EPBufferCounter::Total];
    }

    size_t GetLockedBytes(THostIndex host)
    {
        return PBufferCounters[host][EPBufferCounter::Locked];
    }

    THashSet<ui64> ReadyToClone;
    THashSet<ui64> ReadyToFlush;
    THashSet<ui64> ReadyToErase;
    TMap<THostIndex, TMap<EPBufferCounter, size_t>> PBufferCounters;
};

THostMask MakePrimaryHosts()
{
    return THostMask::MakeAll(3);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TInflightInfoTests)
{
    Y_UNIT_TEST(ShouldHandleRestore)
    {
        TTestReadyQueue readyQueue;
        TInflightInfo inflightInfo(&readyQueue, 123, 4096, THostIndex{0});
        UNIT_ASSERT_VALUES_EQUAL(true, readyQueue.ReadyToClone.contains(123));

        inflightInfo.RestorePBuffer(THostIndex{1});
        UNIT_ASSERT_VALUES_EQUAL(true, readyQueue.ReadyToClone.contains(123));

        inflightInfo.RestorePBuffer(THostIndex{2});
        UNIT_ASSERT_VALUES_EQUAL(false, readyQueue.ReadyToClone.contains(123));
        UNIT_ASSERT_VALUES_EQUAL(true, readyQueue.ReadyToFlush.contains(123));
    }

    Y_UNIT_TEST(ShouldHandleConfirmedWrite)
    {
        TTestReadyQueue readyQueue;
        TInflightInfo inflightInfo(&readyQueue, 123, 4096);
        inflightInfo.OnWritten(MakePrimaryHosts(), MakePrimaryHosts());
        UNIT_ASSERT_VALUES_EQUAL(true, readyQueue.ReadyToFlush.contains(123));

        // Start flushes
        UNIT_ASSERT_VALUES_EQUAL(
            THostIndex{0},
            inflightInfo.RequestFlush(THostIndex{0}, THostMask()));
        UNIT_ASSERT_VALUES_EQUAL(
            THostIndex{1},
            inflightInfo.RequestFlush(THostIndex{1}, THostMask()));
        UNIT_ASSERT_VALUES_EQUAL(
            THostIndex{2},
            inflightInfo.RequestFlush(THostIndex{2}, THostMask()));

        // Confirm flushes
        inflightInfo.ConfirmFlush(THostRoute{
            .SourceHostIndex = THostIndex{0},
            .DestinationHostIndex = THostIndex{0}});
        UNIT_ASSERT_VALUES_EQUAL(false, readyQueue.ReadyToErase.contains(123));
        inflightInfo.ConfirmFlush(
            {.SourceHostIndex = THostIndex{1},
             .DestinationHostIndex = THostIndex{1}});
        UNIT_ASSERT_VALUES_EQUAL(false, readyQueue.ReadyToErase.contains(123));
        inflightInfo.ConfirmFlush(
            {.SourceHostIndex = THostIndex{2},
             .DestinationHostIndex = THostIndex{2}});
        UNIT_ASSERT_VALUES_EQUAL(true, readyQueue.ReadyToErase.contains(123));

        // Check erase requests
        inflightInfo.RequestErase(THostIndex{0});
        inflightInfo.RequestErase(THostIndex{1});
        inflightInfo.RequestErase(THostIndex{2});

        // Confirm erases
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            inflightInfo.ConfirmErase(THostIndex{0}));
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            inflightInfo.ConfirmErase(THostIndex{1}));
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            inflightInfo.ConfirmErase(THostIndex{2}));
    }

    Y_UNIT_TEST(ShouldHandleLock)
    {
        TTestReadyQueue readyQueue;
        TInflightInfo inflightInfo(&readyQueue, 123, 4096);
        inflightInfo.OnWritten(MakePrimaryHosts(), MakePrimaryHosts());
        UNIT_ASSERT_VALUES_EQUAL(true, readyQueue.ReadyToFlush.contains(123));

        // Start flushes
        auto l = inflightInfo.RequestFlush(THostIndex{0}, THostMask());
        l = inflightInfo.RequestFlush(THostIndex{1}, THostMask());
        l = inflightInfo.RequestFlush(THostIndex{2}, THostMask());
        Y_UNUSED(l);

        // Confirm two flushes
        inflightInfo.ConfirmFlush(THostRoute{
            .SourceHostIndex = THostIndex{0},
            .DestinationHostIndex = THostIndex{0}});
        inflightInfo.ConfirmFlush(
            {.SourceHostIndex = THostIndex{1},
             .DestinationHostIndex = THostIndex{1}});

        // Check lock/unlock PBuffer
        inflightInfo.LockPBuffer();
        UNIT_ASSERT_VALUES_EQUAL(true, readyQueue.ReadyToErase.empty());

        // Confirm last flush
        inflightInfo.ConfirmFlush(
            {.SourceHostIndex = THostIndex{2},
             .DestinationHostIndex = THostIndex{2}});
        UNIT_ASSERT_VALUES_EQUAL(true, readyQueue.ReadyToErase.empty());

        inflightInfo.UnlockPBuffer();
        UNIT_ASSERT_VALUES_EQUAL(true, readyQueue.ReadyToErase.contains(123));

        // Check erase requests
        inflightInfo.RequestErase(THostIndex{0});
        inflightInfo.RequestErase(THostIndex{1});
        inflightInfo.RequestErase(THostIndex{2});

        // Confirm erases
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            inflightInfo.ConfirmErase(THostIndex{0}));
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            inflightInfo.ConfirmErase(THostIndex{1}));
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            inflightInfo.ConfirmErase(THostIndex{2}));
    }

    Y_UNIT_TEST(ShouldPutToReadyQueueOnFail)
    {
        TTestReadyQueue readyQueue;
        TInflightInfo inflightInfo(&readyQueue, 123, 4096);
        inflightInfo.OnWritten(MakePrimaryHosts(), MakePrimaryHosts());

        // Flush started
        UNIT_ASSERT_VALUES_EQUAL(
            THostIndex{0},
            inflightInfo.RequestFlush(THostIndex{0}, THostMask()));
        UNIT_ASSERT_VALUES_EQUAL(
            THostIndex{1},
            inflightInfo.RequestFlush(THostIndex{1}, THostMask()));
        UNIT_ASSERT_VALUES_EQUAL(
            THostIndex{2},
            inflightInfo.RequestFlush(THostIndex{2}, THostMask()));

        // When a flush fails, the lsn must be queued for a flush again.
        readyQueue.ReadyToFlush.clear();
        inflightInfo.FlushFailed(THostRoute{
            .SourceHostIndex = THostIndex{0},
            .DestinationHostIndex = THostIndex{0}});
        UNIT_ASSERT_VALUES_EQUAL(true, readyQueue.ReadyToFlush.contains(123));

        // Restart flush to host 0
        UNIT_ASSERT_VALUES_EQUAL(
            THostIndex{0},
            inflightInfo.RequestFlush(THostIndex{0}, THostMask()));

        // Confirm flushes
        inflightInfo.ConfirmFlush(THostRoute{
            .SourceHostIndex = THostIndex{0},
            .DestinationHostIndex = THostIndex{0}});
        UNIT_ASSERT_VALUES_EQUAL(false, readyQueue.ReadyToErase.contains(123));
        inflightInfo.ConfirmFlush(THostRoute{
            .SourceHostIndex = THostIndex{1},
            .DestinationHostIndex = THostIndex{1}});
        UNIT_ASSERT_VALUES_EQUAL(false, readyQueue.ReadyToErase.contains(123));
        inflightInfo.ConfirmFlush(THostRoute{
            .SourceHostIndex = THostIndex{2},
            .DestinationHostIndex = THostIndex{2}});
        UNIT_ASSERT_VALUES_EQUAL(true, readyQueue.ReadyToErase.contains(123));

        // Erase started
        inflightInfo.RequestErase(THostIndex{0});
        inflightInfo.RequestErase(THostIndex{1});
        inflightInfo.RequestErase(THostIndex{2});

        // When a erase fails, the lsn must be queued for a erase again.
        readyQueue.ReadyToErase.clear();
        inflightInfo.EraseFailed(THostIndex{0});
        UNIT_ASSERT_VALUES_EQUAL(true, readyQueue.ReadyToErase.contains(123));
    }

    Y_UNIT_TEST(ShouldCountTotalBytesForRestore)
    {
        TTestReadyQueue readyQueue;
        {
            TInflightInfo inflightInfo(&readyQueue, 123, 4096, THostIndex{0});

            inflightInfo.RestorePBuffer(THostIndex{1});
            inflightInfo.RestorePBuffer(THostIndex{2});

            UNIT_ASSERT_VALUES_EQUAL(
                4096,
                readyQueue.GetTotalBytes(THostIndex{0}));
            UNIT_ASSERT_VALUES_EQUAL(
                4096,
                readyQueue.GetTotalBytes(THostIndex{1}));
            UNIT_ASSERT_VALUES_EQUAL(
                4096,
                readyQueue.GetTotalBytes(THostIndex{2}));
        }
        UNIT_ASSERT_VALUES_EQUAL(0, readyQueue.GetTotalBytes(THostIndex{0}));
        UNIT_ASSERT_VALUES_EQUAL(0, readyQueue.GetTotalBytes(THostIndex{1}));
        UNIT_ASSERT_VALUES_EQUAL(0, readyQueue.GetTotalBytes(THostIndex{2}));
    }

    Y_UNIT_TEST(ShouldCountTotalBytesForConfirmedWrite)
    {
        TTestReadyQueue readyQueue;
        {
            TInflightInfo inflightInfo(&readyQueue, 123, 4096);
            inflightInfo.OnWritten(MakePrimaryHosts(), MakePrimaryHosts());

            UNIT_ASSERT_VALUES_EQUAL(
                4096,
                readyQueue.GetTotalBytes(THostIndex{0}));
            UNIT_ASSERT_VALUES_EQUAL(
                4096,
                readyQueue.GetTotalBytes(THostIndex{1}));
            UNIT_ASSERT_VALUES_EQUAL(
                4096,
                readyQueue.GetTotalBytes(THostIndex{2}));
        }
        UNIT_ASSERT_VALUES_EQUAL(0, readyQueue.GetTotalBytes(THostIndex{0}));
        UNIT_ASSERT_VALUES_EQUAL(0, readyQueue.GetTotalBytes(THostIndex{1}));
        UNIT_ASSERT_VALUES_EQUAL(0, readyQueue.GetTotalBytes(THostIndex{2}));
    }

    Y_UNIT_TEST(ShouldTrackGetEraseNeeded)
    {
        TTestReadyQueue readyQueue;
        TInflightInfo inflightInfo(&readyQueue, 123, 4096);
        inflightInfo.OnWritten(MakePrimaryHosts(), MakePrimaryHosts());

        // Start and confirm flushes to all 3 hosts.
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            inflightInfo.RequestFlush(THostIndex{0}, THostMask()));
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            inflightInfo.RequestFlush(THostIndex{1}, THostMask()));
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            inflightInfo.RequestFlush(THostIndex{2}, THostMask()));

        inflightInfo.ConfirmFlush(THostRoute{
            .SourceHostIndex = THostIndex{0},
            .DestinationHostIndex = THostIndex{0}});
        inflightInfo.ConfirmFlush(THostRoute{
            .SourceHostIndex = THostIndex{1},
            .DestinationHostIndex = THostIndex{1}});
        inflightInfo.ConfirmFlush(THostRoute{
            .SourceHostIndex = THostIndex{2},
            .DestinationHostIndex = THostIndex{2}});

        // Before any erase, all written hosts need erasing.
        auto eraseNeeded = inflightInfo.GetEraseNeeded();
        UNIT_ASSERT_VALUES_EQUAL(true, eraseNeeded.Get(THostIndex{0}));
        UNIT_ASSERT_VALUES_EQUAL(true, eraseNeeded.Get(THostIndex{1}));
        UNIT_ASSERT_VALUES_EQUAL(true, eraseNeeded.Get(THostIndex{2}));
        UNIT_ASSERT_VALUES_EQUAL(3, eraseNeeded.Count());

        // After requesting erase for host 0, it should no longer be in
        // GetEraseNeeded (it's now in EraseRequested).
        inflightInfo.RequestErase(THostIndex{0});
        eraseNeeded = inflightInfo.GetEraseNeeded();
        UNIT_ASSERT_VALUES_EQUAL(false, eraseNeeded.Get(THostIndex{0}));
        UNIT_ASSERT_VALUES_EQUAL(true, eraseNeeded.Get(THostIndex{1}));
        UNIT_ASSERT_VALUES_EQUAL(true, eraseNeeded.Get(THostIndex{2}));
        UNIT_ASSERT_VALUES_EQUAL(2, eraseNeeded.Count());

        // After confirming erase for host 0, it's in EraseConfirmed and still
        // excluded from GetEraseNeeded.
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            inflightInfo.ConfirmErase(THostIndex{0}));
        eraseNeeded = inflightInfo.GetEraseNeeded();
        UNIT_ASSERT_VALUES_EQUAL(false, eraseNeeded.Get(THostIndex{0}));
        UNIT_ASSERT_VALUES_EQUAL(true, eraseNeeded.Get(THostIndex{1}));
        UNIT_ASSERT_VALUES_EQUAL(true, eraseNeeded.Get(THostIndex{2}));
        UNIT_ASSERT_VALUES_EQUAL(2, eraseNeeded.Count());

        // After requesting and confirming all remaining hosts, nothing is left.
        inflightInfo.RequestErase(THostIndex{1});
        inflightInfo.RequestErase(THostIndex{2});
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            inflightInfo.ConfirmErase(THostIndex{1}));
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            inflightInfo.ConfirmErase(THostIndex{2}));
        eraseNeeded = inflightInfo.GetEraseNeeded();
        UNIT_ASSERT_VALUES_EQUAL(0, eraseNeeded.Count());
    }

    Y_UNIT_TEST(ShouldReturnEraseNeededAfterEraseFailed)
    {
        TTestReadyQueue readyQueue;
        TInflightInfo inflightInfo(&readyQueue, 123, 4096);
        inflightInfo.OnWritten(MakePrimaryHosts(), MakePrimaryHosts());

        // Flush all hosts.
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            inflightInfo.RequestFlush(THostIndex{0}, THostMask()));
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            inflightInfo.RequestFlush(THostIndex{1}, THostMask()));
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            inflightInfo.RequestFlush(THostIndex{2}, THostMask()));

        inflightInfo.ConfirmFlush(THostRoute{
            .SourceHostIndex = THostIndex{0},
            .DestinationHostIndex = THostIndex{0}});
        inflightInfo.ConfirmFlush(THostRoute{
            .SourceHostIndex = THostIndex{1},
            .DestinationHostIndex = THostIndex{1}});
        inflightInfo.ConfirmFlush(THostRoute{
            .SourceHostIndex = THostIndex{2},
            .DestinationHostIndex = THostIndex{2}});

        // Request erase for host 0 and then fail it.
        inflightInfo.RequestErase(THostIndex{0});
        auto eraseNeeded = inflightInfo.GetEraseNeeded();
        UNIT_ASSERT_VALUES_EQUAL(2, eraseNeeded.Count());
        UNIT_ASSERT_VALUES_EQUAL(false, eraseNeeded.Get(THostIndex{0}));

        // EraseFailed resets EraseRequested for host 0, so GetEraseNeeded
        // should include it again.
        inflightInfo.EraseFailed(THostIndex{0});
        eraseNeeded = inflightInfo.GetEraseNeeded();
        UNIT_ASSERT_VALUES_EQUAL(3, eraseNeeded.Count());
        UNIT_ASSERT_VALUES_EQUAL(true, eraseNeeded.Get(THostIndex{0}));
    }

    Y_UNIT_TEST(ShouldReturnDDiskReadMaskForPendingWrite)
    {
        TTestReadyQueue readyQueue;
        TInflightInfo inflightInfo(&readyQueue, 123, 4096);

        // In PBufferPendingWrite state, ReadMask should return DDisk with all
        // hosts enabled (Lsn=0 means DDisk read).
        UNIT_ASSERT_VALUES_EQUAL(
            TInflightInfo::EState::PBufferPendingWrite,
            inflightInfo.GetState());

        auto readSource = inflightInfo.ReadMask();
        UNIT_ASSERT_VALUES_EQUAL(true, readSource.OnlyDDisk());
        UNIT_ASSERT_VALUES_EQUAL(false, readSource.Empty());

        // After OnWritten, should switch to PBuffer read with non-zero Lsn.
        inflightInfo.OnWritten(MakePrimaryHosts(), MakePrimaryHosts());
        UNIT_ASSERT_VALUES_EQUAL(
            TInflightInfo::EState::PBufferWritten,
            inflightInfo.GetState());

        readSource = inflightInfo.ReadMask();
        UNIT_ASSERT_VALUES_EQUAL(false, readSource.OnlyDDisk());
        UNIT_ASSERT_VALUES_EQUAL(false, readSource.Empty());
        UNIT_ASSERT_VALUES_EQUAL(123, readSource.Lsn);
    }

    Y_UNIT_TEST(ShouldQuorumReadyFutureForPendingWrite)
    {
        TTestReadyQueue readyQueue;
        TInflightInfo inflightInfo(&readyQueue, 123, 4096);

        auto future = inflightInfo.GetQuorumReadyFuture();
        UNIT_ASSERT_VALUES_EQUAL(false, future.IsReady());

        // OnWritten should not trigger the quorum future (it's for
        // PBufferIncompleteWrite path).
        inflightInfo.OnWritten(MakePrimaryHosts(), MakePrimaryHosts());

        // The future should still not be ready because it's only triggered
        // by RestorePBuffer reaching quorum.
        UNIT_ASSERT_VALUES_EQUAL(false, future.IsReady());
    }

    Y_UNIT_TEST(ShouldQuorumReadyFutureForIncompleteWrite)
    {
        TTestReadyQueue readyQueue;
        TInflightInfo inflightInfo(&readyQueue, 123, 4096, THostIndex{0});

        auto future = inflightInfo.GetQuorumReadyFuture();
        UNIT_ASSERT_VALUES_EQUAL(false, future.IsReady());

        inflightInfo.RestorePBuffer(THostIndex{1});
        UNIT_ASSERT_VALUES_EQUAL(false, future.IsReady());

        // Reaching quorum (3 hosts) should trigger the future.
        inflightInfo.RestorePBuffer(THostIndex{2});
        UNIT_ASSERT_VALUES_EQUAL(true, future.IsReady());
    }

    Y_UNIT_TEST(ShouldCountTotalBytesForPendingWrite)
    {
        TTestReadyQueue readyQueue;
        {
            TInflightInfo inflightInfo(&readyQueue, 123, 4096);

            // Pending write should not account any bytes.
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                readyQueue.GetTotalBytes(THostIndex{0}));
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                readyQueue.GetTotalBytes(THostIndex{1}));
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                readyQueue.GetTotalBytes(THostIndex{2}));

            // After OnWritten, bytes should be accounted on written hosts.
            inflightInfo.OnWritten(MakePrimaryHosts(), MakePrimaryHosts());
            UNIT_ASSERT_VALUES_EQUAL(
                4096,
                readyQueue.GetTotalBytes(THostIndex{0}));
            UNIT_ASSERT_VALUES_EQUAL(
                4096,
                readyQueue.GetTotalBytes(THostIndex{1}));
            UNIT_ASSERT_VALUES_EQUAL(
                4096,
                readyQueue.GetTotalBytes(THostIndex{2}));
        }
        // After destruction, bytes should be released.
        UNIT_ASSERT_VALUES_EQUAL(0, readyQueue.GetTotalBytes(THostIndex{0}));
        UNIT_ASSERT_VALUES_EQUAL(0, readyQueue.GetTotalBytes(THostIndex{1}));
        UNIT_ASSERT_VALUES_EQUAL(0, readyQueue.GetTotalBytes(THostIndex{2}));
    }

    Y_UNIT_TEST(ShouldRemoveHostsNoOpWhenNoOverlap)
    {
        TTestReadyQueue readyQueue;
        TInflightInfo inflightInfo(&readyQueue, 123, 4096);
        inflightInfo.OnWritten(MakePrimaryHosts(), MakePrimaryHosts());

        // Hosts 3 and 4 were never written to — RemoveHosts should be a no-op.
        THostMask removed;
        removed.Set(THostIndex{3});
        removed.Set(THostIndex{4});
        inflightInfo.RemoveHosts(removed);

        // State stays PBufferWritten; bytes unchanged.
        UNIT_ASSERT_VALUES_EQUAL(
            TInflightInfo::EState::PBufferWritten,
            inflightInfo.GetState());
        UNIT_ASSERT_VALUES_EQUAL(4096, readyQueue.GetTotalBytes(THostIndex{0}));
        UNIT_ASSERT_VALUES_EQUAL(4096, readyQueue.GetTotalBytes(THostIndex{1}));
        UNIT_ASSERT_VALUES_EQUAL(4096, readyQueue.GetTotalBytes(THostIndex{2}));
    }

    Y_UNIT_TEST(ShouldRemoveHostsReleaseBytesInWrittenState)
    {
        TTestReadyQueue readyQueue;
        TInflightInfo inflightInfo(&readyQueue, 123, 4096);
        inflightInfo.OnWritten(MakePrimaryHosts(), MakePrimaryHosts());

        // All 3 hosts have 4096 bytes in Total.
        UNIT_ASSERT_VALUES_EQUAL(4096, readyQueue.GetTotalBytes(THostIndex{0}));
        UNIT_ASSERT_VALUES_EQUAL(4096, readyQueue.GetTotalBytes(THostIndex{1}));
        UNIT_ASSERT_VALUES_EQUAL(4096, readyQueue.GetTotalBytes(THostIndex{2}));

        // Remove host 2.
        THostMask removed;
        removed.Set(THostIndex{2});
        inflightInfo.RemoveHosts(removed);

        // State stays PBufferWritten; host 2 bytes released.
        UNIT_ASSERT_VALUES_EQUAL(
            TInflightInfo::EState::PBufferWritten,
            inflightInfo.GetState());
        UNIT_ASSERT_VALUES_EQUAL(4096, readyQueue.GetTotalBytes(THostIndex{0}));
        UNIT_ASSERT_VALUES_EQUAL(4096, readyQueue.GetTotalBytes(THostIndex{1}));
        UNIT_ASSERT_VALUES_EQUAL(0, readyQueue.GetTotalBytes(THostIndex{2}));
    }

    Y_UNIT_TEST(ShouldRemoveHostsCompleteFlush)
    {
        TTestReadyQueue readyQueue;
        TInflightInfo inflightInfo(&readyQueue, 123, 4096);
        inflightInfo.OnWritten(MakePrimaryHosts(), MakePrimaryHosts());

        // Request flush for all 3 hosts.
        auto l = inflightInfo.RequestFlush(THostIndex{0}, THostMask());
        l = inflightInfo.RequestFlush(THostIndex{1}, THostMask());
        l = inflightInfo.RequestFlush(THostIndex{2}, THostMask());
        Y_UNUSED(l);

        // Confirm flush for hosts 0 and 1 only.
        inflightInfo.ConfirmFlush(THostRoute{
            .SourceHostIndex = THostIndex{0},
            .DestinationHostIndex = THostIndex{0}});
        inflightInfo.ConfirmFlush(THostRoute{
            .SourceHostIndex = THostIndex{1},
            .DestinationHostIndex = THostIndex{1}});

        UNIT_ASSERT_VALUES_EQUAL(
            TInflightInfo::EState::PBufferFlushing,
            inflightInfo.GetState());
        UNIT_ASSERT_VALUES_EQUAL(false, readyQueue.ReadyToErase.contains(123));

        // Remove host 2 — flush should now be complete.
        THostMask removed;
        removed.Set(THostIndex{2});
        inflightInfo.RemoveHosts(removed);

        UNIT_ASSERT_VALUES_EQUAL(
            TInflightInfo::EState::PBufferFlushed,
            inflightInfo.GetState());
        // Erase should be registered.
        UNIT_ASSERT_VALUES_EQUAL(true, readyQueue.ReadyToErase.contains(123));
        // Host 2 bytes released.
        UNIT_ASSERT_VALUES_EQUAL(0, readyQueue.GetTotalBytes(THostIndex{2}));
    }

    Y_UNIT_TEST(ShouldRemoveHostsCompleteErase)
    {
        TTestReadyQueue readyQueue;
        TInflightInfo inflightInfo(&readyQueue, 123, 4096);
        inflightInfo.OnWritten(MakePrimaryHosts(), MakePrimaryHosts());

        // Flush all hosts.
        auto l = inflightInfo.RequestFlush(THostIndex{0}, THostMask());
        l = inflightInfo.RequestFlush(THostIndex{1}, THostMask());
        l = inflightInfo.RequestFlush(THostIndex{2}, THostMask());
        Y_UNUSED(l);
        inflightInfo.ConfirmFlush(THostRoute{
            .SourceHostIndex = THostIndex{0},
            .DestinationHostIndex = THostIndex{0}});
        inflightInfo.ConfirmFlush(THostRoute{
            .SourceHostIndex = THostIndex{1},
            .DestinationHostIndex = THostIndex{1}});
        inflightInfo.ConfirmFlush(THostRoute{
            .SourceHostIndex = THostIndex{2},
            .DestinationHostIndex = THostIndex{2}});

        // Start erasing hosts 0 and 1, confirm both.
        inflightInfo.RequestErase(THostIndex{0});
        inflightInfo.RequestErase(THostIndex{1});
        inflightInfo.RequestErase(THostIndex{2});
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            inflightInfo.ConfirmErase(THostIndex{0}));
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            inflightInfo.ConfirmErase(THostIndex{1}));

        UNIT_ASSERT_VALUES_EQUAL(
            TInflightInfo::EState::PBufferErasing,
            inflightInfo.GetState());

        // Remove host 2 — erase should now be complete.
        THostMask removed;
        removed.Set(THostIndex{2});
        inflightInfo.RemoveHosts(removed);

        UNIT_ASSERT_VALUES_EQUAL(
            TInflightInfo::EState::PBufferErased,
            inflightInfo.GetState());
        UNIT_ASSERT_VALUES_EQUAL(0, readyQueue.GetTotalBytes(THostIndex{2}));
    }

    Y_UNIT_TEST(ShouldRemoveHostsReleaseLockBytesWhenLocked)
    {
        TTestReadyQueue readyQueue;
        TInflightInfo inflightInfo(&readyQueue, 123, 4096);
        inflightInfo.OnWritten(MakePrimaryHosts(), MakePrimaryHosts());

        // Lock PBuffer — this should track locked bytes.
        inflightInfo.LockPBuffer();
        UNIT_ASSERT_VALUES_EQUAL(
            4096,
            readyQueue.GetLockedBytes(THostIndex{0}));
        UNIT_ASSERT_VALUES_EQUAL(
            4096,
            readyQueue.GetLockedBytes(THostIndex{1}));
        UNIT_ASSERT_VALUES_EQUAL(
            4096,
            readyQueue.GetLockedBytes(THostIndex{2}));

        // Remove host 2 while locked.
        THostMask removed;
        removed.Set(THostIndex{2});
        inflightInfo.RemoveHosts(removed);

        // Both Total and Locked bytes for host 2 should be released.
        UNIT_ASSERT_VALUES_EQUAL(0, readyQueue.GetTotalBytes(THostIndex{2}));
        UNIT_ASSERT_VALUES_EQUAL(0, readyQueue.GetLockedBytes(THostIndex{2}));

        // Hosts 0 and 1 should be unaffected.
        UNIT_ASSERT_VALUES_EQUAL(4096, readyQueue.GetTotalBytes(THostIndex{0}));
        UNIT_ASSERT_VALUES_EQUAL(
            4096,
            readyQueue.GetLockedBytes(THostIndex{0}));
        UNIT_ASSERT_VALUES_EQUAL(4096, readyQueue.GetTotalBytes(THostIndex{1}));
        UNIT_ASSERT_VALUES_EQUAL(
            4096,
            readyQueue.GetLockedBytes(THostIndex{1}));

        // Unlock so destructor doesn't assert.
        inflightInfo.UnlockPBuffer();
    }

    Y_UNIT_TEST(ShouldRemoveHostsNotRegisterEraseWhenLocked)
    {
        TTestReadyQueue readyQueue;
        TInflightInfo inflightInfo(&readyQueue, 123, 4096);
        inflightInfo.OnWritten(MakePrimaryHosts(), MakePrimaryHosts());

        // Request and confirm flush for hosts 0 and 1.
        auto l = inflightInfo.RequestFlush(THostIndex{0}, THostMask());
        l = inflightInfo.RequestFlush(THostIndex{1}, THostMask());
        l = inflightInfo.RequestFlush(THostIndex{2}, THostMask());
        Y_UNUSED(l);
        inflightInfo.ConfirmFlush(THostRoute{
            .SourceHostIndex = THostIndex{0},
            .DestinationHostIndex = THostIndex{0}});
        inflightInfo.ConfirmFlush(THostRoute{
            .SourceHostIndex = THostIndex{1},
            .DestinationHostIndex = THostIndex{1}});
        UNIT_ASSERT_VALUES_EQUAL(
            TInflightInfo::EState::PBufferFlushing,
            inflightInfo.GetState());

        // Lock PBuffer before completing flush.
        inflightInfo.LockPBuffer();

        // Remove host 2 — flush completes, but erase should NOT register
        // because PBuffer is locked.
        THostMask removed;
        removed.Set(THostIndex{2});
        inflightInfo.RemoveHosts(removed);

        UNIT_ASSERT_VALUES_EQUAL(
            TInflightInfo::EState::PBufferFlushed,
            inflightInfo.GetState());
        UNIT_ASSERT_VALUES_EQUAL(false, readyQueue.ReadyToErase.contains(123));

        // After unlock, erase should be registered.
        inflightInfo.UnlockPBuffer();
        UNIT_ASSERT_VALUES_EQUAL(true, readyQueue.ReadyToErase.contains(123));
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
