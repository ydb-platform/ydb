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

// The default set of DDisk hosts a write is expected to be flushed to. Three
// hosts satisfy the quorum (QuorumDirectBlockGroupHostCount == 3).
THostMask MakeDDisks(size_t hostCount = 3)
{
    return THostMask::MakeAll(hostCount);
}

THostMask MakePrimaryHosts(size_t hostCount = 3)
{
    return THostMask::MakeAll(hostCount);
}

// Drives an inflight from PBufferWritten through a full flush of all
// `hostCount` hosts, leaving it in PBufferFlushed.
void FlushAll(TInflightInfo& inflightInfo)
{
    auto ddisks = MakeDDisks();
    for (THostIndex host: ddisks) {
        THostIndex result = inflightInfo.RequestFlush(host);
        UNIT_ASSERT_VALUES_UNEQUAL(InvalidHostIndex, result);
    }
    for (THostIndex host: ddisks) {
        inflightInfo.ConfirmFlush(host);
    }
}

// Erases all `hostCount` hosts, moving a flushed inflight to PBufferErased.
void EraseAll(TInflightInfo& inflightInfo)
{
    auto pbuffers = inflightInfo.GetEraseNeeded();

    for (THostIndex host: pbuffers) {
        inflightInfo.RequestErase(host);
    }
    for (THostIndex host: pbuffers) {
        Y_UNUSED(inflightInfo.ConfirmErase(host));
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TInflightInfoTests)
{
    Y_UNIT_TEST(ShouldHandleRestore)
    {
        TTestReadyQueue readyQueue;
        TInflightInfo inflightInfo(
            &readyQueue,
            MakeDDisks(),
            THostMask::MakeEmpty(),
            123,
            4096,
            THostIndex{0});
        UNIT_ASSERT_VALUES_EQUAL(true, readyQueue.ReadyToClone.contains(123));
        UNIT_ASSERT_VALUES_EQUAL(
            TInflightInfo::EState::PBufferIncompleteWrite,
            inflightInfo.GetState());

        // Restoring a second PBuffer does not yet reach the quorum (3 hosts).
        inflightInfo.RestorePBuffer(THostIndex{1});
        UNIT_ASSERT_VALUES_EQUAL(true, readyQueue.ReadyToClone.contains(123));
        UNIT_ASSERT_VALUES_EQUAL(
            TInflightInfo::EState::PBufferIncompleteWrite,
            inflightInfo.GetState());

        // Third PBuffer reaches the quorum: switches to Written and the lsn
        // moves from the clone queue to the flush queue.
        inflightInfo.RestorePBuffer(THostIndex{2});
        UNIT_ASSERT_VALUES_EQUAL(false, readyQueue.ReadyToClone.contains(123));
        UNIT_ASSERT_VALUES_EQUAL(true, readyQueue.ReadyToFlush.contains(123));
        UNIT_ASSERT_VALUES_EQUAL(
            TInflightInfo::EState::PBufferWritten,
            inflightInfo.GetState());
    }

    Y_UNIT_TEST(ShouldHandleConfirmedWrite)
    {
        TTestReadyQueue readyQueue;
        TInflightInfo inflightInfo(
            &readyQueue,
            MakeDDisks(),
            THostMask::MakeEmpty(),
            123,
            4096);
        inflightInfo.OnWritten(MakePrimaryHosts(), MakePrimaryHosts());
        UNIT_ASSERT_VALUES_EQUAL(true, readyQueue.ReadyToFlush.contains(123));

        // Start flushes
        UNIT_ASSERT_VALUES_EQUAL(
            THostIndex{0},
            inflightInfo.RequestFlush(THostIndex{0}));
        UNIT_ASSERT_VALUES_EQUAL(
            THostIndex{1},
            inflightInfo.RequestFlush(THostIndex{1}));
        UNIT_ASSERT_VALUES_EQUAL(
            THostIndex{2},
            inflightInfo.RequestFlush(THostIndex{2}));

        // Confirm flushes
        inflightInfo.ConfirmFlush(THostIndex{0});
        UNIT_ASSERT_VALUES_EQUAL(false, readyQueue.ReadyToErase.contains(123));
        inflightInfo.ConfirmFlush(THostIndex{1});
        UNIT_ASSERT_VALUES_EQUAL(false, readyQueue.ReadyToErase.contains(123));
        inflightInfo.ConfirmFlush(THostIndex{2});
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
        TInflightInfo inflightInfo(
            &readyQueue,
            MakeDDisks(),
            THostMask::MakeEmpty(),
            123,
            4096);
        inflightInfo.OnWritten(MakePrimaryHosts(), MakePrimaryHosts());
        UNIT_ASSERT_VALUES_EQUAL(true, readyQueue.ReadyToFlush.contains(123));

        // Start flushes
        Y_UNUSED(inflightInfo.RequestFlush(THostIndex{0}));
        Y_UNUSED(inflightInfo.RequestFlush(THostIndex{1}));
        Y_UNUSED(inflightInfo.RequestFlush(THostIndex{2}));

        // Confirm two flushes
        inflightInfo.ConfirmFlush(THostIndex{0});
        inflightInfo.ConfirmFlush(THostIndex{1});

        // Check lock/unlock PBuffer
        inflightInfo.LockPBuffer();
        UNIT_ASSERT_VALUES_EQUAL(true, readyQueue.ReadyToErase.empty());

        // Confirm last flush
        inflightInfo.ConfirmFlush(THostIndex{2});
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
        TInflightInfo inflightInfo(
            &readyQueue,
            MakeDDisks(),
            THostMask::MakeEmpty(),
            123,
            4096);
        inflightInfo.OnWritten(MakePrimaryHosts(), MakePrimaryHosts());

        // Flush started
        UNIT_ASSERT_VALUES_EQUAL(
            THostIndex{0},
            inflightInfo.RequestFlush(THostIndex{0}));
        UNIT_ASSERT_VALUES_EQUAL(
            THostIndex{1},
            inflightInfo.RequestFlush(THostIndex{1}));
        UNIT_ASSERT_VALUES_EQUAL(
            THostIndex{2},
            inflightInfo.RequestFlush(THostIndex{2}));

        // When a flush fails, the lsn must be queued for a flush again.
        readyQueue.ReadyToFlush.clear();
        inflightInfo.FlushFailed(THostIndex{0});
        UNIT_ASSERT_VALUES_EQUAL(true, readyQueue.ReadyToFlush.contains(123));

        // The failed host is no longer inflight.
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            inflightInfo.GetInflightFlushes().Get(THostIndex{0}));

        // Restart flush to host 0
        UNIT_ASSERT_VALUES_EQUAL(
            THostIndex{0},
            inflightInfo.RequestFlush(THostIndex{0}));

        // Confirm flushes
        inflightInfo.ConfirmFlush(THostIndex{0});
        UNIT_ASSERT_VALUES_EQUAL(false, readyQueue.ReadyToErase.contains(123));
        inflightInfo.ConfirmFlush(THostIndex{1});
        UNIT_ASSERT_VALUES_EQUAL(false, readyQueue.ReadyToErase.contains(123));
        inflightInfo.ConfirmFlush(THostIndex{2});
        UNIT_ASSERT_VALUES_EQUAL(true, readyQueue.ReadyToErase.contains(123));

        // Erase started
        inflightInfo.RequestErase(THostIndex{0});
        inflightInfo.RequestErase(THostIndex{1});
        inflightInfo.RequestErase(THostIndex{2});

        // When a erase fails, the lsn must be queued for a erase again.
        readyQueue.ReadyToErase.clear();
        inflightInfo.EraseFailed(THostIndex{0});
        UNIT_ASSERT_VALUES_EQUAL(true, readyQueue.ReadyToErase.contains(123));

        // Redo the failed erase so the destructor invariants hold.
        inflightInfo.RequestErase(THostIndex{0});
        Y_UNUSED(inflightInfo.ConfirmErase(THostIndex{0}));
    }

    Y_UNIT_TEST(ShouldNotRequestFlushToDisabledOrAbsentHost)
    {
        TTestReadyQueue readyQueue;
        // DesiredDDisks = {0, 1, 2}, host 2 disabled.
        TInflightInfo inflightInfo(
            &readyQueue,
            MakeDDisks(),
            THostMask::MakeMask({THostIndex{2}}),
            123,
            4096);
        inflightInfo.OnWritten(MakePrimaryHosts(), MakePrimaryHosts());

        // Flush to a disabled host is refused.
        UNIT_ASSERT_VALUES_EQUAL(
            InvalidHostIndex,
            inflightInfo.RequestFlush(THostIndex{2}));

        // Flush to a host outside DesiredDDisks is refused.
        UNIT_ASSERT_VALUES_EQUAL(
            InvalidHostIndex,
            inflightInfo.RequestFlush(THostIndex{5}));

        // Flush to an enabled desired host succeeds.
        UNIT_ASSERT_VALUES_EQUAL(
            THostIndex{0},
            inflightInfo.RequestFlush(THostIndex{0}));

        // A second flush request to the same host is refused.
        UNIT_ASSERT_VALUES_EQUAL(
            InvalidHostIndex,
            inflightInfo.RequestFlush(THostIndex{0}));
    }

    Y_UNIT_TEST(ShouldCountTotalBytesForRestore)
    {
        TTestReadyQueue readyQueue;
        {
            TInflightInfo inflightInfo(
                &readyQueue,
                MakeDDisks(),
                THostMask::MakeEmpty(),
                123,
                4096,
                THostIndex{0});

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
            TInflightInfo inflightInfo(
                &readyQueue,
                MakeDDisks(),
                THostMask::MakeEmpty(),
                123,
                4096);
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
        TInflightInfo inflightInfo(
            &readyQueue,
            MakeDDisks(),
            THostMask::MakeEmpty(),
            123,
            4096);
        inflightInfo.OnWritten(MakePrimaryHosts(), MakePrimaryHosts());

        // Start and confirm flushes to all 3 hosts.
        FlushAll(inflightInfo);

        // Before any erase, all written hosts need erasing.
        auto eraseNeeded = inflightInfo.GetEraseNeeded();
        UNIT_ASSERT_VALUES_EQUAL("[H0,H1,H2]", eraseNeeded.Print());

        // After requesting erase for host 0, it should no longer be in
        // GetEraseNeeded (it's now in EraseRequested).
        inflightInfo.RequestErase(THostIndex{0});
        eraseNeeded = inflightInfo.GetEraseNeeded();
        UNIT_ASSERT_VALUES_EQUAL("[H1,H2]", eraseNeeded.Print());

        // After confirming erase for host 0, it's in EraseConfirmed and still
        // excluded from GetEraseNeeded.
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            inflightInfo.ConfirmErase(THostIndex{0}));
        eraseNeeded = inflightInfo.GetEraseNeeded();
        UNIT_ASSERT_VALUES_EQUAL("[H1,H2]", eraseNeeded.Print());

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
        UNIT_ASSERT_VALUES_EQUAL("[]", eraseNeeded.Print());
    }

    Y_UNIT_TEST(ShouldReturnEraseNeededAfterEraseFailed)
    {
        TTestReadyQueue readyQueue;
        TInflightInfo inflightInfo(
            &readyQueue,
            MakeDDisks(),
            THostMask::MakeEmpty(),
            123,
            4096);
        inflightInfo.OnWritten(MakePrimaryHosts(), MakePrimaryHosts());

        FlushAll(inflightInfo);

        // Request erase for host 0 and then fail it.
        inflightInfo.RequestErase(THostIndex{0});
        auto eraseNeeded = inflightInfo.GetEraseNeeded();
        UNIT_ASSERT_VALUES_EQUAL("[H1,H2]", eraseNeeded.Print());

        // EraseFailed resets EraseRequested for host 0, so GetEraseNeeded
        // should include it again.
        inflightInfo.EraseFailed(THostIndex{0});
        eraseNeeded = inflightInfo.GetEraseNeeded();
        UNIT_ASSERT_VALUES_EQUAL("[H0,H1,H2]", eraseNeeded.Print());

        // Complete the erase so the destructor invariants hold.
        EraseAll(inflightInfo);
    }

    Y_UNIT_TEST(ShouldReturnDDiskReadMaskForPendingWrite)
    {
        TTestReadyQueue readyQueue;
        TInflightInfo inflightInfo(
            &readyQueue,
            MakeDDisks(),
            THostMask::MakeEmpty(),
            123,
            4096);

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

    Y_UNIT_TEST(ShouldReturnEmptyReadMaskForIncompleteWrite)
    {
        TTestReadyQueue readyQueue;
        TInflightInfo inflightInfo(
            &readyQueue,
            MakeDDisks(),
            THostMask::MakeEmpty(),
            123,
            4096,
            THostIndex{0});

        // Incomplete write is invisible to reads until the quorum is reached.
        UNIT_ASSERT_VALUES_EQUAL(
            TInflightInfo::EState::PBufferIncompleteWrite,
            inflightInfo.GetState());

        auto readSource = inflightInfo.ReadMask();
        UNIT_ASSERT_VALUES_EQUAL(true, readSource.Empty());
        UNIT_ASSERT_VALUES_EQUAL(true, readSource.OnlyDDisk());

        // Once the quorum is restored, reads go to the confirmed PBuffers.
        inflightInfo.RestorePBuffer(THostIndex{1});
        inflightInfo.RestorePBuffer(THostIndex{2});
        UNIT_ASSERT_VALUES_EQUAL(
            TInflightInfo::EState::PBufferWritten,
            inflightInfo.GetState());

        readSource = inflightInfo.ReadMask();
        UNIT_ASSERT_VALUES_EQUAL(false, readSource.Empty());
        UNIT_ASSERT_VALUES_EQUAL(false, readSource.OnlyDDisk());
        UNIT_ASSERT_VALUES_EQUAL(123, readSource.Lsn);
    }

    Y_UNIT_TEST(ShouldReadFromDDiskAfterFlushed)
    {
        TTestReadyQueue readyQueue;
        TInflightInfo inflightInfo(
            &readyQueue,
            MakeDDisks(),
            THostMask::MakeEmpty(),
            123,
            4096);
        inflightInfo.OnWritten(MakePrimaryHosts(), MakePrimaryHosts());

        FlushAll(inflightInfo);

        UNIT_ASSERT_VALUES_EQUAL(
            TInflightInfo::EState::PBufferFlushed,
            inflightInfo.GetState());

        // Once flushed the data is read from DDisk (Lsn=0).
        auto readSource = inflightInfo.ReadMask();
        UNIT_ASSERT_VALUES_EQUAL(true, readSource.OnlyDDisk());
        UNIT_ASSERT_VALUES_EQUAL(false, readSource.Empty());

        // Drain erases so the destructor invariants hold.
        EraseAll(inflightInfo);
    }

    Y_UNIT_TEST(ShouldQuorumReadyFutureForPendingWrite)
    {
        TTestReadyQueue readyQueue;
        TInflightInfo inflightInfo(
            &readyQueue,
            MakeDDisks(),
            THostMask::MakeEmpty(),
            123,
            4096);

        auto future = inflightInfo.GetQuorumReadyFuture();
        UNIT_ASSERT_VALUES_EQUAL(false, future.IsReady());

        // OnWritten transitions to Written directly and does not go through the
        // quorum-ready promise (that path is only for PBufferIncompleteWrite).
        inflightInfo.OnWritten(MakePrimaryHosts(), MakePrimaryHosts());
        UNIT_ASSERT_VALUES_EQUAL(false, future.IsReady());
    }

    Y_UNIT_TEST(ShouldQuorumReadyFutureForIncompleteWrite)
    {
        TTestReadyQueue readyQueue;
        TInflightInfo inflightInfo(
            &readyQueue,
            MakeDDisks(),
            THostMask::MakeEmpty(),
            123,
            4096,
            THostIndex{0});

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
            TInflightInfo inflightInfo(
                &readyQueue,
                MakeDDisks(),
                THostMask::MakeEmpty(),
                123,
                4096);

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

    Y_UNIT_TEST(ShouldTrackLockedBytes)
    {
        TTestReadyQueue readyQueue;
        TInflightInfo inflightInfo(
            &readyQueue,
            MakeDDisks(),
            THostMask::MakeEmpty(),
            123,
            4096);
        inflightInfo.OnWritten(MakePrimaryHosts(), MakePrimaryHosts());

        // Locking accounts locked bytes on all confirmed hosts and unregisters
        // the lsn from the flush queue.
        inflightInfo.LockPBuffer();
        UNIT_ASSERT_VALUES_EQUAL(false, readyQueue.ReadyToFlush.contains(123));
        UNIT_ASSERT_VALUES_EQUAL(
            4096,
            readyQueue.GetLockedBytes(THostIndex{0}));
        UNIT_ASSERT_VALUES_EQUAL(
            4096,
            readyQueue.GetLockedBytes(THostIndex{1}));
        UNIT_ASSERT_VALUES_EQUAL(
            4096,
            readyQueue.GetLockedBytes(THostIndex{2}));

        // Nested lock does not double-count.
        inflightInfo.LockPBuffer();
        UNIT_ASSERT_VALUES_EQUAL(
            4096,
            readyQueue.GetLockedBytes(THostIndex{0}));

        // First unlock keeps the lock (count still > 0).
        inflightInfo.UnlockPBuffer();
        UNIT_ASSERT_VALUES_EQUAL(
            4096,
            readyQueue.GetLockedBytes(THostIndex{0}));
        UNIT_ASSERT_VALUES_EQUAL(false, readyQueue.ReadyToFlush.contains(123));

        // Final unlock releases the locked bytes and re-registers the flush.
        inflightInfo.UnlockPBuffer();
        UNIT_ASSERT_VALUES_EQUAL(0, readyQueue.GetLockedBytes(THostIndex{0}));
        UNIT_ASSERT_VALUES_EQUAL(0, readyQueue.GetLockedBytes(THostIndex{1}));
        UNIT_ASSERT_VALUES_EQUAL(0, readyQueue.GetLockedBytes(THostIndex{2}));
        UNIT_ASSERT_VALUES_EQUAL(true, readyQueue.ReadyToFlush.contains(123));
    }

    Y_UNIT_TEST(ShouldRegisterEraseAfterUnlockInFlushedState)
    {
        TTestReadyQueue readyQueue;
        TInflightInfo inflightInfo(
            &readyQueue,
            MakeDDisks(),
            THostMask::MakeEmpty(),
            123,
            4096);
        inflightInfo.OnWritten(MakePrimaryHosts(), MakePrimaryHosts());

        // Lock before flushing completes.
        Y_UNUSED(inflightInfo.RequestFlush(THostIndex{0}));
        Y_UNUSED(inflightInfo.RequestFlush(THostIndex{1}));
        Y_UNUSED(inflightInfo.RequestFlush(THostIndex{2}));
        inflightInfo.LockPBuffer();

        inflightInfo.ConfirmFlush(THostIndex{0});
        inflightInfo.ConfirmFlush(THostIndex{1});
        inflightInfo.ConfirmFlush(THostIndex{2});

        // Flush completed but erase must not be registered while locked.
        UNIT_ASSERT_VALUES_EQUAL(
            TInflightInfo::EState::PBufferFlushed,
            inflightInfo.GetState());
        UNIT_ASSERT_VALUES_EQUAL(false, readyQueue.ReadyToErase.contains(123));

        // Unlocking in the flushed state registers the erase.
        inflightInfo.UnlockPBuffer();
        UNIT_ASSERT_VALUES_EQUAL(true, readyQueue.ReadyToErase.contains(123));

        EraseAll(inflightInfo);
    }

    // UpdateHosts requires every removed host to also be disabled
    // (removed is a subset of disabled). Removing an already-disabled host must
    // not touch the byte counters and, for a written inflight, must not change
    // the state.
    Y_UNIT_TEST(ShouldUpdateHostsRemoveDisabledInWrittenState)
    {
        TTestReadyQueue readyQueue;
        TInflightInfo inflightInfo(
            &readyQueue,
            MakeDDisks(),
            THostMask::MakeEmpty(),
            123,
            4096);
        inflightInfo.OnWritten(MakePrimaryHosts(), MakePrimaryHosts());

        UNIT_ASSERT_VALUES_EQUAL(4096, readyQueue.GetTotalBytes(THostIndex{2}));

        // Remove host 2. It must also be reported as disabled.
        auto removed = THostMask::MakeMask({THostIndex{2}});
        auto disabled = THostMask::MakeMask({THostIndex{2}});
        inflightInfo.UpdateHosts(THostMask::MakeEmpty(), removed, disabled);

        // State stays PBufferWritten; per-host byte accounting is unchanged.
        UNIT_ASSERT_VALUES_EQUAL(
            TInflightInfo::EState::PBufferWritten,
            inflightInfo.GetState());
        UNIT_ASSERT_VALUES_EQUAL(4096, readyQueue.GetTotalBytes(THostIndex{0}));
        UNIT_ASSERT_VALUES_EQUAL(4096, readyQueue.GetTotalBytes(THostIndex{1}));
        UNIT_ASSERT_VALUES_EQUAL(4096, readyQueue.GetTotalBytes(THostIndex{2}));
    }

    // In the flushing state, disabling the not-yet-confirmed host drops it from
    // the effective DDisk set. The remaining confirmed hosts already form a
    // quorum, so the flush completes and the erase gets registered.
    Y_UNIT_TEST(ShouldUpdateHostsCompleteFlushWhenDisablingPendingHost)
    {
        TTestReadyQueue readyQueue;
        // 4 desired DDisks so that after disabling one, 3 remain (== quorum).
        TInflightInfo inflightInfo(
            &readyQueue,
            MakeDDisks(4),
            THostMask::MakeEmpty(),
            123,
            4096);
        inflightInfo.OnWritten(MakePrimaryHosts(4), MakePrimaryHosts(4));

        Y_UNUSED(inflightInfo.RequestFlush(THostIndex{0}));
        Y_UNUSED(inflightInfo.RequestFlush(THostIndex{1}));
        Y_UNUSED(inflightInfo.RequestFlush(THostIndex{2}));
        Y_UNUSED(inflightInfo.RequestFlush(THostIndex{3}));

        // Confirm a quorum (hosts 0, 1, 2) but not host 3.
        inflightInfo.ConfirmFlush(THostIndex{0});
        inflightInfo.ConfirmFlush(THostIndex{1});
        inflightInfo.ConfirmFlush(THostIndex{2});
        UNIT_ASSERT_VALUES_EQUAL(
            TInflightInfo::EState::PBufferFlushing,
            inflightInfo.GetState());
        UNIT_ASSERT_VALUES_EQUAL(false, readyQueue.ReadyToErase.contains(123));

        // Disable + remove host 3. Now DesiredDDisks\Disabled == FlushConfirmed
        // and the quorum holds, so the flush completes.
        auto mask = THostMask::MakeMask({THostIndex{3}});
        inflightInfo.UpdateHosts(THostMask::MakeEmpty(), mask, mask);

        UNIT_ASSERT_VALUES_EQUAL(
            TInflightInfo::EState::PBufferFlushed,
            inflightInfo.GetState());
        UNIT_ASSERT_VALUES_EQUAL(true, readyQueue.ReadyToErase.contains(123));

        // Erase all still-written hosts (host 3 excluded via Disabled).
        inflightInfo.RequestErase(THostIndex{0});
        inflightInfo.RequestErase(THostIndex{1});
        inflightInfo.RequestErase(THostIndex{2});
        Y_UNUSED(inflightInfo.ConfirmErase(THostIndex{0}));
        Y_UNUSED(inflightInfo.ConfirmErase(THostIndex{1}));
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            inflightInfo.ConfirmErase(THostIndex{2}));
    }

    // In the erasing state, disabling the last non-erased host lets the erase
    // complete because EraseConfirmed\Disabled == WriteRequested\Disabled.
    Y_UNIT_TEST(ShouldUpdateHostsCompleteEraseWhenDisablingPendingHost)
    {
        TTestReadyQueue readyQueue;
        TInflightInfo inflightInfo(
            &readyQueue,
            MakeDDisks(),
            THostMask::MakeEmpty(),
            123,
            4096);
        inflightInfo.OnWritten(MakePrimaryHosts(), MakePrimaryHosts());

        FlushAll(inflightInfo);

        // Erase and confirm hosts 0 and 1 only.
        inflightInfo.RequestErase(THostIndex{0});
        inflightInfo.RequestErase(THostIndex{1});
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            inflightInfo.ConfirmErase(THostIndex{0}));
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            inflightInfo.ConfirmErase(THostIndex{1}));
        UNIT_ASSERT_VALUES_EQUAL(
            TInflightInfo::EState::PBufferErasing,
            inflightInfo.GetState());

        // Disable + remove host 2. Erase completes.
        auto mask = THostMask::MakeMask({THostIndex{2}});
        inflightInfo.UpdateHosts(THostMask::MakeEmpty(), mask, mask);

        UNIT_ASSERT_VALUES_EQUAL(
            TInflightInfo::EState::PBufferErased,
            inflightInfo.GetState());
    }

    // Adding a new desired DDisk while flushing must re-register the lsn for
    // flushing so the new host gets its copy.
    Y_UNIT_TEST(ShouldUpdateHostsRegisterFlushWhenAddingDesired)
    {
        TTestReadyQueue readyQueue;
        TInflightInfo inflightInfo(
            &readyQueue,
            MakeDDisks(),
            THostMask::MakeEmpty(),
            123,
            4096);
        inflightInfo.OnWritten(MakePrimaryHosts(), MakePrimaryHosts());

        Y_UNUSED(inflightInfo.RequestFlush(THostIndex{0}));
        Y_UNUSED(inflightInfo.RequestFlush(THostIndex{1}));
        Y_UNUSED(inflightInfo.RequestFlush(THostIndex{2}));
        inflightInfo.ConfirmFlush(THostIndex{0});
        inflightInfo.ConfirmFlush(THostIndex{1});
        inflightInfo.ConfirmFlush(THostIndex{2});

        // Flushed to the quorum, erase registered.
        UNIT_ASSERT_VALUES_EQUAL(
            TInflightInfo::EState::PBufferFlushed,
            inflightInfo.GetState());

        EraseAll(inflightInfo);
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
