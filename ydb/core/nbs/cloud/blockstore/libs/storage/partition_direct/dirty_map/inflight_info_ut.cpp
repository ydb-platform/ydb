#include "dirty_map.h"

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
        TInflightInfo inflightInfo(
            &readyQueue,
            123,
            4096,
            MakePrimaryHosts(),
            MakePrimaryHosts());
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
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            inflightInfo.RequestErase(THostIndex{0}));
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            inflightInfo.RequestErase(THostIndex{1}));
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            inflightInfo.RequestErase(THostIndex{2}));
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            inflightInfo.RequestErase(THostIndex{3}));
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            inflightInfo.RequestErase(THostIndex{4}));

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
            123,
            4096,
            MakePrimaryHosts(),
            MakePrimaryHosts());
        UNIT_ASSERT_VALUES_EQUAL(true, readyQueue.ReadyToFlush.contains(123));

        // Start flushes
        auto l = inflightInfo.RequestFlush(THostIndex{0});
        l = inflightInfo.RequestFlush(THostIndex{1});
        l = inflightInfo.RequestFlush(THostIndex{2});
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
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            inflightInfo.RequestErase(THostIndex{0}));
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            inflightInfo.RequestErase(THostIndex{1}));
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            inflightInfo.RequestErase(THostIndex{2}));
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            inflightInfo.RequestErase(THostIndex{3}));
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            inflightInfo.RequestErase(THostIndex{4}));

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
            123,
            4096,
            MakePrimaryHosts(),
            MakePrimaryHosts());

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
        inflightInfo.FlushFailed(THostRoute{
            .SourceHostIndex = THostIndex{0},
            .DestinationHostIndex = THostIndex{0}});
        UNIT_ASSERT_VALUES_EQUAL(true, readyQueue.ReadyToFlush.contains(123));

        // Restart flush to host 0
        UNIT_ASSERT_VALUES_EQUAL(
            THostIndex{0},
            inflightInfo.RequestFlush(THostIndex{0}));

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
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            inflightInfo.RequestErase(THostIndex{0}));
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            inflightInfo.RequestErase(THostIndex{1}));
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            inflightInfo.RequestErase(THostIndex{2}));

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
            TInflightInfo inflightInfo(
                &readyQueue,
                123,
                4096,
                MakePrimaryHosts(),
                MakePrimaryHosts());

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

    Y_UNIT_TEST(ReadMaskDispatchesByState)
    {
        // White-box regression test for the post-flush read source bug.
        // ReadMask() switches over EState; for PBufferFlushed/Erasing/Erased
        // it MUST return FromDDisk=true with allDDisks as the mask. This test
        // exercises every reachable EState value directly and asserts the
        // (Mask, FromDDisk) pair, so it cannot be silently invalidated by
        // upstream state-machine changes the way an integration-level test
        // can.
        const auto allDDisks = THostMask::MakeAll(5);

        // PBufferIncompleteWrite -> empty mask, PB-side.
        {
            TTestReadyQueue readyQueue;
            TInflightInfo inflight(&readyQueue, 1, 4096, THostIndex{0});
            UNIT_ASSERT(
                inflight.GetState() ==
                TInflightInfo::EState::PBufferIncompleteWrite);

            const auto src = inflight.ReadMask(allDDisks);
            UNIT_ASSERT(src.Mask.Empty());
            UNIT_ASSERT_VALUES_EQUAL(false, src.FromDDisk);
        }

        // PBufferWritten -> WriteConfirmed, PB-side.
        {
            TTestReadyQueue readyQueue;
            TInflightInfo inflight(
                &readyQueue,
                2,
                4096,
                MakePrimaryHosts(),
                MakePrimaryHosts());
            UNIT_ASSERT(
                inflight.GetState() == TInflightInfo::EState::PBufferWritten);

            const auto src = inflight.ReadMask(allDDisks);
            UNIT_ASSERT(src.Mask == MakePrimaryHosts());
            UNIT_ASSERT_VALUES_EQUAL(false, src.FromDDisk);
        }

        // Drive PBufferWritten -> PBufferFlushing -> PBufferFlushed
        // -> PBufferErasing -> PBufferErased and assert ReadMask at every
        // intermediate state.
        {
            TTestReadyQueue readyQueue;
            TInflightInfo inflight(
                &readyQueue,
                3,
                4096,
                MakePrimaryHosts(),
                MakePrimaryHosts());

            // -> PBufferFlushing (RequestFlush moves out of PBufferWritten).
            Y_UNUSED(inflight.RequestFlush(THostIndex{0}));
            Y_UNUSED(inflight.RequestFlush(THostIndex{1}));
            Y_UNUSED(inflight.RequestFlush(THostIndex{2}));
            UNIT_ASSERT(
                inflight.GetState() == TInflightInfo::EState::PBufferFlushing);
            {
                const auto src = inflight.ReadMask(allDDisks);
                UNIT_ASSERT(src.Mask == MakePrimaryHosts());
                UNIT_ASSERT_VALUES_EQUAL(false, src.FromDDisk);
            }

            // -> PBufferFlushed (all FlushDesired confirmed). Quorum >= 3
            // is required by RequestErase, so confirm flushes on all three
            // hosts before driving the erase phase.
            inflight.ConfirmFlush(
                {.SourceHostIndex = THostIndex{0},
                 .DestinationHostIndex = THostIndex{0}});
            inflight.ConfirmFlush(
                {.SourceHostIndex = THostIndex{1},
                 .DestinationHostIndex = THostIndex{1}});
            inflight.ConfirmFlush(
                {.SourceHostIndex = THostIndex{2},
                 .DestinationHostIndex = THostIndex{2}});
            UNIT_ASSERT(
                inflight.GetState() == TInflightInfo::EState::PBufferFlushed);
            {
                // CRITICAL: post-flush reads must be served from DDisk.
                const auto src = inflight.ReadMask(allDDisks);
                UNIT_ASSERT(src.Mask == allDDisks);
                UNIT_ASSERT_VALUES_EQUAL(true, src.FromDDisk);
            }

            // -> PBufferErasing.
            UNIT_ASSERT(inflight.RequestErase(THostIndex{0}));
            UNIT_ASSERT(inflight.RequestErase(THostIndex{1}));
            UNIT_ASSERT(inflight.RequestErase(THostIndex{2}));
            UNIT_ASSERT(
                inflight.GetState() == TInflightInfo::EState::PBufferErasing);
            {
                const auto src = inflight.ReadMask(allDDisks);
                UNIT_ASSERT(src.Mask == allDDisks);
                UNIT_ASSERT_VALUES_EQUAL(true, src.FromDDisk);
            }

            // -> PBufferErased (all EraseRequested confirmed).
            UNIT_ASSERT_VALUES_EQUAL(
                false,
                inflight.ConfirmErase(THostIndex{0}));
            UNIT_ASSERT_VALUES_EQUAL(
                false,
                inflight.ConfirmErase(THostIndex{1}));
            UNIT_ASSERT_VALUES_EQUAL(
                true,
                inflight.ConfirmErase(THostIndex{2}));
            UNIT_ASSERT(
                inflight.GetState() == TInflightInfo::EState::PBufferErased);
            {
                const auto src = inflight.ReadMask(allDDisks);
                UNIT_ASSERT(src.Mask == allDDisks);
                UNIT_ASSERT_VALUES_EQUAL(true, src.FromDDisk);
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
