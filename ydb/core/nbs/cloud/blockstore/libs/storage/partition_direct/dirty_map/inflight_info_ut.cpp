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
        ELocation location,
        EPBufferCounter counter,
        size_t size) override
    {
        PBufferCounters[location][counter] += size;
    }

    void DataFromPBufferReleased(
        ELocation location,
        EPBufferCounter counter,
        size_t size) override
    {
        PBufferCounters[location][counter] -= size;
    }

    size_t GetTotalBytes(ELocation location)
    {
        return PBufferCounters[location][EPBufferCounter::Total];
    }

    THashSet<ui64> ReadyToClone;
    THashSet<ui64> ReadyToFlush;
    THashSet<ui64> ReadyToErase;
    TMap<ELocation, TMap<EPBufferCounter, size_t>> PBufferCounters;
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TInflightInfoTests)
{
    Y_UNIT_TEST(ShouldHandleRestore)
    {
        TTestReadyQueue readyQueue;
        TInflightInfo inflightInfo(&readyQueue, 123, 4096, ELocation::PBuffer0);
        UNIT_ASSERT_VALUES_EQUAL(true, readyQueue.ReadyToClone.contains(123));

        inflightInfo.RestorePBuffer(ELocation::PBuffer1);
        UNIT_ASSERT_VALUES_EQUAL(true, readyQueue.ReadyToClone.contains(123));

        inflightInfo.RestorePBuffer(ELocation::PBuffer2);
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
            TLocationMask::MakePrimaryPBuffers(),
            TLocationMask::MakePrimaryPBuffers());
        UNIT_ASSERT_VALUES_EQUAL(true, readyQueue.ReadyToFlush.contains(123));

        // Start flushes
        UNIT_ASSERT_VALUES_EQUAL(
            ELocation::PBuffer0,
            inflightInfo.RequestFlush(ELocation::DDisk0));
        UNIT_ASSERT_VALUES_EQUAL(
            ELocation::PBuffer1,
            inflightInfo.RequestFlush(ELocation::DDisk1));
        UNIT_ASSERT_VALUES_EQUAL(
            ELocation::PBuffer2,
            inflightInfo.RequestFlush(ELocation::DDisk2));

        // Confirm flushes
        inflightInfo.ConfirmFlush(TRoute{
            .Source = ELocation::PBuffer0,
            .Destination = ELocation::DDisk0});
        UNIT_ASSERT_VALUES_EQUAL(false, readyQueue.ReadyToErase.contains(123));
        inflightInfo.ConfirmFlush(
            {.Source = ELocation::PBuffer1, .Destination = ELocation::DDisk1});
        UNIT_ASSERT_VALUES_EQUAL(false, readyQueue.ReadyToErase.contains(123));
        inflightInfo.ConfirmFlush(
            {.Source = ELocation::PBuffer2, .Destination = ELocation::DDisk2});
        UNIT_ASSERT_VALUES_EQUAL(true, readyQueue.ReadyToErase.contains(123));

        // Check erase requests
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            inflightInfo.RequestErase(ELocation::PBuffer0));
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            inflightInfo.RequestErase(ELocation::PBuffer1));
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            inflightInfo.RequestErase(ELocation::PBuffer2));
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            inflightInfo.RequestErase(ELocation::HOPBuffer0));
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            inflightInfo.RequestErase(ELocation::HOPBuffer1));

        // Confirm erases
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            inflightInfo.ConfirmErase(ELocation::PBuffer0));
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            inflightInfo.ConfirmErase(ELocation::PBuffer1));
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            inflightInfo.ConfirmErase(ELocation::PBuffer2));
    }

    Y_UNIT_TEST(ShouldHandleLock)
    {
        TTestReadyQueue readyQueue;
        TInflightInfo inflightInfo(
            &readyQueue,
            123,
            4096,
            TLocationMask::MakePrimaryPBuffers(),
            TLocationMask::MakePrimaryPBuffers());
        UNIT_ASSERT_VALUES_EQUAL(true, readyQueue.ReadyToFlush.contains(123));

        // Start flushes
        auto l = inflightInfo.RequestFlush(ELocation::DDisk0);
        l = inflightInfo.RequestFlush(ELocation::DDisk1);
        l = inflightInfo.RequestFlush(ELocation::DDisk2);
        Y_UNUSED(l);

        // Confirm two flushes
        inflightInfo.ConfirmFlush(TRoute{
            .Source = ELocation::PBuffer0,
            .Destination = ELocation::DDisk0});
        inflightInfo.ConfirmFlush(
            {.Source = ELocation::PBuffer1, .Destination = ELocation::DDisk1});

        // Check lock/unlock PBuffer
        inflightInfo.LockPBuffer();
        UNIT_ASSERT_VALUES_EQUAL(true, readyQueue.ReadyToErase.empty());

        // Confirm last flush
        inflightInfo.ConfirmFlush(
            {.Source = ELocation::PBuffer2, .Destination = ELocation::DDisk2});
        UNIT_ASSERT_VALUES_EQUAL(true, readyQueue.ReadyToErase.empty());

        inflightInfo.UnlockPBuffer();
        UNIT_ASSERT_VALUES_EQUAL(true, readyQueue.ReadyToErase.contains(123));

        // Check erase requests
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            inflightInfo.RequestErase(ELocation::PBuffer0));
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            inflightInfo.RequestErase(ELocation::PBuffer1));
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            inflightInfo.RequestErase(ELocation::PBuffer2));
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            inflightInfo.RequestErase(ELocation::HOPBuffer0));
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            inflightInfo.RequestErase(ELocation::HOPBuffer1));

        // Confirm erases
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            inflightInfo.ConfirmErase(ELocation::PBuffer0));
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            inflightInfo.ConfirmErase(ELocation::PBuffer1));
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            inflightInfo.ConfirmErase(ELocation::PBuffer2));
    }

    Y_UNIT_TEST(ShouldPutToReadyQueueOnFail)
    {
        TTestReadyQueue readyQueue;
        TInflightInfo inflightInfo(
            &readyQueue,
            123,
            4096,
            TLocationMask::MakePrimaryPBuffers(),
            TLocationMask::MakePrimaryPBuffers());

        // Flush started
        UNIT_ASSERT_VALUES_EQUAL(
            ELocation::PBuffer0,
            inflightInfo.RequestFlush(ELocation::DDisk0));
        UNIT_ASSERT_VALUES_EQUAL(
            ELocation::PBuffer1,
            inflightInfo.RequestFlush(ELocation::DDisk1));
        UNIT_ASSERT_VALUES_EQUAL(
            ELocation::PBuffer2,
            inflightInfo.RequestFlush(ELocation::DDisk2));

        // When a flush fails, the lsn must be queued for a flush again.
        readyQueue.ReadyToFlush.clear();
        inflightInfo.FlushFailed(TRoute{
            .Source = ELocation::PBuffer0,
            .Destination = ELocation::DDisk0});
        UNIT_ASSERT_VALUES_EQUAL(true, readyQueue.ReadyToFlush.contains(123));

        // Restart flush to DDisk0
        UNIT_ASSERT_VALUES_EQUAL(
            ELocation::PBuffer0,
            inflightInfo.RequestFlush(ELocation::DDisk0));

        // Confirm flushes
        inflightInfo.ConfirmFlush(TRoute{
            .Source = ELocation::PBuffer0,
            .Destination = ELocation::DDisk0});
        UNIT_ASSERT_VALUES_EQUAL(false, readyQueue.ReadyToErase.contains(123));
        inflightInfo.ConfirmFlush(TRoute{
            .Source = ELocation::PBuffer1,
            .Destination = ELocation::DDisk1});
        UNIT_ASSERT_VALUES_EQUAL(false, readyQueue.ReadyToErase.contains(123));
        inflightInfo.ConfirmFlush(TRoute{
            .Source = ELocation::PBuffer2,
            .Destination = ELocation::DDisk2});
        UNIT_ASSERT_VALUES_EQUAL(true, readyQueue.ReadyToErase.contains(123));

        // Erase started
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            inflightInfo.RequestErase(ELocation::PBuffer0));
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            inflightInfo.RequestErase(ELocation::PBuffer1));
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            inflightInfo.RequestErase(ELocation::PBuffer2));

        // When a erase fails, the lsn must be queued for a erase again.
        readyQueue.ReadyToErase.clear();
        inflightInfo.EraseFailed(ELocation::PBuffer0);
        UNIT_ASSERT_VALUES_EQUAL(true, readyQueue.ReadyToErase.contains(123));
    }

    Y_UNIT_TEST(ShouldCountTotalBytesForRestore)
    {
        TTestReadyQueue readyQueue;
        {
            TInflightInfo inflightInfo(
                &readyQueue,
                123,
                4096,
                ELocation::PBuffer0);

            inflightInfo.RestorePBuffer(ELocation::PBuffer1);
            inflightInfo.RestorePBuffer(ELocation::PBuffer2);

            UNIT_ASSERT_VALUES_EQUAL(
                4096,
                readyQueue.GetTotalBytes(ELocation::PBuffer0));
            UNIT_ASSERT_VALUES_EQUAL(
                4096,
                readyQueue.GetTotalBytes(ELocation::PBuffer1));
            UNIT_ASSERT_VALUES_EQUAL(
                4096,
                readyQueue.GetTotalBytes(ELocation::PBuffer2));
        }
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            readyQueue.GetTotalBytes(ELocation::PBuffer0));
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            readyQueue.GetTotalBytes(ELocation::PBuffer1));
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            readyQueue.GetTotalBytes(ELocation::PBuffer2));
    }

    Y_UNIT_TEST(ShouldCountTotalBytesForConfirmedWrite)
    {
        TTestReadyQueue readyQueue;
        {
            TInflightInfo inflightInfo(
                &readyQueue,
                123,
                4096,
                TLocationMask::MakePrimaryPBuffers(),
                TLocationMask::MakePrimaryPBuffers());

            UNIT_ASSERT_VALUES_EQUAL(
                4096,
                readyQueue.GetTotalBytes(ELocation::PBuffer0));
            UNIT_ASSERT_VALUES_EQUAL(
                4096,
                readyQueue.GetTotalBytes(ELocation::PBuffer1));
            UNIT_ASSERT_VALUES_EQUAL(
                4096,
                readyQueue.GetTotalBytes(ELocation::PBuffer2));
        }
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            readyQueue.GetTotalBytes(ELocation::PBuffer0));
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            readyQueue.GetTotalBytes(ELocation::PBuffer1));
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            readyQueue.GetTotalBytes(ELocation::PBuffer2));
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
