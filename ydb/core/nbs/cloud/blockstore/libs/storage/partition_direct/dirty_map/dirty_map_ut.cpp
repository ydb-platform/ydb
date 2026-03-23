#include "dirty_map.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDirtyMapTest)
{
    Y_UNIT_TEST(ShouldReadWithoutWrites)
    {
        TBlocksDirtyMap dirtyMap;

        // We should be able to get read hints
        auto readHint =
            dirtyMap.MakeReadHint(TBlockRange64::WithLength(10, 10));
        UNIT_ASSERT_EQUAL(false, readHint.WaitReady.Initialized());
        UNIT_ASSERT_EQUAL(1, readHint.RangeHints.size());
        UNIT_ASSERT_EQUAL(
            TLocationMask::MakePrimaryDDisk(),
            readHint.RangeHints[0].LocationMask);
        UNIT_ASSERT_EQUAL(
            TBlockRange64::WithLength(0, 10),
            readHint.RangeHints[0].RequestRelativeRange);
        UNIT_ASSERT_EQUAL(
            TBlockRange64::WithLength(10, 10),
            readHint.RangeHints[0].VChunkRange);
        UNIT_ASSERT_EQUAL(0, readHint.RangeHints[0].Lsn);
    }

    Y_UNIT_TEST(ShouldReadAfterWriteFinished)
    {
        TBlocksDirtyMap dirtyMap;

        TLocationMask requested = TLocationMask::MakePrimaryPBuffers();
        TLocationMask confirmed = TLocationMask::MakePrimaryPBuffers();

        dirtyMap.WriteFinished(
            123,
            TBlockRange64::WithLength(10, 10),
            requested,
            confirmed);

        // After write, we should be able to get read hints
        auto readHint =
            dirtyMap.MakeReadHint(TBlockRange64::WithLength(10, 10));
        UNIT_ASSERT_EQUAL(false, readHint.WaitReady.Initialized());
        UNIT_ASSERT_EQUAL(1, readHint.RangeHints.size());
        UNIT_ASSERT_EQUAL(
            TLocationMask::MakePrimaryPBuffers(),
            readHint.RangeHints[0].LocationMask);
        UNIT_ASSERT_EQUAL(
            TBlockRange64::WithLength(0, 10),
            readHint.RangeHints[0].RequestRelativeRange);
        UNIT_ASSERT_EQUAL(
            TBlockRange64::WithLength(10, 10),
            readHint.RangeHints[0].VChunkRange);
        UNIT_ASSERT_EQUAL(123, readHint.RangeHints[0].Lsn);
    }

    Y_UNIT_TEST(ShouldWriteAndFlushAndErase)
    {
        TBlocksDirtyMap dirtyMap;

        // Without write, we should not get flush hints
        auto flushHint = dirtyMap.MakeFlushHint(1);
        UNIT_ASSERT_EQUAL(true, flushHint.empty());

        TLocationMask requested = TLocationMask::MakePrimaryPBuffers();
        TLocationMask confirmed = TLocationMask::MakePrimaryPBuffers();

        // Flush commands should be generated after completing the required
        // number of write operations.
        dirtyMap.WriteFinished(
            123,
            TBlockRange64::WithLength(10, 10),
            requested,
            confirmed);

        // WriteFinished should generate one inflight item
        UNIT_ASSERT_VALUES_EQUAL(1, dirtyMap.GetInflightCount());

        flushHint = dirtyMap.MakeFlushHint(2);
        UNIT_ASSERT_EQUAL(true, flushHint.empty());

        dirtyMap.WriteFinished(
            124,
            TBlockRange64::WithLength(20, 10),
            requested,
            confirmed);

        // Second writeFinished should generate one more inflight item
        UNIT_ASSERT_VALUES_EQUAL(2, dirtyMap.GetInflightCount());

        flushHint = dirtyMap.MakeFlushHint(2);
        UNIT_ASSERT_EQUAL(false, flushHint.empty());
        UNIT_ASSERT_VALUES_EQUAL(
            "123[10..19];124[20..29];",
            flushHint[ELocation::PBuffer0].DebugPrint());
        UNIT_ASSERT_VALUES_EQUAL(
            "123[10..19];124[20..29];",
            flushHint[ELocation::PBuffer1].DebugPrint());
        UNIT_ASSERT_VALUES_EQUAL(
            "123[10..19];124[20..29];",
            flushHint[ELocation::PBuffer2].DebugPrint());

        // Erase hints should be generated after completing flushing.
        auto eraseHints = dirtyMap.MakeEraseHint(2);
        UNIT_ASSERT_EQUAL(true, eraseHints.empty());

        // After getting flush hints, we should not get it once again
        {
            auto flushHint = dirtyMap.MakeFlushHint(2);
            UNIT_ASSERT_EQUAL(true, flushHint.empty());
        }

        // After getting flushing errors, we should get flush hints again
        dirtyMap.FlushFinished(ELocation::PBuffer0, {123, 124}, {});
        dirtyMap.FlushFinished(ELocation::PBuffer1, {123, 124}, {});
        dirtyMap.FlushFinished(ELocation::PBuffer2, {}, {123, 124});

        flushHint = dirtyMap.MakeFlushHint(2);
        UNIT_ASSERT_EQUAL(false, flushHint.empty());
        UNIT_ASSERT_VALUES_EQUAL(
            "123[10..19];124[20..29];",
            flushHint[ELocation::PBuffer2].DebugPrint());

        // Complete flushing to third ddisk
        dirtyMap.FlushFinished(ELocation::PBuffer2, {123, 124}, {});

        // Erase hints should be generated after completing the required
        // number of write operations.
        eraseHints = dirtyMap.MakeEraseHint(2);
        UNIT_ASSERT_EQUAL(false, eraseHints.empty());
        UNIT_ASSERT_VALUES_EQUAL(
            "123[10..19];124[20..29];",
            eraseHints[ELocation::PBuffer0].DebugPrint());
        UNIT_ASSERT_VALUES_EQUAL(
            "123[10..19];124[20..29];",
            eraseHints[ELocation::PBuffer1].DebugPrint());
        UNIT_ASSERT_VALUES_EQUAL(
            "123[10..19];124[20..29];",
            eraseHints[ELocation::PBuffer2].DebugPrint());

        // After getting erase hints, we should not get it once again
        {
            auto eraseHint = dirtyMap.MakeEraseHint(2);
            UNIT_ASSERT_EQUAL(true, eraseHint.empty());
        }

        // After getting erasing errors, we should get erase hints again
        dirtyMap.EraseFinished(ELocation::PBuffer0, {123, 124}, {});
        dirtyMap.EraseFinished(ELocation::PBuffer1, {123, 124}, {});
        dirtyMap.EraseFinished(ELocation::PBuffer2, {}, {123, 124});

        eraseHints = dirtyMap.MakeEraseHint(2);
        UNIT_ASSERT_EQUAL(false, eraseHints.empty());
        UNIT_ASSERT_VALUES_EQUAL(
            "123[10..19];124[20..29];",
            eraseHints[ELocation::PBuffer2].DebugPrint());

        // Should still have two inflight items
        UNIT_ASSERT_VALUES_EQUAL(2, dirtyMap.GetInflightCount());

        // Complete erasing from third pbuffer
        dirtyMap.EraseFinished(ELocation::PBuffer2, {123, 124}, {});
        eraseHints = dirtyMap.MakeEraseHint(2);
        UNIT_ASSERT_EQUAL(true, eraseHints.empty());

        // Should remove inflight items
        UNIT_ASSERT_VALUES_EQUAL(0, dirtyMap.GetInflightCount());
    }

    Y_UNIT_TEST(ShouldLockPBuffer)
    {
        TBlocksDirtyMap dirtyMap;

        dirtyMap.WriteFinished(
            123,
            TBlockRange64::WithLength(10, 10),
            TLocationMask::MakePrimaryPBuffers(),
            TLocationMask::MakePrimaryPBuffers());

        dirtyMap.WriteFinished(
            124,
            TBlockRange64::WithLength(10, 10),
            TLocationMask::MakePrimaryPBuffers(),
            TLocationMask::MakePrimaryPBuffers());

        auto flushHint = dirtyMap.MakeFlushHint(1);
        UNIT_ASSERT_EQUAL(false, flushHint.empty());
        for (const auto& [location, flush]: flushHint) {
            dirtyMap.FlushFinished(location, {flush.Segments[0].Lsn}, {});
        }

        // Lock pbuffer
        dirtyMap.LockPBuffer(123);

        // Erase hints should not be generated when PBuffer is locked.
        auto eraseHints = dirtyMap.MakeEraseHint(1);
        UNIT_ASSERT_EQUAL(true, eraseHints.empty());

        // UnLock pbuffer
        dirtyMap.UnlockPBuffer(123);

        // Erase hints should be generated when PBuffer is unlocked.
        eraseHints = dirtyMap.MakeEraseHint(1);
        UNIT_ASSERT_EQUAL(false, eraseHints.empty());
    }

    Y_UNIT_TEST(ShouldLockDDisk)
    {
        TBlocksDirtyMap dirtyMap;

        dirtyMap.WriteFinished(
            123,
            TBlockRange64::WithLength(10, 10),
            TLocationMask::MakePrimaryPBuffers(),
            TLocationMask::MakePrimaryPBuffers());

        // Lock range on DDisk
        auto lockHandle =
            dirtyMap.LockDDiskRange(TBlockRange64::WithLength(5, 10));

        // Flush hints should not be generated when DDisk is locked.
        auto flushHint = dirtyMap.MakeFlushHint(1);
        UNIT_ASSERT_EQUAL(true, flushHint.empty());

        // Lock pbuffer
        dirtyMap.UnLockDDiskRange(lockHandle);

        // FLush hints should be generated when DDisk is unlocked.
        auto eraseHints = dirtyMap.MakeEraseHint(1);
        UNIT_ASSERT_EQUAL(true, eraseHints.empty());
    }

    Y_UNIT_TEST(ShouldRestoreCompletePBuffer)
    {
        TBlocksDirtyMap dirtyMap;

        dirtyMap.RestorePBuffer(
            123,
            TBlockRange64::WithLength(10, 10),
            ELocation::PBuffer0);
        dirtyMap.RestorePBuffer(
            123,
            TBlockRange64::WithLength(10, 10),
            ELocation::PBuffer1);
        dirtyMap.RestorePBuffer(
            123,
            TBlockRange64::WithLength(10, 10),
            ELocation::PBuffer2);

        // Flush hints should be generated when has quorum PBuffers.
        auto flushHint = dirtyMap.MakeFlushHint(1);
        UNIT_ASSERT_EQUAL(false, flushHint.empty());

        UNIT_ASSERT_VALUES_EQUAL(
            "123[10..19];",
            flushHint[ELocation::PBuffer0].DebugPrint());
        UNIT_ASSERT_VALUES_EQUAL(
            "123[10..19];",
            flushHint[ELocation::PBuffer1].DebugPrint());
        UNIT_ASSERT_VALUES_EQUAL(
            "123[10..19];",
            flushHint[ELocation::PBuffer2].DebugPrint());
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
