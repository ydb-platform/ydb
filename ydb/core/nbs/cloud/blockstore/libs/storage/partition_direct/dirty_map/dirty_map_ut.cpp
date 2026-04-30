#include "dirty_map.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

namespace {

////////////////////////////////////////////////////////////////////////////////

TVector<ui64> GetLsns(const TVector<TPBufferSegment>& segments)
{
    TVector<ui64> lsns;
    for (const auto& segment: segments) {
        lsns.push_back(segment.Lsn);
    }
    return lsns;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDirtyMapTest)
{
    Y_UNIT_TEST(ShouldReadWithoutWrites)
    {
        TBlocksDirtyMap dirtyMap(
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        // We should be able to get read hints
        auto readHint =
            dirtyMap.MakeReadHint(TBlockRange64::WithLength(10, 10));
        UNIT_ASSERT_VALUES_EQUAL(
            "0{[D+++..P.....][10..19][0..9]};",
            readHint.DebugPrint());

        // Disable DDisk0 and enable Hand-off-0
        auto desired = TLocationMask::Make(false, true, true, true, false);
        auto disabled = TLocationMask::Make(true, false, false, false, false);
        dirtyMap.UpdateConfig(desired, disabled);
        readHint = dirtyMap.MakeReadHint(TBlockRange64::WithLength(10, 10));
        UNIT_ASSERT_VALUES_EQUAL(
            "0{[D.++*.P.....][10..19][0..9]};",
            readHint.DebugPrint());
    }

    Y_UNIT_TEST(ShouldNotReadFromFresh)
    {
        TBlocksDirtyMap dirtyMap(
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        dirtyMap.MarkFresh(ELocation::DDisk0, 30 * DefaultBlockSize);
        dirtyMap.MarkFresh(ELocation::DDisk2, 40 * DefaultBlockSize);

        UNIT_ASSERT_VALUES_EQUAL(
            "DDisk0{Fresh,30,30};"
            "DDisk1{Operational,32768,32768};"
            "DDisk2{Fresh,40,40};"
            "HODDisk0{Operational,32768,32768};"
            "HODDisk1{Operational,32768,32768};",
            dirtyMap.DebugPrintDDiskState());

        // Read below fresh watermark
        auto readHint =
            dirtyMap.MakeReadHint(TBlockRange64::WithLength(10, 10));
        UNIT_ASSERT_VALUES_EQUAL(
            "0{[D+++..P.....][10..19][0..9]};",
            readHint.DebugPrint());

        // Read crossed fresh watermark
        readHint = dirtyMap.MakeReadHint(TBlockRange64::WithLength(25, 10));
        UNIT_ASSERT_VALUES_EQUAL(
            "0{[D.++..P.....][25..34][0..9]};",
            readHint.DebugPrint());

        // Read above fresh watermark
        readHint = dirtyMap.MakeReadHint(TBlockRange64::WithLength(30, 10));
        UNIT_ASSERT_VALUES_EQUAL(
            "0{[D.++..P.....][30..39][0..9]};",
            readHint.DebugPrint());

        // Read above fresh watermark
        readHint = dirtyMap.MakeReadHint(TBlockRange64::WithLength(40, 10));
        UNIT_ASSERT_VALUES_EQUAL(
            "0{[D.+...P.....][40..49][0..9]};",
            readHint.DebugPrint());
    }

    Y_UNIT_TEST(ShouldReadAfterWriteFinished)
    {
        TBlocksDirtyMap dirtyMap(
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        dirtyMap.WriteFinished(
            123,
            TBlockRange64::WithLength(10, 10),
            TLocationMask::MakePrimaryPBuffers(),
            TLocationMask::MakePrimaryPBuffers());

        // After write, we should be able to get read hints
        auto readHint =
            dirtyMap.MakeReadHint(TBlockRange64::WithLength(10, 10));
        UNIT_ASSERT_VALUES_EQUAL(
            "123{[D.....P+++..][10..19][0..9]};",
            readHint.DebugPrint());

        // Disable DDisk0 and enable Hand-off-0
        auto desired = TLocationMask::Make(false, true, true, true, false);
        auto disabled = TLocationMask::Make(true, false, false, false, false);
        dirtyMap.UpdateConfig(desired, disabled);

        readHint = dirtyMap.MakeReadHint(TBlockRange64::WithLength(10, 10));
        UNIT_ASSERT_VALUES_EQUAL(
            "123{[D.....P.++..][10..19][0..9]};",
            readHint.DebugPrint());

        // Counters on primary PBuffers contains one record with 40960 bytes
        for (auto location: TLocationMask::MakePrimaryPBuffers()) {
            auto counters = dirtyMap.GetPBufferCounters(location);
            UNIT_ASSERT_VALUES_EQUAL(1, counters.CurrentRecordsCount);
            UNIT_ASSERT_VALUES_EQUAL(40960, counters.CurrentBytesCount);
            UNIT_ASSERT_VALUES_EQUAL(1, counters.TotalRecordsCount);
            UNIT_ASSERT_VALUES_EQUAL(40960, counters.TotalBytesCount);

            UNIT_ASSERT_VALUES_EQUAL(0, counters.CurrentLockedRecordsCount);
            UNIT_ASSERT_VALUES_EQUAL(0, counters.CurrentLockedBytesCount);
            UNIT_ASSERT_VALUES_EQUAL(0, counters.TotalLockedRecordsCount);
            UNIT_ASSERT_VALUES_EQUAL(0, counters.TotalLockedBytesCount);
        }
    }

    Y_UNIT_TEST(ShouldReadAfterWriteFinishedFromLastLsn)
    {
        TBlocksDirtyMap dirtyMap(
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        dirtyMap.WriteFinished(
            123,
            TBlockRange64::WithLength(10, 10),
            TLocationMask::MakePrimaryPBuffers(),
            TLocationMask::MakePrimaryPBuffers());

        dirtyMap.WriteFinished(
            124,
            TBlockRange64::WithLength(10, 10),
            TLocationMask::MakePBuffer(true, true, false, true, false),
            TLocationMask::MakePBuffer(true, true, false, true, false));

        // After write, we should be able to get read hints
        auto readHint =
            dirtyMap.MakeReadHint(TBlockRange64::WithLength(10, 10));
        UNIT_ASSERT_VALUES_EQUAL(
            "124{[D.....P++.*.][10..19][0..9]};",
            readHint.DebugPrint());

        // Disable DDisk0 and enable Hand-off-0
        auto desired = TLocationMask::Make(false, true, true, true, false);
        auto disabled = TLocationMask::Make(true, false, false, false, false);
        dirtyMap.UpdateConfig(desired, disabled);

        readHint = dirtyMap.MakeReadHint(TBlockRange64::WithLength(10, 10));
        UNIT_ASSERT_VALUES_EQUAL(
            "124{[D.....P.+.*.][10..19][0..9]};",
            readHint.DebugPrint());

        readHint.RangeHints[0].Lock.Arm();

        {
            // PBuffer0 contains two records, one locked for read
            auto counters = dirtyMap.GetPBufferCounters(ELocation::PBuffer0);
            UNIT_ASSERT_VALUES_EQUAL(2, counters.CurrentRecordsCount);
            UNIT_ASSERT_VALUES_EQUAL(81920, counters.CurrentBytesCount);
            UNIT_ASSERT_VALUES_EQUAL(2, counters.TotalRecordsCount);
            UNIT_ASSERT_VALUES_EQUAL(81920, counters.TotalBytesCount);

            UNIT_ASSERT_VALUES_EQUAL(1, counters.CurrentLockedRecordsCount);
            UNIT_ASSERT_VALUES_EQUAL(40960, counters.CurrentLockedBytesCount);
            UNIT_ASSERT_VALUES_EQUAL(1, counters.TotalLockedRecordsCount);
            UNIT_ASSERT_VALUES_EQUAL(40960, counters.TotalLockedBytesCount);
        }
        {
            // HOPBuffer0 contains one records, one locked for read
            auto counters = dirtyMap.GetPBufferCounters(ELocation::HOPBuffer0);
            UNIT_ASSERT_VALUES_EQUAL(1, counters.CurrentRecordsCount);
            UNIT_ASSERT_VALUES_EQUAL(40960, counters.CurrentBytesCount);
            UNIT_ASSERT_VALUES_EQUAL(1, counters.TotalRecordsCount);
            UNIT_ASSERT_VALUES_EQUAL(40960, counters.TotalBytesCount);

            UNIT_ASSERT_VALUES_EQUAL(1, counters.CurrentLockedRecordsCount);
            UNIT_ASSERT_VALUES_EQUAL(40960, counters.CurrentLockedBytesCount);
            UNIT_ASSERT_VALUES_EQUAL(1, counters.TotalLockedRecordsCount);
            UNIT_ASSERT_VALUES_EQUAL(40960, counters.TotalLockedBytesCount);
        }
    }

    Y_UNIT_TEST(ShouldWriteAndFlushAndErase)
    {
        TBlocksDirtyMap dirtyMap(
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        // Without write, we should not get flush hints
        auto flushHint = dirtyMap.MakeFlushHint(1);
        UNIT_ASSERT_EQUAL(true, flushHint.Empty());

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
        UNIT_ASSERT_EQUAL(true, flushHint.Empty());

        dirtyMap.WriteFinished(
            124,
            TBlockRange64::WithLength(20, 10),
            requested,
            confirmed);

        // Second writeFinished should generate one more inflight item
        UNIT_ASSERT_VALUES_EQUAL(2, dirtyMap.GetInflightCount());

        flushHint = dirtyMap.MakeFlushHint(2);
        UNIT_ASSERT_VALUES_EQUAL(
            "PBuffer0->DDisk0:123[10..19],124[20..29];"
            "PBuffer1->DDisk1:123[10..19],124[20..29];"
            "PBuffer2->DDisk2:123[10..19],124[20..29];",
            flushHint.DebugPrint());
        // Erase hints should be generated after completing flushing.
        auto eraseHints = dirtyMap.MakeEraseHint(2);
        UNIT_ASSERT_EQUAL(true, eraseHints.Empty());

        // After getting flush hints, we should not get it once again
        {
            auto flushHint = dirtyMap.MakeFlushHint(2);
            UNIT_ASSERT_EQUAL(true, flushHint.Empty());
        }

        // After getting flushing errors, we should get flush hints again
        dirtyMap.FlushFinished(
            TRoute{
                .Source = ELocation::PBuffer0,
                .Destination = ELocation::DDisk0},
            {123, 124},
            {});
        dirtyMap.FlushFinished(
            TRoute{
                .Source = ELocation::PBuffer1,
                .Destination = ELocation::DDisk1},
            {123, 124},
            {});
        dirtyMap.FlushFinished(
            TRoute{
                .Source = ELocation::PBuffer2,
                .Destination = ELocation::DDisk2},
            {},
            {123, 124});

        flushHint = dirtyMap.MakeFlushHint(2);
        UNIT_ASSERT_EQUAL(false, flushHint.Empty());
        UNIT_ASSERT_VALUES_EQUAL(
            "PBuffer2->DDisk2:123[10..19],124[20..29];",
            flushHint.DebugPrint());

        // Complete flushing to third ddisk
        dirtyMap.FlushFinished(
            TRoute{
                .Source = ELocation::PBuffer2,
                .Destination = ELocation::DDisk2},
            {123, 124},
            {});

        // Erase hints should be generated after completing the required
        // number of write operations.
        eraseHints = dirtyMap.MakeEraseHint(2);
        UNIT_ASSERT_VALUES_EQUAL(
            "PBuffer0:123[10..19],124[20..29];"
            "PBuffer1:123[10..19],124[20..29];"
            "PBuffer2:123[10..19],124[20..29];",
            eraseHints.DebugPrint());

        // After getting erase hints, we should not get it once again
        {
            auto eraseHint = dirtyMap.MakeEraseHint(2);
            UNIT_ASSERT_EQUAL(true, eraseHint.Empty());
        }

        // After getting erasing errors, we should get erase hints again
        dirtyMap.EraseFinished(ELocation::PBuffer0, {123, 124}, {});
        dirtyMap.EraseFinished(ELocation::PBuffer1, {123, 124}, {});
        dirtyMap.EraseFinished(ELocation::PBuffer2, {}, {123, 124});

        eraseHints = dirtyMap.MakeEraseHint(2);
        UNIT_ASSERT_VALUES_EQUAL(
            "PBuffer2:123[10..19],124[20..29];",
            eraseHints.DebugPrint());

        // Should still have two inflight items
        UNIT_ASSERT_VALUES_EQUAL(2, dirtyMap.GetInflightCount());

        // Complete erasing from third pbuffer
        dirtyMap.EraseFinished(ELocation::PBuffer2, {123, 124}, {});
        eraseHints = dirtyMap.MakeEraseHint(2);
        UNIT_ASSERT_EQUAL(true, eraseHints.Empty());

        // Should remove inflight items
        UNIT_ASSERT_VALUES_EQUAL(0, dirtyMap.GetInflightCount());

        // All current counters back to zero.
        for (auto location: TLocationMask::MakePrimaryPBuffers()) {
            auto counters = dirtyMap.GetPBufferCounters(location);
            UNIT_ASSERT_VALUES_EQUAL(0, counters.CurrentRecordsCount);
            UNIT_ASSERT_VALUES_EQUAL(0, counters.CurrentBytesCount);
            UNIT_ASSERT_VALUES_EQUAL(2, counters.TotalRecordsCount);
            UNIT_ASSERT_VALUES_EQUAL(81920, counters.TotalBytesCount);
        }
    }

    Y_UNIT_TEST(ShouldWriteAndFlushAndEraseWhenAdditionalHandOffDesired)
    {
        TBlocksDirtyMap dirtyMap(
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        // Enable additional Hand-off
        auto desired = TLocationMask::Make(true, true, true, true, false);
        dirtyMap.UpdateConfig(desired, TLocationMask::MakeEmpty());

        // Written to 2 primary and 1 hand-off
        TLocationMask requested =
            TLocationMask::MakePBuffer(false, true, true, true, false);
        TLocationMask confirmed = requested;

        dirtyMap.WriteFinished(
            123,
            TBlockRange64::WithLength(10, 10),
            requested,
            confirmed);

        auto flushHint = dirtyMap.MakeFlushHint(1);
        UNIT_ASSERT_VALUES_EQUAL(
            "PBuffer1->DDisk0:123[10..19];"   // Cross-node
            "PBuffer1->DDisk1:123[10..19];"
            "PBuffer2->DDisk2:123[10..19];"
            "HOPBuffer0->HODDisk0:123[10..19];",
            flushHint.DebugPrint());

        // Finish flushes
        for (const auto& [route, hint]: flushHint.GetAllHints()) {
            dirtyMap.FlushFinished(route, GetLsns(hint.Segments), {});
        }

        // Erase hints
        auto eraseHints = dirtyMap.MakeEraseHint(1);
        UNIT_ASSERT_VALUES_EQUAL(
            "PBuffer1:123[10..19];"
            "PBuffer2:123[10..19];"
            "HOPBuffer0:123[10..19];",
            eraseHints.DebugPrint());

        // Finish erasing
        for (const auto& [location, hint]: eraseHints.GetAllHints()) {
            dirtyMap.EraseFinished(location, GetLsns(hint.Segments), {});
        }
    }

    Y_UNIT_TEST(ShouldWriteAndFlushAndEraseWithOneDisabled)
    {
        TBlocksDirtyMap dirtyMap(
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        // Enable Hand-off-0 instead of DDisk0
        auto desired = TLocationMask::Make(false, true, true, true, false);
        auto disabled = TLocationMask::Make(true, false, false, false, false);
        dirtyMap.UpdateConfig(desired, disabled);

        // Written to two primary and one hand-off
        TLocationMask requested = desired.PBuffers();
        TLocationMask confirmed = desired.PBuffers();

        dirtyMap.WriteFinished(
            123,
            TBlockRange64::WithLength(10, 10),
            requested,
            confirmed);

        auto flushHint = dirtyMap.MakeFlushHint(1);
        UNIT_ASSERT_VALUES_EQUAL(
            "PBuffer1->DDisk1:123[10..19];"
            "PBuffer2->DDisk2:123[10..19];"
            "HOPBuffer0->HODDisk0:123[10..19];",
            flushHint.DebugPrint());

        // Finish flushes
        for (const auto& [route, hint]: flushHint.GetAllHints()) {
            dirtyMap.FlushFinished(route, GetLsns(hint.Segments), {});
        }

        // Erase hints
        auto eraseHints = dirtyMap.MakeEraseHint(1);
        UNIT_ASSERT_VALUES_EQUAL(
            "PBuffer1:123[10..19];"
            "PBuffer2:123[10..19];"
            "HOPBuffer0:123[10..19];",
            eraseHints.DebugPrint());

        // Finish erasing
        for (const auto& [location, hint]: eraseHints.GetAllHints()) {
            dirtyMap.EraseFinished(location, GetLsns(hint.Segments), {});
        }
    }

    Y_UNIT_TEST(ShouldWriteAndFlushAndEraseWithTwoDisabled)
    {
        TBlocksDirtyMap dirtyMap(
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        // Enable Hand-off-0 instead of DDisk0
        auto desired = TLocationMask::Make(false, false, true, true, true);
        auto disabled = TLocationMask::Make(true, true, false, false, false);
        dirtyMap.UpdateConfig(desired, disabled);

        // Written to two primary and one hand-off
        TLocationMask requested = desired.PBuffers();
        TLocationMask confirmed = desired.PBuffers();

        dirtyMap.WriteFinished(
            123,
            TBlockRange64::WithLength(10, 10),
            requested,
            confirmed);

        auto flushHint = dirtyMap.MakeFlushHint(1);
        UNIT_ASSERT_VALUES_EQUAL(
            "PBuffer2->DDisk2:123[10..19];"
            "HOPBuffer0->HODDisk0:123[10..19];"
            "HOPBuffer1->HODDisk1:123[10..19];",
            flushHint.DebugPrint());

        // Finish flushes
        for (const auto& [route, hint]: flushHint.GetAllHints()) {
            dirtyMap.FlushFinished(route, GetLsns(hint.Segments), {});
        }

        // Erase hints
        auto eraseHints = dirtyMap.MakeEraseHint(1);
        UNIT_ASSERT_VALUES_EQUAL(
            "PBuffer2:123[10..19];"
            "HOPBuffer0:123[10..19];"
            "HOPBuffer1:123[10..19];",
            eraseHints.DebugPrint());

        // Finish erasing
        for (const auto& [location, hint]: eraseHints.GetAllHints()) {
            dirtyMap.EraseFinished(location, GetLsns(hint.Segments), {});
        }
    }

    Y_UNIT_TEST(ShouldNotFlushAndEraseFromDisabled)
    {
        TBlocksDirtyMap dirtyMap(
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        // Enable Hand-off-0
        // Disable DDisk0
        auto desired = TLocationMask::Make(false, true, true, true, false);
        auto disabled = TLocationMask::Make(true, false, false, false, false);
        dirtyMap.UpdateConfig(desired, disabled);

        // Written to all primary
        TLocationMask requested =
            TLocationMask::Make(true, true, true, false, false);
        TLocationMask confirmed = requested;

        dirtyMap.WriteFinished(
            123,
            TBlockRange64::WithLength(10, 10),
            requested.LogicalAnd(TLocationMask::MakeAllPBuffers()),
            confirmed.LogicalAnd(TLocationMask::MakeAllPBuffers()));

        auto flushHint = dirtyMap.MakeFlushHint(1);
        UNIT_ASSERT_VALUES_EQUAL(
            "PBuffer0->HODDisk0:123[10..19];"
            "PBuffer1->DDisk1:123[10..19];"
            "PBuffer2->DDisk2:123[10..19];",
            flushHint.DebugPrint());

        // Finish flushes
        for (const auto& [route, hint]: flushHint.GetAllHints()) {
            dirtyMap.FlushFinished(route, GetLsns(hint.Segments), {});
        }

        // Erase hints
        auto eraseHints = dirtyMap.MakeEraseHint(1);
        UNIT_ASSERT_VALUES_EQUAL(
            "PBuffer1:123[10..19];"
            "PBuffer2:123[10..19];",
            eraseHints.DebugPrint());

        // Finish erasing
        for (const auto& [location, hint]: eraseHints.GetAllHints()) {
            dirtyMap.EraseFinished(location, GetLsns(hint.Segments), {});
        }

        // Should remove inflight items
        UNIT_ASSERT_VALUES_EQUAL(0, dirtyMap.GetInflightCount());
    }

    Y_UNIT_TEST(ShouldNotFlushOverWriteWatermark)
    {
        TBlocksDirtyMap dirtyMap(
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);
        // Enable 4 DDisks. Available DDisks is enough for a quorum.
        auto desired = TLocationMask::Make(true, true, true, true, false);
        dirtyMap.UpdateConfig(desired, {});

        dirtyMap.SetFlushWatermark(ELocation::DDisk2, 100 * DefaultBlockSize);

        TLocationMask requested =
            TLocationMask::MakePBuffer(true, true, true, false, false);
        TLocationMask confirmed = requested;

        // Range below write watermark. Should be flushed to 4 ddisks.
        dirtyMap.WriteFinished(
            123,
            TBlockRange64::WithLength(10, 10),
            requested,
            confirmed);
        // Range cross write watermark. Should be flushed to 4 ddisks.
        dirtyMap.WriteFinished(
            124,
            TBlockRange64::WithLength(95, 10),
            requested,
            confirmed);
        // Range over write watermark. Should be flushed to 3 ddisks.
        dirtyMap.WriteFinished(
            125,
            TBlockRange64::WithLength(100, 10),
            requested,
            confirmed);

        auto flushHint = dirtyMap.MakeFlushHint(3);
        UNIT_ASSERT_VALUES_EQUAL(
            "PBuffer0->DDisk0:123[10..19],124[95..104],125[100..109];"
            "PBuffer0->HODDisk0:123[10..19],124[95..104],125[100..109];"
            "PBuffer1->DDisk1:123[10..19],124[95..104],125[100..109];"
            "PBuffer2->DDisk2:123[10..19],124[95..104];",
            flushHint.DebugPrint());
    }

    Y_UNIT_TEST(ShouldBlockFlushOverWriteWatermark)
    {
        TBlocksDirtyMap dirtyMap(
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        // Only 3 DDisks available by default. For some requests, ddisk will not
        // be sufficient for quorum.
        dirtyMap.SetFlushWatermark(ELocation::DDisk2, 100 * DefaultBlockSize);

        TLocationMask requested =
            TLocationMask::MakePBuffer(true, true, true, false, false);
        TLocationMask confirmed = requested;

        // Range below write watermark. Should be flushed.
        dirtyMap.WriteFinished(
            123,
            TBlockRange64::WithLength(10, 10),
            requested,
            confirmed);
        // Range cross write watermark. Should be flushed.
        dirtyMap.WriteFinished(
            124,
            TBlockRange64::WithLength(95, 10),
            requested,
            confirmed);
        // Range over write watermark. Should not be flushed.
        dirtyMap.WriteFinished(
            125,
            TBlockRange64::WithLength(100, 10),
            requested,
            confirmed);

        auto flushHint = dirtyMap.MakeFlushHint(3);
        UNIT_ASSERT_VALUES_EQUAL(
            "PBuffer0->DDisk0:123[10..19],124[95..104];"
            "PBuffer1->DDisk1:123[10..19],124[95..104];"
            "PBuffer2->DDisk2:123[10..19],124[95..104];",
            flushHint.DebugPrint());
    }

    Y_UNIT_TEST(ShouldLockPBuffer)
    {
        TBlocksDirtyMap dirtyMap(
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        dirtyMap.WriteFinished(
            123,
            TBlockRange64::WithLength(10, 10),
            TLocationMask::MakePrimaryPBuffers(),
            TLocationMask::MakePrimaryPBuffers());

        auto flushHint = dirtyMap.MakeFlushHint(1);
        UNIT_ASSERT_EQUAL(false, flushHint.Empty());
        for (const auto& [route, flush]: flushHint.GetAllHints()) {
            dirtyMap.FlushFinished(route, {GetLsns(flush.Segments)}, {});
        }

        // Lock pbuffer
        dirtyMap.LockPBuffer(123);

        // Erase hints should not be generated when PBuffer is locked.
        auto eraseHints = dirtyMap.MakeEraseHint(1);
        UNIT_ASSERT_EQUAL(true, eraseHints.Empty());

        // UnLock pbuffer
        dirtyMap.UnlockPBuffer(123);

        // Erase hints should be generated when PBuffer is unlocked.
        eraseHints = dirtyMap.MakeEraseHint(1);
        UNIT_ASSERT_EQUAL(false, eraseHints.Empty());
    }

    Y_UNIT_TEST(ShouldLockDDisk)
    {
        TBlocksDirtyMap dirtyMap(
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);
        TLocationMask mask = TLocationMask ::MakePrimaryDDisks();

        dirtyMap.WriteFinished(
            123,
            TBlockRange64::WithLength(10, 10),
            TLocationMask::MakePrimaryPBuffers(),
            TLocationMask::MakePrimaryPBuffers());

        // Lock range on DDisk
        auto lockHandle =
            dirtyMap.LockDDiskRange(TBlockRange64::WithLength(5, 10), mask);

        // Flush hints should not be generated when DDisk is locked.
        auto flushHint = dirtyMap.MakeFlushHint(1);
        UNIT_ASSERT_EQUAL(true, flushHint.Empty());

        // Lock pbuffer
        dirtyMap.UnLockDDiskRange(lockHandle);

        // FLush hints should be generated when DDisk is unlocked.
        auto eraseHints = dirtyMap.MakeEraseHint(1);
        UNIT_ASSERT_EQUAL(true, eraseHints.Empty());
    }

    Y_UNIT_TEST(ShouldRestoreCompletePBuffer)
    {
        TBlocksDirtyMap dirtyMap(
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

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
        UNIT_ASSERT_EQUAL(false, flushHint.Empty());

        UNIT_ASSERT_VALUES_EQUAL(
            "PBuffer0->DDisk0:123[10..19];"
            "PBuffer1->DDisk1:123[10..19];"
            "PBuffer2->DDisk2:123[10..19];",
            flushHint.DebugPrint());
    }

    Y_UNIT_TEST(ShouldRestoreOverCompletePBuffer)
    {
        TBlocksDirtyMap dirtyMap(
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        // Block written to four PBuffers
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
        dirtyMap.RestorePBuffer(
            123,
            TBlockRange64::WithLength(10, 10),
            ELocation::HOPBuffer0);

        // Flush hints should be generated when has quorum PBuffers.
        auto flushHint = dirtyMap.MakeFlushHint(1);
        UNIT_ASSERT_EQUAL(false, flushHint.Empty());

        UNIT_ASSERT_VALUES_EQUAL(
            "PBuffer0->DDisk0:123[10..19];"
            "PBuffer1->DDisk1:123[10..19];"
            "PBuffer2->DDisk2:123[10..19];",
            flushHint.DebugPrint());

        auto readHint =
            dirtyMap.MakeReadHint(TBlockRange64::WithLength(10, 10));
        UNIT_ASSERT_VALUES_EQUAL(
            "123{[D.....P+++*.][10..19][0..9]};",
            readHint.DebugPrint());

        for (auto location:
             {ELocation::PBuffer0,
              ELocation::PBuffer1,
              ELocation::PBuffer2,
              ELocation::HOPBuffer0})
        {
            auto counters = dirtyMap.GetPBufferCounters(location);
            UNIT_ASSERT_VALUES_EQUAL(1, counters.CurrentRecordsCount);
            UNIT_ASSERT_VALUES_EQUAL(40960, counters.CurrentBytesCount);
            UNIT_ASSERT_VALUES_EQUAL(1, counters.TotalRecordsCount);
            UNIT_ASSERT_VALUES_EQUAL(40960, counters.TotalBytesCount);
        }
    }

    Y_UNIT_TEST(ShouldFlushFromHandOff)
    {
        TBlocksDirtyMap dirtyMap(
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        // Block written to two primary PBuffers and one hand-off PBuffer
        dirtyMap.RestorePBuffer(
            123,
            TBlockRange64::WithLength(10, 10),
            ELocation::PBuffer1);
        dirtyMap.RestorePBuffer(
            123,
            TBlockRange64::WithLength(10, 10),
            ELocation::PBuffer2);
        dirtyMap.RestorePBuffer(
            123,
            TBlockRange64::WithLength(10, 10),
            ELocation::HOPBuffer0);

        // Flush hints should be generated when has quorum PBuffers.
        auto flushHint = dirtyMap.MakeFlushHint(1);
        UNIT_ASSERT_EQUAL(false, flushHint.Empty());

        UNIT_ASSERT_VALUES_EQUAL(
            "PBuffer1->DDisk0:123[10..19];"
            "PBuffer1->DDisk1:123[10..19];"
            "PBuffer2->DDisk2:123[10..19];",
            flushHint.DebugPrint());

        auto readHint =
            dirtyMap.MakeReadHint(TBlockRange64::WithLength(10, 10));
        UNIT_ASSERT_VALUES_EQUAL(
            "123{[D.....P.++*.][10..19][0..9]};",
            readHint.DebugPrint());
    }

    Y_UNIT_TEST(ShouldReadFromDDiskIfRangeIsNotCoveredByInflightRange)
    {
        TBlocksDirtyMap dirtyMap(
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        dirtyMap.WriteFinished(
            123,
            TBlockRange64::WithLength(0, 100),
            TLocationMask::MakePrimaryPBuffers(),
            TLocationMask::MakePrimaryPBuffers());

        auto flushHint = dirtyMap.MakeFlushHint(1);
        UNIT_ASSERT_VALUES_EQUAL(
            "PBuffer0->DDisk0:123[0..99];"
            "PBuffer1->DDisk1:123[0..99];"
            "PBuffer2->DDisk2:123[0..99];",
            flushHint.DebugPrint());

        dirtyMap.FlushFinished(
            TRoute{
                .Source = ELocation::PBuffer0,
                .Destination = ELocation::DDisk0},
            {123},
            {});
        dirtyMap.FlushFinished(
            TRoute{
                .Source = ELocation::PBuffer1,
                .Destination = ELocation::DDisk1},
            {123},
            {});
        dirtyMap.FlushFinished(
            TRoute{
                .Source = ELocation::PBuffer2,
                .Destination = ELocation::DDisk2},
            {123},
            {});

        auto eraseHints = dirtyMap.MakeEraseHint(1);
        UNIT_ASSERT_VALUES_EQUAL(
            "PBuffer0:123[0..99];"
            "PBuffer1:123[0..99];"
            "PBuffer2:123[0..99];",
            eraseHints.DebugPrint());

        dirtyMap.EraseFinished(ELocation::PBuffer0, {123}, {});

        dirtyMap.WriteFinished(
            124,
            TBlockRange64::WithLength(10, 10),
            TLocationMask::MakePrimaryPBuffers(),
            TLocationMask::MakePrimaryPBuffers());

        auto readHint =
            dirtyMap.MakeReadHint(TBlockRange64::WithLength(0, 100));
        UNIT_ASSERT_VALUES_EQUAL(
            "0{[D+++..P.....][0..9][0..9]};"
            "124{[D.....P+++..][10..19][10..19]};"
            "0{[D+++..P.....][20..99][20..99]};",
            readHint.DebugPrint());
    }

    Y_UNIT_TEST(ReadShouldWaitPBufferRestore)
    {
        TBlocksDirtyMap dirtyMap(
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        dirtyMap.RestorePBuffer(
            123,
            TBlockRange64::WithLength(10, 10),
            ELocation::PBuffer0);
        auto readHint1 =
            dirtyMap.MakeReadHint(TBlockRange64::WithLength(10, 10));
        UNIT_ASSERT_VALUES_EQUAL("WaitReady:NotReady", readHint1.DebugPrint());
        UNIT_ASSERT_VALUES_EQUAL(false, readHint1.WaitReady.IsReady());

        dirtyMap.RestorePBuffer(
            123,
            TBlockRange64::WithLength(10, 10),
            ELocation::PBuffer1);
        auto readHint2 =
            dirtyMap.MakeReadHint(TBlockRange64::WithLength(10, 10));
        UNIT_ASSERT_VALUES_EQUAL("WaitReady:NotReady", readHint2.DebugPrint());
        UNIT_ASSERT_VALUES_EQUAL(false, readHint2.WaitReady.IsReady());

        dirtyMap.RestorePBuffer(
            123,
            TBlockRange64::WithLength(10, 10),
            ELocation::PBuffer2);
        auto readHint3 =
            dirtyMap.MakeReadHint(TBlockRange64::WithLength(10, 10));
        UNIT_ASSERT_VALUES_EQUAL(
            "123{[D.....P+++..][10..19][0..9]};",
            readHint3.DebugPrint());

        UNIT_ASSERT_VALUES_EQUAL(true, readHint1.WaitReady.IsReady());
        UNIT_ASSERT_VALUES_EQUAL(true, readHint2.WaitReady.IsReady());
    }

    Y_UNIT_TEST(ShouldReadHintsTwoSequentialNonOverlappingInflightRanges)
    {
        TBlocksDirtyMap dirtyMap(
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        dirtyMap.WriteFinished(
            100,
            TBlockRange64::WithLength(10, 10),
            TLocationMask::MakePrimaryPBuffers(),
            TLocationMask::MakePrimaryPBuffers());

        dirtyMap.WriteFinished(
            200,
            TBlockRange64::WithLength(30, 10),
            TLocationMask::MakePrimaryPBuffers(),
            TLocationMask::MakePrimaryPBuffers());

        auto readHint = dirtyMap.MakeReadHint(TBlockRange64::WithLength(0, 50));

        UNIT_ASSERT_VALUES_EQUAL(5, readHint.RangeHints.size());
        UNIT_ASSERT_VALUES_EQUAL(
            "0{[D+++..P.....][0..9][0..9]};"
            "100{[D.....P+++..][10..19][10..19]};"
            "0{[D+++..P.....][20..29][20..29]};"
            "200{[D.....P+++..][30..39][30..39]};"
            "0{[D+++..P.....][40..49][40..49]};",
            readHint.DebugPrint());
    }

    Y_UNIT_TEST(ShouldReadHintsTwoFullyOverlappingInflightRanges)
    {
        TBlocksDirtyMap dirtyMap(
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        dirtyMap.WriteFinished(
            100,
            TBlockRange64::WithLength(10, 41),
            TLocationMask::MakePrimaryPBuffers(),
            TLocationMask::MakePrimaryPBuffers());

        dirtyMap.WriteFinished(
            200,
            TBlockRange64::WithLength(20, 11),
            TLocationMask::MakePrimaryPBuffers(),
            TLocationMask::MakePrimaryPBuffers());

        auto readHint =
            dirtyMap.MakeReadHint(TBlockRange64::WithLength(10, 41));

        UNIT_ASSERT_VALUES_EQUAL(3, readHint.RangeHints.size());
        UNIT_ASSERT_VALUES_EQUAL(
            "100{[D.....P+++..][10..19][0..9]};"
            "200{[D.....P+++..][20..30][10..20]};"
            "100{[D.....P+++..][31..50][21..40]};",
            readHint.DebugPrint());

        dirtyMap.WriteFinished(
            300,
            TBlockRange64::WithLength(0, 50),
            TLocationMask::MakePrimaryPBuffers(),
            TLocationMask::MakePrimaryPBuffers());
        readHint = dirtyMap.MakeReadHint(TBlockRange64::WithLength(5, 40));

        UNIT_ASSERT_VALUES_EQUAL(1, readHint.RangeHints.size());
        UNIT_ASSERT_VALUES_EQUAL(
            "300{[D.....P+++..][5..44][0..39]};",
            readHint.DebugPrint());
    }

    Y_UNIT_TEST(ShouldReadHintsTwoPartiallyOverlappingInflightRanges)
    {
        TBlocksDirtyMap dirtyMap(
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        dirtyMap.WriteFinished(
            100,
            TBlockRange64::WithLength(10, 21),
            TLocationMask::MakePrimaryPBuffers(),
            TLocationMask::MakePrimaryPBuffers());

        dirtyMap.WriteFinished(
            200,
            TBlockRange64::WithLength(25, 21),
            TLocationMask::MakePrimaryPBuffers(),
            TLocationMask::MakePrimaryPBuffers());

        auto readHint =
            dirtyMap.MakeReadHint(TBlockRange64::WithLength(10, 36));

        UNIT_ASSERT_VALUES_EQUAL(2, readHint.RangeHints.size());
        UNIT_ASSERT_VALUES_EQUAL(
            "100{[D.....P+++..][10..24][0..14]};"
            "200{[D.....P+++..][25..45][15..35]};",
            readHint.DebugPrint());
    }

    Y_UNIT_TEST(ShouldReadHintsThreeOverlappingInflightRanges)
    {
        TBlocksDirtyMap dirtyMap(
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        dirtyMap.WriteFinished(
            100,
            TBlockRange64::WithLength(10, 41),
            TLocationMask::MakePrimaryPBuffers(),
            TLocationMask::MakePrimaryPBuffers());

        dirtyMap.WriteFinished(
            150,
            TBlockRange64::WithLength(20, 21),
            TLocationMask::MakePrimaryPBuffers(),
            TLocationMask::MakePrimaryPBuffers());

        dirtyMap.WriteFinished(
            200,
            TBlockRange64::WithLength(30, 6),
            TLocationMask::MakePrimaryPBuffers(),
            TLocationMask::MakePrimaryPBuffers());

        auto readHint =
            dirtyMap.MakeReadHint(TBlockRange64::WithLength(10, 41));

        UNIT_ASSERT_VALUES_EQUAL(5, readHint.RangeHints.size());
        UNIT_ASSERT_VALUES_EQUAL(
            "100{[D.....P+++..][10..19][0..9]};"
            "150{[D.....P+++..][20..29][10..19]};"
            "200{[D.....P+++..][30..35][20..25]};"
            "150{[D.....P+++..][36..40][26..30]};"
            "100{[D.....P+++..][41..50][31..40]};",
            readHint.DebugPrint());
    }

    Y_UNIT_TEST(ShouldReadHintsRangeWithEdgesOfRequest)
    {
        TBlocksDirtyMap dirtyMap(
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        dirtyMap.WriteFinished(
            100,
            TBlockRange64::WithLength(10, 10),
            TLocationMask::MakePrimaryPBuffers(),
            TLocationMask::MakePrimaryPBuffers());

        auto readHint =
            dirtyMap.MakeReadHint(TBlockRange64::WithLength(10, 10));

        UNIT_ASSERT_VALUES_EQUAL(1, readHint.RangeHints.size());
        UNIT_ASSERT_VALUES_EQUAL(
            "100{[D.....P+++..][10..19][0..9]};",
            readHint.DebugPrint());
    }

    Y_UNIT_TEST(ShouldReadHintsRangeWithSameStart)
    {
        TBlocksDirtyMap dirtyMap(
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        dirtyMap.WriteFinished(
            100,
            TBlockRange64::WithLength(10, 100),
            TLocationMask::MakePrimaryPBuffers(),
            TLocationMask::MakePrimaryPBuffers());

        dirtyMap.WriteFinished(
            200,
            TBlockRange64::WithLength(10, 40),
            TLocationMask::MakePrimaryPBuffers(),
            TLocationMask::MakePrimaryPBuffers());

        auto readHint =
            dirtyMap.MakeReadHint(TBlockRange64::WithLength(0, 100));

        UNIT_ASSERT_VALUES_EQUAL(3, readHint.RangeHints.size());
        UNIT_ASSERT_VALUES_EQUAL(
            "0{[D+++..P.....][0..9][0..9]};"
            "200{[D.....P+++..][10..49][10..49]};"
            "100{[D.....P+++..][50..99][50..99]};",
            readHint.DebugPrint());
    }

    Y_UNIT_TEST(ShouldReadHintsManyConsecutiveRanges)
    {
        TBlocksDirtyMap dirtyMap(
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        const int lsnsCount = 100;
        for (int i = 1; i <= lsnsCount; ++i) {
            dirtyMap.WriteFinished(
                i,
                TBlockRange64::WithLength(i, 1),
                TLocationMask::MakePrimaryPBuffers(),
                TLocationMask::MakePrimaryPBuffers());
        }

        auto readHint =
            dirtyMap.MakeReadHint(TBlockRange64::WithLength(0, lsnsCount + 1));

        UNIT_ASSERT_VALUES_EQUAL(lsnsCount + 1, readHint.RangeHints.size());

        for (size_t i = 0; i < readHint.RangeHints.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(i, readHint.RangeHints[i].Lsn);
            UNIT_ASSERT_VALUES_EQUAL(
                i == 0,
                readHint.RangeHints[i].LocationMask.HasDDisk());
            UNIT_ASSERT_VALUES_EQUAL(
                i,
                readHint.RangeHints[i].RequestRelativeRange.Start);
            UNIT_ASSERT_VALUES_EQUAL(
                i,
                readHint.RangeHints[i].RequestRelativeRange.End);
            UNIT_ASSERT_VALUES_EQUAL(
                i,
                readHint.RangeHints[i].VChunkRange.Start);
            UNIT_ASSERT_VALUES_EQUAL(i, readHint.RangeHints[i].VChunkRange.End);
        }
    }

    Y_UNIT_TEST(ShouldReadHintsStaircaseWithOverlappedRanges)
    {
        TBlocksDirtyMap dirtyMap(
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        dirtyMap.WriteFinished(
            100,
            TBlockRange64::WithLength(10, 21),
            TLocationMask::MakePrimaryPBuffers(),
            TLocationMask::MakePrimaryPBuffers());

        dirtyMap.WriteFinished(
            200,
            TBlockRange64::WithLength(25, 21),
            TLocationMask::MakePrimaryPBuffers(),
            TLocationMask::MakePrimaryPBuffers());

        dirtyMap.WriteFinished(
            300,
            TBlockRange64::WithLength(40, 21),
            TLocationMask::MakePrimaryPBuffers(),
            TLocationMask::MakePrimaryPBuffers());

        auto readHint =
            dirtyMap.MakeReadHint(TBlockRange64::WithLength(10, 51));

        UNIT_ASSERT_VALUES_EQUAL(3, readHint.RangeHints.size());
        UNIT_ASSERT_VALUES_EQUAL(
            "100{[D.....P+++..][10..24][0..14]};"
            "200{[D.....P+++..][25..39][15..29]};"
            "300{[D.....P+++..][40..60][30..50]};",
            readHint.DebugPrint());
    }

    Y_UNIT_TEST(ShouldReadHintsFewRangesInsideOfDDiskData)
    {
        TBlocksDirtyMap dirtyMap(
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        dirtyMap.WriteFinished(
            100,
            TBlockRange64::WithLength(10, 6),
            TLocationMask::MakePrimaryPBuffers(),
            TLocationMask::MakePrimaryPBuffers());

        dirtyMap.WriteFinished(
            200,
            TBlockRange64::WithLength(25, 6),
            TLocationMask::MakePrimaryPBuffers(),
            TLocationMask::MakePrimaryPBuffers());

        dirtyMap.WriteFinished(
            300,
            TBlockRange64::WithLength(45, 6),
            TLocationMask::MakePrimaryPBuffers(),
            TLocationMask::MakePrimaryPBuffers());

        auto readHint = dirtyMap.MakeReadHint(TBlockRange64::WithLength(0, 61));

        UNIT_ASSERT_VALUES_EQUAL(7, readHint.RangeHints.size());
        UNIT_ASSERT_VALUES_EQUAL(
            "0{[D+++..P.....][0..9][0..9]};"
            "100{[D.....P+++..][10..15][10..15]};"
            "0{[D+++..P.....][16..24][16..24]};"
            "200{[D.....P+++..][25..30][25..30]};"
            "0{[D+++..P.....][31..44][31..44]};"
            "300{[D.....P+++..][45..50][45..50]};"
            "0{[D+++..P.....][51..60][51..60]};",
            readHint.DebugPrint());
    }

    Y_UNIT_TEST(ShouldReadHintsFewBiggerLsnsInsideOfOneSmaller)
    {
        TBlocksDirtyMap dirtyMap(
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        dirtyMap.WriteFinished(
            100,
            TBlockRange64::WithLength(10, 91),
            TLocationMask::MakePrimaryPBuffers(),
            TLocationMask::MakePrimaryPBuffers());

        dirtyMap.WriteFinished(
            200,
            TBlockRange64::WithLength(20, 6),
            TLocationMask::MakePrimaryPBuffers(),
            TLocationMask::MakePrimaryPBuffers());

        dirtyMap.WriteFinished(
            300,
            TBlockRange64::WithLength(40, 6),
            TLocationMask::MakePrimaryPBuffers(),
            TLocationMask::MakePrimaryPBuffers());

        dirtyMap.WriteFinished(
            400,
            TBlockRange64::WithLength(70, 6),
            TLocationMask::MakePrimaryPBuffers(),
            TLocationMask::MakePrimaryPBuffers());

        auto readHint =
            dirtyMap.MakeReadHint(TBlockRange64::WithLength(10, 91));

        UNIT_ASSERT_VALUES_EQUAL(7, readHint.RangeHints.size());
        UNIT_ASSERT_VALUES_EQUAL(
            "100{[D.....P+++..][10..19][0..9]};"
            "200{[D.....P+++..][20..25][10..15]};"
            "100{[D.....P+++..][26..39][16..29]};"
            "300{[D.....P+++..][40..45][30..35]};"
            "100{[D.....P+++..][46..69][36..59]};"
            "400{[D.....P+++..][70..75][60..65]};"
            "100{[D.....P+++..][76..100][66..90]};",
            readHint.DebugPrint());
    }

    Y_UNIT_TEST(ShouldReadHintsReturnDDiskWhenNoQuorum)
    {
        TBlocksDirtyMap dirtyMap(
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        auto inflightCounterBeforeWrite = dirtyMap.GetInflightCount();
        dirtyMap.WriteFinished(
            100,
            TBlockRange64::WithLength(10, 41),
            TLocationMask::MakePrimaryPBuffers(),
            TLocationMask::MakePBuffer(true, true, false, false, false));

        // write result with no quorum is skipped
        UNIT_ASSERT_VALUES_EQUAL(
            inflightCounterBeforeWrite,
            dirtyMap.GetInflightCount());

        auto readHint =
            dirtyMap.MakeReadHint(TBlockRange64::WithLength(10, 41));

        UNIT_ASSERT_VALUES_EQUAL(1, readHint.RangeHints.size());
        UNIT_ASSERT_VALUES_EQUAL(
            "0{[D+++..P.....][10..50][0..40]};",
            readHint.DebugPrint());

        dirtyMap.WriteFinished(
            200,
            TBlockRange64::WithLength(10, 41),
            TLocationMask::MakePrimaryPBuffers(),
            TLocationMask::MakePBuffer(true, true, true, false, false));
        auto readHint1 =
            dirtyMap.MakeReadHint(TBlockRange64::WithLength(10, 41));
        UNIT_ASSERT_VALUES_EQUAL(1, readHint1.RangeHints.size());
        UNIT_ASSERT_VALUES_EQUAL(
            "200{[D.....P+++..][10..50][0..40]};",
            readHint1.DebugPrint());
    }
}

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
