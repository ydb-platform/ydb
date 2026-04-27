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

THostStatusList MakeStatuses(
    bool desired0,
    bool desired1,
    bool desired2,
    bool desired3,
    bool desired4,
    bool disabled0 = false,
    bool disabled1 = false,
    bool disabled2 = false,
    bool disabled3 = false,
    bool disabled4 = false)
{
    THostStatusList result(5);
    const bool desired[5] = {desired0, desired1, desired2, desired3, desired4};
    const bool disabled[5] =
        {disabled0, disabled1, disabled2, disabled3, disabled4};
    for (THostIndex i = 0; i < 5; ++i) {
        if (disabled[i]) {
            result.Set(i, EHostStatus::Disabled);
        } else if (desired[i]) {
            result.Set(i, EHostStatus::Primary);
        } else {
            result.Set(i, EHostStatus::HandOff);
        }
    }
    return result;
}

// Default configuration: hosts 0,1,2 are Primary; hosts 3,4 are HandOff.
THostStatusList MakeDefaultStatuses()
{
    return MakeStatuses(true, true, true, false, false);
}

void SetupDefault(TBlocksDirtyMap& dm)
{
    dm.UpdateHostStatuses(MakeDefaultStatuses(), MakeDefaultStatuses());
}

THostMask MakePrimaryHosts()
{
    return THostMask::MakeAll(3);
}

THostMask MakePBufferMask(bool b0, bool b1, bool b2, bool b3, bool b4)
{
    THostMask m;
    if (b0) {
        m.Set(0);
    }
    if (b1) {
        m.Set(1);
    }
    if (b2) {
        m.Set(2);
    }
    if (b3) {
        m.Set(3);
    }
    if (b4) {
        m.Set(4);
    }
    return m;
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
        SetupDefault(dirtyMap);

        // We should be able to get read hints (active = primary + handoff).
        auto readHint =
            dirtyMap.MakeReadHint(TBlockRange64::WithLength(10, 10));
        UNIT_ASSERT_VALUES_EQUAL(
            "0{[H0,H1,H2,H3,H4][10..19][0..9]};",
            readHint.DebugPrint());

        // Disable host 0, demote host 4 to disabled, promote host 3 to
        // primary.
        dirtyMap.UpdateHostStatuses(
            MakeStatuses(
                false,
                true,
                true,
                true,
                false,
                true,
                false,
                false,
                false,
                true),
            MakeStatuses(
                false,
                true,
                true,
                true,
                false,
                true,
                false,
                false,
                false,
                true));
        readHint = dirtyMap.MakeReadHint(TBlockRange64::WithLength(10, 10));
        UNIT_ASSERT_VALUES_EQUAL(
            "0{[H1,H2,H3][10..19][0..9]};",
            readHint.DebugPrint());
    }

    Y_UNIT_TEST(ShouldNotReadFromFresh)
    {
        TBlocksDirtyMap dirtyMap(
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);
        SetupDefault(dirtyMap);

        dirtyMap.MarkFresh(THostIndex{0}, 30 * DefaultBlockSize);
        dirtyMap.MarkFresh(THostIndex{2}, 40 * DefaultBlockSize);

        UNIT_ASSERT_VALUES_EQUAL(
            "H0{Fresh,30,30};"
            "H1{Operational,32768,32768};"
            "H2{Fresh,40,40};"
            "H3{Operational,32768,32768};"
            "H4{Operational,32768,32768};",
            dirtyMap.DebugPrintDDiskState());

        // Read below fresh watermark
        auto readHint =
            dirtyMap.MakeReadHint(TBlockRange64::WithLength(10, 10));
        UNIT_ASSERT_VALUES_EQUAL(
            "0{[H0,H1,H2,H3,H4][10..19][0..9]};",
            readHint.DebugPrint());

        // Read crossed fresh watermark
        readHint = dirtyMap.MakeReadHint(TBlockRange64::WithLength(25, 10));
        UNIT_ASSERT_VALUES_EQUAL(
            "0{[H1,H2,H3,H4][25..34][0..9]};",
            readHint.DebugPrint());

        // Read above fresh watermark
        readHint = dirtyMap.MakeReadHint(TBlockRange64::WithLength(30, 10));
        UNIT_ASSERT_VALUES_EQUAL(
            "0{[H1,H2,H3,H4][30..39][0..9]};",
            readHint.DebugPrint());

        // Read above fresh watermark
        readHint = dirtyMap.MakeReadHint(TBlockRange64::WithLength(40, 10));
        UNIT_ASSERT_VALUES_EQUAL(
            "0{[H1,H3,H4][40..49][0..9]};",
            readHint.DebugPrint());
    }

    Y_UNIT_TEST(ShouldReadAfterWriteFinished)
    {
        TBlocksDirtyMap dirtyMap(
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);
        SetupDefault(dirtyMap);

        dirtyMap.WriteFinished(
            123,
            TBlockRange64::WithLength(10, 10),
            MakePrimaryHosts(),
            MakePrimaryHosts());

        // After write, we should be able to get read hints (read from
        // confirmed PBuffers — hosts {0,1,2}).
        auto readHint =
            dirtyMap.MakeReadHint(TBlockRange64::WithLength(10, 10));
        UNIT_ASSERT_VALUES_EQUAL(
            "123{[H0,H1,H2][10..19][0..9]};",
            readHint.DebugPrint());

        // Disable host 0, promote host 3 to primary.
        dirtyMap.UpdateHostStatuses(
            MakeStatuses(
                false,
                true,
                true,
                true,
                false,
                true,
                false,
                false,
                false,
                false),
            MakeStatuses(
                false,
                true,
                true,
                true,
                false,
                true,
                false,
                false,
                false,
                false));

        readHint = dirtyMap.MakeReadHint(TBlockRange64::WithLength(10, 10));
        // WriteConfirmed mask is {0,1,2}; host 0 is disabled, so it is
        // excluded from the read mask.
        UNIT_ASSERT_VALUES_EQUAL(
            "123{[H1,H2][10..19][0..9]};",
            readHint.DebugPrint());

        // Counters on primary PBuffers contain one record with 40960 bytes
        for (THostIndex h: MakePrimaryHosts()) {
            auto counters = dirtyMap.GetPBufferCounters(h);
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

    Y_UNIT_TEST(ShouldReadFromDDisksAfterFlush)
    {
        // Regression test for the post-flush read hint bug. With the original
        // PBufferHosts/hostMask discriminator, a read covering an inflight
        // item in PBufferFlushed state would mistakenly take the PB lock
        // branch and Arm() would abort because LockPBuffer() rejects
        // post-flush states.
        TBlocksDirtyMap dirtyMap(
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);
        SetupDefault(dirtyMap);

        const ui64 lsn = 123;
        const auto range = TBlockRange64::WithLength(10, 10);

        // Write -> WriteConfirmed quorum on H0,H1,H2.
        dirtyMap
            .WriteFinished(lsn, range, MakePrimaryHosts(), MakePrimaryHosts());

        // Run flush to completion: the inflight item transitions to
        // PBufferFlushed (state where ReadMask must produce a DDisk-side
        // read).
        auto flushHint = dirtyMap.MakeFlushHint(1);
        UNIT_ASSERT_EQUAL(false, flushHint.Empty());
        for (const auto& [route, hint]: flushHint.GetAllHints()) {
            dirtyMap.FlushFinished(route, GetLsns(hint.Segments), {});
        }

        // Read the same range. The inflight is in PBufferFlushed; the read
        // should target DDisk hosts (all active DDisks) and use a DDisk
        // range lock.
        auto readHint = dirtyMap.MakeReadHint(range);
        UNIT_ASSERT_VALUES_EQUAL(1u, readHint.RangeHints.size());

        const auto& rh = readHint.RangeHints[0];
        UNIT_ASSERT(!rh.HostMask.Empty());
        UNIT_ASSERT_VALUES_EQUAL(
            "123{[H0,H1,H2,H3,H4][10..19][0..9]};",
            readHint.DebugPrint());

        // Arm() must not abort: it should take the DDisk-range-lock path,
        // not LockPBuffer() (which rejects post-flush states).
        readHint.RangeHints[0].Lock.Arm();
    }

    Y_UNIT_TEST(ShouldReadAfterWriteFinishedFromLastLsn)
    {
        TBlocksDirtyMap dirtyMap(
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);
        SetupDefault(dirtyMap);

        dirtyMap.WriteFinished(
            123,
            TBlockRange64::WithLength(10, 10),
            MakePrimaryHosts(),
            MakePrimaryHosts());

        dirtyMap.WriteFinished(
            124,
            TBlockRange64::WithLength(10, 10),
            MakePBufferMask(true, true, false, true, false),
            MakePBufferMask(true, true, false, true, false));

        // After write, we should be able to get read hints
        auto readHint =
            dirtyMap.MakeReadHint(TBlockRange64::WithLength(10, 10));
        UNIT_ASSERT_VALUES_EQUAL(
            "124{[H0,H1,H3][10..19][0..9]};",
            readHint.DebugPrint());

        // Disable host 0, promote host 3 to primary.
        dirtyMap.UpdateHostStatuses(
            MakeStatuses(
                false,
                true,
                true,
                true,
                false,
                true,
                false,
                false,
                false,
                false),
            MakeStatuses(
                false,
                true,
                true,
                true,
                false,
                true,
                false,
                false,
                false,
                false));

        readHint = dirtyMap.MakeReadHint(TBlockRange64::WithLength(10, 10));
        UNIT_ASSERT_VALUES_EQUAL(
            "124{[H1,H3][10..19][0..9]};",
            readHint.DebugPrint());

        readHint.RangeHints[0].Lock.Arm();

        {
            // Host 0 contains two records, one locked for read
            auto counters = dirtyMap.GetPBufferCounters(THostIndex{0});
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
            // Host 3 contains one record, one locked for read
            auto counters = dirtyMap.GetPBufferCounters(THostIndex{3});
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
        SetupDefault(dirtyMap);

        // Without write, we should not get flush hints
        auto flushHint = dirtyMap.MakeFlushHint(1);
        UNIT_ASSERT_EQUAL(true, flushHint.Empty());

        const THostMask requested = MakePrimaryHosts();
        const THostMask confirmed = MakePrimaryHosts();

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
            "H0->H0:123[10..19],124[20..29];"
            "H1->H1:123[10..19],124[20..29];"
            "H2->H2:123[10..19],124[20..29];",
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
            THostRoute{.SourceHostIndex = 0, .DestinationHostIndex = 0},
            {123, 124},
            {});
        dirtyMap.FlushFinished(
            THostRoute{.SourceHostIndex = 1, .DestinationHostIndex = 1},
            {123, 124},
            {});
        dirtyMap.FlushFinished(
            THostRoute{.SourceHostIndex = 2, .DestinationHostIndex = 2},
            {},
            {123, 124});

        flushHint = dirtyMap.MakeFlushHint(2);
        UNIT_ASSERT_EQUAL(false, flushHint.Empty());
        UNIT_ASSERT_VALUES_EQUAL(
            "H2->H2:123[10..19],124[20..29];",
            flushHint.DebugPrint());

        // Complete flushing to third ddisk
        dirtyMap.FlushFinished(
            THostRoute{.SourceHostIndex = 2, .DestinationHostIndex = 2},
            {123, 124},
            {});

        // Erase hints should be generated after completing the required
        // number of write operations.
        eraseHints = dirtyMap.MakeEraseHint(2);
        UNIT_ASSERT_VALUES_EQUAL(
            "H0:123[10..19],124[20..29];"
            "H1:123[10..19],124[20..29];"
            "H2:123[10..19],124[20..29];",
            eraseHints.DebugPrint());

        // After getting erase hints, we should not get it once again
        {
            auto eraseHint = dirtyMap.MakeEraseHint(2);
            UNIT_ASSERT_EQUAL(true, eraseHint.Empty());
        }

        // After getting erasing errors, we should get erase hints again
        dirtyMap.EraseFinished(THostIndex{0}, {123, 124}, {});
        dirtyMap.EraseFinished(THostIndex{1}, {123, 124}, {});
        dirtyMap.EraseFinished(THostIndex{2}, {}, {123, 124});

        eraseHints = dirtyMap.MakeEraseHint(2);
        UNIT_ASSERT_VALUES_EQUAL(
            "H2:123[10..19],124[20..29];",
            eraseHints.DebugPrint());

        // Should still have two inflight items
        UNIT_ASSERT_VALUES_EQUAL(2, dirtyMap.GetInflightCount());

        // Complete erasing from third pbuffer
        dirtyMap.EraseFinished(THostIndex{2}, {123, 124}, {});
        eraseHints = dirtyMap.MakeEraseHint(2);
        UNIT_ASSERT_EQUAL(true, eraseHints.Empty());

        // Should remove inflight items
        UNIT_ASSERT_VALUES_EQUAL(0, dirtyMap.GetInflightCount());

        // All current counters back to zero.
        for (THostIndex h: MakePrimaryHosts()) {
            auto counters = dirtyMap.GetPBufferCounters(h);
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

        // Enable additional Hand-off: hosts 0,1,2,3 are primary, host 4 is
        // hand-off.
        const auto statuses = MakeStatuses(true, true, true, true, false);
        dirtyMap.UpdateHostStatuses(statuses, statuses);

        // Written to 2 primary and 1 hand-off
        const THostMask requested =
            MakePBufferMask(false, true, true, true, false);
        const THostMask confirmed = requested;

        dirtyMap.WriteFinished(
            123,
            TBlockRange64::WithLength(10, 10),
            requested,
            confirmed);

        auto flushHint = dirtyMap.MakeFlushHint(1);
        UNIT_ASSERT_VALUES_EQUAL(
            "H1->H0:123[10..19];"   // Cross-node
            "H1->H1:123[10..19];"
            "H2->H2:123[10..19];"
            "H3->H3:123[10..19];",
            flushHint.DebugPrint());

        // Finish flushes
        for (const auto& [route, hint]: flushHint.GetAllHints()) {
            dirtyMap.FlushFinished(route, GetLsns(hint.Segments), {});
        }

        // Erase hints
        auto eraseHints = dirtyMap.MakeEraseHint(1);
        UNIT_ASSERT_VALUES_EQUAL(
            "H1:123[10..19];"
            "H2:123[10..19];"
            "H3:123[10..19];",
            eraseHints.DebugPrint());

        // Finish erasing
        for (const auto& [host, hint]: eraseHints.GetAllHints()) {
            dirtyMap.EraseFinished(host, GetLsns(hint.Segments), {});
        }
    }

    Y_UNIT_TEST(ShouldWriteAndFlushAndEraseWithOneDisabled)
    {
        TBlocksDirtyMap dirtyMap(
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        // Enable Hand-off-0 instead of DDisk0: host 0 disabled,
        // hosts 1,2,3 primary, host 4 hand-off.
        const auto statuses = MakeStatuses(
            false,
            true,
            true,
            true,
            false,
            true,
            false,
            false,
            false,
            false);
        dirtyMap.UpdateHostStatuses(statuses, statuses);

        // Written to two primary and one hand-off
        const THostMask requested =
            MakePBufferMask(false, true, true, true, false);
        const THostMask confirmed = requested;

        dirtyMap.WriteFinished(
            123,
            TBlockRange64::WithLength(10, 10),
            requested,
            confirmed);

        auto flushHint = dirtyMap.MakeFlushHint(1);
        UNIT_ASSERT_VALUES_EQUAL(
            "H1->H1:123[10..19];"
            "H2->H2:123[10..19];"
            "H3->H3:123[10..19];",
            flushHint.DebugPrint());

        // Finish flushes
        for (const auto& [route, hint]: flushHint.GetAllHints()) {
            dirtyMap.FlushFinished(route, GetLsns(hint.Segments), {});
        }

        // Erase hints
        auto eraseHints = dirtyMap.MakeEraseHint(1);
        UNIT_ASSERT_VALUES_EQUAL(
            "H1:123[10..19];"
            "H2:123[10..19];"
            "H3:123[10..19];",
            eraseHints.DebugPrint());

        // Finish erasing
        for (const auto& [host, hint]: eraseHints.GetAllHints()) {
            dirtyMap.EraseFinished(host, GetLsns(hint.Segments), {});
        }
    }

    Y_UNIT_TEST(ShouldWriteAndFlushAndEraseWithTwoDisabled)
    {
        TBlocksDirtyMap dirtyMap(
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        // Hosts 0,1 disabled; hosts 2,3,4 are primary.
        const auto statuses = MakeStatuses(
            false,
            false,
            true,
            true,
            true,
            true,
            true,
            false,
            false,
            false);
        dirtyMap.UpdateHostStatuses(statuses, statuses);

        // Written to one primary and two hand-off
        const THostMask requested =
            MakePBufferMask(false, false, true, true, true);
        const THostMask confirmed = requested;

        dirtyMap.WriteFinished(
            123,
            TBlockRange64::WithLength(10, 10),
            requested,
            confirmed);

        auto flushHint = dirtyMap.MakeFlushHint(1);
        UNIT_ASSERT_VALUES_EQUAL(
            "H2->H2:123[10..19];"
            "H3->H3:123[10..19];"
            "H4->H4:123[10..19];",
            flushHint.DebugPrint());

        // Finish flushes
        for (const auto& [route, hint]: flushHint.GetAllHints()) {
            dirtyMap.FlushFinished(route, GetLsns(hint.Segments), {});
        }

        // Erase hints
        auto eraseHints = dirtyMap.MakeEraseHint(1);
        UNIT_ASSERT_VALUES_EQUAL(
            "H2:123[10..19];"
            "H3:123[10..19];"
            "H4:123[10..19];",
            eraseHints.DebugPrint());

        // Finish erasing
        for (const auto& [host, hint]: eraseHints.GetAllHints()) {
            dirtyMap.EraseFinished(host, GetLsns(hint.Segments), {});
        }
    }

    Y_UNIT_TEST(ShouldNotFlushAndEraseFromDisabled)
    {
        TBlocksDirtyMap dirtyMap(
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        // Host 0 disabled; hosts 1,2,3 primary; host 4 hand-off.
        const auto statuses = MakeStatuses(
            false,
            true,
            true,
            true,
            false,
            true,
            false,
            false,
            false,
            false);
        dirtyMap.UpdateHostStatuses(statuses, statuses);

        // Written to all 3 primary PBuffers (hosts 0,1,2). Host 0 is
        // disabled in the new layout, but the data is still on its PBuffer.
        const THostMask requested =
            MakePBufferMask(true, true, true, false, false);
        const THostMask confirmed = requested;

        dirtyMap.WriteFinished(
            123,
            TBlockRange64::WithLength(10, 10),
            requested,
            confirmed);

        auto flushHint = dirtyMap.MakeFlushHint(1);
        UNIT_ASSERT_VALUES_EQUAL(
            "H0->H3:123[10..19];"
            "H1->H1:123[10..19];"
            "H2->H2:123[10..19];",
            flushHint.DebugPrint());

        // Finish flushes
        for (const auto& [route, hint]: flushHint.GetAllHints()) {
            dirtyMap.FlushFinished(route, GetLsns(hint.Segments), {});
        }

        // Erase hints
        auto eraseHints = dirtyMap.MakeEraseHint(1);
        UNIT_ASSERT_VALUES_EQUAL(
            "H1:123[10..19];"
            "H2:123[10..19];",
            eraseHints.DebugPrint());

        // Finish erasing
        for (const auto& [host, hint]: eraseHints.GetAllHints()) {
            dirtyMap.EraseFinished(host, GetLsns(hint.Segments), {});
        }

        // Should remove inflight items
        UNIT_ASSERT_VALUES_EQUAL(0, dirtyMap.GetInflightCount());
    }

    Y_UNIT_TEST(ShouldNotFlushOverWriteWatermark)
    {
        TBlocksDirtyMap dirtyMap(
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        // Enable 4 DDisks (hosts 0,1,2,3 primary). Available DDisks is
        // enough for a quorum.
        const auto statuses = MakeStatuses(true, true, true, true, false);
        dirtyMap.UpdateHostStatuses(statuses, statuses);

        dirtyMap.SetFlushWatermark(THostIndex{2}, 100 * DefaultBlockSize);

        const THostMask requested =
            MakePBufferMask(true, true, true, false, false);
        const THostMask confirmed = requested;

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
            "H0->H0:123[10..19],124[95..104],125[100..109];"
            "H0->H3:123[10..19],124[95..104],125[100..109];"
            "H1->H1:123[10..19],124[95..104],125[100..109];"
            "H2->H2:123[10..19],124[95..104];",
            flushHint.DebugPrint());
    }

    Y_UNIT_TEST(ShouldBlockFlushOverWriteWatermark)
    {
        TBlocksDirtyMap dirtyMap(
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);
        SetupDefault(dirtyMap);

        // Only 3 DDisks available by default. For some requests, ddisk will not
        // be sufficient for quorum.
        dirtyMap.SetFlushWatermark(THostIndex{2}, 100 * DefaultBlockSize);

        const THostMask requested =
            MakePBufferMask(true, true, true, false, false);
        const THostMask confirmed = requested;

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
            "H0->H0:123[10..19],124[95..104];"
            "H1->H1:123[10..19],124[95..104];"
            "H2->H2:123[10..19],124[95..104];",
            flushHint.DebugPrint());
    }

    Y_UNIT_TEST(ShouldLockPBuffer)
    {
        TBlocksDirtyMap dirtyMap(
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);
        SetupDefault(dirtyMap);

        dirtyMap.WriteFinished(
            123,
            TBlockRange64::WithLength(10, 10),
            MakePrimaryHosts(),
            MakePrimaryHosts());

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
        SetupDefault(dirtyMap);
        const THostMask mask = MakePrimaryHosts();

        dirtyMap.WriteFinished(
            123,
            TBlockRange64::WithLength(10, 10),
            MakePrimaryHosts(),
            MakePrimaryHosts());

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
        SetupDefault(dirtyMap);

        dirtyMap.RestorePBuffer(
            123,
            TBlockRange64::WithLength(10, 10),
            THostIndex{0});
        dirtyMap.RestorePBuffer(
            123,
            TBlockRange64::WithLength(10, 10),
            THostIndex{1});
        dirtyMap.RestorePBuffer(
            123,
            TBlockRange64::WithLength(10, 10),
            THostIndex{2});

        // Flush hints should be generated when has quorum PBuffers.
        auto flushHint = dirtyMap.MakeFlushHint(1);
        UNIT_ASSERT_EQUAL(false, flushHint.Empty());

        UNIT_ASSERT_VALUES_EQUAL(
            "H0->H0:123[10..19];"
            "H1->H1:123[10..19];"
            "H2->H2:123[10..19];",
            flushHint.DebugPrint());
    }

    Y_UNIT_TEST(ShouldRestoreOverCompletePBuffer)
    {
        TBlocksDirtyMap dirtyMap(
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);
        SetupDefault(dirtyMap);

        // Block written to four PBuffers
        dirtyMap.RestorePBuffer(
            123,
            TBlockRange64::WithLength(10, 10),
            THostIndex{0});
        dirtyMap.RestorePBuffer(
            123,
            TBlockRange64::WithLength(10, 10),
            THostIndex{1});
        dirtyMap.RestorePBuffer(
            123,
            TBlockRange64::WithLength(10, 10),
            THostIndex{2});
        dirtyMap.RestorePBuffer(
            123,
            TBlockRange64::WithLength(10, 10),
            THostIndex{3});

        // Flush hints should be generated when has quorum PBuffers.
        auto flushHint = dirtyMap.MakeFlushHint(1);
        UNIT_ASSERT_EQUAL(false, flushHint.Empty());

        UNIT_ASSERT_VALUES_EQUAL(
            "H0->H0:123[10..19];"
            "H1->H1:123[10..19];"
            "H2->H2:123[10..19];",
            flushHint.DebugPrint());

        auto readHint =
            dirtyMap.MakeReadHint(TBlockRange64::WithLength(10, 10));
        UNIT_ASSERT_VALUES_EQUAL(
            "123{[H0,H1,H2,H3][10..19][0..9]};",
            readHint.DebugPrint());

        for (THostIndex h:
             {THostIndex{0}, THostIndex{1}, THostIndex{2}, THostIndex{3}})
        {
            auto counters = dirtyMap.GetPBufferCounters(h);
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
        SetupDefault(dirtyMap);

        // Block written to two primary PBuffers and one hand-off PBuffer
        dirtyMap.RestorePBuffer(
            123,
            TBlockRange64::WithLength(10, 10),
            THostIndex{1});
        dirtyMap.RestorePBuffer(
            123,
            TBlockRange64::WithLength(10, 10),
            THostIndex{2});
        dirtyMap.RestorePBuffer(
            123,
            TBlockRange64::WithLength(10, 10),
            THostIndex{3});

        // Flush hints should be generated when has quorum PBuffers.
        auto flushHint = dirtyMap.MakeFlushHint(1);
        UNIT_ASSERT_EQUAL(false, flushHint.Empty());

        UNIT_ASSERT_VALUES_EQUAL(
            "H1->H0:123[10..19];"
            "H1->H1:123[10..19];"
            "H2->H2:123[10..19];",
            flushHint.DebugPrint());

        auto readHint =
            dirtyMap.MakeReadHint(TBlockRange64::WithLength(10, 10));
        UNIT_ASSERT_VALUES_EQUAL(
            "123{[H1,H2,H3][10..19][0..9]};",
            readHint.DebugPrint());
    }

    Y_UNIT_TEST(ShouldReadFromDDiskIfRangeIsNotCoveredByInflightRange)
    {
        TBlocksDirtyMap dirtyMap(
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);
        SetupDefault(dirtyMap);

        dirtyMap.WriteFinished(
            123,
            TBlockRange64::WithLength(0, 100),
            MakePrimaryHosts(),
            MakePrimaryHosts());

        auto flushHint = dirtyMap.MakeFlushHint(1);
        UNIT_ASSERT_VALUES_EQUAL(
            "H0->H0:123[0..99];"
            "H1->H1:123[0..99];"
            "H2->H2:123[0..99];",
            flushHint.DebugPrint());

        dirtyMap.FlushFinished(
            THostRoute{.SourceHostIndex = 0, .DestinationHostIndex = 0},
            {123},
            {});
        dirtyMap.FlushFinished(
            THostRoute{.SourceHostIndex = 1, .DestinationHostIndex = 1},
            {123},
            {});
        dirtyMap.FlushFinished(
            THostRoute{.SourceHostIndex = 2, .DestinationHostIndex = 2},
            {123},
            {});

        auto eraseHints = dirtyMap.MakeEraseHint(1);
        UNIT_ASSERT_VALUES_EQUAL(
            "H0:123[0..99];"
            "H1:123[0..99];"
            "H2:123[0..99];",
            eraseHints.DebugPrint());

        dirtyMap.EraseFinished(THostIndex{0}, {123}, {});

        dirtyMap.WriteFinished(
            124,
            TBlockRange64::WithLength(10, 10),
            MakePrimaryHosts(),
            MakePrimaryHosts());

        auto readHint =
            dirtyMap.MakeReadHint(TBlockRange64::WithLength(0, 100));
        UNIT_ASSERT_VALUES_EQUAL(
            "0{[H0,H1,H2,H3,H4][0..99][0..99]};",
            readHint.DebugPrint());
    }

    Y_UNIT_TEST(ReadShouldWaitPBufferRestore)
    {
        TBlocksDirtyMap dirtyMap(
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);
        SetupDefault(dirtyMap);

        dirtyMap.RestorePBuffer(
            123,
            TBlockRange64::WithLength(10, 10),
            THostIndex{0});
        auto readHint1 =
            dirtyMap.MakeReadHint(TBlockRange64::WithLength(10, 10));
        UNIT_ASSERT_VALUES_EQUAL("WaitReady:NotReady", readHint1.DebugPrint());
        UNIT_ASSERT_VALUES_EQUAL(false, readHint1.WaitReady.IsReady());

        dirtyMap.RestorePBuffer(
            123,
            TBlockRange64::WithLength(10, 10),
            THostIndex{1});
        auto readHint2 =
            dirtyMap.MakeReadHint(TBlockRange64::WithLength(10, 10));
        UNIT_ASSERT_VALUES_EQUAL("WaitReady:NotReady", readHint2.DebugPrint());
        UNIT_ASSERT_VALUES_EQUAL(false, readHint2.WaitReady.IsReady());

        dirtyMap.RestorePBuffer(
            123,
            TBlockRange64::WithLength(10, 10),
            THostIndex{2});
        auto readHint3 =
            dirtyMap.MakeReadHint(TBlockRange64::WithLength(10, 10));
        UNIT_ASSERT_VALUES_EQUAL(
            "123{[H0,H1,H2][10..19][0..9]};",
            readHint3.DebugPrint());

        UNIT_ASSERT_VALUES_EQUAL(true, readHint1.WaitReady.IsReady());
        UNIT_ASSERT_VALUES_EQUAL(true, readHint2.WaitReady.IsReady());
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
