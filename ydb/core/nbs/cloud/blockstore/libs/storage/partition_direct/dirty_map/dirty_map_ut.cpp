#include "dirty_map.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host_roles.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/vchunk_config.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 DefaultVChunkSize = RegionSize / DirectBlockGroupsCount;
constexpr size_t HostCount = 5;
constexpr size_t PrimaryCount = 3;

TVChunkConfig MakeTestVChunkConfig()
{
    return TVChunkConfig::Make(
        /*vChunkIndex=*/0,
        HostCount,
        PrimaryCount);
}

void ApplyStatuses(
    TBlocksDirtyMap& map,
    TVector<EHostRole> assignments,
    TVector<EHostState> states)
{
    Y_ABORT_UNLESS(assignments.size() == HostCount);
    Y_ABORT_UNLESS(states.size() == HostCount);

    THostMask desired;
    THostMask disabled;
    for (THostIndex i = 0; i < assignments.size(); ++i) {
        if (assignments[i] == EHostRole::Primary &&
            states[i] == EHostState::Enabled)
        {
            desired.Set(i);
        } else if (
            assignments[i] == EHostRole::None ||
            states[i] == EHostState::Disabled)
        {
            disabled.Set(i);
        }
    }
    map.UpdateConfig(desired, desired, disabled);
}

////////////////////////////////////////////////////////////////////////////////

TVector<ui64> GetLsns(const TVector<TPBufferSegment>& segments)
{
    TVector<ui64> lsns;
    for (const auto& segment: segments) {
        lsns.push_back(segment.Lsn);
    }
    return lsns;
}

using enum EHostRole;
using enum EHostState;

THostMask MakePrimaryHosts()
{
    return THostMask::MakeAll(3);
}

THostMask MakeHostMask(bool b0, bool b1, bool b2, bool b3, bool b4)
{
    THostMask mask;
    if (b0) {
        mask.Set(0);
    }
    if (b1) {
        mask.Set(1);
    }
    if (b2) {
        mask.Set(2);
    }
    if (b3) {
        mask.Set(3);
    }
    if (b4) {
        mask.Set(4);
    }
    return mask;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDirtyMapTest)
{
    Y_UNIT_TEST(ShouldReadWithoutWrites)
    {
        const auto vchunkConfig = MakeTestVChunkConfig();
        TBlocksDirtyMap dirtyMap(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        // We should be able to get read hints (default DesiredDDisks =
        // primary).
        auto readHint =
            dirtyMap.MakeReadHint(TBlockRange64::WithLength(10, 10));
        UNIT_ASSERT_VALUES_EQUAL(
            "0{[H0,H1,H2][10..19][0..9]};",
            readHint.DebugPrint());

        // Disable host 0, demote host 4 to disabled, promote host 3 to
        // primary.
        ApplyStatuses(
            dirtyMap,
            {HandOff, Primary, Primary, Primary, HandOff},
            {Disabled, Enabled, Enabled, Enabled, Disabled});
        readHint = dirtyMap.MakeReadHint(TBlockRange64::WithLength(10, 10));
        UNIT_ASSERT_VALUES_EQUAL(
            "0{[H1,H2,H3][10..19][0..9]};",
            readHint.DebugPrint());
    }

    Y_UNIT_TEST(ShouldNotReadFromFresh)
    {
        const auto vchunkConfig = MakeTestVChunkConfig();
        TBlocksDirtyMap dirtyMap(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

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
            "0{[H0,H1,H2][10..19][0..9]};",
            readHint.DebugPrint());

        // Read crossed fresh watermark
        readHint = dirtyMap.MakeReadHint(TBlockRange64::WithLength(25, 10));
        UNIT_ASSERT_VALUES_EQUAL(
            "0{[H1,H2][25..34][0..9]};",
            readHint.DebugPrint());

        // Read above fresh watermark
        readHint = dirtyMap.MakeReadHint(TBlockRange64::WithLength(30, 10));
        UNIT_ASSERT_VALUES_EQUAL(
            "0{[H1,H2][30..39][0..9]};",
            readHint.DebugPrint());

        // Read above fresh watermark
        readHint = dirtyMap.MakeReadHint(TBlockRange64::WithLength(40, 10));
        UNIT_ASSERT_VALUES_EQUAL(
            "0{[H1][40..49][0..9]};",
            readHint.DebugPrint());
    }

    Y_UNIT_TEST(ShouldReadAfterWriteFinished)
    {
        const auto vchunkConfig = MakeTestVChunkConfig();
        TBlocksDirtyMap dirtyMap(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

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
        ApplyStatuses(
            dirtyMap,
            {HandOff, Primary, Primary, Primary, HandOff},
            {Disabled, Enabled, Enabled, Enabled, Enabled});

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

    Y_UNIT_TEST(ShouldReadAfterWriteFinishedFromLastLsn)
    {
        const auto vchunkConfig = MakeTestVChunkConfig();
        TBlocksDirtyMap dirtyMap(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        dirtyMap.WriteFinished(
            123,
            TBlockRange64::WithLength(10, 10),
            MakePrimaryHosts(),
            MakePrimaryHosts());

        dirtyMap.WriteFinished(
            124,
            TBlockRange64::WithLength(10, 10),
            MakeHostMask(true, true, false, true, false),
            MakeHostMask(true, true, false, true, false));

        // After write, we should be able to get read hints
        auto readHint =
            dirtyMap.MakeReadHint(TBlockRange64::WithLength(10, 10));
        UNIT_ASSERT_VALUES_EQUAL(
            "124{[H0,H1,H3][10..19][0..9]};",
            readHint.DebugPrint());

        // Disable host 0, promote host 3 to primary.
        ApplyStatuses(
            dirtyMap,
            {HandOff, Primary, Primary, Primary, HandOff},
            {Disabled, Enabled, Enabled, Enabled, Enabled});

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
        const auto vchunkConfig = MakeTestVChunkConfig();
        TBlocksDirtyMap dirtyMap(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

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
        const auto vchunkConfig = MakeTestVChunkConfig();
        TBlocksDirtyMap dirtyMap(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        // Enable additional Hand-off: hosts 0,1,2,3 are primary, host 4 is
        // hand-off.
        ApplyStatuses(
            dirtyMap,
            {Primary, Primary, Primary, Primary, HandOff},
            {Enabled, Enabled, Enabled, Enabled, Enabled});

        // Written to 2 primary and 1 hand-off
        const THostMask requested =
            MakeHostMask(false, true, true, true, false);
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
        const auto vchunkConfig = MakeTestVChunkConfig();
        TBlocksDirtyMap dirtyMap(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        // Enable Hand-off-0 instead of DDisk0: host 0 disabled,
        // hosts 1,2,3 primary, host 4 hand-off.
        ApplyStatuses(
            dirtyMap,
            {HandOff, Primary, Primary, Primary, HandOff},
            {Disabled, Enabled, Enabled, Enabled, Enabled});

        // Written to two primary and one hand-off
        const THostMask requested =
            MakeHostMask(false, true, true, true, false);
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
        const auto vchunkConfig = MakeTestVChunkConfig();
        TBlocksDirtyMap dirtyMap(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        // Hosts 0,1 disabled; hosts 2,3,4 are primary.
        ApplyStatuses(
            dirtyMap,
            {HandOff, HandOff, Primary, Primary, Primary},
            {Disabled, Disabled, Enabled, Enabled, Enabled});

        // Written to one primary and two hand-off
        const THostMask requested =
            MakeHostMask(false, false, true, true, true);
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
        const auto vchunkConfig = MakeTestVChunkConfig();
        TBlocksDirtyMap dirtyMap(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        // Host 0 disabled; hosts 1,2,3 primary; host 4 hand-off.
        ApplyStatuses(
            dirtyMap,
            {HandOff, Primary, Primary, Primary, HandOff},
            {Disabled, Enabled, Enabled, Enabled, Enabled});

        // Written to all 3 primary PBuffers (hosts 0,1,2). Host 0 is
        // disabled, but the data is still on its PBuffer.
        const THostMask requested =
            MakeHostMask(true, true, true, false, false);
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
        const auto vchunkConfig = MakeTestVChunkConfig();
        TBlocksDirtyMap dirtyMap(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        // Enable 4 DDisks (hosts 0,1,2,3 primary). Available DDisks is
        // enough for a quorum.
        ApplyStatuses(
            dirtyMap,
            {Primary, Primary, Primary, Primary, HandOff},
            {Enabled, Enabled, Enabled, Enabled, Enabled});

        dirtyMap.SetFlushWatermark(THostIndex{2}, 100 * DefaultBlockSize);

        const THostMask requested =
            MakeHostMask(true, true, true, false, false);
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
        const auto vchunkConfig = MakeTestVChunkConfig();
        TBlocksDirtyMap dirtyMap(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        // Only 3 DDisks available by default. For some requests, ddisk will not
        // be sufficient for quorum.
        dirtyMap.SetFlushWatermark(THostIndex{2}, 100 * DefaultBlockSize);

        const THostMask requested =
            MakeHostMask(true, true, true, false, false);
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
        const auto vchunkConfig = MakeTestVChunkConfig();
        TBlocksDirtyMap dirtyMap(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

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
        const auto vchunkConfig = MakeTestVChunkConfig();
        TBlocksDirtyMap dirtyMap(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);
        const THostMask mask = MakePrimaryHosts();

        // Lock range on DDisk (for reading).
        auto lockHandle =
            dirtyMap.LockDDiskRange(TBlockRange64::WithLength(5, 10), mask);

        // User write to overlapped with locked range.
        dirtyMap.WriteFinished(
            123,
            TBlockRange64::WithLength(10, 10),
            MakePrimaryHosts(),
            MakePrimaryHosts());

        // Flush hints should not be generated when DDisk is locked.
        auto flushHint = dirtyMap.MakeFlushHint(1);
        UNIT_ASSERT_EQUAL(true, flushHint.Empty());

        // Unlock DDisk
        dirtyMap.UnLockDDiskRange(lockHandle);

        // FLush hints should be generated after DDisk is unlocked.
        auto eraseHints = dirtyMap.MakeEraseHint(1);
        UNIT_ASSERT_EQUAL(true, eraseHints.Empty());
    }

    Y_UNIT_TEST(ShouldRestoreCompletePBuffer)
    {
        const auto vchunkConfig = MakeTestVChunkConfig();
        TBlocksDirtyMap dirtyMap(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

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
        const auto vchunkConfig = MakeTestVChunkConfig();
        TBlocksDirtyMap dirtyMap(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

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
        const auto vchunkConfig = MakeTestVChunkConfig();
        TBlocksDirtyMap dirtyMap(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

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
        const auto vchunkConfig = MakeTestVChunkConfig();
        TBlocksDirtyMap dirtyMap(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

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

        // lsn 123 is post-flush (FromDDisk) over [0..99]; lsn 124 is in
        // PBuffer over [10..19]. The read should be split into three hints:
        // DDisk(lsn=123) for [0..9] and [20..99], PBuffer(lsn=124) for the
        // overlapped [10..19]. Reading the whole range from DDisk would
        // return stale data for [10..19].
        auto readHint =
            dirtyMap.MakeReadHint(TBlockRange64::WithLength(0, 100));
        UNIT_ASSERT_VALUES_EQUAL(
            "0{[H0,H1,H2][0..9][0..9]};"
            "124{[H0,H1,H2][10..19][10..19]};"
            "0{[H0,H1,H2][20..99][20..99]};",
            readHint.DebugPrint());
    }

    Y_UNIT_TEST(ReadShouldWaitPBufferRestore)
    {
        const auto vchunkConfig = MakeTestVChunkConfig();
        TBlocksDirtyMap dirtyMap(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

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

    Y_UNIT_TEST(ShouldReadHintsTwoSequentialNonOverlappingInflightRanges)
    {
        const auto vchunkConfig = MakeTestVChunkConfig();
        TBlocksDirtyMap dirtyMap(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        dirtyMap.WriteFinished(
            100,
            TBlockRange64::WithLength(10, 10),
            MakePrimaryHosts(),
            MakePrimaryHosts());

        dirtyMap.WriteFinished(
            200,
            TBlockRange64::WithLength(30, 10),
            MakePrimaryHosts(),
            MakePrimaryHosts());

        auto readHint = dirtyMap.MakeReadHint(TBlockRange64::WithLength(0, 50));

        UNIT_ASSERT_VALUES_EQUAL(5, readHint.RangeHints.size());
        UNIT_ASSERT_VALUES_EQUAL(
            "0{[H0,H1,H2][0..9][0..9]};"
            "100{[H0,H1,H2][10..19][10..19]};"
            "0{[H0,H1,H2][20..29][20..29]};"
            "200{[H0,H1,H2][30..39][30..39]};"
            "0{[H0,H1,H2][40..49][40..49]};",
            readHint.DebugPrint());
    }

    Y_UNIT_TEST(ShouldReadHintsTwoFullyOverlappingInflightRanges)
    {
        const auto vchunkConfig = MakeTestVChunkConfig();
        TBlocksDirtyMap dirtyMap(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        dirtyMap.WriteFinished(
            100,
            TBlockRange64::WithLength(10, 41),
            MakePrimaryHosts(),
            MakePrimaryHosts());

        dirtyMap.WriteFinished(
            200,
            TBlockRange64::WithLength(20, 11),
            MakePrimaryHosts(),
            MakePrimaryHosts());

        auto readHint =
            dirtyMap.MakeReadHint(TBlockRange64::WithLength(10, 41));

        UNIT_ASSERT_VALUES_EQUAL(3, readHint.RangeHints.size());
        UNIT_ASSERT_VALUES_EQUAL(
            "100{[H0,H1,H2][10..19][0..9]};"
            "200{[H0,H1,H2][20..30][10..20]};"
            "100{[H0,H1,H2][31..50][21..40]};",
            readHint.DebugPrint());

        dirtyMap.WriteFinished(
            300,
            TBlockRange64::WithLength(0, 50),
            MakePrimaryHosts(),
            MakePrimaryHosts());
        readHint = dirtyMap.MakeReadHint(TBlockRange64::WithLength(5, 40));

        UNIT_ASSERT_VALUES_EQUAL(1, readHint.RangeHints.size());
        UNIT_ASSERT_VALUES_EQUAL(
            "300{[H0,H1,H2][5..44][0..39]};",
            readHint.DebugPrint());
    }

    Y_UNIT_TEST(ShouldReadHintsTwoPartiallyOverlappingInflightRanges)
    {
        const auto vchunkConfig = MakeTestVChunkConfig();
        TBlocksDirtyMap dirtyMap(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        dirtyMap.WriteFinished(
            100,
            TBlockRange64::WithLength(10, 21),
            MakePrimaryHosts(),
            MakePrimaryHosts());

        dirtyMap.WriteFinished(
            200,
            TBlockRange64::WithLength(25, 21),
            MakePrimaryHosts(),
            MakePrimaryHosts());

        auto readHint =
            dirtyMap.MakeReadHint(TBlockRange64::WithLength(10, 36));

        UNIT_ASSERT_VALUES_EQUAL(2, readHint.RangeHints.size());
        UNIT_ASSERT_VALUES_EQUAL(
            "100{[H0,H1,H2][10..24][0..14]};"
            "200{[H0,H1,H2][25..45][15..35]};",
            readHint.DebugPrint());
    }

    Y_UNIT_TEST(ShouldReadHintsThreeOverlappingInflightRanges)
    {
        const auto vchunkConfig = MakeTestVChunkConfig();
        TBlocksDirtyMap dirtyMap(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        dirtyMap.WriteFinished(
            100,
            TBlockRange64::WithLength(10, 41),
            MakePrimaryHosts(),
            MakePrimaryHosts());

        dirtyMap.WriteFinished(
            150,
            TBlockRange64::WithLength(20, 21),
            MakePrimaryHosts(),
            MakePrimaryHosts());

        dirtyMap.WriteFinished(
            200,
            TBlockRange64::WithLength(30, 6),
            MakePrimaryHosts(),
            MakePrimaryHosts());

        auto readHint =
            dirtyMap.MakeReadHint(TBlockRange64::WithLength(10, 41));

        UNIT_ASSERT_VALUES_EQUAL(5, readHint.RangeHints.size());
        UNIT_ASSERT_VALUES_EQUAL(
            "100{[H0,H1,H2][10..19][0..9]};"
            "150{[H0,H1,H2][20..29][10..19]};"
            "200{[H0,H1,H2][30..35][20..25]};"
            "150{[H0,H1,H2][36..40][26..30]};"
            "100{[H0,H1,H2][41..50][31..40]};",
            readHint.DebugPrint());
    }

    Y_UNIT_TEST(ShouldReadHintsRangeWithEdgesOfRequest)
    {
        const auto vchunkConfig = MakeTestVChunkConfig();
        TBlocksDirtyMap dirtyMap(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        dirtyMap.WriteFinished(
            100,
            TBlockRange64::WithLength(10, 10),
            MakePrimaryHosts(),
            MakePrimaryHosts());

        auto readHint =
            dirtyMap.MakeReadHint(TBlockRange64::WithLength(10, 10));

        UNIT_ASSERT_VALUES_EQUAL(1, readHint.RangeHints.size());
        UNIT_ASSERT_VALUES_EQUAL(
            "100{[H0,H1,H2][10..19][0..9]};",
            readHint.DebugPrint());
    }

    Y_UNIT_TEST(ShouldReadHintsRangeWithSameStart)
    {
        const auto vchunkConfig = MakeTestVChunkConfig();
        TBlocksDirtyMap dirtyMap(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        dirtyMap.WriteFinished(
            100,
            TBlockRange64::WithLength(10, 100),
            MakePrimaryHosts(),
            MakePrimaryHosts());

        dirtyMap.WriteFinished(
            200,
            TBlockRange64::WithLength(10, 40),
            MakePrimaryHosts(),
            MakePrimaryHosts());

        auto readHint =
            dirtyMap.MakeReadHint(TBlockRange64::WithLength(0, 100));

        UNIT_ASSERT_VALUES_EQUAL(3, readHint.RangeHints.size());
        UNIT_ASSERT_VALUES_EQUAL(
            "0{[H0,H1,H2][0..9][0..9]};"
            "200{[H0,H1,H2][10..49][10..49]};"
            "100{[H0,H1,H2][50..99][50..99]};",
            readHint.DebugPrint());
    }

    Y_UNIT_TEST(ShouldReadHintsManyConsecutiveRanges)
    {
        const auto vchunkConfig = MakeTestVChunkConfig();
        TBlocksDirtyMap dirtyMap(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        const int lsnsCount = 100;
        for (int i = 1; i <= lsnsCount; ++i) {
            dirtyMap.WriteFinished(
                i,
                TBlockRange64::WithLength(i, 1),
                MakePrimaryHosts(),
                MakePrimaryHosts());
        }

        auto readHint =
            dirtyMap.MakeReadHint(TBlockRange64::WithLength(0, lsnsCount + 1));

        UNIT_ASSERT_VALUES_EQUAL(lsnsCount + 1, readHint.RangeHints.size());

        for (size_t i = 0; i < readHint.RangeHints.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(i, readHint.RangeHints[i].Lsn);
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
        const auto vchunkConfig = MakeTestVChunkConfig();
        TBlocksDirtyMap dirtyMap(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        dirtyMap.WriteFinished(
            100,
            TBlockRange64::WithLength(10, 21),
            MakePrimaryHosts(),
            MakePrimaryHosts());

        dirtyMap.WriteFinished(
            200,
            TBlockRange64::WithLength(25, 21),
            MakePrimaryHosts(),
            MakePrimaryHosts());

        dirtyMap.WriteFinished(
            300,
            TBlockRange64::WithLength(40, 21),
            MakePrimaryHosts(),
            MakePrimaryHosts());

        auto readHint =
            dirtyMap.MakeReadHint(TBlockRange64::WithLength(10, 51));

        UNIT_ASSERT_VALUES_EQUAL(3, readHint.RangeHints.size());
        UNIT_ASSERT_VALUES_EQUAL(
            "100{[H0,H1,H2][10..24][0..14]};"
            "200{[H0,H1,H2][25..39][15..29]};"
            "300{[H0,H1,H2][40..60][30..50]};",
            readHint.DebugPrint());
    }

    Y_UNIT_TEST(ShouldReadHintsFewRangesInsideOfDDiskData)
    {
        const auto vchunkConfig = MakeTestVChunkConfig();
        TBlocksDirtyMap dirtyMap(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        dirtyMap.WriteFinished(
            100,
            TBlockRange64::WithLength(10, 6),
            MakePrimaryHosts(),
            MakePrimaryHosts());

        dirtyMap.WriteFinished(
            200,
            TBlockRange64::WithLength(25, 6),
            MakePrimaryHosts(),
            MakePrimaryHosts());

        dirtyMap.WriteFinished(
            300,
            TBlockRange64::WithLength(45, 6),
            MakePrimaryHosts(),
            MakePrimaryHosts());

        auto readHint = dirtyMap.MakeReadHint(TBlockRange64::WithLength(0, 61));

        UNIT_ASSERT_VALUES_EQUAL(7, readHint.RangeHints.size());
        UNIT_ASSERT_VALUES_EQUAL(
            "0{[H0,H1,H2][0..9][0..9]};"
            "100{[H0,H1,H2][10..15][10..15]};"
            "0{[H0,H1,H2][16..24][16..24]};"
            "200{[H0,H1,H2][25..30][25..30]};"
            "0{[H0,H1,H2][31..44][31..44]};"
            "300{[H0,H1,H2][45..50][45..50]};"
            "0{[H0,H1,H2][51..60][51..60]};",
            readHint.DebugPrint());
    }

    Y_UNIT_TEST(ShouldReadHintsFewBiggerLsnsInsideOfOneSmaller)
    {
        const auto vchunkConfig = MakeTestVChunkConfig();
        TBlocksDirtyMap dirtyMap(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        dirtyMap.WriteFinished(
            100,
            TBlockRange64::WithLength(10, 91),
            MakePrimaryHosts(),
            MakePrimaryHosts());

        dirtyMap.WriteFinished(
            200,
            TBlockRange64::WithLength(20, 6),
            MakePrimaryHosts(),
            MakePrimaryHosts());

        dirtyMap.WriteFinished(
            300,
            TBlockRange64::WithLength(40, 6),
            MakePrimaryHosts(),
            MakePrimaryHosts());

        dirtyMap.WriteFinished(
            400,
            TBlockRange64::WithLength(70, 6),
            MakePrimaryHosts(),
            MakePrimaryHosts());

        auto readHint =
            dirtyMap.MakeReadHint(TBlockRange64::WithLength(10, 91));

        UNIT_ASSERT_VALUES_EQUAL(7, readHint.RangeHints.size());
        UNIT_ASSERT_VALUES_EQUAL(
            "100{[H0,H1,H2][10..19][0..9]};"
            "200{[H0,H1,H2][20..25][10..15]};"
            "100{[H0,H1,H2][26..39][16..29]};"
            "300{[H0,H1,H2][40..45][30..35]};"
            "100{[H0,H1,H2][46..69][36..59]};"
            "400{[H0,H1,H2][70..75][60..65]};"
            "100{[H0,H1,H2][76..100][66..90]};",
            readHint.DebugPrint());
    }

    Y_UNIT_TEST(ShouldReadHintsReturnDDiskWhenNoQuorum)
    {
        const auto vchunkConfig = MakeTestVChunkConfig();
        TBlocksDirtyMap dirtyMap(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        auto inflightCounterBeforeWrite = dirtyMap.GetInflightCount();
        dirtyMap.WriteFinished(
            100,
            TBlockRange64::WithLength(10, 41),
            MakePrimaryHosts(),
            MakeHostMask(true, true, false, false, false));

        // write result with no quorum is skipped
        UNIT_ASSERT_VALUES_EQUAL(
            inflightCounterBeforeWrite,
            dirtyMap.GetInflightCount());

        auto readHint =
            dirtyMap.MakeReadHint(TBlockRange64::WithLength(10, 41));

        UNIT_ASSERT_VALUES_EQUAL(1, readHint.RangeHints.size());
        UNIT_ASSERT_VALUES_EQUAL(
            "0{[H0,H1,H2][10..50][0..40]};",
            readHint.DebugPrint());

        dirtyMap.WriteFinished(
            200,
            TBlockRange64::WithLength(10, 41),
            MakePrimaryHosts(),
            MakeHostMask(true, true, true, false, false));
        auto readHint1 =
            dirtyMap.MakeReadHint(TBlockRange64::WithLength(10, 41));
        UNIT_ASSERT_VALUES_EQUAL(1, readHint1.RangeHints.size());
        UNIT_ASSERT_VALUES_EQUAL(
            "200{[H0,H1,H2][10..50][0..40]};",
            readHint1.DebugPrint());
    }
}

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
