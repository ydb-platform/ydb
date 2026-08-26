#include "dirty_map.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host_roles.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/vchunk_config.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos/dirty_map.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 DefaultVChunkSize = RegionSize / DirectBlockGroupsCount;

TVChunkConfig MakeTestVChunkConfig()
{
    return TVChunkConfig::MakeDefault(
        0,   // VChunkIndex
        DirectBlockGroupHostCount,
        DefaultPrimaryCount);
}

////////////////////////////////////////////////////////////////////////////////

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

void FlushAll(const TFlushHints& flushHint, TBlocksDirtyMap& dirtyMap)
{
    for (const auto& [route, hint]: flushHint.GetAllHints()) {
        dirtyMap.FlushFinished(route, MakeLsnVector(hint.Segments), {});
    }
}

void EraseAll(const TEraseHints& eraseHints, TBlocksDirtyMap& dirtyMap)
{
    for (const auto& [host, hint]: eraseHints.GetAllHints()) {
        dirtyMap.EraseFinished(host, MakeLsnVector(hint.Segments), {});
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDirtyMapTest)
{
    Y_UNIT_TEST(ShouldKeepTotalPBufferCountersAfterRelease)
    {
        const auto vchunkConfig = MakeTestVChunkConfig();
        auto dirtyMap = std::make_shared<TBlocksDirtyMap>(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        constexpr THostIndex Host = 0;
        constexpr size_t ByteCount = 4096;

        dirtyMap->DataToPBufferAdded(
            Host,
            IReadyQueue::EPBufferCounter::Total,
            ByteCount);
        dirtyMap->DataToPBufferAdded(
            Host,
            IReadyQueue::EPBufferCounter::Locked,
            ByteCount);
        dirtyMap->DataFromPBufferReleased(
            Host,
            IReadyQueue::EPBufferCounter::Locked,
            ByteCount);
        dirtyMap->DataFromPBufferReleased(
            Host,
            IReadyQueue::EPBufferCounter::Total,
            ByteCount);

        const auto& counters = dirtyMap->GetPBufferCounters(Host);
        UNIT_ASSERT_VALUES_EQUAL(0, counters.Current.Count);
        UNIT_ASSERT_VALUES_EQUAL(0, counters.Current.Size);
        UNIT_ASSERT_VALUES_EQUAL(1, counters.Total.Count);
        UNIT_ASSERT_VALUES_EQUAL(ByteCount, counters.Total.Size);
        UNIT_ASSERT_VALUES_EQUAL(0, counters.CurrentLocked.Count);
        UNIT_ASSERT_VALUES_EQUAL(0, counters.CurrentLocked.Size);
        UNIT_ASSERT_VALUES_EQUAL(1, counters.TotalLocked.Count);
        UNIT_ASSERT_VALUES_EQUAL(ByteCount, counters.TotalLocked.Size);
    }

    Y_UNIT_TEST(ShouldReadWithoutWrites)
    {
        auto vchunkConfig = MakeTestVChunkConfig();
        auto dirtyMap = std::make_shared<TBlocksDirtyMap>(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        UNIT_ASSERT_VALUES_EQUAL(
            "H0*{Operational,32768};"
            "H1*{Operational,32768};"
            "H2*{Operational,32768};"
            "H3+{Disabled,0};"
            "H4+{Disabled,0};",
            dirtyMap->DebugPrintDDiskState());

        // We should be able to get read hints (default DesiredDDisks =
        // primary).
        auto readHint =
            dirtyMap->MakeReadHint(TBlockRange64::WithLength(10, 10));
        UNIT_ASSERT_VALUES_EQUAL(
            "0{[H0,H1,H2][10..19][0..9]};",
            readHint.DebugPrint());

        // Disable host 0
        vchunkConfig.DisableHost(0);
        dirtyMap->UpdateConfig(vchunkConfig);

        readHint = dirtyMap->MakeReadHint(TBlockRange64::WithLength(10, 10));
        UNIT_ASSERT_VALUES_EQUAL(
            "0{[H1,H2][10..19][0..9]};",
            readHint.DebugPrint());
    }

    Y_UNIT_TEST(ShouldResizeHosts)
    {
        auto vchunkConfig = MakeTestVChunkConfig();
        auto dirtyMap = std::make_shared<TBlocksDirtyMap>(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        vchunkConfig.AppendHost();
        const auto newIdx = static_cast<THostIndex>(5);
        dirtyMap->UpdateConfig(vchunkConfig);

        UNIT_ASSERT_VALUES_EQUAL(
            0u,
            dirtyMap->GetPBufferCounters(newIdx).Current.Size);
        UNIT_ASSERT_VALUES_EQUAL(
            "H0*{Operational,32768};"
            "H1*{Operational,32768};"
            "H2*{Operational,32768};"
            "H3+{Disabled,0};"
            "H4+{Disabled,0};"
            "H5+{Disabled,0};",
            dirtyMap->DebugPrintDDiskState());
    }

    Y_UNIT_TEST(ShouldRespectWatermarksWhenConstruct)
    {
        auto vchunkConfig = MakeTestVChunkConfig();
        vchunkConfig.SetWatermark(0, 30 * DefaultBlockSize);
        vchunkConfig.SetWatermark(2, 40 * DefaultBlockSize);

        auto dirtyMap = std::make_shared<TBlocksDirtyMap>(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        UNIT_ASSERT_VALUES_EQUAL(
            "H0*{Fresh+,30};"
            "H1*{Operational,32768};"
            "H2*{Fresh+,40};"
            "H3+{Disabled,0};"
            "H4+{Disabled,0};",
            dirtyMap->DebugPrintDDiskState());
    }

    Y_UNIT_TEST(ShouldRespectWatermarksForAddedDDisks)
    {
        auto vchunkConfig = MakeTestVChunkConfig();
        auto dirtyMap = std::make_shared<TBlocksDirtyMap>(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        vchunkConfig.PromoteHost(3);
        vchunkConfig.SetWatermark(0, 30 * DefaultBlockSize);
        vchunkConfig.SetWatermark(3, 40 * DefaultBlockSize);
        dirtyMap->UpdateConfig(vchunkConfig);
        UNIT_ASSERT_VALUES_EQUAL(
            "H0*{Operational,32768};"
            "H1*{Operational,32768};"
            "H2*{Operational,32768};"
            "H3*{Fresh+,40};"
            "H4+{Disabled,0};",
            dirtyMap->DebugPrintDDiskState());
    }

    Y_UNIT_TEST(ShouldSwitchOffline)
    {
        auto vchunkConfig = MakeTestVChunkConfig();
        // Offline H1
        vchunkConfig.EvacuateHost(1);

        auto dirtyMap = std::make_shared<TBlocksDirtyMap>(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        UNIT_ASSERT_VALUES_EQUAL(
            "H0*{Operational,32768};"
            "H1-{Disabled,0};"
            "H2*{Operational,32768};"
            "H3*{Fresh+,0};"
            "H4+{Disabled,0};",
            dirtyMap->DebugPrintDDiskState());

        // Offline H0
        vchunkConfig.EvacuateHost(0);
        dirtyMap->UpdateConfig(vchunkConfig);
        UNIT_ASSERT_VALUES_EQUAL(
            "H0-{Disabled,0};"
            "H1-{Disabled,0};"
            "H2*{Operational,32768};"
            "H3*{Fresh+,0};"
            "H4*{Fresh+,0};",
            dirtyMap->DebugPrintDDiskState());

        // Can't switch H2 offline
        vchunkConfig.EvacuateHost(2);
        dirtyMap->UpdateConfig(vchunkConfig);
        UNIT_ASSERT_VALUES_EQUAL(
            "H0-{Disabled,0};"
            "H1-{Disabled,0};"
            "H2-{Operational,32768};"
            "H3*{Fresh+,0};"
            "H4*{Fresh+,0};",
            dirtyMap->DebugPrintDDiskState());

        // Offline H3
        vchunkConfig.EvacuateHost(3);
        dirtyMap->UpdateConfig(vchunkConfig);
        UNIT_ASSERT_VALUES_EQUAL(
            "H0-{Disabled,0};"
            "H1-{Disabled,0};"
            "H2-{Operational,32768};"
            "H3-{Disabled,0};"
            "H4*{Fresh+,0};",
            dirtyMap->DebugPrintDDiskState());

        // Offline H4
        vchunkConfig.EvacuateHost(4);
        dirtyMap->UpdateConfig(vchunkConfig);
        UNIT_ASSERT_VALUES_EQUAL(
            "H0-{Disabled,0};"
            "H1-{Disabled,0};"
            "H2-{Operational,32768};"
            "H3-{Disabled,0};"
            "H4-{Disabled,0};",
            dirtyMap->DebugPrintDDiskState());

        // Enable H4
        vchunkConfig.EnableHost(4);
        dirtyMap->UpdateConfig(vchunkConfig);
        UNIT_ASSERT_VALUES_EQUAL(
            "H0-{Disabled,0};"
            "H1-{Disabled,0};"
            "H2-{Operational,32768};"
            "H3-{Disabled,0};"
            "H4*{Fresh+,0};",
            dirtyMap->DebugPrintDDiskState());

        // Enable H0
        vchunkConfig.EnableHost(0);
        dirtyMap->UpdateConfig(vchunkConfig);
        UNIT_ASSERT_VALUES_EQUAL(
            "H0*{Fresh+,0};"
            "H1-{Disabled,0};"
            "H2-{Operational,32768};"
            "H3-{Disabled,0};"
            "H4*{Fresh+,0};",
            dirtyMap->DebugPrintDDiskState());

        // Enable H1
        vchunkConfig.EnableHost(1);
        dirtyMap->UpdateConfig(vchunkConfig);
        UNIT_ASSERT_VALUES_EQUAL(
            "H0*{Fresh+,0};"
            "H1+{Disabled,0};"
            "H2-{Operational,32768};"
            "H3-{Disabled,0};"
            "H4*{Fresh+,0};",
            dirtyMap->DebugPrintDDiskState());

        // Enable H2
        vchunkConfig.EnableHost(2);
        dirtyMap->UpdateConfig(vchunkConfig);
        UNIT_ASSERT_VALUES_EQUAL(
            "H0*{Fresh+,0};"
            "H1+{Disabled,0};"
            "H2*{Operational,32768};"
            "H3-{Disabled,0};"
            "H4*{Fresh+,0};",
            dirtyMap->DebugPrintDDiskState());

        // Enable H3
        vchunkConfig.EnableHost(3);
        dirtyMap->UpdateConfig(vchunkConfig);
        UNIT_ASSERT_VALUES_EQUAL(
            "H0*{Fresh+,0};"
            "H1+{Disabled,0};"
            "H2*{Operational,32768};"
            "H3+{Disabled,0};"
            "H4*{Fresh+,0};",
            dirtyMap->DebugPrintDDiskState());

        // Can't switch H2 offline
        vchunkConfig.EvacuateHost(2);
        dirtyMap->UpdateConfig(vchunkConfig);
        UNIT_ASSERT_VALUES_EQUAL(
            "H0*{Fresh+,0};"
            "H1+{Disabled,0};"
            "H2-{Operational,32768};"
            "H3+{Disabled,0};"
            "H4*{Fresh+,0};",
            dirtyMap->DebugPrintDDiskState());
    }

    Y_UNIT_TEST(ShouldNotReadFromFresh)
    {
        auto vchunkConfig = MakeTestVChunkConfig();

        vchunkConfig.SetWatermark(THostIndex{0}, 30 * DefaultBlockSize);
        vchunkConfig.SetWatermark(THostIndex{2}, 40 * DefaultBlockSize);

        auto dirtyMap = std::make_shared<TBlocksDirtyMap>(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        UNIT_ASSERT_VALUES_EQUAL(
            "H0*{Fresh+,30};"
            "H1*{Operational,32768};"
            "H2*{Fresh+,40};"
            "H3+{Disabled,0};"
            "H4+{Disabled,0};",
            dirtyMap->DebugPrintDDiskState());

        // Read below fresh watermark
        auto readHint =
            dirtyMap->MakeReadHint(TBlockRange64::WithLength(10, 10));
        UNIT_ASSERT_VALUES_EQUAL(
            "0{[H0,H1,H2][10..19][0..9]};",
            readHint.DebugPrint());

        // Read crossed fresh watermark
        readHint = dirtyMap->MakeReadHint(TBlockRange64::WithLength(25, 10));
        UNIT_ASSERT_VALUES_EQUAL(
            "0{[H1,H2][25..34][0..9]};",
            readHint.DebugPrint());

        // Read above fresh watermark
        readHint = dirtyMap->MakeReadHint(TBlockRange64::WithLength(30, 10));
        UNIT_ASSERT_VALUES_EQUAL(
            "0{[H1,H2][30..39][0..9]};",
            readHint.DebugPrint());

        // Read above fresh watermark
        readHint = dirtyMap->MakeReadHint(TBlockRange64::WithLength(40, 10));
        UNIT_ASSERT_VALUES_EQUAL(
            "0{[H1][40..49][0..9]};",
            readHint.DebugPrint());
    }

    Y_UNIT_TEST(ShouldReadAfterWriteFinished)
    {
        auto vchunkConfig = MakeTestVChunkConfig();
        auto dirtyMap = std::make_shared<TBlocksDirtyMap>(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        dirtyMap->RegisterInflightWrite(123, TBlockRange64::WithLength(10, 10));
        dirtyMap->WriteFinished(
            123,
            TBlockRange64::WithLength(10, 10),
            MakePrimaryHosts(),
            MakePrimaryHosts());

        // After write, we should be able to get read hints (read from
        // confirmed PBuffers — hosts {0,1,2}).
        auto readHint =
            dirtyMap->MakeReadHint(TBlockRange64::WithLength(10, 10));
        UNIT_ASSERT_VALUES_EQUAL(
            "123{[H0,H1,H2][10..19][0..9]};",
            readHint.DebugPrint());

        // Disable host 0.
        vchunkConfig.DisableHost(0);
        dirtyMap->UpdateConfig(vchunkConfig);

        readHint = dirtyMap->MakeReadHint(TBlockRange64::WithLength(10, 10));
        // WriteConfirmed mask is {0,1,2}; host 0 is disabled, so it is
        // excluded from the read mask.
        UNIT_ASSERT_VALUES_EQUAL(
            "123{[H1,H2][10..19][0..9]};",
            readHint.DebugPrint());

        // Counters on primary PBuffers contain one record with 40960 bytes
        for (THostIndex h: MakePrimaryHosts()) {
            auto counters = dirtyMap->GetPBufferCounters(h);
            UNIT_ASSERT_VALUES_EQUAL(1, counters.Current.Count);
            UNIT_ASSERT_VALUES_EQUAL(40960, counters.Current.Size);
            UNIT_ASSERT_VALUES_EQUAL(1, counters.Total.Count);
            UNIT_ASSERT_VALUES_EQUAL(40960, counters.Total.Size);

            UNIT_ASSERT_VALUES_EQUAL(0, counters.CurrentLocked.Count);
            UNIT_ASSERT_VALUES_EQUAL(0, counters.CurrentLocked.Size);
            UNIT_ASSERT_VALUES_EQUAL(0, counters.TotalLocked.Count);
            UNIT_ASSERT_VALUES_EQUAL(0, counters.TotalLocked.Size);
        }
    }

    Y_UNIT_TEST(ShouldReadAfterWriteFinishedFromLastLsn)
    {
        auto vchunkConfig = MakeTestVChunkConfig();
        auto dirtyMap = std::make_shared<TBlocksDirtyMap>(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        dirtyMap->RegisterInflightWrite(123, TBlockRange64::WithLength(10, 10));
        dirtyMap->WriteFinished(
            123,
            TBlockRange64::WithLength(10, 10),
            MakePrimaryHosts(),
            MakePrimaryHosts());

        dirtyMap->RegisterInflightWrite(124, TBlockRange64::WithLength(10, 10));
        dirtyMap->WriteFinished(
            124,
            TBlockRange64::WithLength(10, 10),
            MakeHostMask(true, true, false, true, false),
            MakeHostMask(true, true, false, true, false));

        // After write, we should be able to get read hints
        auto readHint =
            dirtyMap->MakeReadHint(TBlockRange64::WithLength(10, 10));
        UNIT_ASSERT_VALUES_EQUAL(
            "124{[H0,H1,H3][10..19][0..9]};",
            readHint.DebugPrint());

        // Disable host 0
        vchunkConfig.DisableHost(0);
        dirtyMap->UpdateConfig(vchunkConfig);

        readHint = dirtyMap->MakeReadHint(TBlockRange64::WithLength(10, 10));
        UNIT_ASSERT_VALUES_EQUAL(
            "124{[H1,H3][10..19][0..9]};",
            readHint.DebugPrint());

        readHint.RangeHints[0].Lock.Arm();

        {
            // Host 0 contains two records, one locked for read
            auto counters = dirtyMap->GetPBufferCounters(THostIndex{0});
            UNIT_ASSERT_VALUES_EQUAL(2, counters.Current.Count);
            UNIT_ASSERT_VALUES_EQUAL(81920, counters.Current.Size);
            UNIT_ASSERT_VALUES_EQUAL(2, counters.Total.Count);
            UNIT_ASSERT_VALUES_EQUAL(81920, counters.Total.Size);

            UNIT_ASSERT_VALUES_EQUAL(1, counters.CurrentLocked.Count);
            UNIT_ASSERT_VALUES_EQUAL(40960, counters.CurrentLocked.Size);
            UNIT_ASSERT_VALUES_EQUAL(1, counters.TotalLocked.Count);
            UNIT_ASSERT_VALUES_EQUAL(40960, counters.TotalLocked.Size);
        }
        {
            // Host 3 contains one record, one locked for read
            auto counters = dirtyMap->GetPBufferCounters(THostIndex{3});
            UNIT_ASSERT_VALUES_EQUAL(1, counters.Current.Count);
            UNIT_ASSERT_VALUES_EQUAL(40960, counters.Current.Size);
            UNIT_ASSERT_VALUES_EQUAL(1, counters.Total.Count);
            UNIT_ASSERT_VALUES_EQUAL(40960, counters.Total.Size);

            UNIT_ASSERT_VALUES_EQUAL(1, counters.CurrentLocked.Count);
            UNIT_ASSERT_VALUES_EQUAL(40960, counters.CurrentLocked.Size);
            UNIT_ASSERT_VALUES_EQUAL(1, counters.TotalLocked.Count);
            UNIT_ASSERT_VALUES_EQUAL(40960, counters.TotalLocked.Size);
        }
    }

    Y_UNIT_TEST(ShouldWriteAndFlushAndErase)
    {
        const auto vchunkConfig = MakeTestVChunkConfig();
        auto dirtyMap = std::make_shared<TBlocksDirtyMap>(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        // Without write, we should not get flush hints
        auto flushHint = dirtyMap->MakeFlushHint(1);
        UNIT_ASSERT_EQUAL(true, flushHint.Empty());

        const THostMask requested = MakePrimaryHosts();
        const THostMask confirmed = MakePrimaryHosts();

        // Flush commands should be generated after completing the required
        // number of write operations.
        dirtyMap->RegisterInflightWrite(123, TBlockRange64::WithLength(10, 10));
        dirtyMap->WriteFinished(
            123,
            TBlockRange64::WithLength(10, 10),
            requested,
            confirmed);

        // WriteFinished should generate one inflight item
        UNIT_ASSERT_VALUES_EQUAL(1, dirtyMap->GetInflightCount());

        flushHint = dirtyMap->MakeFlushHint(2);
        UNIT_ASSERT_EQUAL(true, flushHint.Empty());

        dirtyMap->RegisterInflightWrite(124, TBlockRange64::WithLength(20, 10));
        dirtyMap->WriteFinished(
            124,
            TBlockRange64::WithLength(20, 10),
            requested,
            confirmed);

        // Second writeFinished should generate one more inflight item
        UNIT_ASSERT_VALUES_EQUAL(2, dirtyMap->GetInflightCount());

        flushHint = dirtyMap->MakeFlushHint(2);
        UNIT_ASSERT_VALUES_EQUAL(
            "H0->H0:123[10..19],124[20..29];"
            "H1->H1:123[10..19],124[20..29];"
            "H2->H2:123[10..19],124[20..29];",
            flushHint.DebugPrint());
        // Erase hints should be generated after completing flushing.
        auto eraseHints = dirtyMap->MakeEraseHint(2);
        UNIT_ASSERT_EQUAL(true, eraseHints.Empty());

        // After getting flush hints, we should not get it once again
        {
            auto flushHint = dirtyMap->MakeFlushHint(2);
            UNIT_ASSERT_EQUAL(true, flushHint.Empty());
        }

        // After getting flushing errors, we should get flush hints again
        dirtyMap->FlushFinished(
            THostRoute{.SourceHostIndex = 0, .DestinationHostIndex = 0},
            {123, 124},
            {});
        dirtyMap->FlushFinished(
            THostRoute{.SourceHostIndex = 1, .DestinationHostIndex = 1},
            {123, 124},
            {});
        dirtyMap->FlushFinished(
            THostRoute{.SourceHostIndex = 2, .DestinationHostIndex = 2},
            {},
            {123, 124});

        flushHint = dirtyMap->MakeFlushHint(2);
        UNIT_ASSERT_EQUAL(false, flushHint.Empty());
        UNIT_ASSERT_VALUES_EQUAL(
            "H2->H2:123[10..19],124[20..29];",
            flushHint.DebugPrint());

        // Complete flushing to third ddisk
        dirtyMap->FlushFinished(
            THostRoute{.SourceHostIndex = 2, .DestinationHostIndex = 2},
            {123, 124},
            {});

        // Erase hints should be generated after completing the required
        // number of write operations.
        eraseHints = dirtyMap->MakeEraseHint(2);
        UNIT_ASSERT_VALUES_EQUAL(
            "H0:0:123,0:124;"
            "H1:0:123,0:124;"
            "H2:0:123,0:124;",
            eraseHints.DebugPrint());

        // After getting erase hints, we should not get it once again
        {
            auto eraseHint = dirtyMap->MakeEraseHint(2);
            UNIT_ASSERT_EQUAL(true, eraseHint.Empty());
        }

        // After getting erasing errors, we should get erase hints again
        dirtyMap->EraseFinished(THostIndex{0}, {123, 124}, {});
        dirtyMap->EraseFinished(THostIndex{1}, {123, 124}, {});
        dirtyMap->EraseFinished(THostIndex{2}, {}, {123, 124});

        eraseHints = dirtyMap->MakeEraseHint(2);
        UNIT_ASSERT_VALUES_EQUAL("H2:0:123,0:124;", eraseHints.DebugPrint());

        // Should still have two inflight items
        UNIT_ASSERT_VALUES_EQUAL(2, dirtyMap->GetInflightCount());

        // Complete erasing from third pbuffer
        dirtyMap->EraseFinished(THostIndex{2}, {123, 124}, {});
        eraseHints = dirtyMap->MakeEraseHint(2);
        UNIT_ASSERT_EQUAL(true, eraseHints.Empty());

        // Should remove inflight items
        UNIT_ASSERT_VALUES_EQUAL(0, dirtyMap->GetInflightCount());

        // All current counters back to zero.
        for (THostIndex h: MakePrimaryHosts()) {
            auto counters = dirtyMap->GetPBufferCounters(h);
            UNIT_ASSERT_VALUES_EQUAL(0, counters.Current.Count);
            UNIT_ASSERT_VALUES_EQUAL(0, counters.Current.Size);
            UNIT_ASSERT_VALUES_EQUAL(2, counters.Total.Count);
            UNIT_ASSERT_VALUES_EQUAL(81920, counters.Total.Size);
        }
    }

    Y_UNIT_TEST(ShouldReportSafeBarrierForErase)
    {
        const auto vchunkConfig = MakeTestVChunkConfig();
        auto dirtyMap = std::make_shared<TBlocksDirtyMap>(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        const THostMask requested = MakePrimaryHosts();
        const THostMask confirmed = MakePrimaryHosts();
        const auto range1 = TBlockRange64::WithLength(10, 10);
        const auto range2 = TBlockRange64::WithLength(20, 10);

        // No inflight writes mean no safe barrier.
        UNIT_ASSERT(!dirtyMap->GetSafeBarrierForErase().has_value());

        // A write counts towards the barrier from the moment it is registered
        // (pending), before any PBuffer acknowledges it.
        dirtyMap->RegisterInflightWrite(123, range1);
        UNIT_ASSERT_VALUES_EQUAL(123, *dirtyMap->GetSafeBarrierForErase());

        // The barrier tracks the minimum inflight lsn.
        dirtyMap->RegisterInflightWrite(124, range2);
        UNIT_ASSERT_VALUES_EQUAL(123, *dirtyMap->GetSafeBarrierForErase());

        // The lsn stays inflight through the written and flushed states, so the
        // barrier does not advance past a not-yet-erased write.
        dirtyMap->WriteFinished(123, range1, requested, confirmed);
        dirtyMap->WriteFinished(124, range2, requested, confirmed);
        UNIT_ASSERT_VALUES_EQUAL(123, *dirtyMap->GetSafeBarrierForErase());

        auto flushHint = dirtyMap->MakeFlushHint(2);
        UNIT_ASSERT(!flushHint.Empty());
        UNIT_ASSERT_VALUES_EQUAL(123, *dirtyMap->GetSafeBarrierForErase());
        FlushAll(flushHint, *dirtyMap);
        UNIT_ASSERT_VALUES_EQUAL(123, *dirtyMap->GetSafeBarrierForErase());

        // Erasing lsn 123 from only a sub-quorum of hosts keeps it inflight, so
        // the barrier is still held at 123.
        auto eraseHint = dirtyMap->MakeEraseHint(2);
        UNIT_ASSERT(!eraseHint.Empty());
        dirtyMap->EraseFinished(THostIndex{0}, {123}, {});
        dirtyMap->EraseFinished(THostIndex{1}, {123}, {});
        UNIT_ASSERT_VALUES_EQUAL(123, *dirtyMap->GetSafeBarrierForErase());

        // Once 123 is erased everywhere it leaves the inflight map and the
        // barrier advances to 124.
        dirtyMap->EraseFinished(THostIndex{2}, {123}, {});
        UNIT_ASSERT_VALUES_EQUAL(124, *dirtyMap->GetSafeBarrierForErase());

        // Erasing 124 everywhere drains the map -> no barrier.
        dirtyMap->EraseFinished(THostIndex{0}, {124}, {});
        dirtyMap->EraseFinished(THostIndex{1}, {124}, {});
        dirtyMap->EraseFinished(THostIndex{2}, {124}, {});
        UNIT_ASSERT(!dirtyMap->GetSafeBarrierForErase().has_value());
        UNIT_ASSERT_VALUES_EQUAL(0, dirtyMap->GetInflightCount());
    }

    Y_UNIT_TEST(ShouldNotHoldSafeBarrierForSubQuorumWrite)
    {
        const auto vchunkConfig = MakeTestVChunkConfig();
        auto dirtyMap = std::make_shared<TBlocksDirtyMap>(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        const auto range = TBlockRange64::WithLength(10, 10);

        // A registered (pending) write holds the barrier.
        dirtyMap->RegisterInflightWrite(123, range);
        UNIT_ASSERT_VALUES_EQUAL(123, *dirtyMap->GetSafeBarrierForErase());

        // A write that fails to reach a quorum of PBuffers drops its pending
        // entry and stops holding the barrier.
        dirtyMap->WriteFinished(
            123,
            range,
            MakePrimaryHosts(),
            MakeHostMask(true, true, false, false, false));   // 2 < quorum 3
        UNIT_ASSERT(!dirtyMap->GetSafeBarrierForErase().has_value());
        UNIT_ASSERT_VALUES_EQUAL(0, dirtyMap->GetInflightCount());
    }

    // A late erase response for a record that already left the inflight map
    // must be a no-op, not a crash of the tablet. Covers both a late success
    // and a late failure for a forgotten lsn; the sequence that leads to this
    // state in production is pinned by
    // ShouldIgnoreLateEraseAckAfterHostDisabled below.
    Y_UNIT_TEST(ShouldIgnoreLateEraseAckForForgottenLsn)
    {
        const auto vchunkConfig = MakeTestVChunkConfig();
        auto dirtyMap = std::make_shared<TBlocksDirtyMap>(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        const auto range = TBlockRange64::WithLength(10, 10);
        dirtyMap->RegisterInflightWrite(100, range);
        dirtyMap
            ->WriteFinished(100, range, MakePrimaryHosts(), MakePrimaryHosts());

        auto flushHint = dirtyMap->MakeFlushHint(1);
        UNIT_ASSERT(!flushHint.Empty());
        FlushAll(flushHint, *dirtyMap);

        auto eraseHints = dirtyMap->MakeEraseHint(1);
        UNIT_ASSERT(!eraseHints.Empty());
        EraseAll(eraseHints, *dirtyMap);
        UNIT_ASSERT_VALUES_EQUAL(0, dirtyMap->GetInflightCount());

        // A late success and a late failure for the forgotten lsn: the
        // record is long gone, both must be no-ops.
        dirtyMap->EraseFinished(THostIndex{2}, {100}, {});
        dirtyMap->EraseFinished(THostIndex{2}, {}, {100});
        UNIT_ASSERT_VALUES_EQUAL(0, dirtyMap->GetInflightCount());
    }

    // The sequence that forgets a record while its host still owes a
    // response: the erase on one host does not complete in time, the host
    // gets disabled and the record is dropped from tracking, and only then
    // the genuine response from that host arrives. It must be a no-op.
    Y_UNIT_TEST(ShouldIgnoreLateEraseAckAfterHostDisabled)
    {
        auto vchunkConfig = MakeTestVChunkConfig();
        auto dirtyMap = std::make_shared<TBlocksDirtyMap>(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        const auto range = TBlockRange64::WithLength(10, 10);
        dirtyMap->RegisterInflightWrite(100, range);
        dirtyMap
            ->WriteFinished(100, range, MakePrimaryHosts(), MakePrimaryHosts());

        auto flushHint = dirtyMap->MakeFlushHint(1);
        UNIT_ASSERT(!flushHint.Empty());
        FlushAll(flushHint, *dirtyMap);

        auto eraseHints = dirtyMap->MakeEraseHint(1);
        UNIT_ASSERT(!eraseHints.Empty());
        dirtyMap->EraseFinished(THostIndex{0}, {100}, {});
        dirtyMap->EraseFinished(THostIndex{1}, {100}, {});
        // The erase on host 2 does not complete in time: the request is
        // marked failed and re-queued.
        dirtyMap->EraseFinished(THostIndex{2}, {}, {100});
        UNIT_ASSERT_VALUES_EQUAL(1, dirtyMap->GetInflightCount());

        // The host gets disabled; the re-queued erase is confirmed on its
        // behalf and the record leaves the inflight map.
        vchunkConfig.DisableHost(2);
        dirtyMap->UpdateConfig(vchunkConfig);
        auto retryHints = dirtyMap->MakeEraseHint(1);
        UNIT_ASSERT(retryHints.Empty());
        UNIT_ASSERT_VALUES_EQUAL(0, dirtyMap->GetInflightCount());

        // The genuine response from the disabled host finally arrives.
        dirtyMap->EraseFinished(THostIndex{2}, {100}, {});
        UNIT_ASSERT_VALUES_EQUAL(0, dirtyMap->GetInflightCount());
    }

    Y_UNIT_TEST(ShouldWriteAndFlushAndEraseWhenAdditionalHandOffDesired)
    {
        auto vchunkConfig = MakeTestVChunkConfig();
        auto dirtyMap = std::make_shared<TBlocksDirtyMap>(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        // Promote hand-off H3 to primary.
        vchunkConfig.PromoteHost(3);
        vchunkConfig.SetWatermark(3, DefaultBlockSize * 1024);
        dirtyMap->UpdateConfig(vchunkConfig);
        UNIT_ASSERT_VALUES_EQUAL(
            "H0*{Operational,32768};"
            "H1*{Operational,32768};"
            "H2*{Operational,32768};"
            "H3*{Fresh+,1024};"
            "H4+{Disabled,0};",
            dirtyMap->DebugPrintDDiskState());

        // Written to 2 primary and 1 hand-off
        const THostMask requested =
            MakeHostMask(false, true, true, true, false);
        const THostMask confirmed = requested;

        dirtyMap->RegisterInflightWrite(123, TBlockRange64::WithLength(10, 10));
        dirtyMap->WriteFinished(
            123,
            TBlockRange64::WithLength(10, 10),
            requested,
            confirmed);

        auto flushHint = dirtyMap->MakeFlushHint(1);
        UNIT_ASSERT_VALUES_EQUAL(
            "H1->H0:123[10..19];"   // Cross-node
            "H1->H1:123[10..19];"
            "H2->H2:123[10..19];"
            "H3->H3:123[10..19];",
            flushHint.DebugPrint());
        FlushAll(flushHint, *dirtyMap);

        // Erase hints
        auto eraseHints = dirtyMap->MakeEraseHint(1);
        UNIT_ASSERT_VALUES_EQUAL(
            "H1:0:123;"
            "H2:0:123;"
            "H3:0:123;",
            eraseHints.DebugPrint());
        EraseAll(eraseHints, *dirtyMap);
    }

    // A Fresh DDisk has range tracking enabled. When a write is flushed to it,
    // FlushCompleted must propagate the completion down to the DDisk state so
    // the flushed range is recorded in the DDisk's Ahead field (data that is
    // already up-to-date above the operational watermark and needs no sync).
    // Operational DDisks have tracking disabled, so they record nothing.
    Y_UNIT_TEST(ShouldTrackAheadRangeOnFreshDDiskAfterFlush)
    {
        auto vchunkConfig = MakeTestVChunkConfig();
        auto dirtyMap = std::make_shared<TBlocksDirtyMap>(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        // Promote hand-off H3 to primary and make it Fresh with a low
        // watermark so tracking is enabled and writes above the watermark are
        // recorded as "ahead".
        vchunkConfig.PromoteHost(3);
        vchunkConfig.SetWatermark(3, DefaultBlockSize * 5);
        dirtyMap->UpdateConfig(vchunkConfig);
        UNIT_ASSERT_VALUES_EQUAL(
            "H0*{Operational,32768};"
            "H1*{Operational,32768};"
            "H2*{Operational,32768};"
            "H3*{Fresh+,5};"
            "H4+{Disabled,0};",
            dirtyMap->DebugPrintDDiskState());

        // Nothing tracked before the flush.
        UNIT_ASSERT_VALUES_EQUAL("", dirtyMap->DebugPrintAhead());
        UNIT_ASSERT_VALUES_EQUAL("", dirtyMap->DebugPrintBehind());

        // Write above the fresh watermark to all four DDisks.
        const THostMask requested = MakeHostMask(true, true, true, true, false);
        dirtyMap->RegisterInflightWrite(123, TBlockRange64::WithLength(10, 10));
        dirtyMap->WriteFinished(
            123,
            TBlockRange64::WithLength(10, 10),
            requested,
            requested);

        // Finish flushes to every DDisk.
        auto flushHint = dirtyMap->MakeFlushHint(1);
        UNIT_ASSERT_EQUAL(false, flushHint.Empty());
        FlushAll(flushHint, *dirtyMap);

        // FlushCompleted recorded the flushed range in the Fresh DDisk's Ahead
        // field. Only the Fresh host H3 tracks; the Operational hosts do not.
        UNIT_ASSERT_VALUES_EQUAL(
            "  H3: [10..19]\n",
            dirtyMap->DebugPrintAhead());
        UNIT_ASSERT_VALUES_EQUAL("", dirtyMap->DebugPrintBehind());

        // Drain erases so the inflight map ends clean.
        auto eraseHints = dirtyMap->MakeEraseHint(1);
        EraseAll(eraseHints, *dirtyMap);
        UNIT_ASSERT_VALUES_EQUAL(0, dirtyMap->GetInflightCount());
    }

    Y_UNIT_TEST(ShouldWriteAndFlushAndEraseWithOneDisabled)
    {
        auto vchunkConfig = MakeTestVChunkConfig();

        // Host 0 disabled, hosts 1,2,3 primary, host 4 hand-off.
        vchunkConfig.PromoteHost(3);
        TString error;
        vchunkConfig.EvacuateHost(0);
        UNIT_ASSERT_VALUES_EQUAL("", error);
        vchunkConfig.SetWatermark(3, DefaultBlockSize * 1024);

        auto dirtyMap = std::make_shared<TBlocksDirtyMap>(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        // Written to two primary and one hand-off
        const THostMask requested =
            MakeHostMask(false, true, true, true, false);
        const THostMask confirmed = requested;

        dirtyMap->RegisterInflightWrite(123, TBlockRange64::WithLength(10, 10));
        dirtyMap->WriteFinished(
            123,
            TBlockRange64::WithLength(10, 10),
            requested,
            confirmed);

        auto flushHint = dirtyMap->MakeFlushHint(1);
        UNIT_ASSERT_VALUES_EQUAL(
            "H1->H1:123[10..19];"
            "H1->H4:123[10..19];"   // ???
            "H2->H2:123[10..19];"
            "H3->H3:123[10..19];",
            flushHint.DebugPrint());
        FlushAll(flushHint, *dirtyMap);

        // Erase hints
        auto eraseHints = dirtyMap->MakeEraseHint(1);
        UNIT_ASSERT_VALUES_EQUAL(
            "H1:0:123;"
            "H2:0:123;"
            "H3:0:123;",
            eraseHints.DebugPrint());
        EraseAll(eraseHints, *dirtyMap);
    }

    Y_UNIT_TEST(ShouldWriteAndFlushAndEraseWithTwoDisabled)
    {
        auto vchunkConfig = MakeTestVChunkConfig();

        // Hosts 0,1 disabled; hosts 2,3,4 are primary.
        TString error;
        vchunkConfig.EvacuateHost(0);
        UNIT_ASSERT_VALUES_EQUAL("", error);
        vchunkConfig.EvacuateHost(1);
        UNIT_ASSERT_VALUES_EQUAL("", error);
        vchunkConfig.SetWatermark(3, DefaultBlockSize * 1024);
        vchunkConfig.SetWatermark(4, DefaultBlockSize * 1024);

        auto dirtyMap = std::make_shared<TBlocksDirtyMap>(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);
        UNIT_ASSERT_VALUES_EQUAL(
            "H0-{Disabled,0};"
            "H1-{Disabled,0};"
            "H2*{Operational,32768};"
            "H3*{Fresh+,1024};"
            "H4*{Fresh+,1024};",
            dirtyMap->DebugPrintDDiskState());

        // Written to one primary and two hand-off
        const THostMask requested =
            MakeHostMask(false, false, true, true, true);
        const THostMask confirmed = requested;

        dirtyMap->RegisterInflightWrite(123, TBlockRange64::WithLength(10, 10));
        dirtyMap->WriteFinished(
            123,
            TBlockRange64::WithLength(10, 10),
            requested,
            confirmed);

        auto flushHint = dirtyMap->MakeFlushHint(1);
        UNIT_ASSERT_VALUES_EQUAL(
            "H2->H2:123[10..19];"
            "H3->H3:123[10..19];"
            "H4->H4:123[10..19];",
            flushHint.DebugPrint());
        FlushAll(flushHint, *dirtyMap);

        // Erase hints
        auto eraseHints = dirtyMap->MakeEraseHint(1);
        UNIT_ASSERT_VALUES_EQUAL(
            "H2:0:123;"
            "H3:0:123;"
            "H4:0:123;",
            eraseHints.DebugPrint());
        EraseAll(eraseHints, *dirtyMap);
    }

    Y_UNIT_TEST(ShouldNotFlushAndEraseFromDisabled)
    {
        auto vchunkConfig = MakeTestVChunkConfig();
        // Host 0 disabled; hosts 1,2,3 primary; host 4 hand-off.
        vchunkConfig.EvacuateHost(0);
        vchunkConfig.SetWatermark(3, DefaultBlockSize * 1024);

        auto dirtyMap = std::make_shared<TBlocksDirtyMap>(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);
        UNIT_ASSERT_VALUES_EQUAL(
            "H0-{Disabled,0};"
            "H1*{Operational,32768};"
            "H2*{Operational,32768};"
            "H3*{Fresh+,1024};"
            "H4+{Disabled,0};",
            dirtyMap->DebugPrintDDiskState());

        // Written to all 3 primary PBuffers (hosts 0,1,2). Host 0 is disabled,
        // but the data is still on its PBuffer.
        const THostMask requested =
            MakeHostMask(true, true, true, false, false);
        const THostMask confirmed = requested;

        dirtyMap->RegisterInflightWrite(123, TBlockRange64::WithLength(10, 10));
        dirtyMap->WriteFinished(
            123,
            TBlockRange64::WithLength(10, 10),
            requested,
            confirmed);

        auto flushHint = dirtyMap->MakeFlushHint(1);
        UNIT_ASSERT_VALUES_EQUAL(
            "H1->H1:123[10..19];"
            "H1->H3:123[10..19];"
            "H2->H2:123[10..19];",
            flushHint.DebugPrint());
        FlushAll(flushHint, *dirtyMap);

        // Erase hints
        auto eraseHints = dirtyMap->MakeEraseHint(1);
        UNIT_ASSERT_VALUES_EQUAL(
            "H1:0:123;"
            "H2:0:123;",
            eraseHints.DebugPrint());
        EraseAll(eraseHints, *dirtyMap);
        // Should remove inflight items
        UNIT_ASSERT_VALUES_EQUAL(0, dirtyMap->GetInflightCount());
    }

    Y_UNIT_TEST(ShouldFlushOverWriteWatermark)
    {
        auto vchunkConfig = MakeTestVChunkConfig();
        // Disable DDisks H2
        // Promote DDisks H3 (hosts 0,1,2,3 primary)
        // Available DDisks is enough for a quorum.
        vchunkConfig.DisableHost(2);
        vchunkConfig.PromoteHost(3);
        vchunkConfig.SetWatermark(3, 100);
        auto dirtyMap = std::make_shared<TBlocksDirtyMap>(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        const THostMask requested =
            MakeHostMask(true, true, false, true, false);
        const THostMask confirmed = requested;

        // Range below write watermark. Should be flushed to 3 enabled ddisks.
        dirtyMap->RegisterInflightWrite(123, TBlockRange64::WithLength(10, 10));
        dirtyMap->WriteFinished(
            123,
            TBlockRange64::WithLength(10, 10),
            requested,
            confirmed);
        // Range cross write watermark. Should be flushed to 3 enabled ddisks.
        dirtyMap->RegisterInflightWrite(124, TBlockRange64::WithLength(95, 10));
        dirtyMap->WriteFinished(
            124,
            TBlockRange64::WithLength(95, 10),
            requested,
            confirmed);
        // Range over write watermark. Should be flushed to 3 enabled ddisks.
        dirtyMap->RegisterInflightWrite(
            125,
            TBlockRange64::WithLength(100, 10));
        dirtyMap->WriteFinished(
            125,
            TBlockRange64::WithLength(100, 10),
            requested,
            confirmed);

        auto flushHint = dirtyMap->MakeFlushHint(3);
        UNIT_ASSERT_VALUES_EQUAL(
            "H0->H0:123[10..19],124[95..104],125[100..109];"
            "H1->H1:123[10..19],124[95..104],125[100..109];"
            "H3->H3:123[10..19],124[95..104],125[100..109];",
            flushHint.DebugPrint());
    }

    Y_UNIT_TEST(ShouldLockPBuffer)
    {
        const auto vchunkConfig = MakeTestVChunkConfig();
        auto dirtyMap = std::make_shared<TBlocksDirtyMap>(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        dirtyMap->RegisterInflightWrite(123, TBlockRange64::WithLength(10, 10));
        dirtyMap->WriteFinished(
            123,
            TBlockRange64::WithLength(10, 10),
            MakePrimaryHosts(),
            MakePrimaryHosts());

        auto flushHint = dirtyMap->MakeFlushHint(1);
        UNIT_ASSERT_EQUAL(false, flushHint.Empty());
        FlushAll(flushHint, *dirtyMap);

        // Lock pbuffer
        dirtyMap->LockPBuffer(123);

        // Erase hints should not be generated when PBuffer is locked.
        auto eraseHints = dirtyMap->MakeEraseHint(1);
        UNIT_ASSERT_EQUAL(true, eraseHints.Empty());

        // UnLock pbuffer
        dirtyMap->UnlockPBuffer(123);

        // Erase hints should be generated when PBuffer is unlocked.
        eraseHints = dirtyMap->MakeEraseHint(1);
        UNIT_ASSERT_EQUAL(false, eraseHints.Empty());
    }

    Y_UNIT_TEST(ShouldLockDDisk)
    {
        const auto vchunkConfig = MakeTestVChunkConfig();
        auto dirtyMap = std::make_shared<TBlocksDirtyMap>(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);
        const THostMask mask = MakePrimaryHosts();

        // Lock range on DDisk (for reading).
        auto lockHandle =
            dirtyMap->LockDDiskRange(TBlockRange64::WithLength(5, 10), mask);

        // User write to overlapped with locked range.
        dirtyMap->RegisterInflightWrite(123, TBlockRange64::WithLength(10, 10));
        dirtyMap->WriteFinished(
            123,
            TBlockRange64::WithLength(10, 10),
            MakePrimaryHosts(),
            MakePrimaryHosts());

        // Flush hints should not be generated when DDisk is locked.
        auto flushHint = dirtyMap->MakeFlushHint(1);
        UNIT_ASSERT_EQUAL(true, flushHint.Empty());

        // Unlock DDisk
        dirtyMap->UnLockDDiskRange(lockHandle);

        // FLush hints should be generated after DDisk is unlocked.
        auto eraseHints = dirtyMap->MakeEraseHint(1);
        UNIT_ASSERT_EQUAL(true, eraseHints.Empty());
    }

    Y_UNIT_TEST(ShouldRestoreCompletePBuffer)
    {
        const auto vchunkConfig = MakeTestVChunkConfig();
        auto dirtyMap = std::make_shared<TBlocksDirtyMap>(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        dirtyMap->RestorePBuffer(
            123,
            TBlockRange64::WithLength(10, 10),
            THostIndex{0});
        dirtyMap->RestorePBuffer(
            123,
            TBlockRange64::WithLength(10, 10),
            THostIndex{1});
        dirtyMap->RestorePBuffer(
            123,
            TBlockRange64::WithLength(10, 10),
            THostIndex{2});

        // Flush hints should be generated when has quorum PBuffers.
        auto flushHint = dirtyMap->MakeFlushHint(1);
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
        auto dirtyMap = std::make_shared<TBlocksDirtyMap>(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        // Block written to four PBuffers
        dirtyMap->RestorePBuffer(
            123,
            TBlockRange64::WithLength(10, 10),
            THostIndex{0});
        dirtyMap->RestorePBuffer(
            123,
            TBlockRange64::WithLength(10, 10),
            THostIndex{1});
        dirtyMap->RestorePBuffer(
            123,
            TBlockRange64::WithLength(10, 10),
            THostIndex{2});
        dirtyMap->RestorePBuffer(
            123,
            TBlockRange64::WithLength(10, 10),
            THostIndex{3});

        // Flush hints should be generated when has quorum PBuffers.
        auto flushHint = dirtyMap->MakeFlushHint(1);
        UNIT_ASSERT_EQUAL(false, flushHint.Empty());

        UNIT_ASSERT_VALUES_EQUAL(
            "H0->H0:123[10..19];"
            "H1->H1:123[10..19];"
            "H2->H2:123[10..19];",
            flushHint.DebugPrint());

        auto readHint =
            dirtyMap->MakeReadHint(TBlockRange64::WithLength(10, 10));
        UNIT_ASSERT_VALUES_EQUAL(
            "123{[H0,H1,H2,H3][10..19][0..9]};",
            readHint.DebugPrint());

        for (THostIndex h:
             {THostIndex{0}, THostIndex{1}, THostIndex{2}, THostIndex{3}})
        {
            auto counters = dirtyMap->GetPBufferCounters(h);
            UNIT_ASSERT_VALUES_EQUAL(1, counters.Current.Count);
            UNIT_ASSERT_VALUES_EQUAL(40960, counters.Current.Size);
            UNIT_ASSERT_VALUES_EQUAL(1, counters.Total.Count);
            UNIT_ASSERT_VALUES_EQUAL(40960, counters.Total.Size);
        }
    }

    Y_UNIT_TEST(ShouldFlushFromHandOff)
    {
        const auto vchunkConfig = MakeTestVChunkConfig();
        auto dirtyMap = std::make_shared<TBlocksDirtyMap>(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        // Block written to two primary PBuffers and one hand-off PBuffer
        dirtyMap->RestorePBuffer(
            123,
            TBlockRange64::WithLength(10, 10),
            THostIndex{1});
        dirtyMap->RestorePBuffer(
            123,
            TBlockRange64::WithLength(10, 10),
            THostIndex{2});
        dirtyMap->RestorePBuffer(
            123,
            TBlockRange64::WithLength(10, 10),
            THostIndex{3});

        // Flush hints should be generated when has quorum PBuffers.
        auto flushHint = dirtyMap->MakeFlushHint(1);
        UNIT_ASSERT_EQUAL(false, flushHint.Empty());

        UNIT_ASSERT_VALUES_EQUAL(
            "H1->H0:123[10..19];"
            "H1->H1:123[10..19];"
            "H2->H2:123[10..19];",
            flushHint.DebugPrint());

        auto readHint =
            dirtyMap->MakeReadHint(TBlockRange64::WithLength(10, 10));
        UNIT_ASSERT_VALUES_EQUAL(
            "123{[H1,H2,H3][10..19][0..9]};",
            readHint.DebugPrint());
    }

    Y_UNIT_TEST(ShouldReadFromDDiskIfRangeIsNotCoveredByInflightRange)
    {
        const auto vchunkConfig = MakeTestVChunkConfig();
        auto dirtyMap = std::make_shared<TBlocksDirtyMap>(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        dirtyMap->RegisterInflightWrite(123, TBlockRange64::WithLength(0, 100));
        dirtyMap->WriteFinished(
            123,
            TBlockRange64::WithLength(0, 100),
            MakePrimaryHosts(),
            MakePrimaryHosts());

        auto flushHint = dirtyMap->MakeFlushHint(1);
        UNIT_ASSERT_VALUES_EQUAL(
            "H0->H0:123[0..99];"
            "H1->H1:123[0..99];"
            "H2->H2:123[0..99];",
            flushHint.DebugPrint());
        FlushAll(flushHint, *dirtyMap);

        auto eraseHints = dirtyMap->MakeEraseHint(1);
        UNIT_ASSERT_VALUES_EQUAL(
            "H0:0:123;"
            "H1:0:123;"
            "H2:0:123;",
            eraseHints.DebugPrint());

        dirtyMap->EraseFinished(THostIndex{0}, {123}, {});

        dirtyMap->RegisterInflightWrite(124, TBlockRange64::WithLength(10, 10));
        dirtyMap->WriteFinished(
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
            dirtyMap->MakeReadHint(TBlockRange64::WithLength(0, 100));
        UNIT_ASSERT_VALUES_EQUAL(
            "0{[H0,H1,H2][0..9][0..9]};"
            "124{[H0,H1,H2][10..19][10..19]};"
            "0{[H0,H1,H2][20..99][20..99]};",
            readHint.DebugPrint());
    }

    Y_UNIT_TEST(ReadShouldWaitPBufferRestore)
    {
        const auto vchunkConfig = MakeTestVChunkConfig();
        auto dirtyMap = std::make_shared<TBlocksDirtyMap>(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        dirtyMap->RestorePBuffer(
            123,
            TBlockRange64::WithLength(10, 10),
            THostIndex{0});
        auto readHint1 =
            dirtyMap->MakeReadHint(TBlockRange64::WithLength(10, 10));
        UNIT_ASSERT_VALUES_EQUAL("WaitReady:NotReady", readHint1.DebugPrint());
        UNIT_ASSERT_VALUES_EQUAL(false, readHint1.WaitReady.IsReady());

        dirtyMap->RestorePBuffer(
            123,
            TBlockRange64::WithLength(10, 10),
            THostIndex{1});
        auto readHint2 =
            dirtyMap->MakeReadHint(TBlockRange64::WithLength(10, 10));
        UNIT_ASSERT_VALUES_EQUAL("WaitReady:NotReady", readHint2.DebugPrint());
        UNIT_ASSERT_VALUES_EQUAL(false, readHint2.WaitReady.IsReady());

        dirtyMap->RestorePBuffer(
            123,
            TBlockRange64::WithLength(10, 10),
            THostIndex{2});
        auto readHint3 =
            dirtyMap->MakeReadHint(TBlockRange64::WithLength(10, 10));
        UNIT_ASSERT_VALUES_EQUAL(
            "123{[H0,H1,H2][10..19][0..9]};",
            readHint3.DebugPrint());

        UNIT_ASSERT_VALUES_EQUAL(true, readHint1.WaitReady.IsReady());
        UNIT_ASSERT_VALUES_EQUAL(true, readHint2.WaitReady.IsReady());
    }

    Y_UNIT_TEST(ShouldReadHintsTwoSequentialNonOverlappingInflightRanges)
    {
        const auto vchunkConfig = MakeTestVChunkConfig();
        auto dirtyMap = std::make_shared<TBlocksDirtyMap>(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        dirtyMap->RegisterInflightWrite(100, TBlockRange64::WithLength(10, 10));
        dirtyMap->WriteFinished(
            100,
            TBlockRange64::WithLength(10, 10),
            MakePrimaryHosts(),
            MakePrimaryHosts());

        dirtyMap->RegisterInflightWrite(200, TBlockRange64::WithLength(30, 10));
        dirtyMap->WriteFinished(
            200,
            TBlockRange64::WithLength(30, 10),
            MakePrimaryHosts(),
            MakePrimaryHosts());

        auto readHint =
            dirtyMap->MakeReadHint(TBlockRange64::WithLength(0, 50));

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
        auto dirtyMap = std::make_shared<TBlocksDirtyMap>(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        dirtyMap->RegisterInflightWrite(100, TBlockRange64::WithLength(10, 41));
        dirtyMap->WriteFinished(
            100,
            TBlockRange64::WithLength(10, 41),
            MakePrimaryHosts(),
            MakePrimaryHosts());

        dirtyMap->RegisterInflightWrite(200, TBlockRange64::WithLength(20, 11));
        dirtyMap->WriteFinished(
            200,
            TBlockRange64::WithLength(20, 11),
            MakePrimaryHosts(),
            MakePrimaryHosts());

        auto readHint =
            dirtyMap->MakeReadHint(TBlockRange64::WithLength(10, 41));

        UNIT_ASSERT_VALUES_EQUAL(3, readHint.RangeHints.size());
        UNIT_ASSERT_VALUES_EQUAL(
            "100{[H0,H1,H2][10..19][0..9]};"
            "200{[H0,H1,H2][20..30][10..20]};"
            "100{[H0,H1,H2][31..50][21..40]};",
            readHint.DebugPrint());

        dirtyMap->RegisterInflightWrite(300, TBlockRange64::WithLength(0, 50));
        dirtyMap->WriteFinished(
            300,
            TBlockRange64::WithLength(0, 50),
            MakePrimaryHosts(),
            MakePrimaryHosts());
        readHint = dirtyMap->MakeReadHint(TBlockRange64::WithLength(5, 40));

        UNIT_ASSERT_VALUES_EQUAL(1, readHint.RangeHints.size());
        UNIT_ASSERT_VALUES_EQUAL(
            "300{[H0,H1,H2][5..44][0..39]};",
            readHint.DebugPrint());
    }

    Y_UNIT_TEST(ShouldReadHintsTwoPartiallyOverlappingInflightRanges)
    {
        const auto vchunkConfig = MakeTestVChunkConfig();
        auto dirtyMap = std::make_shared<TBlocksDirtyMap>(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        dirtyMap->RegisterInflightWrite(100, TBlockRange64::WithLength(10, 21));
        dirtyMap->WriteFinished(
            100,
            TBlockRange64::WithLength(10, 21),
            MakePrimaryHosts(),
            MakePrimaryHosts());

        dirtyMap->RegisterInflightWrite(200, TBlockRange64::WithLength(25, 21));
        dirtyMap->WriteFinished(
            200,
            TBlockRange64::WithLength(25, 21),
            MakePrimaryHosts(),
            MakePrimaryHosts());

        auto readHint =
            dirtyMap->MakeReadHint(TBlockRange64::WithLength(10, 36));

        UNIT_ASSERT_VALUES_EQUAL(2, readHint.RangeHints.size());
        UNIT_ASSERT_VALUES_EQUAL(
            "100{[H0,H1,H2][10..24][0..14]};"
            "200{[H0,H1,H2][25..45][15..35]};",
            readHint.DebugPrint());
    }

    Y_UNIT_TEST(ShouldReadHintsThreeOverlappingInflightRanges)
    {
        const auto vchunkConfig = MakeTestVChunkConfig();
        auto dirtyMap = std::make_shared<TBlocksDirtyMap>(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        dirtyMap->RegisterInflightWrite(100, TBlockRange64::WithLength(10, 41));
        dirtyMap->WriteFinished(
            100,
            TBlockRange64::WithLength(10, 41),
            MakePrimaryHosts(),
            MakePrimaryHosts());

        dirtyMap->RegisterInflightWrite(150, TBlockRange64::WithLength(20, 21));
        dirtyMap->WriteFinished(
            150,
            TBlockRange64::WithLength(20, 21),
            MakePrimaryHosts(),
            MakePrimaryHosts());

        dirtyMap->RegisterInflightWrite(200, TBlockRange64::WithLength(30, 6));
        dirtyMap->WriteFinished(
            200,
            TBlockRange64::WithLength(30, 6),
            MakePrimaryHosts(),
            MakePrimaryHosts());

        auto readHint =
            dirtyMap->MakeReadHint(TBlockRange64::WithLength(10, 41));

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
        auto dirtyMap = std::make_shared<TBlocksDirtyMap>(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        dirtyMap->RegisterInflightWrite(100, TBlockRange64::WithLength(10, 10));
        dirtyMap->WriteFinished(
            100,
            TBlockRange64::WithLength(10, 10),
            MakePrimaryHosts(),
            MakePrimaryHosts());

        auto readHint =
            dirtyMap->MakeReadHint(TBlockRange64::WithLength(10, 10));

        UNIT_ASSERT_VALUES_EQUAL(1, readHint.RangeHints.size());
        UNIT_ASSERT_VALUES_EQUAL(
            "100{[H0,H1,H2][10..19][0..9]};",
            readHint.DebugPrint());
    }

    Y_UNIT_TEST(ShouldReadHintsRangeWithSameStart)
    {
        const auto vchunkConfig = MakeTestVChunkConfig();
        auto dirtyMap = std::make_shared<TBlocksDirtyMap>(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        dirtyMap->RegisterInflightWrite(
            100,
            TBlockRange64::WithLength(10, 100));
        dirtyMap->WriteFinished(
            100,
            TBlockRange64::WithLength(10, 100),
            MakePrimaryHosts(),
            MakePrimaryHosts());

        dirtyMap->RegisterInflightWrite(200, TBlockRange64::WithLength(10, 40));
        dirtyMap->WriteFinished(
            200,
            TBlockRange64::WithLength(10, 40),
            MakePrimaryHosts(),
            MakePrimaryHosts());

        auto readHint =
            dirtyMap->MakeReadHint(TBlockRange64::WithLength(0, 100));

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
        auto dirtyMap = std::make_shared<TBlocksDirtyMap>(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        const int lsnsCount = 100;
        for (int i = 1; i <= lsnsCount; ++i) {
            dirtyMap->RegisterInflightWrite(i, TBlockRange64::WithLength(i, 1));
            dirtyMap->WriteFinished(
                i,
                TBlockRange64::WithLength(i, 1),
                MakePrimaryHosts(),
                MakePrimaryHosts());
        }

        auto readHint =
            dirtyMap->MakeReadHint(TBlockRange64::WithLength(0, lsnsCount + 1));

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
        auto dirtyMap = std::make_shared<TBlocksDirtyMap>(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        dirtyMap->RegisterInflightWrite(100, TBlockRange64::WithLength(10, 21));
        dirtyMap->WriteFinished(
            100,
            TBlockRange64::WithLength(10, 21),
            MakePrimaryHosts(),
            MakePrimaryHosts());

        dirtyMap->RegisterInflightWrite(200, TBlockRange64::WithLength(25, 21));
        dirtyMap->WriteFinished(
            200,
            TBlockRange64::WithLength(25, 21),
            MakePrimaryHosts(),
            MakePrimaryHosts());

        dirtyMap->RegisterInflightWrite(300, TBlockRange64::WithLength(40, 21));
        dirtyMap->WriteFinished(
            300,
            TBlockRange64::WithLength(40, 21),
            MakePrimaryHosts(),
            MakePrimaryHosts());

        auto readHint =
            dirtyMap->MakeReadHint(TBlockRange64::WithLength(10, 51));

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
        auto dirtyMap = std::make_shared<TBlocksDirtyMap>(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        dirtyMap->RegisterInflightWrite(100, TBlockRange64::WithLength(10, 6));
        dirtyMap->WriteFinished(
            100,
            TBlockRange64::WithLength(10, 6),
            MakePrimaryHosts(),
            MakePrimaryHosts());

        dirtyMap->RegisterInflightWrite(200, TBlockRange64::WithLength(25, 6));
        dirtyMap->WriteFinished(
            200,
            TBlockRange64::WithLength(25, 6),
            MakePrimaryHosts(),
            MakePrimaryHosts());

        dirtyMap->RegisterInflightWrite(300, TBlockRange64::WithLength(45, 6));
        dirtyMap->WriteFinished(
            300,
            TBlockRange64::WithLength(45, 6),
            MakePrimaryHosts(),
            MakePrimaryHosts());

        auto readHint =
            dirtyMap->MakeReadHint(TBlockRange64::WithLength(0, 61));

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
        auto dirtyMap = std::make_shared<TBlocksDirtyMap>(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        dirtyMap->RegisterInflightWrite(100, TBlockRange64::WithLength(10, 91));
        dirtyMap->WriteFinished(
            100,
            TBlockRange64::WithLength(10, 91),
            MakePrimaryHosts(),
            MakePrimaryHosts());

        dirtyMap->RegisterInflightWrite(200, TBlockRange64::WithLength(20, 6));
        dirtyMap->WriteFinished(
            200,
            TBlockRange64::WithLength(20, 6),
            MakePrimaryHosts(),
            MakePrimaryHosts());

        dirtyMap->RegisterInflightWrite(300, TBlockRange64::WithLength(40, 6));
        dirtyMap->WriteFinished(
            300,
            TBlockRange64::WithLength(40, 6),
            MakePrimaryHosts(),
            MakePrimaryHosts());

        dirtyMap->RegisterInflightWrite(400, TBlockRange64::WithLength(70, 6));
        dirtyMap->WriteFinished(
            400,
            TBlockRange64::WithLength(70, 6),
            MakePrimaryHosts(),
            MakePrimaryHosts());

        auto readHint =
            dirtyMap->MakeReadHint(TBlockRange64::WithLength(10, 91));

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
        auto dirtyMap = std::make_shared<TBlocksDirtyMap>(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        auto inflightCounterBeforeWrite = dirtyMap->GetInflightCount();
        dirtyMap->RegisterInflightWrite(100, TBlockRange64::WithLength(10, 41));
        dirtyMap->WriteFinished(
            100,
            TBlockRange64::WithLength(10, 41),
            MakePrimaryHosts(),
            MakeHostMask(true, true, false, false, false));

        // write result with no quorum is skipped
        UNIT_ASSERT_VALUES_EQUAL(
            inflightCounterBeforeWrite,
            dirtyMap->GetInflightCount());

        auto readHint =
            dirtyMap->MakeReadHint(TBlockRange64::WithLength(10, 41));

        UNIT_ASSERT_VALUES_EQUAL(1, readHint.RangeHints.size());
        UNIT_ASSERT_VALUES_EQUAL(
            "0{[H0,H1,H2][10..50][0..40]};",
            readHint.DebugPrint());

        dirtyMap->RegisterInflightWrite(200, TBlockRange64::WithLength(10, 41));
        dirtyMap->WriteFinished(
            200,
            TBlockRange64::WithLength(10, 41),
            MakePrimaryHosts(),
            MakeHostMask(true, true, true, false, false));
        auto readHint1 =
            dirtyMap->MakeReadHint(TBlockRange64::WithLength(10, 41));
        UNIT_ASSERT_VALUES_EQUAL(1, readHint1.RangeHints.size());
        UNIT_ASSERT_VALUES_EQUAL(
            "200{[H0,H1,H2][10..50][0..40]};",
            readHint1.DebugPrint());
    }

    Y_UNIT_TEST(ShouldReadFromDDiskDuringPendingWrite)
    {
        const auto vchunkConfig = MakeTestVChunkConfig();
        auto dirtyMap = std::make_shared<TBlocksDirtyMap>(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        // Register a pending write (no PBuffer acknowledgement yet).
        dirtyMap->RegisterInflightWrite(123, TBlockRange64::WithLength(10, 10));

        // A read during pending write should see DDisk data (Lsn=0),
        // not the unacknowledged PBuffer data.
        auto readHint =
            dirtyMap->MakeReadHint(TBlockRange64::WithLength(10, 10));
        UNIT_ASSERT_VALUES_EQUAL(
            "0{[H0,H1,H2][10..19][0..9]};",
            readHint.DebugPrint());
        UNIT_ASSERT_VALUES_EQUAL(1, dirtyMap->GetInflightCount());

        // Complete the write. Now reads should see PBuffer data.
        dirtyMap->WriteFinished(
            123,
            TBlockRange64::WithLength(10, 10),
            MakePrimaryHosts(),
            MakePrimaryHosts());

        readHint = dirtyMap->MakeReadHint(TBlockRange64::WithLength(10, 10));
        UNIT_ASSERT_VALUES_EQUAL(
            "123{[H0,H1,H2][10..19][0..9]};",
            readHint.DebugPrint());
    }

    Y_UNIT_TEST(ShouldReadFromPBufferDuringPendingWriteWithExistingInflight)
    {
        const auto vchunkConfig = MakeTestVChunkConfig();
        auto dirtyMap = std::make_shared<TBlocksDirtyMap>(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        // First write completes normally.
        dirtyMap->RegisterInflightWrite(100, TBlockRange64::WithLength(10, 10));
        dirtyMap->WriteFinished(
            100,
            TBlockRange64::WithLength(10, 10),
            MakePrimaryHosts(),
            MakePrimaryHosts());

        // Second write overlaps and is pending.
        dirtyMap->RegisterInflightWrite(200, TBlockRange64::WithLength(10, 10));

        // Actually, the pending write (Lsn=200) overlaps the completed one
        // (Lsn=100) — the latest Lsn wins, and since Lsn 200 is in
        // PBufferPendingWrite state, it returns PBuffer read.
        auto readHint =
            dirtyMap->MakeReadHint(TBlockRange64::WithLength(10, 10));
        UNIT_ASSERT_VALUES_EQUAL(
            "100{[H0,H1,H2][10..19][0..9]};",
            readHint.DebugPrint());

        // Complete the second write.
        dirtyMap->WriteFinished(
            200,
            TBlockRange64::WithLength(10, 10),
            MakePrimaryHosts(),
            MakePrimaryHosts());

        readHint = dirtyMap->MakeReadHint(TBlockRange64::WithLength(10, 10));
        UNIT_ASSERT_VALUES_EQUAL(
            "200{[H0,H1,H2][10..19][0..9]};",
            readHint.DebugPrint());
    }

    Y_UNIT_TEST(ShouldEraseDisabledHostsAutomatically)
    {
        auto vchunkConfig = MakeTestVChunkConfig();
        auto dirtyMap = std::make_shared<TBlocksDirtyMap>(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        const THostMask requested = MakePrimaryHosts();
        const THostMask confirmed = MakePrimaryHosts();

        dirtyMap->RegisterInflightWrite(123, TBlockRange64::WithLength(10, 10));
        dirtyMap->WriteFinished(
            123,
            TBlockRange64::WithLength(10, 10),
            requested,
            confirmed);

        // Flush all hosts.
        auto flushHint = dirtyMap->MakeFlushHint(1);
        FlushAll(flushHint, *dirtyMap);

        // Disable host 0 before erase.
        vchunkConfig.DisableHost(0);
        dirtyMap->UpdateConfig(vchunkConfig);

        // Erase hints should only include enabled hosts.
        auto eraseHints = dirtyMap->MakeEraseHint(1);
        UNIT_ASSERT_VALUES_EQUAL(
            "H1:0:123;"
            "H2:0:123;",
            eraseHints.DebugPrint());
        EraseAll(eraseHints, *dirtyMap);

        // The disabled host's erase was auto-confirmed, so inflight should be
        // clear.
        UNIT_ASSERT_VALUES_EQUAL(0, dirtyMap->GetInflightCount());
    }

    Y_UNIT_TEST(ShouldHandleSafeBarrierWithPendingWrite)
    {
        const auto vchunkConfig = MakeTestVChunkConfig();
        auto dirtyMap = std::make_shared<TBlocksDirtyMap>(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        // No writes yet — no barrier.
        UNIT_ASSERT(!dirtyMap->GetSafeBarrierForErase().has_value());

        // Pending write holds the barrier from the moment of registration.
        dirtyMap->RegisterInflightWrite(100, TBlockRange64::WithLength(10, 10));
        UNIT_ASSERT_VALUES_EQUAL(100, *dirtyMap->GetSafeBarrierForErase());

        // Second pending write — barrier stays at 100.
        dirtyMap->RegisterInflightWrite(200, TBlockRange64::WithLength(20, 10));
        UNIT_ASSERT_VALUES_EQUAL(100, *dirtyMap->GetSafeBarrierForErase());

        // Completing write 100 with sub-quorum drops it.
        dirtyMap->WriteFinished(
            100,
            TBlockRange64::WithLength(10, 10),
            MakePrimaryHosts(),
            MakeHostMask(true, true, false, false, false));
        UNIT_ASSERT_VALUES_EQUAL(200, *dirtyMap->GetSafeBarrierForErase());

        // Completing write 200 with quorum keeps it until erased.
        dirtyMap->WriteFinished(
            200,
            TBlockRange64::WithLength(20, 10),
            MakePrimaryHosts(),
            MakePrimaryHosts());
        UNIT_ASSERT_VALUES_EQUAL(200, *dirtyMap->GetSafeBarrierForErase());
    }

    Y_UNIT_TEST(ShouldCleanupInflightWhenHostEvacuatedDuringFlush)
    {
        auto vchunkConfig = MakeTestVChunkConfig();
        auto dirtyMap = std::make_shared<TBlocksDirtyMap>(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        const THostMask requested = MakePrimaryHosts();
        const THostMask confirmed = MakePrimaryHosts();

        dirtyMap->RegisterInflightWrite(123, TBlockRange64::WithLength(10, 10));
        dirtyMap->WriteFinished(
            123,
            TBlockRange64::WithLength(10, 10),
            requested,
            confirmed);

        UNIT_ASSERT_VALUES_EQUAL(1, dirtyMap->GetInflightCount());

        // Flush all hosts.
        auto flushHint = dirtyMap->MakeFlushHint(1);
        UNIT_ASSERT_EQUAL(false, flushHint.Empty());

        // Confirm flush on hosts 0 and 2 only (leave host 1 pending).
        dirtyMap->FlushFinished(
            THostRoute{.SourceHostIndex = 0, .DestinationHostIndex = 0},
            {123},
            {});
        dirtyMap->FlushFinished(
            THostRoute{.SourceHostIndex = 2, .DestinationHostIndex = 2},
            {123},
            {});

        // Host 1 bytes are still accounted.
        UNIT_ASSERT_VALUES_EQUAL(
            40960,
            dirtyMap->GetPBufferCounters(THostIndex{1}).Current.Size);

        // Evacuate host 1 — this promotes host 3 as replacement.
        // DDisk set changes from {0,1,2} to {0,2,3}, making host 1 "removed".
        vchunkConfig.EvacuateHost(1);
        dirtyMap->UpdateConfig(vchunkConfig);

        // Flush to promoted host requested.
        flushHint = dirtyMap->MakeFlushHint(1);
        UNIT_ASSERT_VALUES_EQUAL("H0->H3:123[10..19];", flushHint.DebugPrint());
        dirtyMap->FlushFinished(
            THostRoute{.SourceHostIndex = 0, .DestinationHostIndex = 3},
            {123},
            {});

        // Flush should have completed (FlushDesired became {0,2} which equals
        // FlushConfirmed), erase should now be possible.
        auto eraseHints = dirtyMap->MakeEraseHint(1);
        UNIT_ASSERT_VALUES_EQUAL(false, eraseHints.Empty());

        // Erase should only cover hosts that still have write data (0 and 2).
        UNIT_ASSERT_VALUES_EQUAL("H0:0:123;H2:0:123;", eraseHints.DebugPrint());
        EraseAll(eraseHints, *dirtyMap);
        // Inflight should be fully cleaned up.
        UNIT_ASSERT_VALUES_EQUAL(0, dirtyMap->GetInflightCount());
    }

    Y_UNIT_TEST(ShouldCleanupInflightWhenHostEvacuatedDuringErase)
    {
        auto vchunkConfig = MakeTestVChunkConfig();
        auto dirtyMap = std::make_shared<TBlocksDirtyMap>(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        const THostMask requested = MakePrimaryHosts();
        const THostMask confirmed = MakePrimaryHosts();

        dirtyMap->RegisterInflightWrite(123, TBlockRange64::WithLength(10, 10));
        dirtyMap->WriteFinished(
            123,
            TBlockRange64::WithLength(10, 10),
            requested,
            confirmed);

        // Flush all hosts.
        auto flushHint = dirtyMap->MakeFlushHint(1);
        UNIT_ASSERT_VALUES_EQUAL(
            "H0->H0:123[10..19];"
            "H1->H1:123[10..19];"
            "H2->H2:123[10..19];",
            flushHint.DebugPrint());
        FlushAll(flushHint, *dirtyMap);

        // Get erase hints and finish erase on hosts 0 and 2 only.
        auto eraseHints = dirtyMap->MakeEraseHint(1);
        UNIT_ASSERT_VALUES_EQUAL(
            "H0:0:123;"
            "H1:0:123;"
            "H2:0:123;",
            eraseHints.DebugPrint());
        dirtyMap->EraseFinished(THostIndex{0}, {123}, {});
        dirtyMap->EraseFinished(THostIndex{2}, {123}, {});

        // Inflight item still present — host 1 erase pending.
        UNIT_ASSERT_VALUES_EQUAL(1, dirtyMap->GetInflightCount());

        // Evacuate host 1
        vchunkConfig.EvacuateHost(1);
        dirtyMap->UpdateConfig(vchunkConfig);

        // The inflight item should be fully erased and removed from the map.
        UNIT_ASSERT_VALUES_EQUAL(0, dirtyMap->GetInflightCount());
    }

    Y_UNIT_TEST(ShouldReleasePBufferCountersOnHostEvacuation)
    {
        auto vchunkConfig = MakeTestVChunkConfig();
        auto dirtyMap = std::make_shared<TBlocksDirtyMap>(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        const THostMask requested = MakePrimaryHosts();
        const THostMask confirmed = MakePrimaryHosts();

        dirtyMap->RegisterInflightWrite(123, TBlockRange64::WithLength(10, 10));
        dirtyMap->WriteFinished(
            123,
            TBlockRange64::WithLength(10, 10),
            requested,
            confirmed);

        // Verify all 3 primary hosts have byte counters.
        for (THostIndex h: MakePrimaryHosts()) {
            auto counters = dirtyMap->GetPBufferCounters(h);
            UNIT_ASSERT_VALUES_EQUAL(40960, counters.Current.Size);
        }

        // Evacuate host 2 — host 3 gets promoted; DDisk set becomes {0,1,3}.
        vchunkConfig.EvacuateHost(2);
        dirtyMap->UpdateConfig(vchunkConfig);

        // Hosts counters should be unchanged.
        UNIT_ASSERT_VALUES_EQUAL(
            40960,
            dirtyMap->GetPBufferCounters(THostIndex{0}).Current.Size);
        UNIT_ASSERT_VALUES_EQUAL(
            40960,
            dirtyMap->GetPBufferCounters(THostIndex{1}).Current.Size);
        UNIT_ASSERT_VALUES_EQUAL(
            40960,
            dirtyMap->GetPBufferCounters(THostIndex{2}).Current.Size);

        // Total bytes should remain as historical record.
        UNIT_ASSERT_VALUES_EQUAL(
            40960,
            dirtyMap->GetPBufferCounters(THostIndex{2}).Total.Size);
    }

    Y_UNIT_TEST(ShouldIgnoreOutdatedFlushResponseAfterInflightRemoved)
    {
        const auto vchunkConfig = MakeTestVChunkConfig();
        auto dirtyMap = std::make_shared<TBlocksDirtyMap>(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        const THostMask requested = MakePrimaryHosts();
        const THostMask confirmed = MakePrimaryHosts();

        // Complete a full write -> flush -> erase lifecycle so the inflight
        // item for lsn 123 is removed from the map.
        dirtyMap->RegisterInflightWrite(123, TBlockRange64::WithLength(10, 10));
        dirtyMap->WriteFinished(
            123,
            TBlockRange64::WithLength(10, 10),
            requested,
            confirmed);

        auto flushHint = dirtyMap->MakeFlushHint(1);
        UNIT_ASSERT_EQUAL(false, flushHint.Empty());
        FlushAll(flushHint, *dirtyMap);

        auto eraseHints = dirtyMap->MakeEraseHint(1);
        UNIT_ASSERT_EQUAL(false, eraseHints.Empty());
        EraseAll(eraseHints, *dirtyMap);
        // The inflight item is gone.
        UNIT_ASSERT_VALUES_EQUAL(0, dirtyMap->GetInflightCount());

        // An outdated flush response for the already-removed inflight item must
        // be ignored gracefully. Previously this tripped a Y_ABORT_UNLESS(item)
        // invariant check. Exercise both the flushOk and flushFailed branches
        // on still-enabled destination hosts.
        dirtyMap->FlushFinished(
            THostRoute{.SourceHostIndex = 0, .DestinationHostIndex = 0},
            {123},
            {});
        dirtyMap->FlushFinished(
            THostRoute{.SourceHostIndex = 1, .DestinationHostIndex = 1},
            {},
            {123});

        // Nothing should have changed and no new flush hints appear.
        UNIT_ASSERT_VALUES_EQUAL(0, dirtyMap->GetInflightCount());
        UNIT_ASSERT_EQUAL(true, dirtyMap->MakeFlushHint(1).Empty());
    }

    // A host can be evacuated (disabled AND demoted out of the DDisk set)
    // after a write was registered (pending) but before its successful write
    // response arrives. WriteFinished must then drop all references to the
    // evacuated host: release its PBuffer byte counters and exclude it from
    // reads. Only demoted hosts (DisabledHosts \ DesiredDDisks) are dropped.
    Y_UNIT_TEST(ShouldReleaseEvacuatedHostOnWriteFinished)
    {
        auto vchunkConfig = MakeTestVChunkConfig();
        auto dirtyMap = std::make_shared<TBlocksDirtyMap>(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        const THostMask requested = MakePrimaryHosts();   // {0,1,2}
        const THostMask confirmed = MakePrimaryHosts();   // {0,1,2}

        // Register a pending write across all three primary hosts.
        dirtyMap->RegisterInflightWrite(123, TBlockRange64::WithLength(10, 10));
        UNIT_ASSERT_VALUES_EQUAL(1, dirtyMap->GetInflightCount());

        // Host 0 is evacuated after the write is registered but before the
        // write response arrives. EvacuateHost disables host 0 and demotes it
        // out of the DDisk set (promoting host 3 as replacement). The write is
        // still pending (WriteRequested is empty), so UpdateConfig's
        // RemoveHosts is a no-op and the inflight item survives.
        vchunkConfig.EvacuateHost(0);
        dirtyMap->UpdateConfig(vchunkConfig);
        UNIT_ASSERT_VALUES_EQUAL(1, dirtyMap->GetInflightCount());

        // The write response finally arrives, confirming all three hosts,
        // including the now-evacuated host 0.
        dirtyMap->WriteFinished(
            123,
            TBlockRange64::WithLength(10, 10),
            requested,
            confirmed);

        // Quorum is still held by the two remaining hosts, so the inflight item
        // survives.
        UNIT_ASSERT_VALUES_EQUAL(1, dirtyMap->GetInflightCount());

        // Counters should not be changed
        UNIT_ASSERT_VALUES_EQUAL(
            40960,
            dirtyMap->GetPBufferCounters(THostIndex{0}).Current.Size);
        UNIT_ASSERT_VALUES_EQUAL(
            40960,
            dirtyMap->GetPBufferCounters(THostIndex{1}).Current.Size);
        UNIT_ASSERT_VALUES_EQUAL(
            40960,
            dirtyMap->GetPBufferCounters(THostIndex{2}).Current.Size);

        // Reads only see the remaining confirmed hosts, never the evacuated
        // one.
        auto readHint =
            dirtyMap->MakeReadHint(TBlockRange64::WithLength(10, 10));
        UNIT_ASSERT_VALUES_EQUAL(
            "123{[H1,H2][10..19][0..9]};",
            readHint.DebugPrint());
    }

    // Contrast with evacuation: a host that is only temporarily disabled but
    // still remains a desired DDisk must NOT have its references dropped on
    // WriteFinished. The demoted set (DisabledHosts \ DesiredDDisks) is empty,
    // so its PBuffer data is preserved (the host is expected to come back).
    Y_UNIT_TEST(ShouldKeepTemporarilyDisabledDDiskOnWriteFinished)
    {
        auto vchunkConfig = MakeTestVChunkConfig();
        auto dirtyMap = std::make_shared<TBlocksDirtyMap>(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        const THostMask requested = MakePrimaryHosts();   // {0,1,2}
        const THostMask confirmed = MakePrimaryHosts();   // {0,1,2}

        // Register a pending write across all three primary hosts.
        dirtyMap->RegisterInflightWrite(123, TBlockRange64::WithLength(10, 10));
        UNIT_ASSERT_VALUES_EQUAL(1, dirtyMap->GetInflightCount());

        // Host 0 is only temporarily disabled: it stays in the DDisk set, so
        // DesiredDDisks is unchanged and UpdateConfig runs no RemoveHosts.
        vchunkConfig.DisableHost(0);
        dirtyMap->UpdateConfig(vchunkConfig);
        UNIT_ASSERT_VALUES_EQUAL(1, dirtyMap->GetInflightCount());

        // The write response finally arrives, confirming all three hosts,
        // including the temporarily-disabled host 0.
        dirtyMap->WriteFinished(
            123,
            TBlockRange64::WithLength(10, 10),
            requested,
            confirmed);

        UNIT_ASSERT_VALUES_EQUAL(1, dirtyMap->GetInflightCount());

        // Host 0 is still a desired DDisk, so its references are preserved: its
        // PBuffer data is kept, unlike the evacuation case.
        UNIT_ASSERT_VALUES_EQUAL(
            40960,
            dirtyMap->GetPBufferCounters(THostIndex{0}).Current.Size);
        UNIT_ASSERT_VALUES_EQUAL(
            40960,
            dirtyMap->GetPBufferCounters(THostIndex{1}).Current.Size);
        UNIT_ASSERT_VALUES_EQUAL(
            40960,
            dirtyMap->GetPBufferCounters(THostIndex{2}).Current.Size);

        // Reads still exclude the disabled host from the hint mask, but the
        // data remains on its PBuffer for when it comes back online.
        auto readHint =
            dirtyMap->MakeReadHint(TBlockRange64::WithLength(10, 10));
        UNIT_ASSERT_VALUES_EQUAL(
            "123{[H1,H2][10..19][0..9]};",
            readHint.DebugPrint());

        // No flush is generated while a desired DDisk is still disabled.
        UNIT_ASSERT_EQUAL(true, dirtyMap->MakeFlushHint(1).Empty());
    }

    Y_UNIT_TEST(ShouldReturnFreshRangeForFreshDDisk)
    {
        auto vchunkConfig = MakeTestVChunkConfig();

        // Make H0 partially fresh: only the first 30 blocks are up to date.
        vchunkConfig.SetWatermark(THostIndex{0}, 30 * DefaultBlockSize);

        auto dirtyMap = std::make_shared<TBlocksDirtyMap>(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        const ui64 totalBlocks = DefaultVChunkSize / DefaultBlockSize;

        // A fully operational DDisk has no fresh range to sync.
        UNIT_ASSERT_EQUAL(std::nullopt, dirtyMap->GetFreshRange(THostIndex{1}));

        UNIT_ASSERT_VALUES_EQUAL(
            "H0*{Fresh+,30};"
            "H1*{Operational,32768};"
            "H2*{Operational,32768};"
            "H3+{Disabled,0};"
            "H4+{Disabled,0};",
            dirtyMap->DebugPrintDDiskState());

        // The fresh range starts right after the operational block count and
        // spans up to the end of the vchunk.
        auto freshRange = dirtyMap->GetFreshRange(THostIndex{0});
        UNIT_ASSERT(freshRange.has_value());
        UNIT_ASSERT_VALUES_EQUAL(
            TBlockRange64::MakeClosedInterval(30, totalBlocks - 1),
            *freshRange);

        // Operational DDisks still have no fresh range.
        UNIT_ASSERT_EQUAL(std::nullopt, dirtyMap->GetFreshRange(THostIndex{1}));
        UNIT_ASSERT_EQUAL(std::nullopt, dirtyMap->GetFreshRange(THostIndex{2}));
    }

    Y_UNIT_TEST(ShouldAdvanceFreshRangeAfterRangeSynced)
    {
        const ui64 totalBlocks = DefaultVChunkSize / DefaultBlockSize;

        auto vchunkConfig = MakeTestVChunkConfig();

        // Make H0 completely fresh (nothing synced yet).
        vchunkConfig.SetWatermark(THostIndex{0}, 0);

        auto dirtyMap = std::make_shared<TBlocksDirtyMap>(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        UNIT_ASSERT_VALUES_EQUAL(
            TBlockRange64::MakeClosedInterval(0, totalBlocks - 1),
            *dirtyMap->GetFreshRange(THostIndex{0}));
        UNIT_ASSERT_VALUES_EQUAL(
            "H0*{Fresh+,0};"
            "H1*{Operational,32768};"
            "H2*{Operational,32768};"
            "H3+{Disabled,0};"
            "H4+{Disabled,0};",
            dirtyMap->DebugPrintDDiskState());

        // Sync the first 256 blocks. The operational block count should advance
        // to 256 and the fresh range should shrink accordingly.
        auto freshRange = dirtyMap->GetFreshRange(THostIndex{0});
        UNIT_ASSERT_VALUES_EQUAL(
            TBlockRange64::MakeClosedInterval(0, totalBlocks - 1),
            *freshRange);
        auto syncRange = TBlockRange64::MakeClosedInterval(0, 255);
        auto syncHint = dirtyMap->BeginRangeSync(THostIndex{0}, syncRange);
        dirtyMap->EndRangeSync(syncHint.SyncId, true);

        UNIT_ASSERT_VALUES_EQUAL(
            TBlockRange64::MakeClosedInterval(256, totalBlocks - 1),
            *dirtyMap->GetFreshRange(THostIndex{0}));
        UNIT_ASSERT_VALUES_EQUAL(
            "H0*{Fresh+,256};"
            "H1*{Operational,32768};"
            "H2*{Operational,32768};"
            "H3+{Disabled,0};"
            "H4+{Disabled,0};",
            dirtyMap->DebugPrintDDiskState());

        // Sync the remaining blocks. The DDisk becomes fully operational and no
        // longer reports a fresh range.
        syncRange = TBlockRange64::MakeClosedInterval(256, totalBlocks - 1);
        syncHint = dirtyMap->BeginRangeSync(THostIndex{0}, syncRange);
        dirtyMap->EndRangeSync(syncHint.SyncId, true);

        UNIT_ASSERT_EQUAL(std::nullopt, dirtyMap->GetFreshRange(THostIndex{0}));
        UNIT_ASSERT_VALUES_EQUAL(
            "H0*{Operational,32768};"
            "H1*{Operational,32768};"
            "H2*{Operational,32768};"
            "H3+{Disabled,0};"
            "H4+{Disabled,0};",
            dirtyMap->DebugPrintDDiskState());
    }

    Y_UNIT_TEST(ShouldTriggerRangeSyncStartWithoutInflightFlush)
    {
        auto vchunkConfig = MakeTestVChunkConfig();

        // Make H0 completely fresh.
        vchunkConfig.SetWatermark(THostIndex{0}, 0);

        auto dirtyMap = std::make_shared<TBlocksDirtyMap>(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        const auto range = TBlockRange64::MakeClosedInterval(0, 255);

        // With no overlapping inflight flush, the sync start trigger should be
        // ready immediately.
        auto syncHint = dirtyMap->BeginRangeSync(THostIndex{0}, range);
        UNIT_ASSERT_VALUES_EQUAL(true, syncHint.ReadyToStart.HasValue());

        // The sync should be registered as in-flight and ready to run.
        UNIT_ASSERT_VALUES_EQUAL(
            "H0[0..255]ready;",
            dirtyMap->DebugPrintInflightSync());

        // Completing the sync removes it from the in-flight sync map.
        dirtyMap->EndRangeSync(syncHint.SyncId, true);
        UNIT_ASSERT_VALUES_EQUAL("", dirtyMap->DebugPrintInflightSync());
    }

    Y_UNIT_TEST(ShouldNotAdvanceFreshRangeWhenRangeSyncFailed)
    {
        const ui64 totalBlocks = DefaultVChunkSize / DefaultBlockSize;

        auto vchunkConfig = MakeTestVChunkConfig();

        // Make H0 completely fresh (nothing synced yet).
        vchunkConfig.SetWatermark(THostIndex{0}, 0);

        auto dirtyMap = std::make_shared<TBlocksDirtyMap>(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        UNIT_ASSERT_VALUES_EQUAL(
            TBlockRange64::MakeClosedInterval(0, totalBlocks - 1),
            *dirtyMap->GetFreshRange(THostIndex{0}));

        const auto syncRange = TBlockRange64::MakeClosedInterval(0, 255);
        auto syncHint = dirtyMap->BeginRangeSync(THostIndex{0}, syncRange);

        // A failed sync must be removed from the in-flight sync map, but it
        // must NOT advance the operational block count / shrink the fresh
        // range.
        dirtyMap->EndRangeSync(syncHint.SyncId, false);
        UNIT_ASSERT_VALUES_EQUAL("", dirtyMap->DebugPrintInflightSync());

        UNIT_ASSERT_VALUES_EQUAL(
            TBlockRange64::MakeClosedInterval(0, totalBlocks - 1),
            *dirtyMap->GetFreshRange(THostIndex{0}));
        UNIT_ASSERT_VALUES_EQUAL(
            "H0*{Fresh+,0};"
            "H1*{Operational,32768};"
            "H2*{Operational,32768};"
            "H3+{Disabled,0};"
            "H4+{Disabled,0};",
            dirtyMap->DebugPrintDDiskState());
    }

    // Exercises the full persist lifecycle:
    //   - NeedPersist() starts false and generation is 0.
    //   - After a flush that populates a fresh DDisk's Ahead field,
    //     NeedPersist() becomes true and generation advances.
    //   - GetStateForPersist() captures the generation and correct DDisk count.
    //   - StatePersisted() resets NeedPersist() to false.
    //   - Only Behind data (not Ahead) can block MakeEraseHint().
    Y_UNIT_TEST(PersistLifecycleAndEraseNotBlockedByAheadData)
    {
        auto vchunkConfig = MakeTestVChunkConfig();

        // H3 is fresh; writes above watermark populate its Ahead field.
        // H1 is lagging; writes populate Behind field.
        vchunkConfig.PromoteHost(3);
        vchunkConfig.SetWatermark(3, DefaultBlockSize * 5);
        vchunkConfig.DisableHost(1);

        auto dirtyMap = std::make_shared<TBlocksDirtyMap>(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        // Initially no changes.
        UNIT_ASSERT_VALUES_EQUAL(false, dirtyMap->NeedPersist());
        UNIT_ASSERT_VALUES_EQUAL(0, dirtyMap->GetCurrentGeneration());

        const THostMask requested = MakeHostMask(true, true, true, true, false);
        dirtyMap->RegisterInflightWrite(100, TBlockRange64::WithLength(10, 10));
        dirtyMap->WriteFinished(
            100,
            TBlockRange64::WithLength(10, 10),
            requested,
            requested);

        // Flush all DDIsks; H3's Ahead field changes → generation increments.
        auto flushHint = dirtyMap->MakeFlushHint(1);
        UNIT_ASSERT_EQUAL(false, flushHint.Empty());
        FlushAll(flushHint, *dirtyMap);

        // Can't erase not persisted red blocks.
        auto eraseHints = dirtyMap->MakeEraseHint(1);
        UNIT_ASSERT_VALUES_EQUAL("", eraseHints.DebugPrint());

        // GetStateForPersist captures current generation.
        // Saves one entry per host slot (DirectBlockGroupHostCount = 5).
        UNIT_ASSERT_VALUES_EQUAL(true, dirtyMap->NeedPersist());
        const ui32 gen = dirtyMap->GetCurrentGeneration();
        UNIT_ASSERT(gen > 0);
        auto state = dirtyMap->GetStateForPersist();
        UNIT_ASSERT_VALUES_EQUAL(gen, state.GetStateGeneration());
        UNIT_ASSERT_VALUES_EQUAL(5, state.DDiskStatesSize());

        // StatePersisted() resets the persist flag.
        dirtyMap->StatePersisted(gen);
        UNIT_ASSERT_VALUES_EQUAL(false, dirtyMap->NeedPersist());

        // Only Behind blocks erase; Ahead does not.
        UNIT_ASSERT_VALUES_EQUAL(
            "  H1: [10..19]\n",
            dirtyMap->DebugPrintBehind());
        eraseHints = dirtyMap->MakeEraseHint(1);
        UNIT_ASSERT_VALUES_EQUAL(
            "H0:0:100;"
            "H2:0:100;"
            "H3:0:100;",
            eraseHints.DebugPrint());
    }

    // Load() restores the per-DDisk Ahead/Behind state captured by
    // GetStateForPersist() into a freshly constructed dirty map.
    Y_UNIT_TEST(ShouldLoadPersistedDDiskState)
    {
        auto vchunkConfig = MakeTestVChunkConfig();

        // H3 is fresh; writes above watermark populate its Ahead field.
        // H1 is lagging; writes populate Behind field.
        vchunkConfig.PromoteHost(3);
        vchunkConfig.SetWatermark(3, DefaultBlockSize * 5);
        vchunkConfig.DisableHost(1);

        auto source = std::make_shared<TBlocksDirtyMap>(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        const THostMask requested = MakeHostMask(true, true, true, true, false);
        source->RegisterInflightWrite(100, TBlockRange64::WithLength(10, 10));
        source->WriteFinished(
            100,
            TBlockRange64::WithLength(10, 10),
            requested,
            requested);

        // Flush all DDisks so H3's Ahead field records the flushed range.
        auto flushHint = source->MakeFlushHint(1);
        UNIT_ASSERT_EQUAL(false, flushHint.Empty());
        FlushAll(flushHint, *source);

        UNIT_ASSERT_VALUES_EQUAL("  H3: [10..19]\n", source->DebugPrintAhead());
        UNIT_ASSERT_VALUES_EQUAL(
            "  H1: [10..19]\n",
            source->DebugPrintBehind());

        const auto persisted = source->GetStateForPersist();
        UNIT_ASSERT_VALUES_EQUAL(5, persisted.DDiskStatesSize());

        // Load into a freshly constructed dirty map with the same config.
        auto target = std::make_shared<TBlocksDirtyMap>(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        // Before load the target has no tracked ranges.
        UNIT_ASSERT_VALUES_EQUAL("", target->DebugPrintAhead());
        UNIT_ASSERT_VALUES_EQUAL("", target->DebugPrintBehind());

        target->Load(persisted);

        // After load the target mirrors the source's Ahead/Behind fields.
        UNIT_ASSERT_VALUES_EQUAL(
            source->DebugPrintAhead(),
            target->DebugPrintAhead());
        UNIT_ASSERT_VALUES_EQUAL(
            source->DebugPrintBehind(),
            target->DebugPrintBehind());
    }

    // Loading a default-constructed (empty) proto must be a no-op: no DDisk
    // states are present, so the target keeps its freshly constructed state
    // with no tracked Ahead/Behind ranges.
    Y_UNIT_TEST(ShouldLoadEmptyStateAsNoOp)
    {
        const auto vchunkConfig = MakeTestVChunkConfig();
        auto dirtyMap = std::make_shared<TBlocksDirtyMap>(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        const auto before = dirtyMap->DebugPrintDDiskState();

        dirtyMap->Load(TDirtyMapStateProto());

        UNIT_ASSERT_VALUES_EQUAL("", dirtyMap->DebugPrintAhead());
        UNIT_ASSERT_VALUES_EQUAL("", dirtyMap->DebugPrintBehind());
        UNIT_ASSERT_VALUES_EQUAL(before, dirtyMap->DebugPrintDDiskState());
    }

    Y_UNIT_TEST(ShouldNotEraseUntaggedLsn)
    {
        auto vchunkConfig = MakeTestVChunkConfig();

        // Promote hand-off H3 to a primary DDisk so we have 4 desired DDisks.
        vchunkConfig.PromoteHost(3);

        auto dirtyMap = std::make_shared<TBlocksDirtyMap>(
            vchunkConfig,
            DefaultBlockSize,
            DefaultVChunkSize / DefaultBlockSize);

        // Disable H3 while it stays a desired DDisk -> it starts lagging and
        // will record ranges it misses as Behind.
        vchunkConfig.DisableHost(3);
        dirtyMap->UpdateConfig(vchunkConfig);

        UNIT_ASSERT_VALUES_EQUAL(false, dirtyMap->NeedPersist());
        UNIT_ASSERT_VALUES_EQUAL(0u, dirtyMap->GetCurrentGeneration());

        // Write to the three enabled DDisks (quorum) and flush. The lagging H3
        // misses the flush, so the range is recorded in its Behind field and
        // the generation advances.
        const THostMask requested =
            MakeHostMask(true, true, true, false, false);
        dirtyMap->RegisterInflightWrite(123, TBlockRange64::WithLength(10, 10));
        dirtyMap->WriteFinished(
            123,
            TBlockRange64::WithLength(10, 10),
            requested,
            requested);

        auto flushHint = dirtyMap->MakeFlushHint(1);
        UNIT_ASSERT_EQUAL(false, flushHint.Empty());
        FlushAll(flushHint, *dirtyMap);

        // H3 now lags behind on the written range; generation is 1.
        UNIT_ASSERT_VALUES_EQUAL(
            "  H3: [10..19]\n",
            dirtyMap->DebugPrintBehind());
        UNIT_ASSERT_VALUES_EQUAL(1u, dirtyMap->GetCurrentGeneration());
        UNIT_ASSERT_VALUES_EQUAL(true, dirtyMap->NeedPersist());

        // Can't erase since state not persisted yet.
        auto eraseHints = dirtyMap->MakeEraseHint(1);
        UNIT_ASSERT_VALUES_EQUAL("", eraseHints.DebugPrint());
        UNIT_ASSERT_VALUES_EQUAL(1, dirtyMap->GetInflightCount());

        // Persist generation 1.
        dirtyMap->StatePersisted(1);
        UNIT_ASSERT_VALUES_EQUAL(false, dirtyMap->NeedPersist());

        // Can erase since red blocks persisted.
        eraseHints = dirtyMap->MakeEraseHint(1);
        UNIT_ASSERT_VALUES_EQUAL(
            "H0:0:123;"
            "H1:0:123;"
            "H2:0:123;",
            eraseHints.DebugPrint());
        EraseAll(eraseHints, *dirtyMap);

        UNIT_ASSERT_VALUES_EQUAL(0, dirtyMap->GetInflightCount());
    }
}

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
