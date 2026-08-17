#include "hulldb_compstrat_selector.h"
#include "hulldb_compstrat_emergency.h"
#include "hulldb_compstrat_ratio.h"
#include <util/stream/null.h>
#include <ydb/core/blobstorage/vdisk/hulldb/test/testhull_index.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_ut.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_arena.h>
#include <ydb/core/blobstorage/vdisk/hulldb/generic/hullds_leveledssts.h>
#include <library/cpp/testing/unittest/registar.h>

#define STR     Cnull

namespace NKikimr {

    Y_UNIT_TEST_SUITE(TBlobStorageCompStrat) {

        static constexpr ui64 ChunkSize = 128u << 20u;
        static constexpr ui32 HullCompLevel0MaxSstsAtOnce = 8u;
        static constexpr ui32 HullCompSortedPartsNum = 8u;
        static constexpr bool Level0UseDreg = true;
        using TStrategy = ::NKikimr::NHullComp::TStrategy<TKeyLogoBlob, TMemRecLogoBlob>;
        using TStrategyEmergency = ::NKikimr::NHullComp::TStrategyEmergency<TKeyLogoBlob, TMemRecLogoBlob>;
        using TTask = ::NKikimr::NHullComp::TTask<TKeyLogoBlob, TMemRecLogoBlob>;
        using TUtils = ::NKikimr::NHullComp::TUtils<TKeyLogoBlob, TMemRecLogoBlob>;
        using TLeveledSstsIterator = TLeveledSsts<TKeyLogoBlob, TMemRecLogoBlob>::TIterator;

        Y_UNIT_TEST(Test1) {
            STR << "Building LevelIndex\n";
            TIntrusivePtr<THullDs> ds = NTest::GenerateDs_17Level_Logs();
            STR << "Taking Snapshot\n";
            auto snap = ds->GetIndexSnapshot();


            // calculate storage ratio
            TIntrusivePtr<TBarriersSnapshot::TBarriersEssence> barriersEssence =
                snap.BarriersSnap.CreateEssence(snap.HullCtx);
            NHullComp::TStrategyStorageRatio<TKeyLogoBlob, TMemRecLogoBlob>
                (snap.HullCtx, snap.LogoBlobsSnap, std::move(barriersEssence), true).Work();

            snap.LogoBlobsSnap.Output(STR);
            STR << "\n";


            STR << "Building Boundaries\n";
            NHullComp::TBoundariesConstPtr boundaries(new NHullComp::TBoundaries(ChunkSize,
                        HullCompLevel0MaxSstsAtOnce, HullCompSortedPartsNum, Level0UseDreg));

            STR << "Selecting Strategy\n";
            TTask task;
            NHullComp::TSelectorParams params = {boundaries, 1.0, TInstant::Seconds(0), {}};
            TStrategy strategy(snap.HullCtx, params, std::move(snap.LogoBlobsSnap), std::move(snap.BarriersSnap),
                    &task, true);
            auto action = strategy.Select();
            STR << "action = " << NHullComp::ActionToStr(action) << "\n";
        }

        struct TSynthHull {
            TTestContexts Ctx;
            std::shared_ptr<TRopeArena> Arena;
            TIntrusivePtr<THullDs> Ds;
            NHullComp::TBoundariesConstPtr Boundaries;

            TSynthHull(ui32 nSortedLevels, ui32 chunkSize = ChunkSize)
                : Ctx(chunkSize)
                , Arena(std::make_shared<TRopeArena>(&TRopeArenaBackend::Allocate))
                , Ds(MakeIntrusive<THullDs>(Ctx.GetHullCtx()))
                , Boundaries(new NHullComp::TBoundaries(chunkSize,
                    HullCompLevel0MaxSstsAtOnce, HullCompSortedPartsNum, Level0UseDreg))
            {
                Ds->LogoBlobs = MakeIntrusive<TLogoBlobsDs>(Ctx.GetLevelIndexSettings(), Arena);
                for (ui32 i = 0; i < nSortedLevels; ++i) {
                    Ds->LogoBlobs->CurSlice->SortedLevels.push_back(
                        TSortedLevel<TKeyLogoBlob, TMemRecLogoBlob>(TKeyLogoBlob()));
                }
                Ds->Blocks = MakeIntrusive<TBlocksDs>(Ctx.GetLevelIndexSettings(), Arena);
                Ds->Barriers = MakeIntrusive<TBarriersDs>(Ctx.GetLevelIndexSettings(), Arena);
                Ds->LogoBlobs->LoadCompleted();
                Ds->Blocks->LoadCompleted();
                Ds->Barriers->LoadCompleted();
            }

            TLogoBlobsSstPtr MakeSst(ui64 tabletId, ui32 firstStep, ui32 lastStep, ui32 chunkIdx,
                    ui32 inplacedSize, NHullComp::TSstRatioPtr ratio)
            {
                auto sst = MakeIntrusive<TLogoBlobsSst>(Ctx.GetVCtx());
                TTrackableVector<TLogoBlobsSst::TRec> index(TMemoryConsumer(Ctx.GetVCtx()->SstIndex));
                for (ui32 step = firstStep; step <= lastStep; ++step) {
                    TLogoBlobID id(tabletId, 1, step, 0, 1, 0, 1);
                    TMemRecLogoBlob memRec;
                    memRec.SetDiskBlob(TDiskPart(chunkIdx, 0, inplacedSize));
                    index.emplace_back(TKeyLogoBlob(id), memRec);
                }
                sst->LoadLinearIndex(index);
                sst->AllChunks.push_back(chunkIdx);
                sst->Info.Chunks = 1;
                sst->Info.Items = lastStep - firstStep + 1;
                sst->Info.InplaceDataTotalSize = ui64(inplacedSize) * sst->Info.Items;
                sst->Info.FirstLsn = 1;
                sst->Info.LastLsn = 1;
                if (ratio) {
                    sst->StorageRatio.Set(ratio, TInstant::Zero());
                }
                return sst;
            }

            static NHullComp::TSstRatioPtr KeepRatio(ui64 keepBytes) {
                auto ratio = MakeIntrusive<NHullComp::TSstRatio>();
                ratio->IndexItemsTotal = 1;
                ratio->IndexItemsKeep = 1;
                ratio->InplacedDataTotal = keepBytes;
                ratio->InplacedDataKeep = keepBytes;
                return ratio;
            }

            void PutLevel(ui32 levelIdx, TLogoBlobsSstPtr sst) {
                Ds->LogoBlobs->CurSlice->SortedLevels[levelIdx].Put(sst);
            }

            void PutL0(TLogoBlobsSstPtr sst) {
                Ds->LogoBlobs->CurSlice->Level0.Put(sst);
            }

            ui32 LastLevelIdx() const {
                return Ds->LogoBlobs->CurSlice->SortedLevels.size() - 1;
            }

            ui32 LastPhysicalLevel() const {
                return LastLevelIdx() + 1;
            }
        };

        ui32 CountSstsToDelete(const TTask &task) {
            ui32 n = 0;
            TLeveledSstsIterator it(&task.GetSstsToDelete());
            it.SeekToFirst();
            while (it.Valid()) {
                ++n;
                it.Next();
            }
            return n;
        }

        void AssertAction(NHullComp::EAction actual, NHullComp::EAction expected) {
            UNIT_ASSERT_VALUES_EQUAL(TString(NHullComp::ActionToStr(actual)),
                TString(NHullComp::ActionToStr(expected)));
        }

        void AssertStrategy(NHullComp::ESelectStrategy actual, NHullComp::ESelectStrategy expected) {
            UNIT_ASSERT_VALUES_EQUAL(ui32(actual), ui32(expected));
        }

        Y_UNIT_TEST(EmergencyPacksTwoSparseSstsOnLastLevel) {
            TSynthHull hull(17);
            const ui64 keep = hull.Ctx.GetHullCtx()->ChunkSize / 3;
            hull.PutLevel(hull.LastLevelIdx(), hull.MakeSst(1, 1, 1, 10, 100, hull.KeepRatio(keep)));
            hull.PutLevel(hull.LastLevelIdx(), hull.MakeSst(1, 2, 2, 11, 100, hull.KeepRatio(keep)));

            auto snap = hull.Ds->GetIndexSnapshot();
            TTask task;
            NHullComp::TSelectorParams params = {hull.Boundaries, 1.0, TInstant::Seconds(0), {}};
            params.FreeChunksBudget = 1;
            params.EmergencyMode = true;

            TStrategyEmergency emergency(snap.HullCtx, params, snap.LogoBlobsSnap, &task);
            AssertAction(emergency.Select(), NHullComp::ActCompactSsts);
            UNIT_ASSERT_VALUES_EQUAL(task.CompactSsts.TargetLevel, hull.LastPhysicalLevel());
            UNIT_ASSERT_VALUES_EQUAL(CountSstsToDelete(task), 2u);
        }

        Y_UNIT_TEST(EmergencyFullSelectPacksTwoSparseSsts) {
            TSynthHull hull(17);
            const ui64 keep = hull.Ctx.GetHullCtx()->ChunkSize / 3;
            hull.PutLevel(hull.LastLevelIdx(), hull.MakeSst(1, 1, 1, 10, 100, hull.KeepRatio(keep)));
            hull.PutLevel(hull.LastLevelIdx(), hull.MakeSst(1, 2, 2, 11, 100, hull.KeepRatio(keep)));

            auto snap = hull.Ds->GetIndexSnapshot();
            TTask task;
            NHullComp::TSelectorParams params = {hull.Boundaries, 1.0, TInstant::Seconds(0), {}};
            params.FreeChunksBudget = 1;
            params.EmergencyMode = true;
            TStrategy strategy(snap.HullCtx, params, std::move(snap.LogoBlobsSnap), std::move(snap.BarriersSnap),
                    &task, true);
            AssertAction(strategy.Select(), NHullComp::ActCompactSsts);
            AssertStrategy(task.SelectStrategy, NHullComp::ESelectStrategy::Emergency);
            UNIT_ASSERT_VALUES_EQUAL(task.CompactSsts.TargetLevel, hull.LastPhysicalLevel());
            UNIT_ASSERT_VALUES_EQUAL(CountSstsToDelete(task), 2u);
        }

        Y_UNIT_TEST(EmergencyDoesNotPackTwoFullSstsUnderTinyBudget) {
            TSynthHull hull(17);
            const ui64 keep = hull.Ctx.GetHullCtx()->ChunkSize;
            hull.PutLevel(hull.LastLevelIdx(), hull.MakeSst(1, 1, 1, 10, 100, hull.KeepRatio(keep)));
            hull.PutLevel(hull.LastLevelIdx(), hull.MakeSst(1, 2, 2, 11, 100, hull.KeepRatio(keep)));

            auto snap = hull.Ds->GetIndexSnapshot();
            TTask task;
            NHullComp::TSelectorParams params = {hull.Boundaries, 1.0, TInstant::Seconds(0), {}};
            params.FreeChunksBudget = 1;
            params.EmergencyMode = true;
            TStrategyEmergency emergency(snap.HullCtx, params, snap.LogoBlobsSnap, &task);
            AssertAction(emergency.Select(), NHullComp::ActNothing);
        }

        Y_UNIT_TEST(EmergencySkipsWideCrossLevelAndPacksLastLevel) {
            TSynthHull hull(17);
            const ui64 keep = hull.Ctx.GetHullCtx()->ChunkSize / 3;
            // Wide SST on the previous level overlaps every SST on the last level.
            hull.PutLevel(hull.LastLevelIdx() - 1, hull.MakeSst(1, 1, 100, 9, 100, hull.KeepRatio(keep)));
            for (ui32 i = 0; i < 10; ++i) {
                const ui32 step = i + 1;
                hull.PutLevel(hull.LastLevelIdx(), hull.MakeSst(1, step, step, 20 + i, 100, hull.KeepRatio(keep)));
            }

            auto snap = hull.Ds->GetIndexSnapshot();
            TTask task;
            NHullComp::TSelectorParams params = {hull.Boundaries, 1.0, TInstant::Seconds(0), {}};
            params.FreeChunksBudget = 2;
            params.EmergencyMode = true;
            TStrategyEmergency emergency(snap.HullCtx, params, snap.LogoBlobsSnap, &task);
            AssertAction(emergency.Select(), NHullComp::ActCompactSsts);
            UNIT_ASSERT_VALUES_EQUAL(task.CompactSsts.TargetLevel, hull.LastPhysicalLevel());
            UNIT_ASSERT(CountSstsToDelete(task) >= 2);
            UNIT_ASSERT(CountSstsToDelete(task) <= 8);
        }

        Y_UNIT_TEST(BalanceStillSelectedWhenSpaceIsPlenty) {
            TSynthHull hull(17);
            for (ui32 i = 0; i < 20; ++i) {
                hull.PutL0(hull.MakeSst(1, i + 1, i + 1, 100 + i, 100, hull.KeepRatio(hull.Ctx.GetHullCtx()->ChunkSize)));
            }

            auto snap = hull.Ds->GetIndexSnapshot();
            TTask task;
            NHullComp::TSelectorParams params = {hull.Boundaries, 1.0, TInstant::Seconds(0), {}};
            params.FreeChunksBudget = Max<ui32>();
            params.EmergencyMode = false;
            TStrategy strategy(snap.HullCtx, params, std::move(snap.LogoBlobsSnap), std::move(snap.BarriersSnap),
                    &task, true);
            AssertAction(strategy.Select(), NHullComp::ActCompactSsts);
            AssertStrategy(task.SelectStrategy, NHullComp::ESelectStrategy::BalanceLevel);
        }

        Y_UNIT_TEST(DelSstWinsOverEmergencyForFullyDeadSst) {
            TSynthHull hull(17);
            auto deadRatio = hull.KeepRatio(0);
            deadRatio->IndexItemsTotal = 1;
            deadRatio->IndexItemsKeep = 0;
            deadRatio->InplacedDataTotal = 100;
            deadRatio->InplacedDataKeep = 0;
            auto deadSst = hull.MakeSst(1, 1, 1, 10, 100, deadRatio);
            // Stamp the ratio as freshly calculated so TStrategyStorageRatio keeps it.
            deadSst->StorageRatio.Set(deadRatio, TAppData::TimeProvider->Now());
            hull.PutLevel(hull.LastLevelIdx(), deadSst);

            auto snap = hull.Ds->GetIndexSnapshot();
            TTask task;
            NHullComp::TSelectorParams params = {hull.Boundaries, 1.0, TInstant::Seconds(0), {}};
            params.FreeChunksBudget = 1;
            params.EmergencyMode = true;
            TStrategy strategy(snap.HullCtx, params, std::move(snap.LogoBlobsSnap), std::move(snap.BarriersSnap),
                    &task, true);
            AssertAction(strategy.Select(), NHullComp::ActDeleteSsts);
            AssertStrategy(task.SelectStrategy, NHullComp::ESelectStrategy::DelSst);
        }

        Y_UNIT_TEST(EstimateOutputChunksIsConservative) {
            UNIT_ASSERT_VALUES_EQUAL(TUtils::EstimateOutputChunks(0, 4096), 0u);
            UNIT_ASSERT_VALUES_EQUAL(TUtils::EstimateOutputChunks(1, 4096), 1u);
            const ui32 usable = 4096 - sizeof(TIdxDiskPlaceHolder);
            UNIT_ASSERT(TUtils::EstimateOutputChunks(usable, 4096) >= 1);
            UNIT_ASSERT(TUtils::EstimateOutputChunks(usable * 2, 4096) >= 2);
        }
    }

} // NKikimr
