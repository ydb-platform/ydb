#include "hulldb_compstrat_selector.h"
#include "hulldb_compstrat_emergency.h"
#include "hulldb_compstrat_explicit.h"
#include "hulldb_compstrat_ratio.h"
#include "hulldb_compstrat_ranks.h"
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
        using TStrategyExplicit = ::NKikimr::NHullComp::TStrategyExplicit<TKeyLogoBlob, TMemRecLogoBlob>;
        using TTask = ::NKikimr::NHullComp::TTask<TKeyLogoBlob, TMemRecLogoBlob>;
        using TUtils = ::NKikimr::NHullComp::TUtils<TKeyLogoBlob, TMemRecLogoBlob>;
        using TLeveledSstsIterator = TLeveledSsts<TKeyLogoBlob, TMemRecLogoBlob>::TIterator;

        struct TPriorityTestEnv {
            TTestContexts Contexts;
            TIntrusivePtr<THullDs> Ds = MakeIntrusive<THullDs>(Contexts.GetHullCtx());
            NHullComp::TBoundariesConstPtr Boundaries = MakeIntrusive<NHullComp::TBoundaries>(ChunkSize,
                HullCompLevel0MaxSstsAtOnce, HullCompSortedPartsNum, Level0UseDreg);
            ui64 NextSstId = 1;

            TPriorityTestEnv() {
                auto arena = std::make_shared<TRopeArena>(&TRopeArenaBackend::Allocate);
                const auto &settings = Contexts.GetLevelIndexSettings();
                Ds->LogoBlobs = MakeIntrusive<TLogoBlobsDs>(settings, arena);
                Ds->Blocks = MakeIntrusive<TBlocksDs>(settings, arena);
                Ds->Barriers = MakeIntrusive<TBarriersDs>(settings, arena);
                Ds->LogoBlobs->LoadCompleted();
                Ds->Blocks->LoadCompleted();
                Ds->Barriers->LoadCompleted();
            }

            ui64 AddSst(ui32 level, ui32 chunks) {
                auto sst = MakeIntrusive<TLogoBlobsSst>(Contexts.GetVCtx());
                sst->AssignedSstId = NextSstId++;
                sst->AllChunks.resize(chunks, 1);
                TMemRecLogoBlob memRec;
                memRec.SetNoBlob();
                TTrackableVector<TLogoBlobsSst::TRec> index(TMemoryConsumer(Contexts.GetVCtx()->SstIndex));
                index.emplace_back(TKeyLogoBlob(TLogoBlobID(sst->AssignedSstId, 1, 1, 0, 1, 0)), memRec);
                sst->LoadLinearIndex(index);
                auto &slice = *Ds->LogoBlobs->CurSlice;
                if (level == 0) {
                    slice.Level0.Put(sst);
                } else {
                    while (slice.SortedLevels.size() < level) {
                        slice.SortedLevels.emplace_back(TKeyLogoBlob());
                    }
                    slice.SortedLevels[level - 1].Put(sst);
                }
                return sst->AssignedSstId;
            }

            NHullComp::TLevelRanks GetRanks() const {
                auto snap = Ds->LogoBlobs->GetIndexSnapshot();
                return NHullComp::TLevelRanks(*Boundaries, snap.SliceSnap);
            }

            void UpdateRankSensors() const {
                auto snap = Ds->GetIndexSnapshot();
                NHullComp::TSelectorParams params = {Boundaries, 1.0, TInstant::Zero(), {}};
                TTask task;
                TStrategy strategy(snap.HullCtx, params, std::move(snap.LogoBlobsSnap),
                    std::move(snap.BarriersSnap), &task, false);
            }

            void CheckRankSensors(const TString &db, ui64 rank0, ui64 rank1to16, ui64 rank17plus) const {
                auto lsm = Ds->HullCtx->VCtx->VDiskCounters->FindSubgroup("subsystem", "lsmhull");
                UNIT_ASSERT(lsm);
                auto counters = lsm->FindSubgroup("hull_db", db);
                UNIT_ASSERT(counters);
                for (const auto &[name, expected] : {std::make_pair("Rank0", rank0),
                        std::make_pair("Rank1_16", rank1to16), std::make_pair("Rank17_", rank17plus)}) {
                    auto counter = counters->FindCounter(name);
                    UNIT_ASSERT_C(counter, name);
                    UNIT_ASSERT_VALUES_EQUAL_C(counter->Val(), expected, name);
                }
            }

            NHullComp::EAction Select(TTask &task, THashSet<ui64> tablesToCompact = {}, bool emergencyMode = false) {
                auto snap = Ds->GetIndexSnapshot();
                NHullComp::TSelectorParams params = {Boundaries, 1.0, TInstant::Zero(), {}};
                params.EmergencyMode = emergencyMode;
                if (!tablesToCompact.empty()) {
                    params.FullCompactionAttrs.emplace(1, TInstant::Zero(), std::move(tablesToCompact));
                }
                TStrategy strategy(snap.HullCtx, params, std::move(snap.LogoBlobsSnap), std::move(snap.BarriersSnap),
                    &task, false);
                return strategy.Select();
            }
        };

        Y_UNIT_TEST(EmptyLevelRanks) {
            TPriorityTestEnv env;
            const auto ranks = env.GetRanks();
            UNIT_ASSERT_VALUES_EQUAL(ranks.Ranks.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(ranks.GetMaxRank(), 0.0);
            UNIT_ASSERT_VALUES_EQUAL(ranks.FreePartiallySortedLevelsNum, 16);
            UNIT_ASSERT_VALUES_EQUAL(ranks.VirtualLevelToCompact, 0);
        }

        Y_UNIT_TEST(LevelRanksUseChunksAndAllLevels) {
            TPriorityTestEnv env;
            env.AddSst(0, 8);
            env.AddSst(1, 100); // Partially sorted rank depends on occupied levels, not chunks.
            env.AddSst(17, 192);
            env.AddSst(18, 1024);
            const auto ranks = env.GetRanks();
            UNIT_ASSERT_VALUES_EQUAL(ranks.Ranks.size(), 4);
            UNIT_ASSERT_VALUES_EQUAL(ranks.Ranks[0], 0.5);
            UNIT_ASSERT_VALUES_EQUAL(ranks.Ranks[1], 1.0 / 16);
            UNIT_ASSERT_VALUES_EQUAL(ranks.Ranks[2], 1.5);
            UNIT_ASSERT_VALUES_EQUAL(ranks.Ranks[3], 2.0);
            UNIT_ASSERT_VALUES_EQUAL(ranks.GetMaxRank(), 2.0);
            UNIT_ASSERT_VALUES_EQUAL(ranks.VirtualLevelToCompact, 3);
        }

        Y_UNIT_TEST(FullPartiallySortedLevelsRank) {
            TPriorityTestEnv env;
            for (ui32 level = 1; level <= 16; ++level) {
                env.AddSst(level, 1);
            }
            const auto ranks = env.GetRanks();
            UNIT_ASSERT_VALUES_EQUAL(ranks.FreePartiallySortedLevelsNum, 0);
            UNIT_ASSERT_VALUES_EQUAL(ranks.GetMaxRank(), 1000000.0);
            UNIT_ASSERT_VALUES_EQUAL(ranks.VirtualLevelToCompact, 1);
            env.UpdateRankSensors();
            env.CheckRankSensors("LogoBlobs", 0, 100000000, 0);
        }

        Y_UNIT_TEST(RankSensorsTrackMaxAndReset) {
            TPriorityTestEnv env;
            env.AddSst(0, 8);
            env.AddSst(1, 1);
            env.AddSst(17, 192);
            env.AddSst(18, 1024);
            env.AddSst(19, 2048); // Rank17_ is the maximum, not the last level's rank.
            env.GetRanks();
            env.CheckRankSensors("LogoBlobs", 0, 0, 0); // Rank calculation alone does not publish sensors.
            env.UpdateRankSensors();
            env.CheckRankSensors("LogoBlobs", 50, 6, 200);

            env.Ds->LogoBlobs->CurSlice->SortedLevels.clear();
            env.GetRanks();
            env.CheckRankSensors("LogoBlobs", 50, 6, 200);
            env.UpdateRankSensors();
            env.CheckRankSensors("LogoBlobs", 50, 0, 0);
        }

        Y_UNIT_TEST(StrategyConstructorReportsRanksForEachDatabase) {
            TPriorityTestEnv env;
            env.AddSst(0, 8);
            auto blocksSst = MakeIntrusive<TBlocksSst>(env.Contexts.GetVCtx());
            blocksSst->AllChunks.resize(32, 1);
            env.Ds->Blocks->CurSlice->Level0.Put(blocksSst);
            auto barriersSst = MakeIntrusive<TBarriersSst>(env.Contexts.GetVCtx());
            barriersSst->AllChunks.resize(4, 1);
            env.Ds->Barriers->CurSlice->Level0.Put(barriersSst);
            NHullComp::TSelectorParams params = {env.Boundaries, 1.0, TInstant::Zero(), {}};

            // Construction alone publishes gauges; Select() has not run yet.
            {
                auto snap = env.Ds->GetIndexSnapshot();
                TTask task;
                TStrategy strategy(snap.HullCtx, params, std::move(snap.LogoBlobsSnap),
                    std::move(snap.BarriersSnap), &task, false);
                UNIT_ASSERT_VALUES_EQUAL(task.Priority.MaxRank, 0.5);
                env.CheckRankSensors("LogoBlobs", 50, 0, 0);
                env.CheckRankSensors("Blocks", 0, 0, 0);
                env.CheckRankSensors("Barriers", 0, 0, 0);
            }
            {
                auto snap = env.Ds->GetIndexSnapshot();
                NHullComp::TTask<TKeyBlock, TMemRecBlock> task;
                NHullComp::TStrategy<TKeyBlock, TMemRecBlock> strategy(snap.HullCtx, params,
                    std::move(snap.BlocksSnap), std::move(snap.BarriersSnap), &task, false);
                UNIT_ASSERT_VALUES_EQUAL(task.Priority.MaxRank, 2.0);
                env.CheckRankSensors("Blocks", 200, 0, 0);
                env.CheckRankSensors("LogoBlobs", 50, 0, 0);
            }
            {
                auto snap = env.Ds->GetIndexSnapshot();
                auto barriersSnap = env.Ds->Barriers->GetIndexSnapshot();
                NHullComp::TTask<TKeyBarrier, TMemRecBarrier> task;
                NHullComp::TStrategy<TKeyBarrier, TMemRecBarrier> strategy(snap.HullCtx, params,
                    std::move(snap.BarriersSnap), std::move(barriersSnap), &task, false);
                UNIT_ASSERT_VALUES_EQUAL(task.Priority.MaxRank, 0.25);
                env.CheckRankSensors("Barriers", 25, 0, 0);
                env.CheckRankSensors("Blocks", 200, 0, 0);
                env.CheckRankSensors("LogoBlobs", 50, 0, 0);
            }
        }

        Y_UNIT_TEST(RankSensorsUpdatedBeforePromotion) {
            TPriorityTestEnv env;
            env.AddSst(17, 192);
            env.Ds->LogoBlobs->CurSlice->SortedLevels.emplace_back(TKeyLogoBlob());
            TTask task;
            UNIT_ASSERT(env.Select(task) == NHullComp::ActMoveSsts);
            UNIT_ASSERT(task.SelectStrategy == NHullComp::ESelectStrategy::PromoteSsts);
            UNIT_ASSERT_VALUES_EQUAL(task.Priority.MaxRank, 1.5);
            env.CheckRankSensors("LogoBlobs", 0, 0, 150);
        }

        Y_UNIT_TEST(LevelRanksRespectSnapshotBoundary) {
            TPriorityTestEnv env;
            env.AddSst(0, 8);
            auto snap = env.Ds->LogoBlobs->GetIndexSnapshot();
            env.AddSst(0, 16);
            const NHullComp::TLevelRanks oldRanks(*env.Boundaries, snap.SliceSnap);
            UNIT_ASSERT_VALUES_EQUAL(oldRanks.Ranks[0], 0.5);
            UNIT_ASSERT_VALUES_EQUAL(env.GetRanks().Ranks[0], 1.5);
        }

        Y_UNIT_TEST(ExplicitCompactionPriorityTracksBacklog) {
            TPriorityTestEnv env;
            const ui64 explicitSst = env.AddSst(0, 1);
            TTask task;
            for (const double previousPriority : {0.0, 100.0}) {
                task.Priority.MaxRank = previousPriority;
                UNIT_ASSERT(env.Select(task, {explicitSst}) == NHullComp::ActCompactSsts);
                UNIT_ASSERT(task.SelectStrategy == NHullComp::ESelectStrategy::Explicit);
                UNIT_ASSERT_VALUES_EQUAL(task.Priority.MaxRank, 1.0 / 16);
            }

            // The same explicit request is selected again while unrelated SSTs accumulate.
            env.AddSst(0, 32);
            UNIT_ASSERT(env.Select(task, {explicitSst}) == NHullComp::ActCompactSsts);
            UNIT_ASSERT(task.SelectStrategy == NHullComp::ESelectStrategy::Explicit);
            UNIT_ASSERT_VALUES_EQUAL(task.Priority.MaxRank, 33.0 / 16);
            UNIT_ASSERT_VALUES_EQUAL(task.CompactSsts.CompactionChains.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(task.CompactSsts.CompactionChains.front()->Segments.front()->AssignedSstId,
                explicitSst);
            UNIT_ASSERT(!task.FullCompactionInfo.second);

            // Pressure on a different level must also raise the explicit request's priority.
            env.AddSst(17, 512);
            UNIT_ASSERT(env.Select(task, {explicitSst}) == NHullComp::ActCompactSsts);
            UNIT_ASSERT(task.SelectStrategy == NHullComp::ESelectStrategy::Explicit);
            UNIT_ASSERT_VALUES_EQUAL(task.Priority.MaxRank, 4.0);

            // Refresh emergency mode independently of the selected explicit strategy.
            for (const bool emergencyMode : {true, false, true}) {
                UNIT_ASSERT(env.Select(task, {explicitSst}, emergencyMode) == NHullComp::ActCompactSsts);
                UNIT_ASSERT_VALUES_EQUAL(task.Priority.EmergencyMode, emergencyMode);
                UNIT_ASSERT_VALUES_EQUAL(task.Priority.MaxRank, 4.0);
            }

            task.Clear();
            UNIT_ASSERT_VALUES_EQUAL(task.Priority.MaxRank, 0.0);
            UNIT_ASSERT(!task.Priority.EmergencyMode);
        }

        Y_UNIT_TEST(BalanceCompactionUsesCalculatedRanks) {
            TPriorityTestEnv env;
            env.AddSst(0, 16);
            TTask task;
            UNIT_ASSERT(env.Select(task) == NHullComp::ActCompactSsts);
            UNIT_ASSERT(task.SelectStrategy == NHullComp::ESelectStrategy::BalanceLevel);
            UNIT_ASSERT_VALUES_EQUAL(task.Priority.MaxRank, 1.0);
            UNIT_ASSERT_VALUES_EQUAL(task.CompactSsts.TargetLevel, 1);
        }


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

        // Three ssts on one level, each needing about half a chunk of output: one fits a
        // budget of one chunk, all three fit two, and none fits zero.
        struct TExplicitFixture {
            TSynthHull Hull{17};
            THashSet<ui64> Requested;

            TExplicitFixture() {
                const ui64 keep = Hull.Ctx.GetHullCtx()->ChunkSize / 2;
                for (ui32 i = 0; i < 3; ++i) {
                    auto sst = Hull.MakeSst(1, i + 1, i + 1, 10 + i, 100, Hull.KeepRatio(keep));
                    sst->AssignedSstId = 100 + i;
                    Requested.insert(sst->AssignedSstId);
                    Hull.PutLevel(Hull.LastLevelIdx(), sst);
                }
            }

            NHullComp::TSelectorParams Params(ui32 budget, const THashSet<ui64>& ids) const {
                NHullComp::TSelectorParams params = {Hull.Boundaries, 1.0, TInstant::Seconds(0),
                    NHullComp::TFullCompactionAttrs(1, TInstant::Seconds(0), ids)};
                params.FreeChunksBudget = budget;
                return params;
            }
        };

        // An explicit request bigger than the output this VDisk may allocate is cut down to
        // what fits instead of being started and failing its reservation.
        Y_UNIT_TEST(ExplicitTrimsRequestToFreeChunksBudget) {
            TExplicitFixture f;
            auto snap = f.Hull.Ds->GetIndexSnapshot();
            auto params = f.Params(1, f.Requested);

            TTask task;
            task.FullCompactionInfo.first = params.FullCompactionAttrs;
            TStrategyExplicit explicitStrategy(snap.HullCtx, params, snap.LogoBlobsSnap, &task);

            AssertAction(explicitStrategy.Select(), NHullComp::ActCompactSsts);
            AssertStrategy(task.SelectStrategy, NHullComp::ESelectStrategy::Explicit);
            UNIT_ASSERT_VALUES_EQUAL(CountSstsToDelete(task), 1u);
            UNIT_ASSERT_VALUES_EQUAL(task.CompactSsts.TargetLevel, f.Hull.LastPhysicalLevel());
            // The rest of the request is still outstanding.
            UNIT_ASSERT(!task.FullCompactionInfo.second);
        }

        Y_UNIT_TEST(ExplicitTakesEverythingThatFitsTheBudget) {
            TExplicitFixture f;
            auto snap = f.Hull.Ds->GetIndexSnapshot();
            auto params = f.Params(2, f.Requested);

            TTask task;
            task.FullCompactionInfo.first = params.FullCompactionAttrs;
            TStrategyExplicit explicitStrategy(snap.HullCtx, params, snap.LogoBlobsSnap, &task);

            AssertAction(explicitStrategy.Select(), NHullComp::ActCompactSsts);
            AssertStrategy(task.SelectStrategy, NHullComp::ESelectStrategy::Explicit);
            UNIT_ASSERT_VALUES_EQUAL(CountSstsToDelete(task), 3u);
        }

        // No budget reported yet: the request is taken whole, as before.
        Y_UNIT_TEST(ExplicitIgnoresAnUnboundedBudget) {
            TExplicitFixture f;
            auto snap = f.Hull.Ds->GetIndexSnapshot();
            auto params = f.Params(Max<ui32>(), f.Requested);

            TTask task;
            task.FullCompactionInfo.first = params.FullCompactionAttrs;
            TStrategyExplicit explicitStrategy(snap.HullCtx, params, snap.LogoBlobsSnap, &task);

            AssertAction(explicitStrategy.Select(), NHullComp::ActCompactSsts);
            AssertStrategy(task.SelectStrategy, NHullComp::ESelectStrategy::Explicit);
            UNIT_ASSERT_VALUES_EQUAL(CountSstsToDelete(task), 3u);
        }

        // Not even one sst fits: yield so a budgeted emergency compaction can reclaim
        // something, and keep the request pending rather than reporting it finished.
        Y_UNIT_TEST(ExplicitYieldsWhenNothingFitsAndStaysPending) {
            TExplicitFixture f;
            auto snap = f.Hull.Ds->GetIndexSnapshot();
            auto params = f.Params(0, f.Requested);

            TTask task;
            task.FullCompactionInfo.first = params.FullCompactionAttrs;
            TStrategyExplicit explicitStrategy(snap.HullCtx, params, snap.LogoBlobsSnap, &task);

            AssertAction(explicitStrategy.Select(), NHullComp::ActNothing);
            AssertStrategy(task.SelectStrategy, NHullComp::ESelectStrategy::None);
            UNIT_ASSERT(!task.FullCompactionInfo.second);
        }

        // The requested ssts are gone from the index, so the request really is done -- a
        // tight budget must not be confused with this case.
        Y_UNIT_TEST(ExplicitReportsDoneWhenRequestedSstsAreGone) {
            TExplicitFixture f;
            auto snap = f.Hull.Ds->GetIndexSnapshot();
            auto params = f.Params(0, THashSet<ui64>{999});

            TTask task;
            task.FullCompactionInfo.first = params.FullCompactionAttrs;
            TStrategyExplicit explicitStrategy(snap.HullCtx, params, snap.LogoBlobsSnap, &task);

            AssertAction(explicitStrategy.Select(), NHullComp::ActNothing);
            AssertStrategy(task.SelectStrategy, NHullComp::ESelectStrategy::None);
            UNIT_ASSERT(task.FullCompactionInfo.second);
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
            AssertStrategy(task.SelectStrategy, NHullComp::ESelectStrategy::Emergency);
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
            UNIT_ASSERT_VALUES_EQUAL(task.Priority.MaxRank, 2.0 / 128);
            UNIT_ASSERT(task.Priority.EmergencyMode);
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
            AssertStrategy(task.SelectStrategy, NHullComp::ESelectStrategy::None);
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
            AssertStrategy(task.SelectStrategy, NHullComp::ESelectStrategy::Emergency);
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

        // Whatever picked the job, it has to say what it will cost: that number is what a
        // later admission step can hand out, and what the job can reserve from PDisk.
        Y_UNIT_TEST(EmergencyPublishesItsMeasuredForecast) {
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
            UNIT_ASSERT(task.Forecast.Valid);
            UNIT_ASSERT_VALUES_EQUAL(task.Forecast.InputChunks, 2u);
            UNIT_ASSERT_VALUES_EQUAL(task.Forecast.OutputChunks, 1u);
            // Two chunks in, one out: worth admitting even on a disk with little room left.
            UNIT_ASSERT_VALUES_EQUAL(task.Forecast.NetChunks(), 1);
        }

        Y_UNIT_TEST(ExplicitJobCarriesAForecast) {
            TExplicitFixture f;
            auto snap = f.Hull.Ds->GetIndexSnapshot();
            auto params = f.Params(2, f.Requested);

            TTask task;
            TStrategy strategy(snap.HullCtx, params, std::move(snap.LogoBlobsSnap), std::move(snap.BarriersSnap),
                    &task, true);
            AssertAction(strategy.Select(), NHullComp::ActCompactSsts);
            AssertStrategy(task.SelectStrategy, NHullComp::ESelectStrategy::Explicit);
            UNIT_ASSERT(task.Forecast.Valid);
            UNIT_ASSERT_VALUES_EQUAL(task.Forecast.InputChunks, CountSstsToDelete(task));
            UNIT_ASSERT(task.Forecast.OutputChunks >= 1);
            // The selection already trimmed itself to the budget, so the forecast does not
            // ask for more than that budget.
            UNIT_ASSERT(task.Forecast.OutputChunks <= params.FreeChunksBudget);
        }

        Y_UNIT_TEST(NoForecastWithoutAJobThatWritesChunks) {
            TSynthHull hull(17);
            auto snap = hull.Ds->GetIndexSnapshot();
            TTask task;
            NHullComp::TSelectorParams params = {hull.Boundaries, 1.0, TInstant::Seconds(0), {}};
            TStrategy strategy(snap.HullCtx, params, std::move(snap.LogoBlobsSnap), std::move(snap.BarriersSnap),
                    &task, true);
            AssertAction(strategy.Select(), NHullComp::ActNothing);
            UNIT_ASSERT(!task.Forecast.Valid);
        }

        Y_UNIT_TEST(EstimateOutputChunksIsConservative) {
            UNIT_ASSERT_VALUES_EQUAL(TUtils::EstimateOutputChunks(0, 4096), 0u);
            UNIT_ASSERT_VALUES_EQUAL(TUtils::EstimateOutputChunks(1, 4096), 1u);
            const ui32 usable = 4096 - sizeof(TIdxDiskPlaceHolder);
            UNIT_ASSERT(TUtils::EstimateOutputChunks(usable, 4096) >= 1);
            UNIT_ASSERT(TUtils::EstimateOutputChunks(usable * 2, 4096) >= 2);
        }

        // Same packing as EstimateOutputStripeBlocks: 10% slack, one placeholder per stripe,
        // each stripe aligned up to an append block.
        ui32 ExpectedStripeBlocks(ui64 keepBytes, ui32 append, ui32 maxStripe) {
            const ui32 capacity = static_cast<ui32>(
                (ui64(maxStripe) + append - 1) / append * append);
            const ui32 suffix = sizeof(TIdxDiskPlaceHolder);
            const ui32 usable = capacity > suffix ? capacity - suffix : 0;
            const ui64 withSlack = keepBytes + keepBytes / 10;
            ui32 blocks = 0;
            ui64 left = withSlack;
            while (left) {
                const ui64 piece = Min<ui64>(left, usable);
                blocks += static_cast<ui32>((piece + suffix + append - 1) / append);
                left -= piece;
            }
            return blocks;
        }

        using TBlockUtils = ::NKikimr::NHullComp::TUtils<TKeyBlock, TMemRecBlock>;
        using TBlockTask = ::NKikimr::NHullComp::TTask<TKeyBlock, TMemRecBlock>;

        TBlocksSstPtr MakeBareSst(TSynthHull &hull, ui32 idxBytes) {
            auto sst = MakeIntrusive<TBlocksSst>(hull.Ctx.GetVCtx());
            sst->Info.IdxTotalSize = idxBytes;
            sst->Info.Chunks = 1;
            sst->Info.Items = 1;
            sst->Info.FirstLsn = 1;
            sst->Info.LastLsn = 1;
            return sst;
        }

        // A stripe SST shares its chunk. The forecast must release the extent, in
        // append blocks, and must not promise the chunk back.
        Y_UNIT_TEST(StripeInputReleasesBlocksNotTheChunk) {
            TSynthHull hull(1);
            const ui32 append = 4096;
            auto chunkSst = MakeBareSst(hull, 100);
            chunkSst->AllChunks.push_back(3);

            auto stripeSst = MakeBareSst(hull, 100);
            stripeSst->AllChunks.push_back(7);
            stripeSst->HeapStripe = TDiskPart(7, append, append * 2 + 10);

            TBlockTask task;
            task.CompactSsts.PushOneSst(1, chunkSst);
            task.CompactSsts.PushOneSst(1, stripeSst);

            const auto forecast = TBlockUtils::ForecastCompactSsts(task.CompactSsts, ChunkSize, append, 0);
            UNIT_ASSERT(forecast.Valid);
            UNIT_ASSERT_VALUES_EQUAL(forecast.InputChunks, 1u);
            UNIT_ASSERT_VALUES_EQUAL(forecast.StripeBlocksReleased, 3u);
            UNIT_ASSERT(forecast.OutputChunks >= 1);
            UNIT_ASSERT_VALUES_EQUAL(forecast.StripeBlocksAllocated, 0u);

            // No append block size: the extent still is not an exclusive chunk, and
            // the block count stays unknown rather than a guessed 1.
            const auto unknown = TBlockUtils::ForecastCompactSsts(task.CompactSsts, ChunkSize, 0, 0);
            UNIT_ASSERT_VALUES_EQUAL(unknown.InputChunks, 1u);
            UNIT_ASSERT_VALUES_EQUAL(unknown.StripeBlocksReleased, 0u);
        }

        Y_UNIT_TEST(StripeReleaseRoundsUpToAppendBlock) {
            UNIT_ASSERT_VALUES_EQUAL(TBlockUtils::SstReleasedStripeBlocks(
                *MakeIntrusive<TBlocksSst>(TTestContexts().GetVCtx()), 4096), 0u);

            TSynthHull hull(1);
            auto exact = MakeBareSst(hull, 1);
            exact->HeapStripe = TDiskPart(1, 0, 8192);
            UNIT_ASSERT_VALUES_EQUAL(TBlockUtils::SstReleasedStripeBlocks(*exact, 4096), 2u);

            auto over = MakeBareSst(hull, 1);
            over->HeapStripe = TDiskPart(1, 0, 8193);
            UNIT_ASSERT_VALUES_EQUAL(TBlockUtils::SstReleasedStripeBlocks(*over, 4096), 3u);
        }

        // Blocks/Barriers output goes into the stripe heap. LogoBlobs do not, even if
        // a stripe size is passed in.
        Y_UNIT_TEST(StripeOutputIsBlocksForBlocksAndChunksForLogoBlobs) {
            TSynthHull hull(1);
            const ui32 append = 128;
            const ui32 maxStripe = 1u << 20;
            const ui32 keep = 100;
            auto sst = MakeBareSst(hull, keep);
            sst->AllChunks.push_back(4);
            sst->HeapStripe = TDiskPart(4, 0, append); // one block in

            TBlockTask task;
            task.CompactSsts.PushOneSst(1, sst);
            const auto striped = TBlockUtils::ForecastCompactSsts(task.CompactSsts, ChunkSize, append, maxStripe);
            UNIT_ASSERT_VALUES_EQUAL(striped.OutputChunks, 0u);
            UNIT_ASSERT_VALUES_EQUAL(striped.InputChunks, 0u);
            UNIT_ASSERT_VALUES_EQUAL(striped.StripeBlocksReleased, 1u);
            // 110 bytes of slack plus the placeholder is more than one 128-byte block
            // and less than two, so a missing placeholder or a missed round-up shows up.
            const ui64 written = ui64(keep + keep / 10) + sizeof(TIdxDiskPlaceHolder);
            UNIT_ASSERT(written > append && written <= append * 2);
            UNIT_ASSERT_VALUES_EQUAL(striped.StripeBlocksAllocated, (written + append - 1) / append);

            // Larger than one stripe: several extents, still no exclusive chunk.
            const ui32 tinyStripe = append * 4;
            const ui32 manyKeep = 4000;
            const ui32 many = ExpectedStripeBlocks(manyKeep, append, tinyStripe);
            UNIT_ASSERT(many > tinyStripe / append);
            auto fat = MakeBareSst(hull, manyKeep);
            fat->HeapStripe = TDiskPart(4, 0, append);
            TBlockTask fatTask;
            fatTask.CompactSsts.PushOneSst(1, fat);
            const auto splitForecast = TBlockUtils::ForecastCompactSsts(
                fatTask.CompactSsts, ChunkSize, append, tinyStripe);
            UNIT_ASSERT_VALUES_EQUAL(splitForecast.OutputChunks, 0u);
            UNIT_ASSERT_VALUES_EQUAL(splitForecast.StripeBlocksAllocated, many);
            UNIT_ASSERT_VALUES_EQUAL(splitForecast.StripeBlocksReleased, 1u);

            TTask logoTask;
            auto logo = hull.MakeSst(1, 1, 1, 9, 100, hull.KeepRatio(keep));
            logo->HeapStripe = TDiskPart(9, 0, append * 3);
            logoTask.CompactSsts.PushOneSst(1, logo);
            const auto logoForecast = TUtils::ForecastCompactSsts(logoTask.CompactSsts, ChunkSize, append, maxStripe);
            UNIT_ASSERT(logoForecast.OutputChunks >= 1);
            UNIT_ASSERT_VALUES_EQUAL(logoForecast.StripeBlocksAllocated, 0u);
            UNIT_ASSERT_VALUES_EQUAL(logoForecast.InputChunks, 0u);
            UNIT_ASSERT_VALUES_EQUAL(logoForecast.StripeBlocksReleased, 3u);
        }

        // The selector publishes the stripe numbers, and a job that only writes a
        // stripe is not refused for lack of exclusive chunks.
        Y_UNIT_TEST(BlocksJobForecastLivesInTheStripe) {
            TSynthHull hull(17);
            for (ui32 i = 0; i < 17; ++i) {
                hull.Ds->Blocks->CurSlice->SortedLevels.push_back(
                    TSortedLevel<TKeyBlock, TMemRecBlock>(TKeyBlock()));
            }
            const ui32 append = 4096;
            auto sst = MakeBareSst(hull, 100);
            TTrackableVector<TBlocksSst::TRec> index(TMemoryConsumer(hull.Ctx.GetVCtx()->SstIndex));
            index.emplace_back(TKeyBlock(1), TMemRecBlock(1));
            sst->LoadedIndex = std::move(index);
            sst->AssignedSstId = 42;
            sst->AllChunks.push_back(7);
            sst->HeapStripe = TDiskPart(7, 0, append * 2);
            auto &level = hull.Ds->Blocks->CurSlice->SortedLevels[hull.Ds->Blocks->CurSlice->SortedLevels.size() - 1];
            level.Put(sst);

            auto snap = hull.Ds->GetIndexSnapshot();
            THashSet<ui64> ids{42};
            NHullComp::TSelectorParams params = {hull.Boundaries, 1.0, TInstant::Seconds(0),
                NHullComp::TFullCompactionAttrs(1, TInstant::Seconds(0), ids)};
            params.FreeChunksBudget = 0;
            params.AppendBlockSize = append;
            params.StripeSstBytes = 1u << 20;

            using TBlockStrategy = ::NKikimr::NHullComp::TStrategy<TKeyBlock, TMemRecBlock>;
            TBlockTask task;
            TBlockStrategy strategy(snap.HullCtx, params, std::move(snap.BlocksSnap), std::move(snap.BarriersSnap),
                &task, true);
            AssertAction(strategy.Select(), NHullComp::ActCompactSsts);
            UNIT_ASSERT(task.Forecast.Valid);
            UNIT_ASSERT_VALUES_EQUAL(task.Forecast.InputChunks, 0u);
            UNIT_ASSERT_VALUES_EQUAL(task.Forecast.OutputChunks, 0u);
            UNIT_ASSERT_VALUES_EQUAL(task.Forecast.StripeBlocksReleased, 2u);
            UNIT_ASSERT_VALUES_EQUAL(task.Forecast.StripeBlocksAllocated, ExpectedStripeBlocks(100, append, 1u << 20));
            UNIT_ASSERT_VALUES_EQUAL(task.Forecast.StripeBlocksAllocated, 1u);

            // Allocator off: the same SST is rewritten into an exclusive chunk, which
            // a zero budget cannot pay for, so the job is not started.
            params.StripeSstBytes = 0;
            auto snap2 = hull.Ds->GetIndexSnapshot();
            TBlockTask chunkTask;
            TBlockStrategy chunkStrategy(snap2.HullCtx, params, std::move(snap2.BlocksSnap),
                std::move(snap2.BarriersSnap), &chunkTask, true);
            AssertAction(chunkStrategy.Select(), NHullComp::ActNothing);
            UNIT_ASSERT(!chunkTask.Forecast.Valid);
        }

        // Emergency measures the candidate itself. A stripe SST in that set must
        // show up as released blocks, not as another input chunk.
        Y_UNIT_TEST(EmergencyForecastSplitsStripeFromChunks) {
            TSynthHull hull(17);
            const ui32 append = 4096;
            const ui64 keep = hull.Ctx.GetHullCtx()->ChunkSize / 3;
            hull.PutLevel(hull.LastLevelIdx(), hull.MakeSst(1, 1, 1, 10, 100, hull.KeepRatio(keep)));
            auto stripe = hull.MakeSst(1, 2, 2, 11, 100, hull.KeepRatio(1000));
            stripe->HeapStripe = TDiskPart(11, 0, append * 3);
            hull.PutLevel(hull.LastLevelIdx(), stripe);
            hull.PutLevel(hull.LastLevelIdx(), hull.MakeSst(1, 3, 3, 12, 100, hull.KeepRatio(keep)));

            auto snap = hull.Ds->GetIndexSnapshot();
            TTask task;
            NHullComp::TSelectorParams params = {hull.Boundaries, 1.0, TInstant::Seconds(0), {}};
            params.FreeChunksBudget = 1;
            params.EmergencyMode = true;
            params.AppendBlockSize = append;

            TStrategy strategy(snap.HullCtx, params, std::move(snap.LogoBlobsSnap), std::move(snap.BarriersSnap),
                &task, true);
            AssertAction(strategy.Select(), NHullComp::ActCompactSsts);
            AssertStrategy(task.SelectStrategy, NHullComp::ESelectStrategy::Emergency);
            UNIT_ASSERT(task.Forecast.Valid);
            UNIT_ASSERT_VALUES_EQUAL(task.Forecast.InputChunks, 2u);
            UNIT_ASSERT_VALUES_EQUAL(task.Forecast.OutputChunks, 1u);
            UNIT_ASSERT_VALUES_EQUAL(task.Forecast.StripeBlocksReleased, 3u);
            UNIT_ASSERT_VALUES_EQUAL(task.Forecast.StripeBlocksAllocated, 0u);
            UNIT_ASSERT_VALUES_EQUAL(task.Forecast.NetChunks(), 1);
        }
    }

} // NKikimr
