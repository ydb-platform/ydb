#include "hulldb_compstrat_selector.h"
#include "hulldb_compstrat_ratio.h"
#include "hulldb_compstrat_ratio_singlepass.h"
#include <ydb/core/blobstorage/vdisk/defrag/defrag_search.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_hugeblobctx.h>
#include <util/stream/null.h>
#include <map>
#include <ydb/core/blobstorage/vdisk/hulldb/test/testhull_index.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_ut.h>
#include <library/cpp/testing/unittest/registar.h>

#define STR     Cnull

namespace NKikimr {

    Y_UNIT_TEST_SUITE(TBlobStorageCompStrat) {

        static constexpr ui64 ChunkSize = 128u << 20u;
        static constexpr ui32 HullCompLevel0MaxSstsAtOnce = 8u;
        static constexpr ui32 HullCompSortedPartsNum = 8u;
        static constexpr bool Level0UseDreg = true;
        using TStrategy = ::NKikimr::NHullComp::TStrategy<TKeyLogoBlob, TMemRecLogoBlob>;
        using TTask = ::NKikimr::NHullComp::TTask<TKeyLogoBlob, TMemRecLogoBlob>;


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

        // The single-pass collector must produce per-SST TSstRatio field-identical to the legacy
        // per-SST TStrategyStorageRatio::CalculateSstRatio() walk.
        Y_UNIT_TEST(SinglePassRatioEquivalence) {
            using TSegment = ::NKikimr::TLevelSegment<TKeyLogoBlob, TMemRecLogoBlob>;
            using TLevelSliceSnapshot = ::NKikimr::TLevelSliceSnapshot<TKeyLogoBlob, TMemRecLogoBlob>;
            using TSstIterator = TLevelSliceSnapshot::TSstIterator;

            TIntrusivePtr<THullDs> ds = NTest::GenerateDs_17Level_Logs();
            auto snap = ds->GetIndexSnapshot();

            auto forEachSst = [&](auto&& fn) {
                TSstIterator it(&snap.LogoBlobsSnap.SliceSnap);
                it.SeekToFirst();
                while (it.Valid()) {
                    fn(it.Get().SstPtr.Get());
                    it.Next();
                }
            };

            // The jittered scheduler (NBYDB-1732) treats freshly created SSTs (Info.CTime ~ now) as still
            // fresh for calcPeriod, so Work() would never recompute the generated snapshot. Mark every SST as
            // computed long ago so the legacy per-SST walk actually runs and gives us the baseline to compare.
            forEachSst([&](const TSegment* sst) {
                const_cast<TSegment*>(sst)->StorageRatio.SetCalculationTime(TInstant::Seconds(1));
            });

            // Legacy: run Work() until every SST has a computed ratio. Work() throttles each call to
            // HullCompStorageRatioMaxCalcDuration (1s in test ctx), so a large snapshot may need several
            // calls; already-computed SSTs stay fresh (calcPeriod 5min) and are skipped.
            for (int iter = 0; iter < 100; ++iter) {
                bool allComputed = true;
                forEachSst([&](const TSegment* sst) {
                    if (sst->StorageRatio.GetTime() == TInstant()) {
                        allComputed = false;
                    }
                });
                if (allComputed) {
                    break;
                }
                auto essence = snap.BarriersSnap.CreateEssence(snap.HullCtx);
                NHullComp::TStrategyStorageRatio<TKeyLogoBlob, TMemRecLogoBlob>(
                    snap.HullCtx, snap.LogoBlobsSnap, std::move(essence), true).Work();
            }

            std::map<const TSegment*, NHullComp::TSstRatio> legacy;
            forEachSst([&](const TSegment* sst) {
                auto r = sst->StorageRatio.Get();
                UNIT_ASSERT_C(r, "SST ratio was not computed by legacy Work()");
                legacy[sst] = *r;
            });
            UNIT_ASSERT_GT(legacy.size(), 0u);

            // Single-pass
            auto essence = snap.BarriersSnap.CreateEssence(snap.HullCtx);
            NHullComp::TSinglePassRatioCollector<TKeyLogoBlob, TMemRecLogoBlob> coll(
                snap.HullCtx, snap.LogoBlobsSnap, std::move(essence), true);
            coll.Run();
            const auto& single = coll.GetRatios();

            // Compare field-by-field. Absent-in-single is treated as all-zero (empty SST).
            for (const auto& [sst, lr] : legacy) {
                NHullComp::TSstRatio sr;
                if (auto it = single.find(sst); it != single.end()) {
                    sr = *it->second;
                }
                UNIT_ASSERT_VALUES_EQUAL(sr.IndexItemsTotal,   lr.IndexItemsTotal);
                UNIT_ASSERT_VALUES_EQUAL(sr.IndexItemsKeep,    lr.IndexItemsKeep);
                UNIT_ASSERT_VALUES_EQUAL(sr.IndexBytesTotal,   lr.IndexBytesTotal);
                UNIT_ASSERT_VALUES_EQUAL(sr.IndexBytesKeep,    lr.IndexBytesKeep);
                UNIT_ASSERT_VALUES_EQUAL(sr.InplacedDataTotal, lr.InplacedDataTotal);
                UNIT_ASSERT_VALUES_EQUAL(sr.InplacedDataKeep,  lr.InplacedDataKeep);
                UNIT_ASSERT_VALUES_EQUAL(sr.HugeDataTotal,     lr.HugeDataTotal);
                UNIT_ASSERT_VALUES_EQUAL(sr.HugeDataKeep,      lr.HugeDataKeep);
            }
            // No extra SSTs invented by single-pass.
            for (const auto& [sst, r] : single) {
                UNIT_ASSERT_C(legacy.count(sst), "single-pass produced a ratio for an unknown SST");
            }

            // The combined defrag+ratio collector (TDefragCalcStatWithRatio) must produce the SAME per-SST
            // ratios during its single walk (this validates the TDefragScanner CRTP hooks fire correctly).
            auto hugeBlobCtx = std::make_shared<THugeBlobCtx>(nullptr, true);
            TDefragCalcStatWithRatio combined(ds->GetIndexSnapshot(), hugeBlobCtx, /*produceRatios=*/true);
            combined.Scan(TDuration::Max());
            const auto& combinedRatios = combined.GetRatios();
            for (const auto& [sst, lr] : legacy) {
                NHullComp::TSstRatio cr;
                if (auto it = combinedRatios.find(sst); it != combinedRatios.end()) {
                    cr = *it->second;
                }
                UNIT_ASSERT_VALUES_EQUAL(cr.IndexItemsTotal,   lr.IndexItemsTotal);
                UNIT_ASSERT_VALUES_EQUAL(cr.IndexItemsKeep,    lr.IndexItemsKeep);
                UNIT_ASSERT_VALUES_EQUAL(cr.IndexBytesTotal,   lr.IndexBytesTotal);
                UNIT_ASSERT_VALUES_EQUAL(cr.IndexBytesKeep,    lr.IndexBytesKeep);
                UNIT_ASSERT_VALUES_EQUAL(cr.InplacedDataTotal, lr.InplacedDataTotal);
                UNIT_ASSERT_VALUES_EQUAL(cr.InplacedDataKeep,  lr.InplacedDataKeep);
                UNIT_ASSERT_VALUES_EQUAL(cr.HugeDataTotal,     lr.HugeDataTotal);
                UNIT_ASSERT_VALUES_EQUAL(cr.HugeDataKeep,      lr.HugeDataKeep);
            }
        }

    }

} // NKikimr
