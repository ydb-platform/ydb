#include "hulldb_compstrat_selector.h"
#include "hulldb_compstrat_ratio.h"
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

        Y_UNIT_TEST(CheckDefragCalculationRatioEqCompactionSelectorCalculaation) {
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

            forEachSst([&](const TSegment* sst) {
                const_cast<TSegment*>(sst)->StorageRatio.SetCalculationTime(TInstant::Seconds(1));
            });

            // compaction selector calculation
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

            std::map<const TSegment*, NHullComp::TSstRatio> selectorRatios;
            forEachSst([&](const TSegment* sst) {
                auto r = sst->StorageRatio.Get();
                UNIT_ASSERT_C(r, "SST ratio was not computed by the compaction selector");
                selectorRatios[sst] = *r;
            });
            UNIT_ASSERT_GT(selectorRatios.size(), 0u);

            // calculation during defrag
            auto hugeBlobCtx = std::make_shared<THugeBlobCtx>("", nullptr, EBlobHeaderMode::OLD_HEADER);
            TDefragCalcStatWithRatio defragCalc(ds->GetIndexSnapshot(), hugeBlobCtx, /*produceRatios=*/true);
            defragCalc.Scan(TDuration::Max());
            const auto& defragRatios = defragCalc.GetRatios();

            // Check equality of the two calculations
            for (const auto& [sst, selectorRatio] : selectorRatios) {
                NHullComp::TSstRatio defragRatio;
                if (auto it = defragRatios.find(sst); it != defragRatios.end()) {
                    defragRatio = *it->second;
                }
                UNIT_ASSERT_VALUES_EQUAL(defragRatio.IndexItemsTotal,   selectorRatio.IndexItemsTotal);
                UNIT_ASSERT_VALUES_EQUAL(defragRatio.IndexItemsKeep,    selectorRatio.IndexItemsKeep);
                UNIT_ASSERT_VALUES_EQUAL(defragRatio.IndexBytesTotal,   selectorRatio.IndexBytesTotal);
                UNIT_ASSERT_VALUES_EQUAL(defragRatio.IndexBytesKeep,    selectorRatio.IndexBytesKeep);
                UNIT_ASSERT_VALUES_EQUAL(defragRatio.InplacedDataTotal, selectorRatio.InplacedDataTotal);
                UNIT_ASSERT_VALUES_EQUAL(defragRatio.InplacedDataKeep,  selectorRatio.InplacedDataKeep);
                UNIT_ASSERT_VALUES_EQUAL(defragRatio.HugeDataTotal,     selectorRatio.HugeDataTotal);
                UNIT_ASSERT_VALUES_EQUAL(defragRatio.HugeDataKeep,      selectorRatio.HugeDataKeep);
            }
            for (const auto& [sst, r] : defragRatios) {
                UNIT_ASSERT_C(selectorRatios.count(sst), "calculation during defrag produced a ratio for an unknown SST");
            }
        }

    }

} // NKikimr
