#include "fresh_data.h"
#include "fresh_datasnap.h"
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_ut.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/blobstorage_blob.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_logoblob.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/null.h>

// change to Cerr if you want logging
#define STR Cnull

namespace NKikimr {

    static std::shared_ptr<TRopeArena> Arena = std::make_shared<TRopeArena>(&TRopeArenaBackend::Allocate);

    Y_UNIT_TEST_SUITE(TBlobStorageHullFresh) {

        ////////////////////////////////////////////////////////////////////////////////////////
        // Definitions
        ////////////////////////////////////////////////////////////////////////////////////////
        typedef TFreshData<TKeyLogoBlob, TMemRecLogoBlob> TFreshData;
        typedef std::shared_ptr<TFreshData> TFreshDataPtr;
        static const ui32 ChunkSize = 8u << 20u;
        static const ui32 Level0MaxSstsAtOnce = 8u;
        static const ui32 BufSize = 64u << 20u;
        static const ui64 CompThreshold = 8u << 20u;
        static const ui32 CompWorthReadSize = 2u << 20u;
        static const TDuration HistoryWindow = TDuration::Minutes(10);
        static const ui64 HistoryBuckets = 10u;
        static const char Data[] = "hello";
        static TTestContexts TestCtx(ChunkSize, CompWorthReadSize);

        TLevelIndexSettings GetLevelIndexSetting() {
            static TLevelIndexSettings settings(TestCtx.GetHullCtx(),
                    Level0MaxSstsAtOnce, BufSize, CompThreshold, HistoryWindow, HistoryBuckets, false, false);
            return settings;
        }

        ////////////////////////////////////////////////////////////////////////////////////////
        // Generate different datasets
        ////////////////////////////////////////////////////////////////////////////////////////

        // Simple dataset
        struct TDatasetSimple {
            TLogoBlobID LogoBlob1;
            TLogoBlobID LogoBlob2;
            TLogoBlobID LogoBlob3;
            TMemRecLogoBlob MemRec1;
            TMemRecLogoBlob MemRec2;
            TMemRecLogoBlob MemRec3;
            TFreshDataPtr Fresh;

            std::unique_ptr<TFreshData::TFreshDataSnapshot> Snap3;
            std::unique_ptr<TFreshData::TFreshDataSnapshot> Snap10;

            TDatasetSimple() {
                LogoBlob1 = TLogoBlobID(0, 1, 1, 0, sizeof(Data), 0, 1);
                LogoBlob2 = TLogoBlobID(0, 1, 2, 0, sizeof(Data), 0, 1);
                LogoBlob3 = TLogoBlobID(0, 2, 1, 0, sizeof(Data), 0, 1);

                Fresh = std::make_shared<TFreshData>(GetLevelIndexSetting(), CreateDefaultTimeProvider(), Arena);

                // several puts
                PutToFresh(1, LogoBlob1);
                PutToFresh(2, LogoBlob2);
                PutToFresh(3, LogoBlob3);

                Snap3 = std::make_unique<TFreshData::TFreshDataSnapshot>(Fresh->GetSnapshot());
                Snap10 = std::make_unique<TFreshData::TFreshDataSnapshot>(Fresh->GetSnapshot());
            }

            void PutToFresh(ui64 lsn, const TLogoBlobID &key) {
                TRope buffer(Data);
                Fresh->PutLogoBlobWithData(lsn, TKeyLogoBlob(key), key.PartId(), TIngress(), std::move(buffer));
            }
        };

        TDatasetSimple DsSimple;

        // Simple dataset, put 2 times with different lsns
        struct TDatasetSimple2Times : public TDatasetSimple {
            TDatasetSimple2Times()
                : TDatasetSimple()
            {
                PutToFresh(5, LogoBlob1);
                PutToFresh(6, LogoBlob2);
                PutToFresh(7, LogoBlob3);
                Snap10 = std::make_unique<TFreshData::TFreshDataSnapshot>(Fresh->GetSnapshot());
            }
        };

        TDatasetSimple2Times DsSimple2Times;


        ////////////////////////////////////////////////////////////////////////////////////////
        // Test SimpleForward
        ////////////////////////////////////////////////////////////////////////////////////////
        Y_UNIT_TEST(SimpleForward) {
            const TFreshData::TFreshDataSnapshot &snapshot = *DsSimple.Snap3;
            THullCtxPtr hullCtx = TestCtx.GetHullCtx();
            TFreshData::TFreshDataSnapshot::TForwardIterator it(hullCtx, &snapshot);

            it.Seek(DsSimple.LogoBlob1);
            UNIT_ASSERT_EQUAL(it.GetCurKey(), DsSimple.LogoBlob1);
            it.Next();
            UNIT_ASSERT(it.Valid());
            UNIT_ASSERT_EQUAL(it.GetCurKey(), DsSimple.LogoBlob2);
            it.Next();
            UNIT_ASSERT(it.Valid());
            UNIT_ASSERT_EQUAL(it.GetCurKey(), DsSimple.LogoBlob3);
            it.Next();
            UNIT_ASSERT(!it.Valid());
        }


        ////////////////////////////////////////////////////////////////////////////////////////
        // Test Simple2TimesBackward
        ////////////////////////////////////////////////////////////////////////////////////////
        template <class TDataset>
        void SimpleBackwardEnd(TDataset &ds) {
            const TFreshData::TFreshDataSnapshot &snapshot = *ds.Snap10;
            THullCtxPtr hullCtx = TestCtx.GetHullCtx();
            TFreshData::TFreshDataSnapshot::TBackwardIterator it(hullCtx, &snapshot);

            TLogoBlobID last(0, 4294967295, 4294967295, 0,
                TLogoBlobID::MaxBlobSize, TLogoBlobID::MaxCookie, TLogoBlobID::MaxPartId);
            /*
            it.Seek(last);
            while (!it.Empty()) {
                fprintf(stderr, "%s\n", ~it.GetCurKey().ToString());
                it.Prev();
            }
            */

            it.Seek(last);
            UNIT_ASSERT(it.Valid());
            UNIT_ASSERT_EQUAL(it.GetCurKey(), ds.LogoBlob3);
            it.Prev();
            UNIT_ASSERT(it.Valid());
            UNIT_ASSERT_EQUAL(it.GetCurKey(), ds.LogoBlob2);
            it.Prev();
            UNIT_ASSERT(it.Valid());
            UNIT_ASSERT_EQUAL(it.GetCurKey(), ds.LogoBlob1);
            it.Prev();
            UNIT_ASSERT(!it.Valid());
        }

        template <class TDataset>
        void SimpleBackwardMiddle(TDataset &ds) {
            const TFreshData::TFreshDataSnapshot &snapshot = *ds.Snap10;
            THullCtxPtr hullCtx = TestCtx.GetHullCtx();
            TFreshData::TFreshDataSnapshot::TBackwardIterator it(hullCtx, &snapshot);

            TLogoBlobID last(0, 1, 2, 0,
                TLogoBlobID::MaxBlobSize, TLogoBlobID::MaxCookie, TLogoBlobID::MaxPartId);
/*
            it.Seek(last);
            while (!it.Empty()) {
                fprintf(stderr, "%s\n", ~it.GetCurKey().ToString());
                it.Prev();
            }
*/
            it.Seek(last);
            UNIT_ASSERT(it.Valid());
            UNIT_ASSERT_EQUAL(it.GetCurKey(), ds.LogoBlob2);
            it.Prev();
            UNIT_ASSERT(it.Valid());
            UNIT_ASSERT_EQUAL(it.GetCurKey(), ds.LogoBlob1);
            it.Prev();
            UNIT_ASSERT(!it.Valid());
        }


        Y_UNIT_TEST(SimpleBackwardEnd) {
            SimpleBackwardEnd<TDatasetSimple>(DsSimple);
        }

        Y_UNIT_TEST(SimpleBackWardEnd2Times) {
            SimpleBackwardEnd<TDatasetSimple2Times>(DsSimple2Times);
        }

        Y_UNIT_TEST(SimpleBackwardMiddle) {
            SimpleBackwardMiddle<TDatasetSimple>(DsSimple);
        }

        Y_UNIT_TEST(SimpleBackWardMiddle2Times) {
            SimpleBackwardMiddle<TDatasetSimple2Times>(DsSimple2Times);
        }

        Y_UNIT_TEST(SolomonStandCrash) {
            TFreshDataPtr fresh(new TFreshData(GetLevelIndexSetting(),
                CreateDefaultTimeProvider(), Arena));

            auto putFunc = [] (TFreshDataPtr fresh, ui64 tabletID, ui32 gen, ui32 step, ui64 lsn) {
                TMemRecLogoBlob memRec;
                memRec.SetNoBlob();
                TKeyLogoBlob id(TLogoBlobID(tabletID, gen, step, 0, 0, 0));
                fresh->Put(lsn, id, memRec);
            };

            using namespace std::placeholders;
            auto concretePut = std::bind(putFunc, fresh, 0x100100000010003ul, 2, _1, _2);

            concretePut(22744, 2360767588ul);
            concretePut(22745, 2360767589ul);
            concretePut(22746, 2360767590ul);
            concretePut(22747, 2360767591ul);
            concretePut(22748, 2360767592ul);
            concretePut(22749, 2360767593ul);

            TFreshData::TFreshDataSnapshot snapshot = fresh->GetSnapshot();
            THullCtxPtr hullCtx = TestCtx.GetHullCtx();
            TFreshData::TFreshDataSnapshot::TForwardIterator it(hullCtx, &snapshot);
            TLogoBlobID f(0x100100000010003ul, 2u, 22744u, 0, 0, 0);
            it.Seek(f);
            UNIT_ASSERT_VALUES_EQUAL(it.GetCurKey().ToString(), "[72075186224037891:2:22744:0:0:0:0]");
        }

        Y_UNIT_TEST(Perf) {
            TFreshDataPtr fresh(new TFreshData(GetLevelIndexSetting(), CreateDefaultTimeProvider(), Arena));

            using TIdxKey = TFreshIndex<TKeyLogoBlob, TMemRecLogoBlob>::TIdxKey;
            UNIT_ASSERT_VALUES_EQUAL(bool(TTypeTraits<TKeyLogoBlob>::IsPod), true);
            UNIT_ASSERT_VALUES_EQUAL(bool(TTypeTraits<TKeyLogoBlob>::IsPod), true);
            UNIT_ASSERT_VALUES_EQUAL(bool(TTypeTraits<TIdxKey>::IsPod), true);

            {
                TInstant dataStart = TAppData::TimeProvider->Now();
                ui64 tabletId = 0x100100000010003ul;
                ui32 gen = 5;
                TMemRecLogoBlob memRec;
                memRec.SetNoBlob();
                for (ui32 i = 1; i < 5000000; i++) {
                    ui64 lsn = i;
                    ui32 step = i;
                    TKeyLogoBlob id(TLogoBlobID(tabletId, gen, step, 0, 0, 0));
                    fresh->Put(lsn, id, memRec);
                }
                TInstant dataStop = TAppData::TimeProvider->Now();
                STR << "Load data: " << (dataStop - dataStart).ToString() << "\n";
            }

            {
                TInstant delStart = TAppData::TimeProvider->Now();
                fresh.reset();
                TInstant delStop = TAppData::TimeProvider->Now();
                STR << "Data delete: " << (delStop - delStart).ToString() << "\n";
            }
        }


        //////////////////////////////////////////////////////////////////////////////////////////
        //////////////////////////////////////////////////////////////////////////////////////////
        //////////////////////////////////////////////////////////////////////////////////////////
        //////////////////////////////////////////////////////////////////////////////////////////
        //////////////////////////////////////////////////////////////////////////////////////////
        //////////////////////////////////////////////////////////////////////////////////////////
        //////////////////////////////////////////////////////////////////////////////////////////
        //////////////////////////////////////////////////////////////////////////////////////////
        //////////////////////////////////////////////////////////////////////////////////////////
        class TDataGenerator {
        public:
            TLogoBlobID Next() {
                TTabletState &state = Vec[Pos];
                Pos = ++Pos % Vec.size();
                TLogoBlobID id(state.TabletId, state.Gen, ++state.Step, 0, 0, 0);
                return id;
            }

            // put every n th record into sample keys
            static TVector<TLogoBlobID> GenerateSampleKeys(ui64 total, ui64 n) {
                TVector<TLogoBlobID> sample;
                sample.reserve(total / n);
                TDataGenerator gen;
                for (ui64 i = 0; i < total; ++i) {
                    auto id = gen.Next();
                    if (i % n == 0) {
                        sample.push_back(id);
                    }
                }
                return sample;
            }

        private:
            struct TTabletState {
                ui64 TabletId;
                ui32 Gen;
                ui32 Step;
            };

            TVector<TTabletState> Vec = {
                {100, 5, 1},
                {200, 10, 1},
                {300, 15, 1},
                {400, 20, 1},
                {500, 25, 1},
                {600, 30, 1},
                {700, 35, 1},
                {800, 40, 1},
                {900, 45, 1}
            };
            ui32 Pos = 0;
        };


        struct TCreateAppendixCtx {
            ::NMonitoring::TDynamicCounters DynCounters;
            TMemoryConsumer MemConsumer;
            TMemRecLogoBlob MemRec;

            TCreateAppendixCtx()
                : MemConsumer(DynCounters.GetSubgroup("subsystem", "memhull")->GetCounter("MemTotal:FreshIndex"))
            {
                MemRec.SetNoBlob();
            }
        };

        auto CreateAppendix(ui64 elems, TCreateAppendixCtx &ctx, TDataGenerator &generator) {
            TVector<TLogoBlobID> vec;
            vec.reserve(elems);
            for (ui64 i = 0; i < elems; ++i) {
                vec.push_back(generator.Next());
            }
            std::sort(vec.begin(), vec.end());

            auto result = std::make_shared<TFreshAppendix<TKeyLogoBlob, TMemRecLogoBlob>>(ctx.MemConsumer);
            result->Reserve(elems);
            for (const auto x : vec) {
                result->Add(TKeyLogoBlob(x), ctx.MemRec);
            }
            return result;
        }


        class TFreshBuilder {
            const ui64 Elems;
            const ui64 AppendixBatchSize;
            const bool AppendixCompaction;
            // take snapshot at this element
            const ui64 SnapshotCutoffLsn;

            static ui64 CalculateSnapshotCutoffLsn(ui64 elems, ui64 appendixBatchSize, ui64 percentValidLsns) {
                ui64 e = elems * 100 / percentValidLsns;
                // round it up to appendixBatchSize
                bool up = !!(e % appendixBatchSize);
                return (e / appendixBatchSize + ui64(up)) * appendixBatchSize;
            }

        public:
            struct TFreshPlusNthSnap {
                // built fresh data
                TFreshDataPtr FreshData;
                // fresh data snapshot at nth percent
                std::unique_ptr<TFreshData::TFreshDataSnapshot> SnapPercent;
            };

            TFreshBuilder(ui64 elems, ui64 appendixBatchSize, bool appendixCompaction, ui64 percentValidLsns)
                : Elems(elems)
                , AppendixBatchSize(appendixBatchSize)
                , AppendixCompaction(appendixCompaction)
                , SnapshotCutoffLsn(CalculateSnapshotCutoffLsn(elems, appendixBatchSize, percentValidLsns))
            {}

            TFreshPlusNthSnap BuildFreshFromAppendix() {
                TFreshPlusNthSnap result;
                TInstant startTime = TAppData::TimeProvider->Now();
                TFreshDataPtr fresh(new TFreshData(GetLevelIndexSetting(), CreateDefaultTimeProvider(), Arena));
                result.FreshData = fresh;
                TCreateAppendixCtx ctx;
                TDataGenerator dataGenerator;
                ui64 lsn = 1000;
                ui64 elems = 0;
                while (elems < Elems) {
                    ui64 batch = Min(AppendixBatchSize, Elems - elems);
                    auto a = CreateAppendix(batch, ctx, dataGenerator);
                    fresh->PutAppendix(std::move(a), lsn, lsn + batch - 1);
                    if (AppendixCompaction) {
                        auto job = fresh->CompactAppendix();
                        if (!job.Empty()) {
                            //STR << "CompactAppendix job\n";
                            job.Work();
                            fresh->ApplyAppendixCompactionResult(std::move(job));
                        }
                    }
                    lsn += batch;
                    elems += batch;

                    // take snapshot
                    if (elems == SnapshotCutoffLsn) {
                        result.SnapPercent = std::make_unique<TFreshData::TFreshDataSnapshot>(fresh->GetSnapshot());
                    }
                }
                if (!result.SnapPercent) {
                    result.SnapPercent = std::make_unique<TFreshData::TFreshDataSnapshot>(fresh->GetSnapshot());
                }
                TInstant stopTime = TAppData::TimeProvider->Now();
                STR << "Build Fresh from Appendix(compact="<< AppendixCompaction <<"): "
                    << (stopTime - startTime).ToString() << "\n";
                return result;
            }

            TFreshPlusNthSnap BuildFreshFromPuts() {
                TFreshPlusNthSnap result;
                TInstant startTime = TAppData::TimeProvider->Now();
                TFreshDataPtr fresh(new TFreshData(GetLevelIndexSetting(), CreateDefaultTimeProvider(), Arena));
                result.FreshData = fresh;
                TCreateAppendixCtx ctx;
                TDataGenerator dataGenerator;

                ui64 lsn = 1000;
                for (ui64 elems = 0; elems < Elems; ++elems) {
                    fresh->Put(lsn++, TKeyLogoBlob(dataGenerator.Next()), ctx.MemRec);
                    // take snapshot
                    if (elems == SnapshotCutoffLsn) {
                        result.SnapPercent = std::make_unique<TFreshData::TFreshDataSnapshot>(fresh->GetSnapshot());
                    }
                }
                if (!result.SnapPercent) {
                    result.SnapPercent = std::make_unique<TFreshData::TFreshDataSnapshot>(fresh->GetSnapshot());
                }

                TInstant stopTime = TAppData::TimeProvider->Now();
                STR << "Build Fresh from Puts:                " << (stopTime - startTime).ToString() << "\n";
                return result;
            }
        };

        TVector<TLogoBlobID> ForwardIteratorsBenchmark(
                const char *description,
                TFreshData::TFreshDataSnapshot &snap,
                const TVector<TLogoBlobID> &sampleKeys) {
            THullCtxPtr hullCtx = TestCtx.GetHullCtx();
            TVector<TLogoBlobID> result;
            result.reserve(sampleKeys.size() * 10);

            // forward iterators benchmark
            TInstant startTime = TAppData::TimeProvider->Now();
            TFreshData::TFreshDataSnapshot::TForwardIterator it(hullCtx, &snap);
            for (const auto &x : sampleKeys) {
                it.Seek(x);
                for (int i = 0; i < 10 && it.Valid(); ++i) {
                    result.push_back(it.GetCurKey().LogoBlobID());
                    it.Next();
                }
            }
            TInstant stopTime = TAppData::TimeProvider->Now();
            STR << "ForwardIteratorsBenchmark(" << description << "): "
                << (stopTime - startTime).ToString() << "\n";
            return result;
        }

        TVector<TLogoBlobID> BackwardIteratorsBenchmark(
                const char *description,
                TFreshData::TFreshDataSnapshot &snap,
                const TVector<TLogoBlobID> &sampleKeys) {
            THullCtxPtr hullCtx = TestCtx.GetHullCtx();
            TVector<TLogoBlobID> result;
            result.reserve(sampleKeys.size() * 10);

            // backward iterators benchmark
            TInstant startTime = TAppData::TimeProvider->Now();
            TFreshData::TFreshDataSnapshot::TBackwardIterator it(hullCtx, &snap);
            for (const auto &x : sampleKeys) {
                it.Seek(x);
                for (int i = 0; i < 10 && it.Valid(); ++i) {
                    result.push_back(it.GetCurKey().LogoBlobID());
                    it.Prev();
                }
            }
            TInstant stopTime = TAppData::TimeProvider->Now();
            STR << "BackwardIteratorsBenchmark(" << description << "): "
                << (stopTime - startTime).ToString() << "\n";
            return result;
        }

        // total -- total elements in fresh
        // nth -- take every nth key for iterator test
        // percentValidLsns -- percent (0-100) of records are valid for lsn snaphsot was taken
        void RunTest(ui64 total, ui64 nth, ui64 percentValidLsns) {
            STR << "################### total=" << total << " nth=" << nth << " percent=" << percentValidLsns << "%\n";
            TFreshBuilder::TFreshPlusNthSnap pureFresh;
            TFreshBuilder::TFreshPlusNthSnap appFreshCompaction;
            TFreshBuilder::TFreshPlusNthSnap appFreshNoCompaction;

            {
                TFreshBuilder builder(total, 1000, true, percentValidLsns);
                appFreshCompaction = builder.BuildFreshFromAppendix();
                pureFresh = builder.BuildFreshFromPuts();
            }
            {
                TFreshBuilder builder(total, 1000, false, percentValidLsns);
                appFreshNoCompaction = builder.BuildFreshFromAppendix();
            }

            TVector<TLogoBlobID> sampleKeys = TDataGenerator::GenerateSampleKeys(total, nth);


            auto forwPure = ForwardIteratorsBenchmark("           pureFresh", *pureFresh.SnapPercent, sampleKeys);
            auto forwAComp = ForwardIteratorsBenchmark("  appFreshCompaction", *appFreshCompaction.SnapPercent, sampleKeys);
            auto forwANComp = ForwardIteratorsBenchmark("appFreshNoCompaction", *appFreshNoCompaction.SnapPercent, sampleKeys);
            UNIT_ASSERT(forwPure == forwAComp && forwPure == forwANComp);

            auto backPure = BackwardIteratorsBenchmark("           pureFresh", *pureFresh.SnapPercent, sampleKeys);
            auto backAComp = BackwardIteratorsBenchmark("  appFreshCompaction", *appFreshCompaction.SnapPercent, sampleKeys);
            auto backANComp = BackwardIteratorsBenchmark("appFreshNoCompaction", *appFreshNoCompaction.SnapPercent, sampleKeys);
            UNIT_ASSERT(backPure == backAComp && backPure == backANComp);
            STR << "\n";
        }

        Y_UNIT_TEST(AppendixPerf) {
            ui64 total = 300000;
            RunTest(total, 100, 100);
            RunTest(total, 100, 10);

            total = 1000000;
            RunTest(total, 100, 100);
            RunTest(total, 100, 10);
        }

        Y_UNIT_TEST(AppendixPerf_Tune) {
            RunTest(300000, 100, 100);
        }
    }

} // NKikimr
