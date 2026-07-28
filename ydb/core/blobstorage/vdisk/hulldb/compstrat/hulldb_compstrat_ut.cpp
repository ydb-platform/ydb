#include "hulldb_compstrat_selector.h"
#include "hulldb_compstrat_ratio.h"
#include <util/stream/null.h>
#include <ydb/core/blobstorage/vdisk/hulldb/test/testhull_index.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_ut.h>
#include <library/cpp/time_provider/time_provider.h>
#include <library/cpp/testing/unittest/registar.h>

#define STR     Cnull

namespace NKikimr {

    Y_UNIT_TEST_SUITE(TBlobStorageCompStrat) {

        using TLogoSst = TLevelSegment<TKeyLogoBlob, TMemRecLogoBlob>;
        using TLogoSstPtr = TIntrusivePtr<TLogoSst>;
        using TStorageRatioStrategy = NHullComp::TStrategyStorageRatio<TKeyLogoBlob, TMemRecLogoBlob>;
        using TCalcStat = TStorageRatioStrategy::TCalcStat;
        using TSstRatio = NHullComp::TSstRatio;
        using TSstRatioPtr = NHullComp::TSstRatioPtr;

        static constexpr ui64 ChunkSize = 128u << 20u;
        static constexpr ui32 HullCompLevel0MaxSstsAtOnce = 8u;
        static constexpr ui32 HullCompSortedPartsNum = 8u;
        static constexpr bool Level0UseDreg = true;
        using TStrategy = ::NKikimr::NHullComp::TStrategy<TKeyLogoBlob, TMemRecLogoBlob>;
        using TTask = ::NKikimr::NHullComp::TTask<TKeyLogoBlob, TMemRecLogoBlob>;

        struct TTestRecord {
            TKeyLogoBlob Key;
            TMemRecLogoBlob MemRec;
        };

        struct TRatioValues {
            ui64 IndexItemsTotal;
            ui64 IndexItemsKeep;
            ui64 IndexBytesTotal;
            ui64 IndexBytesKeep;
            ui64 InplacedDataTotal;
            ui64 InplacedDataKeep;
            ui64 HugeDataTotal;
            ui64 HugeDataKeep;

            explicit TRatioValues(const TSstRatio& ratio)
                : IndexItemsTotal(ratio.IndexItemsTotal)
                , IndexItemsKeep(ratio.IndexItemsKeep)
                , IndexBytesTotal(ratio.IndexBytesTotal)
                , IndexBytesKeep(ratio.IndexBytesKeep)
                , InplacedDataTotal(ratio.InplacedDataTotal)
                , InplacedDataKeep(ratio.InplacedDataKeep)
                , HugeDataTotal(ratio.HugeDataTotal)
                , HugeDataKeep(ratio.HugeDataKeep)
            {}
        };

        class TStorageRatioTestDb {
        public:
            TTestContexts Context;
            std::shared_ptr<TRopeArena> Arena =
                std::make_shared<TRopeArena>(&TRopeArenaBackend::Allocate);
            TIntrusivePtr<THullDs> Ds = MakeIntrusive<THullDs>(Context.GetHullCtx());
            TVector<TLogoSstPtr> Ssts;

            explicit TStorageRatioTestDb(bool allowKeepFlags = true)
                : Context(135249920, 2ul << 20ul, allowKeepFlags)
            {
                const TLevelIndexSettings& settings = Context.GetLevelIndexSettings();
                Ds->LogoBlobs = MakeIntrusive<TLogoBlobsDs>(settings, Arena);
                Ds->Blocks = MakeIntrusive<TBlocksDs>(settings, Arena);
                Ds->Barriers = MakeIntrusive<TBarriersDs>(settings, Arena);
            }

            TLogoSstPtr AddSst(
                    ui32 level,
                    TVector<TTestRecord> records,
                    const TVector<TDiskPart>& outbound = {})
            {
                Sort(records.begin(), records.end(), [](const TTestRecord& x, const TTestRecord& y) {
                    return x.Key < y.Key;
                });

                TTrackableVector<TLogoSst::TRec> index(
                    TMemoryConsumer(Context.GetVCtx()->SstIndex));
                index.reserve(records.size());
                for (const TTestRecord& record : records) {
                    index.emplace_back(record.Key, record.MemRec);
                }

                TLogoSstPtr sst = MakeIntrusive<TLogoSst>(Context.GetVCtx());
                for (const TDiskPart& part : outbound) {
                    sst->LoadedOutbound.push_back(part);
                }
                sst->LoadLinearIndex(index);
                sst->Info.Items = records.size();
                sst->Info.FirstLsn = Ssts.size() + 1;
                sst->Info.LastLsn = Ssts.size() + 1;
                sst->Info.CTime = TInstant::Zero();
                sst->AssignedSstId = Ssts.size() + 1;
                ResetRatio(sst);

                if (level == 0) {
                    Ds->LogoBlobs->CurSlice->Level0.Put(sst);
                } else {
                    while (Ds->LogoBlobs->CurSlice->SortedLevels.size() < level) {
                        Ds->LogoBlobs->CurSlice->SortedLevels.emplace_back(TKeyLogoBlob());
                    }
                    Ds->LogoBlobs->CurSlice->SortedLevels[level - 1].Put(sst);
                }

                Ssts.push_back(sst);
                return sst;
            }

            void Load() {
                Ds->LogoBlobs->LoadCompleted();
                Ds->Blocks->LoadCompleted();
                Ds->Barriers->LoadCompleted();
            }

            void AddFresh(const TKeyLogoBlob& key, ECollectMode mode, ui64 lsn) {
                TIngress ingress;
                ingress.SetKeep(
                    TIngress::IngressMode(Context.GetVCtx()->Top->GType),
                    mode);
                Ds->LogoBlobs->PutToFresh(lsn, key, TMemRecLogoBlob(ingress));
            }

            void AddBarrier(
                    ui64 tabletId,
                    ui8 channel,
                    ui32 collectGen,
                    ui32 collectStep,
                    bool hard)
            {
                TBarrierIngress ingress;
                for (ui32 i = 0; i < Context.GetHullCtx()->IngressCache->TotalVDisks; ++i) {
                    TBarrierIngress::Merge(ingress, TBarrierIngress(static_cast<ui8>(i)));
                }

                Ds->Barriers->PutToFresh(
                    1,
                    TKeyBarrier(tabletId, channel, 1, 1, hard),
                    TMemRecBarrier(collectGen, collectStep, ingress));
            }

            void AddSoftBarrier(ui64 tabletId, ui8 channel, ui32 collectGen, ui32 collectStep) {
                AddBarrier(tabletId, channel, collectGen, collectStep, false);
            }

            void ResetRatios() {
                for (const TLogoSstPtr& sst : Ssts) {
                    ResetRatio(sst);
                }
            }

        private:
            static void ResetRatio(const TLogoSstPtr& sst) {
                TSstRatioPtr ratio = MakeIntrusive<TSstRatio>(TInstant::Zero());
                ratio->IndexItemsTotal = sst->Elements();
                ratio->IndexItemsKeep = sst->Elements();
                sst->StorageRatio.Set(ratio, TInstant::Zero());
            }
        };

        class TSteppingTimeProvider : public ITimeProvider {
        public:
            TSteppingTimeProvider(TInstant now, TDuration step)
                : Current(now)
                , Step(step)
            {}

            TInstant Now() override {
                const TInstant result = Current;
                Current += Step;
                return result;
            }

        private:
            TInstant Current;
            const TDuration Step;
        };

        class TTimeProviderGuard {
        public:
            explicit TTimeProviderGuard(TIntrusivePtr<ITimeProvider> timeProvider)
                : Original(TAppData::TimeProvider)
            {
                TAppData::TimeProvider = std::move(timeProvider);
            }

            ~TTimeProviderGuard() {
                TAppData::TimeProvider = std::move(Original);
            }

        private:
            TIntrusivePtr<ITimeProvider> Original;
        };

        static TKeyLogoBlob MakeKey(ui32 step) {
            return TKeyLogoBlob(TLogoBlobID(1, 1, step, 0, 100, 0));
        }

        static TIngress MakeIngress(TBlobStorageGroupType gtype, ECollectMode mode) {
            TIngress ingress;
            ingress.SetKeep(TIngress::IngressMode(gtype), mode);
            return ingress;
        }

        static TMemRecLogoBlob MakeDiskBlob(
                TBlobStorageGroupType gtype,
                ui32 size,
                ECollectMode mode = CollectModeDefault)
        {
            TMemRecLogoBlob memRec(MakeIngress(gtype, mode));
            memRec.SetDiskBlob(TDiskPart(100, size, size));
            return memRec;
        }

        static TMemRecLogoBlob MakeHugeBlob(
                TBlobStorageGroupType gtype,
                ui32 size,
                ECollectMode mode = CollectModeDefault)
        {
            TMemRecLogoBlob memRec(MakeIngress(gtype, mode));
            memRec.SetHugeBlob(TDiskPart(200, size, size));
            return memRec;
        }

        static TMemRecLogoBlob MakeManyHugeBlobs(
                TBlobStorageGroupType gtype,
                ui32 totalSize,
                ui32 numParts,
                ECollectMode mode = CollectModeDefault,
                ui32 outboundIndex = 0)
        {
            TMemRecLogoBlob memRec(MakeIngress(gtype, mode));
            memRec.SetManyHugeBlobs(outboundIndex, numParts, totalSize);
            return memRec;
        }

        static void AddGeneratedSst(
                TStorageRatioTestDb& db,
                ui32 level,
                const TVector<ui32>& steps,
                ui32 salt)
        {
            const TBlobStorageGroupType gtype = db.Context.GetVCtx()->Top->GType;
            TVector<TTestRecord> records;
            TVector<TDiskPart> outbound;
            records.reserve(steps.size());

            for (size_t i = 0; i < steps.size(); ++i) {
                const ui32 step = steps[i];
                const auto mode = static_cast<ECollectMode>((step + salt + i) % 4);
                const ui32 size = 1 + (step * 13 + salt * 7 + i * 3) % 1000;

                TMemRecLogoBlob memRec;
                switch ((step + salt + i) % 4) {
                    case 0:
                        memRec = TMemRecLogoBlob(MakeIngress(gtype, mode));
                        break;
                    case 1:
                        memRec = MakeDiskBlob(gtype, size, mode);
                        break;
                    case 2:
                        memRec = MakeHugeBlob(gtype, size, mode);
                        break;
                    case 3: {
                        const ui32 numParts = 1 + (step + salt) % 3;
                        const ui32 outboundIndex = outbound.size();
                        ui32 totalSize = 0;
                        for (ui32 part = 0; part < numParts; ++part) {
                            const ui32 partSize = size + part;
                            totalSize += partSize;
                            outbound.emplace_back(
                                1000 + salt * 10 + part,
                                part * 100,
                                partSize);
                        }
                        memRec = MakeManyHugeBlobs(
                            gtype,
                            totalSize,
                            numParts,
                            mode,
                            outboundIndex);
                        break;
                    }
                }

                records.push_back({MakeKey(step), memRec});
            }

            db.AddSst(level, std::move(records), outbound);
        }

        class TDeterministicRandom {
        public:
            explicit TDeterministicRandom(ui64 seed)
                : State(seed)
            {}

            ui32 Next(ui32 limit) {
                Y_ABORT_UNLESS(limit);
                State = State * 6364136223846793005ULL + 1442695040888963407ULL;
                return static_cast<ui32>((State >> 32) % limit);
            }

        private:
            ui64 State;
        };

        static std::unique_ptr<TStorageRatioTestDb> MakeStorageRatioTestDb() {
            auto db = std::make_unique<TStorageRatioTestDb>();
            const TBlobStorageGroupType gtype = db->Context.GetVCtx()->Top->GType;

            db->AddSst(0, {
                {MakeKey(1), MakeDiskBlob(gtype, 11, CollectModeKeep)},
                {MakeKey(2), MakeHugeBlob(gtype, 22, CollectModeDoNotKeep)},
                {MakeKey(4), MakeManyHugeBlobs(gtype, 77, 2)},
            }, {
                TDiskPart(301, 0, 33),
                TDiskPart(302, 0, 44),
            });

            db->AddSst(0, {
                {MakeKey(1), MakeHugeBlob(gtype, 12, CollectModeDoNotKeep)},
                {MakeKey(3), MakeDiskBlob(gtype, 23)},
                {MakeKey(4), MakeDiskBlob(gtype, 34, CollectModeKeep)},
            });

            db->AddSst(1, {
                {MakeKey(1), MakeDiskBlob(gtype, 13)},
                {MakeKey(2), MakeDiskBlob(gtype, 24, CollectModeKeep)},
                {MakeKey(5), MakeHugeBlob(gtype, 35)},
            });

            db->Load();
            db->AddFresh(MakeKey(1), CollectModeKeep, 10);
            db->AddFresh(MakeKey(4), CollectModeDoNotKeep, 11);
            db->AddSoftBarrier(1, 0, 1, 3);
            return db;
        }

        static void RunStorageRatio(
                const THullDsSnap& snap,
                bool enableOptimization,
                bool allowGarbageCollection = true,
                TCalcStat* calcStat = nullptr)
        {
            snap.HullCtx->VCfg->FeatureFlags.SetEnableHullCompStorageRatioOptimization(
                enableOptimization);
            auto barriers = snap.BarriersSnap.CreateEssence(snap.HullCtx);
            TStorageRatioStrategy(
                snap.HullCtx,
                snap.LogoBlobsSnap,
                std::move(barriers),
                allowGarbageCollection,
                calcStat).Work();
        }

        static void SetDueSsts(
                TStorageRatioTestDb& db,
                const TVector<size_t>& dueIndices)
        {
            TVector<bool> due(db.Ssts.size(), false);
            for (size_t index : dueIndices) {
                UNIT_ASSERT(index < due.size());
                due[index] = true;
            }

            for (size_t i = 0; i < db.Ssts.size(); ++i) {
                const TInstant calculationTime = due[i]
                    ? TInstant::Zero()
                    : TInstant::Seconds(900);
                TSstRatioPtr ratio = MakeIntrusive<TSstRatio>(calculationTime);
                ratio->IndexItemsTotal = db.Ssts[i]->Elements();
                ratio->IndexItemsKeep = db.Ssts[i]->Elements();
                db.Ssts[i]->StorageRatio.Set(ratio, calculationTime);
            }
        }

        static THashMap<const TLogoSst*, TRatioValues> ReadRatios(
                const TVector<TLogoSstPtr>& ssts)
        {
            THashMap<const TLogoSst*, TRatioValues> result;
            for (const TLogoSstPtr& sst : ssts) {
                TSstRatioPtr ratio = sst->StorageRatio.Get();
                UNIT_ASSERT(ratio);
                result.emplace(sst.Get(), TRatioValues(*ratio));
            }
            return result;
        }

        static void AssertRatioEqual(
                const TRatioValues& expected,
                const TSstRatio& actual,
                const TString& context)
        {
            UNIT_ASSERT_VALUES_EQUAL_C(expected.IndexItemsTotal, actual.IndexItemsTotal, context);
            UNIT_ASSERT_VALUES_EQUAL_C(expected.IndexItemsKeep, actual.IndexItemsKeep, context);
            UNIT_ASSERT_VALUES_EQUAL_C(expected.IndexBytesTotal, actual.IndexBytesTotal, context);
            UNIT_ASSERT_VALUES_EQUAL_C(expected.IndexBytesKeep, actual.IndexBytesKeep, context);
            UNIT_ASSERT_VALUES_EQUAL_C(expected.InplacedDataTotal, actual.InplacedDataTotal, context);
            UNIT_ASSERT_VALUES_EQUAL_C(expected.InplacedDataKeep, actual.InplacedDataKeep, context);
            UNIT_ASSERT_VALUES_EQUAL_C(expected.HugeDataTotal, actual.HugeDataTotal, context);
            UNIT_ASSERT_VALUES_EQUAL_C(expected.HugeDataKeep, actual.HugeDataKeep, context);
        }

        static void AssertRatiosEqual(
                const THashMap<const TLogoSst*, TRatioValues>& expected,
                const TVector<TLogoSstPtr>& ssts,
                const TString& context = {})
        {
            for (const TLogoSstPtr& sst : ssts) {
                TSstRatioPtr actual = sst->StorageRatio.Get();
                UNIT_ASSERT(actual);
                AssertRatioEqual(
                    expected.at(sst.Get()),
                    *actual,
                    TStringBuilder() << context << " sst# " << sst->FirstKey().ToString());
            }
        }

        static void AssertAlgorithmsEqual(
                TStorageRatioTestDb& db,
                bool allowGarbageCollection,
                const TString& context)
        {
            auto snap = db.Ds->GetIndexSnapshot();
            TTimeProviderGuard timeProvider(CreateDeterministicTimeProvider(1000));

            RunStorageRatio(
                snap,
                false,
                allowGarbageCollection);
            const auto expected = ReadRatios(db.Ssts);

            db.ResetRatios();
            RunStorageRatio(
                snap,
                true,
                allowGarbageCollection);
            AssertRatiosEqual(expected, db.Ssts, context);
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

        Y_UNIT_TEST(StorageRatioBatchMatchesPerSst) {
            auto db = MakeStorageRatioTestDb();
            auto snap = db->Ds->GetIndexSnapshot();
            TTimeProviderGuard timeProvider(CreateDeterministicTimeProvider(1000));

            RunStorageRatio(snap, false);
            const auto expected = ReadRatios(db->Ssts);

            const TRatioValues& mixedDataRatio = expected.at(db->Ssts[0].Get());
            UNIT_ASSERT_VALUES_EQUAL(3, mixedDataRatio.IndexItemsTotal);
            UNIT_ASSERT_VALUES_EQUAL(11, mixedDataRatio.InplacedDataTotal);
            UNIT_ASSERT_VALUES_EQUAL(
                22 + 33 + 44 + 2 * sizeof(TDiskPart),
                mixedDataRatio.HugeDataTotal);

            db->ResetRatios();
            RunStorageRatio(snap, true);
            AssertRatiosEqual(expected, db->Ssts);

            TVector<TSstRatioPtr> currentRatios;
            currentRatios.reserve(db->Ssts.size());
            for (const TLogoSstPtr& sst : db->Ssts) {
                currentRatios.push_back(sst->StorageRatio.Get());
            }

            RunStorageRatio(snap, true);
            for (size_t i = 0; i < db->Ssts.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL(
                    currentRatios[i].Get(),
                    db->Ssts[i]->StorageRatio.Get().Get());
            }
        }

        Y_UNIT_TEST(StorageRatioBatchReducesWork) {
            TStorageRatioTestDb db;
            TVector<ui32> steps;
            steps.reserve(100);
            for (ui32 step = 1; step <= 100; ++step) {
                steps.push_back(step);
            }

            for (ui32 sst = 0; sst < 8; ++sst) {
                AddGeneratedSst(db, 0, steps, sst * 10);
            }
            db.Load();
            for (ui32 step = 1; step <= 100; ++step) {
                db.AddFresh(
                    MakeKey(step),
                    static_cast<ECollectMode>(step % 4),
                    1000 + step);
            }

            auto snap = db.Ds->GetIndexSnapshot();
            TTimeProviderGuard timeProvider(CreateDeterministicTimeProvider(1000));

            TCalcStat perSstStat;
            RunStorageRatio(
                snap,
                false,
                true,
                &perSstStat);
            const auto expected = ReadRatios(db.Ssts);

            db.ResetRatios();
            TCalcStat batchStat;
            RunStorageRatio(
                snap,
                true,
                true,
                &batchStat);
            AssertRatiosEqual(expected, db.Ssts, "work statistics");

            UNIT_ASSERT_VALUES_EQUAL(800, perSstStat.KeysProcessed);
            UNIT_ASSERT_VALUES_EQUAL(100, batchStat.KeysProcessed);
            UNIT_ASSERT_VALUES_EQUAL(800, perSstStat.SourceRecordsProcessed);
            UNIT_ASSERT_VALUES_EQUAL(800, batchStat.SourceRecordsProcessed);
            UNIT_ASSERT_VALUES_EQUAL(7200, perSstStat.DbRecordsMerged);
            UNIT_ASSERT_VALUES_EQUAL(900, batchStat.DbRecordsMerged);
            UNIT_ASSERT_VALUES_EQUAL(8, perSstStat.Seeks);
            UNIT_ASSERT_VALUES_EQUAL(1, batchStat.Seeks);

            UNIT_ASSERT(batchStat.DbRecordsMerged < perSstStat.DbRecordsMerged);
            UNIT_ASSERT(batchStat.Seeks < perSstStat.Seeks);
        }

        Y_UNIT_TEST(StorageRatioSkipsUnrelatedKeysForDisjointDueRanges) {
            TStorageRatioTestDb db;
            constexpr ui32 numSsts = 8;
            constexpr ui32 recordsPerSst = 10;

            for (ui32 sstIndex = 0; sstIndex < numSsts; ++sstIndex) {
                TVector<ui32> steps;
                steps.reserve(recordsPerSst);
                for (ui32 record = 0; record < recordsPerSst; ++record) {
                    steps.push_back(1 + sstIndex * 100 + record);
                }
                AddGeneratedSst(db, 0, steps, sstIndex);
            }
            db.Load();

            auto snap = db.Ds->GetIndexSnapshot();
            TTimeProviderGuard timeProvider(CreateDeterministicTimeProvider(1000));

            SetDueSsts(db, {0, numSsts - 1});
            TCalcStat perSstStat;
            RunStorageRatio(snap, false, true, &perSstStat);
            const auto expected = ReadRatios(db.Ssts);

            SetDueSsts(db, {0, numSsts - 1});
            TCalcStat batchStat;
            RunStorageRatio(snap, true, true, &batchStat);
            AssertRatiosEqual(expected, db.Ssts, "scattered due SSTs");

            constexpr ui64 dueRecords = 2 * recordsPerSst;
            UNIT_ASSERT_VALUES_EQUAL(dueRecords, perSstStat.KeysProcessed);
            UNIT_ASSERT_VALUES_EQUAL(dueRecords, batchStat.KeysProcessed);
            UNIT_ASSERT_VALUES_EQUAL(dueRecords, batchStat.SourceRecordsProcessed);
            UNIT_ASSERT_VALUES_EQUAL(dueRecords, batchStat.DbRecordsMerged);
            UNIT_ASSERT_VALUES_EQUAL(2, batchStat.Seeks);
            UNIT_ASSERT(batchStat.KeysProcessed < numSsts * recordsPerSst);
        }

        Y_UNIT_TEST(StorageRatioBatchSeeksAcrossLargeNonDueGap) {
            TStorageRatioTestDb db;

            TVector<ui32> spanningSteps;
            for (ui32 step = 1; step <= 10; ++step) {
                spanningSteps.push_back(step);
            }
            for (ui32 step = 701; step <= 710; ++step) {
                spanningSteps.push_back(step);
            }
            AddGeneratedSst(db, 0, spanningSteps, 1);

            TVector<ui32> overlappingSteps;
            TVector<ui32> gapSteps;
            for (ui32 offset = 0; offset < 10; ++offset) {
                overlappingSteps.push_back(701 + offset);
                gapSteps.push_back(100 + offset);
            }
            AddGeneratedSst(db, 0, overlappingSteps, 2);
            AddGeneratedSst(db, 0, gapSteps, 3);
            db.Load();

            auto snap = db.Ds->GetIndexSnapshot();
            TTimeProviderGuard timeProvider(CreateDeterministicTimeProvider(1000));

            SetDueSsts(db, {0, 1});
            TCalcStat perSstStat;
            RunStorageRatio(snap, false, true, &perSstStat);
            const auto expected = ReadRatios(db.Ssts);

            SetDueSsts(db, {0, 1});
            TCalcStat batchStat;
            RunStorageRatio(snap, true, true, &batchStat);
            AssertRatiosEqual(expected, db.Ssts, "large non-due gap");

            UNIT_ASSERT_VALUES_EQUAL(20, batchStat.KeysProcessed);
            UNIT_ASSERT_VALUES_EQUAL(30, batchStat.SourceRecordsProcessed);
            UNIT_ASSERT_VALUES_EQUAL(30, batchStat.DbRecordsMerged);
            UNIT_ASSERT_VALUES_EQUAL(2, batchStat.Seeks);
            UNIT_ASSERT(batchStat.Seeks < perSstStat.Seeks);
        }

        Y_UNIT_TEST(StorageRatioDoesNoWorkWhenNothingIsDue) {
            auto db = MakeStorageRatioTestDb();
            auto snap = db->Ds->GetIndexSnapshot();
            TTimeProviderGuard timeProvider(CreateDeterministicTimeProvider(1000));

            auto checkAlgorithm = [&](bool enableOptimization) {
                SetDueSsts(*db, {});

                TVector<TSstRatioPtr> ratiosBeforeCalculation;
                ratiosBeforeCalculation.reserve(db->Ssts.size());
                for (const TLogoSstPtr& sst : db->Ssts) {
                    ratiosBeforeCalculation.push_back(sst->StorageRatio.Get());
                }

                TCalcStat stat;
                RunStorageRatio(
                    snap,
                    enableOptimization,
                    true,
                    &stat);

                for (size_t i = 0; i < db->Ssts.size(); ++i) {
                    UNIT_ASSERT_VALUES_EQUAL(
                        ratiosBeforeCalculation[i].Get(),
                        db->Ssts[i]->StorageRatio.Get().Get());
                }
                UNIT_ASSERT_VALUES_EQUAL(0, stat.KeysProcessed);
                UNIT_ASSERT_VALUES_EQUAL(0, stat.SourceRecordsProcessed);
                UNIT_ASSERT_VALUES_EQUAL(0, stat.DbRecordsMerged);
                UNIT_ASSERT_VALUES_EQUAL(0, stat.Seeks);
            };

            checkAlgorithm(false);
            checkAlgorithm(true);
        }

        Y_UNIT_TEST(StorageRatioBatchFallsBackForDuplicateSst) {
            TStorageRatioTestDb db;
            TLogoSstPtr sst = db.AddSst(0, {
                {MakeKey(1), TMemRecLogoBlob()},
                {MakeKey(2), TMemRecLogoBlob()},
                {MakeKey(3), TMemRecLogoBlob()},
            });

            db.Ds->LogoBlobs->CurSlice->SortedLevels.emplace_back(TKeyLogoBlob());
            db.Ds->LogoBlobs->CurSlice->SortedLevels.back().Put(sst);
            db.Load();

            auto snap = db.Ds->GetIndexSnapshot();
            TTimeProviderGuard timeProvider(CreateDeterministicTimeProvider(1000));

            TCalcStat perSstStat;
            RunStorageRatio(snap, false, true, &perSstStat);
            const auto expected = ReadRatios(db.Ssts);

            db.ResetRatios();
            TCalcStat batchStat;
            RunStorageRatio(snap, true, true, &batchStat);
            AssertRatiosEqual(expected, db.Ssts, "duplicate SST fallback");

            UNIT_ASSERT_VALUES_EQUAL(perSstStat.KeysProcessed, batchStat.KeysProcessed);
            UNIT_ASSERT_VALUES_EQUAL(
                perSstStat.SourceRecordsProcessed,
                batchStat.SourceRecordsProcessed);
            UNIT_ASSERT_VALUES_EQUAL(perSstStat.DbRecordsMerged, batchStat.DbRecordsMerged);
            UNIT_ASSERT_VALUES_EQUAL(perSstStat.Seeks, batchStat.Seeks);
            UNIT_ASSERT_VALUES_EQUAL(2, batchStat.Seeks);
        }

        Y_UNIT_TEST(StorageRatioBatchOnlyPublishesDueSsts) {
            auto db = MakeStorageRatioTestDb();
            auto snap = db->Ds->GetIndexSnapshot();
            TTimeProviderGuard timeProvider(CreateDeterministicTimeProvider(1000));

            RunStorageRatio(snap, false);
            const auto expected = ReadRatios(db->Ssts);

            db->ResetRatios();
            TVector<TSstRatioPtr> ratiosBeforeCalculation;
            ratiosBeforeCalculation.reserve(db->Ssts.size());
            ratiosBeforeCalculation.push_back(db->Ssts[0]->StorageRatio.Get());

            for (size_t i = 1; i < db->Ssts.size(); ++i) {
                TSstRatioPtr ratio = MakeIntrusive<TSstRatio>(TInstant::Seconds(900));
                ratio->IndexItemsTotal = db->Ssts[i]->Elements();
                ratio->IndexItemsKeep = db->Ssts[i]->Elements();
                db->Ssts[i]->StorageRatio.Set(ratio, ratio->Time);
                ratiosBeforeCalculation.push_back(std::move(ratio));
            }

            RunStorageRatio(snap, true);

            TSstRatioPtr recalculated = db->Ssts[0]->StorageRatio.Get();
            UNIT_ASSERT(recalculated.Get() != ratiosBeforeCalculation[0].Get());
            AssertRatioEqual(
                expected.at(db->Ssts[0].Get()),
                *recalculated,
                db->Ssts[0]->FirstKey().ToString());

            for (size_t i = 1; i < db->Ssts.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL(
                    ratiosBeforeCalculation[i].Get(),
                    db->Ssts[i]->StorageRatio.Get().Get());
            }
        }

        Y_UNIT_TEST(StorageRatioAlgorithmsMatchScenarioMatrix) {
            {
                TStorageRatioTestDb db;
                AddGeneratedSst(db, 0, {1, 2, 3, 4}, 1);
                db.Load();
                AssertAlgorithmsEqual(db, true, "single L0, no Fresh, no barriers");
            }

            {
                TStorageRatioTestDb db;
                AddGeneratedSst(db, 0, {1, 3, 5, 7, 9}, 10);
                AddGeneratedSst(db, 0, {2, 3, 6, 7, 10}, 20);
                AddGeneratedSst(db, 0, {1, 4, 6, 8, 10}, 30);
                AddGeneratedSst(db, 0, {2, 4, 5, 8, 9}, 40);
                AddGeneratedSst(db, 0, {1, 2, 7, 8, 10}, 50);
                AddGeneratedSst(db, 0, {3, 4, 5, 6, 9}, 60);
                db.Load();
                db.AddFresh(MakeKey(0), CollectModeDefault, 100);
                db.AddFresh(MakeKey(6), CollectModeKeep, 101);
                db.AddFresh(MakeKey(11), CollectModeDoNotKeep, 102);
                AssertAlgorithmsEqual(db, true, "six overlapping L0 SSTs");
            }

            {
                TStorageRatioTestDb db;
                AddGeneratedSst(db, 0, {0, 3, 7, 10}, 70);
                AddGeneratedSst(db, 1, {1, 2, 3}, 71);
                AddGeneratedSst(db, 1, {4, 5, 6}, 72);
                AddGeneratedSst(db, 1, {8, 9}, 73);
                AddGeneratedSst(db, 2, {2, 4, 6, 8}, 74);
                AddGeneratedSst(db, 3, {1, 5, 9}, 75);
                db.Load();
                db.AddFresh(MakeKey(4), CollectModeKeep, 100);
                db.AddFresh(MakeKey(11), CollectModeDoNotKeep, 101);
                db.AddSoftBarrier(1, 0, 1, 6);
                AssertAlgorithmsEqual(
                    db,
                    true,
                    "multiple sorted levels and non-overlapping ranges");
            }

            for (bool allowKeepFlags : {false, true}) {
                for (bool allowGarbageCollection : {false, true}) {
                    for (ui32 barrierKind = 0; barrierKind < 3; ++barrierKind) {
                        TStorageRatioTestDb db(allowKeepFlags);
                        AddGeneratedSst(db, 0, {1, 2, 4, 7}, 80 + barrierKind);
                        AddGeneratedSst(db, 0, {1, 3, 4, 8}, 90 + barrierKind);
                        AddGeneratedSst(db, 1, {1, 2, 5, 9}, 100 + barrierKind);
                        db.Load();

                        db.AddFresh(MakeKey(1), CollectModeDefault, 100);
                        db.AddFresh(MakeKey(2), CollectModeKeep, 101);
                        db.AddFresh(MakeKey(4), CollectModeDoNotKeep, 102);
                        db.AddFresh(
                            MakeKey(10),
                            static_cast<ECollectMode>(CollectModeKeep | CollectModeDoNotKeep),
                            103);

                        if (barrierKind) {
                            db.AddBarrier(1, 0, 1, 5, barrierKind == 2);
                        }

                        const TString context = TStringBuilder()
                            << "allowKeepFlags# " << allowKeepFlags
                            << " allowGarbageCollection# " << allowGarbageCollection
                            << " barrierKind# " << barrierKind;
                        AssertAlgorithmsEqual(db, allowGarbageCollection, context);
                    }
                }
            }
        }

        Y_UNIT_TEST(StorageRatioAlgorithmsMatchRandomizedSnapshots) {
            constexpr ui64 seed = 0x6a09e667f3bcc909ULL;
            constexpr ui32 iterations = 64;
            TDeterministicRandom random(seed);

            for (ui32 iteration = 0; iteration < iterations; ++iteration) {
                const bool allowKeepFlags = random.Next(2);
                const bool allowGarbageCollection = random.Next(2);
                TStorageRatioTestDb db(allowKeepFlags);

                const ui32 numSsts = 1 + random.Next(8);
                const ui32 levelLayout = random.Next(3);
                for (ui32 sstIndex = 0; sstIndex < numSsts; ++sstIndex) {
                    ui32 level = 0;
                    if (levelLayout == 1) {
                        level = sstIndex % 3 ? sstIndex + 1 : 0;
                    } else if (levelLayout == 2) {
                        level = sstIndex + 1;
                    }

                    const ui32 numRecords = 1 + random.Next(10);
                    TVector<bool> usedSteps(25, false);
                    TVector<ui32> steps;
                    steps.reserve(numRecords);
                    while (steps.size() < numRecords) {
                        const ui32 step = 1 + random.Next(24);
                        if (!usedSteps[step]) {
                            usedSteps[step] = true;
                            steps.push_back(step);
                        }
                    }

                    AddGeneratedSst(
                        db,
                        level,
                        steps,
                        iteration * 17 + sstIndex);
                }

                db.Load();

                const ui32 numFreshRecords = random.Next(13);
                TVector<bool> usedFreshSteps(29, false);
                for (ui32 freshIndex = 0; freshIndex < numFreshRecords; ++freshIndex) {
                    ui32 step;
                    do {
                        step = random.Next(29);
                    } while (usedFreshSteps[step]);
                    usedFreshSteps[step] = true;

                    db.AddFresh(
                        MakeKey(step),
                        static_cast<ECollectMode>(random.Next(4)),
                        100 + freshIndex);
                }

                const ui32 barrierKind = random.Next(3);
                if (barrierKind) {
                    db.AddBarrier(
                        1,
                        0,
                        1,
                        1 + random.Next(24),
                        barrierKind == 2);
                }

                const TString context = TStringBuilder()
                    << "random seed# " << seed
                    << " iteration# " << iteration
                    << " ssts# " << numSsts
                    << " Fresh# " << numFreshRecords
                    << " allowKeepFlags# " << allowKeepFlags
                    << " allowGarbageCollection# " << allowGarbageCollection
                    << " barrierKind# " << barrierKind;
                AssertAlgorithmsEqual(db, allowGarbageCollection, context);
            }
        }

        Y_UNIT_TEST(StorageRatioFeatureFlagSelectsAlgorithm) {
            auto db = MakeStorageRatioTestDb();
            auto snap = db->Ds->GetIndexSnapshot();
            TTimeProviderGuard timeProvider(CreateDeterministicTimeProvider(1000));

            TCalcStat perSstStat;
            RunStorageRatio(snap, false, true, &perSstStat);
            const auto expected = ReadRatios(db->Ssts);

            db->ResetRatios();
            TCalcStat batchStat;
            RunStorageRatio(snap, true, true, &batchStat);
            AssertRatiosEqual(expected, db->Ssts);
            UNIT_ASSERT_VALUES_EQUAL(1, batchStat.Seeks);
            UNIT_ASSERT(batchStat.Seeks < perSstStat.Seeks);
        }

        Y_UNIT_TEST(StorageRatioBatchPublishesAllCompleteRatiosOnTimeout) {
            auto db = MakeStorageRatioTestDb();
            auto snap = db->Ds->GetIndexSnapshot();

            {
                TTimeProviderGuard timeProvider(CreateDeterministicTimeProvider(1000));
                RunStorageRatio(snap, false);
            }
            const auto expected = ReadRatios(db->Ssts);

            db->ResetRatios();
            TVector<TSstRatioPtr> initialRatios;
            initialRatios.reserve(db->Ssts.size());
            for (const TLogoSstPtr& sst : db->Ssts) {
                initialRatios.push_back(sst->StorageRatio.Get());
            }

            {
                TTimeProviderGuard timeProvider(
                    MakeIntrusive<TSteppingTimeProvider>(
                        TInstant::Seconds(1000),
                        TDuration::Seconds(2)));
                RunStorageRatio(snap, true);
            }

            size_t publishedRatios = 0;
            for (size_t i = 0; i < db->Ssts.size(); ++i) {
                TSstRatioPtr ratio = db->Ssts[i]->StorageRatio.Get();
                if (ratio.Get() != initialRatios[i].Get()) {
                    ++publishedRatios;
                    AssertRatioEqual(
                        expected.at(db->Ssts[i].Get()),
                        *ratio,
                        db->Ssts[i]->FirstKey().ToString());
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(
                        db->Ssts[i]->Elements(),
                        ratio->IndexItemsKeep);
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(2, publishedRatios);
        }

    }

} // NKikimr
