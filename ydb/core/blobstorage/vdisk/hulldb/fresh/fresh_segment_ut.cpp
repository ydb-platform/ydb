#include "fresh_segment.h"
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_ut.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_settings.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/blobstorage_blob.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_logoblob.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/null.h>

// change to Cerr if you want logging
#define STR Cnull

namespace NKikimr {

    static std::shared_ptr<TRopeArena> Arena = std::make_shared<TRopeArena>(&TRopeArenaBackend::Allocate);

    Y_UNIT_TEST_SUITE(TBlobStorageHullFreshSegment) {

        ////////////////////////////////////////////////////////////////////////////////////////
        // Definitions
        ////////////////////////////////////////////////////////////////////////////////////////
        using TFreshSegment = ::NKikimr::TFreshSegment<TKeyLogoBlob, TMemRecLogoBlob>;
        using TAppendix = ::NKikimr::TFreshAppendix<TKeyLogoBlob, TMemRecLogoBlob>;
        static const ui32 ChunkSize = 8u << 20u;
        static const ui32 Level0MaxSstsAtOnce = 8u;
        static const ui32 BufSize = 64u << 20u;
        static const ui64 CompThreshold = 8u << 20u;
        static const ui32 CompWorthReadSize = 2u << 20u;
        static const TDuration HistoryWindow = TDuration::Minutes(10);
        static const ui64 HistoryBuckets = 10u;
        static TTestContexts TestCtx(ChunkSize, CompWorthReadSize);

        TLevelIndexSettings GetLevelIndexSetting() {
            static TLevelIndexSettings settings(TestCtx.GetHullCtx(),
                    Level0MaxSstsAtOnce, BufSize, CompThreshold, HistoryWindow, HistoryBuckets, false, false);
            return settings;
        }

        ////////////////////////////////////////////////////////////////////////////////////////
        // Generate different datasets
        ////////////////////////////////////////////////////////////////////////////////////////
        struct TOtherVDisksSkipList {
            TOtherVDisksSkipList(ui64 *lsnCounter)
                : LsnCounter(lsnCounter)
            {
                Aps.resize(To - From + 1);
                ReinitAps();
            }

            void ReinitAps() {
                for (Part = From; Part <= To; ++Part) {
                    Aps[Part - From].clear();
                }
            }

            void AddToAll(TLogoBlobID id, const TMemRecLogoBlob &memRec) {
                ++Inserts;
                for (Part = From; Part <= To; ++Part) {
                    Aps[Part - From].push_back(std::pair<TKeyLogoBlob, TMemRecLogoBlob>(
                        TKeyLogoBlob(TLogoBlobID(id, Part)), memRec));
                }
            }

            void ApplyIfRequired(TFreshSegment *seg) {
                if (Inserts == InsertsThreshold) {
                    Inserts = 0;
                    for (Part = From; Part <= To; ++Part) {
                        for (auto &x : Aps[Part - From]) {
                            seg->Put((*LsnCounter)++, x.first, x.second);
                        }
                    }
                    ReinitAps();
                }
            }

            const ui32 From = 2;
            const ui32 To = 6;
            const ui32 InsertsThreshold = 1000;
            ui64 *LsnCounter = nullptr;
            ui32 Part = 0;
            ui32 Inserts = 0;
            ui32 Compactions = 0;
            TVector<TVector<std::pair<TKeyLogoBlob, TMemRecLogoBlob>>> Aps;
        };

        struct TOtherVDisksAppendix {
            TOtherVDisksAppendix(ui64 *lsnCounter)
                : MemConsumer(DynCounters.GetSubgroup("subsystem", "memhull")->GetCounter("MemTotal:FreshIndex"))
                , LsnCounter(lsnCounter)
            {
                Aps.resize(To - From + 1);
                ReinitAps();
            }

            void ReinitAps() {
                for (Part = From; Part <= To; ++Part) {
                    Aps[Part - From] = std::make_shared<TAppendix>(MemConsumer);
                }
            }

            void AddToAll(TLogoBlobID id, const TMemRecLogoBlob &memRec) {
                ++Inserts;
                for (Part = From; Part <= To; ++Part) {
                    Aps[Part - From]->Add(TKeyLogoBlob(TLogoBlobID(id, Part)), memRec);
                }
            }

            void ApplyIfRequired(TFreshSegment *seg) {
                if (Inserts == InsertsThreshold) {
                    Inserts = 0;
                    for (Part = From; Part <= To; ++Part) {
                        ui64 firstLsn = *LsnCounter;
                        ui64 lastLsn = *LsnCounter + InsertsThreshold - 1;
                        *LsnCounter += InsertsThreshold;
                        seg->PutAppendix(std::move(Aps[Part - From]), firstLsn, lastLsn);

                        auto job = seg->Compact();
                        if (!job.Empty()) {
                            ++Compactions;
                            job.Work();
                            job.ApplyCompactionResult();
                        }
                    }
                    ReinitAps();
                }
            }

            const ui32 From = 2;
            const ui32 To = 6;
            const ui32 InsertsThreshold = 1000;
            ::NMonitoring::TDynamicCounters DynCounters;
            TMemoryConsumer MemConsumer;
            ui64 *LsnCounter = nullptr;
            ui32 Part = 0;
            ui32 Inserts = 0;
            ui32 Compactions = 0;
            TVector<std::shared_ptr<TAppendix>> Aps;
        };

        template <class TOtherVDisks>
        void Load() {
            ui64 compThreshold = 128 << 20;
            auto seg = MakeIntrusive<TFreshSegment>(GetLevelIndexSetting().HullCtx, compThreshold,
                TAppData::TimeProvider->Now(), Arena);

            {
                ui64 LsnCounter = 1;
                TOtherVDisks aps(&LsnCounter);

                TInstant dataStart = TAppData::TimeProvider->Now();
                ui64 tabletId = 0x100100000010003ul;
                ui32 gen = 5;
                TMemRecLogoBlob memRec;
                memRec.SetNoBlob();
                for (ui32 i = 1; i < 500000; i++) {
                    ui32 step = i;
                    // put regular update
                    ui32 part = 1;
                    TLogoBlobID id(tabletId, gen, step, 0, 0, 0);
                    seg->Put(LsnCounter++, TKeyLogoBlob(TLogoBlobID(id, part)), memRec);
                    // put to appendixes
                    aps.AddToAll(id, memRec);
                    aps.ApplyIfRequired(seg.Get());
                }
                TInstant dataStop = TAppData::TimeProvider->Now();
                STR << "Load data: " << (dataStop - dataStart).ToString() << "\n";
                STR << "InPlaceSizeApproximation: " << seg->InPlaceSizeApproximation() << "\n";
                STR << "aps.Compactions: " << aps.Compactions << "\n";
                STR << "CompactionStat: ";
                seg->GetCompactionStat().Output(STR);
                STR << "\n";
            }

            {
                TInstant delStart = TAppData::TimeProvider->Now();
                seg.Reset(nullptr);
                TInstant delStop = TAppData::TimeProvider->Now();
                STR << "Data delete: " << (delStop - delStart).ToString() << "\n";
            }
        }

        Y_UNIT_TEST(PerfAppendix) {
            Load<TOtherVDisksAppendix>();
        }

        Y_UNIT_TEST(PerfSkipList) {
            Load<TOtherVDisksSkipList>();
        }

        Y_UNIT_TEST(IteratorTest) {
            ui64 compThreshold = 128 << 20;
            auto hullCtx = GetLevelIndexSetting().HullCtx;
            auto seg = MakeIntrusive<TFreshSegment>(hullCtx, compThreshold, TAppData::TimeProvider->Now(), Arena);

            std::deque<TLogoBlobID> withMerge, withoutMerge;

            ui64 lsn = 1;
            const ui64 tabletId = 0x100100000010003ul;
            const ui32 gen = 5;

            TMemRecLogoBlob memRec;
            memRec.SetNoBlob();

            for (ui32 i = 1; i < 500000; i++) {
                const ui32 step = i;

                TLogoBlobID id(tabletId, gen, step, 0, 0, 0);
                seg->Put(lsn++, TKeyLogoBlob(id), memRec);
                withMerge.push_back(id);
                withoutMerge.push_back(id);
                if (RandomNumber<ui32>() % 2) {
                    seg->Put(lsn++, TKeyLogoBlob(id), memRec);
                    withoutMerge.push_back(id);
                }
            }

            auto snap = seg->GetSnapshot();
            using T = std::decay_t<decltype(snap)>;

            auto check = [&](auto iter, auto& queue, auto&& getKey) {
                for (iter.SeekToFirst(); iter.Valid(); iter.Next()) {
                    UNIT_ASSERT(!queue.empty());
                    UNIT_ASSERT_VALUES_EQUAL(queue.front(), getKey(iter).LogoBlobID());
                    queue.pop_front();
                }
                UNIT_ASSERT(queue.empty());
            };

            check(T::TForwardIterator(hullCtx, &snap), withMerge, [](auto& iter) { return iter.GetCurKey(); });
            check(T::TIteratorWOMerge(hullCtx, &snap), withoutMerge, [](auto& iter) { return iter.GetUnmergedKey(); });
        }
    }

} // NKikimr
