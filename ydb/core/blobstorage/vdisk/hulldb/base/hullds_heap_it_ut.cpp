#include "blobstorage_hulldefs.h"
#include "hullbase_logoblob.h"
#include "hullds_heap_it.h"
#include "hullds_ut.h"
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/blobstorage/vdisk/hulldb/generic/hullds_sst_it_all_ut.h>

#include <util/stream/null.h>

// change to Cerr if you want logging
#define STR Cnull

namespace NKikimr {

    Y_UNIT_TEST_SUITE(THullDsHeapItTest) {

        static std::shared_ptr<TRopeArena> Arena = std::make_shared<TRopeArena>(&TRopeArenaBackend::Allocate);
        static TVector<int> IntKeys;

        using namespace NBlobStorageHullSstItHelpers;
        using TFreshAppendix = TFreshAppendix<TKeyLogoBlob, TMemRecLogoBlob>;
        using TFreshAppendixTree = TFreshAppendixTree<TKeyLogoBlob, TMemRecLogoBlob>;
        using TFreshAppendixTreeSnap = TFreshAppendixTreeSnap<TKeyLogoBlob, TMemRecLogoBlob>;
        using TMemIterator = TLogoBlobSst::TMemIterator;
        using TReadIterator = TLogoBlobOrderedSsts::TReadIterator;
        using TFreshAppendixIterator = TFreshAppendix::TIterator;
        using TFreshData = TFreshData<TKeyLogoBlob, TMemRecLogoBlob>;
        using TFreshDataPtr = std::shared_ptr<TFreshData>;
        TTestContexts TestCtx(ChunkSize, CompWorthReadSize);

        static const ui32 Level0MaxSstsAtOnce = 8u;
        static const ui32 BufSize = 64u << 20u;
        static const ui64 CompThreshold = 8u << 20u;
        static const TDuration HistoryWindow = TDuration::Minutes(10);
        static const ui64 HistoryBuckets = 10u;

        TLevelIndexSettings GetLevelIndexSetting() {
            static TLevelIndexSettings settings(TestCtx.GetHullCtx(),
                    Level0MaxSstsAtOnce, BufSize, CompThreshold, HistoryWindow, HistoryBuckets, false, false);
            return settings;
        }

        static ::NMonitoring::TDynamicCounters DynCounters;

        TKeyLogoBlob CreateKey(int num, ui64 tabletId = 0, ui32 generation = 0, ui32 channel = 0, ui32 cookie = 0) {
            return TKeyLogoBlob(TLogoBlobID(tabletId, generation, num, channel, 0, cookie));
        }

        std::shared_ptr<TFreshAppendix> CreateAppendix(const TVector<int> &v, ui64 tabletId = 0, ui32 generation = 0,
                                                       ui32 channel = 0, ui32 cookie = 0) {
            auto group = DynCounters.GetSubgroup("subsystem", "memhull");
            TMemoryConsumer memConsumer(group->GetCounter("MemTotal:FreshIndex"));
            auto result = std::make_shared<TFreshAppendix>(memConsumer);
            for (const auto &x : v) {
                IntKeys.push_back(x);
                result->Add(CreateKey(x, tabletId, generation, channel, cookie), TMemRecLogoBlob());
            }
            return result;
        }

        void AddSstKeys(ui32 step, ui32 recs, ui32 plus) {
            for (ui32 i = 0; i < recs; i++) {
                IntKeys.push_back(step);
                step += plus;
            }
        }

        void AddOrderedSstsKeys(ui32 step, ui32 recs, ui32 plus, ui32 ssts) {
            for (ui32 i = 0; i < ssts; i++) {
                IntKeys.push_back(step);
                step += recs * plus;
            }
        }

        std::shared_ptr<TFreshData> CreateFreshData(const TVector<int> &v, ui64 tabletId = 0, ui32 generation = 0,
                                                    ui32 channel = 0, ui32 cookie = 0) {
            TFreshDataPtr fresh(new TFreshData(GetLevelIndexSetting(), CreateDefaultTimeProvider(), Arena));
            ui64 lsn = 1;
            for (const auto &x : v) {
                IntKeys.push_back(x);
                fresh->Put(lsn++, CreateKey(x, tabletId, generation, channel, cookie), TMemRecLogoBlob());
            }
            return fresh;
        }

        void AddAppendix(
                std::shared_ptr<TFreshAppendixTree> c,
                std::shared_ptr<TFreshAppendix> a,
                ui64 &curLsn) {
            ui64 firstLsn = curLsn;
            ui64 lastLsn = curLsn + a->GetSize() - 1;
            c->AddAppendix(a, firstLsn, lastLsn);
            curLsn += a->GetSize();
        }

        auto PrepareAppendixTreeSnap(THullCtxPtr &hullCtx) {
            TVector<int> v1{3, 4, 24, 56};
            TVector<int> v2{3, 5, 7, 74};
            IntKeys.insert(IntKeys.end(), v1.begin(), v1.end());
            IntKeys.insert(IntKeys.end(), v2.begin(), v2.end());
            auto a1 = CreateAppendix(v1);
            auto a2 = CreateAppendix(v2);
            const size_t stagingCapacity = 4;
            auto c = std::make_shared<TFreshAppendixTree>(hullCtx, stagingCapacity);
            ui64 lsn = 1;
            AddAppendix(c, a1, lsn);
            AddAppendix(c, a2, lsn);
            auto snapFreshAppendix = c->GetSnapshot();
            return snapFreshAppendix;
        }

        auto PrepareFreshDataSnap() {
            TVector<int> vFresh{2, 5, 13, 17};
            IntKeys.insert(IntKeys.end(), vFresh.begin(), vFresh.end());
            auto freshData = CreateFreshData(vFresh);
            return freshData->GetSnapshot();
        }


        Y_UNIT_TEST(HeapForwardIteratorAllEntities) {
            IntKeys.clear();
            TLogoBlobSstPtr ptr(GenerateSst(10, 5, 2));
            TLogoBlobOrderedSstsPtr ptrOrdered(GenerateOrderedSsts(9, 2, 3, 3));
            AddSstKeys(10, 5, 2);
            AddOrderedSstsKeys(9, 2, 3, 3);
            TMemIterator memIt(ptr.Get());
            THullCtxPtr hullCtx = TestCtx.GetHullCtx();
            TReadIterator readIt(hullCtx, ptrOrdered.Get());
            THeapIterator<TKeyLogoBlob, TMemRecLogoBlob, true> heapIt;

            auto snapFreshAppendix = PrepareAppendixTreeSnap(hullCtx);
            TFreshAppendixTreeSnap::TForwardIterator appendixTreeIt(hullCtx, &snapFreshAppendix);

            auto snapFreshData = PrepareFreshDataSnap();
            TFreshDataSnapshot<TKeyLogoBlob, TMemRecLogoBlob>::TForwardIterator freshDataIt(hullCtx, &snapFreshData);

            memIt.PutToHeap(heapIt);
            readIt.PutToHeap(heapIt);
            appendixTreeIt.PutToHeap(heapIt);
            freshDataIt.PutToHeap(heapIt);

            heapIt.SeekToFirst();
            TStringStream str;
            while (heapIt.Valid()) {
                str << heapIt.GetCurKey().ToString();
                STR << heapIt.GetCurKey().ToString() << Endl;
                heapIt.Next();
            }

            std::sort(IntKeys.begin(), IntKeys.end());
            IntKeys.erase(std::unique(IntKeys.begin(), IntKeys.end()), IntKeys.end());
            TStringStream result;
            for (const auto &x : IntKeys) {
                result << CreateKey(x).ToString();
            }

            UNIT_ASSERT(str.Str() == result.Str());

            //selected seeks
            TVector<std::pair<TKeyLogoBlob, TString>> keys;
            keys.emplace_back(CreateKey(0), "[0:0:2:0:0:0:0]");
            keys.emplace_back(CreateKey(6), "[0:0:7:0:0:0:0]");
            keys.emplace_back(CreateKey(12), "[0:0:12:0:0:0:0]");
            keys.emplace_back(CreateKey(22), "[0:0:24:0:0:0:0]");
            for (const auto& x: keys) {
                heapIt.Seek(x.first);
                UNIT_ASSERT(heapIt.Valid() && heapIt.GetCurKey().ToString() == x.second);
            }
            heapIt.Seek(CreateKey(100));
            UNIT_ASSERT(!heapIt.Valid());
        }

        Y_UNIT_TEST(HeapBackwardIteratorAllEntities) {
            IntKeys.clear();
            TLogoBlobSstPtr ptr(GenerateSst(10, 5, 2));
            TLogoBlobOrderedSstsPtr ptrOrdered(GenerateOrderedSsts(9, 2, 3, 3));
            AddSstKeys(10, 5, 2);
            AddOrderedSstsKeys(9, 2, 3, 3);
            TMemIterator memIt(ptr.Get());
            THullCtxPtr hullCtx = TestCtx.GetHullCtx();
            TReadIterator readIt(hullCtx, ptrOrdered.Get());
            THeapIterator<TKeyLogoBlob, TMemRecLogoBlob, false> heapIt;

            auto snapFreshAppendix = PrepareAppendixTreeSnap(hullCtx);
            TFreshAppendixTreeSnap::TBackwardIterator appendixTreeIt(hullCtx, &snapFreshAppendix);

            auto snapFreshData = PrepareFreshDataSnap();
            TFreshDataSnapshot<TKeyLogoBlob, TMemRecLogoBlob>::TBackwardIterator freshDataIt(hullCtx, &snapFreshData);

            memIt.PutToHeap(heapIt);
            readIt.PutToHeap(heapIt);
            appendixTreeIt.PutToHeap(heapIt);
            freshDataIt.PutToHeap(heapIt);
            auto key = CreateKey(100);
            heapIt.Seek(key);
            TStringStream str;
            while (heapIt.Valid()) {
                str << heapIt.GetCurKey().ToString();
                //STR << heapIt.GetCurKey().ToString() << Endl;
                STR << heapIt.GetCurKey().ToString() << Endl;
                heapIt.Prev();
            }

            std::sort(IntKeys.begin(), IntKeys.end(), std::greater<int>());
            IntKeys.erase(std::unique(IntKeys.begin(), IntKeys.end()), IntKeys.end());
            TStringStream result;
            for (const auto &x : IntKeys) {
                result << CreateKey(x).ToString();
            }

            UNIT_ASSERT(str.Str() == result.Str());

            //selected seeks
            TVector<std::pair<TKeyLogoBlob, TString>> keys;
            keys.emplace_back(CreateKey(2), "[0:0:2:0:0:0:0]");
            keys.emplace_back(CreateKey(6), "[0:0:5:0:0:0:0]");
            keys.emplace_back(CreateKey(12), "[0:0:12:0:0:0:0]");
            keys.emplace_back(CreateKey(25), "[0:0:24:0:0:0:0]");
            for (const auto& x: keys) {
                heapIt.Seek(x.first);
                UNIT_ASSERT(heapIt.Valid() && heapIt.GetCurKey().ToString() == x.second);
            }
            heapIt.Seek(CreateKey(0));
            UNIT_ASSERT(!heapIt.Valid());
        }
    }

} // NKikimr

