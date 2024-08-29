#include "blobstorage_hulldefs.h"
#include "hullbase_logoblob.h"
#include "hullds_heap_it.h"
#include "hullds_ut.h"
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/blobstorage/vdisk/hulldb/generic/hullds_idxsnap_it.h>
#include <ydb/core/blobstorage/vdisk/hulldb/generic/hullds_sst_it_all_ut.h>
#include <ydb/core/blobstorage/vdisk/hulldb/generic/hullds_sstslice_it.h>

#include <ydb/core/blobstorage/vdisk/hulldb/fresh/fresh_data.h>
#include <ydb/core/blobstorage/vdisk/hulldb/fresh/fresh_datasnap.h>

#include <util/stream/null.h>
#include <util/system/hp_timer.h>

// change to Cerr if you want logging
#define STR Cnull

namespace NKikimr {

    Y_UNIT_TEST_SUITE(THullDsHeapItTest) {

        static std::shared_ptr<TRopeArena> Arena = std::make_shared<TRopeArena>(&TRopeArenaBackend::Allocate);

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

        static ::NMonitoring::TDynamicCounters DynCounters;

        TKeyLogoBlob CreateKey(int num, ui64 tabletId = 0, ui32 generation = 0, ui32 channel = 0, ui32 cookie = 0) {
            return TKeyLogoBlob(TLogoBlobID(tabletId, generation, num, channel, 0, cookie));
        }

        TLevelIndexSettings GetLevelIndexSetting() {
            static TLevelIndexSettings settings(TestCtx.GetHullCtx(),
                    Level0MaxSstsAtOnce, BufSize, CompThreshold, HistoryWindow, HistoryBuckets, false, false);
            return settings;
        }

        class TTestSampler {
        public:
            void LoadAppendixTreeKeys(const TVector <int> &keys) {
                AppendixTreeKeys.push_back(keys);
                IntKeys.insert(IntKeys.end(), keys.begin(), keys.end());
            }

            void LoadSstKeys(ui32 step, ui32 recs, ui32 plus) {
                for (ui32 i = 0; i < recs; i++) {
                    IntKeys.push_back(step);
                    step += plus;
                }
            }

            void LoadOrderedSstsKeys(ui32 step, ui32 recs, ui32 plus, ui32 ssts) {
                for (ui32 i = 0; i < ssts; i++) {
                    IntKeys.push_back(step);
                    step += recs * plus;
                }
            }

            void GenRandTree(ui32 appendixNum, ui32 appendixSize, ui32 maxKey = 100) {
                for (ui32 i = 0; i < appendixNum; i++) {
                    TVector <int> appendixKeys;
                    for (ui32 j = 0; j < appendixSize; j++) {
                        appendixKeys.push_back(TAppData::RandomProvider->GenRand64() % maxKey);
                    }
                    std::sort(appendixKeys.begin(), appendixKeys.end());
                    appendixKeys.erase(std::unique(appendixKeys.begin(), appendixKeys.end()), appendixKeys.end());
                    LoadAppendixTreeKeys(appendixKeys);
                }
            }

            void GenRandLogoBlobOrderedSsts(ui32 maxPlus, ui32 maxRec, ui32 maxSsts, ui32 step = 0) {
                LogoBlobOrderedSsts.push_back(GenerateOrderedSsts(step, TAppData::RandomProvider->GenRand64() % maxPlus + 1,
                                                        TAppData::RandomProvider->GenRand64() % maxRec + 1,
                                                        TAppData::RandomProvider->GenRand64() % maxSsts + 1));
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
                size_t stagingCapacity = AppendixTreeKeys.size();
                auto c = std::make_shared<TFreshAppendixTree>(hullCtx, stagingCapacity);
                ui64 lsn = 1;
                for (const auto &x : AppendixTreeKeys) {
                    auto appendix = CreateAppendix(x);
                    AddAppendix(c, appendix, lsn);
                    ++stagingCapacity;
                }
                auto snapFreshAppendix = c->GetSnapshot();
                return snapFreshAppendix;
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

            auto PrepareFreshDataSnap(const TVector<int> &vFresh) {
                IntKeys.insert(IntKeys.end(), vFresh.begin(), vFresh.end());
                auto freshData = CreateFreshData(vFresh);
                return freshData->GetSnapshot();
            }

            auto PrepareLogoBlobOrderedSsts() {
                return LogoBlobOrderedSsts;
            }

            auto GetKeys() {
                return IntKeys;
            }

        private:
            TVector<TVector<int>> AppendixTreeKeys;
            TVector<int> IntKeys;
            TVector<TLogoBlobOrderedSstsPtr> LogoBlobOrderedSsts;
        };

        Y_UNIT_TEST(HeapForwardIteratorAllEntities) {
            TTestSampler sampler;
            TLogoBlobSstPtr ptr(GenerateSst(10, 5, 2));
            TLogoBlobOrderedSstsPtr ptrOrdered(GenerateOrderedSsts(9, 2, 3, 3));
            sampler.LoadSstKeys(10, 5, 2);
            sampler.LoadOrderedSstsKeys(9, 2, 3, 3);
            sampler.LoadAppendixTreeKeys({3, 4, 24, 56});
            sampler.LoadAppendixTreeKeys({3, 5, 7, 74});
            TMemIterator memIt(ptr.Get());
            THullCtxPtr hullCtx = TestCtx.GetHullCtx();
            TReadIterator readIt(hullCtx, ptrOrdered.Get());
            THeapIterator<TKeyLogoBlob, TMemRecLogoBlob, true> heapIt;

            auto snapFreshAppendix = sampler.PrepareAppendixTreeSnap(hullCtx);
            TFreshAppendixTreeSnap::TForwardIterator appendixTreeIt(hullCtx, &snapFreshAppendix);

            auto snapFreshData = sampler.PrepareFreshDataSnap({2, 5, 13, 17});
            TFreshDataSnapshot<TKeyLogoBlob, TMemRecLogoBlob>::TForwardIterator freshDataIt(hullCtx, &snapFreshData);

            memIt.PutToHeap(heapIt);
            readIt.PutToHeap(heapIt);
            appendixTreeIt.PutToHeap(heapIt);
            freshDataIt.PutToHeap(heapIt);

            heapIt.SeekToFirst();
            TStringStream str;
            while (heapIt.Valid()) {
                str << heapIt.GetCurKey().ToString();
                heapIt.Next();
            }

            auto intKeys = sampler.GetKeys();
            std::sort(intKeys.begin(), intKeys.end());
            intKeys.erase(std::unique(intKeys.begin(), intKeys.end()), intKeys.end());
            TStringStream result;
            for (const auto &x : intKeys) {
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
            TTestSampler sampler;
            TLogoBlobSstPtr ptr(GenerateSst(10, 5, 2));
            TLogoBlobOrderedSstsPtr ptrOrdered(GenerateOrderedSsts(9, 2, 3, 3));
            sampler.LoadSstKeys(10, 5, 2);
            sampler.LoadOrderedSstsKeys(9, 2, 3, 3);
            sampler.LoadAppendixTreeKeys({3, 4, 24, 56});
            sampler.LoadAppendixTreeKeys({3, 5, 7, 74});
            TMemIterator memIt(ptr.Get());
            THullCtxPtr hullCtx = TestCtx.GetHullCtx();
            TReadIterator readIt(hullCtx, ptrOrdered.Get());
            THeapIterator<TKeyLogoBlob, TMemRecLogoBlob, false> heapIt;

            auto snapFreshAppendix = sampler.PrepareAppendixTreeSnap(hullCtx);
            TFreshAppendixTreeSnap::TBackwardIterator appendixTreeIt(hullCtx, &snapFreshAppendix);

            auto snapFreshData = sampler.PrepareFreshDataSnap({2, 5, 13, 17});
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
                heapIt.Prev();
            }

            auto intKeys = sampler.GetKeys();
            std::sort(intKeys.begin(), intKeys.end(), std::greater<int>());
            intKeys.erase(std::unique(intKeys.begin(), intKeys.end()), intKeys.end());
            TStringStream result;
            for (const auto &x : intKeys) {
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

        template<typename TIterator>
        void ForwardIterationBenchmark(TIterator &it, const char* description) {
            THPTimer timer;
            THeapIterator<TKeyLogoBlob, TMemRecLogoBlob, true> heapIt(&it);
            heapIt.SeekToFirst();
            while (heapIt.Valid()) {
                heapIt.Next();
            }
            STR << "HeapForwardIterator: " << timer.Passed() << "\n";
            timer.Reset();
            it.SeekToFirst();
            while (it.Valid()) {
                it.Next();
            }
            STR << description << ": " << timer.Passed() << "\n";
        }

        template<typename TIterator>
        void BackwardIterationBenchmark(TIterator &it, const char* description, TKeyLogoBlob maxKey) {
            THPTimer timer;
            it.Seek(maxKey);
            while (it.Valid()) {
                it.Prev();
            }
            STR << description << ": " << timer.Passed() << "\n";
            THeapIterator<TKeyLogoBlob, TMemRecLogoBlob, false> heapIt(&it);

            timer.Reset();
            heapIt.Seek(maxKey);
            while (heapIt.Valid()) {
                heapIt.Prev();
            }
            STR << "HeapBackwardIterator: " << timer.Passed() << "\n";
        }

        Y_UNIT_TEST(HeapAppendixTreeForwardIteratorBenchmark) {
            THullCtxPtr hullCtx = TestCtx.GetHullCtx();
            TTestSampler sampler;
            sampler.GenRandTree(1000, 1000);
            auto snapFreshAppendix = sampler.PrepareAppendixTreeSnap(hullCtx);
            TFreshAppendixTreeSnap::TForwardIterator appendixTreeIt(hullCtx, &snapFreshAppendix);
            ForwardIterationBenchmark(appendixTreeIt, "AppendixTreeForwardIterator");
        }

        Y_UNIT_TEST(HeapAppendixTreeBackwardIteratorBenchmark) {
            THullCtxPtr hullCtx = TestCtx.GetHullCtx();
            TTestSampler sampler;
            sampler.GenRandTree(1000, 1000);
            auto snapFreshAppendix = sampler.PrepareAppendixTreeSnap(hullCtx);
            TFreshAppendixTreeSnap::TBackwardIterator appendixTreeIt(hullCtx, &snapFreshAppendix);
            BackwardIterationBenchmark(appendixTreeIt, "AppendixTreeBackwardIterator", CreateKey(1001));
        }

        Y_UNIT_TEST(HeapLevelSliceForwardIteratorBenchmark) {
            THullCtxPtr hullCtx = TestCtx.GetHullCtx();
            TTestSampler sampler;
            sampler.GenRandLogoBlobOrderedSsts(50, 100, 100);
            auto logoBlobOrderedSsts = sampler.PrepareLogoBlobOrderedSsts();
            TLevelSlice<TKeyLogoBlob, TMemRecLogoBlob>::TForwardIterator levelSliceIterator(hullCtx, logoBlobOrderedSsts);
            ForwardIterationBenchmark(levelSliceIterator, "LevelSliceForwardIterator");
        }

        Y_UNIT_TEST(HeapLevelSliceBackwardIteratorBenchmark) {
            THullCtxPtr hullCtx = TestCtx.GetHullCtx();
            TTestSampler sampler;
            sampler.GenRandLogoBlobOrderedSsts(50, 100, 100);
            auto logoBlobOrderedSsts = sampler.PrepareLogoBlobOrderedSsts();
            TLevelSlice<TKeyLogoBlob, TMemRecLogoBlob>::TBackwardIterator levelSliceIterator(hullCtx, logoBlobOrderedSsts);
            BackwardIterationBenchmark(levelSliceIterator, "LevelSliceBackwardIterator", CreateKey(1'000'000));
        }
    }

} // NKikimr