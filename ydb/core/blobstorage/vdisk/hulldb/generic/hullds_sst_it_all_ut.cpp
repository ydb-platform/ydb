#include "hullds_sst_it_all_ut.h"

#include <algorithm>
#include <random>
#include <util/system/hp_timer.h>

namespace NKikimr {

    Y_UNIT_TEST_SUITE(TBlobStorageHullSstIt) {

        using namespace NBlobStorageHullSstItHelpers;
        using TMemIterator = TLogoBlobSst::TMemIterator;

        Y_UNIT_TEST(TestSeekToFirst) {
            TLogoBlobSstPtr ptr(GenerateSst(10, 10, 1));
            TMemIterator it(ptr.Get());
            it.SeekToFirst();

            TStringStream str;
            while (it.Valid()) {
                str << it.GetCurKey().ToString();
                it.Next();
            }
            TString result("[0:0:10:0:0:0:0][0:0:11:0:0:0:0]"
                          "[0:0:12:0:0:0:0][0:0:13:0:0:0:0]"
                          "[0:0:14:0:0:0:0][0:0:15:0:0:0:0]"
                          "[0:0:16:0:0:0:0][0:0:17:0:0:0:0]"
                          "[0:0:18:0:0:0:0][0:0:19:0:0:0:0]");
            UNIT_ASSERT(str.Str() == result);
        }

        Y_UNIT_TEST(TestSeekToLast) {
            TLogoBlobSstPtr ptr(GenerateSst(10, 10, 1));
            TMemIterator it(ptr.Get());
            it.SeekToLast();

            TStringStream str;
            while (it.Valid()) {
                str << it.GetCurKey().ToString();
                it.Prev();
            }
            TString result("[0:0:19:0:0:0:0][0:0:18:0:0:0:0]"
                          "[0:0:17:0:0:0:0][0:0:16:0:0:0:0]"
                          "[0:0:15:0:0:0:0][0:0:14:0:0:0:0]"
                          "[0:0:13:0:0:0:0][0:0:12:0:0:0:0]"
                          "[0:0:11:0:0:0:0][0:0:10:0:0:0:0]");
            UNIT_ASSERT(str.Str() == result);
        }

        Y_UNIT_TEST(TestSeekExactAndNext) {
            TLogoBlobSstPtr ptr(GenerateSst(10, 10, 1));
            TMemIterator it(ptr.Get());

            TLogoBlobID id;
            id = TLogoBlobID(0, 0, 15, 0, 0, 0);
            it.Seek(id);
            UNIT_ASSERT(it.GetCurKey().ToString() == TString("[0:0:15:0:0:0:0]"));

            TStringStream str;
            while (it.Valid()) {
                str << it.GetCurKey().ToString();
                it.Next();
            }
            TString result("[0:0:15:0:0:0:0][0:0:16:0:0:0:0]"
                          "[0:0:17:0:0:0:0][0:0:18:0:0:0:0]"
                          "[0:0:19:0:0:0:0]");
            UNIT_ASSERT(str.Str() == result);
        }

        Y_UNIT_TEST(TestSeekExactAndPrev) {
            TLogoBlobSstPtr ptr(GenerateSst(10, 10, 1));
            TMemIterator it(ptr.Get());

            TLogoBlobID id;
            id = TLogoBlobID(0, 0, 15, 0, 0, 0);
            it.Seek(id);
            UNIT_ASSERT(it.GetCurKey().ToString() == TString("[0:0:15:0:0:0:0]"));

            TStringStream str;
            while (it.Valid()) {
                str << it.GetCurKey().ToString();
                it.Prev();
            }
            TString result("[0:0:15:0:0:0:0][0:0:14:0:0:0:0]"
                          "[0:0:13:0:0:0:0][0:0:12:0:0:0:0]"
                          "[0:0:11:0:0:0:0][0:0:10:0:0:0:0]");
            UNIT_ASSERT(str.Str() == result);
        }

        Y_UNIT_TEST(TestSeekBefore) {
            TLogoBlobSstPtr ptr(GenerateSst(10, 10, 1));
            TMemIterator it(ptr.Get());

            TLogoBlobID id;
            id = TLogoBlobID(0, 0, 5, 0, 0, 0);
            it.Seek(id);
            UNIT_ASSERT(it.GetCurKey().ToString() == "[0:0:10:0:0:0:0]");
        }

        Y_UNIT_TEST(TestSeekAfterAndPrev) {
            TLogoBlobSstPtr ptr(GenerateSst(10, 10, 1));
            TMemIterator it(ptr.Get());

            TLogoBlobID id;
            id = TLogoBlobID(0, 0, 25, 0, 0, 0);
            it.Seek(id);
            UNIT_ASSERT(!it.Valid());
            it.Prev();
            UNIT_ASSERT(it.Valid());
            UNIT_ASSERT(it.GetCurKey().ToString() == "[0:0:19:0:0:0:0]");
        }

        Y_UNIT_TEST(TestSeekNotExactBefore) {
            TLogoBlobSstPtr ptr(GenerateSst(10, 10, 2));
            TMemIterator it(ptr.Get());

            TLogoBlobID id;
            id = TLogoBlobID(0, 0, 15, 0, 0, 0);
            it.Seek(id);
            UNIT_ASSERT(it.GetCurKey().ToString() == "[0:0:16:0:0:0:0]");
        }

        Y_UNIT_TEST(TestSstIndexSeekAndIterate) {
            TTestContexts ctxs;
            TTrackableVector<TLogoBlobSst::TRec> index(TMemoryConsumer(ctxs.GetVCtx()->SstIndex));

            auto addRecord = [&index](ui64 tabletId, ui32 step) {
                TLogoBlobID id(tabletId, 0, step, 0, 0, 0);
                index.emplace_back(TKeyLogoBlob(id), TMemRecLogoBlob());
            };

            addRecord(10, 0);
            addRecord(10, 10);
            addRecord(20, 0);
            addRecord(20, 10);
            addRecord(20, 300);

            TLogoBlobSstPtr ptr(new TLogoBlobSst(ctxs.GetVCtx()));
            ptr->LoadLinearIndex(index);

            TMemIterator it(ptr.Get());

            it.Seek(TLogoBlobID(5, 0, 0, 0, 0, 0));
            UNIT_ASSERT(it.GetCurKey().ToString() == "[10:0:0:0:0:0:0]");

            it.Seek(TLogoBlobID(10, 0, 0, 0, 0, 0));
            UNIT_ASSERT(it.GetCurKey().ToString() == "[10:0:0:0:0:0:0]");

            it.Seek(TLogoBlobID(10, 0, 5, 0, 0, 0));
            UNIT_ASSERT(it.GetCurKey().ToString() == "[10:0:10:0:0:0:0]");

            it.Seek(TLogoBlobID(10, 0, 10, 0, 0, 0));
            UNIT_ASSERT(it.GetCurKey().ToString() == "[10:0:10:0:0:0:0]");

            it.Seek(TLogoBlobID(10, 0, 15, 0, 0, 0));
            UNIT_ASSERT(it.GetCurKey().ToString() == "[20:0:0:0:0:0:0]");

            it.Seek(TLogoBlobID(15, 0, 0, 0, 0, 0));
            UNIT_ASSERT(it.GetCurKey().ToString() == "[20:0:0:0:0:0:0]");

            it.Seek(TLogoBlobID(20, 0, 0, 0, 0, 0));
            UNIT_ASSERT(it.GetCurKey().ToString() == "[20:0:0:0:0:0:0]");

            it.Seek(TLogoBlobID(20, 0, 5, 0, 0, 0));
            UNIT_ASSERT(it.GetCurKey().ToString() == "[20:0:10:0:0:0:0]");

            it.Seek(TLogoBlobID(20, 0, 10, 0, 0, 0));
            UNIT_ASSERT(it.GetCurKey().ToString() == "[20:0:10:0:0:0:0]");

            it.Seek(TLogoBlobID(20, 0, 15, 0, 0, 0));
            UNIT_ASSERT(it.GetCurKey().ToString() == "[20:0:300:0:0:0:0]");

            it.Seek(TLogoBlobID(20, 0, 300, 0, 0, 0));
            UNIT_ASSERT(it.GetCurKey().ToString() == "[20:0:300:0:0:0:0]");

            it.Seek(TLogoBlobID(20, 0, 400, 0, 0, 0));
            UNIT_ASSERT(!it.Valid());

            it.Seek(TLogoBlobID(25, 0, 0, 0, 0, 0));
            UNIT_ASSERT(!it.Valid());

            it.SeekToFirst();
            it.Prev();
            UNIT_ASSERT(!it.Valid());

            it.SeekToLast();
            it.Next();
            UNIT_ASSERT(!it.Valid());

            it.SeekToFirst();
            TStringStream str1;
            while (it.Valid()) {
                str1 << it.GetCurKey().ToString();
                it.Next();
            }
            UNIT_ASSERT(str1.Str()
                == "[10:0:0:0:0:0:0][10:0:10:0:0:0:0][20:0:0:0:0:0:0][20:0:10:0:0:0:0][20:0:300:0:0:0:0]");

            it.SeekToLast();
            TStringStream str2;
            while (it.Valid()) {
                str2 << it.GetCurKey().ToString();
                it.Prev();
            }
            UNIT_ASSERT(str2.Str()
                == "[20:0:300:0:0:0:0][20:0:10:0:0:0:0][20:0:0:0:0:0:0][10:0:10:0:0:0:0][10:0:0:0:0:0:0]");
        }

        Y_UNIT_TEST(TestSstIndexSaveLoad) {
            TTestContexts ctxs;
            TTrackableVector<TLogoBlobSst::TRec> index(TMemoryConsumer(ctxs.GetVCtx()->SstIndex));

            auto addRecord = [&index](ui64 tabletId, ui32 step, ui32 blobSize) {
                TLogoBlobID id(tabletId, 0, step, 0, blobSize, 0);
                index.emplace_back(TKeyLogoBlob(id), TMemRecLogoBlob());
            };

            addRecord(10, 0, 1);
            addRecord(10, 10, 2);
            addRecord(20, 0, 3);
            addRecord(20, 10, 4);
            addRecord(20, 300, 5);

            TLogoBlobSstPtr ptr(new TLogoBlobSst(ctxs.GetVCtx()));
            ptr->LoadLinearIndex(index);

            const auto& indexHigh = ptr->IndexHigh;
            auto high = indexHigh.begin();

            using TLogoBlobIdHigh = TRecIndex<TKeyLogoBlob, TMemRecLogoBlob>::TLogoBlobIdHigh;

            UNIT_ASSERT(high->GetKey() == TLogoBlobIdHigh(10, 0, 0, 0));
            UNIT_ASSERT(high->GetLowRangeEndIndex() == 2);
            ++high;
            UNIT_ASSERT(high->GetKey() == TLogoBlobIdHigh(20, 0, 0, 0));
            UNIT_ASSERT(high->GetLowRangeEndIndex() == 4);
            ++high;
            UNIT_ASSERT(high->GetKey() == TLogoBlobIdHigh(20, 0, 300, 0));
            UNIT_ASSERT(high->GetLowRangeEndIndex() == 5);
            ++high;
            UNIT_ASSERT(high == indexHigh.end());

            const auto& indexLow = ptr->IndexLow;
            auto low = indexLow.begin();

            using TLogoBlobIdLow = TRecIndex<TKeyLogoBlob, TMemRecLogoBlob>::TLogoBlobIdLow;

            UNIT_ASSERT(low->GetKey() == TLogoBlobIdLow(0, 0, 0, 1, 0));
            ++low;
            UNIT_ASSERT(low->GetKey() == TLogoBlobIdLow(10, 0, 0, 2, 0));
            ++low;
            UNIT_ASSERT(low->GetKey() == TLogoBlobIdLow(0, 0, 0, 3, 0));
            ++low;
            UNIT_ASSERT(low->GetKey() == TLogoBlobIdLow(10, 0, 0, 4, 0));
            ++low;
            UNIT_ASSERT(low->GetKey() == TLogoBlobIdLow(300, 0, 0, 5, 0));
            ++low;
            UNIT_ASSERT(low == indexLow.end());

            TTrackableVector<TLogoBlobSst::TRec> checkIndex(TMemoryConsumer(ctxs.GetVCtx()->SstIndex));
            ptr->SaveLinearIndex(&checkIndex);

            for (auto i = index.begin(), c = checkIndex.begin(); i != index.end(); ++i, ++c) {
                UNIT_ASSERT(i->GetKey() == c->GetKey());
            }
        }
    } // TBlobStorageHullSstIt

    Y_UNIT_TEST_SUITE(TBlobStorageHullOrderedSstsIt) {

        using namespace NBlobStorageHullSstItHelpers;
        using TIterator = TLogoBlobOrderedSsts::TReadIterator;
        TTestContexts TestCtx(ChunkSize, CompWorthReadSize);

        Y_UNIT_TEST(TestSeekToFirst) {
            TLogoBlobOrderedSstsPtr ptr(GenerateOrderedSsts(10, 5, 1, 3));
            THullCtxPtr hullCtx = TestCtx.GetHullCtx();
            TIterator it(hullCtx, ptr.Get());
            it.SeekToFirst();

            TStringStream str;
            while (it.Valid()) {
                str << it.GetCurKey().ToString();
                it.Next();
            }
            TString result("[0:0:10:0:0:0:0][0:0:11:0:0:0:0]"
                          "[0:0:12:0:0:0:0][0:0:13:0:0:0:0]"
                          "[0:0:14:0:0:0:0][0:0:15:0:0:0:0]"
                          "[0:0:16:0:0:0:0][0:0:17:0:0:0:0]"
                          "[0:0:18:0:0:0:0][0:0:19:0:0:0:0]"
                          "[0:0:20:0:0:0:0][0:0:21:0:0:0:0]"
                          "[0:0:22:0:0:0:0][0:0:23:0:0:0:0]"
                          "[0:0:24:0:0:0:0]");
            UNIT_ASSERT(str.Str() == result);
        }

        Y_UNIT_TEST(TestSeekToLast) {
            TLogoBlobOrderedSstsPtr ptr(GenerateOrderedSsts(10, 5, 1, 3));
            THullCtxPtr hullCtx = TestCtx.GetHullCtx();
            TIterator it(hullCtx, ptr.Get());
            it.SeekToLast();

            TStringStream str;
            while (it.Valid()) {
                str << it.GetCurKey().ToString();
                it.Prev();
            }
            TString result("[0:0:24:0:0:0:0][0:0:23:0:0:0:0]"
                          "[0:0:22:0:0:0:0][0:0:21:0:0:0:0]"
                          "[0:0:20:0:0:0:0][0:0:19:0:0:0:0]"
                          "[0:0:18:0:0:0:0][0:0:17:0:0:0:0]"
                          "[0:0:16:0:0:0:0][0:0:15:0:0:0:0]"
                          "[0:0:14:0:0:0:0][0:0:13:0:0:0:0]"
                          "[0:0:12:0:0:0:0][0:0:11:0:0:0:0]"
                          "[0:0:10:0:0:0:0]");
            UNIT_ASSERT(str.Str() == result);
        }

        Y_UNIT_TEST(TestSeekAfterAndPrev) {
            TLogoBlobOrderedSstsPtr ptr(GenerateOrderedSsts(10, 5, 1, 3));
            THullCtxPtr hullCtx = TestCtx.GetHullCtx();
            TIterator it(hullCtx, ptr.Get());

            TLogoBlobID id;
            id = TLogoBlobID(0, 0, 30, 0, 0, 0);
            it.Seek(id);
            UNIT_ASSERT(!it.Valid());
            it.Prev();
            UNIT_ASSERT(it.Valid());
            UNIT_ASSERT(it.GetCurKey().ToString() == "[0:0:24:0:0:0:0]");
        }

        // FIXME: not all cases covered
    }

    Y_UNIT_TEST_SUITE(TBlobStorageHullReversePhysicalIt) {

        using namespace NBlobStorageHullSstItHelpers;
        using TLevelSlice = ::NKikimr::TLevelSlice<TKeyLogoBlob, TMemRecLogoBlob>;
        using TLevelSliceSnapshot = ::NKikimr::TLevelSliceSnapshot<TKeyLogoBlob, TMemRecLogoBlob>;

        struct TRecordPosition {
            ui32 Level;
            ui64 SstId;
            TLogoBlobID BlobId;
        };

        void AssertEqual(const TVector<TRecordPosition>& actual, const TVector<TRecordPosition>& expected,
                ui32 round) {
            UNIT_ASSERT_C(actual.size() == expected.size(),
                "round# " << round << " actual size# " << actual.size() << " expected size# " << expected.size());
            for (size_t i = 0; i < expected.size(); ++i) {
                UNIT_ASSERT_C(actual[i].Level == expected[i].Level
                        && actual[i].SstId == expected[i].SstId
                        && actual[i].BlobId == expected[i].BlobId,
                    "round# " << round << " item# " << i
                    << " actual# {level# " << actual[i].Level << " sst# " << actual[i].SstId
                    << " key# " << actual[i].BlobId.ToString() << "}"
                    << " expected# {level# " << expected[i].Level << " sst# " << expected[i].SstId
                    << " key# " << expected[i].BlobId.ToString() << "}");
            }
        }

        TLogoBlobSstPtr MakeSst(TVDiskContextPtr vctx, ui64 tabletId, ui32 firstStep, ui32 numRecords,
                ui64 sstId) {
            TTrackableVector<TLogoBlobSst::TRec> index(TMemoryConsumer(vctx->SstIndex));
            auto sst = MakeIntrusive<TLogoBlobSst>(vctx);
            for (ui32 i = 0; i < numRecords; ++i) {
                TLogoBlobID id(tabletId, 1, firstStep + i, 0, 0, 0);
                index.emplace_back(TKeyLogoBlob(id), TMemRecLogoBlob());
            }
            sst->LoadLinearIndex(index);
            sst->AssignedSstId = sstId;
            return sst;
        }

        void AppendExpected(TVector<TRecordPosition>& expected, ui32 level, ui64 sstId,
                ui64 tabletId, ui32 firstStep, ui32 numRecords) {
            for (ui32 i = 0; i < numRecords; ++i) {
                expected.push_back({level, sstId, TLogoBlobID(tabletId, 1, firstStep + i, 0, 0, 0)});
            }
        }

        TVector<TRecordPosition> ReadSnapshot(const TLevelSliceSnapshot& snapshot, bool reverse) {
            TVector<TRecordPosition> result;
            TLevelSliceSnapshot::TSstIterator sstIt(&snapshot);
            if (reverse) {
                sstIt.SeekToLast();
            } else {
                sstIt.SeekToFirst();
            }

            while (sstIt.Valid()) {
                const auto levelSst = sstIt.Get();
                TLogoBlobSst::TMemIterator memIt(levelSst.SstPtr.Get());
                if (reverse) {
                    memIt.SeekToLast();
                } else {
                    memIt.SeekToFirst();
                }
                while (memIt.Valid()) {
                    result.push_back({levelSst.Level, levelSst.SstPtr->AssignedSstId,
                        memIt.GetCurKey().LogoBlobID()});
                    if (reverse) {
                        memIt.Prev();
                    } else {
                        memIt.Next();
                    }
                }
                if (reverse) {
                    sstIt.Prev();
                } else {
                    sstIt.Next();
                }
            }
            return result;
        }

        Y_UNIT_TEST(GenericMemIteratorReverseBoundaries) {
            using TBlockSst = TLevelSegment<TKeyBlock, TMemRecBlock>;
            TTestContexts contexts;

            auto empty = MakeIntrusive<TBlockSst>(contexts.GetVCtx());
            TBlockSst::TMemIterator emptyIt(empty.Get());
            emptyIt.SeekToLast();
            UNIT_ASSERT(!emptyIt.Valid());

            auto sst = MakeIntrusive<TBlockSst>(contexts.GetVCtx());
            for (ui64 tabletId = 1; tabletId <= 5; ++tabletId) {
                sst->LoadedIndex.emplace_back(TKeyBlock(tabletId), TMemRecBlock(tabletId));
            }

            TBlockSst::TMemIterator it(sst.Get());
            it.SeekToLast();
            for (ui64 expected = 5; expected != 0; --expected) {
                UNIT_ASSERT(it.Valid());
                UNIT_ASSERT_VALUES_EQUAL(it.GetCurKey().TabletId, expected);
                it.Prev();
            }
            UNIT_ASSERT(!it.Valid());

            it.SeekToLast();
            UNIT_ASSERT(it.Valid());
            UNIT_ASSERT_VALUES_EQUAL(it.GetCurKey().TabletId, 5);
        }

        Y_UNIT_TEST(RandomizedSnapshotReverseWalk) {
            std::mt19937_64 random(0x8f3d9a24c61b507eULL);
            TTestContexts contexts;
            THPTimer timer;

            for (ui32 round = 0; TDuration::Seconds(timer.Passed()) < TDuration::Minutes(5); ++round) {
                auto levelCtx = std::make_shared<TLevelIndexCtx>();
                auto slice = MakeIntrusive<TLevelSlice>(contexts.GetLevelIndexSettings(), levelCtx);
                TVector<TRecordPosition> expected;
                ui64 nextSstId = 1;

                const ui32 numLevel0Ssts = random() % 8;
                const ui32 level0SnapshotLimit = random() % (numLevel0Ssts + 1);
                for (ui32 sstIdx = 0; sstIdx < numLevel0Ssts; ++sstIdx) {
                    const ui32 numRecords = 1 + random() % 9;
                    const ui32 firstStep = random() % 1000000;
                    const ui64 tabletId = 100 + sstIdx;
                    auto sst = MakeSst(contexts.GetVCtx(), tabletId, firstStep, numRecords, nextSstId);
                    slice->Level0.Put(sst);
                    if (sstIdx < level0SnapshotLimit) {
                        AppendExpected(expected, 0, nextSstId, tabletId, firstStep, numRecords);
                    }
                    ++nextSstId;
                }

                const ui32 numSortedLevels = random() % 8;
                for (ui32 levelIdx = 0; levelIdx < numSortedLevels; ++levelIdx) {
                    slice->SortedLevels.emplace_back(TKeyLogoBlob());
                    const ui32 numSsts = random() % 7; // empty levels are intentional
                    ui32 nextStep = random() % 1000;
                    const ui64 tabletId = 1000 + levelIdx;
                    for (ui32 sstIdx = 0; sstIdx < numSsts; ++sstIdx) {
                        const ui32 numRecords = 1 + random() % 9;
                        auto sst = MakeSst(contexts.GetVCtx(), tabletId, nextStep, numRecords, nextSstId);
                        slice->SortedLevels.back().Put(sst);
                        AppendExpected(expected, levelIdx + 1, nextSstId, tabletId, nextStep, numRecords);
                        nextStep += numRecords + 1 + random() % 20;
                        ++nextSstId;
                    }
                }

                TLevelSliceSnapshot snapshot(slice, level0SnapshotLimit);

                TLevelSliceSnapshot::TSortedLevelsIter levelIt(&snapshot);
                levelIt.SeekToLast();
                for (ui32 level = numSortedLevels; level != 0; --level) {
                    UNIT_ASSERT(levelIt.Valid());
                    UNIT_ASSERT_VALUES_EQUAL(levelIt.Get().Level, level);
                    levelIt.Prev();
                }
                UNIT_ASSERT(!levelIt.Valid());

                AssertEqual(ReadSnapshot(snapshot, false), expected, round);

                std::reverse(expected.begin(), expected.end());
                AssertEqual(ReadSnapshot(snapshot, true), expected, round);
            }
        }
    }

} // NKikimr
