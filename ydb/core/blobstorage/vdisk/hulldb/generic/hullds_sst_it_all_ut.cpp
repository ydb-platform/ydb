#include "hullds_sst_it_all_ut.h"
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_block.h>

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

    Y_UNIT_TEST_SUITE(TBlobStorageHullSstHeapStripe) {
        Y_UNIT_TEST(StripeIsDerivedFromChunkOwnership) {
            TTestContexts ctxs;
            using TSst = TLevelSegment<TKeyBlock, TMemRecBlock>;
            TIntrusivePtr<TSst> seg(new TSst(ctxs.GetVCtx()));
            seg->LastPartAddr = TDiskPart(7, 4064, 80);
            seg->HeapStripe = seg->LastPartAddr;
            seg->AllChunks = {7};

            // nothing about the stripe is written down; the SST address is the whole record
            NKikimrVDiskData::TDiskPart pb;
            seg->SerializeToProto(pb);
            UNIT_ASSERT_VALUES_EQUAL(pb.GetChunkIdx(), 7u);
            UNIT_ASSERT_VALUES_EQUAL(pb.GetOffset(), 4064u);
            UNIT_ASSERT_VALUES_EQUAL(pb.GetSize(), 80u);

            TSst loaded(ctxs.GetVCtx(), pb);
            UNIT_ASSERT(loaded.HeapStripe.Empty());

            // a chunk owned by the slot heap leaves the SST unstriped
            loaded.ResolveHeapStripe(THashSet<TChunkIdx>{9});
            UNIT_ASSERT(loaded.HeapStripe.Empty());

            loaded.ResolveHeapStripe(THashSet<TChunkIdx>{7});
            UNIT_ASSERT_VALUES_EQUAL(loaded.HeapStripe.ChunkIdx, 7u);
            UNIT_ASSERT_VALUES_EQUAL(loaded.HeapStripe.Offset, 4064u);
            UNIT_ASSERT_VALUES_EQUAL(loaded.HeapStripe.Size, 80u);

            TVector<ui32> ids;
            loaded.FillInChunkIds(ids);
            UNIT_ASSERT(ids.empty());

            loaded.AllChunks = {7};
            TSet<TChunkIdx> chunks;
            loaded.GetOwnedChunks(chunks);
            UNIT_ASSERT(chunks.contains(7));

            // a second stripe SST in the same chunk is allowed to claim it again
            TSst other(ctxs.GetVCtx());
            other.AllChunks = {7};
            other.HeapStripe = loaded.HeapStripe;
            other.GetOwnedChunks(chunks);
            UNIT_ASSERT_VALUES_EQUAL(chunks.size(), 1u);
        }
    }

} // NKikimr
