#include "hullds_sstvec_it.h"
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_ut.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_logoblob.h>
#include <library/cpp/testing/unittest/registar.h>


namespace NKikimr {

    namespace NBlobStorageHullSstItHelpers {
        using TLogoBlobSst = TLevelSegment<TKeyLogoBlob, TMemRecLogoBlob>;
        using TLogoBlobSstPtr = TIntrusivePtr<TLogoBlobSst>;
        using TLogoBlobOrderedSsts = TOrderedLevelSegments<TKeyLogoBlob, TMemRecLogoBlob>;
        using TLogoBlobOrderedSstsPtr = TIntrusivePtr<TLogoBlobOrderedSsts>;
        using TSegments = TVector<TLogoBlobSstPtr>;

        static const ui32 ChunkSize = 8u << 20u;
        static const ui32 CompWorthReadSize = 2u << 20u;

        TLogoBlobSstPtr GenerateSst(ui32 step, ui32 recs, ui32 plus, ui64 tabletId = 0, ui32 generation = 0,
                                    ui32 channel = 0, ui32 cookie = 0) {
            using TRec = TLogoBlobSst::TRec;
            Y_UNUSED(step);
            TLogoBlobSstPtr ptr(new TLogoBlobSst(TTestContexts().GetVCtx()));
            for (ui32 i = 0; i < recs; i++) {
                TLogoBlobID id(tabletId, generation, step + i * plus, channel, 0, cookie);
                TRec rec {TKeyLogoBlob(id), TMemRecLogoBlob()};
                ptr->LoadedIndex.push_back(rec);
            }
            return ptr;
        }

        TLogoBlobOrderedSstsPtr GenerateOrderedSsts(ui32 step, ui32 recs, ui32 plus, ui32 ssts, ui64 tabletId = 0,
                                                    ui32 generation = 0, ui32 channel = 0, ui32 cookie = 0) {
            TSegments vec;
            for (ui32 i = 0; i < ssts; i++) {
                vec.push_back(GenerateSst(step, recs, plus, tabletId, generation, channel, cookie));
                step += recs * plus;
            }

            return TLogoBlobOrderedSstsPtr(new TLogoBlobOrderedSsts(vec.begin(), vec.end()));

        }

    } // NBlobStorageHullSstItHelpers

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

} // NKikimr
