#include "blobstorage_synclogdsk.h"
#include "blobstorage_synclogdata.h"
#include <library/cpp/testing/unittest/registar.h>


namespace NKikimr {

    using namespace NSyncLog;

    Y_UNIT_TEST_SUITE(TBlobStorageSyncLogDsk) {

        struct TFillIn1Context {
            ui64 Lsn;
            ui32 Gen;
        };

        TSyncLogPagePtr AppendToSyncLogPage(TSyncLogPagePtr page, ui32 num, ui32 pageSize, TFillIn1Context &ctx) {
            char buf[NSyncLog::MaxRecFullSize];
            ui32 size = 0;
            ui64 tabletId = 1;
            for (unsigned i = 0; i < num; i++) {
                size = NSyncLog::TSerializeRoutines::SetBlock(buf, ctx.Lsn, tabletId, ctx.Gen, 0);
                ctx.Lsn += 2;
                ctx.Gen++;
                page->Put(pageSize, (const NSyncLog::TRecordHdr *)buf, size);
            }

            return page;
        }

        TSyncLogPagePtr CreateSyncLogPage(ui32 num, ui32 pageSize, TFillIn1Context &ctx) {
            // create a page
            TMemoryConsumer memBytes(new NMonitoring::TCounterForPtr(false));
            TSyncLogPageDeleter d(std::move(memBytes), pageSize);
            TSyncLogPagePtr page(TSyncLogPage::Create(d));
            return AppendToSyncLogPage(page, num, pageSize, ctx);
        }


        TMemRecLogSnapshotPtr CreateSwapSnapForTest(ui32 pageSize, ui32 pagesNum, TFillIn1Context &ctx) {
            TMemRecLog memLog(pageSize);

            char buf[NSyncLog::MaxRecFullSize];
            ui64 tabletId = 1;
            while (memLog.GetNumberOfPages() < pagesNum) {
                ui32 size = NSyncLog::TSerializeRoutines::SetBlock(buf, ctx.Lsn, tabletId, ctx.Gen, 0);
                ctx.Lsn++;
                ctx.Gen++;
                memLog.PutOne((const TRecordHdr *)buf, size);
            }

            return memLog.GetSnapshot();
        }

        ////////////////////////////////////////////////////////////////////////////
        // TUpdChecker
        // This class is used for updating TDiskRecLog (i.e. Index)
        // together with checking that TDiskRecLogSnapshot::Serialize
        // works correctly
        ////////////////////////////////////////////////////////////////////////////
        class TUpdChecker {
        public:
            TUpdChecker(ui32 chunkSize, ui32 pageSize, ui32 indexBulk)
                : ChunkSize(chunkSize)
                , PageSize(pageSize)
                , IndexBulk(indexBulk)
            {}

            // Apply delta to Index (i.e. dsk) and check
            // that 'dsk->UpdateIndex' == 'dsk->GetSnapshot()->Serialize(delta) + Load'
            void UpdateIndexWithCheck(TDiskRecLog *dsk, const TDeltaToDiskRecLog &delta) {
                // build separate index via snapshot
                std::unique_ptr<TDiskRecLog> uDsk = BuildUpdatedIndex(dsk, delta);
                // update dsk in 'traditional way'
                dsk->UpdateIndex(delta);
                // check that both ways give the same result
                UNIT_ASSERT(dsk->Equal(*uDsk));
            }

        private:
            ui32 ChunkSize;
            ui32 PageSize;
            ui32 IndexBulk;

            // Build new TDiskRecLog via snapshot serialization and loading
            std::unique_ptr<TDiskRecLog> BuildUpdatedIndex(TDiskRecLog *dsk,
                                                   const TDeltaToDiskRecLog &delta) {
                // get snapshot and serialize it with delta
                auto snap = dsk->GetSnapshot();
                TStringStream s;
                snap->Serialize(s, delta);
                const TString serialized = s.Str();
                std::unique_ptr<TDiskRecLog> uDsk = std::make_unique<TDiskRecLog>(ChunkSize,
                                                                    PageSize,
                                                                    IndexBulk,
                                                                    serialized.data(),
                                                                    serialized.data() + serialized.size());
                return uDsk;
            }
        };

        ////////////////////////////////////////////////////////////////////////////
        // TESTS BEGIN HERE
        ////////////////////////////////////////////////////////////////////////////
        Y_UNIT_TEST(AddByOne) {
            ui32 chunkSize = 256u << 10u;
            ui32 pageSize = 16u << 10u;
            ui32 indexBulk = 4;
            TUpdChecker uc(chunkSize, pageSize, indexBulk);
            std::unique_ptr<TDiskRecLog> dsk(new TDiskRecLog(chunkSize, pageSize, indexBulk, nullptr, nullptr));

            TFillIn1Context ctx {1, 1};
            for (int i = 0; i < 5; i++) {
                TSyncLogPagePtr page = CreateSyncLogPage(10, pageSize, ctx);
                TVector<TSyncLogPageSnap> pages;
                pages.push_back(page);

                TStringStream s;
                dsk->Serialize(s);
                TString serialized = s.Str();
                dsk.reset(new TDiskRecLog(chunkSize, pageSize, indexBulk, serialized.data(), serialized.data() + serialized.size()));

                // update
                TDeltaToDiskRecLog delta(indexBulk);
                delta.Append(0, pages);
                uc.UpdateIndexWithCheck(dsk.get(), delta);
            }

            TString result = "{0 {{FirstLsn# 1 OffsInPages# 0 PagesNum# 1} {FirstLsn# 21 OffsInPages# 1 PagesNum# 1} "
                            "{FirstLsn# 41 OffsInPages# 2 PagesNum# 1} {FirstLsn# 61 OffsInPages# 3 PagesNum# 1} "
                            "{FirstLsn# 81 OffsInPages# 4 PagesNum# 1} LastRealLsn# 99}}";
            UNIT_ASSERT(dsk->ToString() == result);
            UNIT_ASSERT(dsk->LastChunkIdx() == 0);
            UNIT_ASSERT(dsk->LastChunkFreePagesNum() == 11);

            // check HowManyChunksAdds
            // in fact we have this number of free pages in last chunk
            UNIT_ASSERT(dsk->LastChunkFreePagesNum() == 11);
            // check 'add to current chunk'
            UNIT_ASSERT(dsk->HowManyChunksAdds(CreateSwapSnapForTest(pageSize, 5, ctx)) == 0);
            // check 'add additional chunk 1'
            UNIT_ASSERT(dsk->HowManyChunksAdds(CreateSwapSnapForTest(pageSize, 17, ctx)) == 1);
            // check 'add additional chunk 2'
            UNIT_ASSERT(dsk->HowManyChunksAdds(CreateSwapSnapForTest(pageSize, 27, ctx)) == 1);
            // check 'add additional chunk 3'
            UNIT_ASSERT(dsk->HowManyChunksAdds(CreateSwapSnapForTest(pageSize, 28, ctx)) == 2);
        }

        Y_UNIT_TEST(AddFive) {
            ui32 chunkSize = 256u << 10u;
            ui32 pageSize = 16u << 10u;
            ui32 indexBulk = 4;
            TUpdChecker uc(chunkSize, pageSize, indexBulk);
            std::unique_ptr<TDiskRecLog> dsk(new TDiskRecLog(chunkSize, pageSize, indexBulk, nullptr, nullptr));

            TFillIn1Context ctx {1, 1};
            // add first portion
            {
                TVector<TSyncLogPageSnap> pages;
                for (int i = 0; i < 5; i++) {
                    TSyncLogPagePtr page = CreateSyncLogPage(10, pageSize, ctx);
                    pages.push_back(page);
                }
                // update
                TDeltaToDiskRecLog delta(indexBulk);
                delta.Append(0, pages);
                uc.UpdateIndexWithCheck(dsk.get(), delta);

                TStringStream s;
                dsk->Serialize(s);
                TString serialized = s.Str();
                dsk.reset(new TDiskRecLog(chunkSize, pageSize, indexBulk, serialized.data(), serialized.data() + serialized.size()));
            }

            TString result = "{0 {{FirstLsn# 1 OffsInPages# 0 PagesNum# 4} {FirstLsn# 81 OffsInPages# 4 PagesNum# 1} "
                            "LastRealLsn# 99}}";
            UNIT_ASSERT(dsk->ToString() == result);

            // add second portion
            {
                TVector<TSyncLogPageSnap> pages;
                for (int i = 0; i < 5; i++) {
                    TSyncLogPagePtr page = CreateSyncLogPage(10, pageSize, ctx);
                    pages.push_back(page);
                }

                // update
                TDeltaToDiskRecLog delta(indexBulk);
                delta.Append(0, pages);
                uc.UpdateIndexWithCheck(dsk.get(), delta);

                TStringStream s;
                dsk->Serialize(s);
                TString serialized = s.Str();
                dsk.reset(new TDiskRecLog(chunkSize, pageSize, indexBulk, serialized.data(), serialized.data() + serialized.size()));
            }

            result = "{0 {{FirstLsn# 1 OffsInPages# 0 PagesNum# 4} {FirstLsn# 81 OffsInPages# 4 PagesNum# 1} "
                     "{FirstLsn# 101 OffsInPages# 5 PagesNum# 4} {FirstLsn# 181 OffsInPages# 9 PagesNum# 1} "
                     "LastRealLsn# 199}}";
            UNIT_ASSERT(dsk->ToString() == result);
            UNIT_ASSERT(dsk->LastChunkIdx() == 0);
            UNIT_ASSERT(dsk->LastChunkFreePagesNum() == 6);
        }

        Y_UNIT_TEST(SeveralChunks) {
            ui32 chunkSize = 64u << 10u;
            ui32 pageSize = 16u << 10u;
            ui32 pagesInChunk = chunkSize / pageSize;
            ui32 indexBulk = 4;
            TUpdChecker uc(chunkSize, pageSize, indexBulk);
            std::unique_ptr<TDiskRecLog> dsk(new TDiskRecLog(chunkSize, pageSize, indexBulk, nullptr, nullptr));

            TFillIn1Context ctx {1, 1};
            for (int k = 0; k < 4; k++) {
                TVector<TSyncLogPageSnap> pages;
                for (int i = 0; i < 2; i++) {
                    TSyncLogPagePtr page = CreateSyncLogPage(10, pageSize, ctx);
                    pages.push_back(page);
                }

                // update
                TDeltaToDiskRecLog delta(indexBulk);
                delta.Append(k * 2 / pagesInChunk, pages);
                uc.UpdateIndexWithCheck(dsk.get(), delta);

                TStringStream s;
                dsk->Serialize(s);
                TString serialized = s.Str();
                dsk.reset(new TDiskRecLog(chunkSize, pageSize, indexBulk, serialized.data(), serialized.data() + serialized.size()));
            }
            TString result = "{0 {{FirstLsn# 1 OffsInPages# 0 PagesNum# 2} {FirstLsn# 41 OffsInPages# 2 PagesNum# 2} "
                            "LastRealLsn# 79}} {1 {{FirstLsn# 81 OffsInPages# 0 PagesNum# 2} "
                            "{FirstLsn# 121 OffsInPages# 2 PagesNum# 2} LastRealLsn# 159}}";
            UNIT_ASSERT(dsk->ToString() == result);
            UNIT_ASSERT(dsk->LastChunkIdx() == 1);
            UNIT_ASSERT(dsk->LastChunkFreePagesNum() == 0);
        }

        Y_UNIT_TEST(OverlappingPages_OnePageIndexed) {
            ui32 chunkSize = 256u << 10u;
            ui32 pageSize = 16u << 10u;
            ui32 indexBulk = 4;
            TUpdChecker uc(chunkSize, pageSize, indexBulk);
            std::unique_ptr<TDiskRecLog> dsk(new TDiskRecLog(chunkSize, pageSize, indexBulk, nullptr, nullptr));

            TFillIn1Context ctx {1, 1};
            for (int i = 0; i < 2; i++) {
                TSyncLogPagePtr page = CreateSyncLogPage(10, pageSize, ctx);
                TVector<TSyncLogPageSnap> pages;
                pages.push_back(page);

                // update
                TDeltaToDiskRecLog delta(indexBulk);
                delta.Append(0, pages);
                uc.UpdateIndexWithCheck(dsk.get(), delta);

                TStringStream s;
                dsk->Serialize(s);
                TString serialized = s.Str();
                dsk.reset(new TDiskRecLog(chunkSize, pageSize, indexBulk, serialized.data(), serialized.data() + serialized.size()));
            }

            // create a page
            TSyncLogPagePtr page = CreateSyncLogPage(10, pageSize, ctx);
            {
                TVector<TSyncLogPageSnap> pages;
                pages.push_back(page);

                // update
                TDeltaToDiskRecLog delta(indexBulk);
                delta.Append(0, pages);
                uc.UpdateIndexWithCheck(dsk.get(), delta);

                TStringStream s;
                dsk->Serialize(s);
                TString serialized = s.Str();
                dsk.reset(new TDiskRecLog(chunkSize, pageSize, indexBulk, serialized.data(), serialized.data() + serialized.size()));

            }

            // append to the page
            page = AppendToSyncLogPage(page, 4, pageSize, ctx);
            {
                TVector<TSyncLogPageSnap> pages;
                pages.push_back(page);

                // update
                TDeltaToDiskRecLog delta(indexBulk);
                delta.Append(0, pages);
                uc.UpdateIndexWithCheck(dsk.get(), delta);

                TStringStream s;
                dsk->Serialize(s);
                TString serialized = s.Str();
                dsk.reset(new TDiskRecLog(chunkSize, pageSize, indexBulk, serialized.data(), serialized.data() + serialized.size()));
            }

            TString result = "{0 {{FirstLsn# 1 OffsInPages# 0 PagesNum# 1} {FirstLsn# 21 OffsInPages# 1 PagesNum# 1} "
                            "{FirstLsn# 41 OffsInPages# 3 PagesNum# 1} LastRealLsn# 67}}";
            UNIT_ASSERT(dsk->ToString() == result);
            UNIT_ASSERT(dsk->LastChunkIdx() == 0);
            UNIT_ASSERT(dsk->LastChunkFreePagesNum() == 12);
        }

        Y_UNIT_TEST(OverlappingPages_SeveralPagesIndexed) {
            ui32 chunkSize = 256u << 10u;
            ui32 pageSize = 16u << 10u;
            ui32 indexBulk = 4;
            TUpdChecker uc(chunkSize, pageSize, indexBulk);
            std::unique_ptr<TDiskRecLog> dsk(new TDiskRecLog(chunkSize, pageSize, indexBulk, nullptr, nullptr));

            TFillIn1Context ctx {1, 1};
            for (int i = 0; i < 2; i++) {
                TSyncLogPagePtr page = CreateSyncLogPage(10, pageSize, ctx);
                TVector<TSyncLogPageSnap> pages;
                pages.push_back(page);

                // update
                TDeltaToDiskRecLog delta(indexBulk);
                delta.Append(0, pages);
                uc.UpdateIndexWithCheck(dsk.get(), delta);

                TStringStream s;
                dsk->Serialize(s);
                TString serialized = s.Str();
                dsk.reset(new TDiskRecLog(chunkSize, pageSize, indexBulk, serialized.data(), serialized.data() + serialized.size()));
            }

            // create a page
            TSyncLogPagePtr page0 = CreateSyncLogPage(10, pageSize, ctx);
            TSyncLogPagePtr page = CreateSyncLogPage(10, pageSize, ctx);
            {
                TVector<TSyncLogPageSnap> pages;
                pages.push_back(page0);
                pages.push_back(page);

                // update
                TDeltaToDiskRecLog delta(indexBulk);
                delta.Append(0, pages);
                uc.UpdateIndexWithCheck(dsk.get(), delta);

                TStringStream s;
                dsk->Serialize(s);
                TString serialized = s.Str();
                dsk.reset(new TDiskRecLog(chunkSize, pageSize, indexBulk, serialized.data(), serialized.data() + serialized.size()));
            }

            // append to the page
            page = AppendToSyncLogPage(page, 4, pageSize, ctx);
            {
                TVector<TSyncLogPageSnap> pages;
                pages.push_back(page);

                // update
                TDeltaToDiskRecLog delta(indexBulk);
                delta.Append(0, pages);
                uc.UpdateIndexWithCheck(dsk.get(), delta);

                TStringStream s;
                dsk->Serialize(s);
                TString serialized = s.Str();
                dsk.reset(new TDiskRecLog(chunkSize, pageSize, indexBulk, serialized.data(), serialized.data() + serialized.size()));
            }

            TString result = "{0 {{FirstLsn# 1 OffsInPages# 0 PagesNum# 1} {FirstLsn# 21 OffsInPages# 1 PagesNum# 1} "
                            "{FirstLsn# 41 OffsInPages# 2 PagesNum# 1} {FirstLsn# 61 OffsInPages# 4 PagesNum# 1} "
                            "LastRealLsn# 87}}";
            UNIT_ASSERT(dsk->ToString() == result);
            UNIT_ASSERT(dsk->LastChunkIdx() == 0);
            UNIT_ASSERT(dsk->LastChunkFreePagesNum() == 11);
        }

        Y_UNIT_TEST(ComplicatedSerializeWithOverlapping) {
            ui32 chunkSize = 256u << 10u;
            ui32 pageSize = 16u << 10u;
            ui32 indexBulk = 4;
            TUpdChecker uc(chunkSize, pageSize, indexBulk);
            std::unique_ptr<TDiskRecLog> dsk(new TDiskRecLog(chunkSize, pageSize, indexBulk, nullptr, nullptr));

            TFillIn1Context ctx {1, 1};

            TSyncLogPagePtr lastPage;
            {
                // CASE: start from empty dsk
                TVector<TSyncLogPageSnap> pages;
                for (int i = 0; i < 5; ++i) {
                    lastPage = CreateSyncLogPage(10, pageSize, ctx);
                    pages.push_back(lastPage);
                }

                // update
                TDeltaToDiskRecLog delta(indexBulk);
                delta.Append(0, pages);
                uc.UpdateIndexWithCheck(dsk.get(), delta);
            }

            {
                // CASE:  update the last page in index same page
                AppendToSyncLogPage(lastPage, 5, pageSize, ctx);

                // prepare delta
                TDeltaToDiskRecLog delta(indexBulk);
                TVector<TSyncLogPageSnap> pages;
                pages.push_back(lastPage);
                delta.Append(0, pages);
                // one more page
                TSyncLogPagePtr anotherPage = CreateSyncLogPage(10, pageSize, ctx);
                pages.clear();
                pages.push_back(anotherPage);
                delta.Append(1, pages);

                // update: update page from chunk 0 and add page from chunk 1
                uc.UpdateIndexWithCheck(dsk.get(), delta);
            }
        }

        Y_UNIT_TEST(TrimLog) {
            ui32 chunkSize = 64u << 10u;
            ui32 pageSize = 16u << 10u;
            ui32 pagesInChunk = chunkSize / pageSize;
            ui32 indexBulk = 4;
            TUpdChecker uc(chunkSize, pageSize, indexBulk);
            std::unique_ptr<TDiskRecLog> dsk(new TDiskRecLog(chunkSize, pageSize, indexBulk, nullptr, nullptr));

            TFillIn1Context ctx {1, 1};
            for (int k = 0; k < 8; k++) {
                TVector<TSyncLogPageSnap> pages;
                for (int i = 0; i < 2; i++) {
                    TSyncLogPagePtr page = CreateSyncLogPage(10, pageSize, ctx);
                    pages.push_back(page);
                }
                // update
                TDeltaToDiskRecLog delta(indexBulk);
                delta.Append(k * 2 / pagesInChunk, pages);
                uc.UpdateIndexWithCheck(dsk.get(), delta);

                TStringStream s;
                dsk->Serialize(s);
                TString serialized = s.Str();
                dsk.reset(new TDiskRecLog(chunkSize, pageSize, indexBulk, serialized.data(), serialized.data() + serialized.size()));
            }

            TVector<ui32> chunks;
            ui32 num = 0;

            chunks.clear();
            num = dsk->TrimLog(87, nullptr, chunks);
            UNIT_ASSERT(num == 1);
            UNIT_ASSERT(dsk->ToString() == "{1 {{FirstLsn# 81 OffsInPages# 0 PagesNum# 2} "
                        "{FirstLsn# 121 OffsInPages# 2 PagesNum# 2} LastRealLsn# 159}} "
                        "{2 {{FirstLsn# 161 OffsInPages# 0 PagesNum# 2} {FirstLsn# 201 OffsInPages# 2 PagesNum# 2} "
                        "LastRealLsn# 239}} {3 {{FirstLsn# 241 OffsInPages# 0 PagesNum# 2} "
                        "{FirstLsn# 281 OffsInPages# 2 PagesNum# 2} LastRealLsn# 319}}");

            chunks.clear();
            num = dsk->TrimLog(319, nullptr, chunks);
            UNIT_ASSERT(num == 3);
            UNIT_ASSERT(dsk->ToString() == "Empty");
        }

        Y_UNIT_TEST(DeleteChunks) {
            ui32 chunkSize = 64u << 10u;
            ui32 pageSize = 16u << 10u;
            ui32 pagesInChunk = chunkSize / pageSize;
            ui32 indexBulk = 4;
            TUpdChecker uc(chunkSize, pageSize, indexBulk);
            std::unique_ptr<TDiskRecLog> dsk(new TDiskRecLog(chunkSize, pageSize, indexBulk, nullptr, nullptr));

            TFillIn1Context ctx {1, 1};
            for (int k = 0; k < 8; k++) {
                TVector<TSyncLogPageSnap> pages;
                for (int i = 0; i < 2; i++) {
                    TSyncLogPagePtr page = CreateSyncLogPage(10, pageSize, ctx);
                    pages.push_back(page);
                }
                // update
                TDeltaToDiskRecLog delta(indexBulk);
                delta.Append(k * 2 / pagesInChunk, pages);
                uc.UpdateIndexWithCheck(dsk.get(), delta);

                TStringStream s;
                dsk->Serialize(s);
                TString serialized = s.Str();
                dsk.reset(new TDiskRecLog(chunkSize, pageSize, indexBulk, serialized.data(), serialized.data() + serialized.size()));
            }

            TVector<ui32> chunks;
            ui64 lsn = 0;

            chunks.clear();
            lsn = dsk->DeleteChunks(1, nullptr, chunks);
            UNIT_ASSERT(lsn == 80);
            UNIT_ASSERT(dsk->ToString() ==
                        "{1 {{FirstLsn# 81 OffsInPages# 0 PagesNum# 2} {FirstLsn# 121 OffsInPages# 2 PagesNum# 2} "
                            "LastRealLsn# 159}} "
                        "{2 {{FirstLsn# 161 OffsInPages# 0 PagesNum# 2} {FirstLsn# 201 OffsInPages# 2 PagesNum# 2} "
                            "LastRealLsn# 239}} "
                        "{3 {{FirstLsn# 241 OffsInPages# 0 PagesNum# 2} {FirstLsn# 281 OffsInPages# 2 PagesNum# 2} "
                            "LastRealLsn# 319}}");

            chunks.clear();
            lsn = dsk->DeleteChunks(2, nullptr, chunks);
            UNIT_ASSERT(lsn == 240);
            UNIT_ASSERT(dsk->ToString() == "{3 {{FirstLsn# 241 OffsInPages# 0 PagesNum# 2} "
                            "{FirstLsn# 281 OffsInPages# 2 PagesNum# 2} LastRealLsn# 319}}");
        }

    }

} // NKikimr
