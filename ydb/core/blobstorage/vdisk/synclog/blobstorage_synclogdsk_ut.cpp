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

        struct TIndexRecordLocation {
            ui32 ChunkIdx;
            TDiskIndexRecord Record;
        };

        TVector<TIndexRecordLocation> GetIndexRecords(const TDiskRecLog& dsk) {
            TDiskRecLogSnapshotConstPtr snap = dsk.GetSnapshot();
            TDiskRecLogSnapshot::TIndexRecIterator it(snap);
            TVector<TIndexRecordLocation> result;
            for (it.Seek(dsk.GetFirstLsn()); it.Valid(); it.Next()) {
                const auto [chunkIdx, record] = it.Get();
                result.push_back({chunkIdx, *record});
            }
            return result;
        }

        void AssertIndexRecord(const TIndexRecordLocation& actual, ui32 chunkIdx, ui64 firstLsn,
                ui32 offsetInPages, ui32 pagesNum) {
            UNIT_ASSERT_VALUES_EQUAL(actual.ChunkIdx, chunkIdx);
            UNIT_ASSERT_VALUES_EQUAL(actual.Record.FirstLsn, firstLsn);
            UNIT_ASSERT_VALUES_EQUAL(actual.Record.OffsetInPages, offsetInPages);
            UNIT_ASSERT_VALUES_EQUAL(actual.Record.PagesNum, pagesNum);
        }

        void AppendLsnsToSyncLogPage(TSyncLogPagePtr page, const TVector<ui64>& lsns, ui32 pageSize, ui32& gen) {
            char buf[NSyncLog::MaxRecFullSize];
            for (const ui64 lsn : lsns) {
                const ui32 size = NSyncLog::TSerializeRoutines::SetBlock(buf, lsn, 1, gen++, 0);
                page->Put(pageSize, reinterpret_cast<const NSyncLog::TRecordHdr*>(buf), size);
            }
        }

        TSyncLogPagePtr CreateSyncLogPageWithLsns(const TVector<ui64>& lsns, ui32 pageSize, ui32& gen) {
            TMemoryConsumer memBytes(new NMonitoring::TCounterForPtr(false));
            TSyncLogPageDeleter deleter(std::move(memBytes), pageSize);
            TSyncLogPagePtr page(TSyncLogPage::Create(deleter));
            AppendLsnsToSyncLogPage(page, lsns, pageSize, gen);
            return page;
        }

        TVector<ui64> ReadIndexedLsnsFromChunks(const TDiskRecLog& dsk,
                const TVector<TVector<TSyncLogPageSnap>>& chunkPages, ui64 confirmedLsn) {
            TDiskRecLogSnapshotConstPtr snap = dsk.GetSnapshot();
            TDiskRecLogSnapshot::TIndexRecIterator indexIt(snap);
            indexIt.Seek(confirmedLsn + 1);

            TVector<ui64> result;
            ui64 lastLsn = confirmedLsn;
            for (; indexIt.Valid(); indexIt.Next()) {
                const auto [chunkIdx, record] = indexIt.Get();
                UNIT_ASSERT_C(chunkIdx < chunkPages.size(), "unexpected chunkIdx# " << chunkIdx);
                const TVector<TSyncLogPageSnap>& physicalPages = chunkPages[chunkIdx];
                UNIT_ASSERT_C(record->OffsetInPages + record->PagesNum <= physicalPages.size(),
                    "Index record points outside the physical page sequence");

                for (ui32 i = 0; i < record->PagesNum; ++i) {
                    const TSyncLogPageSnap& page = physicalPages[record->OffsetInPages + i];
                    for (const TRecordHdr* hdr = page.Begin(); hdr != page.End(); hdr = page.Next(hdr)) {
                        if (hdr->Lsn > lastLsn) {
                            result.push_back(hdr->Lsn);
                            lastLsn = hdr->Lsn;
                        }
                    }
                }
            }
            return result;
        }

        TVector<ui64> ReadIndexedLsns(const TDiskRecLog& dsk,
                const TVector<TSyncLogPageSnap>& physicalPages, ui64 confirmedLsn) {
            return ReadIndexedLsnsFromChunks(dsk, {physicalPages}, confirmedLsn);
        }

        // Appends six growing versions of the same mutable page; every append drops the
        // superseded copy, leaving a single index record {FirstLsn=100, offset 5, 1 page}
        // with lsns 100, 105, 120, 125, 130, 135, 140, 145
        TVector<TSyncLogPageSnap> BuildMutablePageVersions(TDiskRecLog& dsk, TUpdChecker& checker,
                ui32 pageSize, ui32 indexBulk, ui32& gen) {
            TSyncLogPagePtr page = CreateSyncLogPageWithLsns({100, 105, 120}, pageSize, gen);
            TVector<TSyncLogPageSnap> physicalPages;
            for (const ui64 lsn : TVector<ui64>{0, 125, 130, 135, 140, 145}) {
                if (lsn) {
                    AppendLsnsToSyncLogPage(page, {lsn}, pageSize, gen);
                }
                physicalPages.push_back(page);
                TDeltaToDiskRecLog delta(indexBulk);
                delta.Append(0, TVector<TSyncLogPageSnap>{page});
                checker.UpdateIndexWithCheck(&dsk, delta);
            }
            return physicalPages;
        }

        ////////////////////////////////////////////////////////////////////////////
        // TESTS BEGIN HERE
        ////////////////////////////////////////////////////////////////////////////
        Y_UNIT_TEST(AdjacentAppendsAreMerged) {
            constexpr ui32 chunkSize = 256u << 10u;
            constexpr ui32 pageSize = 16u << 10u;
            constexpr ui32 indexBulk = 4;
            TUpdChecker checker(chunkSize, pageSize, indexBulk);
            TDiskRecLog dsk(chunkSize, pageSize, indexBulk, nullptr, nullptr);
            TFillIn1Context ctx{1, 1};

            TVector<TSyncLogPageSnap> firstAppend;
            for (ui32 i = 0; i < 2; ++i) {
                firstAppend.push_back(CreateSyncLogPage(1, pageSize, ctx));
            }
            TDeltaToDiskRecLog firstDelta(indexBulk);
            firstDelta.Append(0, firstAppend);
            checker.UpdateIndexWithCheck(&dsk, firstDelta);

            auto records = GetIndexRecords(dsk);
            UNIT_ASSERT_VALUES_EQUAL(records.size(), 1u);
            AssertIndexRecord(records[0], 0, firstAppend[0].GetFirstLsn(), 0, 2);

            TVector<TSyncLogPageSnap> secondAppend{CreateSyncLogPage(1, pageSize, ctx)};
            TDeltaToDiskRecLog secondDelta(indexBulk);
            secondDelta.Append(0, secondAppend);
            checker.UpdateIndexWithCheck(&dsk, secondDelta);

            records = GetIndexRecords(dsk);
            UNIT_ASSERT_VALUES_EQUAL(records.size(), 1u);
            AssertIndexRecord(records[0], 0, firstAppend[0].GetFirstLsn(), 0, 3);

            TVector<TSyncLogPageSnap> thirdAppend;
            for (ui32 i = 0; i < 3; ++i) {
                thirdAppend.push_back(CreateSyncLogPage(1, pageSize, ctx));
            }
            TDeltaToDiskRecLog thirdDelta(indexBulk);
            thirdDelta.Append(0, thirdAppend);
            checker.UpdateIndexWithCheck(&dsk, thirdDelta);

            records = GetIndexRecords(dsk);
            UNIT_ASSERT_VALUES_EQUAL(records.size(), 2u);
            AssertIndexRecord(records[0], 0, firstAppend[0].GetFirstLsn(), 0, 4);
            AssertIndexRecord(records[1], 0, thirdAppend[1].GetFirstLsn(), 4, 2);
            UNIT_ASSERT_VALUES_EQUAL(dsk.GetLastLsn(), thirdAppend.back().GetLastLsn());
        }

        Y_UNIT_TEST(LargeAppendAfterPartialIndexRecord) {
            constexpr ui32 chunkSize = 256u << 10u;
            constexpr ui32 pageSize = 16u << 10u;
            constexpr ui32 indexBulk = 4;
            TUpdChecker checker(chunkSize, pageSize, indexBulk);
            TDiskRecLog dsk(chunkSize, pageSize, indexBulk, nullptr, nullptr);
            TFillIn1Context ctx{1, 1};

            TVector<TSyncLogPageSnap> initialPages;
            for (ui32 i = 0; i < 3; ++i) {
                initialPages.push_back(CreateSyncLogPage(1, pageSize, ctx));
            }
            TDeltaToDiskRecLog initialDelta(indexBulk);
            initialDelta.Append(0, initialPages);
            checker.UpdateIndexWithCheck(&dsk, initialDelta);

            TVector<TSyncLogPageSnap> appendedPages;
            for (ui32 i = 0; i < 10; ++i) {
                appendedPages.push_back(CreateSyncLogPage(1, pageSize, ctx));
            }
            TDeltaToDiskRecLog appendDelta(indexBulk);
            appendDelta.Append(0, appendedPages);
            checker.UpdateIndexWithCheck(&dsk, appendDelta);

            const auto records = GetIndexRecords(dsk);
            UNIT_ASSERT_VALUES_EQUAL(records.size(), 4u);
            AssertIndexRecord(records[0], 0, initialPages[0].GetFirstLsn(), 0, 4);
            AssertIndexRecord(records[1], 0, appendedPages[1].GetFirstLsn(), 4, 4);
            AssertIndexRecord(records[2], 0, appendedPages[5].GetFirstLsn(), 8, 4);
            AssertIndexRecord(records[3], 0, appendedPages[9].GetFirstLsn(), 12, 1);
            UNIT_ASSERT_VALUES_EQUAL(dsk.GetLastLsn(), appendedPages.back().GetLastLsn());
        }

        Y_UNIT_TEST(MergeDoesNotCrossChunkBoundary) {
            constexpr ui32 chunkSize = 64u << 10u;
            constexpr ui32 pageSize = 16u << 10u;
            constexpr ui32 indexBulk = 4;
            TUpdChecker checker(chunkSize, pageSize, indexBulk);
            TDiskRecLog dsk(chunkSize, pageSize, indexBulk, nullptr, nullptr);
            TFillIn1Context ctx{1, 1};

            TVector<TSyncLogPageSnap> chunk0Pages;
            for (ui32 i = 0; i < 3; ++i) {
                chunk0Pages.push_back(CreateSyncLogPage(1, pageSize, ctx));
            }
            TDeltaToDiskRecLog chunk0Delta(indexBulk);
            chunk0Delta.Append(0, chunk0Pages);
            checker.UpdateIndexWithCheck(&dsk, chunk0Delta);

            TVector<TSyncLogPageSnap> chunk1Pages{CreateSyncLogPage(1, pageSize, ctx)};
            TDeltaToDiskRecLog chunk1Delta(indexBulk);
            chunk1Delta.Append(1, chunk1Pages);
            checker.UpdateIndexWithCheck(&dsk, chunk1Delta);

            const auto records = GetIndexRecords(dsk);
            UNIT_ASSERT_VALUES_EQUAL(records.size(), 2u);
            AssertIndexRecord(records[0], 0, chunk0Pages[0].GetFirstLsn(), 0, 3);
            AssertIndexRecord(records[1], 1, chunk1Pages[0].GetFirstLsn(), 0, 1);
            UNIT_ASSERT_VALUES_EQUAL(dsk.GetSizeInChunks(), 2u);
        }

        Y_UNIT_TEST(OverlappingTailPageIsDroppedFromIndex) {
            constexpr ui32 chunkSize = 256u << 10u;
            constexpr ui32 pageSize = 16u << 10u;
            constexpr ui32 indexBulk = 4;
            TUpdChecker checker(chunkSize, pageSize, indexBulk);
            ui32 gen = 1;

            {
                TDiskRecLog dsk(chunkSize, pageSize, indexBulk, nullptr, nullptr);
                TSyncLogPagePtr page = CreateSyncLogPageWithLsns({100, 105, 120}, pageSize, gen);
                TVector<TSyncLogPageSnap> physicalPages;

                auto appendVersion = [&] {
                    TVector<TSyncLogPageSnap> pages{page};
                    physicalPages.push_back(page);
                    TDeltaToDiskRecLog delta(indexBulk);
                    delta.Append(0, pages);
                    checker.UpdateIndexWithCheck(&dsk, delta);
                };

                appendVersion();
                UNIT_ASSERT_VALUES_EQUAL(dsk.GetLastLsn(), 120u);
                AppendLsnsToSyncLogPage(page, {125}, pageSize, gen);
                appendVersion();
                UNIT_ASSERT_VALUES_EQUAL(dsk.GetLastLsn(), 125u);
                AppendLsnsToSyncLogPage(page, {130}, pageSize, gen);
                appendVersion();
                UNIT_ASSERT_VALUES_EQUAL(dsk.GetLastLsn(), 130u);

                // every rewrite drops the superseded copy; only the latest version stays
                // indexed, at the physical position of the last write
                const auto records = GetIndexRecords(dsk);
                UNIT_ASSERT_VALUES_EQUAL(records.size(), 1u);
                AssertIndexRecord(records[0], 0, 100, 2, 1);
                UNIT_ASSERT_VALUES_EQUAL(ReadIndexedLsns(dsk, physicalPages, 0),
                    (TVector<ui64>{100, 105, 120, 125, 130}));
            }

            {
                TDiskRecLog dsk(chunkSize, pageSize, indexBulk, nullptr, nullptr);
                const TVector<TSyncLogPageSnap> physicalPages =
                    BuildMutablePageVersions(dsk, checker, pageSize, indexBulk, gen);

                const auto records = GetIndexRecords(dsk);
                UNIT_ASSERT_VALUES_EQUAL(records.size(), 1u);
                AssertIndexRecord(records[0], 0, 100, 5, 1);
                UNIT_ASSERT_VALUES_EQUAL(dsk.GetLastLsn(), 145u);
                UNIT_ASSERT_VALUES_EQUAL(ReadIndexedLsns(dsk, physicalPages, 0),
                    (TVector<ui64>{100, 105, 120, 125, 130, 135, 140, 145}));
            }
        }

        Y_UNIT_TEST(SeekIntoMergedIndexRecord) {
            constexpr ui32 chunkSize = 256u << 10u;
            constexpr ui32 pageSize = 16u << 10u;
            constexpr ui32 indexBulk = 4;
            TUpdChecker checker(chunkSize, pageSize, indexBulk);
            TDiskRecLog dsk(chunkSize, pageSize, indexBulk, nullptr, nullptr);
            ui32 gen = 1;
            TVector<TSyncLogPageSnap> physicalPages;

            TSyncLogPagePtr firstPage = CreateSyncLogPageWithLsns({100, 105}, pageSize, gen);
            TSyncLogPagePtr middlePage = CreateSyncLogPageWithLsns({110, 115}, pageSize, gen);
            TVector<TSyncLogPageSnap> firstAppend{firstPage, middlePage};
            physicalPages.insert(physicalPages.end(), firstAppend.begin(), firstAppend.end());
            TDeltaToDiskRecLog firstDelta(indexBulk);
            firstDelta.Append(0, firstAppend);
            checker.UpdateIndexWithCheck(&dsk, firstDelta);

            TSyncLogPagePtr tailPage = CreateSyncLogPageWithLsns({120}, pageSize, gen);
            TVector<TSyncLogPageSnap> tailAppend{tailPage};
            physicalPages.push_back(tailPage);
            TDeltaToDiskRecLog tailDelta(indexBulk);
            tailDelta.Append(0, tailAppend);
            checker.UpdateIndexWithCheck(&dsk, tailDelta);

            AppendLsnsToSyncLogPage(tailPage, {125}, pageSize, gen);
            TVector<TSyncLogPageSnap> updatedTailAppend{tailPage};
            physicalPages.push_back(tailPage);
            TDeltaToDiskRecLog updatedTailDelta(indexBulk);
            updatedTailDelta.Append(0, updatedTailAppend);
            checker.UpdateIndexWithCheck(&dsk, updatedTailDelta);

            // the tail append merged into the first record; the rewrite then dropped it
            // back and started a separate record after the one-page gap
            const auto records = GetIndexRecords(dsk);
            UNIT_ASSERT_VALUES_EQUAL(records.size(), 2u);
            AssertIndexRecord(records[0], 0, 100, 0, 2);
            AssertIndexRecord(records[1], 0, 120, 3, 1);

            UNIT_ASSERT_VALUES_EQUAL(ReadIndexedLsns(dsk, physicalPages, 99),
                (TVector<ui64>{100, 105, 110, 115, 120, 125}));
            UNIT_ASSERT_VALUES_EQUAL(ReadIndexedLsns(dsk, physicalPages, 109),
                (TVector<ui64>{110, 115, 120, 125}));
            UNIT_ASSERT_VALUES_EQUAL(ReadIndexedLsns(dsk, physicalPages, 119),
                (TVector<ui64>{120, 125}));
            UNIT_ASSERT_VALUES_EQUAL(ReadIndexedLsns(dsk, physicalPages, 124),
                (TVector<ui64>{125}));
        }

        Y_UNIT_TEST(OverlappingPageAcrossChunkBoundary) {
            constexpr ui32 chunkSize = 64u << 10u; // 4 pages per chunk
            constexpr ui32 pageSize = 16u << 10u;
            constexpr ui32 indexBulk = 4;
            TUpdChecker checker(chunkSize, pageSize, indexBulk);
            TDiskRecLog dsk(chunkSize, pageSize, indexBulk, nullptr, nullptr);
            ui32 gen = 1;

            // fill chunk 0 completely; its last page stays partially filled
            TVector<TSyncLogPageSnap> chunk0Pages;
            for (const auto& lsns : TVector<TVector<ui64>>{{100, 101}, {102, 103}, {104, 105}}) {
                chunk0Pages.push_back(CreateSyncLogPageWithLsns(lsns, pageSize, gen));
            }
            TSyncLogPagePtr tail = CreateSyncLogPageWithLsns({106}, pageSize, gen);
            chunk0Pages.push_back(tail);
            TDeltaToDiskRecLog delta0(indexBulk);
            delta0.Append(0, chunk0Pages);
            checker.UpdateIndexWithCheck(&dsk, delta0);

            // the tail page grows and is rewritten into a new chunk; the superseded copy
            // in chunk 0 cannot be dropped and stays indexed there
            AppendLsnsToSyncLogPage(tail, {107}, pageSize, gen);
            TDeltaToDiskRecLog delta1(indexBulk);
            delta1.Append(1, TVector<TSyncLogPageSnap>{tail});
            checker.UpdateIndexWithCheck(&dsk, delta1);

            const auto records = GetIndexRecords(dsk);
            UNIT_ASSERT_VALUES_EQUAL(records.size(), 2u);
            AssertIndexRecord(records[0], 0, 100, 0, 4);
            AssertIndexRecord(records[1], 1, 106, 0, 1);

            // lsn 106 is present in both chunks; readers must see every lsn exactly once
            const TVector<TVector<TSyncLogPageSnap>> chunkPages{chunk0Pages, {tail}};
            const TVector<ui64> allLsns{100, 101, 102, 103, 104, 105, 106, 107};
            for (ui64 cut = 99; cut <= 108; ++cut) {
                TVector<ui64> expected;
                for (const ui64 lsn : allLsns) {
                    if (lsn > cut) {
                        expected.push_back(lsn);
                    }
                }
                UNIT_ASSERT_VALUES_EQUAL_C(ReadIndexedLsnsFromChunks(dsk, chunkPages, cut),
                    expected, "cut# " << cut);
            }
        }

        // Incident-like workload: every commit swaps the grown mutable tail page plus one
        // new page, which becomes the next commit's mutable tail. Every commit drops the
        // superseded tail copy and starts a new record after the resulting gap.
        TVector<TSyncLogPageSnap> BuildOverlappingAppendsWorkload(TDiskRecLog& dsk,
                TUpdChecker& checker, ui32 pageSize, ui32 indexBulk, ui32 commits,
                ui32& gen, TVector<ui64>& expectedLsns) {
            ui64 lsn = 100;
            TVector<TSyncLogPageSnap> physicalPages;
            expectedLsns = {lsn};
            TSyncLogPagePtr tail = CreateSyncLogPageWithLsns({lsn}, pageSize, gen);
            for (ui32 commit = 0; commit < commits; ++commit) {
                AppendLsnsToSyncLogPage(tail, {++lsn}, pageSize, gen);
                expectedLsns.push_back(lsn);
                TSyncLogPagePtr next = CreateSyncLogPageWithLsns({++lsn}, pageSize, gen);
                expectedLsns.push_back(lsn);
                TVector<TSyncLogPageSnap> pages{tail, next};
                physicalPages.insert(physicalPages.end(), pages.begin(), pages.end());
                TDeltaToDiskRecLog delta(indexBulk);
                delta.Append(0, pages);
                checker.UpdateIndexWithCheck(&dsk, delta);
                tail = next;
            }
            return physicalPages;
        }

        Y_UNIT_TEST(ResumeReadFromAnyCutPoint) {
            constexpr ui32 chunkSize = 512u << 10u;
            constexpr ui32 pageSize = 16u << 10u;
            constexpr ui32 indexBulk = 4;
            constexpr ui32 commits = 8;
            TUpdChecker checker(chunkSize, pageSize, indexBulk);
            TDiskRecLog dsk(chunkSize, pageSize, indexBulk, nullptr, nullptr);
            ui32 gen = 1;
            TVector<ui64> expectedLsns;
            const TVector<TSyncLogPageSnap> physicalPages = BuildOverlappingAppendsWorkload(
                dsk, checker, pageSize, indexBulk, commits, gen, expectedLsns);

            // a sync response may be cut at any point; the next request seeks cut + 1
            // and must return exactly the remaining lsns, without duplicates or gaps
            for (ui64 cut = 99; cut <= expectedLsns.back() + 1; ++cut) {
                TVector<ui64> expected;
                for (const ui64 lsn : expectedLsns) {
                    if (lsn > cut) {
                        expected.push_back(lsn);
                    }
                }
                UNIT_ASSERT_VALUES_EQUAL_C(ReadIndexedLsns(dsk, physicalPages, cut), expected,
                    "cut# " << cut);
            }
        }

        Y_UNIT_TEST(OverlappingAppendsLeaveOneRecordPerCommit) {
            constexpr ui32 chunkSize = 512u << 10u;
            constexpr ui32 pageSize = 16u << 10u;
            constexpr ui32 indexBulk = 4;
            constexpr ui32 commits = 8;
            TUpdChecker checker(chunkSize, pageSize, indexBulk);
            TDiskRecLog dsk(chunkSize, pageSize, indexBulk, nullptr, nullptr);
            ui32 gen = 1;
            TVector<ui64> expectedLsns;
            const TVector<TSyncLogPageSnap> physicalPages = BuildOverlappingAppendsWorkload(
                dsk, checker, pageSize, indexBulk, commits, gen, expectedLsns);

            // accepted trade-off of dropping superseded copies: an overlapping append
            // cannot merge into the last record, so this workload grows the index with
            // commits (bounded by chunk trimming), not with data
            const auto records = GetIndexRecords(dsk);
            UNIT_ASSERT_VALUES_EQUAL(records.size(), commits);
            for (ui32 i = 0; i < commits; ++i) {
                AssertIndexRecord(records[i], 0, 100 + 2 * i, 2 * i,
                    i + 1 == commits ? 2 : 1);
            }
            UNIT_ASSERT_VALUES_EQUAL(ReadIndexedLsns(dsk, physicalPages, 99), expectedLsns);
        }

        Y_UNIT_TEST(IndexBulkDecreaseAfterReload) {
            constexpr ui32 chunkSize = 256u << 10u;
            constexpr ui32 pageSize = 16u << 10u;
            constexpr ui32 bigIndexBulk = 8;
            constexpr ui32 smallIndexBulk = 2;
            TFillIn1Context ctx{1, 1};

            // build an index whose last record holds more pages than the decreased IndexBulk
            TDiskRecLog bigDsk(chunkSize, pageSize, bigIndexBulk, nullptr, nullptr);
            TVector<TSyncLogPageSnap> initialPages;
            for (ui32 i = 0; i < bigIndexBulk; ++i) {
                initialPages.push_back(CreateSyncLogPage(1, pageSize, ctx));
            }
            bigDsk.UpdateIndex(0, initialPages);

            TStringStream s;
            bigDsk.Serialize(s);
            const TString serialized = s.Str();
            TDiskRecLog dsk(chunkSize, pageSize, smallIndexBulk,
                serialized.data(), serialized.data() + serialized.size());

            TVector<TSyncLogPageSnap> appendedPages;
            for (ui32 i = 0; i < 3; ++i) {
                appendedPages.push_back(CreateSyncLogPage(1, pageSize, ctx));
            }
            TUpdChecker checker(chunkSize, pageSize, smallIndexBulk);
            TDeltaToDiskRecLog delta(smallIndexBulk);
            delta.Append(0, appendedPages);
            checker.UpdateIndexWithCheck(&dsk, delta);

            // the oversized record must not grow further; new pages obey the new IndexBulk
            const auto records = GetIndexRecords(dsk);
            UNIT_ASSERT_VALUES_EQUAL(records.size(), 3u);
            AssertIndexRecord(records[0], 0, initialPages[0].GetFirstLsn(), 0, bigIndexBulk);
            AssertIndexRecord(records[1], 0, appendedPages[0].GetFirstLsn(), bigIndexBulk,
                smallIndexBulk);
            AssertIndexRecord(records[2], 0, appendedPages[2].GetFirstLsn(),
                bigIndexBulk + smallIndexBulk, 1);
            UNIT_ASSERT_VALUES_EQUAL(dsk.GetLastLsn(), appendedPages.back().GetLastLsn());
        }

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

            TString result = "{0 {{FirstLsn# 1 OffsInPages# 0 PagesNum# 4} "
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

            result = "{0 {{FirstLsn# 1 OffsInPages# 0 PagesNum# 4} {FirstLsn# 81 OffsInPages# 4 PagesNum# 4} "
                     "{FirstLsn# 161 OffsInPages# 8 PagesNum# 2} LastRealLsn# 199}}";
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
            TString result = "{0 {{FirstLsn# 1 OffsInPages# 0 PagesNum# 4} LastRealLsn# 79}} "
                            "{1 {{FirstLsn# 81 OffsInPages# 0 PagesNum# 4} LastRealLsn# 159}}";
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

            TString result = "{0 {{FirstLsn# 1 OffsInPages# 0 PagesNum# 2} "
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

            TString result = "{0 {{FirstLsn# 1 OffsInPages# 0 PagesNum# 3} "
                            "{FirstLsn# 61 OffsInPages# 4 PagesNum# 1} LastRealLsn# 87}}";
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
            UNIT_ASSERT(dsk->ToString() == "{1 {{FirstLsn# 81 OffsInPages# 0 PagesNum# 4} LastRealLsn# 159}} "
                        "{2 {{FirstLsn# 161 OffsInPages# 0 PagesNum# 4} LastRealLsn# 239}} "
                        "{3 {{FirstLsn# 241 OffsInPages# 0 PagesNum# 4} LastRealLsn# 319}}");

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

            TVector<TDeletedChunk> chunks;
            ui64 lsn = 0;

            chunks.clear();
            lsn = dsk->DeleteChunks(1, nullptr, chunks);
            UNIT_ASSERT(lsn == 80);
            UNIT_ASSERT(dsk->ToString() ==
                        "{1 {{FirstLsn# 81 OffsInPages# 0 PagesNum# 4} LastRealLsn# 159}} "
                        "{2 {{FirstLsn# 161 OffsInPages# 0 PagesNum# 4} LastRealLsn# 239}} "
                        "{3 {{FirstLsn# 241 OffsInPages# 0 PagesNum# 4} LastRealLsn# 319}}");

            chunks.clear();
            lsn = dsk->DeleteChunks(2, nullptr, chunks);
            UNIT_ASSERT(lsn == 240);
            UNIT_ASSERT(dsk->ToString() ==
                        "{3 {{FirstLsn# 241 OffsInPages# 0 PagesNum# 4} LastRealLsn# 319}}");
        }

    }

} // NKikimr
