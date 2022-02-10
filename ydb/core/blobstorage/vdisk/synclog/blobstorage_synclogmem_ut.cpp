#include "blobstorage_synclogmem.h"
#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/null.h>

#define STR Cnull

namespace NKikimr {

    using namespace NSyncLog;

    Y_UNIT_TEST_SUITE(TBlobStorageSyncLogMem) {

        /////////////////////////////////////////////////////////////////////////////////////////
        // Put Blocks
        /////////////////////////////////////////////////////////////////////////////////////////
        struct TFillInBlockContext {
            ui64 Lsn;
            ui32 Gen;
        };

        TFillInBlockContext FillInBlock(NSyncLog::TMemRecLog *mem, ui64 num, TFillInBlockContext ctx) {
            char buf[NSyncLog::MaxRecFullSize];
            ui32 size = 0;
            ui64 tabletId = 1;

            for (ui64 i = 0; i < num; i++) {
                size = NSyncLog::TSerializeRoutines::SetBlock(buf, ctx.Lsn, tabletId, ctx.Gen, 0);
                ctx.Lsn += 2;
                ctx.Gen++;
                mem->PutOne((const NSyncLog::TRecordHdr *)buf, size);
            }

            return ctx;
        }


        /////////////////////////////////////////////////////////////////////////////////////////
        // Put LogoBlobs
        /////////////////////////////////////////////////////////////////////////////////////////
        struct TFillInLogoBlobContext {
            ui64 Lsn;
            TLogoBlobID Id;
        };

        TFillInLogoBlobContext FillInLogoBlob(NSyncLog::TMemRecLog *mem, ui64 num, TFillInLogoBlobContext ctx) {
            TBlobStorageGroupInfo groupInfo(TBlobStorageGroupType::ErasureMirror3, 2, 4);
            char buf[NSyncLog::MaxRecFullSize];
            ui32 size = 0;

            for (unsigned i = 0; i < num; i++) {
                size = NSyncLog::TSerializeRoutines::SetLogoBlob(groupInfo.Type, buf, ctx.Lsn, ctx.Id, TIngress());
                ctx.Lsn += 2;
                ctx.Id = TLogoBlobID(ctx.Id.TabletID(), ctx.Id.Generation(), ctx.Id.Step() + 1,
                                     ctx.Id.Channel(), ctx.Id.BlobSize(), ctx.Id.Cookie());
                mem->PutOne((const NSyncLog::TRecordHdr *)buf, size);
            }

            return ctx;
        }

        void TestEmptySnapshot(TMemRecLogSnapshotPtr snap) {
            {
                // check Seek on snapshot
                TMemRecLogSnapshot::TIterator it(snap);
                it.Seek(985);
                UNIT_ASSERT(!it.Valid());
            }

            {
                // check elems iterator
                TMemRecLogSnapshot::TIterator it(snap);
                it.SeekToFirst();
                const TRecordHdr *hdr = nullptr;
                ui64 lsn = 1;
                while (it.Valid()) {
                    hdr = it.Get();
                    UNIT_ASSERT(hdr->Lsn == lsn);
                    it.Next();
                    lsn += 2;
                }
                UNIT_ASSERT(lsn == 1);
            }
        }


        void TestFilledIn1(TMemRecLogSnapshotPtr snap, ui32 elems) {
            {
                TMemRecLogSnapshot::TIterator it(snap);
                const TRecordHdr *hdr = nullptr;

                // in the middle of the page (we have this element)
                it.Seek(985);
                UNIT_ASSERT(it.Valid());
                hdr = it.Get();
                UNIT_ASSERT(hdr->Lsn == 985);

                // in the middle of the page (we don't have this element)
                it.Seek(986);
                UNIT_ASSERT(it.Valid());
                hdr = it.Get();
                UNIT_ASSERT(hdr->Lsn == 987);

                // behind the log
                it.Seek(2005);
                UNIT_ASSERT(!it.Valid());

                // last log record
                it.Seek(1999);
                UNIT_ASSERT(it.Valid());
                hdr = it.Get();
                UNIT_ASSERT(hdr->Lsn == 1999);

                // in the middle, between pages, we don't have this element
                it.Seek(1666);
                UNIT_ASSERT(it.Valid());
                hdr = it.Get();
                UNIT_ASSERT(hdr->Lsn == 1667);

                // below the smallest
                it.Seek(0);
                UNIT_ASSERT(it.Valid());
                hdr = it.Get();
                UNIT_ASSERT(hdr->Lsn == 1);
            }

            {
                // check elems iterator
                TMemRecLogSnapshot::TIterator it(snap);
                it.SeekToFirst();
                const TRecordHdr *hdr = nullptr;
                ui64 lsn = 1;
                while (it.Valid()) {
                    hdr = it.Get();
                    UNIT_ASSERT(hdr->Lsn == lsn);
                    it.Next();
                    lsn += 2;
                }
                UNIT_ASSERT(lsn == 1 + elems * 2);
            }
        }


        Y_UNIT_TEST(EmptyMemRecLog) {
            const ui32 pageSize = 1024u;
            NSyncLog::TMemRecLog mem(pageSize);
            TMemRecLogSnapshotPtr snap = mem.GetSnapshot();
            TestEmptySnapshot(snap);
        }

        Y_UNIT_TEST(FilledIn1) {
            const ui32 pageSize = 1024u;
            const ui32 elems = 1000u;
            NSyncLog::TMemRecLog mem(pageSize);

            // fill in the mem log
            FillInBlock(&mem, elems, TFillInBlockContext {1u, 1u});
            mem.Output(STR);
            TMemRecLogSnapshotPtr snap = mem.GetSnapshot();
            TestFilledIn1(snap, elems);
        }

        Y_UNIT_TEST(EmptyMemRecLogPutAfterSnapshot) {
            const ui32 pageSize = 1024u;
            const ui32 elems = 1000u;
            NSyncLog::TMemRecLog mem(pageSize);
            TMemRecLogSnapshotPtr snap = mem.GetSnapshot();
            FillInBlock(&mem, elems, TFillInBlockContext {1u, 1u}); // write after taking snapshot
            TestEmptySnapshot(snap);
        }

        Y_UNIT_TEST(FilledIn1PutAfterSnapshot) {
            const ui32 pageSize = 1024u;
            const ui32 elems = 1000u;
            NSyncLog::TMemRecLog mem(pageSize);

            // fill in the mem log
            TFillInBlockContext ctx = FillInBlock(&mem, elems, TFillInBlockContext {1u, 1u});
            mem.Output(STR);
            TMemRecLogSnapshotPtr snap = mem.GetSnapshot();
            FillInBlock(&mem, elems, ctx); // write after taking snapshot
            TestFilledIn1(snap, elems);
        }

        Y_UNIT_TEST(ManyLogoBlobsPerf) {
            // just put many records
            const ui32 pageSize = 1024u;
            const ui32 elems = 10000000u;
            NSyncLog::TMemRecLog mem(pageSize);

            // fill in the mem log
            FillInLogoBlob(&mem, elems, TFillInLogoBlobContext {1u, TLogoBlobID(1, 1, 3, 0, 32, 0)});
        }

        Y_UNIT_TEST(ManyLogoBlobsBuildSwapSnapshot) {
            // just put many records
            const ui32 pageSize = 1024u;
            const ui32 elems = 1000u;
            NSyncLog::TMemRecLog mem(pageSize);

            // fill in the mem log
            FillInLogoBlob(&mem, elems, TFillInLogoBlobContext {1u, TLogoBlobID(1, 1, 3, 0, 32, 0)});

            STR << "mem# " << mem.ToString() << "\n";
            ui64 freeUpToLsn = 10;
            auto swapSnap = mem.BuildSwapSnapshot(0, freeUpToLsn, 0);
            STR << "swapSnap# " << swapSnap->BoundariesToString() << "\n";
            UNIT_ASSERT(swapSnap && !swapSnap->Empty());
        }

    }

} // NKikimr
