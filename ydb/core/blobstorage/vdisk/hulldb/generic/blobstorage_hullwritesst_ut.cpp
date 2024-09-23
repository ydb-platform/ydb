#include "blobstorage_hullwritesst.h"
#include "hullds_sstvec_it.h"
#include "blobstorage_hullrecmerger.h"
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_arena.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_ut.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_logoblob.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_block.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/null.h>

#define STR Cnull

namespace NKikimr {

    Y_UNIT_TEST_SUITE(TBlobStorageHullWriteSst) {

        ////////////////////////////////////////////////////////////////////////////////////////
        // Definitions
        ////////////////////////////////////////////////////////////////////////////////////////
        typedef TLevelSegment<TKeyLogoBlob, TMemRecLogoBlob> TSstLogoBlob;
        typedef TSstLogoBlob::TWriter TWriterLogoBlob;
        typedef TCompactRecordMergerIndexPass<TKeyLogoBlob, TMemRecLogoBlob> TTLogoBlobCompactRecordMerger;

        typedef TLevelSegment<TKeyBlock, TMemRecBlock> TSstBlock;
        typedef TSstBlock::TWriter TWriterBlock;
        typedef TCompactRecordMergerIndexPass<TKeyBlock, TMemRecBlock> TBlockCompactRecordMerger;
        TTestContexts TestCtx;


        ////////////////////////////////////////////////////////////////////////////////////////
        // TTest
        ////////////////////////////////////////////////////////////////////////////////////////
        template <class TKey, class TMemRec, class TWriter>
        class TTest {
        public:
            struct TSstStat {
                TIndexWriterConclusion<TKey, TMemRec> WriterConclusion;
                ui32 Step;
                TSstStat(const TIndexWriterConclusion<TKey, TMemRec> &writerStat, ui32 step)
                    : WriterConclusion(writerStat)
                    , Step(step)
                {}

                TString ToString() const {
                    return Sprintf("%s step: %u", WriterConclusion.ToString().data(), Step);
                }
            };

            struct TAllStat {
                TVector<TSstStat> Stat;
                void Push(const TIndexWriterConclusion<TKey, TMemRec> &writerStat, ui32 step) {
                    Stat.push_back(TSstStat(writerStat, step));
                }

                TString ToString() const {
                    TStringStream s;
                    bool space = false;
                    for (const auto &stat : Stat) {
                        if (space)
                            s << " ";
                        else
                            space = true;
                        s << "{SST " << stat.ToString() << "}";
                    }
                    return s.Str();
                }
            };

            TTest(ui32 chunksToUse, ui8 owner, ui64 ownerRound, ui32 chunkSize, ui32 appendBlockSize, ui32 writeBlockSize)
                : ChunksToUse(chunksToUse)
                , Owner(owner)
                , OwnerRound(ownerRound)
                , ChunkSize(chunkSize)
                , AppendBlockSize(appendBlockSize)
                , WriteBlockSize(writeBlockSize)
                , WriterPtr(new TWriter(TestCtx.GetVCtx(), EWriterDataType::Fresh, ChunksToUse, Owner, OwnerRound,
                        ChunkSize, AppendBlockSize, WriteBlockSize, 0, false, ReservedChunks, Arena, true))
                , Arena(&TRopeArenaBackend::Allocate)
                , ReservedChunks()
                , Stat()
            {
                for (ui32 i = 1; i < 2000; i++)
                    ReservedChunks.push_back(i);
            }

            void Test(ui32 maxStep, const TString &data);
            void TestOutbound(ui32 maxStep);
            void Test(ui32 maxGen);

            const TAllStat &GetStat() const {
                return Stat;
            }

            void PrintOutResult() const {
                fprintf(stderr, "Entry: %s\n", ~WriterPtr->GetStat().Addr.ToString());
                fprintf(stderr, "UsedChunks:");
                for (auto c : WriterPtr->GetStat().UsedChunks)
                    fprintf(stderr, " %u", c);
                fprintf(stderr, "\n");
            }

        private:
            const ui32 ChunksToUse;
            const ui8 Owner;
            const ui64 OwnerRound;
            const ui32 ChunkSize;
            const ui32 AppendBlockSize;
            const ui32 WriteBlockSize;

            std::unique_ptr<TWriter> WriterPtr;
            TRopeArena Arena;
            TDeque<ui32> ReservedChunks;
            TAllStat Stat;
            THashMap<ui32, TMap<ui32, ui32>> WriteSpan;

            void Apply(std::unique_ptr<NPDisk::TEvChunkWrite> &msg) {
                ui32 chunkIdx = msg->ChunkIdx;
                UNIT_ASSERT(msg->ChunkIdx != 0);
                NPDisk::TEvChunkWrite::TPartsPtr partsPtr = msg->PartsPtr;
                const ui32 begin = msg->Offset;
                const ui32 end = begin + partsPtr->ByteSize();
                UNIT_ASSERT(!WriteSpan[msg->ChunkIdx].count(begin));
                WriteSpan[msg->ChunkIdx].emplace(begin, end);
                void *cookie = msg->Cookie;
                auto res = std::make_unique<NPDisk::TEvChunkWriteResult>(NKikimrProto::OK, chunkIdx, partsPtr, cookie, 0, TString());
                msg.reset(nullptr);
            }

            void Finish(ui32 step) {
                std::unique_ptr<NPDisk::TEvChunkWrite> msg;

                bool done = false;
                while (!done) {
                    done = WriterPtr->FlushNext(0, 100500, 1);
                    while (auto msg = WriterPtr->GetPendingMessage()) {
                        Apply(msg);
                    }
                }
                Stat.Push(WriterPtr->GetConclusion(), step);

                ValidateWriteSpan();
            }

            void ValidateWriteSpan() {
                for (const auto& kv : WriteSpan) {
                    ui32 expectedBegin = 0;
                    for (auto it = kv.second.begin(); it != kv.second.end(); ++it) {
                        UNIT_ASSERT_VALUES_EQUAL(expectedBegin, it->first);
                        expectedBegin = it->second;
                    }
                }
            }
        };


        template <>
        void TTest<TKeyLogoBlob, TMemRecLogoBlob, TWriterLogoBlob>::Test(ui32 maxStep, const TString &data) {
            TTLogoBlobCompactRecordMerger merger(TBlobStorageGroupType::ErasureMirror3, true);

            for (ui32 step = 0; step < maxStep; step++) {
                TLogoBlobID id(1, 1, step, 0, 0, 0);
                TKeyLogoBlob key(id);

                ui64 ingressMagic = ui64(1) << ui64(2); // magic
                TIngress ingress(ingressMagic);
                TMemRecLogoBlob memRec(ingress);


                TRope blobBuf = TDiskBlob::Create(data.size(), 1, 3, TRope(data), Arena, true);

                memRec.SetDiskBlob(TDiskPart(0, 0, data.size()));
                merger.Clear();
                merger.SetLoadDataMode(true);
                merger.AddFromFresh(memRec, &blobBuf, key, step + 1);
                merger.Finish();

                bool pushRes = WriterPtr->Push(key, memRec, merger.GetDataMerger());
                if (!pushRes) {
                    Finish(step);
                    WriterPtr = std::make_unique<TWriterLogoBlob>(TestCtx.GetVCtx(), EWriterDataType::Fresh, ChunksToUse,
                        Owner, OwnerRound, ChunkSize, AppendBlockSize, WriteBlockSize, 0, false, ReservedChunks, Arena,
                        true);
                    pushRes = WriterPtr->Push(key, memRec, merger.GetDataMerger());
                    Y_ABORT_UNLESS(pushRes);
                }
                while (auto msg = WriterPtr->GetPendingMessage()) {
                    Apply(msg);
                }
            }

            Finish(maxStep);
        }

        template <>
        void TTest<TKeyLogoBlob, TMemRecLogoBlob, TWriterLogoBlob>::TestOutbound(ui32 maxStep) {
            TTLogoBlobCompactRecordMerger merger(TBlobStorageGroupType::ErasureMirror3, true);

            for (ui32 step = 0; step < maxStep; step++) {
                TLogoBlobID id(1, 1, step, 0, 0, 0);
                TKeyLogoBlob key(id);

                ui64 ingressMagic1 = ui64(1) << ui64(2); // magic
                TIngress ingress1(ingressMagic1);
                TMemRecLogoBlob memRec1(ingress1);
                memRec1.SetHugeBlob(TDiskPart(1, 2, 3));


                ui64 ingressMagic2 = ui64(1) << ui64(1); // magic
                TIngress ingress2(ingressMagic2);
                TMemRecLogoBlob memRec2(ingress2);
                memRec2.SetHugeBlob(TDiskPart(1, 2, 3));

                merger.Clear();
                merger.SetLoadDataMode(true);
                merger.AddFromFresh(memRec1, nullptr, key, 1);
                merger.AddFromFresh(memRec2, nullptr, key, 2);
                merger.Finish();

                bool pushRes = WriterPtr->Push(key, merger.GetMemRec(), merger.GetDataMerger());
                if (!pushRes) {
                    Finish(step);

                    WriterPtr = std::make_unique<TWriterLogoBlob>(TestCtx.GetVCtx(), EWriterDataType::Fresh, ChunksToUse,
                        Owner, OwnerRound, ChunkSize, AppendBlockSize, WriteBlockSize, 0, false, ReservedChunks, Arena,
                        true);
                    pushRes = WriterPtr->Push(key, merger.GetMemRec(), merger.GetDataMerger());
                    Y_ABORT_UNLESS(pushRes);
                }
                while (auto msg = WriterPtr->GetPendingMessage()) {
                    Apply(msg);
                }
            }

            Finish(maxStep);
        }

        template <>
        void TTest<TKeyBlock, TMemRecBlock, TWriterBlock>::Test(ui32 maxGen) {
            TBlockCompactRecordMerger merger(TBlobStorageGroupType::ErasureMirror3, true);

            for (ui32 gen = 0; gen < maxGen; gen++) {
                TKeyBlock key(34 + gen);
                TMemRecBlock memRec(gen);
                merger.Clear();
                merger.SetLoadDataMode(true);
                merger.AddFromFresh(memRec, nullptr, key, gen + 1);
                merger.Finish();

                bool pushRes = WriterPtr->Push(key, memRec, merger.GetDataMerger());
                if (!pushRes) {
                    Finish(gen);

                    WriterPtr = std::make_unique<TWriterBlock>(TestCtx.GetVCtx(), EWriterDataType::Fresh, ChunksToUse,
                        Owner, OwnerRound, ChunkSize, AppendBlockSize, WriteBlockSize, 0, false, ReservedChunks, Arena,
                        true);
                    pushRes = WriterPtr->Push(key, memRec, merger.GetDataMerger());
                    Y_ABORT_UNLESS(pushRes);
                }
                while (auto msg = WriterPtr->GetPendingMessage()) {
                    Apply(msg);
                }
            }

            Finish(maxGen);
        }



        ////////////////////////////////////////////////////////////////////////////////////////
        // TESTS (LogoBlobs)
        ////////////////////////////////////////////////////////////////////////////////////////
        Y_UNIT_TEST(LogoBlobOneSstOneIndex) {
            // TODO(kruall): fix the test and remove the line below
            return;
            ui32 chunksToUse = 4;
            ui8 owner = 1;
            ui64 ownerRound = 1;
            ui32 chunkSize = 1u << 20u;
            ui32 appendBlockSize = 4u << 10u;
            ui32 writeBlockSize = 16u << 10u;
            TString data("Hello, world!");
            TTest<TKeyLogoBlob, TMemRecLogoBlob, TWriterLogoBlob> test(chunksToUse, owner, ownerRound, chunkSize, appendBlockSize, writeBlockSize);
            test.Test(10000, data);

            TString res("{SST {Addr: {ChunkIdx: 1 Offset: 200000 Size: 440096} "
                            "IndexParts: 1 {UsedChunks: 1}} step: 10000}");
            STR << res << "\n";
            STR << test.GetStat().ToString() << "\n";
            UNIT_ASSERT_VALUES_EQUAL(test.GetStat().ToString(), res);
        }

        Y_UNIT_TEST(LogoBlobOneSstOneIndexWithSmallWriteBlocks) {
            // TODO(kruall): fix the test and remove the line below
            return;
            ui32 chunksToUse = 4;
            ui8 owner = 1;
            ui64 ownerRound = 1;
            ui32 chunkSize = 1u << 20u;
            ui32 appendBlockSize = 1 << 10u;
            ui32 writeBlockSize = appendBlockSize;

            TString data("Hello, world!");
            while (data.size() < writeBlockSize * 2) {
                data += data;
            }

            ui32 numBlobs = chunkSize / 2 / data.size();

            TTest<TKeyLogoBlob, TMemRecLogoBlob, TWriterLogoBlob> test(chunksToUse, owner, ownerRound, chunkSize, appendBlockSize, writeBlockSize);
            test.Test(numBlobs, data);

            const ui32 offset = ((data.size() + 8) & ~3) * numBlobs; // 8 = 5 byte blob header + blob data + alignment up to 4 bytes
            const ui32 size = numBlobs * (sizeof(TKeyLogoBlob) + sizeof(TMemRecLogoBlob)) + sizeof(TIdxDiskPlaceHolder);
            const ui32 step = numBlobs;

            auto res = Sprintf("{SST {Addr: {ChunkIdx: 1 Offset: %u Size: %u} IndexParts: 1 {UsedChunks: 1}} step: %u}",
                offset, size, step);

            STR << res << "\n";
            STR << test.GetStat().ToString() << "\n";
            UNIT_ASSERT_VALUES_EQUAL(test.GetStat().ToString(), res);
        }

        Y_UNIT_TEST(LogoBlobOneSstMultiIndex) {
            // TODO(kruall): fix the test and remove the line below
            return;
            ui32 chunksToUse = 4;
            ui8 owner = 1;
            ui64 ownerRound = 1;
            ui32 chunkSize = 1u << 20u;
            ui32 appendBlockSize = 4u << 10u;
            ui32 writeBlockSize = 16u << 10u;
            TString data("X");
            TTest<TKeyLogoBlob, TMemRecLogoBlob, TWriterLogoBlob> test(chunksToUse, owner, ownerRound, chunkSize, appendBlockSize, writeBlockSize);
            test.Test(50000, data);

            TString res("{SST {Addr: {ChunkIdx: 3 Offset: 0 Size: 502972} "
                            "IndexParts: 3 {UsedChunks: 1 2 3}} step: 50000}");
            STR << res << "\n";
            STR << test.GetStat().ToString() << "\n";
            UNIT_ASSERT_VALUES_EQUAL(test.GetStat().ToString(), res);
        }

        Y_UNIT_TEST(LogoBlobMultiSstOneIndex) {
            // TODO(kruall): fix the test and remove the line below
            return;
            ui32 chunksToUse = 2;
            ui8 owner = 1;
            ui64 ownerRound = 1;
            ui32 chunkSize = 1u << 20u;
            ui32 appendBlockSize = 4u << 10u;
            ui32 writeBlockSize = 16u << 10u;
            TString data("Hello, world!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
            TTest<TKeyLogoBlob, TMemRecLogoBlob, TWriterLogoBlob> test(chunksToUse, owner, ownerRound, chunkSize, appendBlockSize, writeBlockSize);
            test.Test(50000, data);
            TString res("{SST {Addr: {ChunkIdx: 2 Offset: 125776 Size: 922776} "
                            "IndexParts: 1 {UsedChunks: 1 2}} step: 20970} "
                       "{SST {Addr: {ChunkIdx: 4 Offset: 125776 Size: 922776} "
                            "IndexParts: 1 {UsedChunks: 3 4}} step: 41940} "
                       "{SST {Addr: {ChunkIdx: 5 Offset: 451360 Size: 354736} "
                       "IndexParts: 1 {UsedChunks: 5}} step: 50000}");
            STR << res << "\n";
            STR << test.GetStat().ToString() << "\n";
            UNIT_ASSERT_VALUES_EQUAL(test.GetStat().ToString(), res);
        }

        Y_UNIT_TEST(LogoBlobMultiSstMultiIndex) {
            // TODO(kruall): fix the test and remove the line below
            return;
            ui32 chunksToUse = 4;
            ui8 owner = 1;
            ui64 ownerRound = 1;
            ui32 chunkSize = 1u << 20u;
            ui32 appendBlockSize = 4u << 10u;
            ui32 writeBlockSize = 16u << 10u;
            TString data("X");
            TTest<TKeyLogoBlob, TMemRecLogoBlob, TWriterLogoBlob> test(chunksToUse, owner, ownerRound, chunkSize, appendBlockSize, writeBlockSize);
            test.Test(1000000, data);
            TString res("{SST {Addr: {ChunkIdx: 4 Offset: 0 Size: 1048572} "
                            "IndexParts: 4 {UsedChunks: 1 2 3 4}} step: 80657} "
                       "{SST {Addr: {ChunkIdx: 8 Offset: 0 Size: 1048572} "
                            "IndexParts: 4 {UsedChunks: 5 6 7 8}} step: 161314} "
                       "{SST {Addr: {ChunkIdx: 12 Offset: 0 Size: 1048572} "
                            "IndexParts: 4 {UsedChunks: 9 10 11 12}} step: 241971} "
                       "{SST {Addr: {ChunkIdx: 16 Offset: 0 Size: 1048572} "
                            "IndexParts: 4 {UsedChunks: 13 14 15 16}} step: 322628} "
                       "{SST {Addr: {ChunkIdx: 20 Offset: 0 Size: 1048572} "
                            "IndexParts: 4 {UsedChunks: 17 18 19 20}} step: 403285} "
                       "{SST {Addr: {ChunkIdx: 24 Offset: 0 Size: 1048572} "
                            "IndexParts: 4 {UsedChunks: 21 22 23 24}} step: 483942} "
                       "{SST {Addr: {ChunkIdx: 28 Offset: 0 Size: 1048572} "
                            "IndexParts: 4 {UsedChunks: 25 26 27 28}} step: 564599} "
                       "{SST {Addr: {ChunkIdx: 32 Offset: 0 Size: 1048572} "
                            "IndexParts: 4 {UsedChunks: 29 30 31 32}} step: 645256} "
                       "{SST {Addr: {ChunkIdx: 36 Offset: 0 Size: 1048572} "
                            "IndexParts: 4 {UsedChunks: 33 34 35 36}} step: 725913} "
                       "{SST {Addr: {ChunkIdx: 40 Offset: 0 Size: 1048572} "
                            "IndexParts: 4 {UsedChunks: 37 38 39 40}} step: 806570} "
                       "{SST {Addr: {ChunkIdx: 44 Offset: 0 Size: 1048572} "
                            "IndexParts: 4 {UsedChunks: 41 42 43 44}} step: 887227} "
                       "{SST {Addr: {ChunkIdx: 48 Offset: 0 Size: 1048572} "
                            "IndexParts: 4 {UsedChunks: 45 46 47 48}} step: 967884} "
                       "{SST {Addr: {ChunkIdx: 50 Offset: 0 Size: 621596} "
                            "IndexParts: 2 {UsedChunks: 49 50}} step: 1000000}");
            STR << res << "\n";
            STR << test.GetStat().ToString() << "\n";
            UNIT_ASSERT_VALUES_EQUAL(test.GetStat().ToString(), res);
        }

        ////////////////////////////////////////////////////////////////////////////////////////
        // TESTS (Outbound LogoBlobs)
        ////////////////////////////////////////////////////////////////////////////////////////
        Y_UNIT_TEST(LogoBlobOneSstOneIndexPartOutbound) {
            ui32 chunksToUse = 4;
            ui8 owner = 1;
            ui64 ownerRound = 1;
            ui32 chunkSize = 1u << 20u;
            ui32 appendBlockSize = 4u << 10u;
            ui32 writeBlockSize = 16u << 10u;
            TTest<TKeyLogoBlob, TMemRecLogoBlob, TWriterLogoBlob> test(chunksToUse, owner, ownerRound, chunkSize, appendBlockSize, writeBlockSize);
            test.TestOutbound(10000);

            TString res("{SST {Addr: {ChunkIdx: 1 Offset: 0 Size: 680096} "
                            "IndexParts: 1 OutboundItems: 20000 {UsedChunks: 1}} step: 10000}");
            STR << res << "\n";
            STR << test.GetStat().ToString() << "\n";
            UNIT_ASSERT_VALUES_EQUAL(test.GetStat().ToString(), res);
        }

        Y_UNIT_TEST(LogoBlobOneSstMultiIndexPartOutbound) {
            // TODO(kruall): fix the test and remove the line below
            return;
            ui32 chunksToUse = 4;
            ui8 owner = 1;
            ui64 ownerRound = 1;
            ui32 chunkSize = 1u << 20u;
            ui32 appendBlockSize = 4u << 10u;
            ui32 writeBlockSize = 16u << 10u;
            TTest<TKeyLogoBlob, TMemRecLogoBlob, TWriterLogoBlob> test(chunksToUse, owner, ownerRound, chunkSize, appendBlockSize, writeBlockSize);
            test.TestOutbound(50000);

            TString res("{SST {Addr: {ChunkIdx: 4 Offset: 0 Size: 254412} "
                            "IndexParts: 4 OutboundItems: 100000 {UsedChunks: 1 2 3 4}} step: 50000}");
            STR << res << "\n";
            STR << test.GetStat().ToString() << "\n";
            UNIT_ASSERT_VALUES_EQUAL(test.GetStat().ToString(), res);
        }


        Y_UNIT_TEST(LogoBlobMultiSstOneIndexPartOutbound) {
            ui32 chunksToUse = 1;
            ui8 owner = 1;
            ui64 ownerRound = 1;
            ui32 chunkSize = 1u << 20u;
            ui32 appendBlockSize = 4u << 10u;
            ui32 writeBlockSize = 16u << 10u;
            TTest<TKeyLogoBlob, TMemRecLogoBlob, TWriterLogoBlob> test(chunksToUse, owner, ownerRound, chunkSize, appendBlockSize, writeBlockSize);
            test.TestOutbound(20000);
            TString res("{SST {Addr: {ChunkIdx: 1 Offset: 0 Size: 1048520} "
                            "IndexParts: 1 OutboundItems: 30836 {UsedChunks: 1}} step: 15418} "
                       "{SST {Addr: {ChunkIdx: 2 Offset: 0 Size: 311672} "
                            "IndexParts: 1 OutboundItems: 9164 {UsedChunks: 2}} step: 20000}");
            STR << res << "\n";
            STR << test.GetStat().ToString() << "\n";
            UNIT_ASSERT_VALUES_EQUAL(test.GetStat().ToString(), res);
        }


        ////////////////////////////////////////////////////////////////////////////////////////
        // TESTS (NoData)
        ////////////////////////////////////////////////////////////////////////////////////////
        Y_UNIT_TEST(BlockOneSstOneIndex) {
            ui32 chunksToUse = 2;
            ui8 owner = 1;
            ui64 ownerRound = 1;
            ui32 chunkSize = 1u << 20u;
            ui32 appendBlockSize = 4u << 10u;
            ui32 writeBlockSize = 16u << 10u;
            TTest<TKeyBlock, TMemRecBlock, TWriterBlock> test(chunksToUse, owner, ownerRound, chunkSize, appendBlockSize, writeBlockSize);
            test.Test(5000);

            TString res("{SST {Addr: {ChunkIdx: 1 Offset: 0 Size: 60096} "
                       "IndexParts: 1 {UsedChunks: 1}} step: 5000}");
            STR << res << "\n";
            STR << test.GetStat().ToString() << "\n";
            UNIT_ASSERT_VALUES_EQUAL(test.GetStat().ToString(), res);
        }

        Y_UNIT_TEST(BlockOneSstMultiIndex) {
            ui32 chunksToUse = 4;
            ui8 owner = 1;
            ui64 ownerRound = 1;
            ui32 chunkSize = 1u << 20u;
            ui32 appendBlockSize = 4u << 10u;
            ui32 writeBlockSize = 16u << 10u;
            TTest<TKeyBlock, TMemRecBlock, TWriterBlock> test(chunksToUse, owner, ownerRound, chunkSize, appendBlockSize, writeBlockSize);
            test.Test(150000);

            TString res("{SST {Addr: {ChunkIdx: 2 Offset: 0 Size: 751536} "
                            "IndexParts: 2 {UsedChunks: 1 2}} step: 150000}");
            STR << res << "\n";
            STR << test.GetStat().ToString() << "\n";
            UNIT_ASSERT_VALUES_EQUAL(test.GetStat().ToString(), res);
        }

        Y_UNIT_TEST(BlockMultiSstOneIndex) {
            ui32 chunksToUse = 1;
            ui8 owner = 1;
            ui64 ownerRound = 1;
            ui32 chunkSize = 1u << 20u;
            ui32 appendBlockSize = 4u << 10u;
            ui32 writeBlockSize = 16u << 10u;
            TTest<TKeyBlock, TMemRecBlock, TWriterBlock> test(chunksToUse, owner, ownerRound, chunkSize, appendBlockSize, writeBlockSize);
            test.Test(150000);

            TString res("{SST {Addr: {ChunkIdx: 1 Offset: 0 Size: 1048572} "
                            "IndexParts: 1 {UsedChunks: 1}} step: 87373} "
                       "{SST {Addr: {ChunkIdx: 2 Offset: 0 Size: 751620} "
                            "IndexParts: 1 {UsedChunks: 2}} step: 150000}");
            STR << res << "\n";
            STR << test.GetStat().ToString() << "\n";
            UNIT_ASSERT_VALUES_EQUAL(test.GetStat().ToString(), res);
        }
    }

} // NKikimr
