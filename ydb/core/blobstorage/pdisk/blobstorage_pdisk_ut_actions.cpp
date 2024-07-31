#include "blobstorage_pdisk_ut_actions.h"

namespace NKikimr {

void TTestInitCorruptedError::TestFSM(const TActorContext &ctx) {
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(1, VDiskID, *PDiskGuid));
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        ctx.Send(Yard, new NPDisk::TEvYardInit(3, VDiskID, *PDiskGuid));
        break;
    case 10:
        TEST_RESPONSE(EvYardInitResult, CORRUPTED);
        VERBOSE_COUT(" Sending TEvInit again");
        ctx.Send(Yard, new NPDisk::TEvYardInit(4, VDiskID, *PDiskGuid));
        break;
    case 20:
        TEST_RESPONSE(EvYardInitResult, CORRUPTED);
        break;
    case 30:
        TEST_RESPONSE(EvYardInitResult, CORRUPTED);
        break;
    case 40:
        TEST_RESPONSE(EvYardInitResult, CORRUPTED);

        VERBOSE_COUT("Done");
        SignalDoneEvent();
        break;
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
}

void TTestInitOwner::TestFSM(const TActorContext &ctx) {
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));

        VERBOSE_COUT(" Sending TEvInit 0");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, TVDiskID(0, 0, 0, 0, 0), *PDiskGuid));
        break;
    case 10:
        TEST_RESPONSE(EvYardInitResult, OK);
        ASSERT_YTHROW(LastResponse.OwnedChunks.empty(),
            "non-empty OwnedChuns, size=" << (int)LastResponse.OwnedChunks.size());
        Owner = LastResponse.Owner;
        OwnerRound = LastResponse.OwnerRound;

        VERBOSE_COUT(" Sending TEvInit 1");
        ctx.Send(Yard, new NPDisk::TEvYardInit(3, TVDiskID(0, 0, 0, 0, 1), *PDiskGuid));
        break;
    case 20:
        TEST_RESPONSE(EvYardInitResult, OK);
        ASSERT_YTHROW(Owner != LastResponse.Owner, "Different vdisks have matching owner ids=" << (int)Owner);
        ASSERT_YTHROW(LastResponse.OwnedChunks.empty(),
            "non-empty OwnedChuns, size=" << (int)LastResponse.OwnedChunks.size());

        VERBOSE_COUT("Done");
        SignalDoneEvent();
        break;
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
}

void TTestLogStartingPoint::TestFSM(const TActorContext &ctx) {
    TRcBuf data(TString("testdata"));
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        break;
    case 10:
    {
        TEST_RESPONSE(EvYardInitResult, OK);
        Owner = LastResponse.Owner;
        OwnerRound = LastResponse.OwnerRound;
        VERBOSE_COUT(" Sending TEvLog");
        NPDisk::TCommitRecord commitRecord;
        commitRecord.IsStartingPoint = true;
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, commitRecord, data, TLsnSeg(1, 1), nullptr));
        break;
    }
    case 20:
        TEST_RESPONSE(EvLogResult, OK);
        VERBOSE_COUT("Done");
        SignalDoneEvent();
        break;
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
}

void TTestIncorrectRequests::TestFSM(const TActorContext &ctx) {
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        Lsn = 123;
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        break;
    case 10:
    {
        TEST_RESPONSE(EvYardInitResult, OK);
        Owner = LastResponse.Owner;
        OwnerRound = LastResponse.OwnerRound;
        ChunkSize = LastResponse.ChunkSize;
        VERBOSE_COUT(" Sending TEvLog");
        auto lsn = NextLsn();
        ctx.Send(Yard, new NPDisk::TEvLog(Owner+1, OwnerRound, 0, TRcBuf(), TLsnSeg(lsn, lsn), (void*)456));
        break;
    }
    case 20:
        TEST_RESPONSE(EvLogResult, INVALID_OWNER);
        VERBOSE_COUT(" Sending TEvLogRead");
        ctx.Send(Yard, new NPDisk::TEvReadLog(Owner, OwnerRound, NPDisk::TLogPosition{0, 100500}));
        break;
    case 30:
    {
        TEST_RESPONSE(EvReadLogResult, ERROR);
        VERBOSE_COUT(" Sending TEvChunkReserve");
        ctx.Send(Yard, new NPDisk::TEvChunkReserve(Owner, OwnerRound, 1));
        TestStep = 35;
        return;
    }
    case 35:
    {
        TEST_RESPONSE(EvChunkReserveResult, OK);
        ASSERT_YTHROW(LastResponse.ChunkIds.size() == 1,
            "Unexpected ChunkIds.size() == " << LastResponse.ChunkIds.size());
        ChunkIdx0 = LastResponse.ChunkIds[0];
        VERBOSE_COUT(" Sending TEvChunkWrite");
        ctx.Send(Yard, new NPDisk::TEvChunkWrite(Owner, OwnerRound, ChunkIdx0, 0,
            nullptr, (void*)42, false, 1));
        TestStep = 40;
        return;
    }
    case 40:
    {
        TEST_RESPONSE(EvChunkWriteResult, ERROR);
        VERBOSE_COUT(" Sending TEvLog to commit");
        NPDisk::TCommitRecord commitRecord;
        commitRecord.CommitChunks.push_back(1);
        auto lsn = NextLsn();
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, commitRecord, TRcBuf(), TLsnSeg(lsn, lsn),
                    (void*)43));
        break;
    }
    case 50:
        TEST_RESPONSE(EvLogResult, ERROR);
        VERBOSE_COUT(" Sending TEvChunkRead");
        ctx.Send(Yard, new NPDisk::TEvChunkRead(Owner, OwnerRound, 0, 100500, 128, 1, nullptr));
        break;
    case 60:
        TEST_RESPONSE(EvChunkReadResult, ERROR);
        VERBOSE_COUT(" Sending TEvChunkRead");
        ctx.Send(Yard, new NPDisk::TEvChunkRead(Owner, OwnerRound, 1, 100500, 128, 1, nullptr));
        break;
    case 70:
        TEST_RESPONSE(EvChunkReadResult, ERROR);
        VERBOSE_COUT(" Sending TEvChunkRead");
        ctx.Send(Yard, new NPDisk::TEvChunkRead(Owner, OwnerRound, 3, 100500, 128, 1, nullptr));
        break;
    case 80:
    {
        TEST_RESPONSE(EvChunkReadResult, ERROR);
        VERBOSE_COUT(" Sending TEvLog to commit");
        NPDisk::TCommitRecord commitRecord;
        commitRecord.CommitChunks.push_back(1);
        auto lsn = NextLsn();
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, commitRecord, TRcBuf(), TLsnSeg(lsn, lsn),
                    (void*)43));
        break;
    }
    case 90:
    {
        TEST_RESPONSE(EvLogResult, ERROR);
        VERBOSE_COUT(" Sending TEvLog to commit");
        NPDisk::TCommitRecord commitRecord;
        commitRecord.CommitChunks.push_back(3);
        auto lsn = NextLsn();
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, commitRecord, TRcBuf(), TLsnSeg(lsn, lsn),
                    (void*)43));
        break;
    }
    case 100:
    {
        TEST_RESPONSE(EvLogResult, ERROR);
        VERBOSE_COUT(" Sending TEvLog to commit");
        NPDisk::TCommitRecord commitRecord;
        commitRecord.DeleteChunks.push_back(0);
        auto lsn = NextLsn();
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, commitRecord, TRcBuf(), TLsnSeg(lsn, lsn),
                    (void*)43));
        break;
    }
    case 110:
    {
        TEST_RESPONSE(EvLogResult, ERROR);
        VERBOSE_COUT(" Sending TEvLog to commit");
        NPDisk::TCommitRecord commitRecord;
        commitRecord.DeleteChunks.push_back(1);
        auto lsn = NextLsn();
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, commitRecord, TRcBuf(), TLsnSeg(lsn, lsn),
                    (void*)43));
        break;
    }
    case 120:
    {
        TEST_RESPONSE(EvLogResult, ERROR);
        VERBOSE_COUT(" Sending TEvLog to commit");
        NPDisk::TCommitRecord commitRecord;
        commitRecord.DeleteChunks.push_back(3);
        auto lsn = NextLsn();
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, commitRecord, TRcBuf(), TLsnSeg(lsn, lsn),
                    (void*)43));
        break;
    }
    case 130:
    {
        TEST_RESPONSE(EvLogResult, ERROR);
        VERBOSE_COUT(" Sending TEvLog to commit");
        NPDisk::TCommitRecord commitRecord;
        commitRecord.DeleteChunks.push_back((ui32)-1);
        auto lsn = NextLsn();
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, commitRecord, TRcBuf(), TLsnSeg(lsn, lsn),
                    (void*)43));
        break;
    }
    case 140:
        TEST_RESPONSE(EvLogResult, ERROR);
        VERBOSE_COUT(" Sending TEvChunkWrite");
        ChunkWriteParts.Reset(new NPDisk::TEvChunkWrite::TPart[1]);
        ChunkWriteParts[0].Data = nullptr;
        ChunkWriteParts[0].Size = 100500;
        ctx.Send(Yard, new NPDisk::TEvChunkWrite(Owner, OwnerRound, ChunkIdx0, 0,
            new NPDisk::TEvChunkWrite::TNonOwningParts(ChunkWriteParts.Get(), 1), (void*)42, false, 1));
        break;
    case 150:
        TEST_RESPONSE(EvChunkWriteResult, OK); // OK due to interface changes
        VERBOSE_COUT(" Sending TEvChunkWrite");
        ChunkWriteData = PrepareData(1000);
        ChunkWriteParts[0].Data = ChunkWriteData.data();
        ChunkWriteParts[0].Size = ChunkWriteData.size();
        ctx.Send(Yard, new NPDisk::TEvChunkWrite(Owner, OwnerRound, ChunkIdx0, ChunkSize,
            new NPDisk::TEvChunkWrite::TNonOwningParts(ChunkWriteParts.Get(), 1), (void*)42, false, 1));
        break;
    case 160:
        TEST_RESPONSE(EvChunkWriteResult, ERROR);
        VERBOSE_COUT(" Sending TEvChunkWrite");
        ctx.Send(Yard, new NPDisk::TEvChunkWrite(Owner, OwnerRound, ChunkIdx0, ChunkSize - 500,
            new NPDisk::TEvChunkWrite::TNonOwningParts(ChunkWriteParts.Get(), 1), (void*)42, false, 1));
        break;
    case 170:
        TEST_RESPONSE(EvChunkWriteResult, ERROR);
        VERBOSE_COUT(" Sending TEvChunkWrite that actually does the thing");
        ctx.Send(Yard, new NPDisk::TEvChunkWrite(Owner, OwnerRound, ChunkIdx0, ChunkWriteData.size(),
            new NPDisk::TEvChunkWrite::TNonOwningParts(ChunkWriteParts.Get(), 1), (void*)42, false, 1));
        break;
    case 180:
        TEST_RESPONSE(EvChunkWriteResult, OK);
        ChunkIdx = LastResponse.ChunkIdx;
        VERBOSE_COUT(" Sending TEvChunkWrite");
        ctx.Send(Yard, new NPDisk::TEvChunkWrite(Owner, OwnerRound, ChunkIdx, ChunkWriteData.size() / 2,
            new NPDisk::TEvChunkWrite::TNonOwningParts(ChunkWriteParts.Get(), 1), (void*)42, false, 1));
        break;
    case 190:
        TEST_RESPONSE(EvChunkWriteResult, OK);
        VERBOSE_COUT(" Sending TEvInit for invalid id");
        ctx.Send(Yard, new NPDisk::TEvYardInit(3, TVDiskID::InvalidId, *PDiskGuid));
        break;
    case 200:
        TEST_RESPONSE(EvYardInitResult, ERROR);
        VERBOSE_COUT("Done");
        SignalDoneEvent();
        break;
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
}

void TTestEmptyLogRead::TestFSM(const TActorContext &ctx) {
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        break;
    case 10:
        TEST_RESPONSE(EvYardInitResult, OK);
        Owner = LastResponse.Owner;
        OwnerRound = LastResponse.OwnerRound;
        VERBOSE_COUT(" Sending TEvLogRead");
        ctx.Send(Yard, new NPDisk::TEvReadLog(Owner, OwnerRound));
        break;
    case 20:
        TEST_RESPONSE(EvReadLogResult, OK);
        ASSERT_YTHROW(LastResponse.LogRecords.size() == 0,
            "Unexpected LogRecords size == " << LastResponse.LogRecords.size());
        ASSERT_YTHROW(LastResponse.IsEndOfLog,
            "Unexpected IsEndOfLog = " << (int)LastResponse.IsEndOfLog);

        VERBOSE_COUT("Done");
        SignalDoneEvent();
        break;
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
}


void TTestChunkWriteReadWhole::TestFSM(const TActorContext &ctx) {
    TRcBuf data2(TString("testdata2"));
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        break;
    case 10:
    {
        TEST_RESPONSE(EvYardInitResult, OK);
        Owner = LastResponse.Owner;
        OwnerRound = LastResponse.OwnerRound;
        ChunkSize = LastResponse.ChunkSize;
        ctx.Send(Yard, new NPDisk::TEvChunkReserve(Owner, OwnerRound, 1));
        break;
    }
    case 20:
    {
        TEST_RESPONSE(EvChunkReserveResult, OK);
        ASSERT_YTHROW(LastResponse.ChunkIds.size() == 1,
            "Unexpected ChunkIds.size() == " << LastResponse.ChunkIds.size());
        ChunkIdx = LastResponse.ChunkIds[0];
        VERBOSE_COUT(" Sending TEvChunkWrite");
        ChunkWriteParts.Reset(new NPDisk::TEvChunkWrite::TPart[1]);
        ChunkWriteData = PrepareData(ChunkSize);
        ChunkWriteParts[0].Data = ChunkWriteData.data();
        ChunkWriteParts[0].Size = (ui32)ChunkWriteData.size();
        ctx.Send(Yard, new NPDisk::TEvChunkWrite(Owner, OwnerRound, ChunkIdx, 0,
            new NPDisk::TEvChunkWrite::TNonOwningParts(ChunkWriteParts.Get(), 1), (void*)42, false, 1));
        break;
    }
    case 30:
    {
        TEST_RESPONSE(EvChunkWriteResult, OK);
        ASSERT_YTHROW(LastResponse.Cookie == (void*)42, "Unexpected cookie=" << LastResponse.Cookie);
        VERBOSE_COUT(" Sending TEvLog to commit");
        NPDisk::TCommitRecord commitRecord;
        commitRecord.CommitChunks.push_back(ChunkIdx);
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, commitRecord, data2, TLsnSeg(1, 1), (void*)43));
        break;
    }
    case 40:
        TEST_RESPONSE(EvLogResult, OK);
        VERBOSE_COUT(" Sending TEvChunkRead");
        ctx.Send(Yard, new NPDisk::TEvChunkRead(Owner, OwnerRound, ChunkIdx, 0, ChunkSize, 1, nullptr));
        break;
    case 50:
    {
        TEST_RESPONSE(EvChunkReadResult, OK);
        TString expectedData = ChunkWriteData;
        TEST_DATA_EQUALS(LastResponse.Data.ToString(), expectedData);

        VERBOSE_COUT("Done");
        SignalDoneEvent();
        break;
    }
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
}

void TTestChunkWrite20Read02::TestFSM(const TActorContext &ctx) {
    TString data2("testdata2");
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        break;
    case 10:
    {
        TEST_RESPONSE(EvYardInitResult, OK);
        Owner = LastResponse.Owner;
        OwnerRound = LastResponse.OwnerRound;
        BlockSize = LastResponse.AppendBlockSize;
        VERBOSE_COUT(" Sending TEvChunkReserve");
        ctx.Send(Yard, new NPDisk::TEvChunkReserve(Owner, OwnerRound, 1));
        break;
    }
    case 20:
    {
        TEST_RESPONSE(EvChunkReserveResult, OK);
        ASSERT_YTHROW(LastResponse.ChunkIds.size() == 1,
            "Unexpected ChunkIds.size() == " << LastResponse.ChunkIds.size());
        ReservedChunks = LastResponse.ChunkIds;
        ChunkIdx = ReservedChunks[0];
        VERBOSE_COUT(" Sending TEvLog to commit Chunk " << ChunkIdx);
        for (ui32 i = 0; i < ReservedChunks.size(); ++i) {
            VERBOSE_COUT("  id = " << ReservedChunks[i]);
        }
        CommitData = TRcBuf::Uninitialized(sizeof(ui32) * ReservedChunks.size());
        memcpy((void*)CommitData.data(), &(ReservedChunks[0]), sizeof(ui32) * ReservedChunks.size());
        NPDisk::TCommitRecord commitRecord;
        commitRecord.CommitChunks = ReservedChunks;
        commitRecord.IsStartingPoint = false;
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, commitRecord, CommitData, TLsnSeg(1, 1),
                    (void*)43));
        break;
    }
    case 30:
    {
        TEST_RESPONSE(EvLogResult, OK);
        VERBOSE_COUT(" Sending TEvChunkWrite");
        ChunkWriteParts.Reset(new NPDisk::TEvChunkWrite::TPart[1]);
        ChunkWriteData = PrepareData(BlockSize);
        ChunkWriteParts[0].Data = ChunkWriteData.data();
        ChunkWriteParts[0].Size = (ui32)ChunkWriteData.size();
        ctx.Send(Yard, new NPDisk::TEvChunkWrite(Owner, OwnerRound, ChunkIdx, BlockSize * 3,
            new NPDisk::TEvChunkWrite::TNonOwningParts(ChunkWriteParts.Get(), 1), (void*)42, true, 1, false));
        break;
    }
    case 40:
    {
        TEST_RESPONSE(EvChunkWriteResult, OK);
        ASSERT_YTHROW(LastResponse.Cookie == (void*)42, "Unexpected cookie=" << LastResponse.Cookie);
        VERBOSE_COUT(" Sending TEvChunkWrite");
        ChunkWriteParts.Reset(new NPDisk::TEvChunkWrite::TPart[1]);
        ChunkWriteParts[0].Data = ChunkWriteData.data();
        ChunkWriteParts[0].Size = (ui32)ChunkWriteData.size();
        ctx.Send(Yard, new NPDisk::TEvChunkWrite(Owner, OwnerRound, ChunkIdx, BlockSize,
            new NPDisk::TEvChunkWrite::TNonOwningParts(ChunkWriteParts.Get(), 1), (void*)42, true, 1, false));
        break;
    }
    case 50:
    {
        TEST_RESPONSE(EvChunkWriteResult, OK);
        ASSERT_YTHROW(LastResponse.Cookie == (void*)42, "Unexpected cookie=" << LastResponse.Cookie);
        VERBOSE_COUT(" Sending TEvChunkRead");
        ctx.Send(Yard, new NPDisk::TEvChunkRead(Owner, OwnerRound, ChunkIdx, BlockSize, BlockSize, 1, nullptr));
        break;
    }
    case 60:
    {
        TEST_RESPONSE(EvChunkReadResult, OK);
        TEST_DATA_EQUALS(LastResponse.Data.ToString(), ChunkWriteData);
        VERBOSE_COUT(" Sending TEvChunkRead with offset# 0 * AppendBlockSize");
        ctx.Send(Yard, new NPDisk::TEvChunkRead(Owner, OwnerRound, ChunkIdx, BlockSize * 3, BlockSize, 1, nullptr));
        break;
    }
    case 70:
    {
        TEST_RESPONSE(EvChunkReadResult, OK);
        TEST_DATA_EQUALS(LastResponse.Data.ToString(), ChunkWriteData);
        VERBOSE_COUT(" Sending TEvChunkRead with offset# 0 size# 3 * AppendBlockSize");
        ctx.Send(Yard, new NPDisk::TEvChunkRead(Owner, OwnerRound, ChunkIdx, BlockSize, BlockSize * 3, 1, nullptr));
        break;
    }
    case 80:
    {
        TEST_RESPONSE(EvChunkReadResult, OK);
        ASSERT_YTHROW(LastResponse.Data.Size() == BlockSize * 3,
            "Unexpected data size=" << LastResponse.Data.Size() << " expected " << BlockSize * 3);
        ASSERT_YTHROW(LastResponse.Data.IsReadable(0, BlockSize),
            "Unexpected !IsReadable offset# 0");
        ASSERT_YTHROW(!LastResponse.Data.IsReadable(BlockSize, BlockSize),
            "Unexpected IsReadable offset# AppendBlockSize");
        ASSERT_YTHROW(LastResponse.Data.IsReadable(BlockSize * 2, BlockSize),
            "Unexpected !IsReadable offset# AppendBlockSize * 2");
        VERBOSE_COUT("Done");
        SignalDoneEvent();
        break;
    }
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
}


void TTestChunkRecommit::TestFSM(const TActorContext &ctx) {
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        break;
    case 10:
    {
        TEST_RESPONSE(EvYardInitResult, OK);
        Owner = LastResponse.Owner;
        OwnerRound = LastResponse.OwnerRound;
        BlockSize = LastResponse.AppendBlockSize;
        ctx.Send(Yard, new NPDisk::TEvChunkReserve(Owner, OwnerRound, 1));
        break;
    }
    case 20:
    {
        TEST_RESPONSE(EvChunkReserveResult, OK);
        ASSERT_YTHROW(LastResponse.ChunkIds.size() == 1,
            "Unexpected ChunkIds.size() == " << LastResponse.ChunkIds.size());
        ChunkIdx = LastResponse.ChunkIds[0];
        VERBOSE_COUT(" Sending TEvChunkWrite");
        ChunkWriteData1 = PrepareData(BlockSize * 2);
        ChunkWriteData2 = PrepareData(BlockSize);
        Commit1Data = TRcBuf(PrepareData(5030));
        Commit2Data = TRcBuf(PrepareData(5030));
        ChunkData = ChunkWriteData1 + ChunkWriteData2;
        ChunkWriteParts.Reset(new NPDisk::TEvChunkWrite::TPart[1]);
        ChunkWriteParts[0].Data = ChunkWriteData1.data();
        ChunkWriteParts[0].Size = (ui32)ChunkWriteData1.size();
        ctx.Send(Yard, new NPDisk::TEvChunkWrite(Owner, OwnerRound, ChunkIdx, 0,
            new NPDisk::TEvChunkWrite::TNonOwningParts(ChunkWriteParts.Get(), 1), (void*)42, false, 1));
        break;
    }
    case 30:
    {
        TEST_RESPONSE(EvChunkWriteResult, OK);
        ASSERT_YTHROW(LastResponse.Cookie == (void*)42, "Unexpected cookie=" << LastResponse.Cookie);
        ChunkIdx = LastResponse.ChunkIdx;
        VERBOSE_COUT(" Sending TEvLog to commit");
        NPDisk::TCommitRecord commitRecord;
        commitRecord.CommitChunks.push_back(ChunkIdx);
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, commitRecord, Commit1Data, TLsnSeg(1, 1),
                    (void*)43));
        break;
    }
    case 40:
        TEST_RESPONSE(EvLogResult, OK);
        ChunkWriteParts.Reset(new NPDisk::TEvChunkWrite::TPart[1]);
        ChunkWriteParts[0].Data = ChunkWriteData2.data();
        ChunkWriteParts[0].Size = (ui32)ChunkWriteData2.size();
        VERBOSE_COUT(" Sending TEvChunkWrite");
        ctx.Send(Yard, new NPDisk::TEvChunkWrite(Owner, OwnerRound, ChunkIdx, (ui32)ChunkWriteData1.size(),
            new NPDisk::TEvChunkWrite::TNonOwningParts(ChunkWriteParts.Get(), 1), (void*)42, false, 1));
        break;
    case 50:
    {
        TEST_RESPONSE(EvChunkWriteResult, OK);
        ASSERT_YTHROW(LastResponse.Cookie == (void*)42, "Unexpected cookie=" << LastResponse.Cookie);
        VERBOSE_COUT(" Sending TEvLog to commit");
        NPDisk::TCommitRecord commitRecord;
        commitRecord.CommitChunks.push_back(ChunkIdx);
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, commitRecord, Commit2Data, TLsnSeg(2, 2),
                    (void*)43));
        break;
    }
    case 60:
        TEST_RESPONSE(EvLogResult, OK);
        VERBOSE_COUT(" Sending TEvChunkRead");
        ctx.Send(Yard, new NPDisk::TEvChunkRead(Owner, OwnerRound, ChunkIdx, 0, (ui32)ChunkData.size(), 1,
                    nullptr));
        break;
    case 70:
        TEST_RESPONSE(EvChunkReadResult, OK);
        TEST_DATA_EQUALS(LastResponse.Data.ToString(), ChunkData);
        VERBOSE_COUT("Done");
        SignalDoneEvent();
        break;
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
}

void TTestChunkRestartRecommit1::TestFSM(const TActorContext &ctx) {
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        break;
    case 10:
        TEST_RESPONSE(EvYardInitResult, OK);
        ASSERT_YTHROW(LastResponse.OwnedChunks.empty(),
            "non-empty OwnedChuns, size=" << (int)LastResponse.OwnedChunks.size());
        Owner = LastResponse.Owner;
        OwnerRound = LastResponse.OwnerRound;
        ChunkWriteData1 = PrepareData(LastResponse.AppendBlockSize);
        ctx.Send(Yard, new NPDisk::TEvChunkReserve(Owner, OwnerRound, 1));
        break;
    case 20:
    {
        TEST_RESPONSE(EvChunkReserveResult, OK);
        ASSERT_YTHROW(LastResponse.ChunkIds.size() == 1,
            "Unexpected ChunkIds.size() == " << LastResponse.ChunkIds.size());
        ChunkIdx = LastResponse.ChunkIds[0];
        Commit1Data = TRcBuf(PrepareData(5030));
        VERBOSE_COUT(" Sending TEvChunkWrite");
        ChunkWriteParts.Reset(new NPDisk::TEvChunkWrite::TPart[1]);
        ChunkWriteParts[0].Data = ChunkWriteData1.data();
        ChunkWriteParts[0].Size = (ui32)ChunkWriteData1.size();
        ctx.Send(Yard, new NPDisk::TEvChunkWrite(Owner, OwnerRound, ChunkIdx, 0,
            new NPDisk::TEvChunkWrite::TNonOwningParts(ChunkWriteParts.Get(), 1), (void*)42, true, 1));
        break;
    }
    case 30:
    {
        TEST_RESPONSE(EvChunkWriteResult, OK);
        ASSERT_YTHROW(LastResponse.Cookie == (void*)42, "Unexpected cookie=" << LastResponse.Cookie);
        VERBOSE_COUT(" Sending TEvLog to commit ChunkIdx=" << ChunkIdx);
        Commit1Data = TRcBuf::Uninitialized(sizeof(ChunkIdx));
        *(ui32*)Commit1Data.data() = ChunkIdx;
        NPDisk::TCommitRecord commitRecord;
        commitRecord.CommitChunks.push_back(ChunkIdx);
        commitRecord.IsStartingPoint = true;
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, commitRecord, Commit1Data, TLsnSeg(1, 1),
            (void*)43));
        break;
    }
    case 40:
        TEST_RESPONSE(EvLogResult, OK);
        VERBOSE_COUT("Done");
        SignalDoneEvent();
        break;
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
}
void TTestChunkRestartRecommit2::TestFSM(const TActorContext &ctx) {
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        break;
    case 10:
    {
        TEST_RESPONSE(EvYardInitResult, OK);
        ASSERT_YTHROW(LastResponse.StartingPoints.size() == 1,
            "Unexpected starting points size =" << LastResponse.StartingPoints.size());
        ASSERT_YTHROW(LastResponse.StartingPoints.begin()->second.Data.size() == sizeof(ChunkIdx),
            "Unexpected starting point size = " << LastResponse.StartingPoints.begin()->second.Data.size());
        ASSERT_YTHROW(LastResponse.OwnedChunks.size() == 1,
            "Unexpected OwnedChuns.size=" << (int)LastResponse.OwnedChunks.size());
        ChunkIdx = *(ui32*)LastResponse.StartingPoints.begin()->second.Data.data();
        ASSERT_YTHROW(LastResponse.OwnedChunks[0] == ChunkIdx,
            "Unexpected OwnedChunks[0] != ChunkIdx, OwnedChunks[0]# "
            << LastResponse.OwnedChunks[0] << " ChunkIdx# " << ChunkIdx);

        Owner = LastResponse.Owner;
        OwnerRound = LastResponse.OwnerRound;
        ChunkWriteData1 = PrepareData(LastResponse.AppendBlockSize);
        ChunkWriteData2 = PrepareData(LastResponse.AppendBlockSize);
        Commit2Data = TRcBuf(PrepareData(5030));
        ChunkData = ChunkWriteData1 + ChunkWriteData2;
        VERBOSE_COUT(" Sending TEvChunkWrite ChunkIdx=" << ChunkIdx);
        ChunkWriteParts.Reset(new NPDisk::TEvChunkWrite::TPart[1]);
        ChunkWriteParts[0].Data = ChunkWriteData2.data();
        ChunkWriteParts[0].Size = (ui32)ChunkWriteData2.size();
        ctx.Send(Yard, new NPDisk::TEvChunkWrite(Owner, OwnerRound, ChunkIdx, (ui32)ChunkWriteData1.size(),
            new NPDisk::TEvChunkWrite::TNonOwningParts(ChunkWriteParts.Get(), 1), (void*)42, false, 1));
        break;
    }
    case 20:
    {
        TEST_RESPONSE(EvChunkWriteResult, OK);
        ASSERT_YTHROW(LastResponse.Cookie == (void*)42, "Unexpected cookie=" << LastResponse.Cookie);
        VERBOSE_COUT(" Sending TEvLog to commit");
        NPDisk::TCommitRecord commitRecord;
        commitRecord.CommitChunks.push_back(ChunkIdx);
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, commitRecord, Commit2Data, TLsnSeg(1, 1),
            (void*)43));
        break;
    }
    case 30:
        TEST_RESPONSE(EvLogResult, OK);
        VERBOSE_COUT(" Sending TEvChunkRead");
        ctx.Send(Yard, new NPDisk::TEvChunkRead(Owner, OwnerRound, ChunkIdx, 0, (ui32)ChunkData.size(), 1,
                    nullptr));
        break;
    case 40:
        TEST_RESPONSE(EvChunkReadResult, OK);
        TEST_DATA_EQUALS(LastResponse.Data.ToString(), ChunkData);
        VERBOSE_COUT("Done");
        SignalDoneEvent();
        break;
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
}

void TTestChunkDelete1::TestFSM(const TActorContext &ctx) {
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        break;
    case 10:
    {
        TEST_RESPONSE(EvYardInitResult, OK);
        Owner = LastResponse.Owner;
        OwnerRound = LastResponse.OwnerRound;
        VERBOSE_COUT(" Sending TEvChunkReserve");
        ctx.Send(Yard, new NPDisk::TEvChunkReserve(Owner, OwnerRound, 4));
        break;
    case 20:
        TEST_RESPONSE(EvChunkReserveResult, OK);
        ASSERT_YTHROW(LastResponse.ChunkIds.size() == 4,
            "Unexpected ChunkIds.size() == " << LastResponse.ChunkIds.size());
        ReservedChunks = LastResponse.ChunkIds;
        VERBOSE_COUT(" Sending TEvLog to commit Chunks");
        for (ui32 i = 0; i < ReservedChunks.size(); ++i) {
            VERBOSE_COUT("  id = " << ReservedChunks[i]);
        }
        CommitData = TRcBuf::Uninitialized(sizeof(ui32) * ReservedChunks.size());
        memcpy((void*)CommitData.data(), &(ReservedChunks[0]), sizeof(ui32) * ReservedChunks.size());
        NPDisk::TCommitRecord commitRecord;
        commitRecord.CommitChunks = ReservedChunks;
        commitRecord.IsStartingPoint = true;
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, commitRecord, CommitData, TLsnSeg(1, 1),
                    (void*)43));
        break;
    }
    case 30:
    {
        TEST_RESPONSE(EvLogResult, OK);
        VERBOSE_COUT(" Sending TEvLog to commit Chunks");
        CommitData = TRcBuf::Uninitialized(sizeof(ui32) * (ReservedChunks.size() - 3));
        memcpy((void*)CommitData.data(), &(ReservedChunks[3]), sizeof(ui32) * (ReservedChunks.size()-3));
        NPDisk::TCommitRecord commitRecord;
        for (int i = 0; i < 3; ++i) {
            commitRecord.DeleteChunks.push_back(ReservedChunks[i]);
        }
        commitRecord.IsStartingPoint = true;
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, commitRecord, CommitData, TLsnSeg(2, 2),
                    (void*)43));
        break;
    }
    case 40:
        TEST_RESPONSE(EvLogResult, OK);
        VERBOSE_COUT("Done");
        SignalDoneEvent();
        break;
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
}

void TTestChunkDelete2::TestFSM(const TActorContext &ctx) {
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        break;
    case 10:
    {
        TEST_RESPONSE(EvYardInitResult, OK);
        ASSERT_YTHROW(LastResponse.StartingPoints.size() == 1,
            "Unexpected starting points size =" << LastResponse.StartingPoints.size());
        ASSERT_YTHROW(LastResponse.StartingPoints.begin()->second.Data.size() == sizeof(ui32),
            "Unexpected data size = " << LastResponse.StartingPoints.begin()->second.Data.size());
        ReservedChunks.resize(3);
        for (ui32 i = 0; i < ReservedChunks.size(); ++i) {
            ReservedChunks[i] = ((ui32*)LastResponse.StartingPoints.begin()->second.Data.data())[i];
        }
        Owner = LastResponse.Owner;
        OwnerRound = LastResponse.OwnerRound;
        VERBOSE_COUT(" Sending TEvChunkReserve");
        ctx.Send(Yard, new NPDisk::TEvChunkReserve(Owner, OwnerRound, 3));
        break;
    }
    case 20:
        TEST_RESPONSE(EvChunkReserveResult, OK);
        VERBOSE_COUT(" Sending TEvChunkReserve");
        ctx.Send(Yard, new NPDisk::TEvChunkReserve(Owner, OwnerRound, 1000));
        break;
    case 30:
        TEST_RESPONSE(EvChunkReserveResult, OUT_OF_SPACE);
        VERBOSE_COUT("Done");
        SignalDoneEvent();
        break;
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
}

void TTestChunkForget1::TestFSM(const TActorContext &ctx) {
    constexpr ui32 toReserve = 535;
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        break;
    case 10:
    {
        TEST_RESPONSE(EvYardInitResult, OK);
        Owner = LastResponse.Owner;
        OwnerRound = LastResponse.OwnerRound;
        VERBOSE_COUT(" Sending TEvChunkReserve");
        ctx.Send(Yard, new NPDisk::TEvChunkReserve(Owner, OwnerRound, toReserve));
        break;
    case 20:
        TEST_RESPONSE(EvChunkReserveResult, OK);
        ASSERT_YTHROW(LastResponse.ChunkIds.size() == toReserve,
            "Unexpected ChunkIds.size() == " << LastResponse.ChunkIds.size());
        ReservedChunks = LastResponse.ChunkIds;
        VERBOSE_COUT(" Sending TEvLog to commit Chunks");
        for (ui32 i = 0; i < ReservedChunks.size(); ++i) {
            VERBOSE_COUT("  id = " << ReservedChunks[i]);
        }
        CommitData = TRcBuf::Uninitialized(sizeof(ui32) * ReservedChunks.size());
        memcpy((void*)CommitData.data(), &(ReservedChunks[0]), sizeof(ui32) * ReservedChunks.size());
        NPDisk::TCommitRecord commitRecord;
        commitRecord.CommitChunks = ReservedChunks;
        commitRecord.IsStartingPoint = true;
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, commitRecord, CommitData, TLsnSeg(1, 1),
                    (void*)43));
        break;
    }
    case 30:
        TEST_RESPONSE(EvLogResult, OK);
        VERBOSE_COUT(" Sending TEvReserve to make sure no more chunks can be reserved");
        ctx.Send(Yard, new NPDisk::TEvChunkReserve(Owner, OwnerRound, toReserve));
        break;
    case 40:
        TEST_RESPONSE(EvChunkReserveResult, OUT_OF_SPACE);
        VERBOSE_COUT(" Sending TEvLog to delete Chunks");
        CommitData = TRcBuf();
    {
        NPDisk::TCommitRecord commitRecord;
        commitRecord.DeleteChunks = ReservedChunks;
        commitRecord.IsStartingPoint = true;
        commitRecord.DeleteToDecommitted = true;
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, commitRecord, CommitData, TLsnSeg(2, 2),
                    (void*)43));
        break;
    }
    case 50:
        TEST_RESPONSE(EvLogResult, OK);
        VERBOSE_COUT(" Sending TEvReserve to make sure no more chunks can be reserved");
        ctx.Send(Yard, new NPDisk::TEvChunkReserve(Owner, OwnerRound, toReserve));
        break;
    case 60:
        TEST_RESPONSE(EvChunkReserveResult, OUT_OF_SPACE);
        ctx.Send(Yard, new NPDisk::TEvChunkForget(Owner, OwnerRound, ReservedChunks));
        break;
    case 70:
        TEST_RESPONSE(EvChunkForgetResult, OK);
        VERBOSE_COUT(" Sending TEvReserve to make sure some chunks can be reserved");
        ctx.Send(Yard, new NPDisk::TEvChunkReserve(Owner, OwnerRound, toReserve));
        break;
    case 80:
        TEST_RESPONSE(EvChunkReserveResult, OK);
        VERBOSE_COUT("Done");
        SignalDoneEvent();
        break;
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
}

void TTestInitStartingPoints::TestFSM(const TActorContext &ctx) {
    TString data("testdata");
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        break;
    case 10:
        TEST_RESPONSE(EvYardInitResult, OK);
        ASSERT_YTHROW(LastResponse.StartingPoints.size() == 1,
            "Unexpected number of starting points in response (" << LastResponse.StartingPoints.size() << ")");
        TEST_DATA_EQUALS(LastResponse.StartingPoints.begin()->second.Data.ExtractUnderlyingContainerOrCopy<TString>(), data);
        VERBOSE_COUT("Done");
        SignalDoneEvent();
        break;
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
};

void TTestChunkReserve::TestFSM(const TActorContext &ctx) {
    TString data("testdata");
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        break;
    case 10:
        TEST_RESPONSE(EvYardInitResult, OK);
        Owner = LastResponse.Owner;
        OwnerRound = LastResponse.OwnerRound;
        VERBOSE_COUT(" Sending TEvChunkReserve");
        ctx.Send(Yard, new NPDisk::TEvChunkReserve(Owner, OwnerRound, 3));
        break;
    case 20:
        TEST_RESPONSE(EvChunkReserveResult, OK);
        ASSERT_YTHROW(LastResponse.ChunkIds.size() == 3,
            "Unexpected ChunkIds.size() == " << LastResponse.ChunkIds.size());
        VERBOSE_COUT("Done");
        SignalDoneEvent();
        break;
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
};

void TTestChunkLock::TestFSM(const TActorContext &ctx) {
    using EFrom = NPDisk::TEvChunkLock::ELockFrom;
    using TColor = NKikimrBlobStorage::TPDiskSpaceColor;
    TString data("testdata");
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        break;
    case 10:
        TEST_RESPONSE(EvYardInitResult, OK);
        Owner = LastResponse.Owner;
        OwnerRound = LastResponse.OwnerRound;
        VERBOSE_COUT(" Sending TEvChunkLock from LOG");
        ctx.Send(Yard, new NPDisk::TEvChunkLock(EFrom::LOG, 5, TColor::GREEN));
        break;
    case 20:
        TEST_RESPONSE(EvChunkLockResult, OK);
        ASSERT_YTHROW(!LastResponse.ChunkIds.empty(), "Didn't lock anything");
        VERBOSE_COUT(" Sending TEvChunkLock from PERSONAL_QUOTA by count");
        ctx.Send(Yard, new NPDisk::TEvChunkLock(EFrom::PERSONAL_QUOTA, Owner, 5, TColor::GREEN));
        break;
    case 30:
        TEST_RESPONSE(EvChunkLockResult, OK);
        ASSERT_YTHROW(LastResponse.ChunkIds.size() == 5,
            "Unexpected LockedChunks.size() == " << LastResponse.ChunkIds.size());
        VERBOSE_COUT(" Sending TEvChunkLock from PERSONAL_QUOTA");
        ctx.Send(Yard, new NPDisk::TEvChunkLock(EFrom::PERSONAL_QUOTA, Owner, 0, TColor::RED));
        break;
    case 40:
        TEST_RESPONSE(EvChunkLockResult, OK);
        ASSERT_YTHROW(!LastResponse.ChunkIds.empty(), "Didn't lock anything");
        ctx.Send(Yard, new NPDisk::TEvChunkReserve(Owner, OwnerRound, 10));
        break;
    case 50:
        TEST_RESPONSE(EvChunkReserveResult, OUT_OF_SPACE);
        VERBOSE_COUT("Done");
        SignalDoneEvent();
        break;
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
};

void TTestChunkUnlock::TestFSM(const TActorContext &ctx) {
    using EFrom = NPDisk::TEvChunkLock::ELockFrom;
    using TColor = NKikimrBlobStorage::TPDiskSpaceColor;
    TString data("testdata");
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        break;
    case 10:
        TEST_RESPONSE(EvYardInitResult, OK);
        Owner = LastResponse.Owner;
        OwnerRound = LastResponse.OwnerRound;
        VERBOSE_COUT(" Sending TEvChunkLock from LOG");
        ctx.Send(Yard, new NPDisk::TEvChunkLock(EFrom::LOG, 0, TColor::YELLOW));
        break;
    case 20:
        TEST_RESPONSE(EvChunkLockResult, OK);
        LockedNumLog = LastResponse.ChunkIds.size();
        ASSERT_YTHROW(LockedNumLog, "Didn't lock anything");
        VERBOSE_COUT(" Sending TEvChunkLock from PERSONAL_QUOTA");
        ctx.Send(Yard, new NPDisk::TEvChunkLock(EFrom::PERSONAL_QUOTA, Owner, 5, TColor::GREEN));
        break;
    case 30:
        TEST_RESPONSE(EvChunkLockResult, OK);
        LockedNumPersonal = LastResponse.ChunkIds.size();
        ASSERT_YTHROW(LockedNumPersonal, "Didn't lock anything");
        ctx.Send(Yard, new NPDisk::TEvChunkUnlock(EFrom::LOG));
        break;
    case 40:
        TEST_RESPONSE(EvChunkUnlockResult, OK);
        ASSERT_YTHROW(LastResponse.UnlockedChunks == LockedNumLog, "Expected" << LockedNumLog <<
            " unlocked chunks, got " << LastResponse.UnlockedChunks);
        ctx.Send(Yard, new NPDisk::TEvChunkUnlock(EFrom::PERSONAL_QUOTA, Owner));
        break;
    case 50:
        TEST_RESPONSE(EvChunkUnlockResult, OK);
        ASSERT_YTHROW(LastResponse.UnlockedChunks == LockedNumPersonal, "Expected" << LockedNumPersonal <<
            " unlocked chunks, got " << LastResponse.UnlockedChunks);
        ctx.Send(Yard, new NPDisk::TEvChunkReserve(Owner, OwnerRound, 10));
        break;
    case 60:
        TEST_RESPONSE(EvChunkReserveResult, OK);
        VERBOSE_COUT("Done");
        SignalDoneEvent();
        break;
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
};

void TTestChunkUnlockHarakiri::TestFSM(const TActorContext &ctx) {
    using EFrom = NPDisk::TEvChunkLock::ELockFrom;
    using TColor = NKikimrBlobStorage::TPDiskSpaceColor;
    TString data("testdata");
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        break;
    case 10:
        TEST_RESPONSE(EvYardInitResult, OK);
        Owner = LastResponse.Owner;
        OwnerRound = LastResponse.OwnerRound;
        VERBOSE_COUT(" Sending TEvChunkLock from PERSONAL_QUOTA");
        ctx.Send(Yard, new NPDisk::TEvChunkLock(EFrom::PERSONAL_QUOTA, Owner, 0, TColor::RED));
        break;
    case 20:
        TEST_RESPONSE(EvChunkLockResult, OK);
        VERBOSE_COUT(" Checking space to get TotalFree");
        ASSERT_YTHROW(LastResponse.ChunkIds.size(), "Didn't lock anything");
        ctx.Send(Yard, new NPDisk::TEvCheckSpace(Owner, OwnerRound));
        break;
    case 30:
        TEST_RESPONSE(EvCheckSpaceResult, OK);
        TotalFree = LastResponse.TotalChunks;
        VERBOSE_COUT(" Sending TEvHarakiri");
        ctx.Send(Yard, new NPDisk::TEvHarakiri(Owner, OwnerRound));
        break;
    case 40:
        TEST_RESPONSE(EvHarakiriResult, OK);
        VERBOSE_COUT(" Sending second TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(3, VDiskID, *PDiskGuid));
        break;
    case 50:
        TEST_RESPONSE(EvYardInitResult, OK);
        Owner = LastResponse.Owner;
        OwnerRound = LastResponse.OwnerRound;
        VERBOSE_COUT(" Checking space");
        ctx.Send(Yard, new NPDisk::TEvCheckSpace(Owner, OwnerRound));
        break;
    case 60:
        TEST_RESPONSE(EvCheckSpaceResult, OK);
        ASSERT_YTHROW(TotalFree = LastResponse.TotalChunks, "Didn't unlock chunks after Harakiri, expected " <<
            TotalFree << ", got " << LastResponse.TotalChunks);
        VERBOSE_COUT("Done");
        SignalDoneEvent();
        break;
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
};

void TTestChunkUnlockRestart::TestFSM(const TActorContext &ctx) {
    using EFrom = NPDisk::TEvChunkLock::ELockFrom;
    using TColor = NKikimrBlobStorage::TPDiskSpaceColor;
    TString data("testdata");
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        WhiteboardID = NNodeWhiteboard::MakeNodeWhiteboardServiceId(SelfId().NodeId());
        ctx.ExecutorThread.ActorSystem->RegisterLocalService(WhiteboardID, SelfId());
        NodeWardenId = MakeBlobStorageNodeWardenID(SelfId().NodeId());
        ctx.ExecutorThread.ActorSystem->RegisterLocalService(NodeWardenId, SelfId());
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid, TActorId(), SelfId()));
        break;
    case 10:
        TEST_RESPONSE(EvYardInitResult, OK);
        VERBOSE_COUT(" Sending TEvChunkLock from PERSONAL_QUOTA");
        ctx.Send(Yard, new NPDisk::TEvChunkLock(EFrom::PERSONAL_QUOTA, Owner, 0, TColor::RED));
        break;
    case 20:
        TEST_RESPONSE(EvChunkLockResult, OK);
        ASSERT_YTHROW(LastResponse.ChunkIds.size(), "Didn't lock anything");
        if (!LastResponse.whiteboardPDiskResult || !LastResponse.whiteboardPDiskResult->Record.HasPDiskId()) {
            VERBOSE_COUT(" Whiteboard didn't return PDiskId, test terminated");
            VERBOSE_COUT("Terminated");
            SignalDoneEvent();
            break;
        }
        ctx.Send(NodeWardenId, new TEvBlobStorage::TEvAskWardenRestartPDisk(LastResponse.whiteboardPDiskResult->Record.GetPDiskId()));
        break;
    case 30:
        TEST_RESPONSE(EvHarakiri, OK);
        ctx.Send(Yard, new NPDisk::TEvChunkReserve(Owner, OwnerRound, 1));
        break;
    case 40:
        TEST_RESPONSE(EvChunkReserveResult, OK);
        VERBOSE_COUT("Done");
        SignalDoneEvent();
        break;
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
};

void TTestCheckSpace::TestFSM(const TActorContext &ctx) {
    TString data("testdata");
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        break;
    case 10:
        TEST_RESPONSE(EvYardInitResult, OK);
        Owner = LastResponse.Owner;
        OwnerRound = LastResponse.OwnerRound;
        VERBOSE_COUT(" Sending TEvCheckSpace");
        ctx.Send(Yard, new NPDisk::TEvCheckSpace(Owner, OwnerRound));
        break;
    case 20:
        TEST_RESPONSE(EvCheckSpaceResult, OK);
        VERBOSE_COUT("Free# " << LastResponse.FreeChunks
            << " Total# " << LastResponse.TotalChunks
            << " Owned# " << LastResponse.UsedChunks);
        UNIT_ASSERT(LastResponse.UsedChunks == 0);
        UNIT_ASSERT(LastResponse.FreeChunks == 585);
        UNIT_ASSERT(LastResponse.TotalChunks == 585);
        VERBOSE_COUT(" Sending TEvChunkReserve");
        ctx.Send(Yard, new NPDisk::TEvChunkReserve(Owner, OwnerRound, 3));
        break;
    case 30:
        TEST_RESPONSE(EvChunkReserveResult, OK);
        ASSERT_YTHROW(LastResponse.ChunkIds.size() == 3,
            "Unexpected ChunkIds.size() == " << LastResponse.ChunkIds.size());
        VERBOSE_COUT(" Sending TEvCheckSpace");
        ctx.Send(Yard, new NPDisk::TEvCheckSpace(Owner, OwnerRound));
        break;
    case 40:
        TEST_RESPONSE(EvCheckSpaceResult, OK);
        VERBOSE_COUT("Free# " << LastResponse.FreeChunks
            << " Total# " << LastResponse.TotalChunks
            << " Owned# " << LastResponse.UsedChunks);
        UNIT_ASSERT(LastResponse.UsedChunks == 3);
        UNIT_ASSERT(LastResponse.FreeChunks == 582);
        UNIT_ASSERT(LastResponse.TotalChunks == 585);
        VERBOSE_COUT("Done");
        SignalDoneEvent();
        break;
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
};

void TTestHttpInfo::TestFSM(const TActorContext &ctx) {
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        break;
    case 10:
        TEST_RESPONSE(EvYardInitResult, OK);
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT("Sending TEvHttpInfo");
        ctx.Send(Yard, new NMon::TEvHttpInfo(MonService2HttpRequest));
        break;
    case 20:
        ASSERT_YTHROW(LastResponse.HttpResult, "Recived unexpected event");
        ASSERT_YTHROW(LastResponse.HttpResult->Type() == NActors::NMon::HttpInfoRes,
            "Unexpected message");
        ASSERT_YTHROW(LastResponse.HttpResult->Answer.Contains("VDiskId"),
            "Http answer dosn't contain expected word (VDiskId)");
        VERBOSE_COUT("Done");
        LastResponse.Status = NKikimrProto::OK;
        SignalDoneEvent();
        break;
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
};

void TTestHttpInfoFileDoesntExist::TestFSM(const TActorContext &ctx) {
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        break;
    case 10:
        TEST_RESPONSE(EvYardInitResult, CORRUPTED);
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::CORRUPTED, StatusToString(LastResponse.Status));
        VERBOSE_COUT("Sending TEvHttpInfo");
        ctx.Send(Yard, new NMon::TEvHttpInfo(MonService2HttpRequest));
        break;
    case 20:
        ASSERT_YTHROW(LastResponse.HttpResult, "Recived unexpected event");
        ASSERT_YTHROW(LastResponse.HttpResult->Type() == NActors::NMon::HttpInfoRes,
            "Unexpected message");
        ASSERT_YTHROW(LastResponse.HttpResult->Answer.Contains("VDiskId"),
            "Http answer dosn't contain expected word (VDiskId)");
        VERBOSE_COUT("Done");
        LastResponse.Status = NKikimrProto::OK;
        SignalDoneEvent();
        break;
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
};

void TTestBootingState::TestFSM(const TActorContext &ctx) {
    switch (TestStep) {
    case 0:
        VERBOSE_COUT("Test step " << TestStep);
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT("Sending " << HttpRequestsCount << " of TEvHttpInfo requests");
        for (ui32 i = 0; i < HttpRequestsCount; ++i) {
            ctx.Send(Yard, new NMon::TEvHttpInfo(MonService2HttpRequest));
        }
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        TestStep += 10;
        break;
    case 10:
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        if (LastResponse.HttpResult && LastResponse.HttpResult->Type() == NActors::NMon::HttpInfoRes) {
            ++AnsweredRequests;
            if (LastResponse.HttpResult->Answer.Contains(">Booting<")) {
                ++BootingAnsweredRequests;
            }
            if (LastResponse.HttpResult->Answer.Contains(">OK<")) {
                ++OKAnsweredRequests;
            }
            if (LastResponse.HttpResult->Answer.Contains(">Error<")) {
                ++ErrorAnsweredRequests;
            }
        } else if (LastResponse.EventType == TEvBlobStorage::EvYardInitResult) {
            EvYardAnswered = true;
        } else {
            ASSERT_YTHROW(false, "Unexpecter response type");
        }
        VERBOSE_COUT("EvYardAnswered# " << EvYardAnswered << " EvHttpInfo, answered " << AnsweredRequests
                << " of " << HttpRequestsCount << " requests answered: "
                << BootingAnsweredRequests << " requests answered in Booting state "
                << OKAnsweredRequests << " requests answered in OK state "
                << ErrorAnsweredRequests << " requests answered in Error state" << Endl);
        if (AnsweredRequests == HttpRequestsCount && EvYardAnswered) {
            VERBOSE_COUT("Done");
            SignalDoneEvent();
        }
        break;
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
};

void TTestWhiteboard::TestFSM(const TActorContext &ctx) {
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
    {
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        TActorId whiteboardID = NNodeWhiteboard::MakeNodeWhiteboardServiceId(SelfId().NodeId());
        ctx.ExecutorThread.ActorSystem->RegisterLocalService(whiteboardID, SelfId());
        TActorId nodeWardenId = MakeBlobStorageNodeWardenID(SelfId().NodeId());
        ctx.ExecutorThread.ActorSystem->RegisterLocalService(nodeWardenId, SelfId());
        for (int owner = 0; owner < ExpectedOwnerCount; ++owner) {
            ctx.Send(Yard, new NPDisk::TEvYardInit(2, TVDiskID(TGroupId::Zero(), 0, 0, 0, owner), *PDiskGuid, TActorId(), SelfId()));
        }
        TestStep += 10;
        break;
    }
    case 10:
        ReceiveEvent();
        if (IsPDiskResultReceived && RemainingVDiskResults == 0
            && IsDiskMetricsResultReceived && IsPDiskLightsResultReceived) {
            LastResponse.Status = NKikimrProto::OK;
            SignalDoneEvent();
            VERBOSE_COUT("Done");
            TestStep += 10;
        }
        break;
    case 20:
        break;
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
};

void TTestWhiteboard::ReceiveEvent() {
    if (LastResponse.whiteboardPDiskResult) {
        ASSERT_YTHROW(LastResponse.whiteboardPDiskResult->Type() ==
                NNodeWhiteboard::TEvWhiteboard::EvPDiskStateUpdate, "Unexpected message");
        if (LastResponse.whiteboardPDiskResult->Record.HasPDiskId()
                && LastResponse.whiteboardPDiskResult->Record.HasAvailableSize()
                && LastResponse.whiteboardPDiskResult->Record.HasTotalSize()
                && LastResponse.whiteboardPDiskResult->Record.HasState()) {
            IsPDiskResultReceived = true;
            VERBOSE_COUT("Received PDiskResult");
        } else if (LastResponse.whiteboardPDiskResult->Record.HasPDiskId()
                && LastResponse.whiteboardPDiskResult->Record.HasRealtime()
                && LastResponse.whiteboardPDiskResult->Record.HasDevice()) {
            IsPDiskLightsResultReceived = true;
            VERBOSE_COUT("Received PDiskLightsResult");
        }
    }

    if (LastResponse.whiteboardVDiskResult && LastResponse.whiteboardVDiskResult->Record.HasAllocatedSize()) {
        ASSERT_YTHROW(LastResponse.whiteboardVDiskResult->Type() ==
                NNodeWhiteboard::TEvWhiteboard::EvVDiskStateUpdate, "Unexpected message");
        const TVDiskID vDiskID = VDiskIDFromVDiskID(LastResponse.whiteboardVDiskResult->Record.GetVDiskId());

        ui32 vDisk = vDiskID.VDisk;
        VERBOSE_COUT("VDiskResult received, VDiskId# " << vDiskID << " vDisk#" << vDisk);
        if (--RemainingVDiskResults == 0) {
            VERBOSE_COUT("Received all VDiskResults");
        }
    }
    if (LastResponse.whiteboardDiskMetricsResult) {
        ASSERT_YTHROW(LastResponse.whiteboardDiskMetricsResult->Type() ==
            TEvBlobStorage::EvControllerUpdateDiskStatus, "Unexpected message");
        IsDiskMetricsResultReceived = true;
        VERBOSE_COUT("Received DiskMetricsResult");
    }
}

void TTestFirstRecordToKeepWriteAB::TestFSM(const TActorContext &ctx) {
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        break;
    case 10:
        TEST_RESPONSE(EvYardInitResult, OK);
        Owner = LastResponse.Owner;
        OwnerRound = LastResponse.OwnerRound;
        VERBOSE_COUT(" Sending TEvLog");
        Data = TRcBuf(PrepareData(67890));
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 7, Data, TLsnSeg(Lsn, Lsn), (void*)456));
        break;
    case 20:
    {
        TEST_RESPONSE(EvLogResult, OK);
        VERBOSE_COUT(" Sending TEvLog");
        Data = TRcBuf(PrepareData(100500));
        NPDisk::TCommitRecord commitRecord;
        ++Lsn;
        commitRecord.FirstLsnToKeep = Lsn;
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 7, commitRecord, Data, TLsnSeg(Lsn, Lsn), (void*)567));
        break;
    }
    case 30:
        TEST_RESPONSE(EvLogResult, OK);
        VERBOSE_COUT("Done");
        SignalDoneEvent();
        break;
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
}

void TTestFirstRecordToKeepReadB::TestFSM(const TActorContext &ctx) {
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        break;
    case 10:
        TEST_RESPONSE(EvYardInitResult, OK);
        Owner = LastResponse.Owner;
        OwnerRound = LastResponse.OwnerRound;
        VERBOSE_COUT(" Sending TEvLogRead");
        ctx.Send(Yard, new NPDisk::TEvReadLog(Owner, OwnerRound));
        break;
    case 20:
        Data = TRcBuf(PrepareData(100500));
        TEST_RESPONSE(EvReadLogResult, OK);
        ASSERT_YTHROW(LastResponse.LogRecords.size() == 1,
            "Unexpected LogRecords size == " << LastResponse.LogRecords.size());
        TEST_LOG_RECORD(LastResponse.LogRecords[0], 2, 7, Data.ExtractUnderlyingContainerOrCopy<TString>());
        VERBOSE_COUT("Done");
        SignalDoneEvent();
        break;
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
}

void TTestLotsOfTinyAsyncLogLatency::TestFSM(const TActorContext &ctx) {
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        Data = TRcBuf(PrepareData(1500));
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        break;
    case 10:
    {
        TEST_RESPONSE(EvYardInitResult, OK);
        Owner = LastResponse.Owner;
        OwnerRound = LastResponse.OwnerRound;
        VERBOSE_COUT(" Sending TEvLog messages");
        TotalDataSize = (ENABLE_VALGRIND_REQUESTS | IS_SLOW_MACHINE) ? (8 << 20) : (128 << 20);
        MessagesToSend = TotalDataSize / Data.size();
        StartTime = Now();
        for (ui32 messageIdx = 0; messageIdx < MessagesToSend; ++messageIdx) {
            ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 7, Data,
                TLsnSeg((ui64)messageIdx+1, (ui64)messageIdx+1), (void*)456));
        }
        Responses = 0;
        PreviousTime = Now();
        DurationSum = 0.0;
        TotalResponses = 0.0;
        break;
    }
    case 20:
    {
        TEST_RESPONSE(EvLogResult, OK);
        TInstant currentTime = Now();
        double duration = (currentTime - PreviousTime).SecondsFloat();
        double durationMs = duration * 1000.0;
        VERBOSE_COUT(" durationMs = " << durationMs);
        if (durationMs > 50.0) {
            LOW_VERBOSE_COUT("durationMs = " << durationMs);
        }
        Durations.push(duration);
        PreviousTime = currentTime;
        DurationSum += duration;
        TotalResponses += 1.0f;
        Responses += LastResponse.LogResults.size();

        if (Responses < MessagesToSend) {
            TestStep -= 10;
            break;
        }

        if (!ENABLE_VALGRIND_REQUESTS && ENABLE_SPEED_TESTS) {
            double meanDuration = DurationSum / TotalResponses;
            double avgDurationMs = meanDuration * 1000.0;
            double timeMultiplier = IS_SLOW_MACHINE ? 8.0 : 2.0;
            ASSERT_YTHROW(avgDurationMs < 50.0 * timeMultiplier, "avgDurationMs="
                << avgDurationMs << " TotalResponses=" << TotalResponses);
            double sumDeltaSq = 0.0;
            double deltaSqCount = 0.0;
            while (!Durations.empty()) {
                double delta = Durations.front() - meanDuration;
                Durations.pop();
                sumDeltaSq += delta * delta;
                deltaSqCount += 1.0;
            }
            double sigmaMs = 0.0;
            if (sumDeltaSq > 0.0) {
                sigmaMs = ::sqrt(sumDeltaSq / deltaSqCount) * 1000.0;
            }
            double plusLimitMs = avgDurationMs;// + 2.0 * sigmaMs;
            ASSERT_YTHROW(plusLimitMs < 50.0 * timeMultiplier, "avgDurationMs=" << avgDurationMs <<
                " TotalResponses=" << TotalResponses << " sigmaMs=" << sigmaMs <<
                " plusLimitMs=" << plusLimitMs);

            double totalDuration = (currentTime - StartTime).SecondsFloat();
            double speed = (double)TotalDataSize / totalDuration / 1024.0 / 1024.0;
            LOW_VERBOSE_COUT("totalDuration=" << totalDuration << " speed=" << speed
                << " avg=" << avgDurationMs << " sigma=" << sigmaMs);
        }

        VERBOSE_COUT("Done");
        SignalDoneEvent();
        break;
    }
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
}


void TTestLogLatency::TestFSM(const TActorContext &ctx) {
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        Data = TRcBuf(PrepareData(500));
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        break;
    case 10:
    {
        TEST_RESPONSE(EvYardInitResult, OK);
        Owner = LastResponse.Owner;
        OwnerRound = LastResponse.OwnerRound;

        TotalDataSize = 32 << 10;
        MessagesToSend = TotalDataSize / Data.size();
        Responses = 0;
        DurationSum = 0.0;
        TotalResponses = 0.0;
        StartTime = Now();
        PreviousTime = Now();
        MessageIdx = 0;

        VERBOSE_COUT(" Sending TEvLog messages");
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 7, Data, TLsnSeg(MessageIdx+1, MessageIdx+1),
            (void*)456));
        break;
    }
    case 20:
    {
        TEST_RESPONSE(EvLogResult, OK);
        TInstant currentTime = Now();
        double duration = (currentTime - PreviousTime).SecondsFloat();
        double durationMs = duration * 1000.0;
        VERBOSE_COUT(" durationMs = " << durationMs);
        if (durationMs > 50.0) {
            LOW_VERBOSE_COUT("durationMs = " << durationMs);
        }
        Durations.push(duration);
        PreviousTime = currentTime;
        DurationSum += duration;
        TotalResponses += 1.0f;
        Responses += LastResponse.LogResults.size();

        if (Responses < MessagesToSend) {
            Sleep(TDuration::MilliSeconds(rand() % 20));
            VERBOSE_COUT(" Sending TEvLog messages");
            ++MessageIdx;
            PreviousTime = Now();
            ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 7, Data, TLsnSeg(MessageIdx+1, MessageIdx+1),
                (void*)456));
            TestStep -= 10;
            break;
        }
        if (!ENABLE_VALGRIND_REQUESTS && ENABLE_SPEED_TESTS) {
            double meanDuration = DurationSum / TotalResponses;
            double avgDurationMs = meanDuration * 1000.0;
            double timeMultiplier = IS_SLOW_MACHINE ? 8.0 : 2.0;
            ASSERT_YTHROW(avgDurationMs < 50.0 * timeMultiplier, "avgDurationMs="
                << avgDurationMs << " TotalResponses=" << TotalResponses);
            double sumDeltaSq = 0.0;
            double deltaSqCount = 0.0;
            while (!Durations.empty()) {
                double delta = Durations.front() - meanDuration;
                Durations.pop();
                sumDeltaSq += delta * delta;
                deltaSqCount += 1.0;
            }
            double sigmaMs = 0.0;
            if (sumDeltaSq > 0.0) {
                sigmaMs = ::sqrt(sumDeltaSq / deltaSqCount) * 1000.0;
            }
            double plusLimitMs = avgDurationMs;// + 2.0 * sigmaMs;
            ASSERT_YTHROW(plusLimitMs < 50.0 * timeMultiplier, "avgDurationMs=" << avgDurationMs <<
                " TotalResponses=" << TotalResponses << " sigmaMs=" << sigmaMs <<
                " plusLimitMs=" << plusLimitMs);

            double totalDuration = (currentTime - StartTime).SecondsFloat();
            double speed = (double)TotalDataSize / totalDuration / 1024.0 / 1024.0;
            LOW_VERBOSE_COUT("totalDuration=" << totalDuration << " speed=" << speed
                << " avg=" << avgDurationMs << " sigma=" << sigmaMs);
        }

        VERBOSE_COUT("Done");
        SignalDoneEvent();
        break;
    }
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
}

void TTestHugeChunkAndLotsOfTinyAsyncLogOrder::TestFSM(const TActorContext &ctx) {
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        Data = TRcBuf(PrepareData(1500));
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        break;
    case 10:
    {
        TEST_RESPONSE(EvYardInitResult, OK);
        Owner = LastResponse.Owner;
        OwnerRound = LastResponse.OwnerRound;
        BlockSize = LastResponse.AppendBlockSize;
        ctx.Send(Yard, new NPDisk::TEvChunkReserve(Owner, OwnerRound, 1));
        break;
    }
    case 20:
    {
        TEST_RESPONSE(EvChunkReserveResult, OK);
        ASSERT_YTHROW(LastResponse.ChunkIds.size() == 1,
            "Unexpected ChunkIds.size() == " << LastResponse.ChunkIds.size());
        ChunkIdx = LastResponse.ChunkIds[0];
        VERBOSE_COUT(" Sending TEvYardControl Pause");
        ctx.Send(Yard, new NPDisk::TEvYardControl(NPDisk::TEvYardControl::ActionPause, nullptr));
        VERBOSE_COUT(" Sending TEvChunkWrite");
        ChunkWriteParts.Reset(new NPDisk::TEvChunkWrite::TPart[1]);
        TotalDataSize = (8 << 20) / BlockSize * BlockSize;
        ChunkWriteData = PrepareData(TotalDataSize);
        ChunkWriteParts[0].Data = ChunkWriteData.data();
        ChunkWriteParts[0].Size = (ui32)ChunkWriteData.size();
        ctx.Send(Yard, new NPDisk::TEvChunkWrite(Owner, OwnerRound, ChunkIdx, 0,
            new NPDisk::TEvChunkWrite::TNonOwningParts(ChunkWriteParts.Get(), 1), (void*)42, false, 6));
        VERBOSE_COUT(" Sending TEvLog messages");
        MessagesToSend = TotalDataSize / Data.size();
        StartTime = Now();
        for (ui32 messageIdx = 0; messageIdx < MessagesToSend; ++messageIdx) {
            ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 7, Data,
                TLsnSeg((ui64)messageIdx+1, (ui64)messageIdx+1), (void*)456));
        }
        VERBOSE_COUT(" Sending TEvYardControl Resume");
        ctx.Send(Yard, new NPDisk::TEvYardControl(NPDisk::TEvYardControl::ActionResume, nullptr));
        PreviousTime = Now();
        DurationSum = 0.0;
        TotalResponses = 0.0;
        Responses = 0;
        break;
    }
    case 30:
        TEST_RESPONSE(EvYardControlResult, OK);
        break;
    case 40:
        TEST_RESPONSE(EvYardControlResult, OK);
        break;
    case 50:
    case 60:
        if (LastResponse.EventType == TEvBlobStorage::EvLogResult) {
            TEST_RESPONSE(EvLogResult, OK);
            TInstant currentTime = Now();
            double duration = (currentTime - PreviousTime).SecondsFloat();
            double durationMs = duration * 1000.0;
            Durations.push(duration);
            PreviousTime = currentTime;
            DurationSum += duration;
            TotalResponses += 1.0f;

            Responses += LastResponse.LogResults.size();
            VERBOSE_COUT(" durationMs# " << durationMs << " responses# " << Responses << " / "
                << MessagesToSend);
            if (durationMs > 50.0) {
                LOW_VERBOSE_COUT("durationMs = " << durationMs);
            }
            if (Responses < MessagesToSend) {
                TestStep -= 10;
                break;
            }
            VERBOSE_COUT(" enough");
            if (!ENABLE_VALGRIND_REQUESTS && ENABLE_SPEED_TESTS) {
                double meanDuration = DurationSum / TotalResponses;
                double avgDurationMs = meanDuration * 1000.0;
                double timeMultiplier = (ENABLE_VALGRIND_REQUESTS | IS_SLOW_MACHINE) ? 8.0 : 2.0;
                ASSERT_YTHROW(avgDurationMs < 50.0 * timeMultiplier, "avgDurationMs=" <<
                    avgDurationMs << " TotalResponses=" << TotalResponses);
                double sumDeltaSq = 0.0;
                double deltaSqCount = 0.0;
                while (!Durations.empty()) {
                    double delta = Durations.front() - meanDuration;
                    Durations.pop();
                    sumDeltaSq += delta * delta;
                    deltaSqCount += 1.0;
                }
                double sigmaMs = 0.0;
                if (sumDeltaSq > 0.0) {
                    sigmaMs = ::sqrt(sumDeltaSq / deltaSqCount) * 1000.0;
                }
                double plusLimitMs = avgDurationMs;// + 1.0 * sigmaMs;
                ASSERT_YTHROW(plusLimitMs < 50.0 * timeMultiplier, "avgDurationMs=" << avgDurationMs <<
                    " TotalResponses=" << TotalResponses << " sigmaMs=" << sigmaMs <<
                    " plusLimitMs=" << plusLimitMs);

                double totalDuration = (currentTime - StartTime).SecondsFloat();
                double speed = (double)TotalDataSize / totalDuration / 1024.0 / 1024.0;
                LOW_VERBOSE_COUT("totalDuration=" << totalDuration << " speed=" << speed
                    << " avg=" << avgDurationMs << " sigma=" << sigmaMs);
            }
            if (Responses == MessagesToSend) {
                break;
            }
            TestStep += 10;
            // fall through via [[fallthrough]] below
        } else {
            TEST_RESPONSE(EvChunkWriteResult, OK);
            ASSERT_YTHROW(LastResponse.Cookie == (void*)42, "Unexpected cookie=" << LastResponse.Cookie);
            ChunkIdx = LastResponse.ChunkIdx;
            VERBOSE_COUT(" Sending TEvLog to commit");
            NPDisk::TCommitRecord commitRecord;
            commitRecord.CommitChunks.push_back(ChunkIdx);
            Commit1Data = TRcBuf(PrepareData(302010));
            ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, commitRecord, Commit1Data,
                TLsnSeg((ui64)MessagesToSend+1, (ui64)MessagesToSend+1),
                (void*)43));
            break;
        }
        [[fallthrough]];
    case 70:
        TEST_RESPONSE(EvLogResult, OK);
        VERBOSE_COUT(" Sending TEvChunkRead");
        ctx.Send(Yard, new NPDisk::TEvChunkRead(Owner, OwnerRound, ChunkIdx, 0, (ui32)ChunkWriteData.size(), 1,
                    nullptr));
        break;
    case 80:
        TEST_RESPONSE(EvChunkReadResult, OK);
        ASSERT_YTHROW(LastResponse.ChunkIdx == ChunkIdx, "Unexpected chunkIdx=" << LastResponse.ChunkIdx);
        TEST_DATA_EQUALS(LastResponse.Data.ToString(), ChunkWriteData);
        VERBOSE_COUT("Done");
        SignalDoneEvent();
        break;
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
}

void TTestChunkPriorityBlock::TestFSM(const TActorContext &ctx) {
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        break;
    case 10:
        TEST_RESPONSE(EvYardInitResult, OK);
        Iteration = 3;
        Owner = LastResponse.Owner;
        OwnerRound = LastResponse.OwnerRound;
        SafeSize = ((ENABLE_VALGRIND_REQUESTS | IS_SLOW_MACHINE) ? (8 << 20) :
            (32 << 20)) / LastResponse.AppendBlockSize * LastResponse.AppendBlockSize;
        ChunkWriteData = PrepareData(SafeSize);
        ctx.Send(Yard, new NPDisk::TEvChunkReserve(Owner, OwnerRound, 5));
        break;
    case 20:
        TEST_RESPONSE(EvChunkReserveResult, OK);
        ASSERT_YTHROW(LastResponse.ChunkIds.size() == PausedChunkWrites,
            "Unexpected ChunkIds.size() == " << LastResponse.ChunkIds.size());
        ChunkIds = LastResponse.ChunkIds;

        ChunkWriteParts.Reset(new NPDisk::TEvChunkWrite::TPart[1]);
        ChunkWriteParts[0].Data = ChunkWriteData.data();
        ChunkWriteParts[0].Size = (ui32)ChunkWriteData.size();

        VERBOSE_COUT(" Sending TEvYardControl Pause");
        ctx.Send(Yard, new NPDisk::TEvYardControl(NPDisk::TEvYardControl::ActionPause, nullptr));

        for (ui32 i = 0; i < PausedChunkWrites; ++i) {
            bool last = i == PausedChunkWrites - 1;
            ui8 priority = last ? 6 : 5;
            ui64 cookie = last ? 1 : 2;
            VERBOSE_COUT(" Sending TEvChunkWrite cookie# " << cookie << " priority# " << (int)priority);
            ctx.Send(Yard, new NPDisk::TEvChunkWrite(Owner, OwnerRound, ChunkIds[i], 0,
                new NPDisk::TEvChunkWrite::TNonOwningParts(ChunkWriteParts.Get(), 1), (void*)cookie, true, priority));
        }

        VERBOSE_COUT(" Sending TEvYardControl Resume");
        ctx.Send(Yard, new NPDisk::TEvYardControl(NPDisk::TEvYardControl::ActionResume, nullptr));
        break;
    case 30:
    case 40:
        TEST_RESPONSE(EvYardControlResult, OK);
        break;
    case 50:
        if (LastResponse.EventType == TEvBlobStorage::EvChunkWriteResult) {

            TEST_RESPONSE(EvChunkWriteResult, OK);
            if (LastResponse.Cookie == (void*)1) {
                SignalDoneEvent();
                return;
            }

            ++Iteration;

            ASSERT_YTHROW(Iteration < 30, "ERROR: Low priority write is blocked.");
            ctx.Send(Yard, new NPDisk::TEvChunkWrite(Owner, OwnerRound, ChunkIds[0], 0,
                new NPDisk::TEvChunkWrite::TNonOwningParts(ChunkWriteParts.Get(), 1), (void*)2, true, 1));
            VERBOSE_COUT(" Sending TEvChunkWrite cookie# 2 priority# 1");
        }
        return;
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
}

void TTestLog2Records3Sectors::TestFSM(const TActorContext &ctx) {
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        Data = TRcBuf(PrepareData(5500));
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        break;
    case 10:
        TEST_RESPONSE(EvYardInitResult, OK);
        Owner = LastResponse.Owner;
        OwnerRound = LastResponse.OwnerRound;
        ChunkSize = LastResponse.ChunkSize;

        VERBOSE_COUT(" Sending TEvYardControl Pause, 2 TEvLog, TEvYardControl Resume ");
        ctx.Send(Yard, new NPDisk::TEvYardControl(NPDisk::TEvYardControl::ActionPause, nullptr));
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 7, Data, TLsnSeg((ui64)1, (ui64)1), (void*)456));
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 7, Data, TLsnSeg((ui64)2, (ui64)2), (void*)456));
        ctx.Send(Yard, new NPDisk::TEvYardControl(NPDisk::TEvYardControl::ActionResume, nullptr));
        break;
    case 20:
        TEST_RESPONSE(EvYardControlResult, OK);
        break;
    case 30:
        TEST_RESPONSE(EvYardControlResult, OK);
        break;
    case 40:
        TEST_RESPONSE(EvLogResult, OK);
        ASSERT_YTHROW(LastResponse.LogResults.size() == 2,
            "LogResults size=" << LastResponse.LogResults.size());
        VERBOSE_COUT("Done");
        SignalDoneEvent();
        break;
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
}

void TTestLogDamageSector3Append1::TestFSM(const TActorContext &ctx) {
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        Data = TRcBuf(PrepareData(5501));
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        break;
    case 10:
        TEST_RESPONSE(EvYardInitResult, OK);
        Owner = LastResponse.Owner;
        OwnerRound = LastResponse.OwnerRound;
        VERBOSE_COUT(" Sending TEvReadLog");
        ctx.Send(Yard, new NPDisk::TEvReadLog(Owner, OwnerRound));
        break;
    case 20:
    {
        TEST_RESPONSE(EvReadLogResult, OK);
        ASSERT_YTHROW(LastResponse.LogRecords.size() == 1,
            "Unexpected LogRecords size == " << LastResponse.LogRecords.size());
        TString expectedData = PrepareData(5500);
        TEST_LOG_RECORD(LastResponse.LogRecords[0], 1, 7, expectedData);
        VERBOSE_COUT(" Sending TEvLog");
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 7, Data, TLsnSeg((ui64)2, (ui64)2), (void*)456));
        break;
    }
    case 30:
        TEST_RESPONSE(EvLogResult, OK);
        VERBOSE_COUT("Done");
        SignalDoneEvent();
        break;
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
}

void TTestLogRead2Sectors::TestFSM(const TActorContext &ctx) {
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        break;
    case 10:
        TEST_RESPONSE(EvYardInitResult, OK);
        Owner = LastResponse.Owner;
        OwnerRound = LastResponse.OwnerRound;
        VERBOSE_COUT(" Sending TEvReadLog");
        ctx.Send(Yard, new NPDisk::TEvReadLog(Owner, OwnerRound));
        break;
    case 20:
    {
        TEST_RESPONSE(EvReadLogResult, OK);
        ASSERT_YTHROW(LastResponse.LogRecords.size() == 2,
            "Unexpected LogRecords size == " << LastResponse.LogRecords.size());
        TRcBuf expectedData = TRcBuf(PrepareData(5500));
        TEST_LOG_RECORD(LastResponse.LogRecords[0], 1, 7, expectedData.ExtractUnderlyingContainerOrCopy<TString>());
        expectedData = TRcBuf(PrepareData(5501));
        TEST_LOG_RECORD(LastResponse.LogRecords[1], 2, 7, expectedData.ExtractUnderlyingContainerOrCopy<TString>());
        VERBOSE_COUT("Done");
        SignalDoneEvent();
        break;
    }
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
}

void TTestLogFillChunkPlus1::TestFSM(const TActorContext &ctx) {
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        break;
    case 10:
        TEST_RESPONSE(EvYardInitResult, OK);
        Owner = LastResponse.Owner;
        OwnerRound = LastResponse.OwnerRound;
        ChunkSize = LastResponse.ChunkSize;
        VERBOSE_COUT(" Owner=" << (int)Owner << " ChunkSize=" << ChunkSize);
        VERBOSE_COUT(" Sending TEvLog");
        Data = TRcBuf(PrepareData(5 << 10));
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 7, Data, TLsnSeg(1, 1), (void*)456));
        break;
    case 20:
        TEST_RESPONSE(EvLogResult, OK);
        VERBOSE_COUT(" Sending TEvLog");
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 7, Data, TLsnSeg(2, 2), (void*)456));
        break;
    case 30:
    {
        TEST_RESPONSE(EvLogResult, OK);
        VERBOSE_COUT(" Sending TEvLog");
        NPDisk::TCommitRecord commitRecord;
        commitRecord.FirstLsnToKeep = 2;
        commitRecord.IsStartingPoint = true;
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 7, commitRecord, Data, TLsnSeg(3, 3), (void*)567));
        break;
    }
    case 40:
        TEST_RESPONSE(EvLogResult, OK);
        VERBOSE_COUT(" Sending TEvLog");
        Data = TRcBuf(PrepareData(ChunkSize));
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 7, Data, TLsnSeg(4, 4), (void*)456));
        break;
    case 50:
        TEST_RESPONSE(EvLogResult, OK);
        VERBOSE_COUT(" Sending TEvLog");
        Data = TRcBuf(PrepareData(5 << 10));
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 7, Data, TLsnSeg(5, 5), (void*)456));
        break;
    case 60:
        TEST_RESPONSE(EvLogResult, OK);
        VERBOSE_COUT("Done");
        SignalDoneEvent();
        break;
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
}

void TTestLogKeep5Plus1::TestFSM(const TActorContext &ctx) {
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        break;
    case 10:
    {
        TEST_RESPONSE(EvYardInitResult, OK);
        Owner = LastResponse.Owner;
        OwnerRound = LastResponse.OwnerRound;
        ChunkSize = LastResponse.ChunkSize;
        VERBOSE_COUT(" Owner=" << (int)Owner << " ChunkSize=" << ChunkSize);
        VERBOSE_COUT(" Sending TEvReadLog");
        ctx.Send(Yard, new NPDisk::TEvReadLog(Owner, OwnerRound, NPDisk::TLogPosition{0, 0}, ChunkSize * 2));
        break;
    }
    case 20:
    {
        TEST_RESPONSE(EvReadLogResult, OK);
        ASSERT_YTHROW(LastResponse.LogRecords.size() == 4,
            "Unexpected LogRecords size == " << LastResponse.LogRecords.size());
        TString expectedData = PrepareData(5 << 10);
        TEST_LOG_RECORD(LastResponse.LogRecords[0], 2, 7, expectedData);
        TEST_LOG_RECORD(LastResponse.LogRecords[1], 3, 7, expectedData);
        expectedData = PrepareData(ChunkSize);
        TEST_LOG_RECORD(LastResponse.LogRecords[2], 4, 7, expectedData);
        expectedData = PrepareData(5 << 10);
        TEST_LOG_RECORD(LastResponse.LogRecords[3], 5, 7, expectedData);

        VERBOSE_COUT(" Sending TEvLog commit");
        Data = TRcBuf(PrepareData(12 << 10));
        NPDisk::TCommitRecord commitRecord;
        commitRecord.FirstLsnToKeep = 5;
        commitRecord.IsStartingPoint = true;
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 7, commitRecord, Data, TLsnSeg(6, 6), (void*)567));
        break;
    }
    case 30:
        TEST_RESPONSE(EvLogResult, OK);
        VERBOSE_COUT(" Sending TEvLog");
        Data = TRcBuf(PrepareData(1 << 10));
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 7, Data, TLsnSeg(7, 7), (void*)456));
        break;
    case 40:
        TEST_RESPONSE(EvLogResult, OK);
        VERBOSE_COUT(" Sending TEvChunkReserve");
        ctx.Send(Yard, new NPDisk::TEvChunkReserve(Owner, OwnerRound, 3));
        break;
    case 50:
        TEST_RESPONSE(EvChunkReserveResult, OK);
        ASSERT_YTHROW(LastResponse.ChunkIds.size() == 3,
            "Unexpected ChunkIds.size() == " << LastResponse.ChunkIds.size());
        VERBOSE_COUT("Done");
        SignalDoneEvent();
        break;
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
}

void TTestLogReadRecords2To5::TestFSM(const TActorContext &ctx) {
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        break;
    case 10:
        TEST_RESPONSE(EvYardInitResult, OK);
        Owner = LastResponse.Owner;
        OwnerRound = LastResponse.OwnerRound;
        ChunkSize = LastResponse.ChunkSize;
        VERBOSE_COUT(" Owner=" << (int)Owner << " ChunkSize=" << ChunkSize);
        ASSERT_YTHROW(LastResponse.StartingPoints.size() == 1,
            "Expected 1 starting point, got " << LastResponse.StartingPoints.size());

        VERBOSE_COUT(" Sending TEvReadLog");
        ctx.Send(Yard, new NPDisk::TEvReadLog(Owner, OwnerRound, NPDisk::TLogPosition{0, 0}, ChunkSize * 2));
        break;
    case 20:
    {
        TEST_RESPONSE(EvReadLogResult, OK);
        ASSERT_YTHROW(LastResponse.LogRecords.size() == 3,
            "Unexpected LogRecords size == " << LastResponse.LogRecords.size());
        TString expectedData = PrepareData(5 << 10);
        TEST_LOG_RECORD(LastResponse.LogRecords[0], 5, 7, expectedData);
        expectedData = PrepareData(12 << 10);
        TEST_LOG_RECORD(LastResponse.LogRecords[1], 6, 7, expectedData);
        expectedData = PrepareData(1 << 10);
        TEST_LOG_RECORD(LastResponse.LogRecords[2], 7, 7, expectedData);
        VERBOSE_COUT("Done");
        SignalDoneEvent();
        break;
    }
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
}

std::atomic<ui32> TTestSysLogReordering::VDiskNum = 0;
std::atomic<ui32> TTestSysLogReorderingLogCheck::VDiskNum = 0;

void TTestWriteAndReleaseChunk2A::TestFSM(const TActorContext &ctx) {
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        break;
    case 10:
    {
        TEST_RESPONSE(EvYardInitResult, OK);
        Owner = LastResponse.Owner;
        OwnerRound = LastResponse.OwnerRound;
        AppendBlockSize = LastResponse.AppendBlockSize;
        ctx.Send(Yard, new NPDisk::TEvChunkReserve(Owner, OwnerRound, 1));
        break;
    }
    case 20:
        TEST_RESPONSE(EvChunkReserveResult, OK);
        ASSERT_YTHROW(LastResponse.ChunkIds.size() == 1,
            "Unexpected ChunkIds.size() == " << LastResponse.ChunkIds.size());
        ChunkIdx = LastResponse.ChunkIds[0];
        VERBOSE_COUT(" Sending TEvChunkWrite");
        ChunkWriteParts.Reset(new NPDisk::TEvChunkWrite::TPart[2]);
        ChunkWriteData = PrepareData(AppendBlockSize, 1);
        ChunkWriteParts[0].Data = ChunkWriteData.data();
        ChunkWriteParts[0].Size = (ui32)ChunkWriteData.size();
        ChunkWriteParts[1].Data = ChunkWriteData.data();
        ChunkWriteParts[1].Size = (ui32)ChunkWriteData.size();
        ctx.Send(Yard, new NPDisk::TEvChunkWrite(Owner, OwnerRound, ChunkIdx, 0,
            new NPDisk::TEvChunkWrite::TNonOwningParts(ChunkWriteParts.Get(), 2), (void*)42, false, 1));
        break;
    case 30:
    {
        TEST_RESPONSE(EvChunkWriteResult, OK);
        ASSERT_YTHROW(LastResponse.Cookie == (void*)42, "Unexpected cookie=" << LastResponse.Cookie);
        ASSERT_YTHROW(ChunkIdx == 2, "This test is designed to work with chunk 2, but got chunk " << ChunkIdx);
        VERBOSE_COUT(" Sending TEvLog to commit");
        NPDisk::TCommitRecord commitRecord;
        commitData = TRcBuf(TString("commit A data"));
        commitRecord.CommitChunks.push_back(ChunkIdx);
        commitRecord.IsStartingPoint = true;
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, commitRecord, commitData, TLsnSeg(1, 1),
            (void*)43));
        break;
    }
    case 40:
        TEST_RESPONSE(EvLogResult, OK);
        VERBOSE_COUT(" Sending TEvChunkRead");
        ctx.Send(Yard, new NPDisk::TEvChunkRead(Owner, OwnerRound, ChunkIdx, 0, ChunkWriteData.size(), 1, nullptr));
        break;
    case 50:
    {
        TEST_RESPONSE(EvChunkReadResult, OK);
        TEST_DATA_EQUALS(LastResponse.Data.ToString(), ChunkWriteData);
        VERBOSE_COUT(" Sending TEvLog to delete");
        NPDisk::TCommitRecord commitRecord;
        commitData = TRcBuf(TString("delete A data"));
        commitRecord.DeleteChunks.push_back(ChunkIdx);
        commitRecord.IsStartingPoint = true;
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, commitRecord, commitData, TLsnSeg(2, 2),
            (void*)43));
        break;
    }
    case 60:
        TEST_RESPONSE(EvLogResult, OK);
        VERBOSE_COUT("Done");
        SignalDoneEvent();
        break;
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
}

void TTestWriteAndCheckChunk2B::TestFSM(const TActorContext &ctx) {
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        break;
    case 10:
    {
        TEST_RESPONSE(EvYardInitResult, OK);
        Owner = LastResponse.Owner;
        OwnerRound = LastResponse.OwnerRound;
        AppendBlockSize = LastResponse.AppendBlockSize;
        ctx.Send(Yard, new NPDisk::TEvChunkReserve(Owner, OwnerRound, 1));
        break;
    }
    case 20:
    {
        TEST_RESPONSE(EvChunkReserveResult, OK);
        ASSERT_YTHROW(LastResponse.ChunkIds.size() == 1,
            "Unexpected ChunkIds.size() == " << LastResponse.ChunkIds.size());
        ChunkIdx = LastResponse.ChunkIds[0];
        ASSERT_YTHROW(ChunkIdx == 2, "This test is designed to work with chunk 2, but got chunk " << ChunkIdx);
        VERBOSE_COUT(" Sending TEvChunkWrite");
        ChunkWriteParts.Reset(new NPDisk::TEvChunkWrite::TPart[2]);
        ChunkWriteData = PrepareData(AppendBlockSize, 2);
        ChunkWriteParts[0].Data = ChunkWriteData.data();
        ChunkWriteParts[0].Size = (ui32)ChunkWriteData.size();
        ChunkWriteParts[1].Data = ChunkWriteData.data();
        ChunkWriteParts[1].Size = (ui32)ChunkWriteData.size();
        ctx.Send(Yard, new NPDisk::TEvChunkWrite(Owner, OwnerRound, ChunkIdx, 0,
            new NPDisk::TEvChunkWrite::TNonOwningParts(ChunkWriteParts.Get(), 2), (void*)42, false, 1));
        break;
    }
    case 30:
    {
        TEST_RESPONSE(EvChunkWriteResult, OK);
        ASSERT_YTHROW(LastResponse.Cookie == (void*)42, "Unexpected cookie=" << LastResponse.Cookie);
        VERBOSE_COUT(" Sending TEvLog to commit");
        NPDisk::TCommitRecord commitRecord;
        commitRecord.CommitChunks.push_back(ChunkIdx);
        Data = TRcBuf(TString("commit B data"));
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, commitRecord, Data, TLsnSeg(3, 3), (void*)43));
        break;
    }
    case 40:
        TEST_RESPONSE(EvLogResult, OK);
        VERBOSE_COUT(" Sending TEvChunkRead");
        ctx.Send(Yard, new NPDisk::TEvChunkRead(Owner, OwnerRound, ChunkIdx, 0, AppendBlockSize, 1, nullptr));
        break;
    case 50:
        TEST_RESPONSE(EvChunkReadResult, OK);
        TEST_DATA_EQUALS(LastResponse.Data.ToString(), ChunkWriteData);
        VERBOSE_COUT(" Sending TEvChunkRead");
        ctx.Send(Yard, new NPDisk::TEvChunkRead(Owner, OwnerRound, ChunkIdx, AppendBlockSize, AppendBlockSize, 1,
                    nullptr));
        break;
    case 60:
        TEST_RESPONSE(EvChunkReadResult, OK);
        TEST_DATA_EQUALS(LastResponse.Data.ToString(), ChunkWriteData);
        VERBOSE_COUT("Done");
        SignalDoneEvent();
        break;
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
}

void TTestCheckErrorChunk2B::TestFSM(const TActorContext &ctx) {
    TString data2("testdata2");
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        break;
    case 10:
    {
        TEST_RESPONSE(EvYardInitResult, OK);
        Owner = LastResponse.Owner;
        OwnerRound = LastResponse.OwnerRound;
        AppendBlockSize = LastResponse.AppendBlockSize;
        VERBOSE_COUT(" Sending TEvChunkRead");
        ChunkIdx = 2;
        ctx.Send(Yard, new NPDisk::TEvChunkRead(Owner, OwnerRound, ChunkIdx, 0, AppendBlockSize, 1, nullptr));
        break;
    }
    case 20:
    {
        TEST_RESPONSE(EvChunkReadResult, OK);
        ASSERT_YTHROW(!LastResponse.Data.IsReadable(0, LastResponse.Data.Size()), "Unexpected IsReadable");

        //TString expectedData = PrepareData(AppendBlockSize, 1);
        //TEST_DATA_EQUALS(LastResponse.Data.ToString(), expectedData);

        VERBOSE_COUT("Done");
        SignalDoneEvent();
        break;
    }
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
}

void TTestWriteChunksAndLog::TestFSM(const TActorContext &ctx) {
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        Lsn = 0;
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        break;
    case 10:
    {
        TEST_RESPONSE(EvYardInitResult, OK);
        Owner = LastResponse.Owner;
        OwnerRound = LastResponse.OwnerRound;
        AppendBlockSize = LastResponse.AppendBlockSize;
        ChunkSize = LastResponse.ChunkSize;
        VERBOSE_COUT(" Sending TEvChunkWrite");
        ChunkWriteParts.Reset(new NPDisk::TEvChunkWrite::TPart[3]);
        ChunkWriteData = PrepareData(AppendBlockSize, 2);
        for (ui32 i = 0; i < 3; ++i) {
            ChunkWriteParts[i].Data = ChunkWriteData.data();
            ChunkWriteParts[i].Size = (ui32)ChunkWriteData.size();
        }
        ctx.Send(Yard, new NPDisk::TEvChunkWrite(Owner, OwnerRound, 0, 0,
            new NPDisk::TEvChunkWrite::TNonOwningParts(ChunkWriteParts.Get(), 3), (void*)43, false, 1));
        break;
    }
    case 20:
    {
        TEST_RESPONSE(EvChunkWriteResult, OK);
        ASSERT_YTHROW(LastResponse.Cookie == (void*)43, "Unexpected cookie=" << LastResponse.Cookie);
        ChunkIdx2 = LastResponse.ChunkIdx;
        ASSERT_YTHROW(ChunkIdx2 == 2, "This test is designed to work with chunk 2, but got " << ChunkIdx2);
        VERBOSE_COUT(" Sending TEvChunkWrite");
        ChunkWriteParts.Reset(new NPDisk::TEvChunkWrite::TPart[3]);
        ChunkWriteData = PrepareData(AppendBlockSize, 3);
        for (ui32 i = 0; i < 3; ++i) {
            ChunkWriteParts[i].Data = ChunkWriteData.data();
            ChunkWriteParts[i].Size = (ui32)ChunkWriteData.size();
        }
        ctx.Send(Yard, new NPDisk::TEvChunkWrite(Owner, OwnerRound, 0, 0,
            new NPDisk::TEvChunkWrite::TNonOwningParts(ChunkWriteParts.Get(), 3), (void*)43, false, 1));
        break;
    }
    case 30:
    {
        TEST_RESPONSE(EvChunkWriteResult, OK);
        ASSERT_YTHROW(LastResponse.Cookie == (void*)43, "Unexpected cookie=" << LastResponse.Cookie);
        ChunkIdx3 = LastResponse.ChunkIdx;
        ASSERT_YTHROW(ChunkIdx3 == 3, "This test is designed to work with chunk 2, but got " << ChunkIdx3);
        VERBOSE_COUT(" Sending TEvLog to commit");
        NPDisk::TCommitRecord commitRecord;
        commitData = TRcBuf(TString("commit A data"));
        commitRecord.CommitChunks.push_back(ChunkIdx3);
        commitRecord.CommitChunks.push_back(ChunkIdx2);
        commitRecord.IsStartingPoint = true;
        auto lsn = NextLsn();
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, commitRecord, commitData, TLsnSeg(lsn, lsn),
            (void*)43));
        break;
    }
    case 40:
    {
        TEST_RESPONSE(EvLogResult, OK);
        VERBOSE_COUT(" Sending huge TEvLog");
        TRcBuf hugeData = TRcBuf(PrepareData(ChunkSize));
        auto lsn = NextLsn();
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, hugeData, TLsnSeg(lsn, lsn), (void*)43));
        break;
    }
    case 50:
    case 60:
    case 70:
    {
        TEST_RESPONSE(EvLogResult, OK);
        VERBOSE_COUT(" Sending large TEvLog");
        TRcBuf largeData = TRcBuf(PrepareData(ChunkSize / 8));
        auto lsn = NextLsn();
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, largeData, TLsnSeg(lsn, lsn), (void*)43));
        break;
    }
    case 80:
    {
        TEST_RESPONSE(EvLogResult, OK);
        VERBOSE_COUT(" Sending small TEvLogs");
        TRcBuf smallData = TRcBuf(PrepareData(3000));
        ui64 lsn = NextLsn();
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, smallData, TLsnSeg(lsn, lsn), (void*)43));
        lsn = NextLsn();
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, smallData, TLsnSeg(lsn, lsn), (void*)43));
        lsn = NextLsn();
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, smallData, TLsnSeg(lsn, lsn), (void*)43));
        lsn = NextLsn();
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, smallData, TLsnSeg(lsn, lsn), (void*)43));
        [[fallthrough]]; // AUTOGENERATED_FALLTHROUGH_FIXME
    }
    case 90:
    case 100:
    case 110:
    {
        TEST_RESPONSE(EvLogResult, OK);
        TestStep += 10 * LastResponse.LogResults.size();
        if (TestStep <= 120) {
            TestStep -= 10;
            break;
        }
        TestStep -= 10;
        [[fallthrough]]; // AUTOGENERATED_FALLTHROUGH_FIXME
    }
    case 120:
    {
        TEST_RESPONSE(EvLogResult, OK);
        VERBOSE_COUT(" Sending medium TEvLog");
        TRcBuf mediumData = TRcBuf(PrepareData(100500));
        auto lsn = NextLsn();
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, mediumData, TLsnSeg(lsn, lsn), (void*)43));
        break;
    }
    case 130:
        TEST_RESPONSE(EvLogResult, OK);
        // By now we have chunks 2 and 3 with some data and chunks 1, 4 - log.
        // Log contains: 1 commitData, 1 hugeData, 3 largeData, 4 smallData, 1 mediumData (10 in total).

        VERBOSE_COUT("Done");
        SignalDoneEvent();
        break;
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
}

void TTestContinueWriteLogChunk::TestFSM(const TActorContext &ctx) {
    Ctest << "Test step " << TestStep << Endl;
    switch (TestStep) {
    case 0:
        Lsn = 0;
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        Ctest << " Sending TEvInit" << Endl;
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        break;
    case 10:
    {
        TEST_RESPONSE(EvYardInitResult, OK);
        Owner = LastResponse.Owner;
        OwnerRound = LastResponse.OwnerRound;
        AppendBlockSize = LastResponse.AppendBlockSize;
        ChunkSize = LastResponse.ChunkSize;

        Ctest << " Sending TEvLogRead" << Endl;
        ctx.Send(Yard, new NPDisk::TEvReadLog(Owner, OwnerRound));
        break;
    }
    case 20:
    {
        TEST_RESPONSE(EvReadLogResult, OK);
        for (ui64 idx = 0; idx < LastResponse.LogRecords.size(); ++idx) {
            if (Lsn < LastResponse.LogRecords[idx].Lsn) {
                Lsn = LastResponse.LogRecords[idx].Lsn;
            }
        }
        if (!LastResponse.IsEndOfLog) {
            ctx.Send(Yard, new NPDisk::TEvReadLog(Owner, OwnerRound, LastResponse.NextPosition));
            return;
        }
        Ctest << " Read log done" << Endl;
        Ctest << " Sending small TEvLogs" << Endl;
        TRcBuf smallData = TRcBuf(PrepareData(3000));
        ui64 lsn = NextLsn();
        SentSize = smallData.size();
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, smallData, TLsnSeg(lsn, lsn), (void*)43));
        break;
    }
    case 30:
    {
        TEST_RESPONSE(EvLogResult, OK);
        if (SentSize > ChunkSize) {
            Ctest << "Done";
            SignalDoneEvent();
            break;
        } else {
            TRcBuf smallData = TRcBuf(PrepareData(3000));
            ui64 lsn = NextLsn();
            SentSize += smallData.size();
            ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, smallData, TLsnSeg(lsn, lsn), (void*)43));
            return;
        }
    }
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
}

void TTestLastLsn::TestFSM(const TActorContext &ctx) {
    Ctest << "Test step " << TestStep << Endl;
    switch (TestStep) {
    case 0:
        Lsn = 0;
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        Ctest << " Sending TEvInit" << Endl;
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        break;
    case 10:
    {
        TEST_RESPONSE(EvYardInitResult, OK);
        Owner = LastResponse.Owner;
        OwnerRound = LastResponse.OwnerRound;
        AppendBlockSize = LastResponse.AppendBlockSize;
        ChunkSize = LastResponse.ChunkSize;

        Ctest << " Sending TEvLogRead" << Endl;
        ctx.Send(Yard, new NPDisk::TEvReadLog(Owner, OwnerRound));
        break;
    }
    case 20:
    {
        TEST_RESPONSE(EvReadLogResult, OK);
        for (ui64 idx = 0; idx < LastResponse.LogRecords.size(); ++idx) {
            if (Lsn < LastResponse.LogRecords[idx].Lsn) {
                Lsn = LastResponse.LogRecords[idx].Lsn;
            }
        }
        if (!LastResponse.IsEndOfLog) {
            ctx.Send(Yard, new NPDisk::TEvReadLog(Owner, OwnerRound, LastResponse.NextPosition));
            return;
        }
        Ctest << " Read log done" << Endl;
        ASSERT_YTHROW(Lsn > ChunkSize / 4096, "Unexpected Lsn# " << Lsn << " Expected > " << (ChunkSize / 4096));
        Ctest << "Done";
        SignalDoneEvent();
        break;
    }
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
}


void TTestCheckLog::TestFSM(const TActorContext &ctx) {
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        break;
    case 10:
    {
        TEST_RESPONSE(EvYardInitResult, OK);
        Owner = LastResponse.Owner;
        OwnerRound = LastResponse.OwnerRound;
        AppendBlockSize = LastResponse.AppendBlockSize;
        ChunkSize = LastResponse.ChunkSize;
        VERBOSE_COUT(" Sending TEvReadLog");
        ctx.Send(Yard, new NPDisk::TEvReadLog(Owner, OwnerRound, NPDisk::TLogPosition{0, 0}, 16 << 20));
        break;
    }
    case 20:
    {
        TEST_RESPONSE(EvReadLogResult, OK);
        ASSERT_YTHROW(LastResponse.LogRecords.size() == 9 || LastResponse.LogRecords.size() == 10,
            "Unexpected LogRecords size == " << LastResponse.LogRecords.size());
        TString expectedData = "commit A data";
        TEST_LOG_RECORD(LastResponse.LogRecords[0], 1, 0, expectedData);
        expectedData = PrepareData(ChunkSize);
        TEST_LOG_RECORD(LastResponse.LogRecords[1], 2, 0, expectedData);
        expectedData = PrepareData(ChunkSize / 8);
        TEST_LOG_RECORD(LastResponse.LogRecords[2], 3, 0, expectedData);
        TEST_LOG_RECORD(LastResponse.LogRecords[3], 4, 0, expectedData);
        TEST_LOG_RECORD(LastResponse.LogRecords[4], 5, 0, expectedData);
        expectedData = PrepareData(3000);
        TEST_LOG_RECORD(LastResponse.LogRecords[5], 6, 0, expectedData);
        TEST_LOG_RECORD(LastResponse.LogRecords[6], 7, 0, expectedData);
        TEST_LOG_RECORD(LastResponse.LogRecords[7], 8, 0, expectedData);
        TEST_LOG_RECORD(LastResponse.LogRecords[8], 9, 0, expectedData);
        if (LastResponse.LogRecords.size() == 10) {
            expectedData = PrepareData(100500);
            TEST_LOG_RECORD(LastResponse.LogRecords[9], 10, 0, expectedData);
        }
        VERBOSE_COUT("Done");
        SignalDoneEvent();
        break;
    }
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
}

void TTestChunkFlush::TestFSM(const TActorContext &ctx) {
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        break;
    case 10:
    {
        TEST_RESPONSE(EvYardInitResult, OK);
        Owner = LastResponse.Owner;
        OwnerRound = LastResponse.OwnerRound;
        AppendBlockSize = LastResponse.AppendBlockSize;
        ctx.Send(Yard, new NPDisk::TEvChunkReserve(Owner, OwnerRound, 1));
        break;
    }
    case 20:
        TEST_RESPONSE(EvChunkReserveResult, OK);
        ASSERT_YTHROW(LastResponse.ChunkIds.size() == 1,
            "Unexpected ChunkIds.size() == " << LastResponse.ChunkIds.size());
        ChunkIdx = LastResponse.ChunkIds[0];
        ASSERT_YTHROW(ChunkIdx == 2, "This test is designed to work with chunk 2, but got " << ChunkIdx);
        VERBOSE_COUT(" Sending TEvChunkWrite");
        ChunkWriteParts.Reset(new NPDisk::TEvChunkWrite::TPart[1]);
        ChunkWriteData = PrepareData(AppendBlockSize, 1);
        ChunkWriteParts[0].Data = ChunkWriteData.data();
        ChunkWriteParts[0].Size = (ui32)ChunkWriteData.size();
        ctx.Send(Yard, new NPDisk::TEvChunkWrite(Owner, OwnerRound, ChunkIdx, 0,
            new NPDisk::TEvChunkWrite::TNonOwningParts(ChunkWriteParts.Get(), 1), (void*)42, true, 1));
        break;
    case 30:
    {
        TEST_RESPONSE(EvChunkWriteResult, OK);
        ASSERT_YTHROW(LastResponse.Cookie == (void*)42, "Unexpected cookie=" << LastResponse.Cookie);
        VERBOSE_COUT(" Sending TEvLog to set a starting point");
        NPDisk::TCommitRecord commitRecord;
        commitData = TRcBuf::Uninitialized(sizeof(ui32));
        *(ui32*)commitData.data() = ChunkIdx;
        commitRecord.IsStartingPoint = true;
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, commitRecord, commitData, TLsnSeg(1, 1),
                    (void*)43));
        break;
    }
    case 40:
        TEST_RESPONSE(EvLogResult, OK);
        VERBOSE_COUT("Done");
        SignalDoneEvent();
        break;
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
}

void TTestChunkUnavailable::TestFSM(const TActorContext &ctx) {
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        break;
    case 10:
    {
        TEST_RESPONSE(EvYardInitResult, OK);
        Owner = LastResponse.Owner;
        OwnerRound = LastResponse.OwnerRound;
        AppendBlockSize = LastResponse.AppendBlockSize;
        ASSERT_YTHROW(LastResponse.StartingPoints.size() == 1,
            "Unexpected StartingPoints.size() = " << LastResponse.StartingPoints.size());
        ASSERT_YTHROW(LastResponse.StartingPoints[0].Data.size() == sizeof(ui32),
            "Unexpected StartingPoints[0].Data.size() = " << LastResponse.StartingPoints[0].Data.size());
        ChunkIdx = *(ui32*)LastResponse.StartingPoints[0].Data.data();

        VERBOSE_COUT(" Sending TEvChunkRead");
        ctx.Send(Yard, new NPDisk::TEvChunkRead(Owner, OwnerRound, ChunkIdx, 0, AppendBlockSize, 1, nullptr));
        break;
    }
    case 20:
        TEST_RESPONSE(EvChunkReadResult, ERROR);
        VERBOSE_COUT("Done");
        SignalDoneEvent();
        break;
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
}

void TTestRedZoneSurvivability::TestFSM(const TActorContext &ctx) {
    TString data("testdata");
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        break;
    case 10:
        TEST_RESPONSE(EvYardInitResult, OK);
        TEST_PDISK_STATUS(ui32(NKikimrBlobStorage::StatusIsValid)
            | ui32(NKikimrBlobStorage::StatusNewOwner));
        Owner = LastResponse.Owner;
        OwnerRound = LastResponse.OwnerRound;
        ChunkSize = LastResponse.ChunkSize;
        VERBOSE_COUT(" Sending TEvChunkReserve to reserve 1 chunk");
        ctx.Send(Yard, new NPDisk::TEvChunkReserve(Owner, OwnerRound, 1));
        break;
    case 20:
    {
        TEST_RESPONSE(EvChunkReserveResult, OK);
        ChunkIds.push_back(LastResponse.ChunkIds[0]);
        if ((LastResponse.StatusFlags & ui32(NKikimrBlobStorage::StatusDiskSpaceRed)) == 0) {
            VERBOSE_COUT(" Sending TEvChunkReserve to reserve 1 chunk, already have " << ChunkIds.size());
            ctx.Send(Yard, new NPDisk::TEvChunkReserve(Owner, OwnerRound, 1));
            return; // loop
        }

        TEST_PDISK_STATUS(ui32(NKikimrBlobStorage::StatusIsValid)
            | ui32(NKikimrBlobStorage::StatusDiskSpaceCyan)
            | ui32(NKikimrBlobStorage::StatusDiskSpaceRed)
            | ui32(NKikimrBlobStorage::StatusDiskSpaceOrange)
            | ui32(NKikimrBlobStorage::StatusDiskSpacePreOrange)
            | ui32(NKikimrBlobStorage::StatusDiskSpaceLightOrange)
            | ui32(NKikimrBlobStorage::StatusDiskSpaceYellowStop)
            | ui32(NKikimrBlobStorage::StatusDiskSpaceLightYellowMove));
        ASSERT_YTHROW(ChunkIds.size() > 0, "Unexpected ChunkIds.size() == " << ChunkIds.size());
        VERBOSE_COUT(" Sending large TEvLog");
        TRcBuf tinyData = TRcBuf(PrepareData(1));
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, tinyData, TLsnSeg(1, 1), (void*)43));
        break;
    }
    case 30:
    {
        //TEST_RESPONSE(EvLogResult, ERROR);
        TEST_PDISK_STATUS(ui32(NKikimrBlobStorage::StatusIsValid));
        VERBOSE_COUT(" Sending TEvLog to delete a chunk");
        NPDisk::TCommitRecord commitRecord;
        TRcBuf commitData = TRcBuf(TString("hello"));
        ui32 count = ChunkIds.size();
        for (ui32 i = 0; i < count / 2; ++i) {
            commitRecord.DeleteChunks.push_back(ChunkIds.back());
            ChunkIds.pop_back();
        }
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, commitRecord, commitData, TLsnSeg(2, 2),
                    (void*)43));
        break;
    }
    case 40:
    {
        TEST_RESPONSE(EvLogResult, OK);

        VERBOSE_COUT(" Sending TEvLog to log ChunkSize bytes");
        TRcBuf largeData = TRcBuf(PrepareData(ChunkSize * 130));
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, largeData, TLsnSeg(3, 3), (void*)43));
        break;
    }
    case 50:
    {
        TEST_RESPONSE(EvLogResult, OK);
        TEST_PDISK_STATUS(ui32(NKikimrBlobStorage::StatusIsValid)
            | ui32(NKikimrBlobStorage::StatusDiskSpaceCyan)
            | ui32(NKikimrBlobStorage::StatusDiskSpaceLightYellowMove)
            | ui32(NKikimrBlobStorage::StatusDiskSpaceYellowStop));

        VERBOSE_COUT(" Sending TEvLog to log 3 * ChunkSize bytes");
        TRcBuf largeData = TRcBuf(PrepareData(ChunkSize * 3));
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, largeData, TLsnSeg(4, 4), (void*)43));
        break;
    }
    case 60:
    {
        TEST_RESPONSE(EvLogResult, OK);
        TEST_PDISK_STATUS(ui32(NKikimrBlobStorage::StatusIsValid)
            | ui32(NKikimrBlobStorage::StatusDiskSpaceCyan)
            | ui32(NKikimrBlobStorage::StatusDiskSpaceLightYellowMove)
            | ui32(NKikimrBlobStorage::StatusDiskSpaceYellowStop));

        VERBOSE_COUT(" Sending TEvLog to cut log");
        NPDisk::TCommitRecord commitRecord;
        TRcBuf commitData = TRcBuf(TString("cut 1"));
        commitRecord.FirstLsnToKeep = 3;
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, commitRecord, commitData, TLsnSeg(5, 5),
                    (void*)43));
        break;
    }
    case 70:
    {
        TEST_RESPONSE(EvLogResult, OK);
        VERBOSE_COUT(" Sending TEvLog to cut log");
        NPDisk::TCommitRecord commitRecord;
        TRcBuf commitData = TRcBuf(TString("cut 2"));
        commitRecord.FirstLsnToKeep = 4;
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, commitRecord, commitData, TLsnSeg(6, 6),
                    (void*)43));
        break;
    }
    case 80:
    {
        TEST_RESPONSE(EvLogResult, OK);
        // We don't promise to update status color after cutting the log
        VERBOSE_COUT(" Sending TEvLog to log ChunkSize bytes");
        TRcBuf largeData = TRcBuf(PrepareData(ChunkSize));
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, largeData, TLsnSeg(7, 7), (void*)43));
        break;
    }
    case 90:
    {
        TEST_RESPONSE(EvLogResult, OK);

        ctx.Send(Yard, new NPDisk::TEvCheckSpace(Owner, OwnerRound));
        break;
    }
    case 100:
        TEST_RESPONSE(EvCheckSpaceResult, OK);
        TEST_PDISK_STATUS(ui32(NKikimrBlobStorage::StatusIsValid));
        VERBOSE_COUT("Done");
        SignalDoneEvent();
        break;
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
};

void TTestFillDiskPhase1::TestFSM(const TActorContext &ctx) {
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        break;
    case 10:
        TEST_RESPONSE(EvYardInitResult, OK);
        Owner = LastResponse.Owner;
        OwnerRound = LastResponse.OwnerRound;
        ChunkSize = LastResponse.ChunkSize;
        MessageIdx = 0;
        Data = TRcBuf(PrepareData(ChunkSize / 8));
        VERBOSE_COUT(" Sending TEvChunkReserve");
        ctx.Send(Yard, new NPDisk::TEvChunkReserve(Owner, OwnerRound, 1));
        break;
    case 20:
        TEST_RESPONSE(EvChunkReserveResult, OK);
        ASSERT_YTHROW(LastResponse.ChunkIds.size() == 1,
            "Unexpected ChunkIds.size() == " << LastResponse.ChunkIds.size());
        ChunkId = LastResponse.ChunkIds[0];
        VERBOSE_COUT(" Sending TEvChunkWrite");
        ChunkWriteParts.Reset(new NPDisk::TEvChunkWrite::TPart[1]);
        ChunkWriteData = PrepareData(700);
        ChunkWriteParts[0].Data = ChunkWriteData.data();
        ChunkWriteParts[0].Size = (ui32)ChunkWriteData.size();
        ctx.Send(Yard, new NPDisk::TEvChunkWrite(Owner, OwnerRound, ChunkId, 0,
            new NPDisk::TEvChunkWrite::TNonOwningParts(ChunkWriteParts.Get(), 1), (void*)42, false, 1));
        break;
    case 30:
    {
        TEST_RESPONSE(EvChunkWriteResult, OK);

        VERBOSE_COUT(" Sending TEvLog to commit");
        NPDisk::TCommitRecord commitRecord;
        commitRecord.CommitChunks.push_back(ChunkId);
        commitRecord.IsStartingPoint = true;
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, commitRecord, Data, TLsnSeg(1, 1), (void*)43));
        break;
    }
    case 40:
        TEST_RESPONSE(EvLogResult, OK);

        VERBOSE_COUT("Done");
        SignalDoneEvent();
        break;
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
}

void TTestFillDiskPhase2::TestFSM(const TActorContext &ctx) {
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        break;
    case 10:
        TEST_RESPONSE(EvYardInitResult, OK);
        Owner = LastResponse.Owner;
        OwnerRound = LastResponse.OwnerRound;
        ChunkSize = LastResponse.ChunkSize;
        MessageIdx = 0;
        Data = TRcBuf(PrepareData(ChunkSize / 8));
        VERBOSE_COUT(" Sending TEvLog messages");
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 7, Data, TLsnSeg(MessageIdx+1, MessageIdx+1),
                    (void*)456));
        break;
    case 20:
        SentCount++;
        if (LastResponse.Status == NKikimrProto::OK &&
                (LastResponse.StatusFlags & ui32(NKikimrBlobStorage::StatusDiskSpaceRed)) == 0 &&
                SentCount < 180) {
            TEST_RESPONSE(EvLogResult, OK);
            VERBOSE_COUT(" Sending TEvLog messages");
            ++MessageIdx;
            ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 7, Data, TLsnSeg(MessageIdx+1, MessageIdx+1),
                        (void*)456));
            TestStep -= 10;
            break;
        }
        //TEST_RESPONSE(EvLogResult, ERROR);
        if (Data.size() > 2) {
            Data = TRcBuf(PrepareData(Data.size() / 2));
            VERBOSE_COUT(" Sending TEvLog messages");
            ++MessageIdx;
            ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 7, Data, TLsnSeg(MessageIdx+1, MessageIdx+1),
                        (void*)456));
            TestStep -= 10;
            break;
        }
        VERBOSE_COUT("Done");
        SignalDoneEvent();
        break;
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
}

void TTestHarakiri::TestFSM(const TActorContext &ctx) {
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        break;
    case 10:
        TEST_RESPONSE(EvYardInitResult, OK);
        Owner = LastResponse.Owner;
        OwnerRound = LastResponse.OwnerRound;
        VERBOSE_COUT(" Sending Harakiri message");
        ctx.Send(Yard, new NPDisk::TEvHarakiri(Owner, OwnerRound));
        break;
    case 20:
        TEST_RESPONSE(EvHarakiriResult, OK);
        VERBOSE_COUT(" Sending TEvReadLog");
        ctx.Send(Yard, new NPDisk::TEvReadLog(Owner, OwnerRound));
        break;
    case 30:
        TEST_RESPONSE(EvReadLogResult, INVALID_OWNER);
        GoodChunkCount = 0;
        VERBOSE_COUT(" Sending TEvChunkRead");
        ctx.Send(Yard, new NPDisk::TEvChunkRead(Owner, OwnerRound, ChunkIdx, 0, 1, 1, nullptr));
        break;
    case 40:
        if (LastResponse.Status == NKikimrProto::OK) {
            TEST_RESPONSE(EvChunkReadResult, OK);
            ++GoodChunkCount;
        } else {
            TEST_RESPONSE(EvChunkReadResult, INVALID_OWNER);
        }
        ++ChunkIdx;
        if (ChunkIdx < 10) {
            VERBOSE_COUT(" Sending TEvChunkRead");
            ctx.Send(Yard, new NPDisk::TEvChunkRead(Owner, OwnerRound, ChunkIdx, 0, 700, 1, nullptr));
            TestStep -= 10;
            break;
        }
        // ASSERT_YTHROW(GoodChunkCount == 3, "Unexpedcted good chunk count = " << GoodChunkCount);
        // TODO: invalidate chunk nonces and thus render them unreadable, pass owner to read requests.
        VERBOSE_COUT(" Sending TEvLog");
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, TRcBuf(PrepareData(100)), TLsnSeg(123, 123), (void*)456));
        break;
    case 50:
        TEST_RESPONSE(EvLogResult, INVALID_OWNER);
        VERBOSE_COUT(" Sending TEvChunkWrite");
        ChunkWriteParts.Reset(new NPDisk::TEvChunkWrite::TPart[1]);
        ChunkWriteData = PrepareData(300);
        ChunkWriteParts[0].Data = ChunkWriteData.data();
        ChunkWriteParts[0].Size = (ui32)ChunkWriteData.size();
        ctx.Send(Yard, new NPDisk::TEvChunkWrite(Owner, OwnerRound, 0, 0,
            new NPDisk::TEvChunkWrite::TNonOwningParts(ChunkWriteParts.Get(), 1), (void*)42, false, 1));
        break;
    case 60:
        TEST_RESPONSE(EvChunkWriteResult, INVALID_OWNER);

        VERBOSE_COUT("Done");
        SignalDoneEvent();
        break;
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
}

void TTestSimpleHarakiri::TestFSM(const TActorContext &ctx) {
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        break;
    case 10:
        TEST_RESPONSE(EvYardInitResult, OK);
        Owner = LastResponse.Owner;
        OwnerRound = LastResponse.OwnerRound;
        VERBOSE_COUT(" Sending Harakiri message");
        ctx.Send(Yard, new NPDisk::TEvHarakiri(Owner, OwnerRound));
        break;
    case 20:
        TEST_RESPONSE(EvHarakiriResult, OK);
        VERBOSE_COUT("Done");
        SignalDoneEvent();
        break;
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
}

void TTestSlay::TestFSM(const TActorContext &ctx) {
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        break;
    case 10:
        TEST_RESPONSE(EvYardInitResult, OK);
        Owner = LastResponse.Owner;
        OwnerRound = LastResponse.OwnerRound;
        VERBOSE_COUT(" Sending Slay message");
        ctx.Send(Yard, new NPDisk::TEvSlay(VDiskID, 3, 1, 1));
        break;
    case 20:
        TEST_RESPONSE(EvSlayResult, OK);
        VERBOSE_COUT(" Sending TEvReadLog");
        ctx.Send(Yard, new NPDisk::TEvReadLog(Owner, OwnerRound));
        break;
    case 30:
        TEST_RESPONSE(EvReadLogResult, INVALID_OWNER);
        VERBOSE_COUT(" Sending TEvLog");
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, TRcBuf(PrepareData(100)), TLsnSeg(123, 123), (void*)456));
        break;
    case 40:
        TEST_RESPONSE(EvLogResult, INVALID_OWNER);
        VERBOSE_COUT(" Sending TEvChunkWrite");
        ChunkWriteParts.Reset(new NPDisk::TEvChunkWrite::TPart[1]);
        ChunkWriteData = PrepareData(300);
        ChunkWriteParts[0].Data = ChunkWriteData.data();
        ChunkWriteParts[0].Size = (ui32)ChunkWriteData.size();
        ctx.Send(Yard, new NPDisk::TEvChunkWrite(Owner, OwnerRound, 42, 0,
            new NPDisk::TEvChunkWrite::TNonOwningParts(ChunkWriteParts.Get(), 1), (void*)42, false, 1));
        break;
    case 50:
        TEST_RESPONSE(EvChunkWriteResult, INVALID_OWNER);

        VERBOSE_COUT("Done");
        SignalDoneEvent();
        break;
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
}

void TTestSlayRace::TestFSM(const TActorContext &ctx) {
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(3, VDiskID, *PDiskGuid));
        break;
    case 10:
        TEST_RESPONSE(EvYardInitResult, OK);
        Owner = LastResponse.Owner;
        OwnerRound = LastResponse.OwnerRound;
        VERBOSE_COUT(" Sending Slay message");
        ctx.Send(Yard, new NPDisk::TEvSlay(VDiskID, 1, 1, 1));
        break;
    case 20:
        TEST_RESPONSE(EvSlayResult, RACE);
        VERBOSE_COUT(" Sending TEvReadLog");
        ctx.Send(Yard, new NPDisk::TEvReadLog(Owner, OwnerRound));
        break;
    case 30:
        TEST_RESPONSE(EvReadLogResult, OK);
        VERBOSE_COUT(" Sending TEvLog");
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, TRcBuf(PrepareData(100)), TLsnSeg(123, 123), (void*)456));
        break;
    case 40:
        TEST_RESPONSE(EvLogResult, OK);
        ctx.Send(Yard, new NPDisk::TEvChunkReserve(Owner, OwnerRound, 1));
        break;
    case 50:
    {
        TEST_RESPONSE(EvChunkReserveResult, OK);
        ASSERT_YTHROW(LastResponse.ChunkIds.size() == 1,
            "Unexpected ChunkIds.size() == " << LastResponse.ChunkIds.size());
        ui32 chunkIdx = LastResponse.ChunkIds[0];
        VERBOSE_COUT(" Sending TEvChunkWrite");
        ChunkWriteParts.Reset(new NPDisk::TEvChunkWrite::TPart[1]);
        ChunkWriteData = PrepareData(300);
        ChunkWriteParts[0].Data = ChunkWriteData.data();
        ChunkWriteParts[0].Size = (ui32)ChunkWriteData.size();
        ctx.Send(Yard, new NPDisk::TEvChunkWrite(Owner, OwnerRound, chunkIdx, 0,
            new NPDisk::TEvChunkWrite::TNonOwningParts(ChunkWriteParts.Get(), 1), (void*)42, false, 1));
        break;
    }
    case 60:
        TEST_RESPONSE(EvChunkWriteResult, OK);
        VERBOSE_COUT("Done");
        SignalDoneEvent();
        break;
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
}

void TTestSlayRecreate::TestFSM(const TActorContext &ctx) {
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        VDiskId2 = VDiskID;
        VDiskId2.GroupGeneration++;
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        break;
    case 10:
        TEST_RESPONSE(EvYardInitResult, OK);
        Owner = LastResponse.Owner;
        OwnerRound = LastResponse.OwnerRound;
        VERBOSE_COUT(" Sending Slay message");
        ctx.Send(Yard, new NPDisk::TEvSlay(VDiskID, 3, 1, 1));
        break;
    case 20:
        TEST_RESPONSE(EvSlayResult, OK);
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(4, VDiskId2, *PDiskGuid));
        break;
    case 30:
        TEST_RESPONSE(EvYardInitResult, OK);
        // There is no guarantee that unknown VDisk will recieve same OwnerId on evere TEvYardInit
        // UNIT_ASSERT_VALUES_EQUAL(Owner, LastResponse.Owner);
        Owner = LastResponse.Owner;
        UNIT_ASSERT(OwnerRound != LastResponse.OwnerRound);
        VERBOSE_COUT(" Sending TEvReadLog");
        ctx.Send(Yard, new NPDisk::TEvReadLog(Owner, OwnerRound));
        break;
    case 40:
        TEST_RESPONSE(EvReadLogResult, INVALID_ROUND);
        VERBOSE_COUT(" Sending TEvLog");
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, TRcBuf(PrepareData(100)), TLsnSeg(123, 123), (void*)456));
        break;
    case 50:
        TEST_RESPONSE(EvLogResult, INVALID_ROUND);
        VERBOSE_COUT(" Sending TEvChunkWrite");
        ChunkWriteParts.Reset(new NPDisk::TEvChunkWrite::TPart[1]);
        ChunkWriteData = PrepareData(300);
        ChunkWriteParts[0].Data = ChunkWriteData.data();
        ChunkWriteParts[0].Size = (ui32)ChunkWriteData.size();
        ctx.Send(Yard, new NPDisk::TEvChunkWrite(Owner, OwnerRound, 0, 0,
            new NPDisk::TEvChunkWrite::TNonOwningParts(ChunkWriteParts.Get(), 1), (void*)42, false, 1));
        break;
    case 60:
        TEST_RESPONSE(EvChunkWriteResult, INVALID_ROUND);
        VERBOSE_COUT("Done");
        SignalDoneEvent();
        break;
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
}

void TTestDestructionWhileWritingChunk::TestFSM(const TActorContext &ctx) {
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        break;
    case 10:
    {
        TEST_RESPONSE(EvYardInitResult, OK);
        Owner = LastResponse.Owner;
        OwnerRound = LastResponse.OwnerRound;
        AppendBlockSize = LastResponse.AppendBlockSize;
        ChunkSize = LastResponse.ChunkSize;
        VERBOSE_COUT(" Sending TEvChunkWrite");
        ChunkWriteParts.Reset(new NPDisk::TEvChunkWrite::TPart[1]);
        ChunkWriteData = PrepareData(ChunkSize, 1);
        ChunkWriteParts[0].Data = ChunkWriteData.data();
        ChunkWriteParts[0].Size = (ui32)ChunkWriteData.size();
        ctx.Send(Yard, new NPDisk::TEvChunkWrite(Owner, OwnerRound, 0, 0,
            new NPDisk::TEvChunkWrite::TNonOwningParts(ChunkWriteParts.Get(), 1), (void*)42, true, 1));
        VERBOSE_COUT("Done");
        SignalDoneEvent();
        break;
    }
    case 20:
        break;
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
}

void TTestDestructionWhileReadingChunk::TestFSM(const TActorContext &ctx) {
    TRcBuf data2(TString("testdata2"));
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        break;
    case 10:
    {
        TEST_RESPONSE(EvYardInitResult, OK);
        Owner = LastResponse.Owner;
        OwnerRound = LastResponse.OwnerRound;
        ChunkSize = LastResponse.ChunkSize;
        ctx.Send(Yard, new NPDisk::TEvChunkReserve(Owner, OwnerRound, 1));
        break;
    }
    case 20:
    {
        TEST_RESPONSE(EvChunkReserveResult, OK);
        ASSERT_YTHROW(LastResponse.ChunkIds.size() == 1,
            "Unexpected ChunkIds.size() == " << LastResponse.ChunkIds.size());
        ChunkIdx = LastResponse.ChunkIds[0];
        VERBOSE_COUT(" Sending TEvChunkWrite");
        ChunkWriteParts.Reset(new NPDisk::TEvChunkWrite::TPart[1]);
        ChunkWriteData = PrepareData(ChunkSize);
        ChunkWriteParts[0].Data = ChunkWriteData.data();
        ChunkWriteParts[0].Size = (ui32)ChunkWriteData.size();
        ctx.Send(Yard, new NPDisk::TEvChunkWrite(Owner, OwnerRound, ChunkIdx, 0,
            new NPDisk::TEvChunkWrite::TNonOwningParts(ChunkWriteParts.Get(), 1), (void*)42, false, 1));
        break;
    }
    case 30:
    {
        TEST_RESPONSE(EvChunkWriteResult, OK);
        ASSERT_YTHROW(LastResponse.Cookie == (void*)42, "Unexpected cookie=" << LastResponse.Cookie);
        VERBOSE_COUT(" Sending TEvLog to commit");
        NPDisk::TCommitRecord commitRecord;
        commitRecord.CommitChunks.push_back(ChunkIdx);
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, commitRecord, data2, TLsnSeg(1, 1), (void*)43));
        break;
    }
    case 40:
        TEST_RESPONSE(EvLogResult, OK);
        VERBOSE_COUT(" Sending TEvChunkRead");
        ctx.Send(Yard, new NPDisk::TEvChunkRead(Owner, OwnerRound, ChunkIdx, 0, ChunkSize, 1, nullptr));
        VERBOSE_COUT("Done");
        SignalDoneEvent();
        break;
    case 50:
        break;
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
}

void TTestDestructionWhileWritingLog::TestFSM(const TActorContext &ctx) {
    TString data2("testdata2");
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        break;
    case 10:
        TEST_RESPONSE(EvYardInitResult, OK);
        Owner = LastResponse.Owner;
        OwnerRound = LastResponse.OwnerRound;
        ChunkSize = LastResponse.ChunkSize;
        VERBOSE_COUT(" Sending TEvLog");
        Data = TRcBuf(PrepareData(ChunkSize));
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, Data, TLsnSeg(1, 1), (void*)43));

        VERBOSE_COUT("Done");
        SignalDoneEvent();
        break;
    case 20:
        break;
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
}

void TTestDestructionWhileReadingLog::TestFSM(const TActorContext &ctx) {
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        break;
    case 10:
        TEST_RESPONSE(EvYardInitResult, OK);
        Owner = LastResponse.Owner;
        OwnerRound = LastResponse.OwnerRound;
        ChunkSize = LastResponse.ChunkSize;
        VERBOSE_COUT(" Sending TEvLog");
        Data = TRcBuf(PrepareData(ChunkSize));
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, Data, TLsnSeg(1, 1), (void*)43));
        break;
    case 20:
        TEST_RESPONSE(EvLogResult, OK);
        ctx.Send(Yard, new NPDisk::TEvReadLog(Owner, OwnerRound));
        VERBOSE_COUT("Done");
        SignalDoneEvent();
        break;
    case 30:
        break;
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
}

void TTestChunkDeletionWhileWritingIt::TestFSM(const TActorContext &ctx) {
    TRcBuf data2(TString("testdata2"));
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        break;
    case 10:
    {
        TEST_RESPONSE(EvYardInitResult, OK);
        Owner = LastResponse.Owner;
        OwnerRound = LastResponse.OwnerRound;
        ChunkSize = LastResponse.ChunkSize;
        ctx.Send(Yard, new NPDisk::TEvChunkReserve(Owner, OwnerRound, 1));
        break;
    }
    case 20:
    {
        TEST_RESPONSE(EvChunkReserveResult, OK);
        ASSERT_YTHROW(LastResponse.ChunkIds.size() == 1,
            "Unexpected ChunkIds.size() == " << LastResponse.ChunkIds.size());
        ChunkIdx = LastResponse.ChunkIds[0];
        VERBOSE_COUT(" Sending TEvChunkWrite");
        ChunkWriteParts.Reset(new NPDisk::TEvChunkWrite::TPart[1]);
        ChunkWriteData = PrepareData(1);
        ChunkWriteParts[0].Data = ChunkWriteData.data();
        ChunkWriteParts[0].Size = (ui32)ChunkWriteData.size();
        ctx.Send(Yard, new NPDisk::TEvChunkWrite(Owner, OwnerRound, ChunkIdx, 0,
            new NPDisk::TEvChunkWrite::TNonOwningParts(ChunkWriteParts.Get(), 1), (void*)42, false, 1));
        break;
    }
    case 30:
    {
        TEST_RESPONSE(EvChunkWriteResult, OK);
        ASSERT_YTHROW(LastResponse.Cookie == (void*)42, "Unexpected cookie=" << LastResponse.Cookie);

        ChunkWriteParts.Reset(new NPDisk::TEvChunkWrite::TPart[1]);
        ChunkWriteData = PrepareData(ChunkSize - 1);
        ChunkWriteParts[0].Data = ChunkWriteData.data();
        ChunkWriteParts[0].Size = (ui32)ChunkWriteData.size();
        ctx.Send(Yard, new NPDisk::TEvChunkWrite(Owner, OwnerRound, ChunkIdx, 1,
            new NPDisk::TEvChunkWrite::TNonOwningParts(ChunkWriteParts.Get(), 1), (void*)42, false, 5));

        VERBOSE_COUT(" Sending TEvLog to commit");
        NPDisk::TCommitRecord commitRecord;
        commitRecord.DeleteChunks.push_back(ChunkIdx);
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, commitRecord, data2, TLsnSeg(1, 1), (void*)43));
        break;
    }
    case 40:
        if (LastResponse.EventType == TEvBlobStorage::EvLogResult) {
            //TEST_RESPONSE(EvLogResult, ERROR);
            IsOk = false;
        } else {
            TEST_RESPONSE(EvChunkWriteResult, OK);
            IsOk = true;
        }
        break;
    case 50:
        if (IsOk) {
            //TEST_RESPONSE(EvLogResult, OK);
        } else {
            TEST_RESPONSE(EvChunkWriteResult, OK);
        }

        VERBOSE_COUT("Done");
        SignalDoneEvent();
        break;
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
}

void TTestAllocateAllChunks::TestFSM(const TActorContext &ctx) {
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        break;
    case 10:
        TEST_RESPONSE(EvYardInitResult, OK);
        Owner = LastResponse.Owner;
        OwnerRound = LastResponse.OwnerRound;
        ChunkSize = LastResponse.ChunkSize;
        AppendBlockSize = LastResponse.AppendBlockSize;
        MessageIdx = 0;
        VERBOSE_COUT(" Sending TEvChunkReserve");
        ctx.Send(Yard, new NPDisk::TEvChunkReserve(Owner, OwnerRound, 4));
        break;
    case 20:
    {
        TEST_RESPONSE(EvChunkReserveResult, OK);
        ASSERT_YTHROW(LastResponse.ChunkIds.size() == 4,
            "Unexpected ChunkIds.size() == " << LastResponse.ChunkIds.size());
        ReservedChunks = LastResponse.ChunkIds;

        VERBOSE_COUT(" Sending multiple TEvChunkWrite to test another branch");
        ResponsesExpected = 0;
        ChunkWriteParts.Reset(new NPDisk::TEvChunkWrite::TPart[1]);
        ChunkWriteData = PrepareData(AppendBlockSize);
        ChunkWriteParts[0].Data = ChunkWriteData.data();
        ChunkWriteParts[0].Size = (ui32)ChunkWriteData.size();
        for (ui32 blockIdx = 0; blockIdx < 4; ++blockIdx) {
            for (ui32 i = 0; i < ReservedChunks.size(); ++i) {
                ui32 offset = blockIdx * AppendBlockSize;
                ctx.Send(Yard, new NPDisk::TEvChunkWrite(Owner, OwnerRound, ReservedChunks[i], offset,
                    new NPDisk::TEvChunkWrite::TNonOwningParts(ChunkWriteParts.Get(), 1), (void*)42, true, 1));
                ++ResponsesExpected;
            }
        }
        Data = TRcBuf(PrepareData(4096));
        for (ui32 logIdx = 0; logIdx < 32; ++logIdx) {
            ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, Data, TLsnSeg(logIdx+1, logIdx+1), (void*)43));
            ++ResponsesExpected;
        }
        TestStep += 10;
        break;
    }
    case 40:
    {
        if (LastResponse.EventType == TEvBlobStorage::EvChunkWriteResult) {
            TEST_RESPONSE(EvChunkWriteResult, OK);
            --ResponsesExpected;
        } else {
            TEST_RESPONSE(EvLogResult, OK);
            ResponsesExpected -= LastResponse.LogResults.size();
        }

        if (ResponsesExpected) {
            TestStep -= 10;
            break;
        }

        VERBOSE_COUT(" Sending TEvLog to commit");
        Data = TRcBuf(PrepareData(ChunkSize / 4));
        NPDisk::TCommitRecord commitRecord;
        commitRecord.CommitChunks = LastResponse.ChunkIds;
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, commitRecord, Data, TLsnSeg(33, 33), (void*)43));
        break;
    }
    case 50:
    {
        TEST_RESPONSE(EvLogResult, OK);
        VERBOSE_COUT(" Sending TEvLog to delete");
        Data = TRcBuf(PrepareData(ChunkSize / 2));
        NPDisk::TCommitRecord commitRecord;
        commitRecord.DeleteChunks.push_back(ReservedChunks.back());
        ReservedChunks.pop_back();
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, commitRecord, Data, TLsnSeg(34, 34), (void*)43));
        break;
    }
    case 60:
        TEST_RESPONSE(EvLogResult, OK);

        VERBOSE_COUT("Done");
        SignalDoneEvent();
        break;
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
}

void TTestCutMultipleLogChunks1::TestFSM(const TActorContext &ctx) {
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid, ctx.SelfID));
        break;
    case 10:
        TEST_RESPONSE(EvYardInitResult, OK);
        Owner = LastResponse.Owner;
        OwnerRound = LastResponse.OwnerRound;
        ChunkSize = LastResponse.ChunkSize;
        VERBOSE_COUT(" Owner=" << (int)Owner << " ChunkSize=" << ChunkSize);
        VERBOSE_COUT(" Sending TEvLog");
        Data = TRcBuf(PrepareData(ChunkSize / 2));
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 7, Data, TLsnSeg(1, 1), (void*)456));
        break;
    case 20:
        TEST_RESPONSE(EvLogResult, OK);
        VERBOSE_COUT(" Sending TEvLog");
        Data = TRcBuf(PrepareData(ChunkSize * 3));
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 7, Data, TLsnSeg(2, 2), (void*)456));
        break;
    case 30:
    {
        TEST_RESPONSE(EvLogResult, OK);
        VERBOSE_COUT(" Sending TEvLog commit");
        Data = TRcBuf(PrepareData(103));
        NPDisk::TCommitRecord commitRecord;
        commitRecord.FirstLsnToKeep = 3;
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 7, commitRecord, Data, TLsnSeg(3, 3), (void*)567));
        break;
    }
    case 40:
        TEST_RESPONSE(EvLogResult, OK);
        VERBOSE_COUT(" Sending TEvAskForCutLog");
        ctx.Send(Yard, new NPDisk::TEvAskForCutLog(Owner, OwnerRound));
        break;
    case 50:
    {
        TEST_RESPONSE(EvCutLog, OK);
        VERBOSE_COUT(" Sending TEvLog commit");
        Data = TRcBuf(PrepareData(104));
        NPDisk::TCommitRecord commitRecord;
        commitRecord.FirstLsnToKeep = 3;
        commitRecord.IsStartingPoint = true;
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 7, commitRecord, Data, TLsnSeg(4, 4), (void*)567));
        break;
    }
    case 60:
        TEST_RESPONSE(EvLogResult, OK);
        ctx.Send(Yard, new NPDisk::TEvChunkReserve(Owner, OwnerRound, 1));
        break;
    case 70:
        TEST_RESPONSE(EvChunkReserveResult, OK);
        VERBOSE_COUT("Done");
        SignalDoneEvent();
        break;
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
}

void TTestCutMultipleLogChunks2::TestFSM(const TActorContext &ctx) {
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        break;
    case 10:
        TEST_RESPONSE(EvYardInitResult, OK);
        Owner = LastResponse.Owner;
        OwnerRound = LastResponse.OwnerRound;
        ChunkSize = LastResponse.ChunkSize;
        ASSERT_YTHROW(LastResponse.StartingPoints.size() == 1,
            "Unexpected StartingPoints.size() = " << LastResponse.StartingPoints.size());
        ASSERT_YTHROW(LastResponse.StartingPoints.begin()->second.Data.size() == 104,
            "Unexpected StartingPoints[0].Data.size() = " <<
            LastResponse.StartingPoints.begin()->second.Data.size());
        Data = TRcBuf(PrepareData(104));
        TEST_DATA_EQUALS(LastResponse.StartingPoints.begin()->second.Data.ExtractUnderlyingContainerOrCopy<TString>(), Data.ExtractUnderlyingContainerOrCopy<TString>());

        VERBOSE_COUT("Done");
        SignalDoneEvent();
        break;
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
}

void TTestLogOwerwrite1::TestFSM(const TActorContext &ctx) {
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        break;
    case 10:
        TEST_RESPONSE(EvYardInitResult, OK);
        Owner = LastResponse.Owner;
        OwnerRound = LastResponse.OwnerRound;
        ChunkSize = LastResponse.ChunkSize;
        VERBOSE_COUT(" Owner=" << (int)Owner << " ChunkSize=" << ChunkSize);
        VERBOSE_COUT(" Sending TEvLog");
        Data = TRcBuf(PrepareData(ChunkSize * 2, 1));
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 7, Data, TLsnSeg(1, 1), (void*)456));
        break;
    case 20:
    {
        TEST_RESPONSE(EvLogResult, OK);
        VERBOSE_COUT(" Sending TEvLog commit");
        Data = TRcBuf(PrepareData(ChunkSize * 1, 2));
        NPDisk::TCommitRecord commitRecord;
        commitRecord.FirstLsnToKeep = 2;
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 7, commitRecord, Data, TLsnSeg(2, 2), (void*)567));
        break;
    }
    case 30:
    {
        TEST_RESPONSE(EvLogResult, OK);
        VERBOSE_COUT(" Sending TEvLog commit");
        Data = TRcBuf(PrepareData(ChunkSize * 1, 3));
        NPDisk::TCommitRecord commitRecord;
        commitRecord.FirstLsnToKeep = 3;
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 7, commitRecord, Data, TLsnSeg(3, 3), (void*)567));
        break;
    }
    case 40:
    {
        TEST_RESPONSE(EvLogResult, OK);
        VERBOSE_COUT(" Sending TEvLog commit");
        Data = TRcBuf(PrepareData(ChunkSize * 1, 4));
        NPDisk::TCommitRecord commitRecord;
        commitRecord.FirstLsnToKeep = 4;
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 7, commitRecord, Data, TLsnSeg(4, 4), (void*)567));
        break;
    }
    case 50:
    {
        TEST_RESPONSE(EvLogResult, OK);
        VERBOSE_COUT(" Sending TEvLog commit");
        Data = TRcBuf(PrepareData(ChunkSize * 1, 5));
        NPDisk::TCommitRecord commitRecord;
        commitRecord.FirstLsnToKeep = 4;
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 7, commitRecord, Data, TLsnSeg(5, 5), (void*)567));
        break;
    }
    case 60:
    {
        TEST_RESPONSE(EvLogResult, OK);
        VERBOSE_COUT(" Sending TEvLog commit");
        Data = TRcBuf(PrepareData(ChunkSize * 1, 6));
        NPDisk::TCommitRecord commitRecord;
        commitRecord.FirstLsnToKeep = 4;
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 7, commitRecord, Data, TLsnSeg(6, 6), (void*)567));
        break;
    }
    case 70:
    {
        TEST_RESPONSE(EvLogResult, OK);
        VERBOSE_COUT(" Sending TEvLog commit");
        Data = TRcBuf(PrepareData(107));
        NPDisk::TCommitRecord commitRecord;
        commitRecord.FirstLsnToKeep = 5;
        commitRecord.IsStartingPoint = true;
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 7, commitRecord, Data, TLsnSeg(7, 7), (void*)567));
        break;
    }
    case 80:
        TEST_RESPONSE(EvLogResult, OK);
        VERBOSE_COUT("Done");
        SignalDoneEvent();
        break;
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
}

void TTestLogOwerwrite2::TestFSM(const TActorContext &ctx) {
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        break;
    case 10:
        TEST_RESPONSE(EvYardInitResult, OK);
        Owner = LastResponse.Owner;
        OwnerRound = LastResponse.OwnerRound;
        ChunkSize = LastResponse.ChunkSize;
        ASSERT_YTHROW(LastResponse.StartingPoints.size() == 1,
            "Unexpected StartingPoints.size() = " << LastResponse.StartingPoints.size());
        ASSERT_YTHROW(LastResponse.StartingPoints.begin()->second.Data.size() == 107,
            "Unexpected StartingPoints[0].Data.size() = " <<
            LastResponse.StartingPoints.begin()->second.Data.size());
        Data = TRcBuf(PrepareData(107));
        TEST_DATA_EQUALS(LastResponse.StartingPoints.begin()->second.Data.ExtractUnderlyingContainerOrCopy<TString>(), Data.ExtractUnderlyingContainerOrCopy<TString>());

        VERBOSE_COUT("Done");
        SignalDoneEvent();
        break;
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
}

void TTestWriteAndCutLogChunk::TestFSM(const TActorContext &ctx) {
    VERBOSE_COUT("Test step " << TestStep);
    switch (TestStep) {
    case 0:
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        break;
    case 10:
        TEST_RESPONSE(EvYardInitResult, OK);
        Owner = LastResponse.Owner;
        OwnerRound = LastResponse.OwnerRound;
        VERBOSE_COUT(" Sending TEvLog");
        Data = TRcBuf(PrepareData(LastResponse.ChunkSize / 4));
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 7, Data, TLsnSeg(Lsn, Lsn), (void*)456));
        break;
    case 20:
    {
        TEST_RESPONSE(EvLogResult, OK);
        VERBOSE_COUT(" Sending TEvLog Lsn:" << Lsn);
        NPDisk::TCommitRecord commitRecord;
        ++Lsn;
        commitRecord.FirstLsnToKeep = Lsn;
        ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 7, commitRecord, Data, TLsnSeg(Lsn, Lsn), (void*)567));
        if (Lsn <= 100) {
            return;
        }
        break;
    }
    case 30:
        TEST_RESPONSE(EvLogResult, OK);
        VERBOSE_COUT("Done");
        SignalDoneEvent();
        break;
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
}

void TTestStartingPointRebootsIteration::TestFSM(const TActorContext &ctx) {
    VERBOSE_COUT("Test step " << TestStep);
    const ui32 stride = rand() % 5;
    switch (TestStep) {
    case 0:
        ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
        VERBOSE_COUT(" Sending TEvInit");
        ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
        Commit1Data = TRcBuf(PrepareData(rand() % 50030 + 10));
        break;
    case 10:
        TEST_RESPONSE(EvYardInitResult, OK);
        if (LastResponse.StartingPoints.empty()) {
            ASSERT_YTHROW(LastResponse.StatusFlags & ui32(NKikimrBlobStorage::StatusNewOwner),
                "Starting point is lost!");
        } else {
            StartingPointLsn = LastResponse.StartingPoints[0].Lsn;
        }
        Owner = LastResponse.Owner;
        OwnerRound = LastResponse.OwnerRound;
        NextLsn = StartingPointLsn + 1000000;

        VERBOSE_COUT(" Sending TEvLogRead");
        ctx.Send(Yard, new NPDisk::TEvReadLog(Owner, OwnerRound, NPDisk::TLogPosition{0, 0}, 128 << 20));
        break;
    case 20:
        TEST_RESPONSE(EvReadLogResult, OK);
        if (StartingPointLsn) {
            ASSERT_YTHROW(LastResponse.LogRecords.size() != 0,
                "Unexpected LogRecords size == " << LastResponse.LogRecords.size());
            ASSERT_YTHROW(LastResponse.IsEndOfLog,
                "Unexpected IsEndOfLog = " << (int)LastResponse.IsEndOfLog);
            NextLsn = LastResponse.LogRecords.back().Lsn + 1000000;
            FirstLsn = LastResponse.LogRecords[0].Lsn;
            VERBOSE_COUT(" Sending TEvLog");
            ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, Commit1Data, TLsnSeg(NextLsn, NextLsn),
                        (void*)43));
            NextLsn++;
            break;
        } else {
            ASSERT_YTHROW(LastResponse.LogRecords.size() == 0,
                "Unexpected LogRecords size == " << LastResponse.LogRecords.size());
            ASSERT_YTHROW(LastResponse.IsEndOfLog,
                "Unexpected IsEndOfLog = " << (int)LastResponse.IsEndOfLog);
            VERBOSE_COUT(" Sending TEvChunkReserve");
            ctx.Send(Yard, new NPDisk::TEvChunkReserve(Owner, OwnerRound, 1));
            break;
        }
    case 30:
        if (StartingPointLsn) {
            TEST_RESPONSE(EvLogResult, OK);
            VERBOSE_COUT(" Sending TEvLog");
            ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, Commit1Data, TLsnSeg(NextLsn, NextLsn),
                        (void*)43));
        } else {
            TEST_RESPONSE(EvChunkReserveResult, OK);
            ASSERT_YTHROW(LastResponse.ChunkIds.size() == 1,
                    "Unexpected ChunkIds.size() == " << LastResponse.ChunkIds.size());
            VERBOSE_COUT(" Sending TEvLog starting point");
            NPDisk::TCommitRecord commitRecord;
            commitRecord.IsStartingPoint = true;
            commitRecord.CommitChunks.push_back(LastResponse.ChunkIds[0]);
            ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, commitRecord, Commit1Data,
                TLsnSeg(NextLsn, NextLsn), (void*)43));
            StartingPointLsn = NextLsn;
        }
        NextLsn++;
        ItemsToWrite = stride;
        break;
    case 40:
        TEST_RESPONSE(EvLogResult, OK);
        if (ItemsToWrite > 0) {
            VERBOSE_COUT(" Sending TEvLog");
            ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, Commit1Data, TLsnSeg(NextLsn, NextLsn),
                        (void*)43));
            NextLsn++;
            ItemsToWrite--;
            return;
        } else {
            VERBOSE_COUT(" Sending TEvLog FirstLsnToKeep");
            if (rand() % 30 == 0) {
                NPDisk::TCommitRecord commitRecord;
                commitRecord.FirstLsnToKeep = rand() % (StartingPointLsn - FirstLsn + 1) + FirstLsn;
                ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, commitRecord, Commit1Data,
                    TLsnSeg(NextLsn, NextLsn), (void*)43));
            } else {
                VERBOSE_COUT(" Sending TEvLog");
                ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, Commit1Data, TLsnSeg(NextLsn, NextLsn),
                            (void*)43));
            }

            NextLsn++;
            ItemsToWrite = stride;

            break;
        }
    case 50:
        TEST_RESPONSE(EvLogResult, OK);
        if (ItemsToWrite > 0) {
            VERBOSE_COUT(" Sending TEvLog");
            ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, Commit1Data, TLsnSeg(NextLsn, NextLsn),
                        (void*)43));
            NextLsn++;
            ItemsToWrite--;
            return;
        } else {
            if (rand() % 30 == 0) {
                VERBOSE_COUT(" Sending TEvLog starting point");
                NPDisk::TCommitRecord commitRecord;
                commitRecord.IsStartingPoint = true;
                ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, commitRecord, Commit1Data,
                    TLsnSeg(NextLsn, NextLsn), (void*)43));
            } else {
                if (rand() % 30 == 0) {
                    VERBOSE_COUT(" Sending TEvLog FirstLsnToKeep");
                    NPDisk::TCommitRecord commitRecord;
                    commitRecord.FirstLsnToKeep = rand() % (StartingPointLsn - FirstLsn + 1) + FirstLsn;
                    ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, commitRecord, Commit1Data,
                        TLsnSeg(NextLsn, NextLsn), (void*)43));
                } else {
                    VERBOSE_COUT(" Sending TEvLog");
                    ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, Commit1Data,
                        TLsnSeg(NextLsn, NextLsn), (void*)43));
                }
            }
            NextLsn++;
            ItemsToWrite = stride;
            break;
        }
    case 60:
        TEST_RESPONSE(EvLogResult, OK);
        if (ItemsToWrite > 0) {
            VERBOSE_COUT(" Sending TEvLog");
            ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, Commit1Data, TLsnSeg(NextLsn, NextLsn),
                        (void*)43));
            NextLsn++;
            ItemsToWrite--;
            return;
        } else {
            if (rand() % 30 == 0) {
                VERBOSE_COUT(" Sending TEvLog FirstLsnToKeep");
                NPDisk::TCommitRecord commitRecord;
                commitRecord.FirstLsnToKeep = rand() % (StartingPointLsn - FirstLsn + 1) + FirstLsn;
                ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, commitRecord, Commit1Data,
                    TLsnSeg(NextLsn, NextLsn), (void*)43));
            } else {
                VERBOSE_COUT(" Sending TEvLog");
                ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, Commit1Data, TLsnSeg(NextLsn, NextLsn),
                            (void*)43));
            }
            NextLsn++;
            break;
        }
    case 70:
        TEST_RESPONSE(EvLogResult, OK);
        VERBOSE_COUT("Done");
        SignalDoneEvent();
        break;
    default:
        ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        break;
    }
    TestStep += 10;
}

} // NKikimr
