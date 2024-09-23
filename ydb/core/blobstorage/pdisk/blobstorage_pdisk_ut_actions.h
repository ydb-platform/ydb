#pragma once
#include "defs.h"

#include "blobstorage_pdisk_ut.h"
#include "blobstorage_pdisk_ut_base_test.h"
#include "blobstorage_pdisk_ut_http_request.h"
#include "blobstorage_pdisk_chunk_id_formatter.h"

#include <util/random/mersenne64.h>
#include <ydb/core/base/blobstorage_common.h>

namespace NKikimr {

template <bool IsNewOwner, ui32 GroupGeneration>
class TTestInit : public TBaseTest {
    void TestFSM(const TActorContext &ctx) {
        VERBOSE_COUT("Test step " << TestStep);
        switch (TestStep) {
        case 0:
        {
            ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));

            VERBOSE_COUT(" Sending TEvInit");
            TVDiskID vDiskId2 = VDiskID;
            vDiskId2.GroupGeneration = GroupGeneration;
            ctx.Send(Yard, new NPDisk::TEvYardInit(2, vDiskId2, *PDiskGuid));
            break;
        }
        case 10:
            TEST_RESPONSE(EvYardInitResult, OK);
            if (IsNewOwner) {
                ASSERT_YTHROW(LastResponse.StatusFlags & ui32(NKikimrBlobStorage::StatusNewOwner),
                    " Error: found existing owner!");
            } else {
                ASSERT_YTHROW(!(LastResponse.StatusFlags & ui32(NKikimrBlobStorage::StatusNewOwner)),
                    " Error: new owner created!");
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
public:
    TTestInit(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};

class TTestInitCorruptedError : public TBaseTest {
    void TestFSM(const TActorContext &ctx);
public:
    TTestInitCorruptedError(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};

class TTestInitOwner : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;

    void TestFSM(const TActorContext &ctx);
public:
    TTestInitOwner(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};

class TTestLogStartingPoint : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;

    void TestFSM(const TActorContext &ctx);
public:
    TTestLogStartingPoint(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};

class TTestIncorrectRequests : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    ui32 ChunkSize;
    ui32 ChunkIdx0;
    ui32 ChunkIdx;
    ui64 Lsn;

    TString ChunkWriteData;
    TArrayHolder<NPDisk::TEvChunkWrite::TPart> ChunkWriteParts;

    ui64 NextLsn() {
        ++Lsn;
        return Lsn;
    }

    void TestFSM(const TActorContext &ctx);
public:
    TTestIncorrectRequests(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};

class TTestEmptyLogRead : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;

    void TestFSM(const TActorContext &ctx);
public:
    TTestEmptyLogRead(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};

class TTestWholeLogRead : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;

    void TestFSM(const TActorContext &ctx) {
        Ctest << "Test step " << TestStep << Endl;
        switch (TestStep) {
        case 0:
            ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
            Ctest << " Sending TEvInit" << Endl;
            ctx.Send(Yard, new NPDisk::TEvYardInit(5, VDiskID, *PDiskGuid));
            break;
        case 10:
            TEST_RESPONSE(EvYardInitResult, OK);
            Owner = LastResponse.Owner;
            OwnerRound = LastResponse.OwnerRound;
            Ctest << " Sending TEvLogRead" << Endl;
            ctx.Send(Yard, new NPDisk::TEvReadLog(Owner, OwnerRound));
            break;
        case 20:
            TEST_RESPONSE(EvReadLogResult, OK);
            if (!LastResponse.IsEndOfLog) {
                TestStep -= 10;
                ctx.Send(Yard, new NPDisk::TEvReadLog(Owner, OwnerRound, LastResponse.NextPosition));
                break;
            } else {
                Ctest << "Done" << Endl;
                SignalDoneEvent();
                break;
            }
            break;
        default:
            ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
            break;
        }
        TestStep += 10;
    }

public:
    TTestWholeLogRead(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};

template <ui32 Size>
class TTestLogWriteRead : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;

    void TestFSM(const TActorContext &ctx) {
        TRcBuf data = TRcBuf(PrepareData(Size));
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
            ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, data, TLsnSeg(123, 123), (void*)456));
            break;
        case 20:
            TEST_RESPONSE(EvLogResult, OK);
            VERBOSE_COUT(" Sending TEvInit 2");
            ctx.Send(Yard, new NPDisk::TEvYardInit(3, VDiskID, *PDiskGuid));
            break;
        case 30:
            TEST_RESPONSE(EvYardInitResult, OK);
            Owner = LastResponse.Owner;
            OwnerRound = LastResponse.OwnerRound;
            VERBOSE_COUT(" Sending TEvLogRead");
            ctx.Send(Yard, new NPDisk::TEvReadLog(Owner, OwnerRound, NPDisk::TLogPosition{0, 0}));
            break;
        case 40:
            TEST_RESPONSE(EvReadLogResult, OK);
            ASSERT_YTHROW(LastResponse.LogRecords.size() == 1,
                "Unexpected LogRecords size == " << LastResponse.LogRecords.size());
            TEST_LOG_RECORD(LastResponse.LogRecords[0], 123, 0, data.ExtractUnderlyingContainerOrCopy<TString>());
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
public:
    TTestLogWriteRead(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};

template <ui32 Size, ui64 Lsn>
class TTestLogWrite : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;

    void TestFSM(const TActorContext &ctx) {
        TRcBuf data = TRcBuf(PrepareData(Size));
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
            ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 7, data, TLsnSeg(Lsn, Lsn), (void*)456));
            break;
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
public:
    TTestLogWrite(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};

template<bool Equal>
class TTestLogWriteCut : public TBaseTest {
    static constexpr ui32 VDiskCount = 2;

public:
    static TAtomic VDiskNum;
    static TVector<TChunkIdx> CommitedChunks[VDiskCount];

    static void Reset() {
        VDiskNum = 0;
        for (ui32 i = 0; i < VDiskCount; ++i) {
            CommitedChunks[i].clear();
        }
    }

private:

    ui32 MyNum = 0;
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    ui64 Lsn = 100;
    ui32 LogRecordSizeMin = 16 << 10;
    ui32 LogRecordSizeMax = 32 << 10;
    ui32 ChunkSize;
    ui32 EvLogsToSend;
    ui32 EvLogsReceived = 0;

    const ui32 LogRecordsToKeep = 100;

    ui32 ChunksToReserve;
    TVector<TChunkIdx> ReservedChunks;
    ui64 LastCommitLsn = 0;


    NPrivate::TMersenne64 RandGen = Seed();

    void CheckOwnedChunks(TVector<TChunkIdx>& owned) {
        std::sort(CommitedChunks[MyNum].begin(), CommitedChunks[MyNum].end());
        std::sort(owned.begin(), owned.end());

        TStringStream str;
        str << "MyNum# " << MyNum << " CommitedChunks# ";
        NPDisk::TChunkIdFormatter(str).PrintBracedChunksList(CommitedChunks[MyNum]);
        str << " owned# ";
        NPDisk::TChunkIdFormatter(str).PrintBracedChunksList(owned);
        str << Endl;
        Ctest << str.Str();
        ASSERT_YTHROW(CommitedChunks[MyNum].size() <= owned.size(), "MyNum# " << MyNum << " size mismatch, "
                << CommitedChunks[MyNum].size() << " > " << owned.size());
        for (size_t i = 0; i < CommitedChunks[MyNum].size(); ++i) {
            ASSERT_YTHROW(owned[i] == CommitedChunks[MyNum][i],
                    "MyNum# " << MyNum << " lost CommitedChunks, chunkIdx# "
                    << CommitedChunks[MyNum][i] << " != " << owned[i]);
        }
    }

    void TestFSM(const TActorContext &ctx) {
        //Ctest << "Test step " << TestStep << Endl;
        switch (TestStep) {
        case 0:
            Ctest << " Sending TEvInit" << Endl;
            ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
            break;
        case 10:
        {
            auto *ev = Event->Get<NPDisk::TEvYardInitResult>();
            ASSERT_YTHROW(ev->Status == NKikimrProto::OK, ev->ToString());
            Owner = ev->PDiskParams->Owner;
            OwnerRound = ev->PDiskParams->OwnerRound;
            ChunkSize = ev->PDiskParams->ChunkSize;
            CheckOwnedChunks(ev->OwnedChunks);
            ctx.Send(Yard, new NPDisk::TEvReadLog(Owner, OwnerRound));
            break;
        }
        case 20:
        {
            auto *ev = Event->Get<NPDisk::TEvReadLogResult>();
            ASSERT_YTHROW(ev->Status == NKikimrProto::OK && ev->IsEndOfLog, ev->ToString());

            for (const auto& res : ev->Results) {
                Lsn = Max(Lsn, res.Lsn + 1);
            }
            ctx.Send(Yard, new NPDisk::TEvCheckSpace(Owner, OwnerRound));
            Ctest << " Sending TEvLog" << Endl;
            break;
        }
        case 30:
        {
            auto *ev = Event->Get<NPDisk::TEvCheckSpaceResult>();
            ASSERT_YTHROW(ev->Status == NKikimrProto::OK, ev->ToString());
            ui32 totalChunks = ev->TotalChunks;
            ui32 logRecordSizeMean = LogRecordSizeMin + (LogRecordSizeMax - LogRecordSizeMin) / 2;
            if (Equal) {
                EvLogsToSend = totalChunks / 4 * ChunkSize / logRecordSizeMean;
            } else {
                if (MyNum == 0) {
                    // First VDisk writes only one record to first LogChunk
                    EvLogsToSend = ChunkSize / 2 / logRecordSizeMean;
                } else {
                    // Second VDisk writes full device
                    EvLogsToSend = totalChunks / 2 * ChunkSize / logRecordSizeMean + 67;
                }
            }
            Ctest << "totalChunks# " << totalChunks << " ChunkSize# " << ChunkSize << " MyNum# " << MyNum
                << " EvLogsToSend# " << EvLogsToSend << Endl;

            ChunksToReserve = ev->FreeChunks / 4;
            ctx.Send(Yard, new NPDisk::TEvChunkReserve(Owner, OwnerRound, ChunksToReserve));
            break;
        }
        case 40:
        {
            auto *ev = Event->Get<NPDisk::TEvChunkReserveResult>();
            ASSERT_YTHROW(ev->Status == NKikimrProto::OK, ev->ToString());

            ASSERT_YTHROW(ev->ChunkIds.size() == ChunksToReserve,
                "Unexpected ChunkIds.size() == " << ev->ChunkIds.size());
            ReservedChunks = std::move(ev->ChunkIds);
            ui32 logRecordSize = LogRecordSizeMin + RandGen.GenRand() % (LogRecordSizeMax - LogRecordSizeMin);
            TRcBuf data = TRcBuf(PrepareData(logRecordSize));
            NPDisk::TCommitRecord comRec{};
            comRec.FirstLsnToKeep = Lsn;
            comRec.IsStartingPoint = true;
            if (ReservedChunks) {
                comRec.CommitChunks.push_back(ReservedChunks.back());
                LastCommitLsn = Lsn;
                Ctest << "MyNum# " << MyNum << " try commit chunkIdx# " << ReservedChunks.back()
                    << " Lsn# " << Lsn << Endl;
            }
            ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, comRec, data, TLsnSeg(Lsn, Lsn), (void*)0));
            ++Lsn;
            break;
        }
        case 50:
        {
            auto *ev = Event->Get<NPDisk::TEvLogResult>();
            ASSERT_YTHROW(ev->Status == NKikimrProto::OK, ev->ToString());

            if (LastCommitLsn == Lsn - 1) {
                CommitedChunks[MyNum].push_back(ReservedChunks.back());
                Ctest << "MyNum# " << MyNum << " done commit chunkIdx# " << ReservedChunks.back()
                    << " LastCommitLsn# " << LastCommitLsn << Endl;
                ReservedChunks.pop_back();
            }
            ++EvLogsReceived;
            //Ctest << "MyNum# " << MyNum << " recieve event num# " << EvLogsReceived << " Lsn# " << Lsn << Endl;
            if (EvLogsReceived < EvLogsToSend - 1) {
                TestStep -= 10;
            }
            ui32 logRecordSize = LogRecordSizeMin + RandGen.GenRand() % (LogRecordSizeMax - LogRecordSizeMin);
            TRcBuf data = TRcBuf(PrepareData(logRecordSize));
            if (Lsn % LogRecordsToKeep == 0) {
                NPDisk::TCommitRecord comRec{};
                comRec.FirstLsnToKeep = Lsn - LogRecordsToKeep;
                if (ReservedChunks) {
                    comRec.CommitChunks.push_back(ReservedChunks.back());
                    LastCommitLsn = Lsn;
                    Ctest << "try commit chunkIdx# " << ReservedChunks.back() << " Lsn# " << Lsn << Endl;
                }
                comRec.IsStartingPoint = true;
                ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, comRec, data, TLsnSeg(Lsn, Lsn), (void*)0));
            } else {
                ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, data, TLsnSeg(Lsn, Lsn), (void*)0));
            }
            ++Lsn;
            break;
        }
        case 60:
        {
            auto *ev = Event->Get<NPDisk::TEvLogResult>();
            ASSERT_YTHROW(ev->Status == NKikimrProto::OK, ev->ToString());
            ++EvLogsReceived;
            ASSERT_YTHROW(EvLogsReceived == EvLogsToSend, "PDUT-0001");
            Ctest << "MyNum# " << MyNum << " recieve event num# " << EvLogsReceived << Endl;

            ctx.Send(Yard, new NPDisk::TEvYardInit(3, VDiskID, *PDiskGuid));
            break;
        }
        case 70:
        {
            auto *ev = Event->Get<NPDisk::TEvYardInitResult>();
            ASSERT_YTHROW(ev->Status == NKikimrProto::OK, ev->ToString());
            Owner = ev->PDiskParams->Owner;
            OwnerRound = ev->PDiskParams->OwnerRound;
            ctx.Send(Yard, new NPDisk::TEvReadLog(Owner, OwnerRound));
            break;
        }
        case 80:
        {
            auto *ev = Event->Get<NPDisk::TEvReadLogResult>();
            ASSERT_YTHROW(ev->Status == NKikimrProto::OK, ev->ToString());
            Ctest << "MyNum# " << MyNum << Endl;
            for (const NKikimr::NPDisk::TLogRecord &log : ev->Results) {
                Ctest << "Read log record# " << log.ToString();
            }
            Ctest << Endl;

            const ui64 firstLsnToKeep = Lsn - Lsn % LogRecordsToKeep;
            const ui64 lastLsnToKeep = Lsn - 1;
            bool isInsideLsnToKeep = false;
            TMaybe<const NKikimr::NPDisk::TLogRecord*> prevLog;
            for (const NKikimr::NPDisk::TLogRecord &log : ev->Results) {
                if (firstLsnToKeep <= log.Lsn) {
                    isInsideLsnToKeep = true;
                }
                ASSERT_YTHROW(log.Lsn <= lastLsnToKeep, "TestVDisk didn't wrote log with such lsn# " << log.Lsn);

                if (isInsideLsnToKeep && prevLog) {
                    ASSERT_YTHROW((**prevLog).Lsn == log.Lsn - 1, "PDisk must send log record with strongly"
                            << " increasing Lsn, prevLsn# " << (**prevLog).Lsn << " currentLsn# " << log.Lsn);
                }
                prevLog = &log;
            }
            ASSERT_YTHROW(isInsideLsnToKeep, "PDisk didn't send expected log records to TestVDisk");
            ASSERT_YTHROW(ev->Results.back().Lsn == lastLsnToKeep, "PDisk didn't send last log record with"
                    << " Lsn# " << lastLsnToKeep << " last recieved lsn# " << ev->Results.back().Lsn);

            Ctest << "Done" << Endl;
            SignalDoneEvent();
            break;
        }
        default:
            ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
            break;
        }
        TestStep += 10;
    }
public:
    TTestLogWriteCut(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {
        MyNum = AtomicGetAndIncrement(VDiskNum);
        ASSERT_YTHROW(MyNum < VDiskCount, "MyNum should be less than VDiskCount");
    }
};

template<bool Equal>
TAtomic TTestLogWriteCut<Equal>::VDiskNum = 0;

template<bool Equal>
TVector<TChunkIdx> TTestLogWriteCut<Equal>::CommitedChunks[VDiskCount];

template<ui64 LogRequests>
class TTestLogWriteLsnConsistency : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    const ui64 StartLsn = 100;
    ui64 Lsn = StartLsn;
    ui64 LastSeenLsn = 0;
    ui64 LogRequestsRecived = 0;


    void TestFSM(const TActorContext &ctx) {
        TRcBuf data = TRcBuf(PrepareData(2000));
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
            for (ui64 i = 0; i < LogRequests; ++i) {
                ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 7, data, TLsnSeg(Lsn, Lsn), (void*)456));
                ++Lsn;
            }
            break;
        case 20:
            TEST_RESPONSE(EvLogResult, OK);
            for (ui64 i = 0; i < LastResponse.LogResults.size(); ++i) {
                ui64 expectedLsn = StartLsn + LogRequestsRecived;
                ui64 actualLsn = LastResponse.LogResults[i].Lsn;
                VERBOSE_COUT("Expected Lsn# " << expectedLsn << " actual Lsn# " << actualLsn);
                ASSERT_YTHROW(expectedLsn == actualLsn, "Error in response Lsn ");
                ++LogRequestsRecived;
            }
            VERBOSE_COUT("LogRequestsRecived# " << LogRequestsRecived);
            if (LogRequestsRecived < LogRequests) {
                TestStep -= 10;
            } else if (LogRequestsRecived > LogRequests) {
                ythrow TWithBackTrace<yexception>() << "More results recived then expected " << TestStep << Endl;
            } else {
                VERBOSE_COUT("Done");
                SignalDoneEvent();
            }
            break;
        default:
            ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
            break;
        }
        TestStep += 10;
    }
public:
    TTestLogWriteLsnConsistency(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};

template <ui32 Size1, ui32 Size2, ui32 Size3>
class TTestLog3Read : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;

    void TestFSM(const TActorContext &ctx) {
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
            ctx.Send(Yard, new NPDisk::TEvReadLog(Owner, OwnerRound, NPDisk::TLogPosition{0, 0}, Size1+Size2+Size3));
            break;
        case 20:
        {
            TEST_RESPONSE(EvReadLogResult, OK);
            ASSERT_YTHROW(LastResponse.LogRecords.size() == 3,
                "Unexpected LogRecords size == " << LastResponse.LogRecords.size());
            ui32 sizes[] ={Size1, Size2, Size3};
            for (ui32 idx = 0; idx < LastResponse.LogRecords.size(); ++idx) {
                VERBOSE_COUT(" Checking idx " << idx << " size " << sizes[idx]);
                TString data = PrepareData(sizes[idx]);
                TEST_LOG_RECORD(LastResponse.LogRecords[idx], 123 + idx, 7, data);
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
public:
    TTestLog3Read(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};

template <ui32 Size1, ui32 Size2, ui32 Size3>
class TTestLog3Write : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    ui32 RecordsRemaining;

    void TestFSM(const TActorContext &ctx) {
        TRcBuf data1 = TRcBuf(PrepareData(Size1));
        TRcBuf data2 = TRcBuf(PrepareData(Size2));
        TRcBuf data3 = TRcBuf(PrepareData(Size3));
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
            VERBOSE_COUT(" Sending 3 TEvLog messages");
            ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 7, data1, TLsnSeg(123, 123), (void*)456));
            ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 7, data2, TLsnSeg(124, 124), (void*)456));
            ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 7, data3, TLsnSeg(125, 125), (void*)456));
            RecordsRemaining = 3;
            break;
        case 20:
            TEST_RESPONSE(EvLogResult, OK);
            RecordsRemaining -= LastResponse.LogResults.size();
            VERBOSE_COUT(" Got EvLogResult with " << LastResponse.LogResults.size() <<
                " records, RecordsRemaining=" << RecordsRemaining);
            if (RecordsRemaining) {
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
public:
    TTestLog3Write(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};

template <ui32 ReadSize, ui32 StepSize, ui32 StepsCount>
class TTestChunkReadRandomOffset : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    TString ChunkWriteData;
    TArrayHolder<NPDisk::TEvChunkWrite::TPart> ChunkWriteParts;
    ui32 ChunkIdx;
    ui32 ChunkReadsDone = 0;

    void TestFSM(const TActorContext &ctx) {
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
            ui32 chunkSize = LastResponse.ChunkSize;
            UNIT_ASSERT_C(StepSize * StepsCount < chunkSize, "Cannot test chunk reads with readSize < chunkSize,"
                    << " readSize# " << StepSize * StepsCount << " chunkSize# " << chunkSize);

            VERBOSE_COUT(" Sending TEvChunkReserve");
            ctx.Send(Yard, new NPDisk::TEvChunkReserve(Owner, OwnerRound, 1));
            TestStep = 15;
            return;
        }
        case 15:
        {
            TEST_RESPONSE(EvChunkReserveResult, OK);
            ASSERT_YTHROW(LastResponse.ChunkIds.size() == 1,
                "Unexpected ChunkIds.size() == " << LastResponse.ChunkIds.size());
            ChunkIdx = LastResponse.ChunkIds[0];

            VERBOSE_COUT(" Sending TEvChunkWrite");
            ChunkWriteParts.Reset(new NPDisk::TEvChunkWrite::TPart[1]);
            ChunkWriteData = PrepareData(ReadSize + StepSize * StepsCount);
            ChunkWriteParts[0].Data = ChunkWriteData.data();
            ChunkWriteParts[0].Size = (ui32)ChunkWriteData.size();
            ctx.Send(Yard, new NPDisk::TEvChunkWrite(Owner, OwnerRound, ChunkIdx, 0,
                new NPDisk::TEvChunkWrite::TNonOwningParts(ChunkWriteParts.Get(), 1), nullptr, false, 1));
            TestStep = 20;
            return;
        }
        case 20:
        {
            TEST_RESPONSE(EvChunkWriteResult, OK);
            VERBOSE_COUT(" Sending TEvLog to commit");
            NPDisk::TCommitRecord commitRecord;
            commitRecord.CommitChunks.push_back(ChunkIdx);
            ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, commitRecord, TRcBuf(TString("")), TLsnSeg(1, 1), nullptr));
            break;
        }
        case 30:
            TEST_RESPONSE(EvLogResult, OK);
            VERBOSE_COUT(" Sending TEvChunkRead");
            for (ui32 i = 0; i < StepsCount; i++) {
                ui64 offset = i * StepSize;
                ctx.Send(Yard, new NPDisk::TEvChunkRead(Owner, OwnerRound, ChunkIdx, offset, ReadSize, 1, (void*)offset));
            }
            break;
        case 40:
        {
            TEST_RESPONSE(EvChunkReadResult, OK);
            ui64 offset = reinterpret_cast<ui64>(LastResponse.Cookie);
            TString expectedData = TString(ChunkWriteData, offset, ReadSize);
            TEST_DATA_EQUALS(LastResponse.Data.ToString(), expectedData);
            ++ChunkReadsDone;

            if (ChunkReadsDone == StepsCount) {
                SignalDoneEvent();
            }
            return;
        }
        default:
            ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
            break;
        }
        TestStep += 10;
    }
public:
    TTestChunkReadRandomOffset(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
        , ChunkIdx(0)
    {}
};

template <ui32 Size1, ui32 Size2>
class TTestChunkWriteRead : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    TString ChunkWriteData;
    TArrayHolder<NPDisk::TEvChunkWrite::TPart> ChunkWriteParts;
    ui32 ChunkIdx;
    ui32 BlockSize;
    TVector<ui32> ReservedChunks;

    void TestFSM(const TActorContext &ctx) {
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
            BlockSize = LastResponse.AppendBlockSize;

            VERBOSE_COUT(" Sending TEvChunkReserve");
            ctx.Send(Yard, new NPDisk::TEvChunkReserve(Owner, OwnerRound, 1));
            TestStep = 15;
            return;
        }
        case 15:
        {
            TEST_RESPONSE(EvChunkReserveResult, OK);
            ASSERT_YTHROW(LastResponse.ChunkIds.size() == 1,
                "Unexpected ChunkIds.size() == " << LastResponse.ChunkIds.size());
            ReservedChunks = LastResponse.ChunkIds;
            ChunkIdx = ReservedChunks[0];

            VERBOSE_COUT(" Sending TEvChunkWrite");
            ChunkWriteParts.Reset(new NPDisk::TEvChunkWrite::TPart[1]);
            ChunkWriteData = PrepareData((Size1 + BlockSize - 1) / BlockSize * BlockSize);
            ChunkWriteParts[0].Data = ChunkWriteData.data();
            ChunkWriteParts[0].Size = (ui32)ChunkWriteData.size();
            ctx.Send(Yard, new NPDisk::TEvChunkWrite(Owner, OwnerRound, ChunkIdx, 0,
                new NPDisk::TEvChunkWrite::TNonOwningParts(ChunkWriteParts.Get(), 1), (void*)42, false, 1));
            TestStep = 20;
            return;
        }
        case 20:
        {
            TEST_RESPONSE(EvChunkWriteResult, OK);
            ASSERT_YTHROW(LastResponse.Cookie == (void*)42, "Unexpected cookie=" << LastResponse.Cookie);
            VERBOSE_COUT(" Sending TEvLog to commit");
            NPDisk::TCommitRecord commitRecord;
            commitRecord.CommitChunks.push_back(ChunkIdx);
            ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, commitRecord, data2, TLsnSeg(1, 1), (void*)43));
            break;
        }
        case 30:
            TEST_RESPONSE(EvLogResult, OK);
            VERBOSE_COUT(" Sending TEvChunkRead");
            ctx.Send(Yard, new NPDisk::TEvChunkRead(Owner, OwnerRound, ChunkIdx, 0, 128, 1, nullptr));
            break;
        case 40:
        {
            TEST_RESPONSE(EvChunkReadResult, OK);
            TString expectedData = TString(ChunkWriteData, 0, 128);
            TEST_DATA_EQUALS(LastResponse.Data.ToString(), expectedData);

            VERBOSE_COUT(" Sending TEvChunkRead with offset");
            ctx.Send(Yard, new NPDisk::TEvChunkRead(Owner, OwnerRound, ChunkIdx, 4 << 10, 127, 1, nullptr));
            TestStep = 45;
            return;
        }
        case 45:
        {
            TEST_RESPONSE(EvChunkReadResult, OK);
            TString expectedData = TString(ChunkWriteData, 4 << 10, 127);
            TEST_DATA_EQUALS(LastResponse.Data.ToString(), expectedData);

            VERBOSE_COUT(" Sending TEvChunkRead with offset");
            ctx.Send(Yard, new NPDisk::TEvChunkRead(Owner, OwnerRound, ChunkIdx, 127, ChunkWriteData.size() - 127, 1, nullptr));
            TestStep = 46;
            return;
        }
        case 46:
        {
            TEST_RESPONSE(EvChunkReadResult, OK);
            TString expectedData = TString(ChunkWriteData, 127, ChunkWriteData.size() - 127);
            TEST_DATA_EQUALS(LastResponse.Data.ToString(), expectedData);

            VERBOSE_COUT(" Sending TEvChunkRead with offset");
            ctx.Send(Yard, new NPDisk::TEvChunkRead(Owner, OwnerRound, ChunkIdx, 317, 128, 1, nullptr));
            TestStep = 50;
            return;
        }
        case 50:
        {
            TEST_RESPONSE(EvChunkReadResult, OK);
            TString expectedData = TString(ChunkWriteData, 317, 128);
            TEST_DATA_EQUALS(LastResponse.Data.ToString(), expectedData);

            VERBOSE_COUT(" Sending TEvChunkRead with OOB offset");
            ctx.Send(Yard, new NPDisk::TEvChunkRead(Owner, OwnerRound, ChunkIdx, (ui32)-1, 128, 1, nullptr));
            break;
        }
        case 60:
        {
            TEST_RESPONSE(EvChunkReadResult, ERROR);

            VERBOSE_COUT(" Sending TEvChunkWrite");
            ChunkWriteParts.Reset(new NPDisk::TEvChunkWrite::TPart[1]);
            ChunkWriteData = PrepareData(Size2 / BlockSize * BlockSize);
            ChunkWriteParts[0].Data = ChunkWriteData.data();
            ChunkWriteParts[0].Size = (ui32)ChunkWriteData.size();
            ctx.Send(Yard, new NPDisk::TEvChunkWrite(Owner, OwnerRound, ChunkIdx, 0,
                new NPDisk::TEvChunkWrite::TNonOwningParts(ChunkWriteParts.Get(), 1), (void*)42, false, 1));
            ctx.Send(Yard, new NPDisk::TEvChunkWrite(Owner, OwnerRound, ChunkIdx, ChunkWriteData.size(),
                new NPDisk::TEvChunkWrite::TNonOwningParts(ChunkWriteParts.Get(), 1), (void*)42, false, 1));
            break;
        }
        case 70:
            TEST_RESPONSE(EvChunkWriteResult, OK);
            ASSERT_YTHROW(LastResponse.Cookie == (void*)42, "Unexpected cookie=" << LastResponse.Cookie);
            ChunkIdx = LastResponse.ChunkIdx;
            break;
        case 80:
        {
            TEST_RESPONSE(EvChunkWriteResult, OK);
            ASSERT_YTHROW(LastResponse.Cookie == (void*)42, "Unexpected cookie=" << LastResponse.Cookie);
            ChunkIdx = LastResponse.ChunkIdx;

            VERBOSE_COUT(" Sending TEvLog to commit");
            NPDisk::TCommitRecord commitRecord;
            commitRecord.CommitChunks.push_back(ChunkIdx);
            ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, commitRecord, data2, TLsnSeg(2, 2), (void*)43));
            break;
        }
        case 90:
            TEST_RESPONSE(EvLogResult, OK);

            VERBOSE_COUT(" Sending TEvChunkRead");
            ctx.Send(Yard, new NPDisk::TEvChunkRead(Owner, OwnerRound, ChunkIdx, 0, ChunkWriteData.size(), 1, nullptr));
            ctx.Send(Yard, new NPDisk::TEvChunkRead(Owner, OwnerRound, ChunkIdx, ChunkWriteData.size(),
                        ChunkWriteData.size(), 1, nullptr));
            break;
        case 100:
            TEST_RESPONSE(EvChunkReadResult, OK);
            TEST_DATA_EQUALS(LastResponse.Data.ToString(), ChunkWriteData);
            break;
        case 110:
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
public:
    TTestChunkWriteRead(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
        , ChunkIdx(0)
    {}
};

class TTestChunkWriteReadWhole : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    TString ChunkWriteData;
    TArrayHolder<NPDisk::TEvChunkWrite::TPart> ChunkWriteParts;
    ui32 ChunkIdx;
    ui32 ChunkSize;

    void TestFSM(const TActorContext &ctx);
public:
    TTestChunkWriteReadWhole(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
        , ChunkIdx(0)
    {}
};

class TTestChunkWrite20Read02 : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    TString ChunkWriteData;
    TArrayHolder<NPDisk::TEvChunkWrite::TPart> ChunkWriteParts;
    ui32 ChunkIdx;
    ui32 BlockSize;
    TVector<ui32> ReservedChunks;
    TRcBuf CommitData;

    void TestFSM(const TActorContext &ctx);
public:
    TTestChunkWrite20Read02(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
        , ChunkIdx(0)
        , BlockSize(0)
    {}
};


class TTestChunkRecommit : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    TString ChunkWriteData1;
    TString ChunkWriteData2;
    TRcBuf Commit1Data;
    TRcBuf Commit2Data;
    TString ChunkData;
    TArrayHolder<NPDisk::TEvChunkWrite::TPart> ChunkWriteParts;
    ui32 ChunkIdx;
    ui32 BlockSize;

    void TestFSM(const TActorContext &ctx);
public:
    TTestChunkRecommit(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};


class TTestChunkRestartRecommit1 : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    TString ChunkWriteData1;
    TRcBuf Commit1Data;
    TString ChunkData;
    TArrayHolder<NPDisk::TEvChunkWrite::TPart> ChunkWriteParts;
    ui32 ChunkIdx;

    void TestFSM(const TActorContext &ctx);
public:
    TTestChunkRestartRecommit1(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};

class TTestChunkRestartRecommit2 : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    TString ChunkWriteData1;
    TString ChunkWriteData2;
    TRcBuf Commit2Data;
    TString ChunkData;
    TArrayHolder<NPDisk::TEvChunkWrite::TPart> ChunkWriteParts;
    ui32 ChunkIdx;

    void TestFSM(const TActorContext &ctx);
public:
    TTestChunkRestartRecommit2(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};

class TTestChunkDelete1 : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    TString ChunkWriteData;
    TRcBuf CommitData;
    TVector<ui32> ReservedChunks;

    void TestFSM(const TActorContext &ctx);
public:
    TTestChunkDelete1(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};

class TTestChunkDelete2 : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    TRcBuf CommitData;
    TString ChunkData;
    TVector<ui32> ReservedChunks;

    void TestFSM(const TActorContext &ctx);
public:
    TTestChunkDelete2(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};

class TTestChunkForget1 : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    TString ChunkWriteData;
    TRcBuf CommitData;
    TVector<ui32> ReservedChunks;

    void TestFSM(const TActorContext &ctx);
public:
    TTestChunkForget1(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};

template<ui32 WishDataSize>
class TTestChunk3WriteRead : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    ui32 ChunkIdx;
    ui32 Iteration;
    ui32 BlockSize;
    ui32 DataSize;
    TString ChunkWriteData;
    TArrayHolder<NPDisk::TEvChunkWrite::TPart> ChunkWriteParts;
    void TestFSM(const TActorContext &ctx) {
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
            BlockSize = LastResponse.AppendBlockSize;
            DataSize = (WishDataSize + BlockSize - 1) / BlockSize * BlockSize;
            ctx.Send(Yard, new NPDisk::TEvChunkReserve(Owner, OwnerRound, 1));
            break;
        case 20:
            TEST_RESPONSE(EvChunkReserveResult, OK);
            ASSERT_YTHROW(LastResponse.ChunkIds.size() == 1,
                "Unexpected ChunkIds.size() == " << LastResponse.ChunkIds.size());
            ChunkIdx = LastResponse.ChunkIds[0];
            Iteration = 0;
            [[fallthrough]]; // AUTOGENERATED_FALLTHROUGH_FIXME
        case 30:
        case 40:
        {
            if (Iteration) {
                TEST_RESPONSE(EvChunkWriteResult, OK);
                ASSERT_YTHROW(LastResponse.Cookie == (void*)42, "Unexpected cookie=" << LastResponse.Cookie);
            }
            VERBOSE_COUT(" Sending TEvChunkWrite");
            ChunkWriteParts.Reset(new NPDisk::TEvChunkWrite::TPart[1]);
            ChunkWriteData = PrepareData(DataSize);
            ChunkWriteParts[0].Data = ChunkWriteData.data();
            ChunkWriteParts[0].Size = (ui32)ChunkWriteData.size();
            ctx.Send(Yard, new NPDisk::TEvChunkWrite(Owner, OwnerRound, ChunkIdx, DataSize * Iteration,
                new NPDisk::TEvChunkWrite::TNonOwningParts(ChunkWriteParts.Get(), 1), (void*)42, false, 1));
            ++Iteration;
            break;
        }
        case 50:
        {
            TEST_RESPONSE(EvChunkWriteResult, OK);
            ASSERT_YTHROW(LastResponse.Cookie == (void*)42, "Unexpected cookie=" << LastResponse.Cookie);
            VERBOSE_COUT(" Sending TEvLog to commit");
            NPDisk::TCommitRecord commitRecord;
            commitRecord.CommitChunks.push_back(LastResponse.ChunkIdx);
            TRcBuf data = TRcBuf(PrepareData(32));
            ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, commitRecord, data, TLsnSeg(1, 1), (void*)43));
            break;
        }
        case 60:
            TEST_RESPONSE(EvLogResult, OK);
            VERBOSE_COUT(" Sending TEvChunkRead");
            ctx.Send(Yard, new NPDisk::TEvChunkRead(Owner, OwnerRound, ChunkIdx, 0, DataSize*3, 1, nullptr));
            break;
        case 70:
        {
            TString fullData = PrepareData(DataSize) + PrepareData(DataSize) + PrepareData(DataSize);
            TEST_RESPONSE(EvChunkReadResult, OK);
            TEST_DATA_EQUALS(LastResponse.Data.ToString(), fullData);
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
public:
    TTestChunk3WriteRead(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
        , Iteration(0)
    {}
};


class TTestInitStartingPoints : public TBaseTest {
    void TestFSM(const TActorContext &ctx);
public:
    TTestInitStartingPoints(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};

template <ui32 FirstSize, ui32 LastSize, ui32 ReadSizeLimit>
class TTestLogMultipleWriteRead : public TBaseTest {
    bool IsFirstLog;
    bool IsFirstReadLog;
    ui32 CurrentSize;
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    ui64 Lsn;

    void TestFSM(const TActorContext &ctx) {
        VERBOSE_COUT("Test step " << TestStep);
        if (TestStep == 0) {
            Lsn = 1;
            ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
            VERBOSE_COUT(" Sending TEvInit");
            ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
            TestStep += 10;
        } else if (TestStep == 10) {
            if (IsFirstLog) {
                TEST_RESPONSE(EvYardInitResult, OK);
                Owner = LastResponse.Owner;
                OwnerRound = LastResponse.OwnerRound;
                IsFirstLog = false;
            } else {
                TEST_RESPONSE(EvLogResult, OK);
            }
            VERBOSE_COUT(" Sending TEvLog Lsn: " << Lsn);
            ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 7, TRcBuf(PrepareData(CurrentSize)), TLsnSeg(Lsn, Lsn),
                        (void*)456));
            ++Lsn;
            ++CurrentSize;
            if (CurrentSize > LastSize) {
                TestStep += 10;
                CurrentSize = FirstSize;
            }
        } else if (TestStep == 20) {
            TEST_RESPONSE(EvLogResult, OK);
            Lsn = 1;
            VERBOSE_COUT(" Sending TEvInit");
            ctx.Send(Yard, new NPDisk::TEvYardInit(3, VDiskID, *PDiskGuid));
            TestStep += 10;
        } else if (TestStep == 30) {
            NPDisk::TLogPosition position{0, 0};
            if (IsFirstReadLog) {
                TEST_RESPONSE(EvYardInitResult, OK);
                Owner = LastResponse.Owner;
                OwnerRound = LastResponse.OwnerRound;
                IsFirstReadLog = false;
            } else {
                TEST_RESPONSE(EvReadLogResult, OK);
                position = LastResponse.NextPosition;
                ASSERT_YTHROW(LastResponse.LogRecords.size() >= 1,
                    "Unexpected LogRecords size == " << LastResponse.LogRecords.size());
                ui32 totalSize = 0;
                for (ui32 idx = 0; idx < LastResponse.LogRecords.size(); ++idx) {
                    TString data = PrepareData(CurrentSize);
                    TEST_LOG_RECORD(LastResponse.LogRecords[idx], Lsn, 7, data);
                    ++Lsn;
                    ASSERT_YTHROW(CurrentSize <= LastSize, "Excessive records");

                    totalSize += LastResponse.LogRecords[idx].Data.size();
                    ++CurrentSize;
                }
                ASSERT_YTHROW(totalSize <= ReadSizeLimit, "totalSize=" << totalSize << ", sizeLimit=" << ReadSizeLimit);
            }
            if (CurrentSize > LastSize) {
                ASSERT_YTHROW(LastResponse.IsEndOfLog, "Log does not end where is should to");
                VERBOSE_COUT("Done");
                SignalDoneEvent();
                TestStep += 10;
            } else {
                VERBOSE_COUT(" Sending TEvLogRead");
                ctx.Send(Yard, new NPDisk::TEvReadLog(Owner, OwnerRound, position, ReadSizeLimit));
            }
        } else {
            ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
            //TestStep += 10;
        }
    }
public:
    TTestLogMultipleWriteRead(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
        , IsFirstLog(true)
        , IsFirstReadLog(true)
        , CurrentSize(FirstSize)
    {}
};

class TTestChunkLock : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;

    void TestFSM(const TActorContext &ctx);
public:
    TTestChunkLock(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};

class TTestChunkUnlock : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    ui32 LockedNumLog;
    ui32 LockedNumPersonal;

    void TestFSM(const TActorContext &ctx);
public:
    TTestChunkUnlock(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};

class TTestChunkUnlockHarakiri : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    ui32 TotalFree;

    void TestFSM(const TActorContext &ctx);
public:
    TTestChunkUnlockHarakiri(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};

class TTestChunkUnlockRestart : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    TActorId WhiteboardID;
    TActorId NodeWardenId;

    void TestFSM(const TActorContext &ctx);
public:
    TTestChunkUnlockRestart(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};

class TTestChunkReserve : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;

    void TestFSM(const TActorContext &ctx);
public:
    TTestChunkReserve(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};

class TTestCheckSpace : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;

    void TestFSM(const TActorContext &ctx);
public:
    TTestCheckSpace(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};

class TTestHttpInfo : public TBaseTest {
    THttpRequestMock HttpRequest;
    NMonitoring::TMonService2HttpRequest MonService2HttpRequest;

    void TestFSM(const TActorContext &ctx);
public:
    TTestHttpInfo(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
        , MonService2HttpRequest(nullptr, &HttpRequest, nullptr, nullptr, "", nullptr)
    {}
};

class TTestHttpInfoFileDoesntExist : public TBaseTest {
    THttpRequestMock HttpRequest;
    NMonitoring::TMonService2HttpRequest MonService2HttpRequest;

    void TestFSM(const TActorContext &ctx);
public:
    TTestHttpInfoFileDoesntExist(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
        , MonService2HttpRequest(nullptr, &HttpRequest, nullptr, nullptr, "", nullptr)
    {}
};

class TTestBootingState : public TBaseTest {
    const ui32 HttpRequestsCount = 1000;
    THttpRequestMock HttpRequest;
    NMonitoring::TMonService2HttpRequest MonService2HttpRequest;
    bool EvYardAnswered = false;
    ui32 AnsweredRequests = 0;
    ui32 BootingAnsweredRequests = 0;
    ui32 OKAnsweredRequests = 0;
    ui32 ErrorAnsweredRequests = 0;

    void TestFSM(const TActorContext &ctx);
public:
    TTestBootingState(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
        , MonService2HttpRequest(nullptr, &HttpRequest, nullptr, nullptr, "", nullptr)
    {}
};

class TTestWhiteboard : public TBaseTest {
    bool IsPDiskResultReceived = false;
    const int ExpectedOwnerCount = 5;
    int RemainingVDiskResults = ExpectedOwnerCount;
    bool IsDiskMetricsResultReceived = false;
    bool IsPDiskLightsResultReceived = false;

    void TestFSM(const TActorContext &ctx);
    void ReceiveEvent();

public:
    TTestWhiteboard(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};

class TTestFirstRecordToKeepWriteAB : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    ui64 Lsn;
    TRcBuf Data;

    void TestFSM(const TActorContext &ctx);
public:
    TTestFirstRecordToKeepWriteAB(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
        , Owner(0)
        , Lsn(1)
    {}
};

class TTestFirstRecordToKeepReadB : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    TRcBuf Data;

    void TestFSM(const TActorContext &ctx);
public:
    TTestFirstRecordToKeepReadB(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};

class TTestLotsOfTinyAsyncLogLatency : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    TRcBuf Data;
    ui32 MessagesToSend;
    ui32 Responses;
    TInstant PreviousTime;
    TInstant StartTime;
    double DurationSum;
    double TotalResponses;
    TQueue<double> Durations;
    ui32 TotalDataSize;

    void TestFSM(const TActorContext &ctx);
public:
    TTestLotsOfTinyAsyncLogLatency(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};


class TTestLogLatency : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    TRcBuf Data;
    ui32 MessagesToSend;
    ui32 Responses;
    TInstant PreviousTime;
    TInstant StartTime;
    double DurationSum;
    double TotalResponses;
    TQueue<double> Durations;
    ui32 TotalDataSize;
    ui64 MessageIdx;

    void TestFSM(const TActorContext &ctx);
public:
    TTestLogLatency(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};

class TTestHugeChunkAndLotsOfTinyAsyncLogOrder : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    TRcBuf Data;
    ui32 MessagesToSend;
    ui32 Responses;
    ui32 TotalDataSize;
    ui32 ChunkIdx;
    TString ChunkWriteData;
    TArrayHolder<NPDisk::TEvChunkWrite::TPart> ChunkWriteParts;
    TRcBuf Commit1Data;

    TInstant PreviousTime;
    TInstant StartTime;
    double DurationSum;
    double TotalResponses;
    TQueue<double> Durations;
    ui32 BlockSize;

    void TestFSM(const TActorContext &ctx);
public:
    TTestHugeChunkAndLotsOfTinyAsyncLogOrder(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};

class TTestChunkPriorityBlock : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    ui32 SafeSize;
    TString ChunkWriteData;
    TArrayHolder<NPDisk::TEvChunkWrite::TPart> ChunkWriteParts;
    TVector<ui32> ChunkIds;
    ui32 PausedChunkWrites = 5;
    ui32 Iteration;

    void TestFSM(const TActorContext &ctx);
public:
    TTestChunkPriorityBlock(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};

class TTestLog2Records3Sectors : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    ui32 ChunkSize;
    TRcBuf Data;
    TString ChunkWriteData;
    TArrayHolder<NPDisk::TEvChunkWrite::TPart> ChunkWriteParts;

    void TestFSM(const TActorContext &ctx);
public:
    TTestLog2Records3Sectors(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};

template<int Size>
class TTestLogMoreSectors : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    TRcBuf Data;

    void TestFSM(const TActorContext &ctx) {
        VERBOSE_COUT("Test step " << TestStep);
        switch (TestStep) {
        case 0:
            Data = TRcBuf(PrepareData(Size));
            ASSERT_YTHROW(LastResponse.Status == NKikimrProto::OK, StatusToString(LastResponse.Status));
            VERBOSE_COUT(" Sending TEvInit");
            ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
            break;
        case 10:
            TEST_RESPONSE(EvYardInitResult, OK);
            Owner = LastResponse.Owner;
            OwnerRound = LastResponse.OwnerRound;
            VERBOSE_COUT(" Sending 1 TEvLog");
            ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 7, TRcBuf(Data), TLsnSeg((ui64)100500, (ui64)100500),
                        (void*)456));
            break;
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
public:
    TTestLogMoreSectors(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};

class TTestLogDamageSector3Append1 : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    TRcBuf Data;

    void TestFSM(const TActorContext &ctx);
public:
    TTestLogDamageSector3Append1(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};


class TTestLogRead2Sectors : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;

    void TestFSM(const TActorContext &ctx);
public:
    TTestLogRead2Sectors(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};


class TTestLogFillChunkPlus1 : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    ui32 ChunkSize;
    TRcBuf Data;

    void TestFSM(const TActorContext &ctx);
public:
    TTestLogFillChunkPlus1(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
        , Owner(0)
    {}
};


class TTestLogKeep5Plus1 : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    TRcBuf Data;
    ui32 ChunkSize;

    void TestFSM(const TActorContext &ctx);
public:
    TTestLogKeep5Plus1(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
        , Owner(0)
    {}
};


class TTestLogReadRecords2To5 : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    TRcBuf Data;
    ui32 ChunkSize;

    void TestFSM(const TActorContext &ctx);
public:
    TTestLogReadRecords2To5(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
        , Owner(0)
    {}
};

class TTestSysLogReordering : public TBaseTest {
    friend class TTestSysLogReorderingLogCheck;

public:
    static std::atomic<ui32> VDiskNum;

private:
    static constexpr ui32 ChunksToReserve = 20;

    enum ELogRecType : ui8 {
        EGarbage = 0,
        ECommittedChunks = 1,
        EDeleteChunk = 2,
    };

#pragma pack(push, 1)
    struct TLogRecAboutChunks {
        ELogRecType Type;

        union {
            ui32 CommittedChunks[ChunksToReserve];
            ui32 DeletedChunk;
        } Data;

        TLogRecAboutChunks() {
            memset(static_cast<void*>(this), 0, sizeof(TLogRecAboutChunks));
        }
    };
#pragma pack(pop)

    ui32 MyNum = 0;
    TVDiskID VDiskID;
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    TVector<ui32> CommittedChunks;
    const ui32 LogRecordSize = 2000;
    const ui32 LogRecordsToWrite = 2000;
    const ui32 ReleaseLsnStepSize = LogRecordsToWrite / ChunksToReserve;
    ui32 LogRecordsWritten = 0;
    ui32 DeletedChunks = 0;
    TString Garbage;
    ui32 Lsn = 0; // First written Lsn will be 1, last EvLog with data will be LogRecordsToWrite

    TLsnSeg GenLsnSeg() {
        ++Lsn;
        return {Lsn, Lsn};
    }

    void SendEvLog(const TActorContext& ctx, TMaybe<NPDisk::TCommitRecord> commit, TRcBuf data) {
        if (commit) {
            ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, *commit, data, GenLsnSeg(), nullptr));
        } else {
            ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, data, GenLsnSeg(), nullptr));
        }
    }

    void TestFSM(const TActorContext &ctx) {
        VERBOSE_COUT("Test step " << TestStep);
        switch (TestStep) {
        case 0:
            VERBOSE_COUT(" Sending TEvInit");
            VDiskID.GroupID = TGroupId::FromValue(MyNum);
            ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
            break;
        case 10:
        {
            auto* ev = Event->Get<NPDisk::TEvYardInitResult>();
            ASSERT_YTHROW(ev->Status == NKikimrProto::OK, ev->ToString());
            Owner = ev->PDiskParams->Owner;
            OwnerRound = ev->PDiskParams->OwnerRound;
            ctx.Send(Yard, new NPDisk::TEvChunkReserve(Owner, OwnerRound, ChunksToReserve));
            break;
        }
        case 20:
        {
            auto* ev = Event->Get<NPDisk::TEvChunkReserveResult>();
            ASSERT_YTHROW(ev->Status == NKikimrProto::OK, ev->ToString());
            ASSERT_YTHROW(ev->ChunkIds.size() == ChunksToReserve,
                "Unexpected ChunkIds.size() == " << ev->ChunkIds.size());
            VERBOSE_COUT(" Sending TEvLog to commit");

            CommittedChunks = ev->ChunkIds;

            NPDisk::TCommitRecord commitRecord;
            commitRecord.CommitChunks = ev->ChunkIds;
            commitRecord.IsStartingPoint = true;
            SendEvLog(ctx, commitRecord, {});
            break;
        }
        case 30:
        {
            auto* ev = Event->Get<NPDisk::TEvLogResult>();
            ASSERT_YTHROW(ev->Status == NKikimrProto::OK, ev->ToString());
            VERBOSE_COUT(" Sending TEvLog to commit");
            ++LogRecordsWritten;

            if (LogRecordsWritten < LogRecordsToWrite) {
                SendEvLog(ctx, {}, TRcBuf(Garbage));
                TestStep -= 10;
            } else {
                TLogRecAboutChunks log;
                log.Type = ECommittedChunks;
                for (ui32 i = 0; i < CommittedChunks.size(); ++i) {
                    log.Data.CommittedChunks[i] = CommittedChunks[i];
                }
                TRcBuf commitedChunksList = TRcBuf::Uninitialized(sizeof(log));
                memcpy(commitedChunksList.UnsafeGetDataMut(), &log, sizeof(log));
                NPDisk::TCommitRecord commitRecord;
                commitRecord.IsStartingPoint = true;
                SendEvLog(ctx, commitRecord, commitedChunksList);
            }
            break;
        }
        case 40:
        {
            auto* ev = Event->Get<NPDisk::TEvLogResult>();
            ASSERT_YTHROW(ev->Status == NKikimrProto::OK, ev->ToString());

            if (DeletedChunks < ChunksToReserve / 2) {
                NPDisk::TCommitRecord commitRecord;
                commitRecord.FirstLsnToKeep = 1 + ReleaseLsnStepSize * (DeletedChunks + 1);
                Y_ABORT_UNLESS(commitRecord.FirstLsnToKeep <= LogRecordsToWrite + 1);
                TLogRecAboutChunks log;
                log.Type = EDeleteChunk;
                log.Data.DeletedChunk = CommittedChunks.back();
                Ctest << "MyNum# " << MyNum << " Delete chunk# " << CommittedChunks.back() << Endl;
                TRcBuf deleteChunkLog = TRcBuf::Uninitialized(sizeof(log));
                memcpy(deleteChunkLog.UnsafeGetDataMut(), &log, sizeof(log));
                commitRecord.DeleteChunks.push_back(CommittedChunks.back());
                CommittedChunks.pop_back();

                SendEvLog(ctx, commitRecord, deleteChunkLog);
                ++DeletedChunks;
                TestStep -= 10;
            } else {
                SignalDoneEvent();
            }
            break;
        }
        default:
            ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
            break;
        }
        TestStep += 10;
    }

public:
    TTestSysLogReordering(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {
        MyNum = VDiskNum.fetch_add(1);
        Garbage = PrepareData(LogRecordSize);
        TLogRecAboutChunks log;
        log.Type = EGarbage;
        Y_ABORT_UNLESS(LogRecordSize >= sizeof(log));
        memcpy(Garbage.Detach(), &log, sizeof(log));
    }
};

class TTestSysLogReorderingLogCheck : public TBaseTest {
public:
    static std::atomic<ui32> VDiskNum;

private:
    ui32 MyNum = 0;
    TVDiskID VDiskID;
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    TVector<ui32> CommittedChunks;
    TVector<ui32> DeletedChunks; // Delete CommitRecords that was written to disk
    TVector<ui32> OwnedChunks; // PDisk version of which chunks is owned

    void CheckChunksNotMissed() {
        std::sort(CommittedChunks.begin(), CommittedChunks.end());
        std::sort(DeletedChunks.begin(), DeletedChunks.end());
        std::sort(OwnedChunks.begin(), OwnedChunks.end());
        TVector<ui32> knownChunks(DeletedChunks.begin(), DeletedChunks.end());
        knownChunks.insert(knownChunks.end(), OwnedChunks.begin(), OwnedChunks.end());

        std::sort(knownChunks.begin(), knownChunks.end());

        TStringStream str;
        str << "MyNum# " << MyNum << " ";
        str << Endl;
        str << "CommittedChunks# ";
        NPDisk::TChunkIdFormatter(str).PrintBracedChunksList(CommittedChunks);
        str << Endl;
        str << "OwnedChunks# ";
        NPDisk::TChunkIdFormatter(str).PrintBracedChunksList(OwnedChunks);
        str << Endl;
        str << "DeletedChunks# ";
        NPDisk::TChunkIdFormatter(str).PrintBracedChunksList(DeletedChunks);
        str << Endl;
        Ctest << str.Str();

        ASSERT_YTHROW(CommittedChunks == knownChunks, "Some chunks missed " << str.Str());
    }

    void TestFSM(const TActorContext &ctx) {
        VERBOSE_COUT("Test step " << TestStep);
        switch (TestStep) {
        case 0:
            VERBOSE_COUT(" Sending TEvInit");
            VDiskID.GroupID = TGroupId::FromValue(MyNum);
            ctx.Send(Yard, new NPDisk::TEvYardInit(2, VDiskID, *PDiskGuid));
            break;
        case 10:
        {
            auto* ev = Event->Get<NPDisk::TEvYardInitResult>();
            ASSERT_YTHROW(ev->Status == NKikimrProto::OK, ev->ToString());
            Owner = ev->PDiskParams->Owner;
            OwnerRound = ev->PDiskParams->OwnerRound;
            OwnedChunks = ev->OwnedChunks;
            ctx.Send(Yard, new NPDisk::TEvReadLog(Owner, OwnerRound, NPDisk::TLogPosition{0, 0}));
            break;
        }
        case 20:
        {
            auto* ev = Event->Get<NPDisk::TEvReadLogResult>();
            ASSERT_YTHROW(ev->Status == NKikimrProto::OK, ev->ToString());
            ASSERT_YTHROW(ev->IsEndOfLog, ev->ToString());

            for (const NKikimr::NPDisk::TLogRecord &logRec : LastResponse.LogRecords) {
                auto *log = reinterpret_cast<const TTestSysLogReordering::TLogRecAboutChunks*>(logRec.Data.data());
                switch (log->Type) {
                case TTestSysLogReordering::EGarbage:
                    break;
                case TTestSysLogReordering::ECommittedChunks:
                    for (ui32 i = 0; i < TTestSysLogReordering::ChunksToReserve; ++i) {
                        const ui32 chunk = log->Data.CommittedChunks[i];
                        CommittedChunks.push_back(chunk);
                    }
                    break;
                case TTestSysLogReordering::EDeleteChunk:
                    const ui32 chunk = log->Data.DeletedChunk;
                    DeletedChunks.push_back(chunk);
                    break;
                }
            }

            CheckChunksNotMissed();
            SignalDoneEvent();
            break;
        }
        case 30:
        {
            break;
        }
        default:
            ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
            break;
        }
        TestStep += 10;
    }

public:
    TTestSysLogReorderingLogCheck(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {
        MyNum = VDiskNum.fetch_add(1);
    }
};

template <ui32 Size>
class TTestCommitChunks : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    TRcBuf Commit1Data;
    TString ChunkData;
    ui32 ChunkIdx;

    void TestFSM(const TActorContext &ctx) {
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
            ctx.Send(Yard, new NPDisk::TEvChunkReserve(Owner, OwnerRound, Size));
            break;
        case 20:
            TEST_RESPONSE(EvChunkReserveResult, OK);
            ASSERT_YTHROW(LastResponse.ChunkIds.size() == Size,
                "Unexpected ChunkIds.size() == " << LastResponse.ChunkIds.size());

            VERBOSE_COUT(" Sending TEvLog to commit");
            NPDisk::TCommitRecord commitRecord;
            for (size_t i = 0; i < LastResponse.ChunkIds.size(); ++i) {
                commitRecord.CommitChunks.push_back(LastResponse.ChunkIds[i]);
            }
            commitRecord.IsStartingPoint = true;
            ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 0, commitRecord, TRcBuf(Commit1Data), TLsnSeg(1, 1),
                        (void*)43));
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
public:
    TTestCommitChunks(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};


class TTestWriteAndReleaseChunk2A : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    ui32 AppendBlockSize;
    TString ChunkWriteData;
    TArrayHolder<NPDisk::TEvChunkWrite::TPart> ChunkWriteParts;
    ui32 ChunkIdx;
    TRcBuf commitData;

    void TestFSM(const TActorContext &ctx);
public:
    TTestWriteAndReleaseChunk2A(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};


class TTestWriteAndCheckChunk2B : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    ui32 AppendBlockSize;
    TString ChunkWriteData;
    TArrayHolder<NPDisk::TEvChunkWrite::TPart> ChunkWriteParts;
    ui32 ChunkIdx;
    TRcBuf Data;

    void TestFSM(const TActorContext &ctx);
public:
    TTestWriteAndCheckChunk2B(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};

class TTestCheckErrorChunk2B : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    ui32 AppendBlockSize;
    TString ChunkWriteData;
    TArrayHolder<NPDisk::TEvChunkWrite::TPart> ChunkWriteParts;
    ui32 ChunkIdx;

    void TestFSM(const TActorContext &ctx);
public:
    TTestCheckErrorChunk2B(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
        , ChunkIdx(0)
    {}
};

class TTestWriteChunksAndLog : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    ui32 AppendBlockSize;
    ui32 ChunkSize;
    TString ChunkWriteData;
    TArrayHolder<NPDisk::TEvChunkWrite::TPart> ChunkWriteParts;
    ui32 ChunkIdx2;
    ui32 ChunkIdx3;
    TRcBuf commitData;
    ui64 Lsn;

    ui64 NextLsn() {
        ++Lsn;
        return Lsn;
    }

    void TestFSM(const TActorContext &ctx);
public:
    TTestWriteChunksAndLog(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};

class TTestContinueWriteLogChunk : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    ui32 AppendBlockSize;
    ui32 ChunkSize;
    ui64 Lsn;
    ui64 SentSize = 0;

    ui64 NextLsn() {
        ++Lsn;
        return Lsn;
    }

    void TestFSM(const TActorContext &ctx);
public:
    TTestContinueWriteLogChunk(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};

class TTestLastLsn : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    ui32 AppendBlockSize;
    ui32 ChunkSize;
    ui64 Lsn;
    void TestFSM(const TActorContext &ctx);
public:
    TTestLastLsn(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};

class TTestCheckLog : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    ui32 AppendBlockSize;
    ui32 ChunkSize;
    TString ChunkWriteData;
    TArrayHolder<NPDisk::TEvChunkWrite::TPart> ChunkWriteParts;
    TRcBuf commitData;

    void TestFSM(const TActorContext &ctx);
public:
    TTestCheckLog(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};

class TTestChunkFlush : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    ui32 AppendBlockSize;
    TString ChunkWriteData;
    TArrayHolder<NPDisk::TEvChunkWrite::TPart> ChunkWriteParts;
    ui32 ChunkIdx;
    TRcBuf commitData;

    void TestFSM(const TActorContext &ctx);
public:
    TTestChunkFlush(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};

class TTestChunkUnavailable : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    ui32 AppendBlockSize;
    ui32 ChunkIdx;

    void TestFSM(const TActorContext &ctx);
public:
    TTestChunkUnavailable(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};

class TTestRedZoneSurvivability : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    ui32 ChunkSize;
    TVector<TChunkIdx> ChunkIds;

    void TestFSM(const TActorContext &ctx);
public:
    TTestRedZoneSurvivability(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};

class TTestFillDiskPhase1 : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    ui32 ChunkSize;
    TRcBuf Data;
    ui32 MessageIdx;
    TString ChunkWriteData;
    TArrayHolder<NPDisk::TEvChunkWrite::TPart> ChunkWriteParts;
    ui32 ChunkId;

    void TestFSM(const TActorContext &ctx);
public:
    TTestFillDiskPhase1(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};

class TTestFillDiskPhase2 : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    ui32 ChunkSize;
    TRcBuf Data;
    ui32 MessageIdx;
    TString ChunkWriteData;
    TArrayHolder<NPDisk::TEvChunkWrite::TPart> ChunkWriteParts;
    ui64 SentCount = 0;

    void TestFSM(const TActorContext &ctx);
public:
    TTestFillDiskPhase2(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};

class TTestHarakiri : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    ui32 ChunkIdx;
    TString ChunkWriteData;
    TArrayHolder<NPDisk::TEvChunkWrite::TPart> ChunkWriteParts;
    ui32 GoodChunkCount;

    void TestFSM(const TActorContext &ctx);
public:
    TTestHarakiri(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
        , ChunkIdx(0)
    {}
};

class TTestSimpleHarakiri : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;

    void TestFSM(const TActorContext &ctx);
public:
    TTestSimpleHarakiri(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};

class TTestSlay : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    TString ChunkWriteData;
    TArrayHolder<NPDisk::TEvChunkWrite::TPart> ChunkWriteParts;

    void TestFSM(const TActorContext &ctx);
public:
    TTestSlay(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};

class TTestSlayRace : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    TString ChunkWriteData;
    TArrayHolder<NPDisk::TEvChunkWrite::TPart> ChunkWriteParts;

    void TestFSM(const TActorContext &ctx);
public:
    TTestSlayRace(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};

class TTestSlayRecreate : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    TString ChunkWriteData;
    TArrayHolder<NPDisk::TEvChunkWrite::TPart> ChunkWriteParts;
    TVDiskID VDiskId2;

    void TestFSM(const TActorContext &ctx);
public:
    TTestSlayRecreate(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};



class TActorTestSlayLogWriteRace final : public TCommonBaseTest {
    void HandleBoot(TEvTablet::TEvBoot::TPtr &, const TActorContext &ctx) {
        ctx.Send(Yard, new NPDisk::TEvYardInit(3, VDiskID, *PDiskGuid));
    }

    void Handle(NPDisk::TEvYardInitResult::TPtr &event, const TActorContext &ctx) {
        auto *ev = event->Get();
        if (ev->Status != NKikimrProto::OK) {
            SignalError(ev->ToString());
            return;
        }

        Owner = ev->PDiskParams->Owner;
        OwnerRound = ev->PDiskParams->OwnerRound;
        if (FirstRound) {
            for (size_t i = 0; i < LogWriteCount; ++i) {
                if (i == LogWriteCount - 10) {
                    ctx.Send(Yard, new NPDisk::TEvSlay(VDiskID, OwnerRound + 1, 1, 1));
                }
                TRcBuf data = TRcBuf(PrepareData(842));
                if (i == 0) {
                    NPDisk::TCommitRecord commit;
                    commit.IsStartingPoint = true;
                    ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 1, commit, data, TLsnSeg(i+1, i+1), nullptr));
                } else {
                    ctx.Send(Yard, new NPDisk::TEvLog(Owner, OwnerRound, 1, data, TLsnSeg(i+1, i+1), nullptr));
                }
            }
        } else {
            if (!ev->OwnedChunks.empty()) {
                SignalError(ev->ToString());
                return;
            }
            // This sleep doesn't fix a problem with old VDisk's log enteries after slay
            // Sleep(TDuration::Seconds(5));
            ctx.Send(Yard, new NPDisk::TEvReadLog(Owner, OwnerRound));
        }
    }

    void Handle(NPDisk::TEvLogResult::TPtr &event, const TActorContext &) {
        auto *ev = event->Get();
        if (ev->Status != NKikimrProto::OK) {
            return;
        }

        RecievedLogWrites += ev->Results.size();
    }

    void Handle(NPDisk::TEvReadLogResult::TPtr &event, const TActorContext &) {
        auto *ev = event->Get();
        if (ev->Status != NKikimrProto::OK) {
            SignalError(ev->ToString());
            return;
        }

        if (ev->Results) {
            SignalError("Non empty result for newly created Owner" + ev->ToString());
        } else {
            SignalDoneEvent();
        }
    }

    void Handle(NPDisk::TEvSlayResult::TPtr &event, const TActorContext &ctx) {
        auto *ev = event->Get();
        if (ev->Status != NKikimrProto::OK) {
            SignalError(ev->ToString());
            return;
        }
        FirstRound = false;
        // This sleep prevents the problem
        // Sleep(TDuration::Seconds(5));
        ctx.Send(Yard, new NPDisk::TEvYardInit(OwnerRound + 1, VDiskID, *PDiskGuid));
    }

public:
    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTablet::TEvBoot, HandleBoot);
            HFunc(NPDisk::TEvYardInitResult, Handle);
            HFunc(NPDisk::TEvLogResult, Handle);
            HFunc(NPDisk::TEvReadLogResult, Handle);
            HFunc(NPDisk::TEvSlayResult, Handle);
        }
    }

    TActorTestSlayLogWriteRace(const TIntrusivePtr<TTestConfig> &cfg)
        : TCommonBaseTest(cfg)
    {
        this->UnsafeBecome(&TActorTestSlayLogWriteRace::StateFunc);
    }

private:
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    const size_t LogWriteCount = 1e5;
    size_t RecievedLogWrites = 0;
    bool FirstRound = true;
};

class TTestDestructionWhileWritingChunk : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    ui32 AppendBlockSize;
    ui32 ChunkSize;
    TString ChunkWriteData;
    TArrayHolder<NPDisk::TEvChunkWrite::TPart> ChunkWriteParts;
    TRcBuf commitData;

    void TestFSM(const TActorContext &ctx);
public:
    TTestDestructionWhileWritingChunk(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};

class TTestDestructionWhileReadingChunk : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    ui32 ChunkSize;
    TString ChunkWriteData;
    TArrayHolder<NPDisk::TEvChunkWrite::TPart> ChunkWriteParts;
    ui32 ChunkIdx;

    void TestFSM(const TActorContext &ctx);
public:
    TTestDestructionWhileReadingChunk(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
        , ChunkIdx(0)
    {}
};

class TTestDestructionWhileWritingLog : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    ui32 ChunkSize;
    TRcBuf Data;

    void TestFSM(const TActorContext &ctx);
public:
    TTestDestructionWhileWritingLog(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};

class TTestDestructionWhileReadingLog : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    ui32 ChunkSize;
    TRcBuf Data;

    void TestFSM(const TActorContext &ctx);
public:
    TTestDestructionWhileReadingLog(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};

class TTestChunkDeletionWhileWritingIt : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    ui32 ChunkSize;
    TString ChunkWriteData;
    TArrayHolder<NPDisk::TEvChunkWrite::TPart> ChunkWriteParts;
    ui32 ChunkIdx;
    bool IsOk;

    void TestFSM(const TActorContext &ctx);
public:
    TTestChunkDeletionWhileWritingIt(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};

class TTestAllocateAllChunks : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    ui32 ChunkSize;
    TRcBuf Data;
    ui32 MessageIdx;
    TString ChunkWriteData;
    TArrayHolder<NPDisk::TEvChunkWrite::TPart> ChunkWriteParts;
    TVector<ui32> ReservedChunks;
    ui32 AppendBlockSize;
    ui32 ResponsesExpected;

    void TestFSM(const TActorContext &ctx);
public:
    TTestAllocateAllChunks(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};

class TTestCutMultipleLogChunks1 : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    ui32 ChunkSize;
    TRcBuf Data;

    void TestFSM(const TActorContext &ctx);
public:
    TTestCutMultipleLogChunks1(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};

class TTestCutMultipleLogChunks2 : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    ui32 ChunkSize;
    TRcBuf Data;

    void TestFSM(const TActorContext &ctx);
public:
    TTestCutMultipleLogChunks2(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};

class TTestLogOwerwrite1 : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    ui32 ChunkSize;
    TRcBuf Data;

    void TestFSM(const TActorContext &ctx);
public:
    TTestLogOwerwrite1(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};

class TTestLogOwerwrite2 : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    ui32 ChunkSize;
    TRcBuf Data;

    void TestFSM(const TActorContext &ctx);
public:
    TTestLogOwerwrite2(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
    {}
};

class TTestWriteAndCutLogChunk : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    ui64 Lsn;
    TRcBuf Data;

    void TestFSM(const TActorContext &ctx);
public:
    TTestWriteAndCutLogChunk(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
        , Owner(0)
        , Lsn(1)
    {}
};


class TTestStartingPointRebootsIteration : public TBaseTest {
    NPDisk::TOwner Owner;
    NPDisk::TOwnerRound OwnerRound;
    TRcBuf Commit1Data;
    ui64 FirstLsn;
    ui64 StartingPointLsn;
    ui64 NextLsn;
    ui32 ItemsToWrite;
    TString ChunkWriteData;
    TArrayHolder<NPDisk::TEvChunkWrite::TPart> ChunkWriteParts;

    void TestFSM(const TActorContext &ctx);
public:
    TTestStartingPointRebootsIteration(const TIntrusivePtr<TTestConfig> &cfg)
        : TBaseTest(cfg)
        , FirstLsn(0)
        , StartingPointLsn(0)
        , NextLsn(1)
    {}
};


} // NKikimr
