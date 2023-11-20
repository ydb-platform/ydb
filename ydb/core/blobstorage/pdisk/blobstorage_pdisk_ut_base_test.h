#pragma once
#include "defs.h"

#include "blobstorage_pdisk_ut.h"

#include "blobstorage_pdisk_ut_config.h"

namespace NKikimr {

class TCommonBaseTest : public TActor<TCommonBaseTest> {
protected:
    void SignalDoneEvent() {
        AtomicIncrement(*DoneCounter);
        DoneEvent->Signal();
    }

    void SignalError(const TString& error) {
        SignalExceptionEvent(TWithBackTrace<yexception>() << error);
    }

    void SignalExceptionEvent(const yexception& ex) {
        *LastException = ex;
        AtomicSet(*IsLastExceptionSet, 1);
        SignalDoneEvent();
    }

public:
    TCommonBaseTest(const TIntrusivePtr<TTestConfig> &cfg)
        : TActor(&TThis::FakeState)
        , Yard(cfg->YardActorID)
        , VDiskID(cfg->VDiskID)
        , TestStep(0)
    {}

    void Init(TAtomic *doneCounter, TSystemEvent *doneEvent, yexception *lastException, TAtomic *isLastExceptionSet,
            ui64 *pDiskGuid) {
        DoneCounter = doneCounter;
        DoneEvent = doneEvent;
        LastException = lastException;
        IsLastExceptionSet = isLastExceptionSet;
        PDiskGuid = pDiskGuid;
    }

    STFUNC(FakeState) {
        Y_UNUSED(ev);
        Y_ABORT("This class cannot be used directly. For tests inherit from it");
    }

protected:
    const TActorId Yard;
    const TVDiskID VDiskID;
    int TestStep;

    TAtomic *DoneCounter = nullptr;
    TSystemEvent *DoneEvent = nullptr;
    yexception *LastException = nullptr;
    TAtomic *IsLastExceptionSet = nullptr;
    ui64 *PDiskGuid = nullptr;
};

class TBaseTest : public TCommonBaseTest {

protected:
    struct TResponseData {
        TMap<TLogSignature, NPDisk::TLogRecord> StartingPoints;
        TBufferWithGaps Data;
        TVector<NPDisk::TLogRecord> LogRecords;
        NPDisk::TEvLogResult::TResults LogResults;
        TVector<TChunkIdx> ChunkIds;
        TVector<TChunkIdx> OwnedChunks;

        void *Cookie;
        NPDisk::TLogPosition NextPosition;
        ui32 ChunkIdx;
        ui32 Offset;
        ui32 ChunkSize;
        ui32 AppendBlockSize;
        ui32 FreeChunks;
        ui32 TotalChunks;
        ui32 UsedChunks;
        ui32 UnlockedChunks;
        TEvBlobStorage::EEv EventType;
        NKikimrProto::EReplyStatus Status;
        NPDisk::TOwner Owner;
        NPDisk::TOwnerRound OwnerRound;
        TLogSignature Signature;
        bool IsEndOfLog;
        NPDisk::TStatusFlags StatusFlags;
        NMon::TEvHttpInfoRes *HttpResult;
        NNodeWhiteboard::TEvWhiteboard::TEvPDiskStateUpdate *whiteboardPDiskResult;
        NNodeWhiteboard::TEvWhiteboard::TEvVDiskStateUpdate *whiteboardVDiskResult;
        TEvBlobStorage::TEvControllerUpdateDiskStatus *whiteboardDiskMetricsResult;

        TResponseData() {
            Clear();
        }

        void Clear() {
            StartingPoints.clear();
            Data.Clear();
            LogRecords.clear();
            LogResults.clear();
            ChunkIds.clear();
            OwnedChunks.clear();

            Cookie = (void*)((ui64)-1);
            NextPosition = NPDisk::TLogPosition{0, 0};
            ChunkIdx = 0;
            Offset = (ui32)-1;
            ChunkSize = (ui32)-1;
            AppendBlockSize = (ui32)-1;
            EventType = TEvBlobStorage::EvPut;
            Status = NKikimrProto::OK;
            Owner = 0;
            OwnerRound = 0;
            Signature = 0;
            IsEndOfLog = true;
            StatusFlags = 0;
            HttpResult = nullptr;
            whiteboardPDiskResult = nullptr;
            whiteboardVDiskResult = nullptr;
            whiteboardDiskMetricsResult = nullptr;
        }

        void Check() {
            for (TMap<TLogSignature, NPDisk::TLogRecord>::iterator it = StartingPoints.begin();
                it != StartingPoints.end(); ++it) {
                REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&it->first, sizeof(it->first));
                it->second.Verify();
            }
            if (Data.Size()) {
                Data.Sanitize();
            }
            for (TVector<NPDisk::TLogRecord> ::iterator it = LogRecords.begin(); it != LogRecords.end(); ++it) {
                REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&it->Signature, sizeof(it->Signature));
                REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(it->Data.Data(), it->Data.Size());
                REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&it->Lsn, sizeof(it->Lsn));
            }
            for (NPDisk::TEvLogResult::TResults::iterator it = LogResults.begin();
                it != LogResults.end(); ++it) {
                REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&it->Lsn, sizeof(it->Lsn));
                REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&it->Cookie, sizeof(it->Cookie));
            }
            for (TVector<TChunkIdx>::iterator it = ChunkIds.begin(); it != ChunkIds.end(); ++it) {
                REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&*it, sizeof(*it));
            }
            for (TVector<TChunkIdx>::iterator it = OwnedChunks.begin(); it != OwnedChunks.end(); ++it) {
                REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&*it, sizeof(*it));
            }

            REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&Cookie, sizeof(Cookie));
            REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&NextPosition, sizeof(NextPosition));
            REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&ChunkIdx, sizeof(ChunkIdx));
            REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&Offset, sizeof(Offset));
            REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&ChunkSize, sizeof(ChunkSize));
            REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&AppendBlockSize, sizeof(AppendBlockSize));
            REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&EventType, sizeof(EventType));
            REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&Status, sizeof(Status));
            REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&Owner, sizeof(Owner));
            REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&Signature, sizeof(Signature));
            REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&IsEndOfLog, sizeof(IsEndOfLog));
            REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&StatusFlags, sizeof(StatusFlags));
        }
    };

    TResponseData LastResponse;
    IEventHandle *Event;

    virtual void TestFSM(const TActorContext &ctx) = 0;


    void ActTestFSM(const TActorContext &ctx) {
        LastResponse.Check();
        try {
            TestFSM(ctx);
            LastResponse.Clear();
        } catch (const yexception& ex) {
            SignalExceptionEvent(ex);
        }
    }

    void HandleBoot(TEvTablet::TEvBoot::TPtr &ev, const TActorContext &ctx) {
        ActTestFSM(ctx);
        Y_UNUSED(ev);
    }

    void Handle(NPDisk::TEvYardInitResult::TPtr &ev, const TActorContext &ctx) {
        NPDisk::TEvYardInitResult &result = *(ev->Get());
        LastResponse.Status = result.Status;
        LastResponse.EventType = (TEvBlobStorage::EEv)result.Type();
        LastResponse.Owner = result.PDiskParams->Owner;
        LastResponse.OwnerRound = result.PDiskParams->OwnerRound;
        LastResponse.StartingPoints = result.StartingPoints;
        LastResponse.ChunkSize = result.PDiskParams->ChunkSize;
        LastResponse.AppendBlockSize = result.PDiskParams->AppendBlockSize;
        LastResponse.StatusFlags = result.StatusFlags;
        LastResponse.OwnedChunks = result.OwnedChunks;
        VERBOSE_COUT("Got " << result.ToString());
        ActTestFSM(ctx);
    }

    void Handle(NPDisk::TEvCheckSpaceResult::TPtr &ev, const TActorContext &ctx) {
        NPDisk::TEvCheckSpaceResult &result = *(ev->Get());
        LastResponse.Status = result.Status;
        LastResponse.EventType = (TEvBlobStorage::EEv)result.Type();
        LastResponse.StatusFlags = result.StatusFlags;
        LastResponse.FreeChunks = result.FreeChunks;
        LastResponse.TotalChunks = result.TotalChunks;
        LastResponse.UsedChunks = result.UsedChunks;
        VERBOSE_COUT("Got " << result.ToString());
        ActTestFSM(ctx);
    }

    void Handle(NPDisk::TEvLogResult::TPtr &ev, const TActorContext &ctx) {
        NPDisk::TEvLogResult &result = *(ev->Get());
        // Print before move
        VERBOSE_COUT("Got " << result.ToString());
        LastResponse.Status = result.Status;
        LastResponse.LogResults = std::move(result.Results);
        LastResponse.EventType = (TEvBlobStorage::EEv)result.Type();
        LastResponse.StatusFlags = result.StatusFlags;
        ActTestFSM(ctx);
    }

    void Handle(NPDisk::TEvReadLogResult::TPtr &ev, const TActorContext &ctx) {
        NPDisk::TEvReadLogResult &result = *(ev->Get());
        LastResponse.Status = result.Status;
        LastResponse.EventType = (TEvBlobStorage::EEv)result.Type();
        LastResponse.LogRecords = result.Results;
        LastResponse.NextPosition = result.NextPosition;
        LastResponse.IsEndOfLog = result.IsEndOfLog;
        LastResponse.StatusFlags = result.StatusFlags;
        VERBOSE_COUT("Got " << result.ToString());
        ActTestFSM(ctx);
    }

    void Handle(NPDisk::TEvChunkWriteResult::TPtr &ev, const TActorContext &ctx) {
        NPDisk::TEvChunkWriteResult &result = *(ev->Get());
        LastResponse.Status = result.Status;
        LastResponse.EventType = (TEvBlobStorage::EEv)result.Type();
        LastResponse.ChunkIdx = result.ChunkIdx;
        LastResponse.Cookie = result.Cookie;
        LastResponse.StatusFlags = result.StatusFlags;
        VERBOSE_COUT("Got " << result.ToString());
        ActTestFSM(ctx);
    }

    void Handle(NPDisk::TEvChunkReadResult::TPtr &ev, const TActorContext &ctx) {
        NPDisk::TEvChunkReadResult &result = *(ev->Get());
        VERBOSE_COUT("Got " << result.ToString());
        LastResponse.Status = result.Status;
        LastResponse.EventType = (TEvBlobStorage::EEv)result.Type();
        LastResponse.Offset = result.Offset;
        LastResponse.Data = std::move(result.Data);
        LastResponse.ChunkIdx = result.ChunkIdx;
        LastResponse.Cookie = result.Cookie;
        LastResponse.StatusFlags = result.StatusFlags;
        ActTestFSM(ctx);
    }

    void Handle(NPDisk::TEvChunkReserveResult::TPtr &ev, const TActorContext &ctx) {
        NPDisk::TEvChunkReserveResult &result = *(ev->Get());
        LastResponse.Status = result.Status;
        LastResponse.EventType = (TEvBlobStorage::EEv)result.Type();
        LastResponse.ChunkIds = result.ChunkIds;
        LastResponse.StatusFlags = result.StatusFlags;
        VERBOSE_COUT("Got " << result.ToString());
        ActTestFSM(ctx);
    }

    void Handle(NPDisk::TEvChunkForgetResult::TPtr &ev, const TActorContext &ctx) {
        NPDisk::TEvChunkForgetResult &result = *(ev->Get());
        LastResponse.Status = result.Status;
        LastResponse.EventType = (TEvBlobStorage::EEv)result.Type();
        LastResponse.StatusFlags = result.StatusFlags;
        VERBOSE_COUT("Got " << result.ToString());
        ActTestFSM(ctx);
    }

    void Handle(NPDisk::TEvChunkLockResult::TPtr &ev, const TActorContext &ctx) {
        NPDisk::TEvChunkLockResult &result = *(ev->Get());
        LastResponse.Status = result.Status;
        LastResponse.EventType = (TEvBlobStorage::EEv)result.Type();
        LastResponse.ChunkIds = result.LockedChunks;
        LastResponse.FreeChunks = result.AvailableChunksCount;
        VERBOSE_COUT("Got " << result.ToString());
        ActTestFSM(ctx);
    }

    void Handle(NPDisk::TEvChunkUnlockResult::TPtr &ev, const TActorContext &ctx) {
        NPDisk::TEvChunkUnlockResult &result = *(ev->Get());
        LastResponse.Status = result.Status;
        LastResponse.EventType = (TEvBlobStorage::EEv)result.Type();
        LastResponse.UnlockedChunks = result.UnlockedChunks;
        VERBOSE_COUT("Got " << result.ToString());
        ActTestFSM(ctx);
    }

    void Handle(NPDisk::TEvHarakiriResult::TPtr &ev, const TActorContext &ctx) {
        NPDisk::TEvHarakiriResult &result = *(ev->Get());
        LastResponse.Status = result.Status;
        LastResponse.EventType = (TEvBlobStorage::EEv)result.Type();
        LastResponse.StatusFlags = result.StatusFlags;
        VERBOSE_COUT("Got " << result.ToString());
        ActTestFSM(ctx);
    }

    void Handle(NPDisk::TEvSlayResult::TPtr &ev, const TActorContext &ctx) {
        NPDisk::TEvSlayResult &result = *(ev->Get());
        LastResponse.Status = result.Status;
        LastResponse.EventType = (TEvBlobStorage::EEv)result.Type();
        LastResponse.StatusFlags = result.StatusFlags;
        VERBOSE_COUT("Got " << result.ToString());
        ActTestFSM(ctx);
    }

    void Handle(NPDisk::TEvYardControlResult::TPtr &ev, const TActorContext &ctx) {
        NPDisk::TEvYardControlResult &result = *(ev->Get());
        LastResponse.Status = result.Status;
        LastResponse.EventType = (TEvBlobStorage::EEv)result.Type();
        LastResponse.Cookie = result.Cookie;
        VERBOSE_COUT("Got " << result.ToString());
        ActTestFSM(ctx);
    }

    void Handle(NPDisk::TEvCutLog::TPtr &ev, const TActorContext &ctx) {
        NPDisk::TEvCutLog &result = *(ev->Get());
        LastResponse.EventType = (TEvBlobStorage::EEv)result.Type();
        LastResponse.Status = NKikimrProto::EReplyStatus::OK;
        VERBOSE_COUT("Got " << result.ToString());
        ActTestFSM(ctx);
    }

    void Handle(NMon::TEvHttpInfoRes::TPtr &ev, const TActorContext &ctx) {
        LastResponse.HttpResult = static_cast<NMon::TEvHttpInfoRes*>(ev->Get());
        VERBOSE_COUT("Got " << ev->Get()->ToString());
        ActTestFSM(ctx);
    }

    void Handle(NNodeWhiteboard::TEvWhiteboard::TEvPDiskStateUpdate::TPtr &ev, const TActorContext &ctx) {
        LastResponse.whiteboardPDiskResult = ev->Get();
        VERBOSE_COUT("Got " << ev->Get()->ToString());
        ActTestFSM(ctx);
    }

    void Handle(NNodeWhiteboard::TEvWhiteboard::TEvVDiskStateUpdate::TPtr &ev, const TActorContext &ctx) {
        LastResponse.whiteboardVDiskResult = ev->Get();
        VERBOSE_COUT("Got " << ev->Get()->ToString());
        ActTestFSM(ctx);
    }
    void Handle(TEvBlobStorage::TEvControllerUpdateDiskStatus::TPtr &ev, const TActorContext &ctx) {
        LastResponse.whiteboardDiskMetricsResult = ev->Get();
        VERBOSE_COUT("Got " << ev->Get()->ToString());
        ActTestFSM(ctx);
    }

public:
    TBaseTest(const TIntrusivePtr<TTestConfig> &cfg)
        : TCommonBaseTest(cfg)
    {
        this->UnsafeBecome(&TBaseTest::StateRegister);
    }


    STFUNC(StateRegister) {
        Event = ev.Get();
        switch (ev->GetTypeRewrite()) {
            HFunc(NPDisk::TEvYardInitResult, Handle);
            HFunc(NPDisk::TEvCheckSpaceResult, Handle);
            HFunc(NPDisk::TEvLogResult, Handle);
            HFunc(NPDisk::TEvReadLogResult, Handle);
            HFunc(NPDisk::TEvChunkWriteResult, Handle);
            HFunc(NPDisk::TEvChunkReadResult, Handle);
            HFunc(NPDisk::TEvChunkReserveResult, Handle);
            HFunc(NPDisk::TEvChunkLockResult, Handle);
            HFunc(NPDisk::TEvChunkUnlockResult, Handle);
            HFunc(NPDisk::TEvHarakiriResult, Handle);
            HFunc(NPDisk::TEvSlayResult, Handle);
            HFunc(NPDisk::TEvYardControlResult, Handle);
            HFunc(NPDisk::TEvCutLog, Handle);
            HFunc(NPDisk::TEvChunkForgetResult, Handle);
            HFunc(NMon::TEvHttpInfoRes, Handle);
            HFunc(NNodeWhiteboard::TEvWhiteboard::TEvPDiskStateUpdate, Handle);
            HFunc(NNodeWhiteboard::TEvWhiteboard::TEvVDiskStateUpdate, Handle);
            HFunc(TEvBlobStorage::TEvControllerUpdateDiskStatus, Handle);
            HFunc(TEvTablet::TEvBoot, HandleBoot);
        }
    }
};

} // NKikimr
