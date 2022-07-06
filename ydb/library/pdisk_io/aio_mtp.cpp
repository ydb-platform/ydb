#include "aio.h"

#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_util_countedqueueoneone.h>
#include <ydb/core/debug/valgrind_check.h>

#include <util/system/file.h>

namespace NKikimr {
namespace NPDisk {

#define INTRODUCE_BAD_SECTORS 0
#define BAD_SECTORS_STEP 15
#define BAD_SECTOR_IDX 3

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// PreadBad
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
void PreadBad(TFileHandle *file, void* data, ui32 size, ui64 offset) {
#if INTRODUCE_BAD_SECTORS
    void *data0 = data;
    ui32 size0 = size;
    ui64 offset0 = offset;
#endif
    while (size) {
        i32 sizeDone = file->Pread(data, size, offset);
        if (sizeDone == (i32)size) {
            break;
        }

        if (sizeDone < 0) {
            // TODO: get errno, report bad sectors
            sizeDone = 0;
        }
        ui64 nextOffset = ((offset + sizeDone) / 4096 + 1) * 4096;
        ui32 sizeSkipped = (ui32)(nextOffset - offset) - sizeDone;

        if (sizeSkipped) {
            memset((ui8*)data + sizeDone, 0, sizeSkipped);
        }
        size -= sizeDone + sizeSkipped;
        data = (void*)((ui8*)data + sizeDone + sizeSkipped);
        offset = nextOffset;
    }
#if INTRODUCE_BAD_SECTORS
    ui64 firstSector = (offset0 / 4096 + BAD_SECTORS_STEP - 1) / BAD_SECTORS_STEP * BAD_SECTORS_STEP + BAD_SECTOR_IDX;
    ui64 lastSector = (offset0 + size0) / 4096;
    for ( ; firstSector < lastSector; firstSector += BAD_SECTORS_STEP) {
        ui32 dataToSpoilOffset = (firstSector * 4096 - offset0);
        memset((void*)((ui8*)data0 + dataToSpoilOffset), 0, 4096);
    }
#endif
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// PwriteBad
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
void PwriteBad(TFileHandle *file, const void* data, ui32 size, ui64 offset) {
    while (size) {
        REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(data, size);
        REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&offset, sizeof(offset));
        i32 sizeDone = file->Pwrite(data, size, offset);
        if (sizeDone == (i32)size) {
            break;
        }

        if (sizeDone < 0) {
            // TODO: get errno, report bad sectors
            sizeDone = 0;
        }
        ui64 badOffset = ((offset + sizeDone) / 4096) * 4096;
        ui64 nextOffset = badOffset + 4096;
        ui32 sizeSkipped = (ui32)(nextOffset - offset) - sizeDone;

        size -= sizeDone + sizeSkipped;
        data = (void*)((ui8*)data + sizeDone + sizeSkipped);
        offset = nextOffset;
    }
}


struct TAsyncIoOperation : IObjectInQueue, IAsyncIoOperation {
    TMutex &Mutex;
    TCountedQueueOneOne<IAsyncIoOperation*, 4 << 10> &CompleteQueue;
    void *Cookie;
    void *Data;
    ui64 Offset;
    ui64 Size;
    TFileHandle *File;
    EType Type;
    TReqId ReqId;
    ICallback *Callback;
    NWilson::TTraceId TraceId;

    TAsyncIoOperation(TMutex &mutex, TCountedQueueOneOne<IAsyncIoOperation*, 4 << 10> &completeQueue,
            void *cookie, TReqId reqId, NWilson::TTraceId *traceId)
        : Mutex(mutex)
        , CompleteQueue(completeQueue)
        , Cookie(cookie)
        , ReqId(reqId)
        , TraceId(traceId ? std::move(*traceId) : NWilson::TTraceId())
    {}

    ~TAsyncIoOperation() override {
    }

    void* GetCookie() override {
        return Cookie;
    }

    NWilson::TTraceId *GetTraceIdPtr() override {
        return &TraceId;
    }

    void* GetData() override {
        return Data;
    }

    ui64 GetOffset() override {
        return Offset;
    };

    ui64 GetSize() override {
        return Size;
    };

    EType GetType() override {
        return Type;
    };

    TReqId GetReqId() override {
        return ReqId;
    }

    void Process(void*) override {
        switch (Type) {
            case IAsyncIoOperation::EType::PRead:
                PreadBad(File, Data, Size, Offset);
                break;
            case IAsyncIoOperation::EType::PWrite:
                PwriteBad(File, Data, Size, Offset);
                break;
            default:
                Y_FAIL_S("Unexpected operation type# " << (i64)Type);
                break;
        }
        {
            TGuard<TMutex> guard(Mutex);
            CompleteQueue.Push(this);
        }
    }

    void SetCallback(ICallback *callback) override {
        Callback = callback;
    }

    void ExecCallback(TAsyncIoOperationResult *result) override {
        Callback->Exec(result);
    }
};

class TAsyncIoContextMtp : public IAsyncIoContext {
    TAutoPtr<IThreadPool> Queue;
    TMutex Mutex;
    TCountedQueueOneOne<IAsyncIoOperation*, 4 << 10> CompleteQueue;
    ui64 MaxEvents;
    THolder<TFileHandle> File;
    int LastErrno = 0;

    static const ui64 NumThreads = 32;

    TPDiskDebugInfo PDiskInfo;
public:

    TAsyncIoContextMtp(const TString &path, ui32 pDiskId)
        : PDiskInfo(path, pDiskId, "mtp")
    {}

    ~TAsyncIoContextMtp() {
    }

    void InitializeMonitoring(TPDiskMon &mon) override {
        Y_UNUSED(mon);
    }

    IAsyncIoOperation* CreateAsyncIoOperation(void* cookie, TReqId reqId, NWilson::TTraceId *traceId) override {
        IAsyncIoOperation *operation = new TAsyncIoOperation(Mutex, CompleteQueue, cookie, reqId, traceId);
        return operation;
    }

    void DestroyAsyncIoOperation(IAsyncIoOperation *operation) override {
        delete operation;
    }

    EIoResult Destroy() override {
        Queue->Stop();
        if (File) {
            File->Flock(LOCK_UN);
        }
        return EIoResult::Ok;
    }

    i64 GetEvents(ui64 minEvents, ui64 maxEvents, TAsyncIoOperationResult *events, TDuration timeout) override {
        ui64 outputIdx = 0;
        TInstant startTime = TInstant::Now();
        TInstant deadline = startTime + timeout;
        while (true) {
            TAtomicBase size = CompleteQueue.GetWaitingSize();
            if (size > 0) {
                for (TAtomicBase idx = 0; idx < size; ++idx) {
                    TAsyncIoOperation *op = static_cast<TAsyncIoOperation*>(CompleteQueue.Pop());
                    events[outputIdx].Operation = op;
                    events[outputIdx].Result = EIoResult::Ok;
                    events[outputIdx].Operation->ExecCallback(&events[outputIdx]);
                    ++outputIdx;
                    if (outputIdx == maxEvents) {
                        return outputIdx;
                    }
                }
            } else {
                if (outputIdx >= minEvents) {
                    return outputIdx;
                }
                if (!timeout.NanoSeconds()) {
                    CompleteQueue.ProducedWaitI();
                } else {
                    TInstant now = TInstant::Now();
                    if (now > deadline) {
                        return outputIdx;
                    }
                    TDuration remainingTime = deadline - now;
                    bool isOk = CompleteQueue.ProducedWait(remainingTime);
                    if (!isOk) {
                        return outputIdx;
                    }
                }
            }
        }
    }

    void PrepareImpl(IAsyncIoOperation *op, void *data, size_t size, size_t offset, IAsyncIoOperation::EType type) {
        TAsyncIoOperation *operation = static_cast<TAsyncIoOperation*>(op);
        operation->File = File.Get();
        operation->Data = data;
        operation->Size = size;
        operation->Offset = offset;
        operation->Type = type;
    }

    void PreparePRead(IAsyncIoOperation *op, void *destination, size_t size, size_t offset) override {
        PrepareImpl(op, destination, size, offset, IAsyncIoOperation::EType::PRead);
    }

    void PreparePWrite(IAsyncIoOperation *op, const void *source, size_t size, size_t offset) override {
        PrepareImpl(op, const_cast<void*>(source), size, offset, IAsyncIoOperation::EType::PWrite);
    }

    void PreparePTrim(IAsyncIoOperation *op, size_t size, size_t offset) override {
        PrepareImpl(op, nullptr, size, offset, IAsyncIoOperation::EType::PTrim);
    }

    bool DoTrim(IAsyncIoOperation *op) override {
        Y_UNUSED(op);
        return false;
    }

    EIoResult Setup(ui64 maxEvents, bool doLock) override {
        File = MakeHolder<TFileHandle>(PDiskInfo.Path.c_str(),
            OpenExisting | RdWr | DirectAligned | Sync);
        bool isFileOpened = File->IsOpen();
        if (isFileOpened && doLock) {
            bool isLocked = File->Flock(LOCK_EX | LOCK_NB) == 0;
            isFileOpened = isLocked;
        }
        if (!isFileOpened) {
            return EIoResult::FileOpenError;
        }
        MaxEvents = maxEvents;
        Queue = CreateThreadPool(NumThreads, MaxEvents);
        return EIoResult::Ok;
    }

    EIoResult Submit(IAsyncIoOperation *op, ICallback *callback) override {
        op->SetCallback(callback);
        TAsyncIoOperation *operation = static_cast<TAsyncIoOperation*>(op);
        bool isOk = Queue->Add(operation);
        if (isOk) {
            return EIoResult::Ok;
        }
        return EIoResult::TryAgain;
    }

    void SetActorSystem(TActorSystem* /*actorSystem*/) override
    {}

    TString GetPDiskInfo() override {
        return PDiskInfo.Str();
    }

    int GetLastErrno() override {
        return LastErrno;
    }

    TFileHandle *GetFileHandle() override {
        return File.Get();
    }

    void OnAsyncIoOperationCompletion(IAsyncIoOperation *) override {
    }
};

std::unique_ptr<IAsyncIoContext> CreateAsyncIoContextReal(const TString &path, ui32 pDiskId, TDeviceMode::TFlags flags) {
    Y_UNUSED(flags);
    return std::make_unique<TAsyncIoContextMtp>(path, pDiskId);
}

} // NPDisk
} // NKikimr
