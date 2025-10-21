#pragma once

#include "buffer_pool.h"
#include "sector_map.h"
#include "spdk_state.h"

#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_request_id.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_util_devicemode.h>

#include <ydb/library/actors/wilson/wilson_event.h>

#include <util/system/file.h>
#include <util/generic/string.h>
#include <util/string/printf.h>

namespace NKikimr {

struct TPDiskMon;

namespace NPDisk {

using namespace NActors;

struct TAsyncIoOperationResult;

struct ICallback {
    virtual void Exec(TAsyncIoOperationResult *) = 0;

    virtual ~ICallback() = default;
};

struct IAsyncIoOperation {
    enum class EType {
        PRead,
        PWrite,
        PTrim
    };

    virtual ~IAsyncIoOperation() {
    };

    virtual void* GetCookie() = 0;
    virtual NWilson::TTraceId *GetTraceIdPtr() = 0;
    virtual void* GetData() = 0;
    virtual ui64 GetOffset() = 0;
    virtual ui64 GetSize() = 0;
    virtual EType GetType() = 0;
    virtual TReqId GetReqId() = 0;

    virtual void SetCallback(ICallback *) = 0;
    virtual void ExecCallback(TAsyncIoOperationResult *) = 0;

    virtual TString Str() {
        TStringStream str;
        str << "LibaioAsyncOperation { Data# " << (ui64)GetData() << " Offset# " << GetOffset() << " Size# " <<
            GetSize() << " ReqId# " << GetReqId() << " Type# " << GetType();
        return str.Str();
    }
};

struct TPDiskDebugInfo {
    TString Path;
    TString Info;
    ui32 PDiskId;

    TPDiskDebugInfo(const TString &path, ui32 pDiskId, const TString &ioLibrary)
        : Path(path)
        , Info(TStringBuilder() << "PDiskId# " << pDiskId << " path# \"" << path << "\"" << " IoLibrary# " << ioLibrary)
        , PDiskId(pDiskId)
    {}

    const char *c_str() const {
        return Info.c_str();
    }

    TString Str() const {
        return Info;
    }
};

inline IOutputStream &operator<<(IOutputStream &out, const TPDiskDebugInfo& info) {
    out << info.Str();
    return out;
}

enum class EIoResult : i64 {
    Unknown = 1,                //
    Ok = 2,                     //
    BadFileNumber = 3,          // aka EBADF:           Submit
    TryAgain = 4,               // aka EAGAIN:  Setup   Submit
    BadAddress = 5,             // aka EFAULT:  Setup   Submit  GetEvents   Destroy
    InvalidArgument = 6,        // aka EINVAL:  Setup   Submit  GetEvents   Destroy
    FunctionNotImplemented = 7, // aka ENOSYS:  Setup   Submit  GetEvents   Destroy
    InterruptedSystemCall = 8,  // aka EINTR:                   GetEvents
    OutOfMemory = 9,            // aka ENOMEM:  Setup                               Result
    IOError = 10,               // aka EIO:                                         Result
    FileOpenError = 11,         //              Setup
    FileLockError = 12,         //              Setup
    FakeError = 13,             //              Setup

    // From kernel maillist: "this error is not fatal. One can fix it easily by rewriting affected sector"
    InvalidSequence = 14,       // aka EILSEQ:                  GetEvents
    // for broken disk's error-log: "READ_ERROR: The read data could not be recovered from the media"
    NoData = 15,                // aka ENODATA:                 GetEvents
    RemoteIOError = 16,         // aka EREMOTEIO:               GetEvents
    NoSpaceLeft = 17,           // aka ENOSPC:                  GetEvents
    NoDevice = 18,              // aka ENODEV:                  GetEvents
};

struct TAsyncIoOperationResult {
    IAsyncIoOperation *Operation = nullptr;
    EIoResult Result = EIoResult::Unknown;
};

class IAsyncIoContext {
public:
    virtual ~IAsyncIoContext() {
    }

    virtual IAsyncIoOperation* CreateAsyncIoOperation(void *cookie, TReqId reqId, NWilson::TTraceId *traceId) = 0;
    virtual void DestroyAsyncIoOperation(IAsyncIoOperation *operation) = 0;
    virtual EIoResult Destroy() = 0;
    // Returns -EIoResult in case of error
    virtual i64 GetEvents(ui64 minEvents, ui64 maxEvents, TAsyncIoOperationResult *events, TDuration timeout) = 0;
    virtual void PreparePRead(IAsyncIoOperation *op, void *destination, size_t size, size_t offset) = 0;
    virtual void PreparePWrite(IAsyncIoOperation *op, const void *source, size_t size, size_t offset) = 0;
    virtual void PreparePTrim(IAsyncIoOperation *op, size_t size, size_t offset) = 0;
    virtual bool DoTrim(IAsyncIoOperation *op) = 0;
    virtual EIoResult Setup(ui64 maxevents, bool doLock) = 0;
    virtual void InitializeMonitoring(TPDiskMon &mon) = 0;
    virtual EIoResult Submit(IAsyncIoOperation *op, ICallback *callback) = 0;
    virtual void SetActorSystem(TActorSystem *actorSystem) = 0;
    virtual int GetLastErrno() = 0;
    virtual TString GetPDiskInfo() = 0;
    virtual TFileHandle *GetFileHandle() = 0;
    virtual void OnAsyncIoOperationCompletion(IAsyncIoOperation *op) = 0;
};

std::unique_ptr<IAsyncIoContext> CreateAsyncIoContextReal(const TString &path, ui32 pDiskId, TDeviceMode::TFlags flags);
std::unique_ptr<IAsyncIoContext> CreateAsyncIoContextMap(const TString &path, ui32 pDiskId, TIntrusivePtr<TSectorMap> sectorMap);

struct IIoContextFactory {
    virtual std::unique_ptr<IAsyncIoContext> CreateAsyncIoContext(const TString &path, ui32 pDiskId,
            TDeviceMode::TFlags flags, TIntrusivePtr<TSectorMap> sectorMap) const = 0;
    virtual void DetectFileParameters(const TString &path, ui64 &outDiskSizeBytes, bool &outIsBlockDevice) const = 0;
    virtual ISpdkState *CreateSpdkState() const = 0;
    virtual ~IIoContextFactory() {}
};

struct TIoContextFactoryOSS : IIoContextFactory {
    std::unique_ptr<IAsyncIoContext> CreateAsyncIoContext(const TString &path, ui32 pDiskId, TDeviceMode::TFlags flags,
            TIntrusivePtr<TSectorMap> sectorMap) const override;

    ISpdkState *CreateSpdkState() const override;
    void DetectFileParameters(const TString &path, ui64 &outDiskSizeBytes, bool &outIsBlockDevice) const override;
};

} // NPDisk
} // NKikimr
