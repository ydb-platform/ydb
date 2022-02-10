#include "aio.h"
#include "buffers.h"

//#include <ydb/core/blobstorage/base/wilson_events.h> 
#include <ydb/core/debug/valgrind_check.h> 
#include <ydb/core/util/yverify_stream.h> 

#include <ydb/library/pdisk_io/spdk_state.h> 
#include <library/cpp/actors/util/intrinsics.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <util/system/file.h>
#include <util/stream/format.h>
#include <contrib/libs/libaio/libaio.h>

#include <linux/fs.h>
#include <sys/ioctl.h>

namespace NKikimr {
namespace NPDisk {

class TCallbackContext;

struct TAsyncIoOperation : iocb, IAsyncIoOperation {
    void* Cookie;
    ICallback *Callback;
    TReqId ReqId;
    NWilson::TTraceId TraceId;
    bool IsTrim; // Trim is special case of IO_CMD_PWRITE operation

    TAsyncIoOperation() = default;

    TAsyncIoOperation(void *cookie, TReqId reqId, NWilson::TTraceId *traceId)
        : Cookie(cookie)
        , Callback(nullptr)
        , ReqId(reqId)
        , TraceId(traceId ? std::move(*traceId) : NWilson::TTraceId())
        , IsTrim(false)
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
        return u.c.buf;
    }

    ui64 GetOffset() override {
        return u.c.offset;
    };

    ui64 GetSize() override {
        return u.c.nbytes;
    };

    TReqId GetReqId() override {
        return ReqId;
    }

    EType GetType() override {
        switch (aio_lio_opcode) {
            case IO_CMD_PWRITE: return IsTrim ? EType::PTrim : EType::PWrite;
            case IO_CMD_PREAD:  return EType::PRead;
            default:
                Y_FAIL_S("Libaio TAsyncIoOperation::GetType(), unknown type# " << Hex(aio_lio_opcode));
        }
    };

    void SetCallback(ICallback *callback) override {
        Callback = callback;
    }

    void ExecCallback(TAsyncIoOperationResult *result) override {
        Callback->Exec(result);
    }
};

class TAsyncIoContextLibaio : public IAsyncIoContext {
    io_context_t IoContext;
    TActorSystem *ActorSystem;
    TPool<TAsyncIoOperation, 1024> Pool;
    THolder<TFileHandle> File;
    int LastErrno = 0;

    TPDiskDebugInfo PDiskInfo;
public:

    TAsyncIoContextLibaio(const TString &path, ui32 pDiskId, TDeviceMode::TFlags flags)
        : IoContext(nullptr)
        , ActorSystem(nullptr)
        , PDiskInfo(path, pDiskId, "libaio")
    {
        Y_UNUSED(flags);
    }

    ~TAsyncIoContextLibaio() {
    }

    void InitializeMonitoring(TPDiskMon &/*mon*/) override {
        //Pool.InitializeMonitoring(mon);
    }

    IAsyncIoOperation* CreateAsyncIoOperation(void* cookie, TReqId reqId, NWilson::TTraceId *traceId) override {
        void *p = Pool.Pop();
        IAsyncIoOperation *operation = new (p) TAsyncIoOperation(cookie, reqId, traceId);
        return operation;
    }

    void DestroyAsyncIoOperation(IAsyncIoOperation* operation) override {
        Pool.Push(static_cast<TAsyncIoOperation*>(operation));
    }

    EIoResult Destroy() override {
        int ret = io_destroy(IoContext);
        if (ret < 0) {
            switch (-ret) {
                case EFAULT: return EIoResult::BadAddress;
                case EINVAL: return EIoResult::InvalidArgument;
                case ENOSYS: return EIoResult::FunctionNotImplemented;
                default: Y_FAIL_S(PDiskInfo << " unexpected error in io_destroy, error# " << -ret
                                 << " strerror# " << strerror(-ret));
            }
        }
        if (File) {
            ret = File->Flock(LOCK_UN);
            Y_VERIFY_S(ret == 0, "Error in Flock(LOCK_UN), errno# " << errno << " strerror# " << strerror(errno));
            bool isOk = File->Close();
            Y_VERIFY_S(isOk, PDiskInfo << " error on file close, errno# " << errno << " strerror# " << strerror(errno));
        }
        return EIoResult::Ok;
    }

    i64 GetEvents(ui64 minEvents, ui64 maxEvents, TAsyncIoOperationResult *events, TDuration timeout) override {
        TStackVec<io_event, 64> ioEvents;
        ioEvents.resize(maxEvents);
        timespec ioTimeout = { (time_t)timeout.Seconds(), timeout.NanoSecondsOfSecond() };
        REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&IoContext, sizeof(IoContext));
        int ret = io_getevents(IoContext, minEvents, maxEvents, &ioEvents[0], &ioTimeout);
        if (ret < 0) {
            return -static_cast<i64>(RetErrnoToContextError(ret, "io_getevents"));
        }
        for (int i = 0; i < ret; ++i) {
            IAsyncIoOperation *op = static_cast<TAsyncIoOperation*>(ioEvents[i].obj);

#if defined(__has_feature)
#    if __has_feature(thread_sanitizer)
            //
            // Thread Sanitizer does not consider io_submit / io_getevents synchronization.
            //
            AtomicLoad((char*)op);
#    endif
#endif

            events[i].Operation = op;

            events[i].Result = RetErrnoToContextError(ioEvents[i].res, "ioEvents[].res");

            events[i].Operation->ExecCallback(&events[i]);

            //if (ActorSystem) {
            //    WILSON_TRACE(*ActorSystem, op->GetTraceIdPtr(), AsyncIoFinished);
            //}
        }
        return ret;
    }

    EIoResult RetErrnoToContextError(i64 ret, const char *info) {
        if (ret < 0) {
            switch(-ret) {
                case EAGAIN:    return EIoResult::TryAgain;
                case EBADF:     return EIoResult::BadFileNumber;
                case EFAULT:    return EIoResult::BadAddress;
                case EINTR:     return EIoResult::InterruptedSystemCall;
                case EINVAL:    return EIoResult::InvalidArgument;
                case EIO:       return EIoResult::IOError;
                case ENOMEM:    return EIoResult::OutOfMemory;
                case ENOSYS:    return EIoResult::FunctionNotImplemented;
                case EILSEQ:    return EIoResult::InvalidSequence;
                case ENODATA:   return EIoResult::NoData;
                default: Y_FAIL_S(PDiskInfo << " unexpected error in " << info << ", error# " << -ret
                                 << " strerror# " << strerror(-ret));
            }
        } else {
            return EIoResult::Ok;
        }
    }

    void PreparePRead(IAsyncIoOperation *op, void *destination, size_t size, size_t offset) override {
        Y_VERIFY_DEBUG(File);
        iocb* cb =  static_cast<iocb*>(static_cast<TAsyncIoOperation*>(op));
        io_prep_pread(cb, static_cast<FHANDLE>(*File), destination, size, offset);
    }

    void PreparePWrite(IAsyncIoOperation *op, const void *source, size_t size, size_t offset) override {
        Y_VERIFY_DEBUG(File);
        iocb* cb =  static_cast<iocb*>(static_cast<TAsyncIoOperation*>(op));
        io_prep_pwrite(cb, static_cast<FHANDLE>(*File), const_cast<void*>(source), size, offset);
    }

    void PreparePTrim(IAsyncIoOperation *op, size_t size, size_t offset) override {
        PreparePWrite(op, nullptr, size, offset);
        static_cast<TAsyncIoOperation*>(op)->IsTrim = true;
    }

    bool DoTrim(IAsyncIoOperation *op) override {
        TAsyncIoOperation *trim = static_cast<TAsyncIoOperation*>(op);
        Y_VERIFY(trim->IsTrim);

        ui64 range[2] = {trim->GetOffset(), trim->GetSize()};
        bool tryAgain = true;
        TStringStream str;
        str << "BLKDISCARD " << PDiskInfo;
        errno = 0;
        if (ioctl((FHANDLE)*File.Get(), BLKDISCARD, &range) == -1) {
            int errorId = errno;
            if (errorId == EOPNOTSUPP) {
                str << " failed, operation not supported, trimming will be disabled for the device";
                tryAgain = false;
            } else if (errorId == ENOTTY) {
                str << " failed, device is not a typewriter! Trimming will be disabled for the device";
                tryAgain = false;
            } else {
                str << " failed, errno# " << errorId << " strerror# " << strerror(errorId);
                tryAgain = true;
            }

            if (ActorSystem) {
                //LOG_ERROR_S(*ActorSystem, NKikimrServices::BS_DEVICE, str.Str());
            } else {
                Cerr << str.Str() << Endl;
            }
        } else {
            if (ActorSystem) {
                //LOG_DEBUG_S(*ActorSystem, NKikimrServices::BS_DEVICE, str.Str() << " trimmed# " << range[1]
                //        << " size# " << trim->GetSize() << " from# " << range[0] << " offset# " << trim->GetOffset());
            }
            tryAgain = true;
        }
        return tryAgain;
    }

    int LockFile() {
        int ret = -1;
        errno = EWOULDBLOCK;
        int retry = 2;
        while (ret == -1 && errno == EWOULDBLOCK && retry > 0) {
            errno = 0;
            ret = File->Flock(LOCK_EX | LOCK_NB);
            if (ret == 0) {
                break;
            } else {
                LastErrno = errno;
                if (ActorSystem){
                    //LOG_ERROR_S(*ActorSystem, NKikimrServices::BS_DEVICE, PDiskInfo
                    //        << " error on file locking, strerror# " << strerror(errno));
                }
                if (retry > 1) {
                    Sleep(TDuration::Seconds(1));
                }
            }
            --retry;
        }
        return ret;
    }

    EIoResult Setup(ui64 maxEvents, bool doLock) override {
        File = MakeHolder<TFileHandle>(PDiskInfo.Path.c_str(),
            OpenExisting | RdWr | DirectAligned | Sync);
        bool isFileOpened = File->IsOpen();
        if (isFileOpened) {
            if (doLock) {
                int ret = LockFile();
                if (ret == -1) {
                    return EIoResult::FileLockError;
                }
            }
        } else {
            int fd = open(PDiskInfo.Path.c_str(), O_RDWR);
            if (fd < 0) {
                LastErrno = errno;
                return EIoResult::FileOpenError;
            } else {
                close(fd);
                return EIoResult::TryAgain;
            }
        }
        int ret = io_setup(maxEvents, &IoContext);
        if (ret < 0) {
            LastErrno = -ret;
        }
        return RetErrnoToContextError(ret, "io_setup");
    }

    EIoResult Submit(IAsyncIoOperation *op, ICallback *callback) override {
        op->SetCallback(callback);
        iocb* ios[1] = { static_cast<iocb*>(static_cast<TAsyncIoOperation*>(op)) };
        //if (ActorSystem) {
        //    WILSON_TRACE(*ActorSystem, op->GetTraceIdPtr(), AsyncIoInQueue);
        //}

        if (op->GetType() == IAsyncIoOperation::EType::PWrite) {
            //PDISK_FAIL_INJECTION(1);
        }

#if defined(__has_feature)
#    if __has_feature(thread_sanitizer)
        //
        // Thread Sanitizer does not consider io_submit / io_getevents synchronization.
        //
        AtomicStore((char*)op, *(char*)op);
#    endif
#endif

        int ret = io_submit(IoContext, 1, ios);
        if (ret < 0) {
            LastErrno = -ret;
        } else if (ret == 0) {
            return EIoResult::TryAgain;
        }
        return RetErrnoToContextError(ret, "io_setup");
    }

    void SetActorSystem(TActorSystem *actorSystem) override {
        ActorSystem = actorSystem;
    }

    TString GetPDiskInfo() override {
        return PDiskInfo.Str();
    }

    int GetLastErrno() override {
        return LastErrno;
    }

    TFileHandle *GetFileHandle() override {
        return File.Get();
    }
};


//
// TBufferPoolHugePages
//
TBufferPoolHugePages::TBufferPoolHugePages(ui32 bufferSize, ui32 bufferCount, TBufferPool::TPDiskParams params)
    : TBufferPool(bufferSize, bufferCount, params)
{
    TBufferPool::UseHugePages = true;
    constexpr ui32 alignment = 512;
    auto spdkState = Singleton<TSpdkStateOSS>();
    AlignedBuffer = spdkState->Malloc(AlignUp(ui32(bufferSize), ui32(alignment)) * bufferCount, alignment);
    Y_VERIFY((ui64)AlignedBuffer % alignment == 0);
    MarkUpPool(AlignedBuffer);
}

TBufferPoolHugePages::~TBufferPoolHugePages() {
    auto spdkState = Singleton<TSpdkStateOSS>();
    spdkState->Free(AlignedBuffer);
}

std::unique_ptr<IAsyncIoContext> CreateAsyncIoContextReal(const TString &path, ui32 pDiskId, TDeviceMode::TFlags flags) {
    return std::make_unique<TAsyncIoContextLibaio>(path, pDiskId, flags);
}

} // NPDisk
} // NKikimr
