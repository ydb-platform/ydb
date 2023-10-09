#include "aio.h"

#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_util_countedqueueoneone.h>

#include <util/random/random.h>
#include <util/system/spinlock.h>
#include <util/thread/pool.h>

namespace NKikimr {
namespace NPDisk {

struct TAsyncIoOperationMap : IObjectInQueue, IAsyncIoOperation {
    IAsyncIoContext &AsyncIoContext;
    TSectorMap &SectorMap;
    TCountedQueueOneOne<IAsyncIoOperation*, 4 << 10> &CompleteQueue;
    void *Cookie;
    void *Data = nullptr;
    ui64 Offset = 0;
    ui64 Size = 0;
    EType Type = IAsyncIoOperation::EType::PRead;
    TReqId ReqId;
    ICallback *Callback = nullptr;
    NWilson::TTraceId TraceId;

    TInstant Deadline;

    bool PrevAsyncIoOperationIsInProgress = false;

    TAsyncIoOperationMap(IAsyncIoContext &asyncIoContext, TSectorMap &sectorMap,
            TCountedQueueOneOne<IAsyncIoOperation*, 4 << 10> &completeQueue,
            void *cookie, TReqId reqId, NWilson::TTraceId *traceId)
        : AsyncIoContext(asyncIoContext)
        , SectorMap(sectorMap)
        , CompleteQueue(completeQueue)
        , Cookie(cookie)
        , ReqId(reqId)
        , TraceId(traceId ? std::move(*traceId) : NWilson::TTraceId())
    {}

    ~TAsyncIoOperationMap() override {
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
                {
                    SectorMap.Read((ui8*)Data, Size, Offset, PrevAsyncIoOperationIsInProgress);
                    break;
                }
            case IAsyncIoOperation::EType::PWrite:
                {
                    SectorMap.Write((ui8*)Data, Size, Offset, PrevAsyncIoOperationIsInProgress);
                    break;
                }
            default:
                Y_FAIL_S("Unexpected op type# " << (i64)Type);
        }
        AsyncIoContext.OnAsyncIoOperationCompletion(this);
        CompleteQueue.Push(this);
    }

    void SetCallback(ICallback *callback) override {
        Callback = callback;
    }

    void ExecCallback(TAsyncIoOperationResult *result) override {
        Callback->Exec(result);
    }
};

class TRandomWaitThreadPool : public IThreadPool {
    TCountedQueueOneOne<TAsyncIoOperationMap*, 4 << 10> IncomingQueue;
    TMultiMap<TInstant, TAsyncIoOperationMap*> WaitQueue;

    TThread WorkThread;
    std::atomic<bool> StopFlag;
    std::pair<TDuration, TDuration> WaitParams;

    ///////    Thread working part    /////
    static void *Proc(void* that) {
        static_cast<TRandomWaitThreadPool*>(that)->Work();
        return nullptr;
    }

    void Work() {
        bool receivedNullFromIncomingQueue = false;
        while (true) {
            TInstant now = TInstant::Now();
            TAtomicBase size = IncomingQueue.GetWaitingSize();
            for (TAtomicBase idx = 0; idx < size; ++idx) {
                TAsyncIoOperationMap *op = IncomingQueue.Pop();
                if (op) {
                    if (op->Deadline <= now) {
                        op->Process(nullptr);
                    } else {
                        WaitQueue.emplace(op->Deadline, op);
                    }
                } else {
                    receivedNullFromIncomingQueue = true;
                }
            }
            if (StopFlag.load()) {
                Cleanup(receivedNullFromIncomingQueue);
                return;
            }
            now = TInstant::Now();
            auto it = WaitQueue.begin();
            while (it != WaitQueue.end() && it->first <= now) {
                TAsyncIoOperationMap *op = it->second;
                op->Process(nullptr);
                auto curr = it;
                ++it;
                WaitQueue.erase(curr);
            }
            TDuration wait = TDuration::Max();
            if (WaitQueue) {
                Y_ABORT_UNLESS(WaitQueue.begin()->first > now);
                wait = WaitQueue.begin()->first - now;
            }
            IncomingQueue.ProducedWait(wait);
        }
    }

    void Cleanup(bool receiveNull) {
        for (auto& op : WaitQueue) {
            delete op.second;
        }
        WaitQueue.clear();
        while (!receiveNull) {
            TAtomicBase size = IncomingQueue.GetWaitingSize();
            for (TAtomicBase idx = 0; idx < size; ++idx) {
                TAsyncIoOperationMap *op = IncomingQueue.Pop();
                if (op) {
                    delete op;
                } else {
                    receiveNull = true;
                }
            }
        }
    }

    ///////    Intefrace   /////
    bool Add(IObjectInQueue *obj) override {
        if (StopFlag.load()) {
            return false;
        }
        auto op = static_cast<TAsyncIoOperationMap*>(obj);
        op->Deadline = TInstant::Now() + WaitParams.first
            + TDuration::MicroSeconds(RandomNumber<ui32>(WaitParams.second.MicroSeconds()));
        IncomingQueue.Push(op);
        return true;
    }

    size_t Size() const noexcept override {
        return 0; // Size of thread pool, meaningless for that class
    }

    void Start(size_t, size_t) override {
    }

    void Stop() noexcept override {
        Y_ABORT_UNLESS(!StopFlag.load());
        StopFlag.store(true);
        IncomingQueue.Push(nullptr);
        WorkThread.Join();
    }


public:
    TRandomWaitThreadPool(const std::pair<TDuration, TDuration>& waitParams)
        : WorkThread(TThread::TParams(Proc, this))
        , StopFlag(false)
        , WaitParams(waitParams)
    {
        WorkThread.Start();
    }

    ~TRandomWaitThreadPool(){
    }
};

class TAsyncIoContextMap : public IAsyncIoContext {
    TAutoPtr<IThreadPool> Queue;
    TIntrusivePtr<TSectorMap> SectorMap;
    TCountedQueueOneOne<IAsyncIoOperation*, 4 << 10> CompleteQueue;
    ui64 MaxEvents = 0;
    int LastErrno = 0;

    TPDiskDebugInfo PDiskInfo;

    TSpinLock SpinLock;
    IAsyncIoOperation* LastOngoingAsyncIoOperation = nullptr;
public:

    TAsyncIoContextMap(const TString &path, ui32 pDiskId, TIntrusivePtr<TSectorMap> sectorMap)
        : SectorMap(sectorMap)
        , PDiskInfo(path, pDiskId, "map")
    {}

    ~TAsyncIoContextMap() {
    }

    void InitializeMonitoring(TPDiskMon &mon) override {
        Y_UNUSED(mon);
    }

    IAsyncIoOperation* CreateAsyncIoOperation(void* cookie, TReqId reqId, NWilson::TTraceId *traceId) override {
        IAsyncIoOperation *operation = new TAsyncIoOperationMap(*this, *SectorMap, CompleteQueue, cookie, reqId, traceId);
        return operation;
    }

    void DestroyAsyncIoOperation(IAsyncIoOperation *operation) override {
        delete operation;
    }

    EIoResult Destroy() override {
        Queue->Stop();
        SectorMap->Unlock();

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
                    TAsyncIoOperationMap *op = static_cast<TAsyncIoOperationMap*>(CompleteQueue.Pop());
                    events[outputIdx].Operation = op;
                    events[outputIdx].Result = (RandomNumber<double>() <
                            SectorMap->ImitateIoErrorProbability.load())
                        ? EIoResult::FakeError
                        : EIoResult::Ok;
                    if (op->GetType() == IAsyncIoOperation::EType::PRead &&
                            RandomNumber<double>() < SectorMap->ImitateReadIoErrorProbability.load()) {
                        events[outputIdx].Result = EIoResult::FakeError;
                    }
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

    void PrepareImpl(IAsyncIoOperation *op, void *data, size_t size, size_t offset,
            IAsyncIoOperation::EType type) {
        TAsyncIoOperationMap *operation = static_cast<TAsyncIoOperationMap*>(op);
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
        Sleep(TDuration::MilliSeconds(40));

        SectorMap->Trim(op->GetSize(), op->GetOffset());
        return true;
    }

    EIoResult Setup(ui64 maxEvents, bool doLock) override {
        if (doLock) {
            bool isLocked = SectorMap->Lock();
            if (!isLocked) {
                return EIoResult::FileOpenError;
            }
        }
        MaxEvents = maxEvents;
        if (SectorMap->ImitateRandomWait) {
            Queue = new TRandomWaitThreadPool(*SectorMap->ImitateRandomWait);
        } else {
            Queue = new TThreadPool();
            Queue->Start(1, MaxEvents);
        }
        return EIoResult::Ok;
    }

    EIoResult Submit(IAsyncIoOperation *op, ICallback *callback) override {
        op->SetCallback(callback);
        TAsyncIoOperationMap *operation = static_cast<TAsyncIoOperationMap*>(op);

        {
            TGuard<TSpinLock> guard(SpinLock);
            if (LastOngoingAsyncIoOperation != nullptr) {
                operation->PrevAsyncIoOperationIsInProgress = true;
            }
            LastOngoingAsyncIoOperation = operation;
        }

        bool isOk = Queue->Add(operation);
        return isOk ? EIoResult::Ok : EIoResult::TryAgain;
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
        return nullptr;
    }

    void OnAsyncIoOperationCompletion(IAsyncIoOperation *op) override {
        TGuard<TSpinLock> guard(SpinLock);
        if (LastOngoingAsyncIoOperation == op) {
            LastOngoingAsyncIoOperation = nullptr;
        }
    }
};

std::unique_ptr<IAsyncIoContext> CreateAsyncIoContextMap(const TString &path, ui32 pDiskId, TIntrusivePtr<TSectorMap> sectorMap) {
    return std::make_unique<TAsyncIoContextMap>(path, pDiskId, sectorMap);
}

} // NPDisk
} // NKikimr
