#include "blobstorage_pdisk_ut.h"
#include "blobstorage_pdisk_ut_env.h"

#include <ydb/core/blobstorage/base/vdisk_priorities.h>
#include <ydb/library/pdisk_io/aio.h>

#include <util/system/condvar.h>
#include <util/system/mutex.h>

#include <thread>

namespace NKikimr {

namespace {

struct TDelayControl {
    std::atomic<bool> Armed = false;
    ui32 ReleaseAfter = 0;
    TDuration MaxDelay;
    std::atomic<ui32> HeldOps = 0;
    std::atomic<ui32> SubmittedWhileHeld = 0;
};

// Holds back the first operation submitted after the test arms it, until ReleaseAfter later operations have been
// submitted or MaxDelay has passed. The sector map serves operations in submission order, so the held operation
// completes after all of them, like a slow request on a real device. The test arms it only once.
class TDelayingIoContext : public NPDisk::IAsyncIoContext {
    std::unique_ptr<NPDisk::IAsyncIoContext> Inner;
    TDelayControl& Control;
    TMutex Mutex;
    TCondVar CondVar;
    NPDisk::IAsyncIoOperation *HeldOp = nullptr;
    NPDisk::ICallback *HeldCallback = nullptr;
    bool Stopping = false;
    std::thread ReleaseThread;

    void ReleaseHeldOp() {
        NPDisk::IAsyncIoOperation *op = nullptr;
        NPDisk::ICallback *callback = nullptr;
        with_lock (Mutex) {
            const TInstant deadline = TInstant::Now() + Control.MaxDelay;
            while (Control.SubmittedWhileHeld.load() < Control.ReleaseAfter && !Stopping
                    && CondVar.WaitD(Mutex, deadline)) {
            }
            op = std::exchange(HeldOp, nullptr);
            callback = HeldCallback;
        }
        Y_ABORT_UNLESS(Inner->Submit(op, callback) == NPDisk::EIoResult::Ok);
    }

    void StopReleaseThread() {
        with_lock (Mutex) {
            Stopping = true;
            CondVar.Signal();
        }
        if (ReleaseThread.joinable()) {
            ReleaseThread.join();
        }
    }

public:
    TDelayingIoContext(std::unique_ptr<NPDisk::IAsyncIoContext> inner, TDelayControl& control)
        : Inner(std::move(inner))
        , Control(control)
    {}

    ~TDelayingIoContext() override {
        StopReleaseThread();
    }

    NPDisk::EIoResult Submit(NPDisk::IAsyncIoOperation *op, NPDisk::ICallback *callback) override {
        with_lock (Mutex) {
            if (HeldOp) {
                Control.SubmittedWhileHeld.fetch_add(1);
                CondVar.Signal();
            } else if (Control.Armed.exchange(false)) {
                HeldOp = op;
                HeldCallback = callback;
                Control.HeldOps.fetch_add(1);
                ReleaseThread = std::thread([this] { ReleaseHeldOp(); });
                return NPDisk::EIoResult::Ok;
            }
        }
        return Inner->Submit(op, callback);
    }

    NPDisk::EIoResult Destroy() override {
        StopReleaseThread();
        return Inner->Destroy();
    }

    NPDisk::IAsyncIoOperation* CreateAsyncIoOperation(void *cookie, NPDisk::TReqId reqId,
            NWilson::TTraceId *traceId) override {
        return Inner->CreateAsyncIoOperation(cookie, reqId, traceId);
    }

    void DestroyAsyncIoOperation(NPDisk::IAsyncIoOperation *operation) override {
        Inner->DestroyAsyncIoOperation(operation);
    }

    i64 GetEvents(ui64 minEvents, ui64 maxEvents, NPDisk::TAsyncIoOperationResult *events,
            TDuration timeout) override {
        return Inner->GetEvents(minEvents, maxEvents, events, timeout);
    }

    void PreparePRead(NPDisk::IAsyncIoOperation *op, void *destination, size_t size, size_t offset) override {
        Inner->PreparePRead(op, destination, size, offset);
    }

    void PreparePWrite(NPDisk::IAsyncIoOperation *op, const void *source, size_t size, size_t offset) override {
        Inner->PreparePWrite(op, source, size, offset);
    }

    void PreparePTrim(NPDisk::IAsyncIoOperation *op, ui64 size, ui64 offset) override {
        Inner->PreparePTrim(op, size, offset);
    }

    bool DoTrim(NPDisk::IAsyncIoOperation *op) override {
        return Inner->DoTrim(op);
    }

    NPDisk::EIoResult Setup(ui64 maxEvents, bool doLock) override {
        return Inner->Setup(maxEvents, doLock);
    }

    void InitializeMonitoring(TPDiskMon &mon) override {
        Inner->InitializeMonitoring(mon);
    }

    void SetActorSystem(NActors::TActorSystem *actorSystem) override {
        Inner->SetActorSystem(actorSystem);
    }

    int GetLastErrno() override {
        return Inner->GetLastErrno();
    }

    TString GetPDiskInfo() override {
        return Inner->GetPDiskInfo();
    }

    TFileHandle *GetFileHandle() override {
        return Inner->GetFileHandle();
    }

    void OnAsyncIoOperationCompletion(NPDisk::IAsyncIoOperation *op) override {
        Inner->OnAsyncIoOperationCompletion(op);
    }
};

class TDelayingIoContextFactory : public NPDisk::IIoContextFactory {
    NPDisk::TIoContextFactoryOSS Oss;
    TDelayControl& Control;

public:
    TDelayingIoContextFactory(TDelayControl& control)
        : Control(control)
    {}

    std::unique_ptr<NPDisk::IAsyncIoContext> CreateAsyncIoContext(const TString &path, ui32 pDiskId,
            NPDisk::TDeviceMode::TFlags flags, TIntrusivePtr<NPDisk::TSectorMap> sectorMap) const override {
        return std::make_unique<TDelayingIoContext>(Oss.CreateAsyncIoContext(path, pDiskId, flags, sectorMap),
            Control);
    }

    void DetectFileParameters(const TString &path, ui64 &outDiskSizeBytes, bool &outIsBlockDevice) const override {
        Oss.DetectFileParameters(path, outDiskSizeBytes, outIsBlockDevice);
    }

    NPDisk::ISpdkState *CreateSpdkState() const override {
        return Oss.CreateSpdkState();
    }
};

// Every chunk write ends with a flush action, which the block device parks until all earlier device operations have
// completed. Holding one operation back makes the later chunk writes park their flush actions; all of them must be
// answered once the held operation completes.
void WriteChunksWhileOneOperationIsHeld(bool useBytesFlightControl, ui64 deviceInFlight) {
    constexpr ui32 releaseAfter = 600;
    constexpr ui32 writeCount = 1000;

    TDelayControl control;
    control.ReleaseAfter = releaseAfter;
    control.MaxDelay = TDuration::Seconds(1);
    TDelayingIoContextFactory factory(control);

    TActorTestContext testCtx({});
    testCtx.GetRuntime()->GetAppData().IoContextFactory = &factory;
    auto cfg = testCtx.GetPDiskConfig();
    cfg->UseBytesFlightControl = useBytesFlightControl;
    cfg->DeviceInFlight = deviceInFlight;
    testCtx.UpdateConfigRecreatePDisk(cfg);

    TVDiskMock vdisk(&testCtx);
    vdisk.InitFull();
    vdisk.ReserveChunk();
    vdisk.CommitReservedChunks();
    const TChunkIdx chunk = *vdisk.Chunks[EChunkState::COMMITTED].begin();
    const ui32 writeSize = vdisk.PDiskParams->AppendBlockSize;

    control.Armed = true;
    for (ui32 i = 0; i < writeCount; ++i) {
        testCtx.Send(new NPDisk::TEvChunkWrite(vdisk.PDiskParams->Owner, vdisk.PDiskParams->OwnerRound, chunk,
                i * writeSize, new NPDisk::TEvChunkWrite::TAlignedParts(PrepareData(writeSize)), nullptr, false,
                NPriWrite::HullHugeUserData));
    }
    for (ui32 i = 0; i < writeCount; ++i) {
        const auto res = testCtx.Recv<NPDisk::TEvChunkWriteResult>();
        UNIT_ASSERT_VALUES_EQUAL_C(res->Status, NKikimrProto::OK, res->ToString());
    }

    UNIT_ASSERT_VALUES_EQUAL(control.HeldOps.load(), 1);
    Cerr << "operations submitted while one was held# " << control.SubmittedWhileHeld.load() << Endl;
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TPDiskCompletionOrder) {

    // The default flight control does not let a new operation start DeviceInFlight indexes after the oldest
    // incomplete one, so the parked flush actions fit into the block device's table.
    Y_UNIT_TEST(DelayedOperationWithDefaultFlightControl) {
        WriteChunksWhileOneOperationIsHeld(false, 128);
    }

    // The bytes flight control limits only the number of incomplete operations, so more than 256 operations may
    // complete after the held one.
    Y_UNIT_TEST(DelayedOperationWithBytesFlightControl) {
        WriteChunksWhileOneOperationIsHeld(true, 128);
    }

    Y_UNIT_TEST(DelayedOperationWithDeviceInFlightAbove256) {
        WriteChunksWhileOneOperationIsHeld(false, 512);
    }
}

} // namespace NKikimr
