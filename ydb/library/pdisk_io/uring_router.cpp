#include "uring_router.h"

#include <ydb/core/util/hp_timer_helpers.h>
#include <ydb/library/actors/core/actorsystem.h>

#include <util/string/builder.h>
#include <util/system/compiler.h>
#include <util/system/sanitizers.h>
#include <util/system/thread.h>
#include <util/system/yassert.h>

#include <poll.h>
#include <sys/eventfd.h>
#include <unistd.h>

// Must be included AFTER YDB headers because linux/uapi headers pulled by
// liburing may define macros that clash with project headers.
#include "liburing_compat.h"

#include <cerrno>
#include <cstring>

using NActors::TActorSystem;

namespace NKikimr::NPDisk {

namespace {

// Queue-level stop marker. It is never dereferenced.
alignas(void*) char QueueStopSentinelStorage;

// CQE marker for the drain barrier submitted during shutdown.
alignas(void*) char StopCqeMarker;

// CQE marker for the eventfd poll used to wake a parked I/O thread.
alignas(void*) char WakePollMarker;

// The eventfd poll is the normal wakeup path. This timeout only bounds a
// missed-wakeup race and is deliberately much longer than the idle spin.
constexpr ui32 ParkSafetyNetUs = 5000;

struct __kernel_timespec MicrosToTimespec(ui32 micros) {
    struct __kernel_timespec ts;
    ts.tv_sec = micros / 1'000'000;
    ts.tv_nsec = static_cast<long long>(micros % 1'000'000) * 1000;
    return ts;
}

int CreateWakeEventFd() {
    for (;;) {
        int fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        if (fd >= 0 || errno != EINTR) {
            return fd;
        }
    }
}

void ConfigureParams(bool modern, struct io_uring_params& params) {
    memset(&params, 0, sizeof(params));

    // Start disabled so the dedicated I/O thread becomes the issuer when it
    // enables the ring. This also lets that same thread perform registration.
    params.flags = IORING_SETUP_R_DISABLED;
    if (modern) {
        params.flags |= IORING_SETUP_SINGLE_ISSUER
            | IORING_SETUP_DEFER_TASKRUN
            | IORING_SETUP_TASKRUN_FLAG;
    }
}

int InitRingWithFallback(struct io_uring* ring, ui32 queueDepth, bool* usedModernFlags) {
    int lastError = -EINVAL;
    for (bool modern : {true, false}) {
        struct io_uring_params params;
        ConfigureParams(modern, params);

        int ret = io_uring_queue_init_params(queueDepth, ring, &params);
        if (ret == 0) {
            *usedModernFlags = modern;
            return 0;
        }
        lastError = ret;
    }
    return lastError;
}

void WaitForCqe(struct io_uring* ring) {
    struct io_uring_cqe* cqe = nullptr;
    int ret;
    do {
        ret = io_uring_wait_cqe(ring, &cqe);
    } while (ret == -EINTR);
    Y_ABORT_UNLESS(ret == 0,
        "io_uring_wait_cqe failed: %s (errno %d)", strerror(-ret), -ret);
}

} // anonymous namespace

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TUringRouter::TIoThread
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// The ring's single submitter and reaper. All io_uring_enter() calls happen
// here, as required by SINGLE_ISSUER and DEFER_TASKRUN.
class TUringRouter::TIoThread : public ISimpleThread {
public:
    explicit TIoThread(TUringRouter& owner)
        : Owner(owner)
    {}

    void* ThreadProc() override {
        SetCurrentThreadName("UringIo");

        Owner.InitializeOnIoThread();

        bool idle = false;
        NHPTimer::STime idleStart = 0;

        while (!Owner.StopSeen) {
            const NHPTimer::STime cycleStart = HPNow();

            bool didWork = Owner.DrainSubmitQueue();
            if (didWork) {
                Owner.SubmitPendingSqes();
            }

            didWork = Owner.ReapCompletions() > 0 || didWork;
            if (Owner.StopSeen) {
                break;
            }

            auto accountBusy = [&] {
                if (Owner.Counters) {
                    *Owner.Counters->CompletionThreadBusyTimeNs += HPNanoSeconds(HPNow() - cycleStart);
                }
            };

            if (didWork) {
                accountBusy();
                idle = false;
                continue;
            }

            if (!idle) {
                idle = true;
                idleStart = HPNow();
                accountBusy();
                continue;
            }

            if (HPMicroSeconds(HPNow() - idleStart) < Owner.Config.IdleSpinUs) {
                // Busy-spin time is still thread cost and belongs in this metric.
                accountBusy();
                continue;
            }

            Owner.ParkAndWait();
            idle = false;
        }

        Owner.HandleStop();
        return nullptr;
    }

private:
    TUringRouter& Owner;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TUringRouter
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

ui64 TUringRouter::PackLifecycle(ui32 state, ui32 producers) {
    return (static_cast<ui64>(state) << 32) | producers;
}

ui32 TUringRouter::UnpackLifecycle(ui64 value) {
    return static_cast<ui32>(value >> 32);
}

ui32 TUringRouter::UnpackProducers(ui64 value) {
    return static_cast<ui32>(value);
}

TUringOperationBase* TUringRouter::QueueStopSentinel() {
    return reinterpret_cast<TUringOperationBase*>(&QueueStopSentinelStorage);
}

TUringRouter::TUringRouter(FHANDLE fd, TActorSystem* actorSystem, TUringRouterConfig config, TUringCounters* counters)
    : Fd(fd)
    , ActorSystem(actorSystem)
    , Config(config)
    , Counters(counters)
    , Ring(new struct io_uring())
    , WakeEventFd(CreateWakeEventFd())
{
    Y_ABORT_UNLESS(WakeEventFd >= 0,
        "eventfd() failed: %s (errno %d)", strerror(errno), errno);

    bool usedModernFlags = false;
    int ret = InitRingWithFallback(Ring.get(), Config.QueueDepth, &usedModernFlags);
    Y_ABORT_UNLESS(ret == 0,
        "io_uring_queue_init_params failed after fallbacks: %s (errno %d)", strerror(-ret), -ret);
    UsedModernFlags = usedModernFlags;
}

TUringRouter::~TUringRouter() {
    Stop();
    if (WakeEventFd >= 0) {
        close(WakeEventFd);
    }
}

void TUringRouter::RegisterFile() {
    Y_ABORT_UNLESS(UnpackLifecycle(Lifecycle.load(std::memory_order_acquire)) == LifecycleCreated,
        "RegisterFile() must be called before Start()");
    Y_ABORT_UNLESS(!IoThread);
    WantRegisterFile = true;
}

void TUringRouter::RegisterBuffers(const struct iovec* iovs, unsigned count) {
    Y_ABORT_UNLESS(UnpackLifecycle(Lifecycle.load(std::memory_order_acquire)) == LifecycleCreated,
        "RegisterBuffers() must be called before Start()");
    Y_ABORT_UNLESS(!IoThread);
    PendingIovs = iovs;
    PendingIovsCount = count;
    WantRegisterBuffers = true;
}

void TUringRouter::Start() {
    Y_ABORT_UNLESS(UnpackLifecycle(Lifecycle.load(std::memory_order_acquire)) == LifecycleCreated,
        "Start() must be called exactly once before Stop()");
    Y_ABORT_UNLESS(!IoThread);

    IoThread = std::make_unique<TIoThread>(*this);
    IoThread->Start();
    ReadyEvent.WaitI();

    ui64 expected = PackLifecycle(LifecycleCreated, 0);
    Y_ABORT_UNLESS(Lifecycle.compare_exchange_strong(expected, PackLifecycle(LifecycleRunning, 0),
        std::memory_order_release, std::memory_order_relaxed));
}

void TUringRouter::InitializeOnIoThread() {
    int ret;
    do {
        ret = io_uring_enable_rings(Ring.get());
    } while (ret == -EINTR);
    Y_ABORT_UNLESS(ret == 0,
        "io_uring_enable_rings failed: %s (errno %d)", strerror(-ret), -ret);

    if (WantRegisterFile) {
        int fd = Fd;
        do {
            ret = io_uring_register_files(Ring.get(), &fd, 1);
        } while (ret == -EINTR);
        if (ret == 0) {
            FixedFdIndex = 0;
        } else {
            RegisterFileErrno = -ret;
        }
    }

    if (WantRegisterBuffers) {
        do {
            ret = io_uring_register_buffers(Ring.get(), PendingIovs, PendingIovsCount);
        } while (ret == -EINTR);
        if (ret == 0) {
            BuffersRegistered = true;
        } else {
            RegisterBuffersErrno = -ret;
        }
    }

    ReadyEvent.Signal();
}

struct io_uring_sqe* TUringRouter::GetSqe() {
    return io_uring_get_sqe(Ring.get());
}

void TUringRouter::PrepareSqe(struct io_uring_sqe* sqe, TUringOperationBase* op) {
    // Use vectored SQEs for genuine scatter-gather and oversized singleton
    // requests; scalar SQEs take an unsigned byte count and would narrow the latter.
    const int fd = FixedFdIndex >= 0 ? FixedFdIndex : Fd;
    Y_ABORT_UNLESS(op->IovBegin < op->Iov.size(),
        "PrepareSqe called with empty iovec window");
    const unsigned iovCount = static_cast<unsigned>(op->Iov.size() - op->IovBegin);

    if (op->IsFixedBuffer()) {
        Y_ABORT_UNLESS(iovCount == 1,
            "fixed-buffer I/O does not support scatter-gather");
        void* buffer = op->Iov[op->IovBegin].iov_base;
        const size_t size = op->Iov[op->IovBegin].iov_len;
        switch (op->OperationType) {
        case TUringOperationBase::EREAD:
            io_uring_prep_read_fixed(sqe, fd, buffer, size, op->DiskOffset, op->GetBufIndex());
            break;
        case TUringOperationBase::EWRITE:
            io_uring_prep_write_fixed(sqe, fd, buffer, size, op->DiskOffset, op->GetBufIndex());
            break;
        default:
            Y_ABORT("Unknown OperationType");
        }
    } else {
        struct iovec& firstIov = op->Iov[op->IovBegin];
        if (iovCount == 1 && firstIov.iov_len <= Max<unsigned>()) {
            switch (op->OperationType) {
            case TUringOperationBase::EREAD:
                io_uring_prep_read(sqe, fd, firstIov.iov_base,
                    static_cast<unsigned>(firstIov.iov_len), op->DiskOffset);
                break;
            case TUringOperationBase::EWRITE:
                io_uring_prep_write(sqe, fd, firstIov.iov_base,
                    static_cast<unsigned>(firstIov.iov_len), op->DiskOffset);
                break;
            default:
                Y_ABORT("Unknown OperationType");
            }
        } else {
            switch (op->OperationType) {
            case TUringOperationBase::EREAD:
                io_uring_prep_readv(sqe, fd, &firstIov, iovCount, op->DiskOffset);
                break;
            case TUringOperationBase::EWRITE:
                io_uring_prep_writev(sqe, fd, &firstIov, iovCount, op->DiskOffset);
                break;
            default:
                Y_ABORT("Unknown OperationType");
            }
        }
    }

    if (FixedFdIndex >= 0) {
        sqe->flags |= IOSQE_FIXED_FILE;
    }

    // Each short-I/O retry is a distinct kernel request and gets a fresh sample.
    op->SubmitCycles = HPNow();
    io_uring_sqe_set_data(sqe, op);
    NSan::Release(op);
}

bool TUringRouter::BeginSubmit() {
    ui64 current = Lifecycle.load(std::memory_order_acquire);
    while (UnpackLifecycle(current) == LifecycleRunning) {
        Y_ABORT_UNLESS(UnpackProducers(current) != Max<ui32>(),
            "too many concurrent io_uring producers");
        if (Lifecycle.compare_exchange_weak(current, current + 1,
                std::memory_order_acquire, std::memory_order_relaxed)) {
            return true;
        }
    }
    return false;
}

void TUringRouter::EndSubmit() {
    const ui64 previous = Lifecycle.fetch_sub(1, std::memory_order_release);
    Y_DEBUG_ABORT_UNLESS(UnpackProducers(previous) > 0);
    if (UnpackLifecycle(previous) == LifecycleStopping && UnpackProducers(previous) == 1) {
        SubmittersDrained.Signal();
    }
}

void TUringRouter::WakeIoThreadIfParked() {
    if (Parked.load(std::memory_order_seq_cst)) {
        ui64 one = 1;
        ssize_t written;
        do {
            written = write(WakeEventFd, &one, sizeof(one));
        } while (written < 0 && errno == EINTR);
        // EAGAIN only means an earlier wake is already retained in eventfd.
        Y_DEBUG_ABORT_UNLESS(written == static_cast<ssize_t>(sizeof(one)) || errno == EAGAIN);
    }
}

bool TUringRouter::Submit(TUringOperationBase* op) {
    Y_ABORT_UNLESS(op);
    Y_ABORT_UNLESS(op->GetOperationType() != TUringOperationBase::ENOT_SET,
        "Submit() called with an unprepared operation");

    if (!BeginSubmit()) {
        return false;
    }

    InFlightCount.fetch_add(1, std::memory_order_relaxed);
    NSan::Release(op);
    Queue.Push(op);
    WakeIoThreadIfParked();
    EndSubmit();
    return true;
}

bool TUringRouter::Read(TUringOperationBase* op) {
    Y_ABORT_UNLESS(op->GetOperationType() == TUringOperationBase::EREAD);
    return Submit(op);
}

bool TUringRouter::Write(TUringOperationBase* op) {
    Y_ABORT_UNLESS(op->GetOperationType() == TUringOperationBase::EWRITE);
    return Submit(op);
}

bool TUringRouter::ReadFixed(void* buf, ui32 size, ui64 offset, ui16 bufIndex, TUringOperationBase* op) {
    Y_ABORT_UNLESS(BuffersRegistered,
        "RegisterBuffers must succeed before ReadFixed");
    op->SetOperationType(TUringOperationBase::EREAD);
    op->PrepareIov(buf, size, offset);
    op->SetFixedBuffer(bufIndex);
    return Submit(op);
}

bool TUringRouter::WriteFixed(const void* buf, ui32 size, ui64 offset, ui16 bufIndex, TUringOperationBase* op) {
    Y_ABORT_UNLESS(BuffersRegistered,
        "RegisterBuffers must succeed before WriteFixed");
    op->SetOperationType(TUringOperationBase::EWRITE);
    op->PrepareIov(const_cast<void*>(buf), size, offset);
    op->SetFixedBuffer(bufIndex);
    return Submit(op);
}

void TUringRouter::Flush() {
}

bool TUringRouter::DrainSubmitQueue() {
    bool didWork = false;
    for (;;) {
        TUringOperationBase* op = PendingSubmit;
        if (op) {
            PendingSubmit = nullptr;
        } else {
            op = Queue.Pop();
            if (!op) {
                break;
            }
            if (op == QueueStopSentinel()) {
                StopSeen = true;
                break;
            }
            NSan::Acquire(op);
        }

        struct io_uring_sqe* sqe = GetSqe();
        if (!sqe) {
            PendingSubmit = op;
            break;
        }
        PrepareSqe(sqe, op);
        didWork = true;
    }
    return didWork;
}

void TUringRouter::SubmitPendingSqes() {
    while (io_uring_sq_ready(Ring.get()) > 0) {
        int ret;
        do {
            ret = io_uring_submit(Ring.get());
        } while (ret == -EINTR);
        Y_ABORT_UNLESS(ret >= 0,
            "io_uring_submit failed: %s (errno %d)", strerror(-ret), -ret);
        if (io_uring_sq_ready(Ring.get()) == 0) {
            return;
        }
        Y_ABORT_UNLESS(ret > 0,
            "io_uring_submit made no progress with SQEs pending");
    }
}

ui32 TUringRouter::ReapCompletions() {
    struct io_uring* ring = Ring.get();
    ui32 count = 0;

    // DEFER_TASKRUN completion work becomes visible only when the issuer enters
    // the kernel. io_uring_peek_cqe() handles IORING_SQ_TASKRUN and remains
    // non-blocking when no work is pending.
    for (;;) {
        struct io_uring_cqe* cqe = nullptr;
        int ret = io_uring_peek_cqe(ring, &cqe);
        if (ret == -EINTR) {
            continue;
        }
        if (ret == -EAGAIN) {
            break;
        }
        Y_ABORT_UNLESS(ret == 0 && cqe,
            "io_uring_peek_cqe failed: %s (errno %d)", strerror(-ret), -ret);

        void* data = io_uring_cqe_get_data(cqe);
        if (data == &WakePollMarker) {
            WakePollArmed = false;
            ui64 drained = 0;
            while (read(WakeEventFd, &drained, sizeof(drained)) > 0) {
            }
        } else if (data == &StopCqeMarker) {
            SawStopCqeMarker = true;
        } else if (auto* op = reinterpret_cast<TUringOperationBase*>(data)) {
            NSan::Acquire(op);
            op->Result = cqe->res;

            if (SampleSink && op->SubmitCycles != 0 && cqe->res >= 0 &&
                    (op->OperationType == TUringOperationBase::EREAD ||
                     op->OperationType == TUringOperationBase::EWRITE)) {
                TDeviceIoSample sample;
                sample.SubmitCycles = op->SubmitCycles;
                sample.CompleteCycles = HPNow();
                sample.Offset = op->GetDiskOffset();
                sample.Size = op->GetOperationBytes();
                sample.IsWrite = op->OperationType == TUringOperationBase::EWRITE;
                SampleSink(sample);
            }

            if constexpr (NSan::MSanIsOn()) {
                if (op->OperationType == TUringOperationBase::EREAD && cqe->res > 0) {
                    size_t remaining = static_cast<size_t>(cqe->res);
                    for (size_t i = op->IovBegin; i < op->Iov.size() && remaining > 0; ++i) {
                        const size_t unpoisonSize = Min(op->Iov[i].iov_len, remaining);
                        NSan::Unpoison(op->Iov[i].iov_base, unpoisonSize);
                        remaining -= unpoisonSize;
                    }
                }
            }

            op->OnComplete(ActorSystem);
            const ui32 previous = InFlightCount.fetch_sub(1, std::memory_order_release);
            Y_DEBUG_ABORT_UNLESS(previous > 0);
        }

        io_uring_cqe_seen(ring, cqe);
        ++count;
    }

    if (count > 0 && Counters) {
        *Counters->CompletionThreadCPU = ThreadCPUTime();
    }
    return count;
}

void TUringRouter::ParkAndWait() {
    Parked.store(true, std::memory_order_seq_cst);

    // Close the race with a producer that published immediately before Parked.
    const bool didWork = DrainSubmitQueue();
    if (didWork || StopSeen) {
        Parked.store(false, std::memory_order_seq_cst);
        if (didWork) {
            SubmitPendingSqes();
        }
        return;
    }

    if (!WakePollArmed) {
        if (struct io_uring_sqe* sqe = GetSqe()) {
            io_uring_prep_poll_add(sqe, WakeEventFd, POLLIN);
            io_uring_sqe_set_data(sqe, &WakePollMarker);
            WakePollArmed = true;
        }
    }

    struct __kernel_timespec ts = MicrosToTimespec(ParkSafetyNetUs);
    struct io_uring_cqe* cqe = nullptr;
    int ret;
    do {
        ret = io_uring_submit_and_wait_timeout(Ring.get(), &cqe, 1, &ts, nullptr);
    } while (ret == -EINTR);
    Y_ABORT_UNLESS(ret >= 0 || ret == -ETIME,
        "io_uring_submit_and_wait_timeout failed: %s (errno %d)", strerror(-ret), -ret);

    Parked.store(false, std::memory_order_seq_cst);
}

void TUringRouter::HandleStop() {
    struct io_uring* ring = Ring.get();

    while (PendingSubmit) {
        SubmitPendingSqes();
        if (struct io_uring_sqe* sqe = GetSqe()) {
            PrepareSqe(sqe, PendingSubmit);
            PendingSubmit = nullptr;
            break;
        }
        if (ReapCompletions() == 0) {
            WaitForCqe(ring);
        }
    }

    // A timed-out park can leave the eventfd poll pending. Wake and reap it
    // before adding IO_DRAIN, otherwise the drain would wait forever.
    if (WakePollArmed) {
        ui64 one = 1;
        ssize_t written;
        do {
            written = write(WakeEventFd, &one, sizeof(one));
        } while (written < 0 && errno == EINTR);
        Y_DEBUG_ABORT_UNLESS(written == static_cast<ssize_t>(sizeof(one)) || errno == EAGAIN);

        while (WakePollArmed) {
            if (ReapCompletions() == 0) {
                WaitForCqe(ring);
            }
        }
    }

    struct io_uring_sqe* sqe = nullptr;
    while (!(sqe = GetSqe())) {
        SubmitPendingSqes();
        if (ReapCompletions() == 0) {
            WaitForCqe(ring);
        }
    }

    io_uring_prep_nop(sqe);
    sqe->flags |= IOSQE_IO_DRAIN;
    io_uring_sqe_set_data(sqe, &StopCqeMarker);
    SubmitPendingSqes();

    SawStopCqeMarker = false;
    while (!SawStopCqeMarker) {
        if (ReapCompletions() == 0) {
            WaitForCqe(ring);
        }
    }

    Y_ABORT_UNLESS(!Queue.Pop(),
        "operation found behind io_uring stop sentinel");
}

void TUringRouter::Stop() {
    ui64 closedState = 0;
    for (;;) {
        ui64 current = Lifecycle.load(std::memory_order_acquire);
        switch (UnpackLifecycle(current)) {
        case LifecycleStopped:
            return;
        case LifecycleStopping:
            StoppedEvent.WaitI();
            return;
        case LifecycleCreated:
        case LifecycleRunning:
            closedState = PackLifecycle(LifecycleStopping, UnpackProducers(current));
            if (Lifecycle.compare_exchange_weak(current, closedState,
                    std::memory_order_acq_rel, std::memory_order_acquire)) {
                break;
            }
            continue;
        }
        break;
    }

    if (UnpackProducers(closedState) != 0) {
        SubmittersDrained.WaitI();
    }

    if (IoThread) {
        Queue.Push(QueueStopSentinel());
        WakeIoThreadIfParked();
        IoThread->Join();
        IoThread.reset();
    }

    Y_ABORT_UNLESS(InFlightCount.load(std::memory_order_acquire) == 0,
        "io_uring stopped with operations still in flight");
    if (Ring) {
        io_uring_queue_exit(Ring.get());
        Ring.reset();
    }

    Lifecycle.store(PackLifecycle(LifecycleStopped, 0), std::memory_order_release);
    StoppedEvent.Signal();
}

bool TUringRouter::IsFileRegistered() const {
    return FixedFdIndex >= 0;
}

bool TUringRouter::AreBuffersRegistered() const {
    return BuffersRegistered;
}

int TUringRouter::GetRegisterFileErrno() const {
    return RegisterFileErrno;
}

int TUringRouter::GetRegisterBuffersErrno() const {
    return RegisterBuffersErrno;
}

EUringFavor TUringRouter::GetUringFavor() const {
    return UsedModernFlags ? EUringFavor::SingleIssuer : EUringFavor::Plain;
}

ui32 TUringRouter::GetInflight() const {
    return InFlightCount.load(std::memory_order_relaxed);
}

bool TUringRouter::Probe(TUringRouterConfig config) {
    struct io_uring ring;
    bool usedModernFlags = false;
    int ret = InitRingWithFallback(&ring, config.QueueDepth, &usedModernFlags);
    if (ret != 0) {
        return false;
    }

    do {
        ret = io_uring_enable_rings(&ring);
    } while (ret == -EINTR);
    io_uring_queue_exit(&ring);
    return ret == 0;
}

TString TUringRouterConfig::ToString() const {
    return TStringBuilder()
        << "QueueDepth=" << QueueDepth
        << " IdleSpinUs=" << IdleSpinUs;
}

} // namespace NKikimr::NPDisk
