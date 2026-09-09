#include "uring_router.h"
#include "uring_router_backend.h"

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
#include <cstdio>
#include <cstring>
#include <utility>

using NActors::TActorSystem;

namespace NKikimr::NPDisk {

namespace {

class TUringBackend final : public NUringPrivate::IUringRouterBackend {
public:
    int Init(unsigned entries, io_uring* ring, io_uring_params* params) override {
        return io_uring_queue_init_params(entries, ring, params);
    }
    int Enable(io_uring* ring) override {
        return io_uring_enable_rings(ring);
    }
    int RegisterFiles(io_uring* ring, const int* files, unsigned count) override {
        return io_uring_register_files(ring, files, count);
    }
    int RegisterBuffers(io_uring* ring, const iovec* buffers, unsigned count) override {
        return io_uring_register_buffers(ring, buffers, count);
    }
    int Submit(io_uring* ring) override {
        return io_uring_submit(ring);
    }
    int PeekCqe(io_uring* ring, io_uring_cqe** cqe) override {
        return io_uring_peek_cqe(ring, cqe);
    }
    int WaitCqeTimeout(io_uring* ring, io_uring_cqe** cqe, __kernel_timespec* timeout) override {
        return io_uring_wait_cqe_timeout(ring, cqe, timeout);
    }
    void Exit(io_uring* ring) override {
        io_uring_queue_exit(ring);
    }
    void Backoff(ui32 micros) override {
        Sleep(TDuration::MicroSeconds(micros));
    }
};

bool IsRetriableRingCall(int result) {
    // These are io_uring_enter errors, not data CQEs or registration errors.
    return result == -EINTR || result == -EAGAIN || result == -EBUSY;
}

// Queue-level stop marker. It is never dereferenced.
alignas(void*) char QueueStopSentinelStorage;

// CQE marker for the drain barrier submitted during shutdown.
alignas(void*) char StopCqeMarker;

// CQE marker for the eventfd poll used to wake a parked I/O thread.
alignas(void*) char WakePollMarker;

TUringOperationBase* QueueStopSentinel() {
    return reinterpret_cast<TUringOperationBase*>(&QueueStopSentinelStorage);
}

void WakeIoThreadIfParked(std::atomic<bool>& parked, int wakeEventFd) {
    if (parked.load(std::memory_order_seq_cst)) {
        ui64 one = 1;
        ssize_t written;
        do {
            written = write(wakeEventFd, &one, sizeof(one));
        } while (written < 0 && errno == EINTR);
        // EAGAIN only means an earlier wake is already retained in eventfd.
        Y_DEBUG_ABORT_UNLESS(written == static_cast<ssize_t>(sizeof(one)) || errno == EAGAIN);
    }
}

// The eventfd poll is the normal wakeup path. This timeout only bounds a
// missed-wakeup race and is deliberately much longer than the idle spin.
constexpr ui32 ParkSafetyNetUs = 5000;
constexpr ui32 RetryBackoffUs = 1000;

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

void ConfigureParams(bool modern, bool submitAll, struct io_uring_params& params) {
    memset(&params, 0, sizeof(params));

    // Start disabled so the dedicated I/O thread becomes the issuer when it
    // enables the ring. This also lets that same thread perform registration.
    params.flags = IORING_SETUP_R_DISABLED;
    if (submitAll) {
        // A submission-time SQE failure still gets its own CQE, while later
        // SQEs in the batch can proceed. Partial submits still require retries.
        params.flags |= IORING_SETUP_SUBMIT_ALL;
    }
    if (modern) {
        params.flags |= IORING_SETUP_SINGLE_ISSUER
            | IORING_SETUP_DEFER_TASKRUN
            | IORING_SETUP_TASKRUN_FLAG;
    }
}

int InitRingWithFallback(NUringPrivate::IUringRouterBackend& backend,
        struct io_uring* ring, ui32 queueDepth, bool* usedModernFlags) {
    const auto init = [&](bool modern, bool submitAll) {
        struct io_uring_params params;
        int ret;
        do {
            ConfigureParams(modern, submitAll, params);
            ret = backend.Init(queueDepth, ring, &params);
        } while (ret == -EINTR);
        return ret;
    };

    int lastError = -EINVAL;
    for (bool modern : {true, false}) {
        int ret = init(modern, true);
        if (!modern && ret == -EINVAL) {
            // Keep SUBMIT_ALL when falling back from modern flags to a plain
            // ring. Older kernels reject the new flag with EINVAL; retry the
            // original plain setup in that case.
            ret = init(false, false);
        }
        if (ret == 0) {
            *usedModernFlags = modern;
            return 0;
        }
        lastError = ret;
    }
    return lastError;
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
        if (!Owner.RingEnabled) {
            return nullptr;
        }

        bool idle = false;
        NHPTimer::STime idleStart = 0;

        while (!Owner.StopSeen) {
            const NHPTimer::STime cycleStart = HPNow();

            Owner.NeedBackoff = false;
            bool didWork = Owner.DrainSubmitQueue();
            // A partial/transient submission leaves SQEs pending even when
            // no producer publishes another operation.
            didWork = Owner.SubmitPendingSqes() || didWork;

            didWork = Owner.ReapCompletions() > 0 || didWork;
            if (Owner.StopSeen) {
                break;
            }

            auto accountBusy = [&] {
                if (Owner.Counters.CompletionThreadBusyTimeNs) {
                    *Owner.Counters.CompletionThreadBusyTimeNs += HPNanoSeconds(HPNow() - cycleStart);
                }
            };

            if (Owner.NeedBackoff) {
                accountBusy();
                Owner.Backend->Backoff(RetryBackoffUs);
                idle = false;
                continue;
            }

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

TUringRouter::TUringRouter(TFileHandle fd, TActorSystem* actorSystem, TUringRouterConfig config, TUringCounters counters)
    : TUringRouter(std::move(fd), actorSystem, config, std::move(counters), std::make_unique<TUringBackend>())
{}

TUringRouter::TUringRouter(TFileHandle fd, TActorSystem* actorSystem, TUringRouterConfig config,
        TUringCounters counters, std::unique_ptr<NUringPrivate::IUringRouterBackend> backend)
    : Fd(std::move(fd))
    , ActorSystem(actorSystem)
    , Config(config)
    , Counters(std::move(counters))
    , Backend(std::move(backend))
    , Ring(new struct io_uring())
    , WakeEventFd(CreateWakeEventFd())
{
    Y_ABORT_UNLESS(WakeEventFd >= 0,
        "eventfd() failed: %s (errno %d)", strerror(errno), errno);

    bool usedModernFlags = false;
    int ret = InitRingWithFallback(*Backend, Ring.get(), Config.QueueDepth, &usedModernFlags);
    if (ret != 0) {
        FailRing("io_uring_queue_init_params", ret);
        return;
    }
    RingInitialized = true;
    UsedModernFlags = usedModernFlags;
}

TUringRouter::~TUringRouter() {
    StopSync();
    if (WakeEventFd >= 0) {
        close(WakeEventFd);
    }
}

void TUringRouter::RegisterFile() {
    if (!RingInitialized) {
        return;
    }
    Y_ABORT_UNLESS(State.load(std::memory_order_acquire) == EUringRouterState::Created,
        "RegisterFile() must be called before Start()");
    Y_ABORT_UNLESS(!IoThread);
    WantRegisterFile = true;
}

void TUringRouter::RegisterBuffers(const struct iovec* iovs, unsigned count) {
    if (!RingInitialized) {
        return;
    }
    Y_ABORT_UNLESS(State.load(std::memory_order_acquire) == EUringRouterState::Created,
        "RegisterBuffers() must be called before Start()");
    Y_ABORT_UNLESS(!IoThread);
    PendingIovs = iovs;
    PendingIovsCount = count;
    WantRegisterBuffers = true;
}

void TUringRouter::Start() {
    if (!RingInitialized) {
        return;
    }
    Y_ABORT_UNLESS(State.load(std::memory_order_acquire) == EUringRouterState::Created,
        "Start() must be called exactly once before StopAsync()");
    Y_ABORT_UNLESS(!IoThread);

    IoThread = std::make_unique<TIoThread>(*this);
    IoThread->Start();
    ReadyEvent.WaitI();

    EUringRouterState expected = EUringRouterState::Created;
    if (!State.compare_exchange_strong(expected, EUringRouterState::Running,
            std::memory_order_release, std::memory_order_relaxed)) {
        Y_ABORT_UNLESS(expected == EUringRouterState::StoppingBroken);
    }
}

void TUringRouter::StopAsync(bool makeBroken) {
    const EUringRouterState target = makeBroken ? EUringRouterState::StoppingBroken : EUringRouterState::Stopping;

    EUringRouterState state = State.load(std::memory_order_acquire);
    for (;;) {
        switch (state) {
        case EUringRouterState::Created:
            [[fallthrough]];
        case EUringRouterState::Running:
            if (State.compare_exchange_weak(state, target,
                    std::memory_order_acq_rel, std::memory_order_acquire)) {
                WakeIoThreadIfParked(Parked, WakeEventFd);
                return;
            }
            break;
        case EUringRouterState::Broken:
            if (State.compare_exchange_weak(state, EUringRouterState::StoppingBroken,
                    std::memory_order_acq_rel, std::memory_order_acquire)) {
                WakeIoThreadIfParked(Parked, WakeEventFd);
                return;
            }
            break;
        case EUringRouterState::Stopping:
            if (!makeBroken) {
                return;
            }
            if (State.compare_exchange_weak(state, EUringRouterState::StoppingBroken,
                    std::memory_order_acq_rel, std::memory_order_acquire)) {
                WakeIoThreadIfParked(Parked, WakeEventFd);
                return;
            }
            break;
        case EUringRouterState::StoppingBroken:
            [[fallthrough]];
        case EUringRouterState::Stopped:
            [[fallthrough]];
        case EUringRouterState::StoppedBroken:
            return;
        default:
            Y_ABORT("Unknown io_uring router state: %u", static_cast<unsigned>(state));
        }
    }
}

void TUringRouter::StopSync() {
    const EUringRouterState state = State.load(std::memory_order_acquire);
    if (state == EUringRouterState::Stopped || state == EUringRouterState::StoppedBroken) {
        return;
    }
    StopAsync();

    if (IoThread) {
        if (RingEnabled) {
            Queue.Push(QueueStopSentinel());
            WakeIoThreadIfParked(Parked, WakeEventFd);
        }
        IoThread->Join();
        IoThread.reset();
    }

    // HandleStop skips the IO_DRAIN fence on a failed ring, so operations can
    // still be kernel-owned here. Releasing the ring would unmap storage the
    // kernel may write. Fail stop rather than retire that ownership silently.
    const ui64 unresolved = InFlightCount.load(std::memory_order_acquire);
    Y_ABORT_UNLESS(unresolved == 0,
        "io_uring router destroyed with %llu unresolved operations",
        static_cast<unsigned long long>(unresolved));

    if (Ring) {
        if (RingInitialized) {
            Backend->Exit(Ring.get());
        }
        Ring.reset();
    }

    State.store(IsBroken() ? EUringRouterState::StoppedBroken : EUringRouterState::Stopped,
        std::memory_order_release);
}

void TUringRouter::WaitSync() {
    while (InFlightCount.load(std::memory_order_acquire) != 0) {
        Sleep(TDuration::MilliSeconds(10));
    }
}

void TUringRouter::InitializeOnIoThread() {
    int ret;
    do {
        ret = Backend->Enable(Ring.get());
    } while (ret == -EINTR);
    if (ret != 0) {
        FailRing("io_uring_enable_rings", ret);
        ReadyEvent.Signal();
        return;
    }
    RingEnabled = true;

    if (WantRegisterFile) {
        int fd = static_cast<FHANDLE>(Fd);
        do {
            ret = Backend->RegisterFiles(Ring.get(), &fd, 1);
        } while (ret == -EINTR);
        if (ret == 0) {
            FixedFdIndex = 0;
        } else {
            RegisterFileErrno = -ret;
        }
    }

    if (WantRegisterBuffers) {
        do {
            ret = Backend->RegisterBuffers(Ring.get(), PendingIovs, PendingIovsCount);
        } while (ret == -EINTR);
        if (ret == 0) {
            BuffersRegistered = true;
        } else {
            RegisterBuffersErrno = -ret;
        }
    }

    ReadyEvent.Signal();
}

void TUringRouter::FailRing(const char* call, int result) {
    if (!FatalRingError) {
        std::fprintf(stderr, "TUringRouter: %s failed: %s (errno %d); stopping broken\n",
            call, strerror(-result), -result);
    }
    // FIXME: implement failed-ring cancellation and a proven resource fence.
    // A failed enter may have published an unconsumed SQ suffix; even EBADR
    // can leave an unknown operation without its CQE. Retain that ownership
    // rather than release kernel-visible storage or claim a completed stop.
    // Until recovery exists, StopSync aborts if any remain after joining the
    // I/O thread, rather than releasing kernel-visible storage.
    FatalRingError = true;
    StopAsync(true);
}

struct io_uring_sqe* TUringRouter::GetSqe() {
    return io_uring_get_sqe(Ring.get());
}

void TUringRouter::PrepareSqe(struct io_uring_sqe* sqe, TUringOperationBase* op) {
    // Use vectored SQEs for genuine scatter-gather and oversized singleton
    // requests; scalar SQEs take an unsigned byte count and would narrow the latter.
    const int fd = FixedFdIndex >= 0 ? FixedFdIndex : static_cast<FHANDLE>(Fd);
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

ui64 TUringRouter::GetInflight() const {
    return InFlightCount.load(std::memory_order_relaxed);
}

bool TUringRouter::Submit(TUringOperationBase* op) {
    Y_ABORT_UNLESS(op);
    Y_ABORT_UNLESS(op->GetOperationType() != TUringOperationBase::ENOT_SET,
        "Submit() called with an unprepared operation");

    if (State.load(std::memory_order_seq_cst) != EUringRouterState::Running) {
        return false;
    }

    // A client may resubmit after a terminal negative CQE. This is a new
    // admission, even though its advanced iovec/progress remains intact.
    op->IsContinuation = false;
    InFlightCount.fetch_add(1, std::memory_order_relaxed);

    NSan::Release(op);
    Queue.Push(op);
    WakeIoThreadIfParked(Parked, WakeEventFd);
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

bool TUringRouter::DrainSubmitQueue() {
    // Reconcile an already-published suffix before preparing unrelated work.
    // While stopping, still consume the producer queue to settle fresh work.
    if (State.load(std::memory_order_acquire) == EUringRouterState::Running
            && Ring->sq.sqe_head != io_uring_load_sq_head(Ring.get())) {
        return false;
    }
    bool didWork = false;
    for (;;) {
        TUringOperationBase* op = PendingSubmit;
        if (op) {
            PendingSubmit = nullptr;
        } else if (!Continuations.empty()) {
            op = Continuations.front();
            Continuations.pop_front();
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

        if (State.load(std::memory_order_acquire) != EUringRouterState::Running) {
            DropOperation(op);
            didWork = true;
            continue;
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

void TUringRouter::DropOperation(TUringOperationBase* op) {
    if (op->IsContinuation) {
        CompleteOperation(op, -ECANCELED);
        return;
    }
    op->OnDrop(ActorSystem);
    const ui64 previous = InFlightCount.fetch_sub(1, std::memory_order_release);
    Y_DEBUG_ABORT_UNLESS(previous > 0);
}

void TUringRouter::CompleteOperation(TUringOperationBase* op, i64 result) {
    op->Result = result;
    op->OnComplete(ActorSystem);
    // OnComplete may destroy or immediately recycle op, including a new
    // admission on this router. Do not access it after the callback.
    const ui64 previous = InFlightCount.fetch_sub(1, std::memory_order_release);
    Y_DEBUG_ABORT_UNLESS(previous > 0);
}

void TUringRouter::DropPendingSqes() {
    auto& sq = Ring->sq;
    const unsigned head = sq.sqe_head;
    const unsigned tail = sq.sqe_tail;

    // These entries have been acquired with io_uring_get_sqe(), but have not
    // yet been exposed to the kernel by io_uring_submit(). Rewind the local
    // tail first so callbacks cannot observe stale pending SQEs.
    sq.sqe_tail = head;

    for (unsigned index = head; index != tail; ++index) {
        struct io_uring_sqe* sqe = &sq.sqes[
            (index & sq.ring_mask) << io_uring_sqe_shift(Ring.get())];
        auto* op = reinterpret_cast<TUringOperationBase*>(static_cast<uintptr_t>(sqe->user_data));
        if (static_cast<void*>(op) == &WakePollMarker) {
            WakePollArmed = false;
            continue;
        }
        if (static_cast<void*>(op) == &StopCqeMarker) {
            StopCqePending = false;
            continue;
        }
        Y_ABORT_UNLESS(op && op != QueueStopSentinel());
        NSan::Acquire(op);
        DropOperation(op);
    }
}

bool TUringRouter::SubmitPendingSqes(bool allowWhileStopping) {
    if ((FatalRingError || (!allowWhileStopping
            && State.load(std::memory_order_acquire) != EUringRouterState::Running))
            && Ring->sq.sqe_head != Ring->sq.sqe_tail) {
        DropPendingSqes();
    }

    if (FatalRingError || io_uring_sq_ready(Ring.get()) == 0) {
        return false;
    }

    const unsigned head = io_uring_load_sq_head(Ring.get());
    // liburing publishes the local SQEs before enter, including on failure.
    // Retry that exact suffix on a subsequent issuer pass; never rewind it or
    // prepare replacement SQEs. One call per pass also bounds repeated EINTR.
    const int ret = Backend->Submit(Ring.get());
    if (ret < 0 && !IsRetriableRingCall(ret)) {
        FailRing("io_uring_submit", ret);
    }
    const bool progressed = head != io_uring_load_sq_head(Ring.get());
    if (!FatalRingError && (ret < 0 || (!progressed && io_uring_sq_ready(Ring.get()) != 0))) {
        NeedBackoff = true;
    }
    return progressed;
}

ui32 TUringRouter::ReapCompletions() {
    struct io_uring* ring = Ring.get();
    ui32 count = 0;

    // Peek performs non-submitting enters for DEFER_TASKRUN and CQ overflow.
    // It remains safe to drain available CQEs after submission is frozen.
    for (;;) {
        struct io_uring_cqe* cqe = nullptr;
        const int ret = Backend->PeekCqe(ring, &cqe);
        if (ret == -EAGAIN) {
            break; // Empty CQ, not a failed data operation.
        }
        if (ret != 0 || !cqe) {
            if (IsRetriableRingCall(ret)) {
                NeedBackoff = true;
            } else {
                FailRing("io_uring_peek_cqe", ret != 0 ? ret : -EIO);
                NeedBackoff = true;
            }
            break;
        }

        void* data = io_uring_cqe_get_data(cqe);
        const i32 result = cqe->res;
        io_uring_cqe_seen(ring, cqe);
        ++count;

        if (data == &WakePollMarker) {
            WakePollArmed = false;
            if (result != POLLIN) {
                FailRing("wake poll CQE", result < 0 ? result : -EIO);
                continue;
            }
            ui64 drained;
            for (;;) {
                const ssize_t bytes = read(WakeEventFd, &drained, sizeof(drained));
                if (bytes == static_cast<ssize_t>(sizeof(drained))) {
                    continue;
                }
                if (bytes < 0 && errno == EINTR) {
                    continue;
                }
                if (bytes < 0 && errno == EAGAIN) {
                    break;
                }
                FailRing("eventfd read", bytes < 0 ? -errno : -EIO);
                break;
            }
        } else if (data == &StopCqeMarker) {
            StopCqePending = false;
            if (result != 0) {
                FailRing("stop drain CQE", result < 0 ? result : -EIO);
            }
        } else {
            auto* op = reinterpret_cast<TUringOperationBase*>(data);
            Y_ABORT_UNLESS(op);
            NSan::Acquire(op);
            const size_t requested = op->GetOperationBytes();
            Y_ABORT_UNLESS(result <= 0 || static_cast<size_t>(result) <= requested,
                "io_uring CQE exceeds the submitted buffer window");

            if constexpr (NSan::MSanIsOn()) {
                if (op->OperationType == TUringOperationBase::EREAD && result > 0) {
                    size_t remaining = static_cast<size_t>(result);
                    for (size_t i = op->IovBegin; i < op->Iov.size() && remaining > 0; ++i) {
                        const size_t unpoisonSize = Min(op->Iov[i].iov_len, remaining);
                        NSan::Unpoison(op->Iov[i].iov_base, unpoisonSize);
                        remaining -= unpoisonSize;
                    }
                }
            }

            // Sampling describes this physical leg, before advancing its view.
            // Preserve the existing sampling of nonnegative CQEs, including EOF.
            if (SampleSink && op->SubmitCycles != 0 && result >= 0) {
                TDeviceIoSample sample;
                sample.SubmitCycles = op->SubmitCycles;
                sample.CompleteCycles = HPNow();
                sample.Offset = op->GetDiskOffset();
                sample.Size = requested;
                sample.IsWrite = op->OperationType == TUringOperationBase::EWRITE;
                SampleSink(sample);
            }

            if (result > 0) {
                op->AdvanceIov(result);
                if (op->GetOperationBytes() != 0) {
                    ++op->ShortIoCount;
                    op->IsContinuation = true;
                    if (State.load(std::memory_order_acquire) == EUringRouterState::Running) {
                        Continuations.push_back(op);
                    } else {
                        CompleteOperation(op, -ECANCELED);
                    }
                    continue;
                }
                CompleteOperation(op, static_cast<i64>(op->GetTotalSize()));
            } else {
                // The exact-size API cannot make progress after EOF/zero write.
                // Actual negative data CQEs retain the client's existing policy.
                CompleteOperation(op, result == 0 && requested != 0 ? -EIO : result);
            }
        }
    }

    if (count > 0 && Counters.CompletionThreadCPU) {
        *Counters.CompletionThreadCPU = ThreadCPUTime();
    }
    return count;
}

void TUringRouter::WaitForProgress() {
    // Never use an untimed wait while an unconsumed suffix needs this issuer
    // to retry submission. Older liburing timed waits create hidden SQEs and
    // cannot safely be retried, so use finite userspace backoff without EXT_ARG.
    if (FatalRingError || io_uring_sq_ready(Ring.get()) != 0
            || !(Ring->features & IORING_FEAT_EXT_ARG)) {
        Backend->Backoff(RetryBackoffUs);
        return;
    }

    struct __kernel_timespec ts = MicrosToTimespec(ParkSafetyNetUs);
    struct io_uring_cqe* cqe = nullptr;
    const int ret = Backend->WaitCqeTimeout(Ring.get(), &cqe, &ts);
    if (ret == 0 || ret == -ETIME) {
        return;
    }
    if (!IsRetriableRingCall(ret)) {
        FailRing("io_uring_wait_cqe_timeout", ret);
    }
    Backend->Backoff(RetryBackoffUs);
}

void TUringRouter::ParkAndWait() {
    if (FatalRingError || State.load(std::memory_order_acquire) != EUringRouterState::Running
            || !(Ring->features & IORING_FEAT_EXT_ARG)) {
        // No hidden timeout or wake-poll SQEs on older kernels. Ingress gets
        // another check within the existing five-millisecond safety interval.
        Backend->Backoff(io_uring_sq_ready(Ring.get()) != 0 || !Continuations.empty()
            || PendingSubmit ? RetryBackoffUs : ParkSafetyNetUs);
        return;
    }

    Parked.store(true, std::memory_order_seq_cst);

    // Close the race with a producer that published immediately before Parked.
    const bool didWork = DrainSubmitQueue();
    if (didWork || StopSeen || State.load(std::memory_order_acquire) != EUringRouterState::Running) {
        Parked.store(false, std::memory_order_seq_cst);
        SubmitPendingSqes();
        return;
    }

    if (!WakePollArmed) {
        if (struct io_uring_sqe* sqe = GetSqe()) {
            io_uring_prep_poll_add(sqe, WakeEventFd, POLLIN);
            io_uring_sqe_set_data(sqe, &WakePollMarker);
            WakePollArmed = true;
        }
    }

    // Submit separately: the wait below must never implicitly publish a suffix
    // after a fatal error, or inject a new timeout on every retry.
    SubmitPendingSqes(/*allowWhileStopping=*/true);
    if (State.load(std::memory_order_acquire) == EUringRouterState::Running) {
        WaitForProgress();
    }
    Parked.store(false, std::memory_order_seq_cst);
}

void TUringRouter::HandleStop() {
    // No fresh data work may be started after stop. Drop pending, continuation,
    // and staged work that has not been published to the kernel.
    if (PendingSubmit) {
        auto* op = std::exchange(PendingSubmit, nullptr);
        DropOperation(op);
    }
    while (!Continuations.empty()) {
        auto* op = Continuations.front();
        Continuations.pop_front();
        DropOperation(op);
    }
    DropPendingSqes();

    // A timed-out park can leave a published poll pending. Retire it before
    // adding IO_DRAIN, otherwise the drain would wait for an unwoken poll.
    if (WakePollArmed) {
        ui64 one = 1;
        ssize_t written;
        do {
            written = write(WakeEventFd, &one, sizeof(one));
        } while (written < 0 && errno == EINTR);
        if (written != static_cast<ssize_t>(sizeof(one)) && !(written < 0 && errno == EAGAIN)) {
            FailRing("eventfd stop wake", written < 0 ? -errno : -EIO);
        }
    }

    while (WakePollArmed || io_uring_sq_ready(Ring.get()) != 0) {
        SubmitPendingSqes(/*allowWhileStopping=*/true);
        if (ReapCompletions() == 0) {
            WaitForProgress();
        }
    }

    if (!FatalRingError) {
        struct io_uring_sqe* sqe = GetSqe();
        Y_ABORT_UNLESS(sqe); // The SQ is empty and QueueDepth must be positive.
        io_uring_prep_nop(sqe);
        sqe->flags |= IOSQE_IO_DRAIN;
        io_uring_sqe_set_data(sqe, &StopCqeMarker);
        StopCqePending = true;

        do {
            SubmitPendingSqes(/*allowWhileStopping=*/true);
            if (ReapCompletions() == 0) {
                WaitForProgress();
            }
        } while (StopCqePending || io_uring_sq_ready(Ring.get()) != 0);
    }

    Y_ABORT_UNLESS(!Queue.Pop(),
        "operation found behind io_uring stop sentinel");
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
    return !RingInitialized ? EUringFavor::FallbackPDisk
        : UsedModernFlags ? EUringFavor::SingleIssuer : EUringFavor::Plain;
}

bool TUringRouter::IsBroken() const {
    const auto state = State.load(std::memory_order_acquire);
    return state == EUringRouterState::Broken
        || state == EUringRouterState::StoppingBroken
        || state == EUringRouterState::StoppedBroken;
}

bool TUringRouter::Probe(TUringRouterConfig config) {
    struct io_uring ring;
    bool usedModernFlags = false;
    TUringBackend backend;
    int ret = InitRingWithFallback(backend, &ring, config.QueueDepth, &usedModernFlags);
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
