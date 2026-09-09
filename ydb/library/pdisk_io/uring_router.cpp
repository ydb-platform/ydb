#include "uring_router.h"

#include <ydb/core/util/hp_timer_helpers.h>
#include <ydb/library/actors/core/actorsystem.h>

#include <util/string/builder.h>
#include <util/system/datetime.h>
#include <util/system/compiler.h>
#include <util/system/sanitizers.h>
#include <util/system/thread.h>
#include <util/system/yassert.h>

#include <unistd.h>

// Must be included AFTER YDB headers because linux/uapi headers pulled by
// liburing may define macros that clash with project headers.
#include <ydb/library/uring/liburing_linux.h>

#include <cerrno>
<<<<<<< HEAD
=======
#include <chrono>
#include <optional>
#include <cstdio>
>>>>>>> 79b4f9ebdea (Fix DDisk/PB shutdown ordering and PDisk restart ownership (#52618))
#include <cstring>
#include <mutex>
#include <vector>

using NActors::TActorSystem;

namespace NKikimr::NPDisk {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

namespace {

constexpr i32 SharedSQPollLeaderIndex = 0;
alignas(void*) char StopCqeMarker;

class TSharedSQPollLeaders {
private:
    struct TLeaderSlot {
        struct io_uring Ring = {};
        bool Initialized = false;

        ~TLeaderSlot() {
            if (Initialized) {
                io_uring_queue_exit(&Ring);
            }
        }
    };

public:
    int GetOrCreateLeaderFd(size_t index, ui32 queueDepth, ui32 sqThreadIdleMs) {
        std::lock_guard guard(Mutex);
        if (Slots.size() <= index) {
            Slots.resize(index + 1);
        }

        if (!Slots[index]) {
            auto slot = std::make_unique<TLeaderSlot>();

            struct io_uring_params params;
            memset(&params, 0, sizeof(params));
            params.flags |= IORING_SETUP_SQPOLL;
            params.sq_thread_idle = sqThreadIdleMs;

            int ret = io_uring_queue_init_params(queueDepth, &slot->Ring, &params);
            if (ret != 0) {
                return ret;
            }

            slot->Initialized = true;
            Slots[index] = std::move(slot);
        }

        return Slots[index]->Ring.ring_fd;
    }

private:
    std::mutex Mutex;
    std::vector<std::unique_ptr<TLeaderSlot>> Slots;
};

std::vector<TUringRouterConfig> BuildFallbackConfigs(const TUringRouterConfig& requestedConfig) {
    std::vector<TUringRouterConfig> configs;
    configs.reserve(5);
    configs.push_back(requestedConfig); // 1. requested config

    TUringRouterConfig fallback = requestedConfig;
    if (fallback.UseSharedSQPoll) {
        fallback.UseSharedSQPoll = false;
        configs.push_back(fallback); // 2. no shared SQPOLL
    }
    if (fallback.UseIOPoll) {
        fallback.UseIOPoll = false;
        configs.push_back(fallback); // 3. no user polling
    }
    if (fallback.UseSQPoll) {
        fallback.UseSQPoll = false;
        configs.push_back(fallback); // 4. no kernel poller
    }

    return configs;
}

int ConfigureParams(const TUringRouterConfig& config, struct io_uring_params& params) {
    memset(&params, 0, sizeof(params));

    if (config.UseSQPoll) {
        params.flags |= IORING_SETUP_SQPOLL;
        params.sq_thread_idle = config.SqThreadIdleMs;

        if (config.UseSharedSQPoll) {
            static TSharedSQPollLeaders sharedLeaders;
            int sharedFd = sharedLeaders.GetOrCreateLeaderFd(
                SharedSQPollLeaderIndex,
                config.QueueDepth,
                config.SqThreadIdleMs);
            if (sharedFd < 0) {
                return sharedFd;
            }
            params.flags |= IORING_SETUP_ATTACH_WQ;
            params.wq_fd = sharedFd;
        }
    }

    if (config.UseIOPoll) {
        params.flags |= IORING_SETUP_IOPOLL;
    }

    return 0;
}

int InitRingWithFallback(struct io_uring* ring, const TUringRouterConfig& requestedConfig,
                         TUringRouterConfig* effectiveConfig) {
    int lastError = -EINVAL;
    for (const TUringRouterConfig& config : BuildFallbackConfigs(requestedConfig)) {
        struct io_uring_params params;
        if (int paramsRet = ConfigureParams(config, params); paramsRet != 0) {
            lastError = paramsRet;
            continue;
        }

        int ret = io_uring_queue_init_params(config.QueueDepth, ring, &params);
        if (ret == 0) {
            *effectiveConfig = config;
            return 0;
        }

        lastError = ret;
    }

    return lastError;
}

} // anonymous

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TCompletionPoller
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

class TUringRouter::TCompletionPoller : public ISimpleThread {
public:
    TCompletionPoller(TUringRouter& owner)
        : Owner(owner)
    {}

    void* ThreadProc() override {
        SetCurrentThreadName("UringCmpl");

        bool stopSeen = false;
        while (!stopSeen) {
            unsigned head;
            unsigned count = 0;
            struct io_uring_cqe* cqe;

            {
                // liburing itself will decide if it should enter the kernel
                struct io_uring_cqe* waitCqe = nullptr;
                io_uring_wait_cqe(Owner.Ring.get(), &waitCqe);
            }

            auto cycleStart = HPNow();
            io_uring_for_each_cqe(Owner.Ring.get(), head, cqe) {
                void* data = io_uring_cqe_get_data(cqe);
                if (data == &StopCqeMarker) {
                    ++count;
                    stopSeen = true;
                    break;
                }

                auto* op = reinterpret_cast<TUringOperationBase*>(data);
                if (op) {
                    // The synchronization between the submitter and this poller
                    // goes through io_uring's kernel-mediated SQ/CQ rings, which
                    // TSAN cannot observe.  Acquire here pairs with Release in
                    // PrepareSqe/ReadFixed/WriteFixed.
                    NSan::Acquire(op);
                    op->Result = cqe->res;
                    // For read operations the kernel fills the buffer via a syscall
                    // that MSAN cannot observe.  Mark each iovec segment as initialized
                    // so that subsequent reads do not trigger false use-of-uninitialized-
                    // value reports.  Walk the active iovec window (starting at IovBegin)
                    // and unpoison up to cqe->res bytes across all segments.
                    if constexpr (NSan::MSanIsOn()) {
                        if (op->OperationType == TUringOperationBase::EREAD && cqe->res > 0) {
                            size_t remaining = static_cast<size_t>(cqe->res);
                            for (size_t i = op->IovBegin; i < op->Iov.size() && remaining > 0; ++i) {
                                size_t unpoisonSize = op->Iov[i].iov_len;
                                if (unpoisonSize > remaining) {
                                    unpoisonSize = remaining;
                                }
                                NSan::Unpoison(op->Iov[i].iov_base, unpoisonSize);
                                remaining -= unpoisonSize;
                            }
                        }
                    }
                    op->OnComplete(Owner.ActorSystem);
                }
                ++count;
            }

            if (count > 0) {
                io_uring_cq_advance(Owner.Ring.get(), count);
                Owner.InFlightCount.fetch_sub(count, std::memory_order_release);
            }

            auto cycleEnd = HPNow();
            if (Owner.Counters) {
                *Owner.Counters->CompletionThreadBusyTimeNs += HPNanoSeconds(cycleEnd - cycleStart);
                *Owner.Counters->CompletionThreadCPU = ThreadCPUTime();
            }
        }

        return nullptr;
    }

private:
    TUringRouter& Owner;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TUringRouter
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

TUringRouter::TUringRouter(FHANDLE fd, TActorSystem* actorSystem, TUringRouterConfig config, TUringCounters* counters)
    : Fd(fd)
    , ActorSystem(actorSystem)
    , Config(config)
    , Counters(counters)
    , Ring(new struct io_uring())
{
    TUringRouterConfig effectiveConfig = Config;
    int ret = InitRingWithFallback(Ring.get(), Config, &effectiveConfig);
    Y_ABORT_UNLESS(ret == 0, "io_uring_queue_init_params failed after fallbacks: %s (errno %d)", strerror(-ret), -ret);
    Config = effectiveConfig;
}

TUringRouter::~TUringRouter() {
    Stop();
}

std::expected<void, int> TUringRouter::RegisterFile() {
    int fd = Fd;
    int ret = io_uring_register_files(Ring.get(), &fd, 1);
    if (ret == 0) {
        FixedFdIndex = 0;
        return {};
    }
    return std::unexpected(-ret);
}

std::expected<void, int> TUringRouter::RegisterBuffers(const struct iovec* iovs, unsigned count) {
    int ret = io_uring_register_buffers(Ring.get(), iovs, count);
    if (ret == 0) {
        BuffersRegistered = true;
        return {};
    }
    return std::unexpected(-ret);
}

void TUringRouter::Start() {
<<<<<<< HEAD
    Y_ABORT_UNLESS(!Poller, "Start() called twice");
    Poller = std::make_unique<TCompletionPoller>(*this);
    Poller->Start();
=======
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
                    std::memory_order_seq_cst, std::memory_order_acquire)) {
                WakeIoThreadIfParked(Parked, WakeEventFd);
                return;
            }
            break;
        case EUringRouterState::Broken:
            if (State.compare_exchange_weak(state, EUringRouterState::StoppingBroken,
                    std::memory_order_seq_cst, std::memory_order_acquire)) {
                WakeIoThreadIfParked(Parked, WakeEventFd);
                return;
            }
            break;
        case EUringRouterState::Stopping:
            if (!makeBroken) {
                return;
            }
            if (State.compare_exchange_weak(state, EUringRouterState::StoppingBroken,
                    std::memory_order_seq_cst, std::memory_order_acquire)) {
                WakeIoThreadIfParked(Parked, WakeEventFd);
                return;
            }
            break;
        case EUringRouterState::Stopped:
            if (makeBroken && !State.compare_exchange_weak(state, EUringRouterState::StoppedBroken,
                    std::memory_order_seq_cst, std::memory_order_acquire)) {
                break;
            }
            return;
        case EUringRouterState::StoppingBroken:
        case EUringRouterState::StoppedBroken:
            return;
        default:
            Y_ABORT("Unknown io_uring router state: %u", static_cast<unsigned>(state));
        }
    }
}

void TUringRouter::StopSync() {
    std::lock_guard guard(StopMutex);
    const EUringRouterState state = State.load(std::memory_order_acquire);
    if (state == EUringRouterState::Stopped || state == EUringRouterState::StoppedBroken) {
        return;
    }
    StopAsync();
    while (Publishers.load(std::memory_order_seq_cst)) {
        if (TestHooks && TestHooks->WaitingForPublishers) { TestHooks->WaitingForPublishers(); }
        Sleep(TDuration::MilliSeconds(1));
    }

    if (IoThread) {
        if (RingEnabled) {
            Queue.Push(QueueStopSentinel());
            WakeIoThreadIfParked(Parked, WakeEventFd);
        }
        if (TestHooks && TestHooks->BeforeJoin) { TestHooks->BeforeJoin(); }
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

    Fd.Close();
    if (TestHooks && TestHooks->Retired) { TestHooks->Retired(); }
    auto finalState = State.load(std::memory_order_acquire);
    if (TestHooks && TestHooks->BeforeFinalStopCas) { TestHooks->BeforeFinalStopCas(); }
    while (!State.compare_exchange_weak(finalState,
            finalState == EUringRouterState::StoppingBroken ? EUringRouterState::StoppedBroken : EUringRouterState::Stopped,
            std::memory_order_acq_rel, std::memory_order_acquire)) {
    }
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
>>>>>>> 79b4f9ebdea (Fix DDisk/PB shutdown ordering and PDisk restart ownership (#52618))
}

struct io_uring_sqe* TUringRouter::GetSqe() {
    auto sqe = io_uring_get_sqe(Ring.get());
    if (sqe) {
        InFlightCount.fetch_add(1, std::memory_order_acquire);
    }

    return sqe;
}

void TUringRouter::PrepareSqe(struct io_uring_sqe* sqe, TUringOperationBase* op) {
    // Use readv/writev (IORING_OP_READV/WRITEV) instead of read/write
    // (IORING_OP_READ/WRITE) for kernel 5.4 compatibility.
    // IORING_OP_READ/WRITE were added in 5.6; readv/writev exist since 5.1.
    // Supports scatter-gather: Iov may contain multiple entries (writes); the
    // active window starts at IovBegin and is advanced by AdvanceIov on short I/O.
    int fd = (FixedFdIndex >= 0) ? FixedFdIndex : Fd;
    Y_ABORT_UNLESS(op->IovBegin < op->Iov.size(), "PrepareSqe called with empty iovec window");
    const unsigned iovCount = static_cast<unsigned>(op->Iov.size() - op->IovBegin);
    switch (op->OperationType) {
    case TUringOperationBase::EREAD:
        io_uring_prep_readv(sqe, fd, &op->Iov[op->IovBegin], iovCount, op->DiskOffset);
        break;
    case TUringOperationBase::EWRITE:
        io_uring_prep_writev(sqe, fd, &op->Iov[op->IovBegin], iovCount, op->DiskOffset);
        break;
    default:
        Y_ABORT("Unknown OperationType");
    }

    if (FixedFdIndex >= 0) {
        sqe->flags |= IOSQE_FIXED_FILE;
    }

    io_uring_sqe_set_data(sqe, op);
    NSan::Release(op);
}

bool TUringRouter::Read(TUringOperationBase* op) {
    Y_DEBUG_ABORT_UNLESS(Ring, "Read() called after Stop()");
    Y_ABORT_UNLESS(op->OperationType == TUringOperationBase::EREAD);

<<<<<<< HEAD
    struct io_uring_sqe* sqe = GetSqe();
    if (!sqe) {
        return false;
    }
    PrepareSqe(sqe, op);
=======
bool TUringRouter::Submit(TUringOperationBase* op) {
    Y_ABORT_UNLESS(op);
    Y_ABORT_UNLESS(op->GetOperationType() != TUringOperationBase::ENOT_SET,
        "Submit() called with an unprepared operation");

    Publishers.fetch_add(1, std::memory_order_seq_cst);
    if (State.load(std::memory_order_seq_cst) != EUringRouterState::Running) {
        Publishers.fetch_sub(1, std::memory_order_seq_cst);
        return false;
    }

    if (TestHooks && TestHooks->AfterAdmission) { TestHooks->AfterAdmission(); }

    // A client may resubmit after a terminal negative CQE. This is a new
    // admission, even though its advanced iovec/progress remains intact.
    op->IsContinuation = false;
    InFlightCount.fetch_add(1, std::memory_order_relaxed);

    NSan::Release(op);
    Queue.Push(op);
    if (TestHooks && TestHooks->AfterPublication) { TestHooks->AfterPublication(); }
    WakeIoThreadIfParked(Parked, WakeEventFd);
    if (TestHooks && TestHooks->AfterWake) { TestHooks->AfterWake(); }
    Publishers.fetch_sub(1, std::memory_order_seq_cst);
>>>>>>> 79b4f9ebdea (Fix DDisk/PB shutdown ordering and PDisk restart ownership (#52618))
    return true;
}

bool TUringRouter::Write(TUringOperationBase* op) {
    Y_DEBUG_ABORT_UNLESS(Ring, "Write() called after Stop()");
    Y_ABORT_UNLESS(op->OperationType == TUringOperationBase::EWRITE);

    struct io_uring_sqe* sqe = GetSqe();
    if (!sqe) {
        return false;
    }
    PrepareSqe(sqe, op);
    return true;
}

bool TUringRouter::ReadFixed(void* buf, ui32 size, ui64 offset, ui16 bufIndex, TUringOperationBase* op) {
    Y_DEBUG_ABORT_UNLESS(Ring, "ReadFixed() called after Stop()");
    Y_ABORT_UNLESS(BuffersRegistered, "RegisterBuffers must be called before ReadFixed");
    struct io_uring_sqe* sqe = GetSqe();
    if (!sqe) {
        return false;
    }
    // Populate op->Iov and OperationType so the completion handler's MSan
    // unpoison loop, GetIovBase(), and GetOperationBytes() work uniformly.
    // The actual kernel submission still uses the fixed-buffer variant below.
    op->SetOperationType(TUringOperationBase::EREAD);
    op->PrepareIov(buf, size, offset);
    int fd = (FixedFdIndex >= 0) ? FixedFdIndex : Fd;
    io_uring_prep_read_fixed(sqe, fd, buf, size, offset, bufIndex);
    if (FixedFdIndex >= 0) {
        sqe->flags |= IOSQE_FIXED_FILE;
    }
    io_uring_sqe_set_data(sqe, op);
    NSan::Release(op);
    return true;
}

bool TUringRouter::WriteFixed(const void* buf, ui32 size, ui64 offset, ui16 bufIndex, TUringOperationBase* op) {
    Y_DEBUG_ABORT_UNLESS(Ring, "WriteFixed() called after Stop()");
    Y_ABORT_UNLESS(BuffersRegistered, "RegisterBuffers must be called before WriteFixed");
    struct io_uring_sqe* sqe = GetSqe();
    if (!sqe) {
        return false;
    }
    // Mirror ReadFixed: populate op->Iov and OperationType for consistent
    // GetIovBase() logging and GetOperationBytes() bookkeeping in OnComplete.
    op->SetOperationType(TUringOperationBase::EWRITE);
    op->PrepareIov(const_cast<void*>(buf), size, offset);
    int fd = (FixedFdIndex >= 0) ? FixedFdIndex : Fd;
    io_uring_prep_write_fixed(sqe, fd, buf, size, offset, bufIndex);
    if (FixedFdIndex >= 0) {
        sqe->flags |= IOSQE_FIXED_FILE;
    }
    io_uring_sqe_set_data(sqe, op);
    NSan::Release(op);
    return true;
}

void TUringRouter::Flush() {
    Y_DEBUG_ABORT_UNLESS(Ring, "Flush() called after Stop()");
    // Always call io_uring_submit().  It does two things:
    // 1. Flushes the SQ ring tail (__io_uring_flush_sq) so the kernel (or
    //    the SQPOLL thread) can see newly prepared SQEs.  Without this,
    //    io_uring_get_sqe() only updates an internal userspace counter;
    //    the kernel-visible *sq->ktail stays stale.
    // 2. Calls io_uring_enter() when needed: always for non-SQPOLL,
    //    and only for SQPOLL wakeup when IORING_SQ_NEED_WAKEUP is set.
    io_uring_submit(Ring.get());
}

void TUringRouter::Stop() {
    if (!Ring) {
        return; // Already stopped
    }

    if (Poller) {
        // Submit a stop marker that will complete only after all previously
        // submitted operations (including those not yet flushed) are complete.
        while (true) {
            struct io_uring_sqe* sqe = GetSqe();
            if (sqe) {
                io_uring_prep_nop(sqe);
                sqe->flags |= IOSQE_IO_DRAIN;
                io_uring_sqe_set_data(sqe, &StopCqeMarker);
                break;
            }

            int ret = io_uring_submit(Ring.get());
            Y_ABORT_UNLESS(ret >= 0, "io_uring_submit failed in Stop while reserving marker SQE: %s (errno %d)",
                strerror(-ret), -ret);
        }

        int ret = io_uring_submit(Ring.get());
        Y_ABORT_UNLESS(ret >= 0, "io_uring_submit failed in Stop: %s (errno %d)", strerror(-ret), -ret);

        Poller->Join();
        Poller.reset();
    }

    io_uring_queue_exit(Ring.get());
    Ring.reset();
}

<<<<<<< HEAD
ui32 TUringRouter::SubmitItemsLeft() const {
    Y_DEBUG_ABORT_UNLESS(Ring, "SubmitItemsLeft() called after Stop()");
    return io_uring_sq_space_left(Ring.get());
=======
void TUringRouter::DropOperation(TUringOperationBase* op) {
    if (op->IsContinuation) {
        CompleteOperation(op, -ECANCELED);
        return;
    }
    if (TestHooks && TestHooks->BeforeTerminalCallback) { TestHooks->BeforeTerminalCallback(); }
    op->OnDrop(ActorSystem);
    const ui64 previous = InFlightCount.fetch_sub(1, std::memory_order_release);
    Y_DEBUG_ABORT_UNLESS(previous > 0);
}

void TUringRouter::CompleteOperation(TUringOperationBase* op, i64 result) {
    op->Result = result;
    if (TestHooks && TestHooks->BeforeTerminalCallback) { TestHooks->BeforeTerminalCallback(); }
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
    std::optional<std::chrono::steady_clock::time_point> fatalDeadline;
    auto checkFatalDrain = [&] {
        if (!FatalRingError) {
            return;
        }
        const auto now = Backend->Now();
        if (!fatalDeadline) {
            fatalDeadline = now + std::chrono::milliseconds(200);
        }
        Y_ABORT_UNLESS(now < *fatalDeadline,
            "io_uring fatal shutdown timed out: inflight=%llu sq=%u wake=%d stop=%d",
            static_cast<unsigned long long>(GetInflight()), io_uring_sq_ready(Ring.get()),
            WakePollArmed, StopCqePending);
    };
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
        checkFatalDrain();
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
            checkFatalDrain();
            SubmitPendingSqes(/*allowWhileStopping=*/true);
            if (ReapCompletions() == 0) {
                WaitForProgress();
            }
        } while (StopCqePending || io_uring_sq_ready(Ring.get()) != 0);
    }

    while (GetInflight() || WakePollArmed || StopCqePending || io_uring_sq_ready(Ring.get())) {
        checkFatalDrain();
        if (ReapCompletions() == 0) {
            WaitForProgress();
        }
    }
    Y_ABORT_UNLESS(!Queue.Pop(),
        "operation found behind io_uring stop sentinel");
>>>>>>> 79b4f9ebdea (Fix DDisk/PB shutdown ordering and PDisk restart ownership (#52618))
}

bool TUringRouter::IsFileRegistered() const {
    return FixedFdIndex >= 0;
}

EUringFavor TUringRouter::GetUringFavor() const {
    return Config.GetUringFavor();
}

ui32 TUringRouter::GetInflight() const {
    return InFlightCount.load(std::memory_order_relaxed);
}

bool TUringRouter::Probe(TUringRouterConfig config) {
    struct io_uring ring;
    TUringRouterConfig effectiveConfig = config;
    int ret = InitRingWithFallback(&ring, config, &effectiveConfig);
    if (ret == 0) {
        io_uring_queue_exit(&ring);
        return true;
    }
    return false;
}

TString TUringRouterConfig::ToString() const {
    auto boolToString = [](bool value) -> const char* {
        return value ? "true" : "false";
    };

    return TStringBuilder()
        << "QueueDepth=" << QueueDepth
        << " SqThreadIdleMs=" << SqThreadIdleMs
        << " UseSQPoll=" << boolToString(UseSQPoll)
        << " UseIOPoll=" << boolToString(UseIOPoll)
        << " UseSharedSQPoll=" << boolToString(UseSharedSQPoll)
        << " Favor=" << GetUringFavor();
}

} // namespace NKikimr::NPDisk
