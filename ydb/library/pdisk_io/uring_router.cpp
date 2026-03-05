#include "uring_router.h"

#include <ydb/library/actors/core/actorsystem.h>

#include <util/string/builder.h>
#include <util/system/sanitizers.h>
#include <util/system/spinlock.h>
#include <util/system/thread.h>
#include <util/system/yassert.h>

#include <unistd.h>

// Must be included AFTER YDB headers because linux/uapi headers pulled by
// liburing may define macros that clash with project headers.
#include "liburing_compat.h"

#include <cerrno>
#include <cstring>
#include <mutex>
#include <vector>

using NActors::TActorSystem;

namespace NKikimr::NPDisk {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

namespace {

constexpr i32 SharedSQPollLeaderIndex = 0;

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

        // With IOPOLL (and no SQPOLL), the kernel does not post CQEs via
        // interrupts.  We must call io_uring_enter(IORING_ENTER_GETEVENTS)
        // to trigger the kernel to poll the block device for completions.
        // With SQPOLL, the kernel SQPOLL thread handles reaping internally.
        const bool needIoPollReap = Owner.Config.UseIOPoll && !Owner.Config.UseSQPoll;
        const bool useIOPoll = Owner.Config.UseIOPoll;

        while (!Owner.IsStopping.load(std::memory_order_acquire)) {
            // For IOPOLL without SQPOLL, ask the kernel to reap polled
            // completions before we peek the CQ ring.
            if (needIoPollReap) {
                int ret;
                do {
                    ret = io_uring_enter(Owner.Ring->ring_fd, 0, 0, IORING_ENTER_GETEVENTS, nullptr);
                } while (ret == -EINTR);
            }

            unsigned head;
            unsigned count = 0;
            struct io_uring_cqe* cqe;

            io_uring_for_each_cqe(Owner.Ring, head, cqe) {
                auto* op = reinterpret_cast<TUringOperationBase*>(io_uring_cqe_get_data(cqe));
                if (op) {
                    // The synchronization between the submitter and this poller
                    // goes through io_uring's kernel-mediated SQ/CQ rings, which
                    // TSAN cannot observe.  Acquire here pairs with Release in
                    // PrepareSqe/ReadFixed/WriteFixed.
                    NSan::Acquire(op);
                    op->Result = cqe->res;
                    op->OnComplete(Owner.ActorSystem);
                }
                ++count;
            }

            if (count > 0) {
                io_uring_cq_advance(Owner.Ring, count);
            } else if (useIOPoll) {
                // IOPOLL mode (with or without SQPOLL): tight spin with a CPU
                // pause hint.  The poller thread is dedicated, so burning one
                // core gives the lowest possible completion latency.
                //
                // TODO: maybe we want to go to sleep after some amount of time?
                SpinLockPause();
            } else {
                // Interrupt-driven mode: block in the kernel until a CQE arrives.
                // Don't process it here, loop back and let io_uring_for_each_cqe handle it.
                // On shutdown, Stop() submits a NOP to wake this call.
                struct io_uring_cqe* waitCqe = nullptr;
                io_uring_wait_cqe(Owner.Ring, &waitCqe);
            }
        }

        // Drain any remaining CQEs after stop (don't call OnComplete).
        // If provided, call OnDrop so owner memory can be reclaimed.
        //
        // With IOPOLL, completions are not posted via interrupts; we must
        // call io_uring_enter(IORING_ENTER_GETEVENTS) to reap completed I/Os
        // from the device into the CQ ring before peeking.  Operations still
        // truly in-flight on the device will be cleaned up by
        // io_uring_queue_exit() in Stop().
        if (useIOPoll) {
            int ret;
            do {
                ret = io_uring_enter(Owner.Ring->ring_fd, 0, 0, IORING_ENTER_GETEVENTS, nullptr);
            } while (ret == -EINTR);
        }

        unsigned head;
        unsigned count = 0;
        struct io_uring_cqe* cqe;
        io_uring_for_each_cqe(Owner.Ring, head, cqe) {
            auto* op = reinterpret_cast<TUringOperationBase*>(io_uring_cqe_get_data(cqe));
            if (op) {
                // Same rationale as above: synchronization flows through io_uring
                // rings and is invisible to TSAN.
                NSan::Acquire(op);
                op->OnDrop();
            }
            ++count;
        }
        if (count > 0) {
            io_uring_cq_advance(Owner.Ring, count);
        }

        return nullptr;
    }

private:
    TUringRouter& Owner;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TUringRouter
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

TUringRouter::TUringRouter(FHANDLE fd, TActorSystem* actorSystem, TUringRouterConfig config)
    : Fd(fd)
    , ActorSystem(actorSystem)
    , Config(config)
    , Ring(new struct io_uring())
{
    TUringRouterConfig effectiveConfig = Config;
    int ret = InitRingWithFallback(Ring, Config, &effectiveConfig);
    Y_ABORT_UNLESS(ret == 0, "io_uring_queue_init_params failed after fallbacks: %s (errno %d)", strerror(-ret), -ret);
    Config = effectiveConfig;
}

TUringRouter::~TUringRouter() {
    Stop();
}

std::expected<void, int> TUringRouter::RegisterFile() {
    int fd = Fd;
    int ret = io_uring_register_files(Ring, &fd, 1);
    if (ret == 0) {
        FixedFdIndex = 0;
        return {};
    }
    return std::unexpected(-ret);
}

std::expected<void, int> TUringRouter::RegisterBuffers(const struct iovec* iovs, unsigned count) {
    int ret = io_uring_register_buffers(Ring, iovs, count);
    if (ret == 0) {
        BuffersRegistered = true;
        return {};
    }
    return std::unexpected(-ret);
}

void TUringRouter::Start() {
    Y_ABORT_UNLESS(!Poller, "Start() called twice");
    Poller = std::make_unique<TCompletionPoller>(*this);
    Poller->Start();
}

struct io_uring_sqe* TUringRouter::GetSqe() {
    return io_uring_get_sqe(Ring);
}

void TUringRouter::PrepareSqe(struct io_uring_sqe* sqe, TUringOperationBase* op) {
    // Use readv/writev (IORING_OP_READV/WRITEV) instead of read/write
    // (IORING_OP_READ/WRITE) for kernel 5.4 compatibility.
    // IORING_OP_READ/WRITE were added in 5.6; readv/writev exist since 5.1.
    int fd = (FixedFdIndex >= 0) ? FixedFdIndex : Fd;
    switch (op->OperationType) {
    case TUringOperationBase::EREAD:
        io_uring_prep_readv(sqe, fd, &op->Iov, 1, op->DiskOffset);
        break;
    case TUringOperationBase::EWRITE:
        io_uring_prep_writev(sqe, fd, &op->Iov, 1, op->DiskOffset);
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
    Y_DEBUG_ABORT_UNLESS(!IsStopping.load(std::memory_order_relaxed), "Read() called after Stop()");
    Y_ABORT_UNLESS(op->OperationType == TUringOperationBase::EREAD);

    struct io_uring_sqe* sqe = GetSqe();
    if (!sqe) {
        return false;
    }
    PrepareSqe(sqe, op);
    return true;
}

bool TUringRouter::Write(TUringOperationBase* op) {
    Y_DEBUG_ABORT_UNLESS(!IsStopping.load(std::memory_order_relaxed), "Write() called after Stop()");
    Y_ABORT_UNLESS(op->OperationType == TUringOperationBase::EWRITE);

    struct io_uring_sqe* sqe = GetSqe();
    if (!sqe) {
        return false;
    }
    PrepareSqe(sqe, op);
    return true;
}

bool TUringRouter::ReadFixed(void* buf, ui32 size, ui64 offset, ui16 bufIndex, TUringOperationBase* op) {
    Y_DEBUG_ABORT_UNLESS(!IsStopping.load(std::memory_order_relaxed), "ReadFixed() called after Stop()");
    Y_ABORT_UNLESS(BuffersRegistered, "RegisterBuffers must be called before ReadFixed");
    struct io_uring_sqe* sqe = GetSqe();
    if (!sqe) {
        return false;
    }
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
    Y_DEBUG_ABORT_UNLESS(!IsStopping.load(std::memory_order_relaxed), "WriteFixed() called after Stop()");
    Y_ABORT_UNLESS(BuffersRegistered, "RegisterBuffers must be called before WriteFixed");
    struct io_uring_sqe* sqe = GetSqe();
    if (!sqe) {
        return false;
    }
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
    Y_DEBUG_ABORT_UNLESS(!IsStopping.load(std::memory_order_relaxed), "Flush() called after Stop()");
    // Always call io_uring_submit().  It does two things:
    // 1. Flushes the SQ ring tail (__io_uring_flush_sq) so the kernel (or
    //    the SQPOLL thread) can see newly prepared SQEs.  Without this,
    //    io_uring_get_sqe() only updates an internal userspace counter;
    //    the kernel-visible *sq->ktail stays stale.
    // 2. Calls io_uring_enter() when needed: always for non-SQPOLL,
    //    and only for SQPOLL wakeup when IORING_SQ_NEED_WAKEUP is set.
    io_uring_submit(Ring);
}

void TUringRouter::WakePoller() {
    // Submit a NOP with null user_data to produce a CQE that unblocks the
    // completion poller if it is waiting in io_uring_wait_cqe().
    // The poller already handles null op (skips OnComplete), so this is safe.
    struct io_uring_sqe* sqe = io_uring_get_sqe(Ring);
    if (sqe) {
        io_uring_prep_nop(sqe);
        io_uring_sqe_set_data(sqe, nullptr);
        io_uring_submit(Ring);
    } else {
        // ring is full, so that there are already events to wakeup poller
    }
}

void TUringRouter::Stop() {
    if (IsStopping.exchange(true, std::memory_order_acq_rel)) {
        return; // Already stopped
    }

    if (Poller) {
        // In interrupt-driven mode (no IOPOLL), the poller may be blocked in
        // io_uring_wait_cqe().  Submit a NOP to wake it so it can see IsStopping==true.
        if (!Config.UseIOPoll) {
            WakePoller();
        }
        Poller->Join();
        Poller.reset();
    }

    io_uring_queue_exit(Ring);
    delete Ring;
    Ring = nullptr;
}

ui32 TUringRouter::SubmitItemsLeft() const {
    Y_DEBUG_ABORT_UNLESS(!IsStopping.load(std::memory_order_relaxed), "SubmitItemsLeft() called after Stop()");
    return io_uring_sq_space_left(Ring);
}

bool TUringRouter::IsFileRegistered() const {
    return FixedFdIndex >= 0;
}

EUringFavor TUringRouter::GetUringFavor() const {
    return Config.GetUringFavor();
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
