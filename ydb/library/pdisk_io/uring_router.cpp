#include "uring_router.h"

#include <ydb/library/actors/core/actorsystem.h>

#include <util/string/builder.h>
#include <util/system/sanitizers.h>
#include <util/system/spinlock.h>
#include <util/system/thread.h>
#include <util/system/yassert.h>

#include <unistd.h>

// liburing.h must be included AFTER YDB headers because <linux/fs.h> (pulled
// in by liburing) defines a BLOCK_SIZE macro that clashes with bitmap.h.
#include <liburing.h>

#include <cerrno>
#include <cstring>

using NActors::TActorSystem;

namespace NKikimr::NPDisk {

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
                auto* op = reinterpret_cast<TUringOperation*>(io_uring_cqe_get_data(cqe));
                if (op) {
                    // The synchronization between the submitter and this poller
                    // goes through io_uring's kernel-mediated SQ/CQ rings, which
                    // TSAN cannot observe.  Acquire here pairs with Release in
                    // PrepareSqe/ReadFixed/WriteFixed.
                    NSan::Acquire(op);
                    op->Result = cqe->res;
                    if (op->OnComplete) {
                        op->OnComplete(op, Owner.ActorSystem);
                    }
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
            auto* op = reinterpret_cast<TUringOperation*>(io_uring_cqe_get_data(cqe));
            if (op) {
                // Same rationale as above: synchronization flows through io_uring
                // rings and is invisible to TSAN.
                NSan::Acquire(op);
                if (op->OnDrop) {
                    op->OnDrop(op);
                }
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
    struct io_uring_params params;
    memset(&params, 0, sizeof(params));

    if (Config.UseSQPoll) {
        params.flags |= IORING_SETUP_SQPOLL;
        params.sq_thread_idle = Config.SqThreadIdleMs;
    }
    if (Config.UseIOPoll) {
        params.flags |= IORING_SETUP_IOPOLL;
    }

    int ret = io_uring_queue_init_params(Config.QueueDepth, Ring, &params);
    Y_ABORT_UNLESS(ret == 0, "io_uring_queue_init_params failed: %s (errno %d)", strerror(-ret), -ret);
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

void TUringRouter::PrepareSqe(struct io_uring_sqe* sqe, bool isRead, void* buf,
                              ui64 size, ui64 offset, TUringOperation* op) {
    // Use readv/writev (IORING_OP_READV/WRITEV) instead of read/write
    // (IORING_OP_READ/WRITE) for kernel 5.4 compatibility.
    // IORING_OP_READ/WRITE were added in 5.6; readv/writev exist since 5.1.
    op->Iov.iov_base = buf;
    op->Iov.iov_len = size;

    int fd = (FixedFdIndex >= 0) ? FixedFdIndex : Fd;
    if (isRead) {
        io_uring_prep_readv(sqe, fd, &op->Iov, 1, offset);
    } else {
        io_uring_prep_writev(sqe, fd, &op->Iov, 1, offset);
    }
    if (FixedFdIndex >= 0) {
        sqe->flags |= IOSQE_FIXED_FILE;
    }
    io_uring_sqe_set_data(sqe, op);
    NSan::Release(op);
}

bool TUringRouter::Read(void* buf, ui64 size, ui64 offset, TUringOperation* op) {
    Y_DEBUG_ABORT_UNLESS(!IsStopping.load(std::memory_order_relaxed), "Read() called after Stop()");
    struct io_uring_sqe* sqe = GetSqe();
    if (!sqe) {
        return false;
    }
    PrepareSqe(sqe, /*isRead=*/true, buf, size, offset, op);
    return true;
}

bool TUringRouter::Write(const void* buf, ui64 size, ui64 offset, TUringOperation* op) {
    Y_DEBUG_ABORT_UNLESS(!IsStopping.load(std::memory_order_relaxed), "Write() called after Stop()");
    struct io_uring_sqe* sqe = GetSqe();
    if (!sqe) {
        return false;
    }
    PrepareSqe(sqe, /*isRead=*/false, const_cast<void*>(buf), size, offset, op);
    return true;
}

bool TUringRouter::ReadFixed(void* buf, ui32 size, ui64 offset, ui16 bufIndex, TUringOperation* op) {
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

bool TUringRouter::WriteFixed(const void* buf, ui32 size, ui64 offset, ui16 bufIndex, TUringOperation* op) {
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

bool TUringRouter::Probe(TUringRouterConfig config) {
    struct io_uring ring;
    struct io_uring_params params;
    memset(&params, 0, sizeof(params));

    if (config.UseSQPoll) {
        params.flags |= IORING_SETUP_SQPOLL;
        params.sq_thread_idle = config.SqThreadIdleMs;
    }
    if (config.UseIOPoll) {
        params.flags |= IORING_SETUP_IOPOLL;
    }

    int ret = io_uring_queue_init_params(config.QueueDepth, &ring, &params);
    if (ret == 0) {
        io_uring_queue_exit(&ring);
        return true;
    }
    return false;
}

} // namespace NKikimr::NPDisk
