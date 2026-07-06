#include "uring_context.h"

// Must be included AFTER YDB headers because linux/uapi headers pulled by
// liburing may define macros that clash with project headers.
#include <ydb/library/uring/liburing_linux.h>

#include <sys/eventfd.h>
#include <unistd.h>

namespace NActors {

    TUringContext::TUringContext(TActorId writeActorId, TActorId readActorId)
        : WriteActorId(writeActorId)
        , ReadActorId(readActorId)
    {
        EventFd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        Y_ABORT_UNLESS(EventFd >= 0, "eventfd() failed: %s", strerror(errno));
    }

    TUringContext::~TUringContext() {
        if (Ring) {
            io_uring_queue_exit(Ring.get());
        }
        if (EventFd >= 0) {
            close(EventFd);
        }
    }

    bool TUringContext::Init(int anchorWqFd, bool enableSqpoll) {
        Ring = std::make_unique<struct io_uring>();

        struct io_uring_params params = {};
        // SQPOLL is opt-in (TInterconnectSettings::EnableUringSQPOLL, default off). When off,
        // submissions are issued explicitly via io_uring_submit() (io_uring_enter) on the
        // session mailbox thread, which makes the writev complete and signal the eventfd
        // deterministically with no dependency on a kernel poll thread, and avoids a busy
        // poll thread. When on, the anchor ring also has SQPOLL and we set SQPOLL + ATTACH_WQ
        // here (wq_fd = anchor), so all session rings + the anchor share ONE kernel poll thread
        // (not one per session); liburing's io_uring_submit() wakes an idle SQPOLL thread via
        // IORING_SQ_NEED_WAKEUP. Note: SQPOLL was originally suspected of causing an idle
        // keepalive meltdown, but that was traced to unrelated reaper bugs (see uring_poller_actor.cpp).
        if (enableSqpoll) {
            params.flags |= IORING_SETUP_SQPOLL;
            params.sq_thread_idle = SqThreadIdleMs;
        }
        if (anchorWqFd >= 0) {
            // Share the async io-wq backend with the anchor ring so we do not spawn a separate
            // worker pool per session ring.
            params.flags |= IORING_SETUP_ATTACH_WQ;
            params.wq_fd = anchorWqFd;
        }

        int ret = io_uring_queue_init_params(DefaultQueueDepth, Ring.get(), &params);
        if (ret < 0) {
            Ring.reset();
            return false;
        }

        ret = io_uring_register_eventfd(Ring.get(), EventFd);
        if (ret < 0) {
            io_uring_queue_exit(Ring.get());
            Ring.reset();
            return false;
        }

        return true;
    }

    int TUringContext::GetRingFd() const {
        return Ring ? Ring->ring_fd : -1;
    }

    void TUringContext::RegisterFiles(int mainFd, int xdcFd) {
        if (!Ring) {
            return;
        }
        // Index 0 = main socket, index 1 = XDC socket (sparse if absent).
        int fds[2] = {mainFd, xdcFd >= 0 ? xdcFd : -1};
        int ret = io_uring_register_files(Ring.get(), fds, 2);
        if (ret < 0) {
            FixedFiles = false;
            return;
        }
        MainFd = mainFd;
        XdcFd = xdcFd;
        FixedFiles = true;
    }

    bool TUringContext::SubmitWritev(int fd, const struct iovec* iov, unsigned iovcnt, ui64 seqNo, EUringOpTag tag) {
        if (!Ring) {
            return false;
        }

        struct io_uring_sqe* sqe = io_uring_get_sqe(Ring.get());
        if (!sqe) {
            ++SqeFull;
            return false;
        }

        const int fixedIdx = FixedIndexForFd(fd);
        if (fixedIdx >= 0) {
            io_uring_prep_writev(sqe, fixedIdx, iov, iovcnt, 0);
            sqe->flags |= IOSQE_FIXED_FILE;
        } else {
            io_uring_prep_writev(sqe, fd, iov, iovcnt, 0);
        }
        io_uring_sqe_set_data64(sqe, static_cast<ui64>(tag) | (seqNo & UringOpDataMask));
        return true;
    }

    bool TUringContext::SubmitReadv(int fd, const struct iovec* iov, unsigned iovcnt, ui64 seqNo, EUringOpTag tag) {
        if (!Ring) {
            return false;
        }

        struct io_uring_sqe* sqe = io_uring_get_sqe(Ring.get());
        if (!sqe) {
            ++SqeFull;
            return false;
        }

        const int fixedIdx = FixedIndexForFd(fd);
        if (fixedIdx >= 0) {
            io_uring_prep_readv(sqe, fixedIdx, iov, iovcnt, 0);
            sqe->flags |= IOSQE_FIXED_FILE;
        } else {
            io_uring_prep_readv(sqe, fd, iov, iovcnt, 0);
        }
        io_uring_sqe_set_data64(sqe, static_cast<ui64>(tag) | (seqNo & UringOpDataMask));
        return true;
    }

    bool TUringContext::SubmitSendZc(int fd, const void* buf, size_t len, ui64 seqNo) {
        if (!Ring) {
            return false;
        }

        struct io_uring_sqe* sqe = io_uring_get_sqe(Ring.get());
        if (!sqe) {
            ++SqeFull;
            return false;
        }

        const int fixedIdx = FixedIndexForFd(fd);
        if (fixedIdx >= 0) {
            io_uring_prep_send_zc(sqe, fixedIdx, buf, len, 0, 0);
            sqe->flags |= IOSQE_FIXED_FILE;
        } else {
            io_uring_prep_send_zc(sqe, fd, buf, len, 0, 0);
        }
        io_uring_sqe_set_data64(sqe, static_cast<ui64>(EUringOpTag::XdcSendZc) | (seqNo & UringOpDataMask));
        return true;
    }

    bool TUringContext::SubmitRecvMultishot(int fd, ui16 bufGroupId, EUringOpTag tag) {
        if (!Ring) {
            return false;
        }

        struct io_uring_sqe* sqe = io_uring_get_sqe(Ring.get());
        if (!sqe) {
            ++SqeFull;
            return false;
        }

        const int fixedIdx = FixedIndexForFd(fd);
        if (fixedIdx >= 0) {
            io_uring_prep_recv_multishot(sqe, fixedIdx, nullptr, 0, 0);
            sqe->flags |= IOSQE_FIXED_FILE;
        } else {
            io_uring_prep_recv_multishot(sqe, fd, nullptr, 0, 0);
        }
        sqe->buf_group = bufGroupId;
        sqe->flags |= IOSQE_BUFFER_SELECT;
        io_uring_sqe_set_data64(sqe, static_cast<ui64>(tag));
        return true;
    }

    bool TUringContext::SubmitCancelFd(int fd) {
        if (!Ring) {
            return false;
        }

        struct io_uring_sqe* sqe = io_uring_get_sqe(Ring.get());
        if (!sqe) {
            return false;
        }

        const int fixedIdx = FixedIndexForFd(fd);
        if (fixedIdx >= 0) {
            io_uring_prep_cancel_fd(sqe, fixedIdx, IORING_ASYNC_CANCEL_ALL | IORING_ASYNC_CANCEL_FD_FIXED);
        } else {
            io_uring_prep_cancel_fd(sqe, fd, IORING_ASYNC_CANCEL_ALL);
        }
        io_uring_sqe_set_data64(sqe, static_cast<ui64>(EUringOpTag::CancelAll));
        return true;
    }

    void TUringContext::Flush() {
        if (!Ring) {
            return;
        }
        // Number of SQEs queued but not yet submitted. A healthy submit returns exactly this
        // many; anything less means the kernel refused part of the batch (e.g. -EBUSY on CQ
        // overflow), which would silently strand a keepalive writev and starve the peer.
        const unsigned expected = io_uring_sq_ready(Ring.get());
        const int ret = io_uring_submit(Ring.get());
        ++SubmitCalls;
        LastSubmitRet = ret;
        if (ret < 0) {
            ++SubmitErrors;
        } else if (static_cast<unsigned>(ret) < expected) {
            ++SubmitPartials;
        }
    }

    bool TUringContext::IsSupported() {
        static const bool supported = [] {
            // Probe a plain ring (without SQPOLL, which is opt-in and not required for the
            // capability check) and require the provided-buffer-ring API, which is the hard
            // dependency of the multishot recv data path. If SQPOLL is enabled but the kernel
            // refuses the SQPOLL ring at Init time, the session falls back to epoll.
            struct io_uring ring;
            struct io_uring_params params = {};
            int ret = io_uring_queue_init_params(8, &ring, &params);
            if (ret != 0) {
                return false;
            }
            int probeErr = 0;
            struct io_uring_buf_ring* br = io_uring_setup_buf_ring(&ring, 1, /*bgid=*/0, 0, &probeErr);
            const bool ok = (br != nullptr);
            if (ok) {
                io_uring_free_buf_ring(&ring, br, 1, /*bgid=*/0);
            }
            io_uring_queue_exit(&ring);
            return ok;
        }();
        return supported;
    }

} // namespace NActors
