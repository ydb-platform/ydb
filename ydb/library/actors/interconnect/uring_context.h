#pragma once

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/interconnect/poller/poller.h>

#include <util/generic/ptr.h>

#include <atomic>
#include <cstdint>

#ifdef __linux__
#include <sys/uio.h>

struct io_uring;
struct io_uring_buf_ring;
#endif

namespace NActors {

    enum class EUringOpTag : ui64 {
        MainWritev   = 1ULL << 56,
        XdcWritev    = 2ULL << 56,
        MainRecv     = 3ULL << 56,
        XdcRecv      = 4ULL << 56,
        XdcSendZc    = 5ULL << 56,
        XdcSendNotif = 6ULL << 56,
        CancelAll    = 7ULL << 56,
    };

    static constexpr ui64 UringOpTagMask = 0xFF00000000000000ULL;
    static constexpr ui64 UringOpDataMask = ~UringOpTagMask;

    class TUringContext : public TThrRefBase {
    public:
        static constexpr ui32 DefaultQueueDepth = 64;
        static constexpr ui32 MaxPendingWrites = 32;
        // SQPOLL kernel-thread idle window (ms) before it sleeps; only used when SQPOLL is on.
        static constexpr ui32 SqThreadIdleMs = 2000;

#ifdef __linux__
        TUringContext(TActorId writeActorId, TActorId readActorId);
        ~TUringContext();

        bool Init(int anchorWqFd, bool enableSqpoll);
        int GetEventFd() const { return EventFd; }
        int GetRingFd() const;

        // Register the session socket fds as fixed files (index 0 = main, 1 = XDC) to avoid
        // per-I/O fget/fput. Submit helpers below transparently use IOSQE_FIXED_FILE afterwards.
        void RegisterFiles(int mainFd, int xdcFd);

        bool SubmitWritev(int fd, const struct iovec* iov, unsigned iovcnt, ui64 seqNo, EUringOpTag tag);
        bool SubmitReadv(int fd, const struct iovec* iov, unsigned iovcnt, ui64 seqNo, EUringOpTag tag);
        bool SubmitSendZc(int fd, const void* buf, size_t len, ui64 seqNo);
        bool SubmitRecvMultishot(int fd, ui16 bufGroupId, EUringOpTag tag);
        bool SubmitCancelFd(int fd);
        void Flush();

        // PendingWrites/PendingRecvs are incremented on the session mailbox thread (submission)
        // and decremented on the reaper thread (completion), so they must be atomic.
        ui32 GetPendingWrites() const { return PendingWrites.load(std::memory_order_relaxed); }
        ui32 GetPendingRecvs() const { return PendingRecvs.load(std::memory_order_relaxed); }

        // Diagnostic submit accounting (instrumentation for the idle-keepalive DeadPeer hunt).
        // All updated on the session mailbox thread inside Flush()/submit helpers. Plain ui64 is
        // fine: read for logging on the same thread, or torn-read-tolerant from another thread.
        ui64 GetSubmitCalls() const { return SubmitCalls; }
        ui64 GetSubmitErrors() const { return SubmitErrors; }
        ui64 GetSubmitPartials() const { return SubmitPartials; }
        i32 GetLastSubmitRet() const { return LastSubmitRet; }
        ui64 GetSqeFull() const { return SqeFull; }
        void IncrementPendingWrites() { PendingWrites.fetch_add(1, std::memory_order_relaxed); }
        void DecrementPendingWrites() {
            ui32 v = PendingWrites.load(std::memory_order_relaxed);
            while (v > 0 && !PendingWrites.compare_exchange_weak(v, v - 1, std::memory_order_relaxed)) {}
        }
        void IncrementPendingRecvs() { PendingRecvs.fetch_add(1, std::memory_order_relaxed); }
        void DecrementPendingRecvs() {
            ui32 v = PendingRecvs.load(std::memory_order_relaxed);
            while (v > 0 && !PendingRecvs.compare_exchange_weak(v, v - 1, std::memory_order_relaxed)) {}
        }

        TActorId GetWriteActorId() const { return WriteActorId; }
        TActorId GetReadActorId() const { return ReadActorId; }

        struct io_uring* GetRing() { return Ring.get(); }

        static bool IsSupported();
#else
        TUringContext(TActorId, TActorId) {}
        ~TUringContext() = default;

        bool Init(int, bool) { return false; }
        int GetEventFd() const { return -1; }
        int GetRingFd() const { return -1; }

        void RegisterFiles(int, int) {}

        bool SubmitWritev(int, const void*, unsigned, ui64, EUringOpTag) { return false; }
        bool SubmitReadv(int, const void*, unsigned, ui64, EUringOpTag) { return false; }
        bool SubmitSendZc(int, const void*, size_t, ui64) { return false; }
        bool SubmitRecvMultishot(int, ui16, EUringOpTag) { return false; }
        bool SubmitCancelFd(int) { return false; }
        void Flush() {}

        ui32 GetPendingWrites() const { return 0; }
        ui32 GetPendingRecvs() const { return 0; }
        ui64 GetSubmitCalls() const { return 0; }
        ui64 GetSubmitErrors() const { return 0; }
        ui64 GetSubmitPartials() const { return 0; }
        i32 GetLastSubmitRet() const { return 0; }
        ui64 GetSqeFull() const { return 0; }
        void IncrementPendingWrites() {}
        void DecrementPendingWrites() {}
        void IncrementPendingRecvs() {}
        void DecrementPendingRecvs() {}

        TActorId GetWriteActorId() const { return {}; }
        TActorId GetReadActorId() const { return {}; }

        void* GetRing() { return nullptr; }

        static bool IsSupported() { return false; }
#endif

        using TPtr = TIntrusivePtr<TUringContext>;

    private:
#ifdef __linux__
        // Translate a raw socket fd to its registered fixed-file index, or -1 if fixed
        // files are not in use for this fd.
        int FixedIndexForFd(int fd) const {
            if (!FixedFiles) {
                return -1;
            }
            if (fd == MainFd) {
                return 0;
            }
            if (fd == XdcFd) {
                return 1;
            }
            return -1;
        }

        std::unique_ptr<struct io_uring> Ring;
        int EventFd = -1;
        TActorId WriteActorId;
        TActorId ReadActorId;
        std::atomic<ui32> PendingWrites{0};
        std::atomic<ui32> PendingRecvs{0};
        // Diagnostic submit accounting (see getters above). Mutated only on the session mailbox
        // thread; read elsewhere only for best-effort logging, so no synchronization is needed.
        ui64 SubmitCalls = 0;     // number of io_uring_submit() calls via Flush()
        ui64 SubmitErrors = 0;    // io_uring_submit() returned < 0
        ui64 SubmitPartials = 0;  // io_uring_submit() returned fewer than the queued SQE count
        i32 LastSubmitRet = 0;    // last io_uring_submit() return value
        ui64 SqeFull = 0;         // io_uring_get_sqe() returned nullptr (SQ full) in a submit helper
        int MainFd = -1;
        int XdcFd = -1;
        bool FixedFiles = false;
#endif
    };

    class TEventFdWrapper : public TSharedDescriptor {
    public:
        TEventFdWrapper(int fd)
            : Fd(fd)
        {}

        int GetDescriptor() override {
            return Fd;
        }

    private:
        int Fd;
    };

} // namespace NActors
