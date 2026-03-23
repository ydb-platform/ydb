#pragma once

#include "uring_operation.h"

#include <util/generic/string.h>
#include <util/system/fhandle.h>

#include <sys/uio.h>

#include <atomic>
#include <expected>
#include <memory>

struct io_uring;
struct io_uring_sqe;

namespace NActors {
    class TActorSystem;
} // namespace NActors

namespace NKikimr::NPDisk {

enum class EUringFavor {
    Uring,
    IOPoll,
    SQPoll,
    IOPollSQPoll,
    SharedSQPoll,
    FallbackPDisk,
};

struct TUringRouterConfig {
    // Target maximum number of in-flight I/O operations (SQ ring size).
    // Typical devices have hardware queue depth around 128; using 256 entries
    // gives additional headroom to reduce the risk of SQ exhaustion under load
    // and a better device utilization: there is in-kernel queue in front of the device
    ui32 QueueDepth = 256;

    // Submission kernel thread idle timeout before sleeping (only when UseSQPoll)
    ui32 SqThreadIdleMs = 1000;

    // Kernel thread polls submissions (IORING_SETUP_SQPOLL)
    bool UseSQPoll = false;

    // NVMe/polled devices: no interrupts, user polls completion (IORING_SETUP_IOPOLL).
    // It requires support from both device and driver,
    // according to our measurements the latency win is negligible.
    bool UseIOPoll = false;

    // Share kernel poller and backend between uring instances (IORING_SETUP_ATTACH_WQ)
    // On Linux kernel 5.15 this option showed very poor performance in our benchmarks,
    // so it is disabled by default.
    bool UseSharedSQPoll = false;

    EUringFavor GetUringFavor() const {
        if (UseSQPoll && UseSharedSQPoll) {
            return EUringFavor::SharedSQPoll;
        }
        if (UseSQPoll && UseIOPoll) {
            return EUringFavor::IOPollSQPoll;
        }
        if (UseSQPoll) {
            return EUringFavor::SQPoll;
        }
        if (UseIOPoll) {
            return EUringFavor::IOPoll;
        }
        return EUringFavor::Uring;
    }

    TString ToString() const;
};

// TUringRouter is NOT thread-safe.  All public methods (Register*, Start,
// Read, Write, Flush, Stop, SubmitItemsLeft, etc.) must be called from a
// single thread (e.g. the DDisk actor). The only internal concurrency is the
// dedicated TCompletionPoller thread that consumes the CQ ring and invokes
// OnComplete callbacks.
class TUringRouter {
public:
    TUringRouter(FHANDLE fd, NActors::TActorSystem* actorSystem, TUringRouterConfig config = {});
    ~TUringRouter();

    const TUringRouterConfig& GetConfig() const {
        return Config;
    }

    // --- Setup (call before Start) ---
    //
    // On kernel 5.4, io_uring_register syscall calls percpu_ref_kill() and
    // waits for all refs to drain.  If the completion poller is blocked in
    // io_uring_enter (holding a ref), io_uring_register deadlocks.
    // Therefore all Register* calls must happen before Start().

    // Register the fd as a fixed file.  After this, all I/O uses the registered
    // index, avoiding per-I/O fget()/fput() overhead.
    // Returns an error code in std::unexpected on failure.
    std::expected<void, int> RegisterFile();

    // Register a set of pre-allocated aligned buffers for fixed-buffer I/O.
    // iovs[i].iov_base must be aligned to device sector size (typically 4096).
    // Returns an error code in std::unexpected on failure.
    std::expected<void, int> RegisterBuffers(const struct iovec* iovs, unsigned count);

    // Start the completion poller thread.  Must be called after all Register*
    // calls and before any I/O submissions.
    void Start();

    // --- Submission (call from a single thread, e.g., DDisk actor) ---

    // Submit a read operation. op->Iov and op->DiskOffset must be initialized.
    // op must remain alive until op->OnComplete is called.
    // Returns true if SQE was written to the ring, false if SQ is full.
    bool Read(TUringOperationBase* op);
    bool Write(TUringOperationBase* op);

    // Fixed-buffer variants (requires prior RegisterBuffers).
    // bufIndex is the index into the registered iovec array.
    bool ReadFixed(void* buf, ui32 size, ui64 offset, ui16 bufIndex, TUringOperationBase* op);
    bool WriteFixed(const void* buf, ui32 size, ui64 offset, ui16 bufIndex, TUringOperationBase* op);

    // Flush the SQ ring tail and submit prepared SQEs to the kernel.
    // Calls io_uring_submit() which advances the kernel-visible SQ tail
    // and, when needed, calls io_uring_enter().
    void Flush();

    // --- Lifecycle ---

    // Stop the completion thread and tear down the io_uring instance.
    // Outstanding operations OnComplete() will NOT be called.
    // OnDrop() is called for CQEs drained during shutdown.
    void Stop();

    // Returns the number of SQEs still available in the ring.
    ui32 SubmitItemsLeft() const;

    bool IsFileRegistered() const;
    EUringFavor GetUringFavor() const;

    // both waiting, on-device and completed events
    ui32 GetInflight() const;

    // Returns true if an io_uring instance can be created on this system with either the given config or fallback config.
    // Always use in tests to skip when running in restricted environments (seccomp, containers, etc.).
    static bool Probe(TUringRouterConfig config = {});

private:
    struct io_uring_sqe* GetSqe();
    void PrepareSqe(struct io_uring_sqe* sqe, TUringOperationBase* op);

private:
    FHANDLE Fd;
    NActors::TActorSystem* ActorSystem;
    TUringRouterConfig Config;

    std::unique_ptr<struct io_uring> Ring;

    int FixedFdIndex = -1; // -1 means fd is not registered
    bool BuffersRegistered = false;

    // Dedicated completion polling thread
    class TCompletionPoller;
    std::unique_ptr<TCompletionPoller> Poller;

    std::atomic<ui32> InFlightCount{0};
};

} // namespace NKikimr::NPDisk
