#pragma once

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

struct TUringRouterConfig {
    ui32 QueueDepth = 128;          // max inflight I/O operations (SQ/CQ ring size)
    ui32 SqThreadIdleMs = 5000;     // submission kernel thread idle timeout before sleeping (only when UseSQPoll)
    bool UseSQPoll = true;          // kernel thread polls submissions (IORING_SETUP_SQPOLL)
    bool UseIOPoll = true;          // NVMe/polled devices: no interrupts, user polls completion (IORING_SETUP_IOPOLL)
};

// Our cookie passed through io_uring user_data.
// Callers derive from this and add their own context fields.
// Should be allocated from a pool to avoid dynamic allocation in the hot path.
struct TUringOperation {
    // Filled by TUringRouter on completion
    i32 Result = 0;  // io_uring cqe->res: bytes transferred on success, -errno on failure

    // Called from the dedicated completion polling thread outside actor system,
    // thus MUST NOT use TActivationContext, instead should use actorSystem->Send().
    // After OnComplete returns, the caller is free to return this object to its pool.
    void (*OnComplete)(TUringOperation* op, NActors::TActorSystem* actorSystem) noexcept = nullptr;

    // Optional cleanup callback called by TUringRouter::Stop() for CQEs drained
    // after shutdown without invoking OnComplete. Use this to release operation-
    // owned memory/resources for in-flight requests that are no longer delivered.
    void (*OnDrop)(TUringOperation* op) noexcept = nullptr;

    // Scratch space for the iovec used by readv/writev submissions.
    // Populated by TUringRouter and must remain valid until OnComplete is called.
    struct iovec Iov = {};
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

    // Submit a read. Buffer must be aligned and large enough.
    // op must remain alive until op->OnComplete is called.
    // Returns true if SQE was written to the ring, false if SQ is full.
    bool Read(void* buf, ui64 size, ui64 offset, TUringOperation* op);
    bool Write(const void* buf, ui64 size, ui64 offset, TUringOperation* op);

    // Fixed-buffer variants (requires prior RegisterBuffers).
    // bufIndex is the index into the registered iovec array.
    bool ReadFixed(void* buf, ui32 size, ui64 offset, ui16 bufIndex, TUringOperation* op);
    bool WriteFixed(const void* buf, ui32 size, ui64 offset, ui16 bufIndex, TUringOperation* op);

    // Flush the SQ ring tail and submit prepared SQEs to the kernel.
    // Calls io_uring_submit() which advances the kernel-visible SQ tail
    // and, when needed, calls io_uring_enter().
    void Flush();

    // --- Lifecycle ---

    // Stop the completion thread and tear down the io_uring instance.
    // Outstanding operations OnComplete will NOT be called. If TUringOperation::OnDrop
    // is set, it is called for CQEs drained during shutdown.
    void Stop();

    // Returns the number of SQEs still available in the ring.
    ui32 SubmitItemsLeft() const;

    bool IsFileRegistered() const;

    // Returns true if an io_uring instance can be created on this system with the given config.
    // Always use in tests to skip when running in restricted environments (seccomp, containers, etc.).
    static bool Probe(TUringRouterConfig config = {});

private:
    struct io_uring_sqe* GetSqe();
    void PrepareSqe(struct io_uring_sqe* sqe, bool isRead, void* buf, ui64 size,
                    ui64 offset, TUringOperation* op);

    // Submit a NOP to wake the completion poller blocked in io_uring_wait_cqe.
    void WakePoller();

private:
    FHANDLE Fd;
    NActors::TActorSystem* ActorSystem;
    TUringRouterConfig Config;

    // we intentionally use a naked pointer to simplify the code
    struct io_uring* Ring;

    int FixedFdIndex = -1; // -1 means fd is not registered
    bool BuffersRegistered = false;

    // Dedicated completion polling thread
    class TCompletionPoller;
    std::unique_ptr<TCompletionPoller> Poller;
    std::atomic<bool> IsStopping{false};
};

} // namespace NKikimr::NPDisk
