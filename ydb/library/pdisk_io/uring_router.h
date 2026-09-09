#pragma once

#include "uring_operation.h"

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/string.h>
#include <util/system/fhandle.h>

#include <sys/uio.h>

#include <atomic>
#include <expected>
#include <memory>
#include <mutex>

struct io_uring;
struct io_uring_sqe;

namespace NActors {
    class TActorSystem;
} // namespace NActors

namespace NKikimr::NPDisk {

<<<<<<< HEAD
=======
namespace NUringPrivate {
    class IUringRouterBackend;

    // Installed by the test peer before Start; immutable throughout concurrent use.
    struct TRouterHooks {
        std::function<void()> AfterAdmission;
        std::function<void()> AfterPublication;
        std::function<void()> AfterWake;
        std::function<void()> WaitingForPublishers;
        std::function<void()> BeforeTerminalCallback;
        std::function<void()> BeforeJoin;
        std::function<void()> Retired;
        std::function<void()> BeforeFinalStopCas;
    };
}

>>>>>>> 79b4f9ebdea (Fix DDisk/PB shutdown ordering and PDisk restart ownership (#52618))
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

struct TUringCounters {
    NMonitoring::TDynamicCounters::TCounterPtr CompletionThreadCPU;
    NMonitoring::TDynamicCounters::TCounterPtr CompletionThreadBusyTimeNs;
};

<<<<<<< HEAD
// TUringRouter is NOT thread-safe.  All public methods (Register*, Start,
// Read, Write, Flush, Stop, SubmitItemsLeft, etc.) must be called from a
// single thread (e.g. the DDisk actor). The only internal concurrency is the
// dedicated TCompletionPoller thread that consumes the CQ ring and invokes
// OnComplete callbacks.
class TUringRouter {
=======
// TUringRouter owns one io_uring instance for one device, including the
// duplicated disk fd passed to the constructor. Submit(), Read(), Write(),
// ReadFixed(), and WriteFixed() are safe to call concurrently: callers only
// publish operations to an MPSC queue. One dedicated I/O thread is the ring's
// sole submitter and reaper, as required by IORING_SETUP_SINGLE_ISSUER and
// IORING_SETUP_DEFER_TASKRUN. It batches submissions, reaps completions, and
// invokes operation callbacks.
//
// RegisterFile(), RegisterBuffers(), SetSampleSink(), and Start() are setup
// operations and must be called by one thread before concurrent submission.
// StopAsync() closes admission without waiting. StopSync() is the owner-side
// retirement barrier: it waits for publishers through queue publication and
// wake, joins the issuer, destroys the ring, and closes the duplicated device
// fd even if IUringRouterClient references survive. Concurrent StopSync calls
// are serialized. Keep the router alive throughout every client call.
//
// PDisk owns lifecycle; DDisk and PersistentBuffer hold only IUringRouterClient.
// For requested PDisk restarts, Warden first waits for every DDisk incarnation
// and its PB child to drain. PDisk then calls StopSync before replacement.
// The wake descriptor survives StopSync until object destruction so concurrent
// rejected submissions and StopAsync calls cannot wake a reused descriptor.
//
// Accepted operations receive exactly one terminal callback. Clients retain
// their callback state and buffers until retirement, including on Broken rings.
// StopAsync alone never times out stalled I/O. During explicit teardown a fatal
// ring gets 200 ms to retire late data, published SQ entries and control CQEs;
// unresolved ownership aborts the process before any of that storage is freed.
// Unpublished operations are dropped during teardown.
//
// Optional device I/O sample sink: if set via SetSampleSink() before Start(),
// the I/O thread invokes it once per successfully completed Read/Write CQE.
// The sink must be cheap and thread-safe on its own.
using TDeviceIoSampleSink = std::function<void(const TDeviceIoSample&)>;

class TUringRouter : public IUringRouterClient {
    friend class TUringRouterTestPeer;

>>>>>>> 79b4f9ebdea (Fix DDisk/PB shutdown ordering and PDisk restart ownership (#52618))
public:
    TUringRouter(
        FHANDLE fd,
        NActors::TActorSystem* actorSystem,
        TUringRouterConfig config = {},
        TUringCounters* counters = nullptr);

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

<<<<<<< HEAD
    // --- Submission (call from a single thread, e.g., DDisk actor) ---

    // Submit a vectored read (Read) or write (Write) operation.
    // op->Iov and op->DiskOffset must be initialized before calling:
    //   single buffer:      PrepareIov(buf, size, offset)
    //   scatter-gather:     PrepareScatterGather(count, offset) + AddIov() × count
    // op->OperationType must also be set via SetOperationType().
    // op must remain alive until op->OnComplete is called.
    // Returns true if SQE was written to the ring, false if SQ is full.
    bool Read(TUringOperationBase* op);
    bool Write(TUringOperationBase* op);

    // Fixed-buffer variants (requires prior RegisterBuffers).
    // bufIndex is the index into the registered iovec array.
    bool ReadFixed(void* buf, ui32 size, ui64 offset, ui16 bufIndex, TUringOperationBase* op);
    bool WriteFixed(const void* buf, ui32 size, ui64 offset, ui16 bufIndex, TUringOperationBase* op);
=======
    // Close admission without waiting for accepted operations. An in-progress
    // publisher may still be accepted and gets exactly one terminal callback.
    // Call StopSync to retire resources independently of surviving client refs.
    void StopAsync(bool makeBroken = false);

    // Owner-side synchronous retirement, also used by the destructor. Client
    // references may survive; subsequent submissions are rejected.
    // Close admission and post the stop sentinel.
    // HandleStop drops unsubmitted operations and drains submitted I/O before
    // Join returns. Abort if any operations remain unresolved, then tear down
    // the ring.
    void StopSync();

private:

    // Test-only. Blocks until every accepted operation has received its
    // terminal callback. Unlike StopSync() it does not close admission, so a
    // test can wait for quiescence and keep submitting afterwards. It relies
    // on the I/O thread making progress: unresolved operations keep it blocked
    // if the router was never started or a fatal ring error prevents completion.
    void WaitSync();
>>>>>>> 79b4f9ebdea (Fix DDisk/PB shutdown ordering and PDisk restart ownership (#52618))

    // Flush the SQ ring tail and submit prepared SQEs to the kernel.
    // Calls io_uring_submit() which advances the kernel-visible SQ tail
    // and, when needed, calls io_uring_enter().
    void Flush();

    // --- Lifecycle ---

<<<<<<< HEAD
    // Stop the completion thread and tear down the io_uring instance.
    // Outstanding operations OnComplete() will NOT be called.
    // OnDrop() is called for CQEs drained during shutdown.
    void Stop();
=======
    // Enqueue a prepared operation. Publishing transfers its lifetime to the
    // router, and the I/O thread may invoke OnComplete() even before Submit()
    // returns. A caller transferring a smart pointer must therefore release it
    // before this call and restore it only if false is returned. False means
    // the router has not been started or is stopping/stopped and no callback
    // will be delivered. Every accepted operation gets exactly one terminal
    // callback: OnComplete() after kernel submission, or OnDrop() if shutdown
    // reaches it first.
    //
    // Concurrent callers must keep the router alive for the entire call.
    // Accepted publishers are fenced through publication and wake by StopSync.
    [[nodiscard]] bool Submit(TUringOperationBase* op);
>>>>>>> 79b4f9ebdea (Fix DDisk/PB shutdown ordering and PDisk restart ownership (#52618))

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
<<<<<<< HEAD
    FHANDLE Fd;
=======
    std::unique_ptr<const NUringPrivate::TRouterHooks> TestHooks;
    TFileHandle Fd;
>>>>>>> 79b4f9ebdea (Fix DDisk/PB shutdown ordering and PDisk restart ownership (#52618))
    NActors::TActorSystem* ActorSystem;
    TUringRouterConfig Config;
    TUringCounters* Counters;

    std::unique_ptr<struct io_uring> Ring;

    int FixedFdIndex = -1; // -1 means fd is not registered
    bool BuffersRegistered = false;

    // Dedicated completion polling thread
    class TCompletionPoller;
    std::unique_ptr<TCompletionPoller> Poller;

<<<<<<< HEAD
    std::atomic<ui32> InFlightCount{0};
=======
    // Wakes the I/O thread while it is parked. The I/O thread arms an
    // IORING_OP_POLL_ADD on this eventfd so it remains the only ring issuer.
    int WakeEventFd = -1;
    std::atomic<bool> Parked{false};
    bool WakePollArmed = false;

    // Operation popped from Queue while the SQ was full.
    TUringOperationBase* PendingSubmit = nullptr;
    std::deque<TUringOperationBase*> Continuations;

    bool StopSeen = false;
    bool StopCqePending = false;

    NThreading::TObstructiveConsumerQueue<TUringOperationBase, /*DeleteItems=*/false> Queue;

    // Publishers remain counted through queue publication and wake. StopSync
    // closes admission before waiting for this count and serializes join/exit.
    std::mutex StopMutex;
    std::atomic<ui64> Publishers{0};
    alignas(64) std::atomic<EUringRouterState> State{EUringRouterState::Created};
    alignas(64) std::atomic<ui64> InFlightCount{0};

    TManualEvent ReadyEvent;
    std::unique_ptr<TIoThread> IoThread;
>>>>>>> 79b4f9ebdea (Fix DDisk/PB shutdown ordering and PDisk restart ownership (#52618))
};

} // namespace NKikimr::NPDisk
