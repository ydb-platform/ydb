#pragma once

#include "uring_operation.h"
#include "uring_router_client.h"
#include "device_io_sample.h"

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/threading/queue/mpsc_vinfarr_obstructive.h>

#include <util/generic/string.h>
#include <util/system/event.h>
#include <util/system/file.h>
#include <util/system/fhandle.h>

#include <sys/uio.h>

#include <atomic>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>

struct io_uring;
struct io_uring_sqe;

namespace NActors {
    class TActorSystem;
} // namespace NActors

namespace NKikimr::NPDisk {

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

enum class EUringFavor {
    SingleIssuer,   // IORING_SETUP_SINGLE_ISSUER | DEFER_TASKRUN | TASKRUN_FLAG (kernel >= 6.1)
    Plain,          // fallback: plain ring, no modern flags, still one dedicated I/O thread
    FallbackPDisk,  // io_uring unavailable at all; caller routes I/O through PDisk instead
};

enum class EUringRouterState : ui32 {
    Created = 0,
    Running,
    Broken,
    Stopping,
    StoppingBroken,
    Stopped,
    StoppedBroken,
};

struct TUringCounters {
    NMonitoring::TDynamicCounters::TCounterPtr CompletionThreadCPU;
    NMonitoring::TDynamicCounters::TCounterPtr CompletionThreadBusyTimeNs;
};

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

public:
    TUringRouter(
        TFileHandle fd,
        NActors::TActorSystem* actorSystem,
        TUringRouterConfig config = {},
        TUringCounters counters = {});
    TUringRouter(FHANDLE, NActors::TActorSystem*, TUringRouterConfig = {}, TUringCounters = {}) = delete;

    ~TUringRouter() override;

    const TUringRouterConfig& GetConfig() const override {
        return Config;
    }

    // Must be called before Start().
    void SetSampleSink(TDeviceIoSampleSink sink) {
        SampleSink = std::move(sink);
    }

    // --- Setup (call before Start) ---
    //
    // IORING_SETUP_SINGLE_ISSUER requires registration to be performed by the
    // ring's issuer. These methods only record requests; the dedicated I/O
    // thread performs the registrations during Start(). Start() blocks until
    // initialization completes. Inspect the results afterwards with
    // IsFileRegistered()/AreBuffersRegistered() and the corresponding errno
    // accessors.

    void RegisterFile();

    // iovs must remain valid until Start() returns.
    void RegisterBuffers(const struct iovec* iovs, unsigned count);

    // Starts the dedicated I/O thread and blocks until initialization finishes.
    // Required initialization failures leave IsBroken() true and admission closed.
    void Start();

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

public:
    // --- Submission (thread-safe) ---

    // Number of accepted operations that are queued, submitted, or currently
    // executing their completion callback.
    [[nodiscard]] ui64 GetInflight() const;

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

    [[nodiscard]] bool Read(TUringOperationBase* op) override;
    [[nodiscard]] bool Write(TUringOperationBase* op) override;

    // Fixed-buffer variants require successful RegisterBuffers() during Start().
    [[nodiscard]] bool ReadFixed(void* buf, ui32 size, ui64 offset, ui16 bufIndex, TUringOperationBase* op);
    [[nodiscard]] bool WriteFixed(const void* buf, ui32 size, ui64 offset, ui16 bufIndex, TUringOperationBase* op);

    bool IsFileRegistered() const;
    bool AreBuffersRegistered() const;
    int GetRegisterFileErrno() const;
    int GetRegisterBuffersErrno() const;

    EUringFavor GetUringFavor() const;

    // True while the router is Broken, StoppingBroken, or StoppedBroken.
    [[nodiscard]] bool IsBroken() const;

    // Returns true if a disabled io_uring instance can be created and enabled
    // with either the modern flags or the plain fallback configuration.
    [[nodiscard]] static bool Probe(TUringRouterConfig config = {});

private:
    class TIoThread;

    TUringRouter(TFileHandle fd, NActors::TActorSystem* actorSystem,
        TUringRouterConfig config, TUringCounters counters,
        std::unique_ptr<NUringPrivate::IUringRouterBackend> backend);

    struct io_uring_sqe* GetSqe();
    void PrepareSqe(struct io_uring_sqe* sqe, TUringOperationBase* op);

    // Dedicated-I/O-thread methods.
    void InitializeOnIoThread();
    bool DrainSubmitQueue();
    ui32 ReapCompletions();
    bool SubmitPendingSqes(bool allowWhileStopping = false);
    void DropPendingSqes();
    void DropOperation(TUringOperationBase* op);
    void CompleteOperation(TUringOperationBase* op, i64 result);
    void FailRing(const char* call, int result);
    void WaitForProgress();
    void ParkAndWait();
    void HandleStop();

private:
    std::unique_ptr<const NUringPrivate::TRouterHooks> TestHooks;
    TFileHandle Fd;
    NActors::TActorSystem* ActorSystem;
    TUringRouterConfig Config;
    TUringCounters Counters;
    TDeviceIoSampleSink SampleSink;

    std::unique_ptr<NUringPrivate::IUringRouterBackend> Backend;
    std::unique_ptr<struct io_uring> Ring;
    bool RingInitialized = false;
    bool RingEnabled = false;
    bool UsedModernFlags = false;

    // Unlike an ordinary StopAsync(true), a failed ring must never publish or
    // submit its unresolved SQ suffix again. Only the issuer accesses these.
    bool FatalRingError = false;
    bool NeedBackoff = false;

    int FixedFdIndex = -1;
    bool BuffersRegistered = false;
    int RegisterFileErrno = 0;
    int RegisterBuffersErrno = 0;

    bool WantRegisterFile = false;
    bool WantRegisterBuffers = false;
    const struct iovec* PendingIovs = nullptr;
    unsigned PendingIovsCount = 0;

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
};

} // namespace NKikimr::NPDisk
