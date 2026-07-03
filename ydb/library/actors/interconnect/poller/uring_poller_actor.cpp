#include "uring_poller_actor.h"
#include "poller_actor.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/protos/services_common.pb.h>
#include <ydb/library/actors/interconnect/uring_context.h>
#include <ydb/library/actors/interconnect/uring_recv_buffer_pool.h>

#include <util/system/thread.h>
#include <util/system/mutex.h>
#include <util/system/guard.h>

// Must be included AFTER YDB headers because linux/uapi headers pulled by
// liburing may define macros that clash with project headers.
#include <ydb/library/uring/liburing_linux.h>

#include <sys/eventfd.h>
#include <poll.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

namespace NActors {

    // Per-session ring state, owned by the reaper thread.
    struct TUringSessionRing {
        TUringContext::TPtr Context;
        TUringRecvBufferPool MainRecvPool;
        int EventFd = -1;
        int MainSocketFd = -1;
        int XdcSocketFd = -1;
    };

    // Dedicated OS thread that owns the anchor ring and reaps completions from all
    // session rings. It removes the dependency on the shared epoll TPollerActor:
    // each session ring signals its eventfd on a CQE; the anchor ring arms a
    // multishot POLL on every such eventfd, and this thread blocks on the anchor.
    class TUringReaper : public ISimpleThread {
        // Sentinel user_data values for anchor-ring control completions. Real session entries
        // use the eventfd value (a small positive int) in the low 32 bits, so these high-bit
        // sentinels can never collide with them.
        //
        // IMPORTANT: liburing reserves user_data == (__u64)-1 (LIBURING_UDATA_TIMEOUT) for the
        // internal timeout completion that io_uring_wait_cqe_timeout posts on kernels without
        // IORING_FEAT_EXT_ARG. ShutdownUserData previously used ~0 too, so every backstop timeout
        // was misread as a shutdown-fd event: the idle sweep never ran (backstopTicks stayed 0)
        // and the reaper hot-spun re-arming the shutdown poll. Keep ~0 reserved for liburing.
        static constexpr ui64 LiburingTimeoutUserData = ~ui64(0); // == LIBURING_UDATA_TIMEOUT
        static constexpr ui64 ShutdownUserData = ~ui64(0) - 1;
        static constexpr ui64 WakeUserData = ~ui64(0) - 2;
        static constexpr ui64 PollRemoveUserData = ~ui64(0) - 3;
        // Our own periodic backstop timer, armed on the anchor ring (see ArmTimeout). We block in
        // io_uring_wait_cqe() and let this timeout (plus session eventfd polls and WakeFd) wake us,
        // instead of io_uring_wait_cqe_timeout() which busy-spun on this kernel (returning without
        // blocking), pinning a CPU core.
        static constexpr ui64 TimerUserData = ~ui64(0) - 4;

    public:
        explicit TUringReaper(TActorSystem* actorSystem, bool enableSqpoll)
            : ActorSystem(actorSystem)
            , EnableSqpoll(enableSqpoll)
        {
            AnchorRing = std::make_unique<struct io_uring>();
            struct io_uring_params params = {};
            // SQPOLL is opt-in (TInterconnectSettings::EnableUringSQPOLL). When off (default),
            // the reaper submits anchor SQEs explicitly and the session threads submit their own
            // I/O explicitly. When on, the anchor ring sets SQPOLL here and every session ring
            // sets SQPOLL + ATTACH_WQ (wq_fd = this anchor ring), so they all share ONE kernel
            // poll thread rather than spawning one per session.
            if (EnableSqpoll) {
                params.flags |= IORING_SETUP_SQPOLL;
                params.sq_thread_idle = SqThreadIdleMs;
            }
            int ret = io_uring_queue_init_params(AnchorQueueDepth, AnchorRing.get(), &params);
            Y_ABORT_UNLESS(ret >= 0, "io_uring_queue_init_params(anchor) failed: %d", ret);
            AnchorRingFd = AnchorRing->ring_fd;

            ShutdownFd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
            Y_ABORT_UNLESS(ShutdownFd >= 0);
            WakeFd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
            Y_ABORT_UNLESS(WakeFd >= 0);

            with_lock (Mutex) {
                ArmPoll(ShutdownFd, ShutdownUserData);
                ArmPoll(WakeFd, WakeUserData);
                ArmTimeout();
                io_uring_submit(AnchorRing.get());
            }

            GraveyardThread = std::thread([this] { GraveyardProc(); });
        }

        ~TUringReaper() {
            StopAndJoin();
            if (AnchorRing) {
                io_uring_queue_exit(AnchorRing.get());
                AnchorRing.reset();
            }
            if (ShutdownFd >= 0) {
                close(ShutdownFd);
            }
            if (WakeFd >= 0) {
                close(WakeFd);
            }
        }

        int GetAnchorRingFd() const {
            return AnchorRingFd;
        }

        bool GetEnableSqpoll() const {
            return EnableSqpoll;
        }

        // Aggregate reaper liveness counters (instrumentation for the idle-keepalive DeadPeer
        // hunt). Read from the TUringPollerActor thread for periodic logging; the reaper thread
        // is the only writer, so relaxed loads are sufficient.
        struct TStats {
            ui64 BackstopTicks;
            ui64 AnchorWakeups;
            ui64 TotalCqesReaped;
            ui64 RecvCqes;
            ui64 WriteCqes;
            ui64 ActiveSessions;
            ui64 EmptyWakes;
            ui64 LastAnchorUserData;
            ui64 PollWakeups;
        };
        TStats GetStats() const {
            return TStats{
                BackstopTicks.load(std::memory_order_relaxed),
                AnchorWakeups.load(std::memory_order_relaxed),
                TotalCqesReaped.load(std::memory_order_relaxed),
                RecvCqes.load(std::memory_order_relaxed),
                WriteCqes.load(std::memory_order_relaxed),
                ActiveSessions.load(std::memory_order_relaxed),
                EmptyWakes.load(std::memory_order_relaxed),
                LastAnchorUserData.load(std::memory_order_relaxed),
                PollWakeups.load(std::memory_order_relaxed),
            };
        }

        // Called from the actor (TUringPollerActor) thread.
        void AddSession(std::unique_ptr<TUringSessionRing> session) {
            int efd = session->EventFd;
            with_lock (Mutex) {
                Sessions.push_back(std::move(session));
                EventFdToSession[efd] = Sessions.back().get();
                ArmPoll(efd, static_cast<ui64>(static_cast<ui32>(efd)));
                io_uring_submit(AnchorRing.get());
                ActiveSessions.store(Sessions.size(), std::memory_order_relaxed);
            }
        }

        // Called from the actor (TUringPollerActor) thread on session teardown. The actual
        // removal/destruction happens on the reaper thread (it owns the rings) to avoid racing
        // with ReapSession; here we just enqueue the request and wake the reaper.
        void RemoveSession(int eventFd) {
            with_lock (RemovalMutex) {
                PendingRemoval.push_back(eventFd);
            }
            const uint64_t one = 1;
            [[maybe_unused]] ssize_t r = write(WakeFd, &one, sizeof(one));
        }

        void StopAndJoin() {
            if (Stop.exchange(true)) {
                return;
            }
            const uint64_t one = 1;
            [[maybe_unused]] ssize_t r = write(ShutdownFd, &one, sizeof(one));
            Join();
            // The reaper thread has exited and will no longer hand work to the graveyard;
            // stop it and let it drain whatever remains.
            {
                std::lock_guard<std::mutex> lk(GraveyardMutex);
                GraveyardStop = true;
            }
            GraveyardCv.notify_one();
            if (GraveyardThread.joinable()) {
                GraveyardThread.join();
            }
        }

    private:
        void* ThreadProc() override {
            TThread::SetCurrentThreadName("IcUringReaper");
            while (!Stop.load(std::memory_order_acquire)) {
                struct io_uring_cqe* cqe = nullptr;
                // Block until the anchor ring wakes us: a session eventfd POLLIN (real work), the
                // WakeFd (teardown), the ShutdownFd, or our own periodic backstop timer (see
                // ArmTimeout). The timer doubles as a safety net: even if an eventfd wakeup were
                // ever lost, the timer fires every BackstopPeriod and we sweep every session ring,
                // so a session can never be wedged with undelivered completions. A blocking wait
                // (vs io_uring_wait_cqe_timeout, which busy-spun on this kernel) keeps the reaper
                // off-CPU while idle.
                int r = io_uring_wait_cqe(AnchorRing.get(), &cqe);
                if (r < 0) {
                    if (r == -EINTR) {
                        continue;
                    }
                    break;
                }
                bool sawTimeout = false;
                AnchorWakeups.fetch_add(1, std::memory_order_relaxed);

                std::vector<int> reArm;
                bool reArmShutdown = false;
                bool reArmWake = false;
                bool reArmTimeout = false;
                unsigned head;
                unsigned count = 0;
                ui64 LastSeenUd = 0;
                io_uring_for_each_cqe(AnchorRing.get(), head, cqe) {
                    ++count;
                    const ui64 ud = io_uring_cqe_get_data64(cqe);
                    LastSeenUd = ud;
                    const bool more = cqe->flags & IORING_CQE_F_MORE;
                    if (ud == LiburingTimeoutUserData) {
                        // liburing's internal timeout completion; not a real event.
                        sawTimeout = true;
                        continue;
                    }
                    if (ud == TimerUserData) {
                        // Our periodic backstop timer fired; sweep below and re-arm it.
                        sawTimeout = true;
                        reArmTimeout = true;
                        continue;
                    }
                    if (ud == ShutdownUserData) {
                        DrainEventFd(ShutdownFd);
                        if (!more) {
                            reArmShutdown = true;
                        }
                        continue;
                    }
                    if (ud == WakeUserData) {
                        DrainEventFd(WakeFd);
                        if (!more) {
                            reArmWake = true;
                        }
                        continue;
                    }
                    if (ud == PollRemoveUserData) {
                        // Completion of a poll_remove we issued while tearing a session down;
                        // nothing to reap, the session ring is already (being) destroyed.
                        continue;
                    }
                    const int efd = static_cast<int>(static_cast<ui32>(ud));
                    PollWakeups.fetch_add(1, std::memory_order_relaxed);
                    DrainEventFd(efd);
                    if (TUringSessionRing* session = Lookup(efd)) {
                        ReapSession(session);
                    }
                    if (!more) {
                        reArm.push_back(efd);
                    }
                }
                if (count == 0) {
                    EmptyWakes.fetch_add(1, std::memory_order_relaxed);
                } else {
                    LastAnchorUserData.store(LastSeenUd, std::memory_order_relaxed);
                }
                io_uring_cq_advance(AnchorRing.get(), count);

                // Drop any sessions whose output actor asked to unregister. Done on this thread
                // (which owns the rings) so io_uring_queue_exit and eventfd close are race-free.
                ProcessPendingRemovals();

                // Backstop: sweep every session ring directly so a lost eventfd wakeup cannot
                // starve keepalives. Driven by wall-clock elapsed time (not solely the -ETIME
                // return) so it fires reliably regardless of how the kernel delivers the timeout,
                // and is throttled to ~BackstopPeriod so it stays cheap even if the wait returns
                // early. This is the safety net that makes idle multi-session nodes robust.
                const auto nowTp = std::chrono::steady_clock::now();
                if (sawTimeout || (nowTp - LastSweep) >= std::chrono::nanoseconds(BackstopPeriodNs)) {
                    BackstopTicks.fetch_add(1, std::memory_order_relaxed);
                    LastSweep = nowTp;
                    SweepAllSessions();
                }

                if (!reArm.empty() || reArmShutdown || reArmWake || reArmTimeout) {
                    with_lock (Mutex) {
                        for (int efd : reArm) {
                            // Skip eventfds whose session was removed in ProcessPendingRemovals.
                            if (EventFdToSession.contains(efd)) {
                                ArmPoll(efd, static_cast<ui64>(static_cast<ui32>(efd)));
                            }
                        }
                        if (reArmShutdown) {
                            ArmPoll(ShutdownFd, ShutdownUserData);
                        }
                        if (reArmWake) {
                            ArmPoll(WakeFd, WakeUserData);
                        }
                        if (reArmTimeout) {
                            ArmTimeout();
                        }
                        io_uring_submit(AnchorRing.get());
                    }
                }
            }
            return nullptr;
        }

        // Reaper-thread side of RemoveSession: cancel the anchor poll for each torn-down
        // session, detach it from the maps and destroy it (exiting its ring and releasing
        // its registered socket files / eventfd / buffer ring).
        void ProcessPendingRemovals() {
            std::vector<int> toRemove;
            with_lock (RemovalMutex) {
                toRemove.swap(PendingRemoval);
            }
            if (toRemove.empty()) {
                return;
            }

            std::vector<std::unique_ptr<TUringSessionRing>> dying;
            with_lock (Mutex) {
                for (int efd : toRemove) {
                    auto it = EventFdToSession.find(efd);
                    if (it == EventFdToSession.end()) {
                        continue; // never added, or already removed
                    }
                    EventFdToSession.erase(it);
                    // Cancel the multishot poll armed on this eventfd before it is closed.
                    PollRemove(static_cast<ui64>(static_cast<ui32>(efd)));
                    for (auto sit = Sessions.begin(); sit != Sessions.end(); ++sit) {
                        if ((*sit)->EventFd == efd) {
                            dying.push_back(std::move(*sit));
                            Sessions.erase(sit);
                            break;
                        }
                    }
                }
                io_uring_submit(AnchorRing.get());
                ActiveSessions.store(Sessions.size(), std::memory_order_relaxed);
            }

            // Hand the dying rings to the graveyard thread instead of destroying them here.
            // ~TUringSessionRing frees the buffer ring (while the session Ring is still valid)
            // and then drops the TUringContext reference, which runs io_uring_queue_exit() —
            // releasing the registered socket files (so the kernel can finally drop them) and
            // closing the session eventfd. io_uring_queue_exit() can block, so keeping it off
            // this thread ensures connection churn never stalls completion processing (and the
            // idle keepalive backstop) for the still-live sessions.
            if (!dying.empty()) {
                {
                    std::lock_guard<std::mutex> lk(GraveyardMutex);
                    for (auto& d : dying) {
                        Graveyard.push_back(std::move(d));
                    }
                }
                GraveyardCv.notify_one();
            }
        }

        // Dedicated thread that runs the (potentially blocking) destruction of removed session
        // rings, off the reaper hot path.
        void GraveyardProc() {
            TThread::SetCurrentThreadName("IcUringGrave");
            for (;;) {
                std::vector<std::unique_ptr<TUringSessionRing>> batch;
                {
                    std::unique_lock<std::mutex> lk(GraveyardMutex);
                    GraveyardCv.wait(lk, [this] { return GraveyardStop || !Graveyard.empty(); });
                    batch.swap(Graveyard);
                    if (batch.empty() && GraveyardStop) {
                        return;
                    }
                }
                batch.clear(); // runs ~TUringSessionRing -> io_uring_queue_exit off the reaper
            }
        }

        // Reap every live session ring. Used as the idle backstop. Session removal happens only
        // on this same reaper thread (ProcessPendingRemovals), so a snapshot taken under the
        // lock stays valid for the duration of the sweep even if AddSession reallocates the
        // Sessions vector concurrently.
        void SweepAllSessions() {
            std::vector<TUringSessionRing*> snapshot;
            with_lock (Mutex) {
                snapshot.reserve(Sessions.size());
                for (auto& s : Sessions) {
                    snapshot.push_back(s.get());
                }
            }
            for (TUringSessionRing* session : snapshot) {
                ReapSession(session);
            }
        }

        static void DrainEventFd(int fd) {
            uint64_t val;
            [[maybe_unused]] ssize_t r = read(fd, &val, sizeof(val));
        }

        TUringSessionRing* Lookup(int efd) {
            with_lock (Mutex) {
                auto it = EventFdToSession.find(efd);
                return it == EventFdToSession.end() ? nullptr : it->second;
            }
        }

        // Must be called with Mutex held. Returns an SQE from the anchor ring, submitting any
        // already-queued SQEs to make room if the SQ is momentarily full. Returns nullptr only
        // if the SQ stays full after repeated submits (effectively unreachable with the current
        // queue depth).
        struct io_uring_sqe* GetAnchorSqe() {
            struct io_uring_sqe* sqe = io_uring_get_sqe(AnchorRing.get());
            for (int attempt = 0; !sqe && attempt < 1024; ++attempt) {
                io_uring_submit(AnchorRing.get());
                sqe = io_uring_get_sqe(AnchorRing.get());
            }
            return sqe;
        }

        // Must be called with Mutex held. Arms a ONE-SHOT poll for POLLIN on `fd`. We deliberately
        // do NOT use multishot here: a multishot poll armed on a ring-registered eventfd was
        // observed to fire exactly once and then go silent forever (the kernel terminated the
        // multishot after the first completion without setting a final !F_MORE CQE we could detect
        // to re-arm), so every session ran solely off the 100ms backstop timer — fine for idle
        // keepalive but it throttled throughput to ~10 writes/s. A one-shot poll that we re-arm
        // after every reap is robust: after we drain the eventfd and reap, the next CQE re-signals
        // the (now readable) eventfd and the freshly armed one-shot poll fires immediately.
        void ArmPoll(int fd, ui64 userData) {
            struct io_uring_sqe* sqe = GetAnchorSqe();
            if (!sqe) {
                // Graceful degradation instead of aborting the whole process. A missed arm here
                // only delays reaping for this fd until the next backstop sweep re-arms it.
                return;
            }
            io_uring_prep_poll_add(sqe, fd, POLLIN);
            io_uring_sqe_set_data64(sqe, userData);
        }

        // Must be called with Mutex held. Arms a one-shot relative timeout on the anchor ring used
        // as the idle backstop. Re-armed each time it fires. BackstopTs is a member because
        // io_uring_prep_timeout keeps a pointer to it until the timeout completes.
        void ArmTimeout() {
            struct io_uring_sqe* sqe = GetAnchorSqe();
            if (!sqe) {
                return;
            }
            BackstopTs.tv_sec = 0;
            BackstopTs.tv_nsec = BackstopPeriodNs;
            io_uring_prep_timeout(sqe, &BackstopTs, 0, 0);
            io_uring_sqe_set_data64(sqe, TimerUserData);
        }

        // Must be called with Mutex held. Cancels a previously armed multishot poll identified
        // by the user_data it was armed with.
        void PollRemove(ui64 targetUserData) {
            struct io_uring_sqe* sqe = GetAnchorSqe();
            if (!sqe) {
                return;
            }
            io_uring_prep_poll_remove(sqe, targetUserData);
            io_uring_sqe_set_data64(sqe, PollRemoveUserData);
        }

        void SendEv(const TActorId& recipient, IEventBase* ev) {
            ActorSystem->Send(new IEventHandle(recipient, TActorId(), ev));
        }

        void ReapSession(TUringSessionRing* session) {
            struct io_uring* ring = session->Context->GetRing();
            if (!ring) {
                return;
            }

            // Recycle any provided-buffer-ring buffers that consumers have released.
            session->MainRecvPool.DrainFreelist();

            struct io_uring_cqe* cqes[BatchSize];
            unsigned count = io_uring_peek_batch_cqe(ring, cqes, BatchSize);

            for (unsigned i = 0; i < count; ++i) {
                struct io_uring_cqe* cqe = cqes[i];
                const ui64 userData = io_uring_cqe_get_data64(cqe);
                const EUringOpTag tag = static_cast<EUringOpTag>(userData & UringOpTagMask);

                switch (tag) {
                    case EUringOpTag::MainWritev:
                    case EUringOpTag::XdcWritev:
                        WriteCqes.fetch_add(1, std::memory_order_relaxed);
                        session->Context->DecrementPendingWrites();
                        SendEv(session->Context->GetWriteActorId(),
                               new TEvUringWriteComplete(cqe->res, userData));
                        break;

                    case EUringOpTag::MainRecv: {
                        RecvCqes.fetch_add(1, std::memory_order_relaxed);
                        if (!(cqe->flags & IORING_CQE_F_MORE)) {
                            session->Context->DecrementPendingRecvs();
                        }
                        TRcBuf data;
                        if (cqe->res > 0 && (cqe->flags & IORING_CQE_F_BUFFER)) {
                            ui16 bufIdx = cqe->flags >> IORING_CQE_BUFFER_SHIFT;
                            // Zero-copy: wrap the provided buffer directly; it is recycled
                            // back into the buffer ring when the last TRcBuf reference drops.
                            data = session->MainRecvPool.WrapBuffer(bufIdx, cqe->res);
                        }
                        SendEv(session->Context->GetReadActorId(),
                               new TEvUringRecvComplete(cqe->res, cqe->flags, userData, std::move(data)));
                        break;
                    }

                    case EUringOpTag::XdcRecv:
                        RecvCqes.fetch_add(1, std::memory_order_relaxed);
                        // XDC recv reads directly into destination spans (no provided buffer);
                        // forward the byte count to the input session for post-processing.
                        SendEv(session->Context->GetReadActorId(),
                               new TEvUringRecvComplete(cqe->res, cqe->flags, userData, TRcBuf()));
                        break;

                    case EUringOpTag::XdcSendZc:
                        if (cqe->flags & IORING_CQE_F_NOTIF) {
                            // Buffer-release notification: gates buffer reuse, but does not
                            // count against pending writes.
                            SendEv(session->Context->GetWriteActorId(),
                                   new TEvUringSendZcNotif(userData));
                        } else {
                            // Data CQE: the send operation itself has completed. The F_NOTIF
                            // CQE that follows only signals buffer release.
                            session->Context->DecrementPendingWrites();
                            SendEv(session->Context->GetWriteActorId(),
                                   new TEvUringWriteComplete(cqe->res, userData));
                        }
                        break;

                    case EUringOpTag::XdcSendNotif:
                        SendEv(session->Context->GetWriteActorId(),
                               new TEvUringSendZcNotif(userData));
                        break;

                    case EUringOpTag::CancelAll:
                        break;
                }
            }

            if (count > 0) {
                TotalCqesReaped.fetch_add(count, std::memory_order_relaxed);
                io_uring_cq_advance(ring, count);
            }
        }

    private:
        static constexpr unsigned AnchorQueueDepth = 4096;
        static constexpr unsigned BatchSize = 64;
        // SQPOLL kernel-thread idle window (ms) before it sleeps; only used when SQPOLL is on.
        static constexpr ui32 SqThreadIdleMs = 2000;
        // Idle backstop period for the reaper wait. Must stay comfortably below the
        // interconnect DeadPeerTimeout so a worst-case lost wakeup is recovered long before a
        // session could be declared dead.
        static constexpr long long BackstopPeriodNs = 100'000'000; // 100 ms

        TActorSystem* const ActorSystem;
        const bool EnableSqpoll;
        std::unique_ptr<struct io_uring> AnchorRing;
        int AnchorRingFd = -1;
        int ShutdownFd = -1;
        int WakeFd = -1; // signalled by RemoveSession to wake the reaper for teardown
        std::atomic<bool> Stop{false};

        TMutex Mutex; // guards anchor SQ submissions + session maps
        THashMap<int, TUringSessionRing*> EventFdToSession;
        std::vector<std::unique_ptr<TUringSessionRing>> Sessions;

        TMutex RemovalMutex; // guards PendingRemoval
        std::vector<int> PendingRemoval;

        // Off-thread destruction of removed session rings (see GraveyardProc).
        std::thread GraveyardThread;
        std::mutex GraveyardMutex;
        std::condition_variable GraveyardCv;
        std::vector<std::unique_ptr<TUringSessionRing>> Graveyard;
        bool GraveyardStop = false;

        // Backing storage for the periodic backstop timeout SQE (see ArmTimeout); io_uring keeps
        // a pointer to it, so it must outlive the in-flight timeout. Touched only with Mutex held.
        struct __kernel_timespec BackstopTs{};

        // Last time SweepAllSessions ran; throttles the time-based backstop. Reaper-thread only.
        std::chrono::steady_clock::time_point LastSweep = std::chrono::steady_clock::now();

        // Liveness counters (see GetStats). Written only by the reaper thread.
        std::atomic<ui64> BackstopTicks{0};   // idle backstop sweeps performed
        std::atomic<ui64> AnchorWakeups{0};   // woke on at least one anchor CQE
        std::atomic<ui64> TotalCqesReaped{0}; // session-ring CQEs consumed across all sessions
        std::atomic<ui64> RecvCqes{0};        // of which main/XDC recv completions
        std::atomic<ui64> WriteCqes{0};       // of which writev/send_zc data completions
        std::atomic<ui64> ActiveSessions{0};  // current live session rings
        std::atomic<ui64> EmptyWakes{0};         // diag: wait returned but CQ was empty
        std::atomic<ui64> LastAnchorUserData{0}; // diag: last non-empty anchor CQE user_data
        std::atomic<ui64> PollWakeups{0};        // diag: session eventfd poll CQEs processed
    };

    class TUringPollerActor : public TActorBootstrapped<TUringPollerActor> {
        std::unique_ptr<TUringReaper> Reaper;
        const bool EnableSqpoll;
        ui16 NextBufGroupId = 0;

    public:
        static constexpr TDuration StatsLogPeriod = TDuration::Seconds(5);

        explicit TUringPollerActor(bool enableSqpoll)
            : EnableSqpoll(enableSqpoll)
        {}

        void Bootstrap() {
            Reaper = std::make_unique<TUringReaper>(TActivationContext::ActorSystem(), EnableSqpoll);
            Reaper->Start();
            Become(&TUringPollerActor::StateFunc);
            Schedule(StatsLogPeriod, new TEvents::TEvWakeup());
        }

        STFUNC(StateFunc) {
            switch (ev->GetTypeRewrite()) {
                case TEvUringRegister::EventType: {
                    auto* x = reinterpret_cast<TEvUringRegister::TPtr*>(&ev);
                    Handle(*x);
                    break;
                }
                case TEvUringUnregister::EventType: {
                    auto* x = reinterpret_cast<TEvUringUnregister::TPtr*>(&ev);
                    Handle(*x);
                    break;
                }
                case TEvents::TSystem::Wakeup:
                    LogStats();
                    Schedule(StatsLogPeriod, new TEvents::TEvWakeup());
                    break;
                case TEvents::TSystem::PoisonPill:
                    PassAway();
                    break;
                default:
                    break;
            }
        }

        // Periodic proof-of-life for the reaper thread plus enough counters to tell, after the
        // fact, whether completions were flowing while sessions were being declared DeadPeer:
        // if RecvCqes/WriteCqes keep climbing the reaper is healthy and the stall is upstream
        // (e.g. the sender never submitted), whereas frozen counters with live sessions point at
        // a reaper-thread stall.
        void LogStats() {
            const TUringReaper::TStats s = Reaper->GetStats();
            LOG_NOTICE(*TlsActivationContext, NActorsServices::INTERCONNECT,
                "ICUR50 uring reaper stats activeSessions# %" PRIu64 " backstopTicks# %" PRIu64
                " anchorWakeups# %" PRIu64 " totalCqes# %" PRIu64 " recvCqes# %" PRIu64
                " writeCqes# %" PRIu64 " emptyWakes# %" PRIu64 " pollWakeups# %" PRIu64
                " lastUd# 0x%" PRIx64,
                s.ActiveSessions, s.BackstopTicks, s.AnchorWakeups, s.TotalCqesReaped,
                s.RecvCqes, s.WriteCqes, s.EmptyWakes, s.PollWakeups, s.LastAnchorUserData);
        }

        void Handle(TEvUringRegister::TPtr& ev) {
            auto* msg = ev->Get();

            auto session = std::make_unique<TUringSessionRing>();
            session->MainSocketFd = msg->Socket ? msg->Socket->GetDescriptor() : -1;
            session->XdcSocketFd = msg->XdcSocket ? msg->XdcSocket->GetDescriptor() : -1;

            auto context = MakeIntrusive<TUringContext>(msg->WriteActorId, msg->ReadActorId);
            if (!context->Init(Reaper->GetAnchorRingFd(), EnableSqpoll)) {
                // Ring creation failed: tell the session to fall back to the epoll TPollerActor
                // so it is never left without an I/O backend.
                Send(msg->WriteActorId, new TEvUringRegisterFailed());
                return;
            }

            // Register socket fds as fixed files to avoid per-I/O fget/fput. This is a pure
            // optimization; on failure the context transparently uses raw fds, so it is not
            // fatal and does not force the epoll fallback.
            context->RegisterFiles(session->MainSocketFd, session->XdcSocketFd);

            // Provided buffer ring for the main socket recv path. If this fails the recv
            // multishot would loop forever on -ENOBUFS, so treat it as fatal and fall back.
            ui16 mainBufGid = NextBufGroupId++;
            ui16 xdcBufGid = NextBufGroupId++;
            if (!session->MainRecvPool.Init(context->GetRing(), mainBufGid)) {
                Send(msg->WriteActorId, new TEvUringRegisterFailed());
                return;
            }

            session->Context = context;
            // CRITICAL: publish the ring's eventfd so the reaper arms its anchor poll on THIS
            // session's real, unique eventfd. Without it EventFd stayed -1: every session armed a
            // multishot poll on the invalid fd -1 (POLLNVAL hot-spin) and all sessions collided on
            // map key -1, so only the most recently registered session was ever reaped. On a
            // multi-session node every other session got no completions delivered and was declared
            // DeadPeer at the keepalive timeout (the cluster-wide meltdown); a 2-node loopback test
            // has a single session and so accidentally worked.
            session->EventFd = context->GetEventFd();

            Reaper->AddSession(std::move(session));

            Send(msg->WriteActorId, new TEvUringRegisterResult(context, mainBufGid, xdcBufGid));
            Send(msg->ReadActorId, new TEvUringRegisterResult(context, mainBufGid, xdcBufGid));
        }

        void Handle(TEvUringUnregister::TPtr& ev) {
            Reaper->RemoveSession(ev->Get()->EventFd);
        }

        void PassAway() override {
            if (Reaper) {
                Reaper->StopAndJoin();
                Reaper.reset();
            }
            TActorBootstrapped::PassAway();
        }
    };

    IActor* CreateUringPollerActor(bool enableSqpoll) {
        return new TUringPollerActor(enableSqpoll);
    }

} // namespace NActors
