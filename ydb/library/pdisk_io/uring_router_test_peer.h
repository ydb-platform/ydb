#pragma once
#include "uring_router.h"
#include "uring_router_backend.h"
#include "liburing_compat.h"

namespace NKikimr::NPDisk {

// Shared test-only access for scripted issuers and actor integration fixtures.
class TUringRouterTestPeer {
public:
    static void SetHooks(TUringRouter& router, NUringPrivate::TRouterHooks hooks) {
        Y_ABORT_UNLESS(router.State.load() == EUringRouterState::Created);
        router.TestHooks = std::make_unique<const NUringPrivate::TRouterHooks>(std::move(hooks));
    }
    static bool Retired(const TUringRouter& router) {
        return !router.IoThread && !router.Ring && !router.Fd.IsOpen();
    }
    static int WakeFd(const TUringRouter& router) { return router.WakeEventFd; }
    static std::unique_ptr<TUringRouter> Create(
            std::unique_ptr<NUringPrivate::IUringRouterBackend> backend, ui32 depth = 16) {
        return std::unique_ptr<TUringRouter>(new TUringRouter(TFileHandle(), nullptr,
            TUringRouterConfig{.QueueDepth = depth, .IdleSpinUs = 0}, {}, std::move(backend)));
    }

    static void SetFd(TUringRouter& router, TFileHandle fd) { router.Fd = std::move(fd); }

    static void Initialize(TUringRouter& router) {
        if (router.RingInitialized) {
            router.InitializeOnIoThread();
        }
        auto expected = EUringRouterState::Created;
        router.State.compare_exchange_strong(expected, EUringRouterState::Running);
    }

    static bool Drain(TUringRouter& router) { return router.DrainSubmitQueue(); }
    static bool Submit(TUringRouter& router, bool stopping = false) { return router.SubmitPendingSqes(stopping); }
    static ui32 Reap(TUringRouter& router) { return router.ReapCompletions(); }
    static void Park(TUringRouter& router) { router.ParkAndWait(); }
    static void Stop(TUringRouter& router) { router.HandleStop(); }
    static void WaitProgress(TUringRouter& router) { router.WaitForProgress(); }
    static void WaitSync(TUringRouter& router) { router.WaitSync(); }
    static EUringRouterState State(const TUringRouter& router) { return router.State.load(); }
    static unsigned Ready(const TUringRouter& router) { return io_uring_sq_ready(router.Ring.get()); }
    static unsigned Staged(const TUringRouter& router) { return router.Ring->sq.sqe_tail - router.Ring->sq.sqe_head; }

    static void AbandonFakeOperations(TUringRouter& router) {
        // Assertion unwinding must not abort StopSync. This is only safe for the
        // fake below: no kernel can retain an operation or buffer reference.
        router.PendingSubmit = nullptr;
        router.Continuations.clear();
        while (router.Queue.Pop()) {
        }
        router.InFlightCount.store(0);
    }
};

} // namespace NKikimr::NPDisk

