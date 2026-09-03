#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/pdisk_io/uring_router.h>
#include <ydb/library/pdisk_io/uring_router_backend.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/tempfile.h>
#include <util/system/file.h>
#include <util/system/event.h>

#include <sys/uio.h>
#include <poll.h>

#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstring>
#include <memory>
#include <deque>
#include <vector>
#include <thread>

// Keep liburing after project/system headers that may define conflicting macros.
#include <ydb/library/pdisk_io/liburing_compat.h>

using NActors::TActorSystem;
using namespace NKikimr::NPDisk;

namespace NKikimr::NPDisk {

// These tests drive the sole issuer synchronously against a userspace ring.
class TUringRouterTestPeer {
public:
    static std::unique_ptr<TUringRouter> Create(
            std::unique_ptr<NUringPrivate::IUringRouterBackend> backend, ui32 depth = 16) {
        return std::unique_ptr<TUringRouter>(new TUringRouter(TFileHandle(), nullptr,
            TUringRouterConfig{.QueueDepth = depth, .IdleSpinUs = 0}, {}, std::move(backend)));
    }

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

namespace {

TUringRouterConfig DefaultConfig(ui32 queueDepth = 16) {
    return TUringRouterConfig{
        .QueueDepth = queueDepth,
        .IdleSpinUs = 100,
    };
}

// Simple RAII page-aligned buffer for tests
struct TAlignedBuf {
    void* Ptr = nullptr;
    size_t Size = 0;

    explicit TAlignedBuf(size_t size)
        : Size(size)
    {
        int ret = posix_memalign(&Ptr, 4096, size);
        Y_ABORT_UNLESS(ret == 0 && Ptr);
    }

    ~TAlignedBuf() {
        free(Ptr);
    }

    void* Data() { return Ptr; }
    const void* Data() const { return Ptr; }

    TAlignedBuf(const TAlignedBuf&) = delete;
    TAlignedBuf& operator=(const TAlignedBuf&) = delete;
};

// Completion op that signals a TManualEvent
struct TTestOp : TUringOperationBase {
    TManualEvent* Event = nullptr;

    void OnComplete(TActorSystem*) noexcept override {
        if (Event) {
            Event->Signal();
        }
    }

    void OnDrop(NActors::TActorSystem*) noexcept override {
        if (Event) {
            Event->Signal();
        }
    }
};

// Completion op that increments an atomic counter and signals when target reached
struct TCountingOp : TUringOperationBase {
    std::atomic<int>* Counter = nullptr;
    int Target = 0;
    TManualEvent* Event = nullptr;

    void CountAndMaybeSignal() noexcept {
        Y_ABORT_UNLESS(Counter);
        int val = Counter->fetch_add(1, std::memory_order_relaxed) + 1;
        if (Event && val >= Target) {
            Event->Signal();
        }
    }

    void OnComplete(TActorSystem*) noexcept override {
        CountAndMaybeSignal();
    }

    void OnDrop(NActors::TActorSystem*) noexcept override {
        CountAndMaybeSignal();
    }
};

// Keeps completion and drop accounting separate so lifecycle tests can assert
// the router's exact terminal-callback contract.
struct TTerminalOp : TUringOperationBase {
    std::atomic<int>* Completions = nullptr;
    std::atomic<int>* Drops = nullptr;
    std::atomic<int> TerminalCallbacks{0};

    void OnComplete(TActorSystem*) noexcept override {
        TerminalCallbacks.fetch_add(1, std::memory_order_relaxed);
        Completions->fetch_add(1, std::memory_order_relaxed);
    }

    void OnDrop(NActors::TActorSystem*) noexcept override {
        TerminalCallbacks.fetch_add(1, std::memory_order_relaxed);
        Drops->fetch_add(1, std::memory_order_relaxed);
    }
};

struct TSamplingOp : TUringOperationBase {
    std::atomic<bool>* SampleSeen = nullptr;
    std::atomic<bool>* CallbackSawSample = nullptr;
    TManualEvent* Event = nullptr;

    void OnComplete(TActorSystem*) noexcept override {
        CallbackSawSample->store(SampleSeen->load(std::memory_order_acquire), std::memory_order_relaxed);
        Event->Signal();
    }

    void OnDrop(NActors::TActorSystem*) noexcept override {
    }
};

// Short reads and the final EOF are handled internally; clients receive one
// terminal error while sampling still describes both physical requests.
struct TShortCompletionOp : TUringOperationBase {
    TManualEvent* Event = nullptr;
    std::atomic<int> Callbacks{0};
    std::atomic<int> Drops{0};
    std::atomic<i64> Result{0};

    void OnComplete(TActorSystem*) noexcept override {
        Callbacks.fetch_add(1, std::memory_order_relaxed);
        Result.store(GetResult(), std::memory_order_relaxed);
        Event->Signal();
    }

    void OnDrop(NActors::TActorSystem*) noexcept override {
        Drops.fetch_add(1, std::memory_order_relaxed);
        Event->Signal();
    }
};

#define SKIP_IF_NO_URING(config) \
    do { \
        if (!TUringRouter::Probe(config)) { \
            Cerr << "io_uring not available on this system, skipping test" << Endl; \
            return; \
        } \
    } while (false)

void PrepareWriteOp(TUringOperationBase& op, void* buf, ui32 size, ui64 offset) {
    op.SetOperationType(TUringOperationBase::EWRITE);
    op.PrepareIov(buf, size, offset);
}

void PrepareReadOp(TUringOperationBase& op, void* buf, ui32 size, ui64 offset) {
    op.SetOperationType(TUringOperationBase::EREAD);
    op.PrepareIov(buf, size, offset);
}

TFileHandle DupOwned(const TFile& f) {
    const FHANDLE dup = ::dup(f.GetHandle());
    Y_ABORT_UNLESS(dup != INVALID_FHANDLE);
    return TFileHandle(dup);
}

void DoCreateAndDestroy(TUringRouterConfig config) {
    SKIP_IF_NO_URING(config);
    TTempFile tmp(MakeTempName(nullptr, "uring_test"));
    TFile f(tmp.Name(), CreateAlways | RdWr);
    f.Resize(1 << 20); // 1 MB
    auto router = std::make_unique<TUringRouter>(DupOwned(f), nullptr, config);
    router->Start();
    router.reset();
}

void DoWriteAndReadBack(TUringRouterConfig config, bool registerFile = true) {
    SKIP_IF_NO_URING(config);
    TTempFile tmp(MakeTempName(nullptr, "uring_test"));
    TFile f(tmp.Name(), CreateAlways | RdWr);
    f.Resize(1 << 20);
    auto router = std::make_unique<TUringRouter>(DupOwned(f), nullptr, config);
    if (registerFile) {
        router->RegisterFile();
    }
    router->Start();
    if (registerFile) {
        UNIT_ASSERT_C(router->IsFileRegistered(),
            TStringBuilder() << "file registration failed with errno=" << router->GetRegisterFileErrno());
    }

    constexpr ui32 size = 4096;

    // Write
    TAlignedBuf writeBuf(size);
    memset(writeBuf.Data(), 0xAB, size);

    TManualEvent writeEv;
    TTestOp writeOp;
    writeOp.Event = &writeEv;

    PrepareWriteOp(writeOp, writeBuf.Data(), size, 0);
    UNIT_ASSERT(router->Write(&writeOp));
    writeEv.WaitI();
    UNIT_ASSERT_VALUES_EQUAL(writeOp.GetResult(), (i32)size);

    // Read back
    TAlignedBuf readBuf(size);
    memset(readBuf.Data(), 0, size);

    TManualEvent readEv;
    TTestOp readOp;
    readOp.Event = &readEv;

    PrepareReadOp(readOp, readBuf.Data(), size, 0);
    UNIT_ASSERT(router->Read(&readOp));
    readEv.WaitI();
    UNIT_ASSERT_VALUES_EQUAL(readOp.GetResult(), (i32)size);
    UNIT_ASSERT(memcmp(writeBuf.Data(), readBuf.Data(), size) == 0);

    router.reset();
}

void DoMultipleConcurrentOps(TUringRouterConfig config) {
    SKIP_IF_NO_URING(config);
    TTempFile tmp(MakeTempName(nullptr, "uring_test"));
    TFile f(tmp.Name(), CreateAlways | RdWr);
    f.Resize(1 << 20);
    auto router = std::make_unique<TUringRouter>(DupOwned(f), nullptr, config);
    router->RegisterFile();
    router->Start();

    constexpr int N = 8;
    constexpr ui32 size = 4096;

    // Write N buffers with unique patterns
    TAlignedBuf writeBufs[N] = {
        TAlignedBuf(size), TAlignedBuf(size), TAlignedBuf(size), TAlignedBuf(size),
        TAlignedBuf(size), TAlignedBuf(size), TAlignedBuf(size), TAlignedBuf(size),
    };

    {
        std::atomic<int> counter{0};
        TManualEvent allDone;
        TCountingOp ops[N];
        for (int i = 0; i < N; ++i) {
            memset(writeBufs[i].Data(), (ui8)(i + 1), size);
            ops[i].Counter = &counter;
            ops[i].Target = N;
            ops[i].Event = &allDone;

            PrepareWriteOp(ops[i], writeBufs[i].Data(), size, i * size);
            UNIT_ASSERT(router->Write(&ops[i]));
        }
        allDone.WaitI();

        for (int i = 0; i < N; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(ops[i].GetResult(), (i32)size);
        }
    }

    // Read back each buffer and verify contents
    {
        TAlignedBuf readBufs[N] = {
            TAlignedBuf(size), TAlignedBuf(size), TAlignedBuf(size), TAlignedBuf(size),
            TAlignedBuf(size), TAlignedBuf(size), TAlignedBuf(size), TAlignedBuf(size),
        };

        std::atomic<int> counter{0};
        TManualEvent allDone;
        TCountingOp ops[N];
        for (int i = 0; i < N; ++i) {
            memset(readBufs[i].Data(), 0, size);
            ops[i].Counter = &counter;
            ops[i].Target = N;
            ops[i].Event = &allDone;

            PrepareReadOp(ops[i], readBufs[i].Data(), size, i * size);
            UNIT_ASSERT(router->Read(&ops[i]));
        }
        allDone.WaitI();

        for (int i = 0; i < N; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(ops[i].GetResult(), (i32)size);
            UNIT_ASSERT(memcmp(writeBufs[i].Data(), readBufs[i].Data(), size) == 0);
        }
    }

    router.reset();
}

void DoOverloadBeyondQueueDepth(TUringRouterConfig config) {
    config.QueueDepth = 4;
    SKIP_IF_NO_URING(config);
    TTempFile tmp(MakeTempName(nullptr, "uring_test"));
    TFile f(tmp.Name(), CreateAlways | RdWr);
    f.Resize(1 << 20);

    auto router = std::make_unique<TUringRouter>(DupOwned(f), nullptr, config);
    router->RegisterFile();
    router->Start();

    constexpr ui32 size = 4096;
    TAlignedBuf buf(size);
    memset(buf.Data(), 0, size);

    // The userspace MPSC queue is deliberately larger than the kernel SQ.
    constexpr int N = 32;
    TTestOp ops[N];
    TManualEvent events[N];

    for (int i = 0; i < N; ++i) {
        ops[i].Event = &events[i];
        PrepareWriteOp(ops[i], buf.Data(), size, 0);
        UNIT_ASSERT(router->Write(&ops[i]));
    }

    for (int i = 0; i < N; ++i) {
        UNIT_ASSERT(events[i].WaitT(TDuration::Seconds(5)));
        UNIT_ASSERT_VALUES_EQUAL(ops[i].GetResult(), (i32)size);
    }

    router.reset();
}

void DoRegisterBuffersAndFixedIO(TUringRouterConfig config) {
    SKIP_IF_NO_URING(config);
    TTempFile tmp(MakeTempName(nullptr, "uring_test"));
    TFile f(tmp.Name(), CreateAlways | RdWr);
    f.Resize(1 << 20);
    auto router = std::make_unique<TUringRouter>(DupOwned(f), nullptr, config);

    constexpr ui32 size = 4096;
    TAlignedBuf writeBuf(size);
    TAlignedBuf readBuf(size);
    memset(writeBuf.Data(), 0xEF, size);
    memset(readBuf.Data(), 0, size);

    // Register file and buffers before Start()
    router->RegisterFile();

    struct iovec iovs[2];
    iovs[0].iov_base = writeBuf.Data();
    iovs[0].iov_len = size;
    iovs[1].iov_base = readBuf.Data();
    iovs[1].iov_len = size;
    router->RegisterBuffers(iovs, 2);

    router->Start();
    UNIT_ASSERT_C(router->IsFileRegistered(),
        TStringBuilder() << "file registration failed with errno=" << router->GetRegisterFileErrno());
    UNIT_ASSERT_C(router->AreBuffersRegistered(),
        TStringBuilder() << "buffer registration failed with errno=" << router->GetRegisterBuffersErrno());

    // WriteFixed using buffer index 0
    TManualEvent writeEv;
    TTestOp writeOp;
    writeOp.Event = &writeEv;

    UNIT_ASSERT(router->WriteFixed(writeBuf.Data(), size, 0, /*bufIndex=*/0, &writeOp));
    writeEv.WaitI();
    UNIT_ASSERT_VALUES_EQUAL(writeOp.GetResult(), (i32)size);

    // ReadFixed using buffer index 1
    TManualEvent readEv;
    TTestOp readOp;
    readOp.Event = &readEv;

    UNIT_ASSERT(router->ReadFixed(readBuf.Data(), size, 0, /*bufIndex=*/1, &readOp));
    readEv.WaitI();
    UNIT_ASSERT_VALUES_EQUAL(readOp.GetResult(), (i32)size);
    UNIT_ASSERT(memcmp(writeBuf.Data(), readBuf.Data(), size) == 0);

    router.reset();
}

void DoSubmitDirect(TUringRouterConfig config) {
    SKIP_IF_NO_URING(config);
    TTempFile tmp(MakeTempName(nullptr, "uring_test"));
    TFile f(tmp.Name(), CreateAlways | RdWr);
    f.Resize(1 << 20);

    auto router = std::make_unique<TUringRouter>(DupOwned(f), nullptr, config);
    router->RegisterFile();
    router->Start();

    constexpr ui32 size = 4096;
    TAlignedBuf buf(size);
    memset(buf.Data(), 0x5A, size);

    TManualEvent event;
    TTestOp op;
    op.Event = &event;
    PrepareWriteOp(op, buf.Data(), size, 0);
    UNIT_ASSERT(router->Submit(&op));
    UNIT_ASSERT(event.WaitT(TDuration::Seconds(5)));
    UNIT_ASSERT_VALUES_EQUAL(op.GetResult(), (i32)size);

    router.reset();
}

void DoLargeMultiPageIO(TUringRouterConfig config) {
    SKIP_IF_NO_URING(config);
    TTempFile tmp(MakeTempName(nullptr, "uring_test"));
    TFile f(tmp.Name(), CreateAlways | RdWr);
    constexpr ui32 size = 256 * 1024; // 256 KB
    f.Resize(size);
    auto router = std::make_unique<TUringRouter>(DupOwned(f), nullptr, config);
    router->RegisterFile();
    router->Start();

    // Write 256K of a pattern
    TAlignedBuf writeBuf(size);
    for (ui32 i = 0; i < size; ++i) {
        static_cast<ui8*>(writeBuf.Data())[i] = (ui8)(i % 251); // prime modulus for pattern
    }

    TManualEvent writeEv;
    TTestOp writeOp;
    writeOp.Event = &writeEv;

    PrepareWriteOp(writeOp, writeBuf.Data(), size, 0);
    UNIT_ASSERT(router->Write(&writeOp));
    writeEv.WaitI();
    UNIT_ASSERT_VALUES_EQUAL(writeOp.GetResult(), (i32)size);

    // Read it back
    TAlignedBuf readBuf(size);
    memset(readBuf.Data(), 0, size);

    TManualEvent readEv;
    TTestOp readOp;
    readOp.Event = &readEv;

    PrepareReadOp(readOp, readBuf.Data(), size, 0);
    UNIT_ASSERT(router->Read(&readOp));
    readEv.WaitI();
    UNIT_ASSERT_VALUES_EQUAL(readOp.GetResult(), (i32)size);
    UNIT_ASSERT(memcmp(writeBuf.Data(), readBuf.Data(), size) == 0);

    router.reset();
}

void DoNonZeroOffsets(TUringRouterConfig config) {
    SKIP_IF_NO_URING(config);
    TTempFile tmp(MakeTempName(nullptr, "uring_test"));
    TFile f(tmp.Name(), CreateAlways | RdWr);
    f.Resize(1 << 20);
    auto router = std::make_unique<TUringRouter>(DupOwned(f), nullptr, config);
    router->RegisterFile();
    router->Start();

    constexpr ui32 size = 4096;

    // Write different patterns at offsets 0, 4K, 64K, 512K
    const ui64 offsets[] = {0, 4096, 65536, 524288};
    constexpr int N = 4;

    TAlignedBuf writeBufs[N] = {
        TAlignedBuf(size), TAlignedBuf(size), TAlignedBuf(size), TAlignedBuf(size),
    };

    for (int i = 0; i < N; ++i) {
        memset(writeBufs[i].Data(), (ui8)(0xA0 + i), size);

        TManualEvent ev;
        TTestOp op;
        op.Event = &ev;

        PrepareWriteOp(op, writeBufs[i].Data(), size, offsets[i]);
        UNIT_ASSERT(router->Write(&op));
        ev.WaitI();
        UNIT_ASSERT_VALUES_EQUAL(op.GetResult(), (i32)size);
    }

    // Read back each offset and verify
    for (int i = 0; i < N; ++i) {
        TAlignedBuf readBuf(size);
        memset(readBuf.Data(), 0, size);

        TManualEvent ev;
        TTestOp op;
        op.Event = &ev;

        PrepareReadOp(op, readBuf.Data(), size, offsets[i]);
        UNIT_ASSERT(router->Read(&op));
        ev.WaitI();
        UNIT_ASSERT_VALUES_EQUAL(op.GetResult(), (i32)size);
        UNIT_ASSERT(memcmp(writeBufs[i].Data(), readBuf.Data(), size) == 0);
    }

    router.reset();
}

void DoStopAsyncIsIdempotent(TUringRouterConfig config) {
    SKIP_IF_NO_URING(config);
    TTempFile tmp(MakeTempName(nullptr, "uring_test"));
    TFile f(tmp.Name(), CreateAlways | RdWr);
    f.Resize(1 << 20);
    auto router = std::make_unique<TUringRouter>(DupOwned(f), nullptr, config);
    router->RegisterFile();
    router->Start();
    UNIT_ASSERT(!router->IsBroken());

    router->StopAsync();
    UNIT_ASSERT(!router->IsBroken());
    router->StopAsync();
    router->StopAsync(true);
    UNIT_ASSERT(router->IsBroken());
    router->StopAsync();
    UNIT_ASSERT(router->IsBroken());
    router.reset();
}

void DoStopAsyncStoppingBrokenAndIsBroken(TUringRouterConfig config) {
    SKIP_IF_NO_URING(config);
    TTempFile tmp(MakeTempName(nullptr, "uring_test"));
    TFile f(tmp.Name(), CreateAlways | RdWr);
    f.Resize(1 << 20);

    auto router = std::make_unique<TUringRouter>(DupOwned(f), nullptr, config);
    router->RegisterFile();
    router->Start();
    UNIT_ASSERT(!router->IsBroken());

    router->StopAsync(true);
    UNIT_ASSERT(router->IsBroken());

    constexpr ui32 size = 4096;
    TAlignedBuf buf(size);
    memset(buf.Data(), 0x31, size);

    TManualEvent afterEvent;
    TTestOp after;
    after.Event = &afterEvent;
    PrepareWriteOp(after, buf.Data(), size, 0);
    UNIT_ASSERT(!router->Write(&after));
    UNIT_ASSERT(!afterEvent.WaitT(TDuration::MilliSeconds(10)));

    router->StopAsync(true);
    UNIT_ASSERT(router->IsBroken());
    router.reset();
}

void DoErrorResultPropagation(TUringRouterConfig config) {
    SKIP_IF_NO_URING(config);
    TTempFile tmp(MakeTempName(nullptr, "uring_test"));
    TFile f(tmp.Name(), CreateAlways | RdWr);
    // Create a small file (4K) so that I/O at a large offset fails
    constexpr ui32 fileSize = 4096;
    f.Resize(fileSize);

    auto router = std::make_unique<TUringRouter>(DupOwned(f), nullptr, config);
    router->RegisterFile();
    router->Start();

    constexpr ui32 ioSize = 4096;
    TAlignedBuf buf(ioSize);
    memset(buf.Data(), 0xCC, ioSize);

    // Write at a huge offset -- the kernel should return an error (e.g. -EFBIG or
    // short write).  We just verify that op.Result is not the requested size,
    // demonstrating that errors propagate through the completion path.
    const ui64 badOffset = static_cast<ui64>(1) << 60;

    TManualEvent ev;
    TTestOp op;
    op.Event = &ev;

    PrepareWriteOp(op, buf.Data(), ioSize, badOffset);
    UNIT_ASSERT(router->Write(&op));
    ev.WaitI();
    // The kernel should have rejected this; Result should be negative errno
    UNIT_ASSERT_LT(op.GetResult(), 0);

    router.reset();
}

void DoDestroyWithPendingOperations(TUringRouterConfig config) {
    SKIP_IF_NO_URING(config);
    TTempFile tmp(MakeTempName(nullptr, "uring_test"));
    TFile f(tmp.Name(), CreateAlways | RdWr);
    f.Resize(1 << 20);

    auto router = std::make_unique<TUringRouter>(DupOwned(f), nullptr, config);
    router->RegisterFile();
    router->Start();

    constexpr ui32 size = 4096;
    TAlignedBuf buf(size);
    memset(buf.Data(), 0xDD, size);

    // Destroy the router without waiting for the accepted operations first.
    constexpr int N = 4;
    TTestOp ops[N];
    TManualEvent events[N];
    for (int i = 0; i < N; ++i) {
        ops[i].Event = &events[i];
        PrepareWriteOp(ops[i], buf.Data(), size, 0);
        UNIT_ASSERT(router->Write(&ops[i]));
    }

    // Destruction must deliver one terminal callback per accepted operation.
    router.reset();
    for (int i = 0; i < N; ++i) {
        UNIT_ASSERT(events[i].WaitT(TDuration::Seconds(1)));
    }
}

void DoDestroyAfterIdle(TUringRouterConfig config) {
    SKIP_IF_NO_URING(config);
    TTempFile tmp(MakeTempName(nullptr, "uring_test"));
    TFile f(tmp.Name(), CreateAlways | RdWr);
    f.Resize(1 << 20);

    auto router = std::make_unique<TUringRouter>(DupOwned(f), nullptr, config);
    router->RegisterFile();
    router->Start();

    UNIT_ASSERT_VALUES_EQUAL(router->GetInflight(), 0u);
    // Allow the I/O thread to leave its idle spin and park.
    usleep(20000);

    const auto start = std::chrono::steady_clock::now();
    router.reset();
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);

    UNIT_ASSERT_C(elapsed < std::chrono::seconds(5),
        TStringBuilder() << "idle router destruction took " << elapsed.count() << " ms");
}

// Completion op that signals "entered" then blocks until "proceed" is signaled or times out
struct TBlockingOp : TUringOperationBase {
    TManualEvent* EnteredEvent = nullptr;
    TManualEvent* ProceedEvent = nullptr;

    void OnComplete(TActorSystem*) noexcept override {
        // Signal to the main thread that we've entered the callback
        if (EnteredEvent) {
            EnteredEvent->Signal();
        }
        // Block inside the callback until the test explicitly releases it. A
        // timeout is only a safety guard against a broken test process.
        if (ProceedEvent) {
            ProceedEvent->WaitT(TDuration::Seconds(5));
        }
    }

    void OnDrop(NActors::TActorSystem*) noexcept override {
    }
};

void DoDestroyWhileCallbackRunning(TUringRouterConfig config) {
    // Block the sole I/O thread in the first callback while additional
    // accepted operations accumulate behind it.
    config.QueueDepth = 1;
    SKIP_IF_NO_URING(config);
    TTempFile tmp(MakeTempName(nullptr, "uring_test"));
    TFile f(tmp.Name(), CreateAlways | RdWr);
    f.Resize(1 << 20);

    auto router = std::make_unique<TUringRouter>(DupOwned(f), nullptr, config);
    router->RegisterFile();
    router->Start();

    constexpr ui32 size = 4096;
    TAlignedBuf buf(size);
    memset(buf.Data(), 0xFF, size);

    TManualEvent enteredEvent;
    TManualEvent proceedEvent;
    TBlockingOp op;
    op.EnteredEvent = &enteredEvent;
    op.ProceedEvent = &proceedEvent;

    PrepareWriteOp(op, buf.Data(), size, 0);
    UNIT_ASSERT(router->Write(&op));

    // Wait until the callback is actively running on the I/O thread.
    enteredEvent.WaitI();

    constexpr int TailOps = 8;
    std::atomic<int> completions{0};
    std::atomic<int> drops{0};
    TTerminalOp tailOps[TailOps];
    for (auto& tailOp : tailOps) {
        tailOp.Completions = &completions;
        tailOp.Drops = &drops;
        PrepareWriteOp(tailOp, buf.Data(), size, 0);
        UNIT_ASSERT(router->Write(&tailOp));
    }
    UNIT_ASSERT_VALUES_EQUAL(router->GetInflight(), TailOps + 1);

    // Destruction must wait for the active callback. The queued tail has not
    // reached the kernel, so shutdown must finish it through OnDrop().
    std::atomic<bool> destructionReturned{false};
    std::thread stopper([&] {
        router.reset();
        destructionReturned.store(true, std::memory_order_release);
    });
    usleep(20000);
    UNIT_ASSERT(!destructionReturned.load(std::memory_order_acquire));
    proceedEvent.Signal();
    stopper.join();
    UNIT_ASSERT(destructionReturned.load(std::memory_order_acquire));
    UNIT_ASSERT_VALUES_EQUAL(completions.load(std::memory_order_relaxed), 0);
    UNIT_ASSERT_VALUES_EQUAL(drops.load(std::memory_order_relaxed), TailOps);
    for (const auto& tailOp : tailOps) {
        UNIT_ASSERT_VALUES_EQUAL(tailOp.TerminalCallbacks.load(std::memory_order_relaxed), 1);
    }
}

void DoDeviceSampleSink(TUringRouterConfig config) {
    SKIP_IF_NO_URING(config);
    TTempFile tmp(MakeTempName(nullptr, "uring_test"));
    TFile f(tmp.Name(), CreateAlways | RdWr);
    f.Resize(1 << 20);

    auto router = std::make_unique<TUringRouter>(DupOwned(f), nullptr, config);
    router->RegisterFile();

    TDeviceIoSample sample;
    std::atomic<bool> sampleSeen{false};
    router->SetSampleSink([&](const TDeviceIoSample& value) {
        sample = value;
        sampleSeen.store(true, std::memory_order_release);
    });
    router->Start();

    constexpr ui32 size = 4096;
    constexpr ui64 offset = 8192;
    TAlignedBuf buf(size);
    memset(buf.Data(), 0x29, size);
    std::atomic<bool> callbackSawSample{false};
    TManualEvent event;
    TSamplingOp op;
    op.SampleSeen = &sampleSeen;
    op.CallbackSawSample = &callbackSawSample;
    op.Event = &event;
    PrepareWriteOp(op, buf.Data(), size, offset);
    UNIT_ASSERT(router->Write(&op));
    UNIT_ASSERT(event.WaitT(TDuration::Seconds(5)));

    UNIT_ASSERT(callbackSawSample.load(std::memory_order_relaxed));
    UNIT_ASSERT(sample.SubmitCycles != 0);
    UNIT_ASSERT_GE(sample.CompleteCycles, sample.SubmitCycles);
    UNIT_ASSERT_VALUES_EQUAL(sample.Offset, offset);
    UNIT_ASSERT_VALUES_EQUAL(sample.Size, size);
    UNIT_ASSERT(sample.IsWrite);
    router.reset();
}

void DoFixedShortRetrySampling(TUringRouterConfig config) {
    SKIP_IF_NO_URING(config);
    TTempFile tmp(MakeTempName(nullptr, "uring_test"));
    TFile f(tmp.Name(), CreateAlways | RdWr);

    constexpr ui32 fileSize = 4096;
    constexpr ui32 bufferSize = 8192;
    f.Resize(fileSize);

    auto router = std::make_unique<TUringRouter>(DupOwned(f), nullptr, config);
    router->RegisterFile();

    TAlignedBuf buf(bufferSize);
    memset(buf.Data(), 0, bufferSize);
    struct iovec registeredBuffer = {buf.Data(), bufferSize};
    router->RegisterBuffers(&registeredBuffer, 1);

    TDeviceIoSample samples[2];
    std::atomic<int> sampleCount{0};
    router->SetSampleSink([&](const TDeviceIoSample& sample) {
        const int index = sampleCount.fetch_add(1, std::memory_order_relaxed);
        if (index < 2) {
            samples[index] = sample;
        }
    });
    router->Start();
    UNIT_ASSERT_C(router->AreBuffersRegistered(),
        TStringBuilder() << "buffer registration failed with errno=" << router->GetRegisterBuffersErrno());

    TManualEvent event;
    TShortCompletionOp op;
    op.Event = &event;
    UNIT_ASSERT(router->ReadFixed(buf.Data(), bufferSize, 0, /*bufIndex=*/0, &op));
    UNIT_ASSERT(event.WaitT(TDuration::Seconds(5)));
    router.reset();

    UNIT_ASSERT_VALUES_EQUAL(op.Callbacks.load(std::memory_order_relaxed), 1);
    UNIT_ASSERT_VALUES_EQUAL(op.Drops.load(std::memory_order_relaxed), 0);
    UNIT_ASSERT_VALUES_EQUAL(op.Result.load(std::memory_order_relaxed), -EIO);
    UNIT_ASSERT_VALUES_EQUAL(op.TakeShortIoCount(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(sampleCount.load(std::memory_order_relaxed), 2);

    UNIT_ASSERT_VALUES_EQUAL(samples[0].Offset, 0u);
    UNIT_ASSERT_VALUES_EQUAL(samples[0].Size, bufferSize);
    UNIT_ASSERT(!samples[0].IsWrite);
    UNIT_ASSERT_VALUES_EQUAL(samples[1].Offset, fileSize);
    UNIT_ASSERT_VALUES_EQUAL(samples[1].Size, bufferSize - fileSize);
    UNIT_ASSERT(!samples[1].IsWrite);
    UNIT_ASSERT_GE(samples[0].CompleteCycles, samples[0].SubmitCycles);
    UNIT_ASSERT_GE(samples[1].CompleteCycles, samples[1].SubmitCycles);

    UNIT_ASSERT(op.IsFixedBuffer());
    UNIT_ASSERT_VALUES_EQUAL(op.GetBufIndex(), 0u);
    UNIT_ASSERT_VALUES_EQUAL(op.GetDiskOffset(), fileSize);
    UNIT_ASSERT_VALUES_EQUAL(op.GetOperationBytes(), bufferSize - fileSize);
    op.ResetSubmissionState();
    UNIT_ASSERT(!op.IsFixedBuffer());
    UNIT_ASSERT_VALUES_EQUAL(op.SubmitCycles, 0u);
}

void DoScatterGatherShortRetrySampling(TUringRouterConfig config) {
    SKIP_IF_NO_URING(config);
    TTempFile tmp(MakeTempName(nullptr, "uring_test"));
    TFile f(tmp.Name(), CreateAlways | RdWr);

    constexpr ui32 fileSize = 4096;
    constexpr ui32 segmentSize = 4096;
    f.Resize(fileSize);

    auto router = std::make_unique<TUringRouter>(DupOwned(f), nullptr, config);
    router->RegisterFile();

    TDeviceIoSample samples[2];
    std::atomic<int> sampleCount{0};
    router->SetSampleSink([&](const TDeviceIoSample& sample) {
        const int index = sampleCount.fetch_add(1, std::memory_order_relaxed);
        if (index < 2) {
            samples[index] = sample;
        }
    });
    router->Start();

    TAlignedBuf first(segmentSize);
    TAlignedBuf second(segmentSize);
    memset(first.Data(), 0, segmentSize);
    memset(second.Data(), 0, segmentSize);

    TManualEvent event;
    TShortCompletionOp op;
    op.Event = &event;
    op.SetOperationType(TUringOperationBase::EREAD);
    op.PrepareScatterGather(2, 0);
    op.AddIov(first.Data(), segmentSize);
    op.AddIov(second.Data(), segmentSize);
    UNIT_ASSERT(router->Read(&op));
    UNIT_ASSERT(event.WaitT(TDuration::Seconds(5)));
    router.reset();

    UNIT_ASSERT_VALUES_EQUAL(op.Callbacks.load(std::memory_order_relaxed), 1);
    UNIT_ASSERT_VALUES_EQUAL(op.Drops.load(std::memory_order_relaxed), 0);
    UNIT_ASSERT_VALUES_EQUAL(op.Result.load(std::memory_order_relaxed), -EIO);
    UNIT_ASSERT_VALUES_EQUAL(op.TakeShortIoCount(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(sampleCount.load(std::memory_order_relaxed), 2);

    UNIT_ASSERT_VALUES_EQUAL(samples[0].Offset, 0u);
    UNIT_ASSERT_VALUES_EQUAL(samples[0].Size, 2 * segmentSize);
    UNIT_ASSERT(!samples[0].IsWrite);
    UNIT_ASSERT(samples[0].SubmitCycles != 0);
    UNIT_ASSERT_GE(samples[0].CompleteCycles, samples[0].SubmitCycles);
    UNIT_ASSERT_VALUES_EQUAL(samples[1].Offset, fileSize);
    UNIT_ASSERT_VALUES_EQUAL(samples[1].Size, segmentSize);
    UNIT_ASSERT(!samples[1].IsWrite);
    UNIT_ASSERT(samples[1].SubmitCycles != 0);
    UNIT_ASSERT_GE(samples[1].CompleteCycles, samples[1].SubmitCycles);

    UNIT_ASSERT_VALUES_EQUAL(op.GetDiskOffset(), fileSize);
    UNIT_ASSERT_VALUES_EQUAL(op.GetOperationBytes(), segmentSize);
    UNIT_ASSERT_EQUAL(op.GetIovBase(), second.Data());
}

void DoSubmissionLifecycle(TUringRouterConfig config) {
    SKIP_IF_NO_URING(config);
    TTempFile tmp(MakeTempName(nullptr, "uring_test"));
    TFile f(tmp.Name(), CreateAlways | RdWr);
    f.Resize(1 << 20);

    auto router = std::make_unique<TUringRouter>(DupOwned(f), nullptr, config);
    router->RegisterFile();

    constexpr ui32 size = 4096;
    TAlignedBuf buf(size);
    memset(buf.Data(), 0x31, size);

    TManualEvent beforeStartEvent;
    TTestOp beforeStart;
    beforeStart.Event = &beforeStartEvent;
    PrepareWriteOp(beforeStart, buf.Data(), size, 0);
    UNIT_ASSERT(!router->Write(&beforeStart));
    UNIT_ASSERT_VALUES_EQUAL(router->GetInflight(), 0u);

    router->Start();
    UNIT_ASSERT(router->Write(&beforeStart));
    UNIT_ASSERT(beforeStartEvent.WaitT(TDuration::Seconds(5)));

    router->StopAsync();

    TManualEvent afterStopAsyncEvent;
    TTestOp afterStopAsync;
    afterStopAsync.Event = &afterStopAsyncEvent;
    PrepareWriteOp(afterStopAsync, buf.Data(), size, 0);
    UNIT_ASSERT(!router->Write(&afterStopAsync));
    UNIT_ASSERT(!afterStopAsyncEvent.WaitT(TDuration::MilliSeconds(10)));
    UNIT_ASSERT_VALUES_EQUAL(router->GetInflight(), 0u);
    router.reset();
}

void DoWakeAfterIdle(TUringRouterConfig config) {
    SKIP_IF_NO_URING(config);
    TTempFile tmp(MakeTempName(nullptr, "uring_test"));
    TFile f(tmp.Name(), CreateAlways | RdWr);
    f.Resize(1 << 20);

    auto router = std::make_unique<TUringRouter>(DupOwned(f), nullptr, config);
    router->RegisterFile();
    router->Start();

    constexpr ui32 size = 4096;
    constexpr int N = 64;
    TAlignedBuf buf(size);
    memset(buf.Data(), 0x42, size);
    TTestOp ops[N];
    TManualEvent events[N];

    for (int i = 0; i < N; ++i) {
        // Let the I/O thread return to its parked wait and repeatedly exercise
        // the eventfd wakeup path.
        usleep(1000);
        ops[i].Event = &events[i];
        PrepareWriteOp(ops[i], buf.Data(), size, 0);
        UNIT_ASSERT(router->Write(&ops[i]));
        UNIT_ASSERT(events[i].WaitT(TDuration::Seconds(5)));
    }

    router.reset();
}

void DoMultiProducerConcurrentSubmit(TUringRouterConfig config) {
    config.QueueDepth = 4;
    SKIP_IF_NO_URING(config);
    TTempFile tmp(MakeTempName(nullptr, "uring_test"));
    TFile f(tmp.Name(), CreateAlways | RdWr);
    f.Resize(1 << 20);

    auto router = std::make_unique<TUringRouter>(DupOwned(f), nullptr, config);
    router->RegisterFile();
    router->Start();

    constexpr int NumThreads = 8;
    constexpr int OpsPerThread = 64;
    constexpr int N = NumThreads * OpsPerThread;
    constexpr ui32 size = 4096;
    TAlignedBuf buf(size);
    memset(buf.Data(), 0x53, size);
    TTestOp ops[N];
    TManualEvent events[N];
    for (int i = 0; i < N; ++i) {
        ops[i].Event = &events[i];
        PrepareWriteOp(ops[i], buf.Data(), size, 0);
    }

    TManualEvent go;
    std::atomic<bool> allAccepted{true};
    std::thread producers[NumThreads];
    for (int threadIdx = 0; threadIdx < NumThreads; ++threadIdx) {
        producers[threadIdx] = std::thread([&, threadIdx] {
            go.WaitI();
            const int begin = threadIdx * OpsPerThread;
            for (int i = begin; i < begin + OpsPerThread; ++i) {
                if (!router->Write(&ops[i])) {
                    allAccepted.store(false, std::memory_order_relaxed);
                }
            }
        });
    }
    go.Signal();
    for (auto& producer : producers) {
        producer.join();
    }
    UNIT_ASSERT(allAccepted.load(std::memory_order_relaxed));

    for (int i = 0; i < N; ++i) {
        UNIT_ASSERT(events[i].WaitT(TDuration::Seconds(10)));
        UNIT_ASSERT_VALUES_EQUAL(ops[i].GetResult(), (i32)size);
    }
    UNIT_ASSERT_VALUES_EQUAL(router->GetInflight(), 0u);
    router.reset();
}

void DoSubmitStopAsyncRace(TUringRouterConfig config) {
    config.QueueDepth = 4;
    SKIP_IF_NO_URING(config);
    TTempFile tmp(MakeTempName(nullptr, "uring_test"));
    TFile f(tmp.Name(), CreateAlways | RdWr);
    f.Resize(1 << 20);

    auto router = std::make_shared<TUringRouter>(DupOwned(f), nullptr, config);
    router->RegisterFile();
    router->Start();

    constexpr int NumThreads = 8;
    constexpr int N = 1024;
    constexpr ui32 size = 4096;
    TAlignedBuf buf(size);
    memset(buf.Data(), 0x64, size);
    std::atomic<int> completions{0};
    std::atomic<int> drops{0};
    std::atomic<int> accepted{0};
    std::atomic<int> attempted{0};
    std::atomic<int> next{1};
    TTerminalOp ops[N];
    for (int i = 0; i < N; ++i) {
        ops[i].Completions = &completions;
        ops[i].Drops = &drops;
        PrepareWriteOp(ops[i], buf.Data(), size, 0);
    }

    // Guarantee that StopAsync has at least one accepted operation to finish.
    UNIT_ASSERT(router->Write(&ops[0]));
    accepted.store(1, std::memory_order_relaxed);

    TManualEvent go;
    std::thread producers[NumThreads];
    for (auto& producer : producers) {
        auto submitter = router;
        producer = std::thread([&, submitter] {
            go.WaitI();
            for (;;) {
                const int i = next.fetch_add(1, std::memory_order_relaxed);
                if (i >= N) {
                    break;
                }
                attempted.fetch_add(1, std::memory_order_relaxed);
                if (submitter->Write(&ops[i])) {
                    accepted.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }

    go.Signal();
    while (attempted.load(std::memory_order_relaxed) < NumThreads) {
        std::this_thread::yield();
    }
    router->StopAsync();
    for (auto& producer : producers) {
        producer.join();
    }
    router.reset();

    int terminalCallbacks = 0;
    for (const auto& op : ops) {
        const int callbacks = op.TerminalCallbacks.load(std::memory_order_relaxed);
        UNIT_ASSERT(callbacks == 0 || callbacks == 1);
        terminalCallbacks += callbacks;
    }
    UNIT_ASSERT_VALUES_EQUAL(terminalCallbacks, accepted.load(std::memory_order_relaxed));
    UNIT_ASSERT_VALUES_EQUAL(
        completions.load(std::memory_order_relaxed) + drops.load(std::memory_order_relaxed),
        accepted.load(std::memory_order_relaxed));
}

void DoConcurrentStopAsync(TUringRouterConfig config) {
    SKIP_IF_NO_URING(config);
    TTempFile tmp(MakeTempName(nullptr, "uring_test"));
    TFile f(tmp.Name(), CreateAlways | RdWr);
    f.Resize(1 << 20);

    auto router = std::make_unique<TUringRouter>(DupOwned(f), nullptr, config);
    router->RegisterFile();
    router->Start();

    constexpr int N = 64;
    constexpr ui32 size = 4096;
    TAlignedBuf buf(size);
    memset(buf.Data(), 0x75, size);
    std::atomic<int> completions{0};
    std::atomic<int> drops{0};
    TTerminalOp ops[N];
    for (auto& op : ops) {
        op.Completions = &completions;
        op.Drops = &drops;
        PrepareWriteOp(op, buf.Data(), size, 0);
        UNIT_ASSERT(router->Write(&op));
    }

    TManualEvent go;
    std::thread stopper1([&] { go.WaitI(); router->StopAsync(); });
    std::thread stopper2([&] { go.WaitI(); router->StopAsync(); });
    go.Signal();
    stopper1.join();
    stopper2.join();

    router.reset();

    UNIT_ASSERT_VALUES_EQUAL(
        completions.load(std::memory_order_relaxed) + drops.load(std::memory_order_relaxed), N);
    for (const auto& op : ops) {
        UNIT_ASSERT_VALUES_EQUAL(op.TerminalCallbacks.load(std::memory_order_relaxed), 1);
    }
}

// Prepare a vectored write op from a pre-built iovec array.
void PrepareWriteVectored(TUringOperationBase& op, const struct iovec* iovs, int count, ui64 offset) {
    op.SetOperationType(TUringOperationBase::EWRITE);
    op.PrepareScatterGather(count, offset);
    for (int i = 0; i < count; ++i) {
        op.AddIov(iovs[i].iov_base, iovs[i].iov_len);
    }
}

// -------------------------------------------------------------------------
// Scatter-gather round-trip helpers
// -------------------------------------------------------------------------

// Write N 4K segments via one scatter-gather writev, read back into a single
// flat buffer, verify each segment.
void DoScatterGatherWriteReadBack(TUringRouterConfig config) {
    SKIP_IF_NO_URING(config);
    TTempFile tmp(MakeTempName(nullptr, "uring_test"));
    TFile f(tmp.Name(), CreateAlways | RdWr);
    constexpr int N = 3;
    constexpr ui32 segSize = 4096;
    constexpr ui32 totalSize = N * segSize;
    f.Resize(totalSize);

    auto router = std::make_unique<TUringRouter>(DupOwned(f), nullptr, config);
    router->RegisterFile();
    router->Start();

    // Three distinct page-aligned write buffers
    TAlignedBuf wBufs[N] = {TAlignedBuf(segSize), TAlignedBuf(segSize), TAlignedBuf(segSize)};
    for (int i = 0; i < N; ++i) {
        memset(wBufs[i].Data(), (ui8)(0x11 * (i + 1)), segSize);
    }

    struct iovec iovs[N];
    for (int i = 0; i < N; ++i) {
        iovs[i].iov_base = wBufs[i].Data();
        iovs[i].iov_len  = segSize;
    }

    TManualEvent writeEv;
    TTestOp writeOp;
    writeOp.Event = &writeEv;
    PrepareWriteVectored(writeOp, iovs, N, /*offset=*/0);
    UNIT_ASSERT(router->Write(&writeOp));
    writeEv.WaitI();
    UNIT_ASSERT_VALUES_EQUAL(writeOp.GetResult(), (i32)totalSize);

    // Read back into one flat buffer and verify per-segment patterns.
    TAlignedBuf readBuf(totalSize);
    memset(readBuf.Data(), 0, totalSize);

    TManualEvent readEv;
    TTestOp readOp;
    readOp.Event = &readEv;
    PrepareReadOp(readOp, readBuf.Data(), totalSize, 0);
    UNIT_ASSERT(router->Read(&readOp));
    readEv.WaitI();
    UNIT_ASSERT_VALUES_EQUAL(readOp.GetResult(), (i32)totalSize);

    for (int i = 0; i < N; ++i) {
        UNIT_ASSERT(memcmp(wBufs[i].Data(),
                           static_cast<ui8*>(readBuf.Data()) + i * segSize,
                           segSize) == 0);
    }

    router.reset();
}

void DoScatterGatherSingleIovec(TUringRouterConfig config) {
    SKIP_IF_NO_URING(config);
    TTempFile tmp(MakeTempName(nullptr, "uring_test"));
    TFile f(tmp.Name(), CreateAlways | RdWr);
    constexpr ui32 size = 4096;
    f.Resize(size);

    auto router = std::make_unique<TUringRouter>(DupOwned(f), nullptr, config);
    router->RegisterFile();
    router->Start();

    TAlignedBuf writeBuf(size);
    memset(writeBuf.Data(), 0xBB, size);

    struct iovec iov;
    iov.iov_base = writeBuf.Data();
    iov.iov_len  = size;

    TManualEvent writeEv;
    TTestOp writeOp;
    writeOp.Event = &writeEv;
    PrepareWriteVectored(writeOp, &iov, 1, 0);
    UNIT_ASSERT(router->Write(&writeOp));
    writeEv.WaitI();
    UNIT_ASSERT_VALUES_EQUAL(writeOp.GetResult(), (i32)size);

    TAlignedBuf readBuf(size);
    memset(readBuf.Data(), 0, size);

    TManualEvent readEv;
    TTestOp readOp;
    readOp.Event = &readEv;
    PrepareReadOp(readOp, readBuf.Data(), size, 0);
    UNIT_ASSERT(router->Read(&readOp));
    readEv.WaitI();
    UNIT_ASSERT_VALUES_EQUAL(readOp.GetResult(), (i32)size);
    UNIT_ASSERT(memcmp(writeBuf.Data(), readBuf.Data(), size) == 0);

    router.reset();
}

void DoScatterGatherErrorPropagation(TUringRouterConfig config) {
    SKIP_IF_NO_URING(config);
    TTempFile tmp(MakeTempName(nullptr, "uring_test"));
    TFile f(tmp.Name(), CreateAlways | RdWr);
    f.Resize(4096);

    auto router = std::make_unique<TUringRouter>(DupOwned(f), nullptr, config);
    router->RegisterFile();
    router->Start();

    TAlignedBuf buf1(4096), buf2(4096);
    memset(buf1.Data(), 0xCC, 4096);
    memset(buf2.Data(), 0xCC, 4096);

    struct iovec iovs[2];
    iovs[0].iov_base = buf1.Data(); iovs[0].iov_len = 4096;
    iovs[1].iov_base = buf2.Data(); iovs[1].iov_len = 4096;

    const ui64 badOffset = static_cast<ui64>(1) << 60;

    TManualEvent ev;
    TTestOp op;
    op.Event = &ev;
    PrepareWriteVectored(op, iovs, 2, badOffset);
    UNIT_ASSERT(router->Write(&op));
    ev.WaitI();
    UNIT_ASSERT_LT(op.GetResult(), 0);

    router.reset();
}

} // anonymous namespace

// =========================================================================
// Pure logic tests for TUringOperationBase (no kernel ring required)
// =========================================================================

Y_UNIT_TEST_SUITE(TUringOperationBaseTest) {

    Y_UNIT_TEST(PrepareIovSingleBuffer) {
        TTestOp op;
        char buf[4096];
        op.SetOperationType(TUringOperationBase::EWRITE);
        op.PrepareIov(buf, 4096, 1024);

        UNIT_ASSERT_VALUES_EQUAL(op.GetTotalSize(), 4096u);
        UNIT_ASSERT_VALUES_EQUAL(op.GetOperationBytes(), 4096u);
        UNIT_ASSERT_VALUES_EQUAL(op.GetDiskOffset(), 1024u);
        UNIT_ASSERT_EQUAL(op.GetIovBase(), static_cast<void*>(buf));
    }

#if defined(__linux__)
    Y_UNIT_TEST(PrepareIovVectored) {
        TTestOp op;
        char buf1[4096], buf2[4096], buf3[4096];
        struct iovec iovs[3];
        iovs[0] = {buf1, 4096};
        iovs[1] = {buf2, 4096};
        iovs[2] = {buf3, 4096};

        PrepareWriteVectored(op, iovs, 3, 8192);

        UNIT_ASSERT_VALUES_EQUAL(op.GetTotalSize(), 3 * 4096u);
        UNIT_ASSERT_VALUES_EQUAL(op.GetOperationBytes(), 3 * 4096u);
        UNIT_ASSERT_VALUES_EQUAL(op.GetDiskOffset(), 8192u);
        UNIT_ASSERT_EQUAL(op.GetIovBase(), static_cast<void*>(buf1));
    }

    Y_UNIT_TEST(AdvanceIovFullSegments) {
        TTestOp op;
        char buf1[4096], buf2[4096], buf3[4096];
        struct iovec iovs[3];
        iovs[0] = {buf1, 4096};
        iovs[1] = {buf2, 4096};
        iovs[2] = {buf3, 4096};

        PrepareWriteVectored(op, iovs, 3, 0);

        // Advance past the first full segment.
        op.AdvanceIov(4096);
        UNIT_ASSERT_VALUES_EQUAL(op.GetOperationBytes(), 2 * 4096u);
        UNIT_ASSERT_VALUES_EQUAL(op.GetDiskOffset(), 4096u);
        UNIT_ASSERT_EQUAL(op.GetIovBase(), static_cast<void*>(buf2));

        // Advance past the second full segment.
        op.AdvanceIov(4096);
        UNIT_ASSERT_VALUES_EQUAL(op.GetOperationBytes(), 1 * 4096u);
        UNIT_ASSERT_VALUES_EQUAL(op.GetDiskOffset(), 8192u);
        UNIT_ASSERT_EQUAL(op.GetIovBase(), static_cast<void*>(buf3));

        // TotalSize is unchanged throughout.
        UNIT_ASSERT_VALUES_EQUAL(op.GetTotalSize(), 3 * 4096u);
    }

    Y_UNIT_TEST(AdvanceIovPartialSegment) {
        TTestOp op;
        char buf1[4096], buf2[4096];
        struct iovec iovs[2];
        iovs[0] = {buf1, 4096};
        iovs[1] = {buf2, 4096};

        PrepareWriteVectored(op, iovs, 2, 0);

        // Partial advance within the first iovec.
        op.AdvanceIov(1024);
        UNIT_ASSERT_VALUES_EQUAL(op.GetOperationBytes(), 4096u + 3072u);
        UNIT_ASSERT_VALUES_EQUAL(op.GetDiskOffset(), 1024u);
        // iov_base of the first remaining iovec should be advanced.
        UNIT_ASSERT_EQUAL(op.GetIovBase(), static_cast<void*>(buf1 + 1024));
    }

    Y_UNIT_TEST(AdvanceIovCrossSegmentBoundary) {
        TTestOp op;
        char buf1[4096], buf2[8192];
        struct iovec iovs[2];
        iovs[0] = {buf1, 4096};
        iovs[1] = {buf2, 8192};

        PrepareWriteVectored(op, iovs, 2, 0);

        // Advance exactly one full segment + 2048 into the next.
        op.AdvanceIov(4096 + 2048);
        UNIT_ASSERT_VALUES_EQUAL(op.GetOperationBytes(), 8192u - 2048u);
        UNIT_ASSERT_VALUES_EQUAL(op.GetDiskOffset(), 4096u + 2048u);
        UNIT_ASSERT_EQUAL(op.GetIovBase(), static_cast<void*>(buf2 + 2048));
        UNIT_ASSERT_VALUES_EQUAL(op.GetTotalSize(), 4096u + 8192u);
    }

    Y_UNIT_TEST(ResetSubmissionStateClearsIov) {
        TTestOp op;
        char buf1[4096], buf2[4096];
        struct iovec iovs[2];
        iovs[0] = {buf1, 4096};
        iovs[1] = {buf2, 4096};

        PrepareWriteVectored(op, iovs, 2, 512);
        op.AdvanceIov(4096);

        op.ResetSubmissionState();
        UNIT_ASSERT_VALUES_EQUAL(op.GetTotalSize(), 0u);
        UNIT_ASSERT_VALUES_EQUAL(op.GetOperationBytes(), 0u);
        UNIT_ASSERT_VALUES_EQUAL(op.GetDiskOffset(), 0u);
        UNIT_ASSERT_EQUAL(op.GetIovBase(), nullptr);
        UNIT_ASSERT(!op.IsFixedBuffer());
        UNIT_ASSERT_VALUES_EQUAL(op.GetBufIndex(), 0u);
    }
#endif // __linux__

}

Y_UNIT_TEST_SUITE(TUringRouterTest) {

    Y_UNIT_TEST(DefaultIdleSpinIs10Microseconds) {
        UNIT_ASSERT_VALUES_EQUAL(10u, TUringRouterConfig{}.IdleSpinUs);
    }

    Y_UNIT_TEST(CreateAndDestroy) {
        DoCreateAndDestroy(DefaultConfig());
    }

    Y_UNIT_TEST(WriteAndReadBack) {
        DoWriteAndReadBack(DefaultConfig());
    }

    Y_UNIT_TEST(WriteAndReadBackNoFixedFile) {
        DoWriteAndReadBack(DefaultConfig(), /*registerFile=*/false);
    }

    Y_UNIT_TEST(MultipleConcurrentOps) {
        DoMultipleConcurrentOps(DefaultConfig());
    }

    Y_UNIT_TEST(OverloadBeyondQueueDepth) {
        DoOverloadBeyondQueueDepth(DefaultConfig());
    }

    Y_UNIT_TEST(RegisterBuffersAndFixedIO) {
        DoRegisterBuffersAndFixedIO(DefaultConfig());
    }

    Y_UNIT_TEST(SubmitDirect) {
        DoSubmitDirect(DefaultConfig());
    }

    Y_UNIT_TEST(LargeMultiPageIO) {
        DoLargeMultiPageIO(DefaultConfig());
    }

    Y_UNIT_TEST(NonZeroOffsets) {
        DoNonZeroOffsets(DefaultConfig());
    }

    Y_UNIT_TEST(StopAsyncIsIdempotent) {
        DoStopAsyncIsIdempotent(DefaultConfig());
    }

    Y_UNIT_TEST(StopAsyncStoppingBrokenAndIsBroken) {
        DoStopAsyncStoppingBrokenAndIsBroken(DefaultConfig());
    }

    Y_UNIT_TEST(ErrorResultPropagation) {
        DoErrorResultPropagation(DefaultConfig());
    }

    Y_UNIT_TEST(DestroyWithPendingOperations) {
        DoDestroyWithPendingOperations(DefaultConfig());
    }

    Y_UNIT_TEST(DestroyAfterIdle) {
        DoDestroyAfterIdle(DefaultConfig());
    }

    Y_UNIT_TEST(DestroyWhileCallbackRunning) {
        DoDestroyWhileCallbackRunning(DefaultConfig());
    }

    Y_UNIT_TEST(DeviceSampleSink) {
        DoDeviceSampleSink(DefaultConfig());
    }

    Y_UNIT_TEST(FixedShortRetrySampling) {
        DoFixedShortRetrySampling(DefaultConfig());
    }

    Y_UNIT_TEST(ScatterGatherShortRetrySampling) {
        DoScatterGatherShortRetrySampling(DefaultConfig());
    }

    Y_UNIT_TEST(SubmissionLifecycle) {
        DoSubmissionLifecycle(DefaultConfig());
    }

    Y_UNIT_TEST(WakeAfterIdle) {
        DoWakeAfterIdle(DefaultConfig());
    }

    Y_UNIT_TEST(MultiProducerConcurrentSubmit) {
        DoMultiProducerConcurrentSubmit(DefaultConfig());
    }

    Y_UNIT_TEST(SubmitStopAsyncRace) {
        DoSubmitStopAsyncRace(DefaultConfig());
    }

    Y_UNIT_TEST(ConcurrentStopAsync) {
        DoConcurrentStopAsync(DefaultConfig());
    }

    Y_UNIT_TEST(ScatterGatherWriteReadBack) {
        DoScatterGatherWriteReadBack(DefaultConfig());
    }

    Y_UNIT_TEST(ScatterGatherSingleIovec) {
        DoScatterGatherSingleIovec(DefaultConfig());
    }

    Y_UNIT_TEST(ScatterGatherErrorPropagation) {
        DoScatterGatherErrorPropagation(DefaultConfig());
    }
}

namespace {

struct TScriptedUringBackend : NUringPrivate::IUringRouterBackend {
    struct TSubmission {
        io_uring_sqe Sqe{};
        std::vector<iovec> Iovs;
    };

    struct TSubmitStep {
        int Result;
        unsigned Consumed = 0;
    };

    struct TStats {
        std::vector<unsigned> InitFlags;
        unsigned EnableCalls = 0;
        unsigned FileRegistrationCalls = 0;
        unsigned BufferRegistrationCalls = 0;
        unsigned ExitCalls = 0;
        unsigned WaitCalls = 0;
        std::vector<ui32> Backoffs;
        std::vector<std::vector<TSubmission>> Attempts;
        std::vector<TSubmission> Consumed;
    };

    std::shared_ptr<TStats> Stats = std::make_shared<TStats>();
    std::deque<int> InitResults;
    std::deque<int> EnableResults;
    std::deque<int> FileResults;
    std::deque<int> BufferResults;
    std::deque<TSubmitStep> SubmitResults;
    std::deque<int> PeekResults;
    std::deque<int> WaitResults;
    std::deque<int> ControlResults;
    unsigned Features = IORING_FEAT_EXT_ARG;
    bool AutoComplete = false;
    std::function<void()> OnBackoff;

    io_uring* Ring = nullptr;
    unsigned SqHead = 0;
    unsigned SqTail = 0;
    unsigned SqFlags = 0;
    unsigned SqDropped = 0;
    unsigned CqHead = 0;
    unsigned CqTail = 0;
    unsigned CqFlags = 0;
    unsigned CqOverflow = 0;
    std::vector<unsigned> SqArray;
    std::vector<io_uring_sqe> Sqes;
    std::vector<io_uring_cqe> Cqes;
    std::vector<ui64> Outstanding;

    static int Next(std::deque<int>& results, int fallback = 0) {
        if (results.empty()) {
            return fallback;
        }
        const int result = results.front();
        results.pop_front();
        return result;
    }

    int Init(unsigned entries, io_uring* ring, io_uring_params* params) override {
        Stats->InitFlags.push_back(params->flags);
        const int result = Next(InitResults);
        if (result) {
            return result;
        }
        Y_ABORT_UNLESS(entries && !(entries & (entries - 1)));
        Ring = ring;
        *ring = {};
        SqArray.resize(entries);
        Sqes.resize(entries);
        Cqes.resize(4 * entries);
        for (unsigned index = 0; index < entries; ++index) {
            SqArray[index] = index;
        }
        ring->sq.khead = &SqHead;
        ring->sq.ktail = &SqTail;
        ring->sq.kflags = &SqFlags;
        ring->sq.kdropped = &SqDropped;
        ring->sq.sqes = Sqes.data();
        ring->sq.array = SqArray.data();
        ring->sq.ring_entries = entries;
        ring->sq.ring_mask = entries - 1;
        ring->cq.khead = &CqHead;
        ring->cq.ktail = &CqTail;
        ring->cq.kflags = &CqFlags;
        ring->cq.koverflow = &CqOverflow;
        ring->cq.cqes = Cqes.data();
        ring->cq.ring_entries = Cqes.size();
        ring->cq.ring_mask = Cqes.size() - 1;
        ring->features = params->features = Features;
        ring->flags = params->flags;
        return 0;
    }

    int Enable(io_uring*) override {
        ++Stats->EnableCalls;
        return Next(EnableResults);
    }

    int RegisterFiles(io_uring*, const int*, unsigned) override {
        ++Stats->FileRegistrationCalls;
        return Next(FileResults);
    }

    int RegisterBuffers(io_uring*, const iovec*, unsigned) override {
        ++Stats->BufferRegistrationCalls;
        return Next(BufferResults);
    }

    TSubmission Snapshot(unsigned index) const {
        TSubmission submission;
        submission.Sqe = Sqes[index & Ring->sq.ring_mask];
        if (submission.Sqe.opcode == IORING_OP_READV || submission.Sqe.opcode == IORING_OP_WRITEV) {
            const auto* iovs = reinterpret_cast<const iovec*>(submission.Sqe.addr);
            submission.Iovs.assign(iovs, iovs + submission.Sqe.len);
        }
        return submission;
    }

    void PushCqe(ui64 userData, int result) {
        Y_ABORT_UNLESS(CqTail - CqHead < Cqes.size());
        auto& cqe = Cqes[CqTail++ & Ring->cq.ring_mask];
        cqe = {};
        cqe.user_data = userData;
        cqe.res = result;
    }

    void ConsumePublished(unsigned count) {
        Y_ABORT_UNLESS(count <= SqTail - SqHead);
        for (unsigned index = 0; index < count; ++index) {
            const auto submission = Snapshot(SqHead++);
            Stats->Consumed.push_back(submission);
            if (submission.Sqe.opcode == IORING_OP_NOP || submission.Sqe.opcode == IORING_OP_POLL_ADD) {
                PushCqe(submission.Sqe.user_data,
                    Next(ControlResults, submission.Sqe.opcode == IORING_OP_POLL_ADD ? POLLIN : 0));
            } else if (AutoComplete) {
                size_t bytes = submission.Sqe.len;
                if (!submission.Iovs.empty()) {
                    bytes = 0;
                    for (const auto& iov : submission.Iovs) {
                        bytes += iov.iov_len;
                    }
                }
                PushCqe(submission.Sqe.user_data, bytes);
            } else {
                Outstanding.push_back(submission.Sqe.user_data);
            }
        }
    }

    int Submit(io_uring* ring) override {
        // Exactly as __io_uring_flush_sq: publish before returning any result,
        // including an error, and distinguish local from kernel SQ heads.
        ring->sq.sqe_head = ring->sq.sqe_tail;
        SqTail = ring->sq.sqe_tail;
        std::vector<TSubmission> attempt;
        for (unsigned index = SqHead; index != SqTail; ++index) {
            attempt.push_back(Snapshot(index));
        }
        Stats->Attempts.push_back(std::move(attempt));
        TSubmitStep step{static_cast<int>(SqTail - SqHead), SqTail - SqHead};
        if (!SubmitResults.empty()) {
            step = SubmitResults.front();
            SubmitResults.pop_front();
        }
        ConsumePublished(step.Consumed);
        return step.Result;
    }

    int PeekCqe(io_uring*, io_uring_cqe** cqe) override {
        *cqe = nullptr;
        const int result = Next(PeekResults);
        if (result) {
            return result;
        }
        if (CqHead == CqTail) {
            return -EAGAIN;
        }
        *cqe = &Cqes[CqHead & Ring->cq.ring_mask];
        return 0;
    }

    int WaitCqeTimeout(io_uring*, io_uring_cqe** cqe, __kernel_timespec*) override {
        ++Stats->WaitCalls;
        *cqe = nullptr;
        const int result = Next(WaitResults, CqHead == CqTail ? -ETIME : 0);
        if (!result) {
            *cqe = &Cqes[CqHead & Ring->cq.ring_mask];
        }
        return result;
    }

    void Exit(io_uring*) override {
        ++Stats->ExitCalls;
    }

    void Backoff(ui32 micros) override {
        Stats->Backoffs.push_back(micros);
        if (OnBackoff) {
            OnBackoff();
        }
    }

    void Complete(TUringOperationBase& op, int result) {
        const ui64 userData = reinterpret_cast<uintptr_t>(&op);
        auto found = std::find(Outstanding.begin(), Outstanding.end(), userData);
        Y_ABORT_UNLESS(found != Outstanding.end(), "fake completed an unconsumed operation");
        Outstanding.erase(found);
        PushCqe(userData, result);
    }
};

struct TScriptedRouter {
    TScriptedUringBackend* Backend;
    std::unique_ptr<TUringRouter> Router;

    explicit TScriptedRouter(ui32 depth = 16,
            std::unique_ptr<TScriptedUringBackend> backend = std::make_unique<TScriptedUringBackend>())
        : Backend(backend.get())
        , Router(TUringRouterTestPeer::Create(std::move(backend), depth))
    {}

    ~TScriptedRouter() {
        TUringRouterTestPeer::AbandonFakeOperations(*Router);
    }

    void Initialize() { TUringRouterTestPeer::Initialize(*Router); }
    void Issue() {
        TUringRouterTestPeer::Drain(*Router);
        TUringRouterTestPeer::Submit(*Router);
    }
    void Complete(TUringOperationBase& op, int result) {
        Backend->Complete(op, result);
        UNIT_ASSERT_VALUES_EQUAL(TUringRouterTestPeer::Reap(*Router), 1u);
    }
};

struct TScriptedOp : TUringOperationBase {
    unsigned Completions = 0;
    unsigned Drops = 0;
    std::function<void()> Callback;

    void OnComplete(TActorSystem*) noexcept override {
        ++Completions;
        if (Callback) {
            Callback();
        }
    }
    void OnDrop(NActors::TActorSystem*) noexcept override { ++Drops; }
};

void AssertSqe(const TScriptedUringBackend::TSubmission& submission, int opcode,
        const void* buffer, size_t size, ui64 offset) {
    UNIT_ASSERT_VALUES_EQUAL(submission.Sqe.opcode, opcode);
    UNIT_ASSERT_VALUES_EQUAL(submission.Sqe.addr, reinterpret_cast<uintptr_t>(buffer));
    UNIT_ASSERT_VALUES_EQUAL(submission.Sqe.len, size);
    UNIT_ASSERT_VALUES_EQUAL(submission.Sqe.off, offset);
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TUringRouterScriptedTest) {
    Y_UNIT_TEST(IssuerRetriesWithoutNewIngressAndBacksOffUntilProgress) {
        auto backend = std::make_unique<TScriptedUringBackend>();
        auto stats = backend->Stats;
        backend->Features = 0; // Idle polling must not consume the data script.
        backend->AutoComplete = true;
        backend->SubmitResults = {{-EINTR}, {-EAGAIN}, {-EBUSY}, {0}};
        backend->OnBackoff = [] { std::this_thread::yield(); };
        char buffer[8] = {};
        TManualEvent event;
        TTestOp op;
        op.Event = &event;
        PrepareReadOp(op, buffer, sizeof(buffer), 0);
        auto router = TUringRouterTestPeer::Create(std::move(backend));
        router->Start();
        UNIT_ASSERT(router->Read(&op));
        UNIT_ASSERT(event.WaitT(TDuration::Seconds(5)));
        router.reset(); // Join before inspecting issuer-owned script history.
        UNIT_ASSERT_VALUES_EQUAL(op.GetResult(), 8);
        UNIT_ASSERT_VALUES_EQUAL(stats->InitFlags.size(), 1u);
        UNIT_ASSERT(stats->InitFlags.front() & IORING_SETUP_SUBMIT_ALL);
        UNIT_ASSERT(stats->InitFlags.front() & IORING_SETUP_SINGLE_ISSUER);
        unsigned dataAttempts = 0;
        for (const auto& attempt : stats->Attempts) {
            if (attempt.front().Sqe.user_data == reinterpret_cast<uintptr_t>(&op)) {
                ++dataAttempts;
                UNIT_ASSERT_VALUES_EQUAL(attempt.size(), 1u);
                AssertSqe(attempt.front(), IORING_OP_READ, buffer, sizeof(buffer), 0);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(dataAttempts, 5u);
        unsigned retryBackoffs = 0;
        for (const ui32 micros : stats->Backoffs) {
            if (micros <= 1000) {
                ++retryBackoffs;
            }
        }
        UNIT_ASSERT_GE(retryBackoffs, 4u);
    }

    Y_UNIT_TEST(IssuerObservesStopBetweenInterruptedSubmissionAttempts) {
        auto backend = std::make_unique<TScriptedUringBackend>();
        auto* fake = backend.get();
        auto stats = backend->Stats;
        backend->Features = 0;
        backend->SubmitResults = {{-EINTR}, {-EINTR}, {-EINTR}};
        char buffer[8] = {};
        TManualEvent event;
        TTestOp op;
        op.Event = &event;
        PrepareReadOp(op, buffer, sizeof(buffer), 0);
        auto router = TUringRouterTestPeer::Create(std::move(backend));
        bool stopped = false; // Accessed only by the issuer's Backoff callback.
        fake->OnBackoff = [&] {
            if (!stopped && !stats->Attempts.empty()) {
                stopped = true;
                router->StopAsync();
                // Settle the fake published request so StopSync does not abort
                // on unresolved ownership during teardown.
                fake->ConsumePublished(1);
                fake->Complete(op, 8);
                fake->SubmitResults.clear();
            }
            std::this_thread::yield();
        };
        router->Start();
        UNIT_ASSERT(router->Read(&op));
        UNIT_ASSERT(event.WaitT(TDuration::Seconds(5)));
        UNIT_ASSERT(!router->Read(&op));
        router.reset();
        UNIT_ASSERT(stopped);
        UNIT_ASSERT_VALUES_EQUAL(op.GetResult(), 8);
        unsigned dataAttempts = 0;
        for (const auto& attempt : stats->Attempts) {
            if (attempt.front().Sqe.user_data == reinterpret_cast<uintptr_t>(&op)) {
                ++dataAttempts;
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(dataAttempts, 1u);
    }

    Y_UNIT_TEST(CallbackCanImmediatelyRecycleAndReadmitOperation) {
        TScriptedRouter fixture;
        fixture.Initialize();
        char buffer[8] = {};
        TScriptedOp op;
        PrepareReadOp(op, buffer, sizeof(buffer), 0);
        op.Callback = [&] {
            Y_ABORT_UNLESS(fixture.Backend->CqHead == fixture.Backend->CqTail);
            if (op.Completions == 1) {
                op.ResetSubmissionState();
                PrepareWriteOp(op, buffer, 4, 32);
                Y_ABORT_UNLESS(fixture.Router->Write(&op));
            }
        };
        UNIT_ASSERT(fixture.Router->Read(&op));
        fixture.Issue();
        fixture.Complete(op, 3);
        fixture.Issue();
        fixture.Complete(op, 5);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Router->GetInflight(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(op.TakeShortIoCount(), 0u);
        fixture.Issue();
        AssertSqe(fixture.Backend->Stats->Consumed.back(), IORING_OP_WRITE, buffer, 4, 32);
        fixture.Complete(op, 4);
        UNIT_ASSERT_VALUES_EQUAL(op.Completions, 2u);
        UNIT_ASSERT_VALUES_EQUAL(op.GetResult(), 4);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Router->GetInflight(), 0u);
    }

    Y_UNIT_TEST(TerminalCallbackCanDeleteOperationAfterShorts) {
        struct TDeletingOp : TUringOperationBase {
            TScriptedUringBackend* Backend = nullptr;
            unsigned* Completions = nullptr;
            void OnComplete(TActorSystem*) noexcept override {
                Y_ABORT_UNLESS(Backend->CqHead == Backend->CqTail);
                Y_ABORT_UNLESS(GetResult() == 8);
                ++*Completions;
                delete this;
            }
            void OnDrop(NActors::TActorSystem*) noexcept override { delete this; }
        };
        TScriptedRouter fixture;
        fixture.Initialize();
        unsigned completions = 0;
        char buffer[8] = {};
        auto owner = std::make_unique<TDeletingOp>();
        owner->Backend = fixture.Backend;
        owner->Completions = &completions;
        PrepareReadOp(*owner, buffer, sizeof(buffer), 0);
        auto* op = owner.release();
        UNIT_ASSERT(fixture.Router->Read(op));
        fixture.Issue();
        fixture.Complete(*op, 3);
        fixture.Issue();
        fixture.Complete(*op, 5);
        UNIT_ASSERT_VALUES_EQUAL(completions, 1u);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Router->GetInflight(), 0u);
    }

    Y_UNIT_TEST(AggregateSuccessResultExceedsSignedCqeWidth) {
        TScriptedRouter fixture;
        fixture.Initialize();
        // Repeated segments refer to valid storage without allocating the full
        // logical 2 GiB. The fake never accesses write payload bytes.
        constexpr size_t segmentSize = 32u << 20;
        TAlignedBuf buffer(segmentSize);
        TScriptedOp op;
        op.SetOperationType(TUringOperationBase::EWRITE);
        op.PrepareScatterGather(64, 0);
        for (unsigned index = 0; index < 64; ++index) {
            op.AddIov(buffer.Data(), segmentSize);
        }
        UNIT_ASSERT(fixture.Router->Write(&op));
        fixture.Issue();
        fixture.Complete(op, Max<i32>());
        fixture.Issue();
        fixture.Complete(op, 1);
        UNIT_ASSERT_VALUES_EQUAL(op.GetResult(), i64{1} << 31);
        UNIT_ASSERT_VALUES_EQUAL(op.Completions, 1u);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Router->GetInflight(), 0u);
    }

    Y_UNIT_TEST(ScalarAndFixedShortsCompleteOnceWithAggregateResult) {
        for (const bool write : {false, true}) {
            for (const bool fixed : {false, true}) {
                TScriptedRouter fixture;
                char buffer[24] = {};
                iovec registrations[8];
                for (auto& registration : registrations) {
                    registration = {buffer, sizeof(buffer)};
                }
                if (fixed) {
                    fixture.Router->RegisterBuffers(registrations, 8);
                    fixture.Router->RegisterFile();
                }
                fixture.Initialize();
                std::vector<TDeviceIoSample> samples;
                fixture.Router->SetSampleSink([&](const TDeviceIoSample& sample) { samples.push_back(sample); });
                TScriptedOp op;
                op.Callback = [&] {
                    // The completing CQE is retired before clients can recycle.
                    Y_ABORT_UNLESS(fixture.Backend->CqHead == fixture.Backend->CqTail);
                    Y_ABORT_UNLESS(samples.size() == 3);
                };
                if (fixed) {
                    const bool accepted = write
                        ? fixture.Router->WriteFixed(buffer, sizeof(buffer), 100, 7, &op)
                        : fixture.Router->ReadFixed(buffer, sizeof(buffer), 100, 7, &op);
                    UNIT_ASSERT(accepted);
                } else {
                    op.SetOperationType(write ? TUringOperationBase::EWRITE : TUringOperationBase::EREAD);
                    op.PrepareIov(buffer, sizeof(buffer), 100);
                    UNIT_ASSERT(fixture.Router->Submit(&op));
                }
                const int opcode = fixed
                    ? (write ? IORING_OP_WRITE_FIXED : IORING_OP_READ_FIXED)
                    : (write ? IORING_OP_WRITE : IORING_OP_READ);
                for (const unsigned progress : {0u, 4u, 12u}) {
                    fixture.Issue();
                    AssertSqe(fixture.Backend->Stats->Consumed.back(), opcode,
                        buffer + progress, sizeof(buffer) - progress, 100 + progress);
                    if (fixed) {
                        UNIT_ASSERT_VALUES_EQUAL(fixture.Backend->Stats->Consumed.back().Sqe.buf_index, 7u);
                        UNIT_ASSERT(fixture.Backend->Stats->Consumed.back().Sqe.flags & IOSQE_FIXED_FILE);
                    }
                    UNIT_ASSERT_VALUES_EQUAL(fixture.Router->GetInflight(), 1u);
                    UNIT_ASSERT_VALUES_EQUAL(op.Completions, 0u);
                    fixture.Complete(op, progress == 0 ? 4 : progress == 4 ? 8 : 12);
                }
                UNIT_ASSERT_VALUES_EQUAL(op.Completions, 1u);
                UNIT_ASSERT_VALUES_EQUAL(op.Drops, 0u);
                UNIT_ASSERT_VALUES_EQUAL(op.GetResult(), sizeof(buffer));
                UNIT_ASSERT_VALUES_EQUAL(op.GetOperationBytes(), 0u);
                UNIT_ASSERT_VALUES_EQUAL(op.GetDiskOffset(), 124u);
                UNIT_ASSERT_VALUES_EQUAL(op.TakeShortIoCount(), 2u);
                UNIT_ASSERT_VALUES_EQUAL(op.TakeShortIoCount(), 0u);
                UNIT_ASSERT_VALUES_EQUAL(fixture.Router->GetInflight(), 0u);
                for (size_t index = 0; index < samples.size(); ++index) {
                    const unsigned progress = index == 0 ? 0 : index == 1 ? 4 : 12;
                    UNIT_ASSERT_VALUES_EQUAL(samples[index].Offset, 100 + progress);
                    UNIT_ASSERT_VALUES_EQUAL(samples[index].Size, sizeof(buffer) - progress);
                    UNIT_ASSERT_VALUES_EQUAL(samples[index].IsWrite, write);
                    UNIT_ASSERT(samples[index].SubmitCycles);
                    UNIT_ASSERT_GE(samples[index].CompleteCycles, samples[index].SubmitCycles);
                }
            }
        }
    }

    Y_UNIT_TEST(VectoredShortCrossesSegmentsThenUsesScalarRemainder) {
        for (const bool write : {false, true}) {
            TScriptedRouter fixture;
            fixture.Initialize();
            char first[4] = {}, second[8] = {}, third[12] = {};
            TScriptedOp op;
            op.SetOperationType(write ? TUringOperationBase::EWRITE : TUringOperationBase::EREAD);
            op.PrepareScatterGather(3, 64);
            op.AddIov(first, sizeof(first));
            op.AddIov(second, sizeof(second));
            op.AddIov(third, sizeof(third));
            UNIT_ASSERT(fixture.Router->Submit(&op));
            fixture.Issue();
            const auto& initial = fixture.Backend->Stats->Consumed.back();
            UNIT_ASSERT(initial.Sqe.opcode == (write ? IORING_OP_WRITEV : IORING_OP_READV));
            UNIT_ASSERT_VALUES_EQUAL(initial.Iovs.size(), 3u);
            fixture.Complete(op, 6);
            fixture.Issue();
            const auto& middle = fixture.Backend->Stats->Consumed.back();
            UNIT_ASSERT_VALUES_EQUAL(middle.Iovs.size(), 2u);
            UNIT_ASSERT_EQUAL(middle.Iovs[0].iov_base, second + 2);
            UNIT_ASSERT_VALUES_EQUAL(middle.Iovs[0].iov_len, 6u);
            UNIT_ASSERT_VALUES_EQUAL(middle.Sqe.off, 70u);
            fixture.Complete(op, 6);
            fixture.Issue();
            AssertSqe(fixture.Backend->Stats->Consumed.back(), write ? IORING_OP_WRITE : IORING_OP_READ,
                third, sizeof(third), 76);
            fixture.Complete(op, 12);
            UNIT_ASSERT_VALUES_EQUAL(op.Completions, 1u);
            UNIT_ASSERT_VALUES_EQUAL(op.GetResult(), 24);
            UNIT_ASSERT_VALUES_EQUAL(op.TakeShortIoCount(), 2u);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Router->GetInflight(), 0u);
        }
    }

    Y_UNIT_TEST(ShortThenZeroOrNegativeCompletesWithoutRetryingDataError) {
        for (const int result : {0, -EIO, -EAGAIN, -EBUSY, -EINTR}) {
            TScriptedRouter fixture;
            fixture.Initialize();
            char buffer[8] = {};
            TScriptedOp op;
            PrepareReadOp(op, buffer, sizeof(buffer), 0);
            UNIT_ASSERT(fixture.Router->Read(&op));
            fixture.Issue();
            fixture.Complete(op, 3);
            fixture.Issue();
            fixture.Complete(op, result);
            fixture.Issue();
            UNIT_ASSERT_VALUES_EQUAL(fixture.Backend->Stats->Consumed.size(), 2u);
            UNIT_ASSERT_VALUES_EQUAL(op.Completions, 1u);
            UNIT_ASSERT_VALUES_EQUAL(op.Drops, 0u);
            UNIT_ASSERT_VALUES_EQUAL(op.GetResult(), result ? result : -EIO);
            UNIT_ASSERT_VALUES_EQUAL(op.GetDiskOffset(), 3u);
            UNIT_ASSERT_VALUES_EQUAL(op.GetOperationBytes(), 5u);
            UNIT_ASSERT_VALUES_EQUAL(op.TakeShortIoCount(), 1u);
            UNIT_ASSERT(!fixture.Router->IsBroken());
            UNIT_ASSERT_VALUES_EQUAL(fixture.Router->GetInflight(), 0u);
        }
    }

    Y_UNIT_TEST(StopCancelsQueuedAndStagedContinuationsButDropsFreshOperations) {
        for (const bool staged : {false, true}) {
            TScriptedRouter fixture;
            fixture.Initialize();
            char buffer[8] = {};
            TScriptedOp partial, fresh;
            PrepareWriteOp(partial, buffer, sizeof(buffer), 0);
            PrepareWriteOp(fresh, buffer, sizeof(buffer), 0);
            UNIT_ASSERT(fixture.Router->Write(&partial));
            fixture.Issue();
            fixture.Complete(partial, 3);
            UNIT_ASSERT(fixture.Router->Write(&fresh));
            if (staged) {
                TUringRouterTestPeer::Drain(*fixture.Router);
                UNIT_ASSERT_VALUES_EQUAL(TUringRouterTestPeer::Staged(*fixture.Router), 2u);
            }
            fixture.Router->StopAsync();
            fixture.Issue();
            UNIT_ASSERT_VALUES_EQUAL(partial.Completions, 1u);
            UNIT_ASSERT_VALUES_EQUAL(partial.GetResult(), -ECANCELED);
            UNIT_ASSERT_VALUES_EQUAL(partial.Drops, 0u);
            UNIT_ASSERT_VALUES_EQUAL(fresh.Completions, 0u);
            UNIT_ASSERT_VALUES_EQUAL(fresh.Drops, 1u);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Backend->Stats->Consumed.size(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Router->GetInflight(), 0u);
        }
    }

    Y_UNIT_TEST(ShortCompletionAfterStopDoesNotPrepareAnotherSqe) {
        TScriptedRouter fixture;
        fixture.Initialize();
        char buffer[8] = {};
        TScriptedOp op;
        PrepareReadOp(op, buffer, sizeof(buffer), 0);
        UNIT_ASSERT(fixture.Router->Read(&op));
        fixture.Issue();
        fixture.Router->StopAsync();
        fixture.Complete(op, 3);
        fixture.Issue();
        UNIT_ASSERT_VALUES_EQUAL(op.Completions, 1u);
        UNIT_ASSERT_VALUES_EQUAL(op.GetResult(), -ECANCELED);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Backend->Stats->Consumed.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Router->GetInflight(), 0u);
    }

    Y_UNIT_TEST(FullSqDefersContinuationWithoutReadmission) {
        TScriptedRouter fixture(1);
        fixture.Initialize();
        char buffer[8] = {};
        TScriptedOp partial, blocker;
        PrepareReadOp(partial, buffer, sizeof(buffer), 0);
        PrepareWriteOp(blocker, buffer, sizeof(buffer), 0);
        UNIT_ASSERT(fixture.Router->Read(&partial));
        fixture.Issue();
        UNIT_ASSERT(fixture.Router->Write(&blocker));
        TUringRouterTestPeer::Drain(*fixture.Router);
        fixture.Complete(partial, 3);
        TUringRouterTestPeer::Drain(*fixture.Router);
        UNIT_ASSERT_VALUES_EQUAL(TUringRouterTestPeer::Staged(*fixture.Router), 1u);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Router->GetInflight(), 2u);
        TUringRouterTestPeer::Submit(*fixture.Router);
        fixture.Complete(blocker, 8);
        fixture.Issue();
        AssertSqe(fixture.Backend->Stats->Consumed.back(), IORING_OP_READ, buffer + 3, 5, 3);
        fixture.Complete(partial, 5);
        UNIT_ASSERT_VALUES_EQUAL(partial.Completions, 1u);
        UNIT_ASSERT_VALUES_EQUAL(blocker.Completions, 1u);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Router->GetInflight(), 0u);
    }

    Y_UNIT_TEST(TransientSubmitErrorsReusePublishedSqesAndReapBetweenAttempts) {
        TScriptedRouter fixture;
        fixture.Initialize();
        char buffer[8] = {};
        TScriptedOp first, second, third;
        for (auto* op : {&first, &second, &third}) {
            PrepareReadOp(*op, buffer, sizeof(buffer), 0);
            UNIT_ASSERT(fixture.Router->Read(op));
        }
        fixture.Backend->SubmitResults = {{1, 1}, {-EINTR}, {-EAGAIN}, {-EBUSY}, {0}, {2, 2}};
        fixture.Issue();
        fixture.Complete(first, 8);
        UNIT_ASSERT_VALUES_EQUAL(first.Completions, 1u);
        for (unsigned attempt = 0; attempt < 5; ++attempt) {
            UNIT_ASSERT(!TUringRouterTestPeer::Drain(*fixture.Router));
            TUringRouterTestPeer::Submit(*fixture.Router);
            TUringRouterTestPeer::Reap(*fixture.Router);
            UNIT_ASSERT(!fixture.Router->IsBroken());
        }
        UNIT_ASSERT_VALUES_EQUAL(fixture.Backend->Stats->Attempts.size(), 6u);
        for (size_t attempt = 1; attempt < fixture.Backend->Stats->Attempts.size(); ++attempt) {
            const auto& suffix = fixture.Backend->Stats->Attempts[attempt];
            UNIT_ASSERT_VALUES_EQUAL(suffix.size(), 2u);
            UNIT_ASSERT_VALUES_EQUAL(suffix[0].Sqe.user_data, reinterpret_cast<uintptr_t>(&second));
            UNIT_ASSERT_VALUES_EQUAL(suffix[1].Sqe.user_data, reinterpret_cast<uintptr_t>(&third));
        }
        UNIT_ASSERT_VALUES_EQUAL(fixture.Backend->Stats->Consumed.size(), 3u);
        UNIT_ASSERT_VALUES_EQUAL(TUringRouterTestPeer::Ready(*fixture.Router), 0u);
        fixture.Complete(second, 8);
        fixture.Complete(third, 8);
        UNIT_ASSERT_VALUES_EQUAL(second.Completions, 1u);
        UNIT_ASSERT_VALUES_EQUAL(third.Completions, 1u);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Router->GetInflight(), 0u);
    }

    Y_UNIT_TEST(FatalSubmissionRetainsPublishedSuffixAndFreezesEverySubmitPath) {
        for (const bool alreadyStopping : {false, true}) {
            TScriptedRouter fixture;
            fixture.Initialize();
            char buffer[8] = {};
            TScriptedOp consumed, published, fresh;
            for (auto* op : {&consumed, &published}) {
                PrepareReadOp(*op, buffer, sizeof(buffer), 0);
                UNIT_ASSERT(fixture.Router->Read(op));
            }
            PrepareReadOp(fresh, buffer, sizeof(buffer), 0);
            fixture.Backend->SubmitResults = {{-EAGAIN, 1}, {-EBADF}};
            fixture.Issue();
            UNIT_ASSERT(fixture.Router->Read(&fresh));
            if (alreadyStopping) {
                fixture.Router->StopAsync();
            }
            TUringRouterTestPeer::Submit(*fixture.Router, true);
            UNIT_ASSERT(fixture.Router->IsBroken());
            UNIT_ASSERT_EQUAL(TUringRouterTestPeer::State(*fixture.Router), EUringRouterState::StoppingBroken);
            UNIT_ASSERT(!fixture.Router->Read(&fresh));
            fixture.Issue();
            UNIT_ASSERT_VALUES_EQUAL(fresh.Drops, 1u);
            UNIT_ASSERT_VALUES_EQUAL(consumed.Completions + consumed.Drops, 0u);
            UNIT_ASSERT_VALUES_EQUAL(published.Completions + published.Drops, 0u);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Router->GetInflight(), 2u);
            UNIT_ASSERT_VALUES_EQUAL(TUringRouterTestPeer::Ready(*fixture.Router), 1u);
            TUringRouterTestPeer::Park(*fixture.Router);
            TUringRouterTestPeer::Submit(*fixture.Router, true);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Backend->Stats->Attempts.size(), 2u);
            // Artificially settle fake ownership so StopSync does not abort
            // during teardown. This is not a claim that a real non-SQPOLL ring
            // consumes a suffix without enter.
            fixture.Backend->ConsumePublished(1);
            fixture.Complete(consumed, 8);
            fixture.Complete(published, 8);
            TUringRouterTestPeer::Stop(*fixture.Router);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Backend->Stats->Attempts.size(), 2u);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Router->GetInflight(), 0u);
            UNIT_ASSERT_VALUES_EQUAL(consumed.Completions, 1u);
            UNIT_ASSERT_VALUES_EQUAL(published.Completions, 1u);
        }
    }

    Y_UNIT_TEST(FatalSubmitResultIsClassifiedEvenIfKernelConsumedWholeBatch) {
        TScriptedRouter fixture;
        fixture.Initialize();
        char buffer[8] = {};
        TScriptedOp op;
        PrepareReadOp(op, buffer, sizeof(buffer), 0);
        UNIT_ASSERT(fixture.Router->Read(&op));
        fixture.Backend->SubmitResults = {{-EBADF, 1}};
        fixture.Issue();
        UNIT_ASSERT(fixture.Router->IsBroken());
        UNIT_ASSERT_VALUES_EQUAL(TUringRouterTestPeer::Ready(*fixture.Router), 0u);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Router->GetInflight(), 1u);
        fixture.Complete(op, 8);
        UNIT_ASSERT_VALUES_EQUAL(op.Completions, 1u);
        UNIT_ASSERT_VALUES_EQUAL(op.Drops, 0u);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Router->GetInflight(), 0u);
    }

    Y_UNIT_TEST(PeekErrorsRetryTransientFailuresAndBreakOnFatalFailure) {
        TScriptedRouter fixture;
        fixture.Initialize();
        char buffer[8] = {};
        TScriptedOp op;
        PrepareReadOp(op, buffer, sizeof(buffer), 0);
        UNIT_ASSERT(fixture.Router->Read(&op));
        fixture.Issue();
        fixture.Backend->Complete(op, 8);
        fixture.Backend->PeekResults = {-EINTR, -EBUSY, -EAGAIN};
        for (unsigned pass = 0; pass < 4 && !op.Completions; ++pass) {
            TUringRouterTestPeer::Reap(*fixture.Router);
            UNIT_ASSERT(!fixture.Router->IsBroken());
        }
        UNIT_ASSERT_VALUES_EQUAL(op.Completions, 1u);
        fixture.Backend->PeekResults = {-EINVAL};
        TUringRouterTestPeer::Reap(*fixture.Router);
        UNIT_ASSERT(fixture.Router->IsBroken());
        UNIT_ASSERT_EQUAL(TUringRouterTestPeer::State(*fixture.Router), EUringRouterState::StoppingBroken);
    }

    Y_UNIT_TEST(FatalWaitOrPeekRetiresOutstandingWakePollWithoutStopSubmission) {
        for (const bool failWait : {false, true}) {
            TScriptedRouter fixture;
            fixture.Initialize();
            if (failWait) {
                fixture.Backend->WaitResults = {-EBADF};
            }
            TUringRouterTestPeer::Park(*fixture.Router);
            if (!failWait) {
                fixture.Backend->PeekResults = {-EBADF};
                TUringRouterTestPeer::Reap(*fixture.Router);
            }
            UNIT_ASSERT(fixture.Router->IsBroken());
            UNIT_ASSERT_VALUES_EQUAL(fixture.Backend->Stats->Attempts.size(), 1u);
            UNIT_ASSERT(fixture.Backend->Stats->Consumed.front().Sqe.opcode == IORING_OP_POLL_ADD);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Backend->CqTail - fixture.Backend->CqHead, 1u);
            TUringRouterTestPeer::Stop(*fixture.Router);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Backend->Stats->Attempts.size(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Backend->CqTail, fixture.Backend->CqHead);
        }
    }

    Y_UNIT_TEST(ControlCqeFailureBreaksRingAndRetiresControlOwnership) {
        for (const bool failPoll : {false, true}) {
            TScriptedRouter fixture;
            fixture.Initialize();
            fixture.Backend->ControlResults = {-EIO};
            if (failPoll) {
                TUringRouterTestPeer::Park(*fixture.Router);
                TUringRouterTestPeer::Reap(*fixture.Router);
            } else {
                fixture.Router->StopAsync();
                TUringRouterTestPeer::Stop(*fixture.Router);
            }
            UNIT_ASSERT(fixture.Router->IsBroken());
            UNIT_ASSERT_EQUAL(TUringRouterTestPeer::State(*fixture.Router), EUringRouterState::StoppingBroken);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Backend->CqTail, fixture.Backend->CqHead);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Backend->Stats->Attempts.size(), 1u);
        }
    }

    Y_UNIT_TEST(InitializationRetriesInterruptsAndFallsBackToPlainRing) {
        auto backend = std::make_unique<TScriptedUringBackend>();
        backend->InitResults = {-EINTR, -EINVAL, -EINTR, 0};
        backend->EnableResults = {-EINTR, 0};
        backend->FileResults = {-EINTR, -EPERM};
        backend->BufferResults = {-EINTR, -ENOMEM};
        TScriptedRouter fixture(16, std::move(backend));
        fixture.Router->RegisterFile();
        char buffer[8] = {};
        iovec registration{buffer, sizeof(buffer)};
        fixture.Router->RegisterBuffers(&registration, 1);
        fixture.Initialize();
        const auto& stats = *fixture.Backend->Stats;
        UNIT_ASSERT_VALUES_EQUAL(stats.InitFlags.size(), 4u);
        UNIT_ASSERT(stats.InitFlags[0] & IORING_SETUP_SINGLE_ISSUER);
        UNIT_ASSERT_VALUES_EQUAL(stats.InitFlags[0], stats.InitFlags[1]);
        UNIT_ASSERT(!(stats.InitFlags[2] & IORING_SETUP_SINGLE_ISSUER));
        UNIT_ASSERT_VALUES_EQUAL(stats.InitFlags[2], stats.InitFlags[3]);
        for (const unsigned flags : stats.InitFlags) {
            UNIT_ASSERT(flags & IORING_SETUP_SUBMIT_ALL);
            UNIT_ASSERT(flags & IORING_SETUP_R_DISABLED);
        }
        UNIT_ASSERT_VALUES_EQUAL(stats.EnableCalls, 2u);
        UNIT_ASSERT_VALUES_EQUAL(stats.FileRegistrationCalls, 2u);
        UNIT_ASSERT_VALUES_EQUAL(stats.BufferRegistrationCalls, 2u);
        UNIT_ASSERT_EQUAL(fixture.Router->GetUringFavor(), EUringFavor::Plain);
        UNIT_ASSERT(!fixture.Router->IsFileRegistered());
        UNIT_ASSERT(!fixture.Router->AreBuffersRegistered());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Router->GetRegisterFileErrno(), EPERM);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Router->GetRegisterBuffersErrno(), ENOMEM);
        UNIT_ASSERT(!fixture.Router->IsBroken());
        TScriptedOp op;
        PrepareReadOp(op, buffer, sizeof(buffer), 0);
        UNIT_ASSERT(fixture.Router->Read(&op));
        fixture.Issue();
        UNIT_ASSERT(!(stats.Consumed.back().Sqe.flags & IOSQE_FIXED_FILE));
        fixture.Complete(op, 8);
    }

    Y_UNIT_TEST(UnsupportedSubmitAllFallsBackAndRetriesPartialSubmission) {
        auto backend = std::make_unique<TScriptedUringBackend>();
        backend->InitResults = {-EINVAL, -EINTR, -EINVAL, -EINTR, 0};
        TScriptedRouter fixture(16, std::move(backend));
        fixture.Initialize();
        const auto& stats = *fixture.Backend->Stats;
        UNIT_ASSERT_VALUES_EQUAL(stats.InitFlags.size(), 5u);
        UNIT_ASSERT(stats.InitFlags[0] & IORING_SETUP_SINGLE_ISSUER);
        UNIT_ASSERT(stats.InitFlags[0] & IORING_SETUP_SUBMIT_ALL);
        UNIT_ASSERT_VALUES_EQUAL(stats.InitFlags[1], IORING_SETUP_R_DISABLED | IORING_SETUP_SUBMIT_ALL);
        UNIT_ASSERT_VALUES_EQUAL(stats.InitFlags[1], stats.InitFlags[2]);
        UNIT_ASSERT_VALUES_EQUAL(stats.InitFlags[3], IORING_SETUP_R_DISABLED);
        UNIT_ASSERT_VALUES_EQUAL(stats.InitFlags[3], stats.InitFlags[4]);
        UNIT_ASSERT_EQUAL(fixture.Router->GetUringFavor(), EUringFavor::Plain);

        char buffer[8] = {};
        TScriptedOp failed, later;
        for (auto* op : {&failed, &later}) {
            PrepareReadOp(*op, buffer, sizeof(buffer), 0);
            UNIT_ASSERT(fixture.Router->Read(op));
        }
        // Without SUBMIT_ALL, a submission-time failure can stop the batch
        // after its failing SQE. That SQE still receives an error CQE.
        fixture.Backend->SubmitResults = {{1, 1}, {1, 1}};
        fixture.Issue();
        fixture.Complete(failed, -EINVAL);
        UNIT_ASSERT(!fixture.Router->IsBroken());
        UNIT_ASSERT_VALUES_EQUAL(failed.GetResult(), -EINVAL);
        UNIT_ASSERT_VALUES_EQUAL(failed.Completions, 1u);
        UNIT_ASSERT_VALUES_EQUAL(later.Completions, 0u);
        UNIT_ASSERT_VALUES_EQUAL(TUringRouterTestPeer::Ready(*fixture.Router), 1u);
        fixture.Issue();
        UNIT_ASSERT_VALUES_EQUAL(stats.Attempts.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(stats.Attempts.back().size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(stats.Attempts.back().front().Sqe.user_data,
            reinterpret_cast<uintptr_t>(&later));
        fixture.Complete(later, 8);
        UNIT_ASSERT_VALUES_EQUAL(later.Completions, 1u);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Router->GetInflight(), 0u);
    }

    Y_UNIT_TEST(SubmitAllFallbackDoesNotHideFatalSetupErrors) {
        for (const int error : {EPERM, ENOMEM, EOPNOTSUPP}) {
            for (const bool unsupportedSubmitAll : {false, true}) {
                auto backend = std::make_unique<TScriptedUringBackend>();
                const auto stats = backend->Stats;
                backend->InitResults = {-EINVAL};
                if (unsupportedSubmitAll) {
                    backend->InitResults.push_back(-EINVAL);
                }
                backend->InitResults.push_back(-error);
                auto router = TUringRouterTestPeer::Create(std::move(backend));
                router->Start();
                UNIT_ASSERT(router->IsBroken());
                UNIT_ASSERT_EQUAL(router->GetUringFavor(), EUringFavor::FallbackPDisk);
                UNIT_ASSERT_VALUES_EQUAL(stats->InitFlags.size(), unsupportedSubmitAll ? 3u : 2u);
                UNIT_ASSERT_VALUES_EQUAL(stats->InitFlags.back(), IORING_SETUP_R_DISABLED
                    | (unsupportedSubmitAll ? 0u : IORING_SETUP_SUBMIT_ALL));
                router.reset();
                UNIT_ASSERT_VALUES_EQUAL(stats->EnableCalls, 0u);
                UNIT_ASSERT_VALUES_EQUAL(stats->ExitCalls, 0u);
            }
        }
    }

    Y_UNIT_TEST(FailedInitializationAndEnableReleaseStartAndExitOnlyInitializedRing) {
        for (const bool failInit : {false, true}) {
            auto backend = std::make_unique<TScriptedUringBackend>();
            auto stats = backend->Stats;
            if (failInit) {
                backend->InitResults = {-EPERM, -EPERM};
            } else {
                backend->EnableResults = {-EINVAL};
            }
            auto router = TUringRouterTestPeer::Create(std::move(backend));
            router->Start();
            UNIT_ASSERT(router->IsBroken());
            char buffer[8] = {};
            TScriptedOp op;
            PrepareReadOp(op, buffer, sizeof(buffer), 0);
            UNIT_ASSERT(!router->Read(&op));
            UNIT_ASSERT_VALUES_EQUAL(op.Completions + op.Drops, 0u);
            router.reset();
            UNIT_ASSERT_VALUES_EQUAL(stats->EnableCalls, failInit ? 0u : 1u);
            UNIT_ASSERT_VALUES_EQUAL(stats->ExitCalls, failInit ? 0u : 1u);
        }
    }

    Y_UNIT_TEST(TimedWaitFeatureGateAvoidsImplicitTimeoutSqes) {
        for (const bool extArg : {false, true}) {
            auto backend = std::make_unique<TScriptedUringBackend>();
            backend->Features = extArg ? IORING_FEAT_EXT_ARG : 0;
            TScriptedRouter fixture(16, std::move(backend));
            fixture.Initialize();
            TUringRouterTestPeer::WaitProgress(*fixture.Router);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Backend->Stats->WaitCalls, extArg ? 1u : 0u);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Backend->Stats->Attempts.size(), 0u);
            UNIT_ASSERT_VALUES_EQUAL(TUringRouterTestPeer::Staged(*fixture.Router), 0u);
            if (!extArg) {
                UNIT_ASSERT_VALUES_EQUAL(fixture.Backend->Stats->Backoffs.size(), 1u);
                UNIT_ASSERT_LE(fixture.Backend->Stats->Backoffs.back(), 5000u);
                TUringRouterTestPeer::Park(*fixture.Router);
                UNIT_ASSERT_VALUES_EQUAL(fixture.Backend->Stats->Attempts.size(), 0u);
                UNIT_ASSERT_VALUES_EQUAL(fixture.Backend->Stats->Backoffs.back(), 5000u);
            }
            UNIT_ASSERT(!fixture.Router->IsBroken());

            char buffer[8] = {};
            TScriptedOp op;
            PrepareReadOp(op, buffer, sizeof(buffer), 0);
            UNIT_ASSERT(fixture.Router->Read(&op));
            fixture.Backend->SubmitResults = {{-EAGAIN}};
            fixture.Issue();
            const auto waitsBefore = fixture.Backend->Stats->WaitCalls;
            TUringRouterTestPeer::WaitProgress(*fixture.Router);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Backend->Stats->WaitCalls, waitsBefore);
            UNIT_ASSERT_LE(fixture.Backend->Stats->Backoffs.back(), 1000u);
            fixture.Issue();
            fixture.Complete(op, 8);
        }
    }

    Y_UNIT_TEST(TimedWaitErrorsPreserveRetryPolicyAndFatalStoppingUpgrade) {
        TScriptedRouter fixture;
        fixture.Initialize();
        for (const int result : {-EINTR, -EAGAIN, -EBUSY, -ETIME}) {
            fixture.Backend->WaitResults = {result};
            TUringRouterTestPeer::WaitProgress(*fixture.Router);
            UNIT_ASSERT(!fixture.Router->IsBroken());
        }
        fixture.Router->StopAsync();
        fixture.Backend->WaitResults = {-EBADF};
        TUringRouterTestPeer::WaitProgress(*fixture.Router);
        UNIT_ASSERT(fixture.Router->IsBroken());
        UNIT_ASSERT_EQUAL(TUringRouterTestPeer::State(*fixture.Router), EUringRouterState::StoppingBroken);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Backend->Stats->Attempts.size(), 0u);
    }

    Y_UNIT_TEST(ExternalRetryKeepsProgressAndResetsAdmissionBookkeeping) {
        TScriptedRouter fixture;
        fixture.Initialize();
        char buffer[8] = {};
        TScriptedOp op;
        PrepareReadOp(op, buffer, sizeof(buffer), 0);
        UNIT_ASSERT(fixture.Router->Read(&op));
        fixture.Issue();
        fixture.Complete(op, 3);
        fixture.Issue();
        fixture.Complete(op, -EAGAIN);
        UNIT_ASSERT_VALUES_EQUAL(op.TakeShortIoCount(), 1u);
        UNIT_ASSERT(fixture.Router->Read(&op));
        fixture.Issue();
        AssertSqe(fixture.Backend->Stats->Consumed.back(), IORING_OP_READ, buffer + 3, 5, 3);
        fixture.Complete(op, 5);
        UNIT_ASSERT_VALUES_EQUAL(op.GetResult(), 8);
        UNIT_ASSERT_VALUES_EQUAL(op.Completions, 2u);
        UNIT_ASSERT_VALUES_EQUAL(op.TakeShortIoCount(), 0u);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Router->GetInflight(), 0u);
    }

    Y_UNIT_TEST(PreparingNewRequestClearsUnconsumedShortCount) {
        for (const bool scatter : {false, true}) {
            TScriptedRouter fixture;
            fixture.Initialize();
            char buffer[8] = {};
            TScriptedOp op;
            PrepareReadOp(op, buffer, sizeof(buffer), 0);
            UNIT_ASSERT(fixture.Router->Read(&op));
            fixture.Issue();
            fixture.Complete(op, 3);
            fixture.Issue();
            fixture.Complete(op, -EIO);
            if (scatter) {
                op.PrepareScatterGather(2, 16);
                op.AddIov(buffer, 4);
                op.AddIov(buffer + 4, 4);
            } else {
                op.PrepareIov(buffer, sizeof(buffer), 16);
            }
            UNIT_ASSERT_VALUES_EQUAL(op.TakeShortIoCount(), 0u);
            UNIT_ASSERT_VALUES_EQUAL(op.GetOperationBytes(), 8u);
            UNIT_ASSERT_VALUES_EQUAL(op.GetDiskOffset(), 16u);
            UNIT_ASSERT(fixture.Router->Read(&op));
            fixture.Issue();
            fixture.Complete(op, 8);
            UNIT_ASSERT_VALUES_EQUAL(op.Completions, 2u);
            UNIT_ASSERT_VALUES_EQUAL(op.GetResult(), 8);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Router->GetInflight(), 0u);
        }
    }
}
