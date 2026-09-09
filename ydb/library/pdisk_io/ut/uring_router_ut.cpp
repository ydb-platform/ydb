#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/pdisk_io/uring_router.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/scope.h>
#include <util/system/tempfile.h>
#include <util/stream/file.h>
#include <util/system/file.h>
#include <util/system/event.h>

#include <sys/uio.h>
<<<<<<< HEAD
=======
#include <sys/file.h>
#include <sys/wait.h>
#include <sys/resource.h>
#include <poll.h>
>>>>>>> 79b4f9ebdea (Fix DDisk/PB shutdown ordering and PDisk restart ownership (#52618))

#include <unistd.h>

#include <atomic>
#include <cstring>
<<<<<<< HEAD
=======
#include <memory>
#include <deque>
#include <vector>
#include <thread>

// Keep liburing after project/system headers that may define conflicting macros.
#include <ydb/library/pdisk_io/uring_router_test_peer.h>
#include <ydb/library/pdisk_io/uring_test_support.h>
>>>>>>> 79b4f9ebdea (Fix DDisk/PB shutdown ordering and PDisk restart ownership (#52618))

using NActors::TActorSystem;
using namespace NKikimr::NPDisk;

namespace {

TUringRouterConfig NoPollingConfig(ui32 queueDepth = 16) {
    return TUringRouterConfig{
        .QueueDepth = queueDepth,
        .SqThreadIdleMs = 100,
        .UseSQPoll = false,
        .UseIOPoll = false,
    };
}

// SQPOLL only (no IOPOLL).  IOPOLL requires a real NVMe block device opened
// with O_DIRECT; regular temp files return -EOPNOTSUPP, so it can't be
// meaningfully unit-tested.
TUringRouterConfig SQPollConfig(ui32 queueDepth = 16) {
    return TUringRouterConfig{
        .QueueDepth = queueDepth,
        .SqThreadIdleMs = 100,
        .UseSQPoll = true,
        .UseIOPoll = false,
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

    void OnDrop() noexcept override {
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

    void OnDrop() noexcept override {
        CountAndMaybeSignal();
    }
};

#define SKIP_IF_NO_URING(config) \
    do { \
        if (!RequireUring(config)) { \
            return; \
        } \
    } while (false)

void AssertSuccess(const std::expected<void, int>& result) {
    UNIT_ASSERT_C(result.has_value(),
        TStringBuilder() << "operation failed with errno=" << result.error());
}

void PrepareWriteOp(TUringOperationBase& op, void* buf, ui32 size, ui64 offset) {
    op.SetOperationType(TUringOperationBase::EWRITE);
    op.PrepareIov(buf, size, offset);
}

void PrepareReadOp(TUringOperationBase& op, void* buf, ui32 size, ui64 offset) {
    op.SetOperationType(TUringOperationBase::EREAD);
    op.PrepareIov(buf, size, offset);
}

void DoCreateAndDestroy(TUringRouterConfig config) {
    SKIP_IF_NO_URING(config);
    TTempFile tmp(MakeTempName(nullptr, "uring_test"));
    TFile f(tmp.Name(), CreateAlways | RdWr);
    f.Resize(1 << 20); // 1 MB
    TUringRouter router(f.GetHandle(), nullptr, config);
    router.Start();
    router.Stop();
}

void DoWriteAndReadBack(TUringRouterConfig config, bool registerFile = true) {
    SKIP_IF_NO_URING(config);
    TTempFile tmp(MakeTempName(nullptr, "uring_test"));
    TFile f(tmp.Name(), CreateAlways | RdWr);
    f.Resize(1 << 20);
    TUringRouter router(f.GetHandle(), nullptr, config);
    if (registerFile) {
        AssertSuccess(router.RegisterFile());
        UNIT_ASSERT(router.IsFileRegistered());
    }
    router.Start();

    constexpr ui32 size = 4096;

    // Write
    TAlignedBuf writeBuf(size);
    memset(writeBuf.Data(), 0xAB, size);

    TManualEvent writeEv;
    TTestOp writeOp;
    writeOp.Event = &writeEv;

    PrepareWriteOp(writeOp, writeBuf.Data(), size, 0);
    UNIT_ASSERT(router.Write(&writeOp));
    router.Flush();
    writeEv.WaitI();
    UNIT_ASSERT_VALUES_EQUAL(writeOp.GetResult(), (i32)size);

    // Read back
    TAlignedBuf readBuf(size);
    memset(readBuf.Data(), 0, size);

    TManualEvent readEv;
    TTestOp readOp;
    readOp.Event = &readEv;

    PrepareReadOp(readOp, readBuf.Data(), size, 0);
    UNIT_ASSERT(router.Read(&readOp));
    router.Flush();
    readEv.WaitI();
    UNIT_ASSERT_VALUES_EQUAL(readOp.GetResult(), (i32)size);
    UNIT_ASSERT(memcmp(writeBuf.Data(), readBuf.Data(), size) == 0);

    router.Stop();
}

void DoMultipleConcurrentOps(TUringRouterConfig config) {
    SKIP_IF_NO_URING(config);
    TTempFile tmp(MakeTempName(nullptr, "uring_test"));
    TFile f(tmp.Name(), CreateAlways | RdWr);
    f.Resize(1 << 20);
    TUringRouter router(f.GetHandle(), nullptr, config);
    AssertSuccess(router.RegisterFile());
    router.Start();

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
            UNIT_ASSERT(router.Write(&ops[i]));
        }
        router.Flush();
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
            UNIT_ASSERT(router.Read(&ops[i]));
        }
        router.Flush();
        allDone.WaitI();

        for (int i = 0; i < N; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(ops[i].GetResult(), (i32)size);
            UNIT_ASSERT(memcmp(writeBufs[i].Data(), readBufs[i].Data(), size) == 0);
        }
    }

    router.Stop();
}

void DoSubmitQueueFull(TUringRouterConfig config) {
    config.QueueDepth = 4; // Very small queue, test-specific
    SKIP_IF_NO_URING(config);
    TTempFile tmp(MakeTempName(nullptr, "uring_test"));
    TFile f(tmp.Name(), CreateAlways | RdWr);
    f.Resize(1 << 20);

    TUringRouter router(f.GetHandle(), nullptr, config);
    AssertSuccess(router.RegisterFile());
    router.Start();

    constexpr ui32 size = 4096;
    TAlignedBuf buf(size);
    memset(buf.Data(), 0, size);

    TTestOp ops[5];
    TManualEvent events[5];
    int submitted = 0;

    for (int i = 0; i < 5; ++i) {
        ops[i].Event = &events[i];
        PrepareWriteOp(ops[i], buf.Data(), size, 0);
        if (router.Write(&ops[i])) {
            ++submitted;
        }
    }

    // At least one should have been rejected (SQ ring size is 4)
    UNIT_ASSERT_LT(submitted, 5);
    UNIT_ASSERT_GE(submitted, 1);

    // Flush and wait for the submitted ones
    router.Flush();
    for (int i = 0; i < submitted; ++i) {
        events[i].WaitI();
    }

    router.Stop();
}

void DoRegisterBuffersAndFixedIO(TUringRouterConfig config) {
    SKIP_IF_NO_URING(config);
    TTempFile tmp(MakeTempName(nullptr, "uring_test"));
    TFile f(tmp.Name(), CreateAlways | RdWr);
    f.Resize(1 << 20);
    TUringRouter router(f.GetHandle(), nullptr, config);

    constexpr ui32 size = 4096;
    TAlignedBuf writeBuf(size);
    TAlignedBuf readBuf(size);
    memset(writeBuf.Data(), 0xEF, size);
    memset(readBuf.Data(), 0, size);

    // Register file and buffers before Start()
    AssertSuccess(router.RegisterFile());

    struct iovec iovs[2];
    iovs[0].iov_base = writeBuf.Data();
    iovs[0].iov_len = size;
    iovs[1].iov_base = readBuf.Data();
    iovs[1].iov_len = size;
    AssertSuccess(router.RegisterBuffers(iovs, 2));

    router.Start();

    // WriteFixed using buffer index 0
    TManualEvent writeEv;
    TTestOp writeOp;
    writeOp.Event = &writeEv;

    UNIT_ASSERT(router.WriteFixed(writeBuf.Data(), size, 0, /*bufIndex=*/0, &writeOp));
    router.Flush();
    writeEv.WaitI();
    UNIT_ASSERT_VALUES_EQUAL(writeOp.GetResult(), (i32)size);

    // ReadFixed using buffer index 1
    TManualEvent readEv;
    TTestOp readOp;
    readOp.Event = &readEv;

    UNIT_ASSERT(router.ReadFixed(readBuf.Data(), size, 0, /*bufIndex=*/1, &readOp));
    router.Flush();
    readEv.WaitI();
    UNIT_ASSERT_VALUES_EQUAL(readOp.GetResult(), (i32)size);
    UNIT_ASSERT(memcmp(writeBuf.Data(), readBuf.Data(), size) == 0);

    router.Stop();
}

void DoSubmitItemsLeft(TUringRouterConfig config) {
    constexpr ui32 queueDepth = 8;
    config.QueueDepth = queueDepth; // test-specific
    SKIP_IF_NO_URING(config);
    TTempFile tmp(MakeTempName(nullptr, "uring_test"));
    TFile f(tmp.Name(), CreateAlways | RdWr);
    f.Resize(1 << 20);

    TUringRouter router(f.GetHandle(), nullptr, config);
    AssertSuccess(router.RegisterFile());
    router.Start();

    // Initially all slots should be available
    UNIT_ASSERT_VALUES_EQUAL(router.SubmitItemsLeft(), queueDepth);

    // Submit a few ops and check the count decreases
    constexpr ui32 size = 4096;
    TAlignedBuf buf(size);
    memset(buf.Data(), 0, size);

    constexpr int N = 3;
    TTestOp ops[N];
    TManualEvent events[N];
    for (int i = 0; i < N; ++i) {
        ops[i].Event = &events[i];
        PrepareWriteOp(ops[i], buf.Data(), size, 0);
        UNIT_ASSERT(router.Write(&ops[i]));
    }

    UNIT_ASSERT_VALUES_EQUAL(router.SubmitItemsLeft(), queueDepth - N);

    // After flush + completion, slots should be reclaimed
    router.Flush();
    for (int i = 0; i < N; ++i) {
        events[i].WaitI();
    }
    // After completions are consumed by the poller, SQ slots are available again.
    // Poll with a timeout instead of a fixed sleep for robustness under load.
    for (int i = 0; i < 1000; ++i) {
        if (router.SubmitItemsLeft() == queueDepth) break;
        usleep(1000);
    }
    UNIT_ASSERT_VALUES_EQUAL(router.SubmitItemsLeft(), queueDepth);

    router.Stop();
}

void DoLargeMultiPageIO(TUringRouterConfig config) {
    SKIP_IF_NO_URING(config);
    TTempFile tmp(MakeTempName(nullptr, "uring_test"));
    TFile f(tmp.Name(), CreateAlways | RdWr);
    constexpr ui32 size = 256 * 1024; // 256 KB
    f.Resize(size);
    TUringRouter router(f.GetHandle(), nullptr, config);
    AssertSuccess(router.RegisterFile());
    router.Start();

    // Write 256K of a pattern
    TAlignedBuf writeBuf(size);
    for (ui32 i = 0; i < size; ++i) {
        static_cast<ui8*>(writeBuf.Data())[i] = (ui8)(i % 251); // prime modulus for pattern
    }

    TManualEvent writeEv;
    TTestOp writeOp;
    writeOp.Event = &writeEv;

    PrepareWriteOp(writeOp, writeBuf.Data(), size, 0);
    UNIT_ASSERT(router.Write(&writeOp));
    router.Flush();
    writeEv.WaitI();
    UNIT_ASSERT_VALUES_EQUAL(writeOp.GetResult(), (i32)size);

    // Read it back
    TAlignedBuf readBuf(size);
    memset(readBuf.Data(), 0, size);

    TManualEvent readEv;
    TTestOp readOp;
    readOp.Event = &readEv;

    PrepareReadOp(readOp, readBuf.Data(), size, 0);
    UNIT_ASSERT(router.Read(&readOp));
    router.Flush();
    readEv.WaitI();
    UNIT_ASSERT_VALUES_EQUAL(readOp.GetResult(), (i32)size);
    UNIT_ASSERT(memcmp(writeBuf.Data(), readBuf.Data(), size) == 0);

    router.Stop();
}

void DoNonZeroOffsets(TUringRouterConfig config) {
    SKIP_IF_NO_URING(config);
    TTempFile tmp(MakeTempName(nullptr, "uring_test"));
    TFile f(tmp.Name(), CreateAlways | RdWr);
    f.Resize(1 << 20);
    TUringRouter router(f.GetHandle(), nullptr, config);
    AssertSuccess(router.RegisterFile());
    router.Start();

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
        UNIT_ASSERT(router.Write(&op));
        router.Flush();
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
        UNIT_ASSERT(router.Read(&op));
        router.Flush();
        ev.WaitI();
        UNIT_ASSERT_VALUES_EQUAL(op.GetResult(), (i32)size);
        UNIT_ASSERT(memcmp(writeBufs[i].Data(), readBuf.Data(), size) == 0);
    }

    router.Stop();
}

void DoDoubleStop(TUringRouterConfig config) {
    SKIP_IF_NO_URING(config);
    TTempFile tmp(MakeTempName(nullptr, "uring_test"));
    TFile f(tmp.Name(), CreateAlways | RdWr);
    f.Resize(1 << 20);
    TUringRouter router(f.GetHandle(), nullptr, config);
    AssertSuccess(router.RegisterFile());
    router.Start();

    // Explicit stop, then destructor calls Stop() again -- must not crash
    router.Stop();
    router.Stop();
    // Destructor will call Stop() a third time
}

void DoFlushWithNothingPending(TUringRouterConfig config) {
    SKIP_IF_NO_URING(config);
    TTempFile tmp(MakeTempName(nullptr, "uring_test"));
    TFile f(tmp.Name(), CreateAlways | RdWr);
    f.Resize(1 << 20);
    TUringRouter router(f.GetHandle(), nullptr, config);
    AssertSuccess(router.RegisterFile());
    router.Start();

    // Flush on an empty ring must not crash or hang
    router.Flush();
    router.Flush();

    // Verify I/O still works after empty flushes
    constexpr ui32 size = 4096;
    TAlignedBuf buf(size);
    memset(buf.Data(), 0x42, size);

    TManualEvent ev;
    TTestOp op;
    op.Event = &ev;

    PrepareWriteOp(op, buf.Data(), size, 0);
    UNIT_ASSERT(router.Write(&op));
    router.Flush();
    ev.WaitI();
    UNIT_ASSERT_VALUES_EQUAL(op.GetResult(), (i32)size);

    router.Stop();
}

void DoErrorResultPropagation(TUringRouterConfig config) {
    SKIP_IF_NO_URING(config);
    TTempFile tmp(MakeTempName(nullptr, "uring_test"));
    TFile f(tmp.Name(), CreateAlways | RdWr);
    // Create a small file (4K) so that I/O at a large offset fails
    constexpr ui32 fileSize = 4096;
    f.Resize(fileSize);

    TUringRouter router(f.GetHandle(), nullptr, config);
    AssertSuccess(router.RegisterFile());
    router.Start();

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
    UNIT_ASSERT(router.Write(&op));
    router.Flush();
    ev.WaitI();
    // The kernel should have rejected this; Result should be negative errno
    UNIT_ASSERT_LT(op.GetResult(), 0);

    router.Stop();
}

void DoStopAfterFlush(TUringRouterConfig config) {
    SKIP_IF_NO_URING(config);
    TTempFile tmp(MakeTempName(nullptr, "uring_test"));
    TFile f(tmp.Name(), CreateAlways | RdWr);
    f.Resize(1 << 20);

    TUringRouter router(f.GetHandle(), nullptr, config);
    AssertSuccess(router.RegisterFile());
    router.Start();

    constexpr ui32 size = 4096;
    TAlignedBuf buf(size);
    memset(buf.Data(), 0xDD, size);

    // Submit several ops, flush, then immediately stop without waiting
    constexpr int N = 4;
    TTestOp ops[N];
    TManualEvent events[N];
    for (int i = 0; i < N; ++i) {
        ops[i].Event = &events[i];
        PrepareWriteOp(ops[i], buf.Data(), size, 0);
        UNIT_ASSERT(router.Write(&ops[i]));
    }
    router.Flush();

    // Don't wait for completion -- just stop. Must not crash or deadlock.
    // Stop submits a drain marker and waits for poller shutdown.
    router.Stop();
    for (int i = 0; i < N; ++i) {
        UNIT_ASSERT(events[i].WaitT(TDuration::Seconds(1)));
    }
}

void DoStopWithoutFlush(TUringRouterConfig config) {
    SKIP_IF_NO_URING(config);
    TTempFile tmp(MakeTempName(nullptr, "uring_test"));
    TFile f(tmp.Name(), CreateAlways | RdWr);
    f.Resize(1 << 20);

    TUringRouter router(f.GetHandle(), nullptr, config);
    AssertSuccess(router.RegisterFile());
    router.Start();

    constexpr ui32 size = 4096;
    TAlignedBuf buf(size);
    memset(buf.Data(), 0xEE, size);

    // Submit SQEs but never flush -- they stay in the userspace ring only
    constexpr int N = 4;
    TTestOp ops[N];
    TManualEvent events[N];
    for (int i = 0; i < N; ++i) {
        ops[i].Event = &events[i];
        PrepareWriteOp(ops[i], buf.Data(), size, 0);
        UNIT_ASSERT(router.Write(&ops[i]));
    }

    // Stop without flush. Must not crash or deadlock.
    // Stop submits the pending SQEs together with a drain marker.
    router.Stop();
    for (int i = 0; i < N; ++i) {
        UNIT_ASSERT(events[i].WaitT(TDuration::Seconds(1)));
    }
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
        // Block inside the callback until proceed is signaled or timeout (200 ms)
        if (ProceedEvent) {
            ProceedEvent->WaitT(TDuration::MilliSeconds(200));
        }
    }

    void OnDrop() noexcept override {
    }
};

void DoStopWhileCallbackRunning(TUringRouterConfig config) {
    SKIP_IF_NO_URING(config);
    TTempFile tmp(MakeTempName(nullptr, "uring_test"));
    TFile f(tmp.Name(), CreateAlways | RdWr);
    f.Resize(1 << 20);

    TUringRouter router(f.GetHandle(), nullptr, config);
    AssertSuccess(router.RegisterFile());
    router.Start();

    constexpr ui32 size = 4096;
    TAlignedBuf buf(size);
    memset(buf.Data(), 0xFF, size);

    TManualEvent enteredEvent;
    TManualEvent proceedEvent;
    TBlockingOp op;
    op.EnteredEvent = &enteredEvent;
    op.ProceedEvent = &proceedEvent;

    PrepareWriteOp(op, buf.Data(), size, 0);
    UNIT_ASSERT(router.Write(&op));
    router.Flush();

    // Wait until the callback is actively running on the poller thread
    enteredEvent.WaitI();

<<<<<<< HEAD
    // Now Stop() while the callback is still blocked inside OnComplete.
    // Stop() submits a drain stop marker and calls Poller->Join(), which
    // blocks until the callback's WaitT times out and the poller thread exits.
    // Must not crash or deadlock.
    router.Stop();
=======
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

void DoSubmitStopAsyncRace(TUringRouterConfig config, bool sync = false) {
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
    if (sync) {
        router->StopSync();
    } else {
        router->StopAsync();
    }
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

void DoConcurrentStopAsync(TUringRouterConfig config, bool sync = false) {
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
    auto stop = [&] { go.WaitI(); if (sync) { router->StopSync(); } else { router->StopAsync(); } };
    std::thread stopper1(stop);
    std::thread stopper2(stop);
    go.Signal();
    stopper1.join();
    stopper2.join();

    router.reset();

    UNIT_ASSERT_VALUES_EQUAL(
        completions.load(std::memory_order_relaxed) + drops.load(std::memory_order_relaxed), N);
    for (const auto& op : ops) {
        UNIT_ASSERT_VALUES_EQUAL(op.TerminalCallbacks.load(std::memory_order_relaxed), 1);
    }
>>>>>>> 79b4f9ebdea (Fix DDisk/PB shutdown ordering and PDisk restart ownership (#52618))
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

    TUringRouter router(f.GetHandle(), nullptr, config);
    AssertSuccess(router.RegisterFile());
    router.Start();

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
    UNIT_ASSERT(router.Write(&writeOp));
    router.Flush();
    writeEv.WaitI();
    UNIT_ASSERT_VALUES_EQUAL(writeOp.GetResult(), (i32)totalSize);

    // Read back into one flat buffer and verify per-segment patterns.
    TAlignedBuf readBuf(totalSize);
    memset(readBuf.Data(), 0, totalSize);

    TManualEvent readEv;
    TTestOp readOp;
    readOp.Event = &readEv;
    PrepareReadOp(readOp, readBuf.Data(), totalSize, 0);
    UNIT_ASSERT(router.Read(&readOp));
    router.Flush();
    readEv.WaitI();
    UNIT_ASSERT_VALUES_EQUAL(readOp.GetResult(), (i32)totalSize);

    for (int i = 0; i < N; ++i) {
        UNIT_ASSERT(memcmp(wBufs[i].Data(),
                           static_cast<ui8*>(readBuf.Data()) + i * segSize,
                           segSize) == 0);
    }

    router.Stop();
}

void DoScatterGatherSingleIovec(TUringRouterConfig config) {
    SKIP_IF_NO_URING(config);
    TTempFile tmp(MakeTempName(nullptr, "uring_test"));
    TFile f(tmp.Name(), CreateAlways | RdWr);
    constexpr ui32 size = 4096;
    f.Resize(size);

    TUringRouter router(f.GetHandle(), nullptr, config);
    AssertSuccess(router.RegisterFile());
    router.Start();

    TAlignedBuf writeBuf(size);
    memset(writeBuf.Data(), 0xBB, size);

    struct iovec iov;
    iov.iov_base = writeBuf.Data();
    iov.iov_len  = size;

    TManualEvent writeEv;
    TTestOp writeOp;
    writeOp.Event = &writeEv;
    PrepareWriteVectored(writeOp, &iov, 1, 0);
    UNIT_ASSERT(router.Write(&writeOp));
    router.Flush();
    writeEv.WaitI();
    UNIT_ASSERT_VALUES_EQUAL(writeOp.GetResult(), (i32)size);

    TAlignedBuf readBuf(size);
    memset(readBuf.Data(), 0, size);

    TManualEvent readEv;
    TTestOp readOp;
    readOp.Event = &readEv;
    PrepareReadOp(readOp, readBuf.Data(), size, 0);
    UNIT_ASSERT(router.Read(&readOp));
    router.Flush();
    readEv.WaitI();
    UNIT_ASSERT_VALUES_EQUAL(readOp.GetResult(), (i32)size);
    UNIT_ASSERT(memcmp(writeBuf.Data(), readBuf.Data(), size) == 0);

    router.Stop();
}

void DoScatterGatherErrorPropagation(TUringRouterConfig config) {
    SKIP_IF_NO_URING(config);
    TTempFile tmp(MakeTempName(nullptr, "uring_test"));
    TFile f(tmp.Name(), CreateAlways | RdWr);
    f.Resize(4096);

    TUringRouter router(f.GetHandle(), nullptr, config);
    AssertSuccess(router.RegisterFile());
    router.Start();

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
    UNIT_ASSERT(router.Write(&op));
    router.Flush();
    ev.WaitI();
    UNIT_ASSERT_LT(op.GetResult(), 0);

    router.Stop();
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
    }
#endif // __linux__

}

Y_UNIT_TEST_SUITE(TUringRouterTest) {

    Y_UNIT_TEST(CreateAndDestroy) {
        DoCreateAndDestroy(NoPollingConfig());
    }

    Y_UNIT_TEST(WriteAndReadBack) {
        DoWriteAndReadBack(NoPollingConfig());
    }

    Y_UNIT_TEST(WriteAndReadBackNoFixedFile) {
        DoWriteAndReadBack(NoPollingConfig(), /*registerFile=*/false);
    }

    Y_UNIT_TEST(MultipleConcurrentOps) {
        DoMultipleConcurrentOps(NoPollingConfig());
    }

    Y_UNIT_TEST(SubmitQueueFull) {
        DoSubmitQueueFull(NoPollingConfig());
    }

    Y_UNIT_TEST(RegisterBuffersAndFixedIO) {
        DoRegisterBuffersAndFixedIO(NoPollingConfig());
    }

    Y_UNIT_TEST(SubmitItemsLeft) {
        DoSubmitItemsLeft(NoPollingConfig());
    }

    Y_UNIT_TEST(LargeMultiPageIO) {
        DoLargeMultiPageIO(NoPollingConfig());
    }

    Y_UNIT_TEST(NonZeroOffsets) {
        DoNonZeroOffsets(NoPollingConfig());
    }

    Y_UNIT_TEST(DoubleStop) {
        DoDoubleStop(NoPollingConfig());
    }

    Y_UNIT_TEST(FlushWithNothingPending) {
        DoFlushWithNothingPending(NoPollingConfig());
    }

    Y_UNIT_TEST(ErrorResultPropagation) {
        DoErrorResultPropagation(NoPollingConfig());
    }

    Y_UNIT_TEST(StopAfterFlush) {
        DoStopAfterFlush(NoPollingConfig());
    }

    Y_UNIT_TEST(StopWithoutFlush) {
        DoStopWithoutFlush(NoPollingConfig());
    }

<<<<<<< HEAD
    Y_UNIT_TEST(StopWhileCallbackRunning) {
        DoStopWhileCallbackRunning(NoPollingConfig());
=======
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

    Y_UNIT_TEST(SubmitStopSyncRaceWithSurvivingClients) {
        DoSubmitStopAsyncRace(DefaultConfig(), true);
    }

    Y_UNIT_TEST(ConcurrentStopSync) {
        DoConcurrentStopAsync(DefaultConfig(), true);
    }

    Y_UNIT_TEST(SubmitStopAsyncRace) {
        DoSubmitStopAsyncRace(DefaultConfig());
    }

    Y_UNIT_TEST(ConcurrentStopAsync) {
        DoConcurrentStopAsync(DefaultConfig());
>>>>>>> 79b4f9ebdea (Fix DDisk/PB shutdown ordering and PDisk restart ownership (#52618))
    }

    Y_UNIT_TEST(ScatterGatherWriteReadBack) {
        DoScatterGatherWriteReadBack(NoPollingConfig());
    }

    Y_UNIT_TEST(ScatterGatherSingleIovec) {
        DoScatterGatherSingleIovec(NoPollingConfig());
    }

    Y_UNIT_TEST(ScatterGatherErrorPropagation) {
        DoScatterGatherErrorPropagation(NoPollingConfig());
    }
}

Y_UNIT_TEST_SUITE(TUringRouterSQPollTest) {

<<<<<<< HEAD
    Y_UNIT_TEST(CreateAndDestroy) {
        DoCreateAndDestroy(SQPollConfig());
=======
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
    bool HoldControls = false;
    std::function<void()> OnBackoff;
    std::function<void()> OnWait;
    std::function<std::chrono::steady_clock::time_point()> Clock;
    std::chrono::steady_clock::time_point Now() const override {
        return Clock ? Clock() : IUringRouterBackend::Now();
    }

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
>>>>>>> 79b4f9ebdea (Fix DDisk/PB shutdown ordering and PDisk restart ownership (#52618))
    }

    Y_UNIT_TEST(WriteAndReadBack) {
        DoWriteAndReadBack(SQPollConfig());
    }

    // No WriteAndReadBackNoFixedFile for SQPOLL: on kernel 5.4 the SQPOLL
    // thread cannot access unregistered fds (returns -EBADF).

    Y_UNIT_TEST(MultipleConcurrentOps) {
        DoMultipleConcurrentOps(SQPollConfig());
    }

    Y_UNIT_TEST(SubmitQueueFull) {
        DoSubmitQueueFull(SQPollConfig());
    }

    Y_UNIT_TEST(RegisterBuffersAndFixedIO) {
        DoRegisterBuffersAndFixedIO(SQPollConfig());
    }

    Y_UNIT_TEST(SubmitItemsLeft) {
        DoSubmitItemsLeft(SQPollConfig());
    }

    Y_UNIT_TEST(LargeMultiPageIO) {
        DoLargeMultiPageIO(SQPollConfig());
    }

<<<<<<< HEAD
    Y_UNIT_TEST(NonZeroOffsets) {
        DoNonZeroOffsets(SQPollConfig());
=======
    void ConsumePublished(unsigned count) {
        Y_ABORT_UNLESS(count <= SqTail - SqHead);
        for (unsigned index = 0; index < count; ++index) {
            const auto submission = Snapshot(SqHead++);
            Stats->Consumed.push_back(submission);
            if (submission.Sqe.opcode == IORING_OP_NOP || submission.Sqe.opcode == IORING_OP_POLL_ADD) {
                if (!HoldControls) {
                    PushCqe(submission.Sqe.user_data,
                        Next(ControlResults, submission.Sqe.opcode == IORING_OP_POLL_ADD ? POLLIN : 0));
                }
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
>>>>>>> 79b4f9ebdea (Fix DDisk/PB shutdown ordering and PDisk restart ownership (#52618))
    }

    Y_UNIT_TEST(DoubleStop) {
        DoDoubleStop(SQPollConfig());
    }

    Y_UNIT_TEST(FlushWithNothingPending) {
        DoFlushWithNothingPending(SQPollConfig());
    }

<<<<<<< HEAD
    Y_UNIT_TEST(ErrorResultPropagation) {
        DoErrorResultPropagation(SQPollConfig());
=======
    int WaitCqeTimeout(io_uring*, io_uring_cqe** cqe, __kernel_timespec*) override {
        ++Stats->WaitCalls;
        if (OnWait) { OnWait(); }
        *cqe = nullptr;
        const int result = Next(WaitResults, CqHead == CqTail ? -ETIME : 0);
        if (!result) {
            *cqe = &Cqes[CqHead & Ring->cq.ring_mask];
        }
        return result;
>>>>>>> 79b4f9ebdea (Fix DDisk/PB shutdown ordering and PDisk restart ownership (#52618))
    }

    Y_UNIT_TEST(StopAfterFlush) {
        DoStopAfterFlush(SQPollConfig());
    }

    Y_UNIT_TEST(StopWithoutFlush) {
        DoStopWithoutFlush(SQPollConfig());
    }

    Y_UNIT_TEST(StopWhileCallbackRunning) {
        DoStopWhileCallbackRunning(SQPollConfig());
    }

<<<<<<< HEAD
    Y_UNIT_TEST(ScatterGatherWriteReadBack) {
        DoScatterGatherWriteReadBack(SQPollConfig());
=======
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
    Y_UNIT_TEST(StopSyncWaitsForEveryPublisherPhase) {
        for (ui32 phase = 0; phase != 3; ++phase) {
            auto backend = std::make_unique<TScriptedUringBackend>();
            backend->AutoComplete = true;
            auto stats = backend->Stats;
            auto router = TUringRouterTestPeer::Create(std::move(backend));
            TTempFile tmp(MakeTempName(nullptr, "uring_publisher_stop"));
            TFile file(tmp.Name(), CreateAlways | RdWr);
            TUringRouterTestPeer::SetFd(*router, DupOwned(file));
            TManualEvent entered, release, waiting;
            std::atomic<bool> returned = false;
            NUringPrivate::TRouterHooks hooks;
            auto gate = [&] { entered.Signal(); release.WaitI(); };
            if (phase == 0) { hooks.AfterAdmission = gate; }
            if (phase == 1) { hooks.AfterPublication = gate; }
            if (phase == 2) { hooks.AfterWake = gate; }
            hooks.WaitingForPublishers = [&] { waiting.Signal(); };
            TUringRouterTestPeer::SetHooks(*router, std::move(hooks));
            router->Start();
            TScriptedOp op;
            char buffer[4096] = {};
            PrepareWriteOp(op, buffer, sizeof(buffer), 0);
            bool accepted = false;
            std::thread producer([&] { accepted = router->Write(&op); });
            std::thread stopper;
            Y_DEFER {
                release.Signal();
                if (producer.joinable()) { producer.join(); }
                if (stopper.joinable()) { stopper.join(); }
            };
            entered.WaitI();
            stopper = std::thread([&] { router->StopSync(); returned.store(true); });
            waiting.WaitI();
            const bool premature = returned.load();
            release.Signal();
            producer.join();
            stopper.join();
            UNIT_ASSERT(!premature);
            UNIT_ASSERT(accepted);
            UNIT_ASSERT_VALUES_EQUAL(router->GetInflight(), 0);
            UNIT_ASSERT_VALUES_EQUAL(op.Completions + op.Drops, 1);
            UNIT_ASSERT(TUringRouterTestPeer::Retired(*router));
            UNIT_ASSERT(fcntl(TUringRouterTestPeer::WakeFd(*router), F_GETFD) >= 0);
            UNIT_ASSERT_VALUES_EQUAL(stats->ExitCalls, 1);
            TScriptedOp rejected;
            PrepareWriteOp(rejected, buffer, sizeof(buffer), 0);
            UNIT_ASSERT(!router->Write(&rejected));
            UNIT_ASSERT_VALUES_EQUAL(rejected.Completions + rejected.Drops, 0);
        }
    }

    Y_UNIT_TEST(BrokenUpgradeWinsFinalStopCasWithConcurrentStopSync) {
        auto backend = std::make_unique<TScriptedUringBackend>();
        auto stats = backend->Stats;
        auto router = TUringRouterTestPeer::Create(std::move(backend));
        TManualEvent loaded, release;
        NUringPrivate::TRouterHooks hooks;
        hooks.BeforeFinalStopCas = [&] { loaded.Signal(); release.WaitI(); };
        TUringRouterTestPeer::SetHooks(*router, std::move(hooks));
        router->Start();
        std::thread first([&] { router->StopSync(); });
        std::thread second;
        Y_DEFER {
            release.Signal();
            if (first.joinable()) { first.join(); }
            if (second.joinable()) { second.join(); }
        };
        loaded.WaitI();
        router->StopAsync(true);
        second = std::thread([&] { router->StopSync(); });
        release.Signal();
        first.join();
        second.join();
        UNIT_ASSERT(TUringRouterTestPeer::State(*router) == EUringRouterState::StoppedBroken);
        UNIT_ASSERT(TUringRouterTestPeer::Retired(*router));
        UNIT_ASSERT_VALUES_EQUAL(stats->ExitCalls, 1);
    }

    Y_UNIT_TEST(StopSyncReleasesDeviceLockWithRetainedClient) {
        TTempFile tmp(MakeTempName(nullptr, "uring_retained_lock"));
        TFile file(tmp.Name(), CreateAlways | RdWr);
        UNIT_ASSERT_VALUES_EQUAL(flock(file.GetHandle(), LOCK_EX | LOCK_NB), 0);
        auto backend = std::make_unique<TScriptedUringBackend>();
        auto stats = backend->Stats;
        std::shared_ptr<TUringRouter> router = TUringRouterTestPeer::Create(std::move(backend));
        TUringRouterTestPeer::SetFd(*router, DupOwned(file));
        file.Close();
        std::shared_ptr<IUringRouterClient> retained = router;
        TFile replacement(tmp.Name(), OpenExisting | RdWr);
        UNIT_ASSERT_VALUES_EQUAL(flock(replacement.GetHandle(), LOCK_EX | LOCK_NB), -1);
        router->StopSync();
        UNIT_ASSERT_VALUES_EQUAL(stats->ExitCalls, 1);
        UNIT_ASSERT_VALUES_EQUAL(flock(replacement.GetHandle(), LOCK_EX | LOCK_NB), 0);
        router->StopSync();
        UNIT_ASSERT_VALUES_EQUAL(stats->ExitCalls, 1);
        router.reset();
        TScriptedOp rejected;
        char buffer[4096] = {};
        PrepareWriteOp(rejected, buffer, sizeof(buffer), 0);
        UNIT_ASSERT(!retained->Write(&rejected));
        UNIT_ASSERT_VALUES_EQUAL(rejected.Completions + rejected.Drops, 0);
    }

    Y_UNIT_TEST(FatalStopAbortsBeforeReleasingUnresolvedResourcesInEveryPhase) {
        for (ui32 phase = 0; phase != 5; ++phase) {
            TTempFile diagnostic(MakeTempName(nullptr, "uring_fatal_deadline"));
            const pid_t pid = fork();
            UNIT_ASSERT(pid >= 0);
            if (!pid) {
                const rlimit noCore{0, 0};
                setrlimit(RLIMIT_CORE, &noCore);
                alarm(5);
                TFile output(diagnostic.Name(), CreateAlways | WrOnly);
                Y_ABORT_UNLESS(dup2(output.GetHandle(), STDERR_FILENO) >= 0);
                TScriptedRouter fixture;
                if (phase != 4) {
                    fixture.Initialize();
                }
                TScriptedOp op;
                char buffer[4096] = {};
                if (phase < 2) {
                    PrepareWriteOp(op, buffer, sizeof(buffer), 0);
                    Y_ABORT_UNLESS(fixture.Router->Write(&op));
                    if (phase == 1) {
                        fixture.Backend->SubmitResults = {{-EBADF, 0}};
                    }
                    fixture.Issue();
                } else {
                    fixture.Backend->HoldControls = true;
                    if (phase == 2) {
                        TUringRouterTestPeer::Park(*fixture.Router);
                    }
                }
                fixture.Backend->PeekResults = {-EBADF};
                if (phase != 3 && phase != 4) {
                    TUringRouterTestPeer::Reap(*fixture.Router);
                }
                unsigned clockCalls = 0;
                fixture.Backend->Clock = [&] {
                    Y_ABORT_UNLESS(fixture.Backend->Stats->ExitCalls == 0);
                    Y_ABORT_UNLESS(op.Completions + op.Drops == 0);
                    const unsigned millis = clockCalls++ == 0 ? 0 : clockCalls == 2 ? 199 : 200;
                    Cerr << "fatal phase=" << phase << " elapsed=" << millis << Endl;
                    return std::chrono::steady_clock::time_point{} + std::chrono::milliseconds(millis);
                };
                if (phase == 4) {
                    // Force a fatal published stop marker through the public issuer path.
                    fixture.Backend->PeekResults.clear();
                    fixture.Backend->SubmitResults = {{-EBADF, 0}};
                    fixture.Router->Start();
                    fixture.Router->StopSync();
                } else {
                    fixture.Router->StopAsync();
                    TUringRouterTestPeer::Stop(*fixture.Router);
                }
                _exit(1);
            }
            int status = 0;
            UNIT_ASSERT_VALUES_EQUAL(waitpid(pid, &status, 0), pid);
            UNIT_ASSERT(WIFSIGNALED(status));
            UNIT_ASSERT_VALUES_EQUAL(WTERMSIG(status), SIGABRT);
            const auto output = TFileInput(diagnostic.Name()).ReadAll();
            UNIT_ASSERT_STRING_CONTAINS(output, "elapsed=199");
            UNIT_ASSERT_STRING_CONTAINS(output, "elapsed=200");
            UNIT_ASSERT_STRING_CONTAINS(output, "io_uring fatal shutdown timed out:");
        }
    }

    Y_UNIT_TEST(FatalStopAcceptsLateCompletionWithinDrainDeadline) {
        TScriptedRouter fixture;
        fixture.Initialize();
        TScriptedOp op;
        char buffer[4096] = {};
        PrepareWriteOp(op, buffer, sizeof(buffer), 0);
        UNIT_ASSERT(fixture.Router->Write(&op));
        fixture.Issue();
        fixture.Backend->PeekResults = {-EBADF};
        TUringRouterTestPeer::Reap(*fixture.Router);
        bool completed = false;
        fixture.Backend->OnBackoff = [&] {
            if (!std::exchange(completed, true)) {
                fixture.Backend->Complete(op, sizeof(buffer));
            }
        };
        TUringRouterTestPeer::Stop(*fixture.Router);
        UNIT_ASSERT_VALUES_EQUAL(op.Completions, 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Router->GetInflight(), 0);
    }

    Y_UNIT_TEST(HealthyDrainHasNoFatalDeadline) {
        TScriptedRouter fixture;
        fixture.Initialize();
        TScriptedOp op;
        char buffer[4096] = {};
        PrepareWriteOp(op, buffer, sizeof(buffer), 0);
        UNIT_ASSERT(fixture.Router->Write(&op));
        fixture.Issue();
        ui32 elapsed = 0;
        ui32 clockCalls = 0;
        fixture.Backend->Clock = [&] {
            ++clockCalls;
            return std::chrono::steady_clock::time_point{} + std::chrono::milliseconds(elapsed);
        };
        fixture.Backend->OnWait = [&] {
            elapsed += 100;
            if (elapsed == 500) { fixture.Backend->Complete(op, sizeof(buffer)); }
        };
        fixture.Router->StopAsync();
        TUringRouterTestPeer::Stop(*fixture.Router);
        UNIT_ASSERT_VALUES_EQUAL(elapsed, 500);
        UNIT_ASSERT_VALUES_EQUAL(clockCalls, 0);
        UNIT_ASSERT_VALUES_EQUAL(op.Completions, 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Router->GetInflight(), 0);
    }

    Y_UNIT_TEST(FatalDrainRetiresLateDataAndControlAt199Milliseconds) {
        for (ui32 phase = 0; phase != 4; ++phase) {
            TScriptedRouter fixture;
            fixture.Initialize();
            TScriptedOp op;
            char buffer[4096] = {};
            if (phase < 2) {
                PrepareWriteOp(op, buffer, sizeof(buffer), 0);
                UNIT_ASSERT(fixture.Router->Write(&op));
                if (phase == 1) { fixture.Backend->SubmitResults = {{-EBADF, 0}}; }
                fixture.Issue();
            } else {
                fixture.Backend->HoldControls = true;
                if (phase == 2) { TUringRouterTestPeer::Park(*fixture.Router); }
            }
            fixture.Backend->PeekResults = {-EBADF};
            if (phase != 3) { TUringRouterTestPeer::Reap(*fixture.Router); }
            unsigned elapsed = 0;
            bool deadlineStarted = false;
            fixture.Backend->Clock = [&] {
                deadlineStarted = true;
                return std::chrono::steady_clock::time_point{} + std::chrono::milliseconds(elapsed);
            };
            bool retired = false;
            fixture.Backend->OnBackoff = [&] {
                if (!deadlineStarted) { return; }
                if (std::exchange(retired, true)) { return; }
                elapsed = 199;
                if (phase == 1) {
                    fixture.Backend->ConsumePublished(TUringRouterTestPeer::Ready(*fixture.Router));
                }
                if (phase < 2) {
                    fixture.Backend->Complete(op, sizeof(buffer));
                } else {
                    const auto& marker = fixture.Backend->Stats->Consumed.back().Sqe;
                    fixture.Backend->PushCqe(marker.user_data, phase == 2 ? POLLIN : 0);
                }
            };
            fixture.Router->StopAsync();
            TUringRouterTestPeer::Stop(*fixture.Router);
            UNIT_ASSERT(retired);
            UNIT_ASSERT_VALUES_EQUAL(elapsed, 199);
            UNIT_ASSERT_VALUES_EQUAL(op.Completions, phase < 2 ? 1 : 0);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Router->GetInflight(), 0);
            UNIT_ASSERT_VALUES_EQUAL(TUringRouterTestPeer::Ready(*fixture.Router), 0);
        }
    }

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
>>>>>>> 79b4f9ebdea (Fix DDisk/PB shutdown ordering and PDisk restart ownership (#52618))
    }

    Y_UNIT_TEST(ScatterGatherSingleIovec) {
        DoScatterGatherSingleIovec(SQPollConfig());
    }

    Y_UNIT_TEST(ScatterGatherErrorPropagation) {
        DoScatterGatherErrorPropagation(SQPollConfig());
    }
}
