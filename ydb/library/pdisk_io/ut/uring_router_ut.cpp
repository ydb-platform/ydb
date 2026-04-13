#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/pdisk_io/uring_router.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/tempfile.h>
#include <util/system/file.h>
#include <util/system/event.h>

#include <sys/uio.h>

#include <unistd.h>

#include <atomic>
#include <cstring>

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
        if (!TUringRouter::Probe(config)) { \
            Cerr << "io_uring not available on this system, skipping test" << Endl; \
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

    // Now Stop() while the callback is still blocked inside OnComplete.
    // Stop() submits a drain stop marker and calls Poller->Join(), which
    // blocks until the callback's WaitT times out and the poller thread exits.
    // Must not crash or deadlock.
    router.Stop();
}

} // anonymous namespace

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

    Y_UNIT_TEST(StopWhileCallbackRunning) {
        DoStopWhileCallbackRunning(NoPollingConfig());
    }
}

Y_UNIT_TEST_SUITE(TUringRouterSQPollTest) {

    Y_UNIT_TEST(CreateAndDestroy) {
        DoCreateAndDestroy(SQPollConfig());
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

    Y_UNIT_TEST(NonZeroOffsets) {
        DoNonZeroOffsets(SQPollConfig());
    }

    Y_UNIT_TEST(DoubleStop) {
        DoDoubleStop(SQPollConfig());
    }

    Y_UNIT_TEST(FlushWithNothingPending) {
        DoFlushWithNothingPending(SQPollConfig());
    }

    Y_UNIT_TEST(ErrorResultPropagation) {
        DoErrorResultPropagation(SQPollConfig());
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
}
