#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/pdisk_io/uring_router.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/tempfile.h>
#include <util/system/file.h>
#include <util/system/event.h>

#include <sys/uio.h>

#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <thread>

using NActors::TActorSystem;
using namespace NKikimr::NPDisk;

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

    void OnDrop() noexcept override {
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

    void OnDrop() noexcept override {
    }
};

// Exercises resubmission of the same fixed-buffer operation after a short
// read. The first read reaches EOF part-way through the registered buffer;
// the callback advances the active iovec window and submits the remainder.
struct TFixedShortRetryOp : TUringOperationBase {
    TUringRouter* Router = nullptr;
    TManualEvent* Event = nullptr;
    std::atomic<int> Callbacks{0};
    std::atomic<int> Drops{0};
    std::atomic<i32> Results[2] = {};
    std::atomic<bool> RetryAccepted{false};

    void OnComplete(TActorSystem*) noexcept override {
        const int callback = Callbacks.fetch_add(1, std::memory_order_relaxed);
        if (callback < 2) {
            Results[callback].store(GetResult(), std::memory_order_relaxed);
        }

        if (callback == 0 && GetResult() > 0 && static_cast<size_t>(GetResult()) < GetOperationBytes()) {
            AdvanceIov(GetResult());
            const bool accepted = Router->Read(this);
            RetryAccepted.store(accepted, std::memory_order_release);
            if (accepted) {
                return;
            }
        }
        Event->Signal();
    }

    void OnDrop() noexcept override {
        Drops.fetch_add(1, std::memory_order_relaxed);
        Event->Signal();
    }
};

// Exercises a short read whose active window contracts from two iovecs to one,
// so the retry uses the router's scalar singleton preparation path.
struct TScatterGatherShortRetryOp : TUringOperationBase {
    TUringRouter* Router = nullptr;
    TManualEvent* Event = nullptr;
    std::atomic<int> Callbacks{0};
    std::atomic<int> Drops{0};
    std::atomic<i32> Results[2] = {};
    std::atomic<bool> RetryAccepted{false};

    void OnComplete(TActorSystem*) noexcept override {
        const int callback = Callbacks.fetch_add(1, std::memory_order_relaxed);
        if (callback < 2) {
            Results[callback].store(GetResult(), std::memory_order_relaxed);
        }

        if (callback == 0 && GetResult() > 0 && static_cast<size_t>(GetResult()) < GetOperationBytes()) {
            AdvanceIov(GetResult());
            const bool accepted = Router->Read(this);
            RetryAccepted.store(accepted, std::memory_order_release);
            if (accepted) {
                return;
            }
        }
        Event->Signal();
    }

    void OnDrop() noexcept override {
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
        router.RegisterFile();
    }
    router.Start();
    if (registerFile) {
        UNIT_ASSERT_C(router.IsFileRegistered(),
            TStringBuilder() << "file registration failed with errno=" << router.GetRegisterFileErrno());
    }

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
    router.RegisterFile();
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

void DoOverloadBeyondQueueDepth(TUringRouterConfig config) {
    config.QueueDepth = 4;
    SKIP_IF_NO_URING(config);
    TTempFile tmp(MakeTempName(nullptr, "uring_test"));
    TFile f(tmp.Name(), CreateAlways | RdWr);
    f.Resize(1 << 20);

    TUringRouter router(f.GetHandle(), nullptr, config);
    router.RegisterFile();
    router.Start();

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
        UNIT_ASSERT(router.Write(&ops[i]));
    }

    for (int i = 0; i < N; ++i) {
        UNIT_ASSERT(events[i].WaitT(TDuration::Seconds(5)));
        UNIT_ASSERT_VALUES_EQUAL(ops[i].GetResult(), (i32)size);
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
    router.RegisterFile();

    struct iovec iovs[2];
    iovs[0].iov_base = writeBuf.Data();
    iovs[0].iov_len = size;
    iovs[1].iov_base = readBuf.Data();
    iovs[1].iov_len = size;
    router.RegisterBuffers(iovs, 2);

    router.Start();
    UNIT_ASSERT_C(router.IsFileRegistered(),
        TStringBuilder() << "file registration failed with errno=" << router.GetRegisterFileErrno());
    UNIT_ASSERT_C(router.AreBuffersRegistered(),
        TStringBuilder() << "buffer registration failed with errno=" << router.GetRegisterBuffersErrno());

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

void DoSubmitDirect(TUringRouterConfig config) {
    SKIP_IF_NO_URING(config);
    TTempFile tmp(MakeTempName(nullptr, "uring_test"));
    TFile f(tmp.Name(), CreateAlways | RdWr);
    f.Resize(1 << 20);

    TUringRouter router(f.GetHandle(), nullptr, config);
    router.RegisterFile();
    router.Start();

    constexpr ui32 size = 4096;
    TAlignedBuf buf(size);
    memset(buf.Data(), 0x5A, size);

    TManualEvent event;
    TTestOp op;
    op.Event = &event;
    PrepareWriteOp(op, buf.Data(), size, 0);
    UNIT_ASSERT(router.Submit(&op));
    UNIT_ASSERT(event.WaitT(TDuration::Seconds(5)));
    UNIT_ASSERT_VALUES_EQUAL(op.GetResult(), (i32)size);

    router.Stop();
}

void DoLargeMultiPageIO(TUringRouterConfig config) {
    SKIP_IF_NO_URING(config);
    TTempFile tmp(MakeTempName(nullptr, "uring_test"));
    TFile f(tmp.Name(), CreateAlways | RdWr);
    constexpr ui32 size = 256 * 1024; // 256 KB
    f.Resize(size);
    TUringRouter router(f.GetHandle(), nullptr, config);
    router.RegisterFile();
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
    router.RegisterFile();
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
    router.RegisterFile();
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
    router.RegisterFile();
    router.Start();

    // The compatibility no-op must remain harmless.
    router.Flush();
    router.Flush();

    // Verify I/O still works after compatibility flush calls.
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
    router.RegisterFile();
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
    router.RegisterFile();
    router.Start();

    constexpr ui32 size = 4096;
    TAlignedBuf buf(size);
    memset(buf.Data(), 0xDD, size);

    // Submit several ops, call the compatibility flush, then immediately stop.
    constexpr int N = 4;
    TTestOp ops[N];
    TManualEvent events[N];
    for (int i = 0; i < N; ++i) {
        ops[i].Event = &events[i];
        PrepareWriteOp(ops[i], buf.Data(), size, 0);
        UNIT_ASSERT(router.Write(&ops[i]));
    }
    router.Flush();

    // Don't wait for completion -- just stop. The I/O thread must drain every
    // accepted operation before shutdown.
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
    router.RegisterFile();
    router.Start();

    constexpr ui32 size = 4096;
    TAlignedBuf buf(size);
    memset(buf.Data(), 0xEE, size);

    // Enqueue operations without calling the compatibility Flush() API.
    constexpr int N = 4;
    TTestOp ops[N];
    TManualEvent events[N];
    for (int i = 0; i < N; ++i) {
        ops[i].Event = &events[i];
        PrepareWriteOp(ops[i], buf.Data(), size, 0);
        UNIT_ASSERT(router.Write(&ops[i]));
    }

    // Stop must drain queued and kernel-submitted operations alike.
    router.Stop();
    for (int i = 0; i < N; ++i) {
        UNIT_ASSERT(events[i].WaitT(TDuration::Seconds(1)));
    }
}

void DoStopAfterIdle(TUringRouterConfig config) {
    SKIP_IF_NO_URING(config);
    TTempFile tmp(MakeTempName(nullptr, "uring_test"));
    TFile f(tmp.Name(), CreateAlways | RdWr);
    f.Resize(1 << 20);

    TUringRouter router(f.GetHandle(), nullptr, config);
    router.RegisterFile();
    router.Start();

    UNIT_ASSERT_VALUES_EQUAL(router.GetInflight(), 0u);
    // Allow the I/O thread to leave its idle spin and park. This covers the
    // public stop-after-idle contract, not a specific internal timeout branch.
    usleep(20000);

    const auto start = std::chrono::steady_clock::now();
    router.Stop();
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);

    UNIT_ASSERT_C(elapsed < std::chrono::seconds(5),
        TStringBuilder() << "idle router stop took " << elapsed.count() << " ms");
    UNIT_ASSERT_VALUES_EQUAL(router.GetInflight(), 0u);
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

    void OnDrop() noexcept override {
    }
};

void DoStopWhileCallbackRunning(TUringRouterConfig config) {
    // Block the sole I/O thread in the first callback while additional
    // accepted operations and the stop sentinel accumulate behind it.
    config.QueueDepth = 1;
    SKIP_IF_NO_URING(config);
    TTempFile tmp(MakeTempName(nullptr, "uring_test"));
    TFile f(tmp.Name(), CreateAlways | RdWr);
    f.Resize(1 << 20);

    TUringRouter router(f.GetHandle(), nullptr, config);
    router.RegisterFile();
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
        UNIT_ASSERT(router.Write(&tailOp));
    }
    UNIT_ASSERT_VALUES_EQUAL(router.GetInflight(), TailOps + 1);

    // Stop must not return while an accepted operation is still inside its
    // completion callback, nor leave the saturated queued tail behind.
    std::atomic<bool> stopReturned{false};
    std::thread stopper([&] {
        router.Stop();
        stopReturned.store(true, std::memory_order_release);
    });
    usleep(20000);
    UNIT_ASSERT(!stopReturned.load(std::memory_order_acquire));
    proceedEvent.Signal();
    stopper.join();
    UNIT_ASSERT(stopReturned.load(std::memory_order_acquire));
    UNIT_ASSERT_VALUES_EQUAL(completions.load(std::memory_order_relaxed), TailOps);
    UNIT_ASSERT_VALUES_EQUAL(drops.load(std::memory_order_relaxed), 0);
    for (const auto& tailOp : tailOps) {
        UNIT_ASSERT_VALUES_EQUAL(tailOp.TerminalCallbacks.load(std::memory_order_relaxed), 1);
    }
    UNIT_ASSERT_VALUES_EQUAL(router.GetInflight(), 0u);
}

void DoDeviceSampleSink(TUringRouterConfig config) {
    SKIP_IF_NO_URING(config);
    TTempFile tmp(MakeTempName(nullptr, "uring_test"));
    TFile f(tmp.Name(), CreateAlways | RdWr);
    f.Resize(1 << 20);

    TUringRouter router(f.GetHandle(), nullptr, config);
    router.RegisterFile();

    TDeviceIoSample sample;
    std::atomic<bool> sampleSeen{false};
    router.SetSampleSink([&](const TDeviceIoSample& value) {
        sample = value;
        sampleSeen.store(true, std::memory_order_release);
    });
    router.Start();

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
    UNIT_ASSERT(router.Write(&op));
    UNIT_ASSERT(event.WaitT(TDuration::Seconds(5)));

    UNIT_ASSERT(callbackSawSample.load(std::memory_order_relaxed));
    UNIT_ASSERT(sample.SubmitCycles != 0);
    UNIT_ASSERT_GE(sample.CompleteCycles, sample.SubmitCycles);
    UNIT_ASSERT_VALUES_EQUAL(sample.Offset, offset);
    UNIT_ASSERT_VALUES_EQUAL(sample.Size, size);
    UNIT_ASSERT(sample.IsWrite);
    router.Stop();
}

void DoFixedShortRetrySampling(TUringRouterConfig config) {
    SKIP_IF_NO_URING(config);
    TTempFile tmp(MakeTempName(nullptr, "uring_test"));
    TFile f(tmp.Name(), CreateAlways | RdWr);

    constexpr ui32 fileSize = 4096;
    constexpr ui32 bufferSize = 8192;
    f.Resize(fileSize);

    TUringRouter router(f.GetHandle(), nullptr, config);
    router.RegisterFile();

    TAlignedBuf buf(bufferSize);
    memset(buf.Data(), 0, bufferSize);
    struct iovec registeredBuffer = {buf.Data(), bufferSize};
    router.RegisterBuffers(&registeredBuffer, 1);

    TDeviceIoSample samples[2];
    std::atomic<int> sampleCount{0};
    router.SetSampleSink([&](const TDeviceIoSample& sample) {
        const int index = sampleCount.fetch_add(1, std::memory_order_relaxed);
        if (index < 2) {
            samples[index] = sample;
        }
    });
    router.Start();
    UNIT_ASSERT_C(router.AreBuffersRegistered(),
        TStringBuilder() << "buffer registration failed with errno=" << router.GetRegisterBuffersErrno());

    TManualEvent event;
    TFixedShortRetryOp op;
    op.Router = &router;
    op.Event = &event;
    UNIT_ASSERT(router.ReadFixed(buf.Data(), bufferSize, 0, /*bufIndex=*/0, &op));
    UNIT_ASSERT(event.WaitT(TDuration::Seconds(5)));
    router.Stop();

    UNIT_ASSERT(op.RetryAccepted.load(std::memory_order_acquire));
    UNIT_ASSERT_VALUES_EQUAL(op.Callbacks.load(std::memory_order_relaxed), 2);
    UNIT_ASSERT_VALUES_EQUAL(op.Drops.load(std::memory_order_relaxed), 0);
    UNIT_ASSERT_VALUES_EQUAL(op.Results[0].load(std::memory_order_relaxed), (i32)fileSize);
    UNIT_ASSERT_VALUES_EQUAL(op.Results[1].load(std::memory_order_relaxed), 0);
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

    TUringRouter router(f.GetHandle(), nullptr, config);
    router.RegisterFile();

    TDeviceIoSample samples[2];
    std::atomic<int> sampleCount{0};
    router.SetSampleSink([&](const TDeviceIoSample& sample) {
        const int index = sampleCount.fetch_add(1, std::memory_order_relaxed);
        if (index < 2) {
            samples[index] = sample;
        }
    });
    router.Start();

    TAlignedBuf first(segmentSize);
    TAlignedBuf second(segmentSize);
    memset(first.Data(), 0, segmentSize);
    memset(second.Data(), 0, segmentSize);

    TManualEvent event;
    TScatterGatherShortRetryOp op;
    op.Router = &router;
    op.Event = &event;
    op.SetOperationType(TUringOperationBase::EREAD);
    op.PrepareScatterGather(2, 0);
    op.AddIov(first.Data(), segmentSize);
    op.AddIov(second.Data(), segmentSize);
    UNIT_ASSERT(router.Read(&op));
    UNIT_ASSERT(event.WaitT(TDuration::Seconds(5)));
    router.Stop();

    UNIT_ASSERT(op.RetryAccepted.load(std::memory_order_acquire));
    UNIT_ASSERT_VALUES_EQUAL(op.Callbacks.load(std::memory_order_relaxed), 2);
    UNIT_ASSERT_VALUES_EQUAL(op.Drops.load(std::memory_order_relaxed), 0);
    UNIT_ASSERT_VALUES_EQUAL(op.Results[0].load(std::memory_order_relaxed), (i32)fileSize);
    UNIT_ASSERT_VALUES_EQUAL(op.Results[1].load(std::memory_order_relaxed), 0);
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

    TUringRouter router(f.GetHandle(), nullptr, config);
    router.RegisterFile();

    constexpr ui32 size = 4096;
    TAlignedBuf buf(size);
    memset(buf.Data(), 0x31, size);

    TManualEvent beforeStartEvent;
    TTestOp beforeStart;
    beforeStart.Event = &beforeStartEvent;
    PrepareWriteOp(beforeStart, buf.Data(), size, 0);
    UNIT_ASSERT(!router.Write(&beforeStart));
    UNIT_ASSERT_VALUES_EQUAL(router.GetInflight(), 0u);

    router.Start();
    UNIT_ASSERT(router.Write(&beforeStart));
    UNIT_ASSERT(beforeStartEvent.WaitT(TDuration::Seconds(5)));

    router.Stop();

    TManualEvent afterStopEvent;
    TTestOp afterStop;
    afterStop.Event = &afterStopEvent;
    PrepareWriteOp(afterStop, buf.Data(), size, 0);
    UNIT_ASSERT(!router.Write(&afterStop));
    UNIT_ASSERT(!afterStopEvent.WaitT(TDuration::MilliSeconds(10)));
    UNIT_ASSERT_VALUES_EQUAL(router.GetInflight(), 0u);
}

void DoWakeAfterIdle(TUringRouterConfig config) {
    SKIP_IF_NO_URING(config);
    TTempFile tmp(MakeTempName(nullptr, "uring_test"));
    TFile f(tmp.Name(), CreateAlways | RdWr);
    f.Resize(1 << 20);

    TUringRouter router(f.GetHandle(), nullptr, config);
    router.RegisterFile();
    router.Start();

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
        UNIT_ASSERT(router.Write(&ops[i]));
        UNIT_ASSERT(events[i].WaitT(TDuration::Seconds(5)));
    }

    router.Stop();
}

void DoMultiProducerConcurrentSubmit(TUringRouterConfig config) {
    config.QueueDepth = 4;
    SKIP_IF_NO_URING(config);
    TTempFile tmp(MakeTempName(nullptr, "uring_test"));
    TFile f(tmp.Name(), CreateAlways | RdWr);
    f.Resize(1 << 20);

    TUringRouter router(f.GetHandle(), nullptr, config);
    router.RegisterFile();
    router.Start();

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
                if (!router.Write(&ops[i])) {
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
    router.Stop();
    UNIT_ASSERT_VALUES_EQUAL(router.GetInflight(), 0u);
}

void DoSubmitStopRace(TUringRouterConfig config) {
    config.QueueDepth = 4;
    SKIP_IF_NO_URING(config);
    TTempFile tmp(MakeTempName(nullptr, "uring_test"));
    TFile f(tmp.Name(), CreateAlways | RdWr);
    f.Resize(1 << 20);

    TUringRouter router(f.GetHandle(), nullptr, config);
    router.RegisterFile();
    router.Start();

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

    // Guarantee that Stop has at least one accepted operation to drain.
    UNIT_ASSERT(router.Write(&ops[0]));
    accepted.store(1, std::memory_order_relaxed);

    TManualEvent go;
    std::thread producers[NumThreads];
    for (auto& producer : producers) {
        producer = std::thread([&] {
            go.WaitI();
            for (;;) {
                const int i = next.fetch_add(1, std::memory_order_relaxed);
                if (i >= N) {
                    break;
                }
                attempted.fetch_add(1, std::memory_order_relaxed);
                if (router.Write(&ops[i])) {
                    accepted.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }

    go.Signal();
    while (attempted.load(std::memory_order_relaxed) < NumThreads) {
        std::this_thread::yield();
    }
    router.Stop();
    for (auto& producer : producers) {
        producer.join();
    }

    int terminalCallbacks = 0;
    for (const auto& op : ops) {
        const int callbacks = op.TerminalCallbacks.load(std::memory_order_relaxed);
        UNIT_ASSERT(callbacks == 0 || callbacks == 1);
        terminalCallbacks += callbacks;
    }
    UNIT_ASSERT_VALUES_EQUAL(terminalCallbacks, accepted.load(std::memory_order_relaxed));
    UNIT_ASSERT_VALUES_EQUAL(completions.load(std::memory_order_relaxed), accepted.load(std::memory_order_relaxed));
    UNIT_ASSERT_VALUES_EQUAL(drops.load(std::memory_order_relaxed), 0);
    UNIT_ASSERT_VALUES_EQUAL(router.GetInflight(), 0u);
}

void DoConcurrentStop(TUringRouterConfig config) {
    SKIP_IF_NO_URING(config);
    TTempFile tmp(MakeTempName(nullptr, "uring_test"));
    TFile f(tmp.Name(), CreateAlways | RdWr);
    f.Resize(1 << 20);

    TUringRouter router(f.GetHandle(), nullptr, config);
    router.RegisterFile();
    router.Start();

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
        UNIT_ASSERT(router.Write(&op));
    }

    TManualEvent go;
    std::thread stopper1([&] { go.WaitI(); router.Stop(); });
    std::thread stopper2([&] { go.WaitI(); router.Stop(); });
    go.Signal();
    stopper1.join();
    stopper2.join();

    UNIT_ASSERT_VALUES_EQUAL(completions.load(std::memory_order_relaxed), N);
    UNIT_ASSERT_VALUES_EQUAL(drops.load(std::memory_order_relaxed), 0);
    for (const auto& op : ops) {
        UNIT_ASSERT_VALUES_EQUAL(op.TerminalCallbacks.load(std::memory_order_relaxed), 1);
    }
    UNIT_ASSERT_VALUES_EQUAL(router.GetInflight(), 0u);
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
    router.RegisterFile();
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
    router.RegisterFile();
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
    router.RegisterFile();
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
        UNIT_ASSERT(!op.IsFixedBuffer());
        UNIT_ASSERT_VALUES_EQUAL(op.GetBufIndex(), 0u);
    }
#endif // __linux__

}

Y_UNIT_TEST_SUITE(TUringRouterTest) {

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

    Y_UNIT_TEST(DoubleStop) {
        DoDoubleStop(DefaultConfig());
    }

    Y_UNIT_TEST(FlushWithNothingPending) {
        DoFlushWithNothingPending(DefaultConfig());
    }

    Y_UNIT_TEST(ErrorResultPropagation) {
        DoErrorResultPropagation(DefaultConfig());
    }

    Y_UNIT_TEST(StopAfterFlush) {
        DoStopAfterFlush(DefaultConfig());
    }

    Y_UNIT_TEST(StopWithoutFlush) {
        DoStopWithoutFlush(DefaultConfig());
    }

    Y_UNIT_TEST(StopAfterIdle) {
        DoStopAfterIdle(DefaultConfig());
    }

    Y_UNIT_TEST(StopWhileCallbackRunning) {
        DoStopWhileCallbackRunning(DefaultConfig());
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

    Y_UNIT_TEST(SubmitStopRace) {
        DoSubmitStopRace(DefaultConfig());
    }

    Y_UNIT_TEST(ConcurrentStop) {
        DoConcurrentStop(DefaultConfig());
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
