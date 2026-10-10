#include <ydb/library/pdisk_io/aio.h>
#include <ydb/library/pdisk_io/aio_completion.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/tempfile.h>

#include <array>
#include <cstring>

using namespace NKikimr::NPDisk;
namespace NIoDetail = NKikimr::NPDisk::NDetail;

namespace {

struct TCallback : ICallback {
    size_t Calls = 0;
    EIoResult Result = EIoResult::Unknown;

    void Exec(TAsyncIoOperationResult* result) override {
        ++Calls;
        Result = result->Result;
    }
};

struct TContext {
    TTempFileHandle File = TTempFileHandle::InCurrentDir("aio-completion");
    alignas(4096) std::array<char, 8192> Buffer{};
    std::unique_ptr<IAsyncIoContext> Io;
    IAsyncIoOperation* Operation = nullptr;
    TCallback Callback;
    bool Initialized = false;

    TContext() {
        File.Resize(4096);
        File.Flush();
        Io = CreateAsyncIoContextReal(File.Name(), 1, TDeviceMode::None);
        const auto result = Io->Setup(4, false);
        Initialized = result == EIoResult::Ok;
        UNIT_ASSERT_C(Initialized, "I/O setup failed: " << static_cast<i64>(result));
        Operation = Io->CreateAsyncIoOperation(nullptr, {}, nullptr);
    }

    ~TContext() {
        if (Initialized) {
            Io->Destroy();
        }
        if (Operation) {
            Io->DestroyAsyncIoOperation(Operation);
        }
    }

    EIoResult Complete() {
        const size_t previousCalls = Callback.Calls;
        UNIT_ASSERT(Io->Submit(Operation, &Callback) == EIoResult::Ok);
        TAsyncIoOperationResult event;
        UNIT_ASSERT_VALUES_EQUAL(Io->GetEvents(1, 1, &event, TDuration::Seconds(10)), 1);
        UNIT_ASSERT_VALUES_EQUAL(Callback.Calls, previousCalls + 1);
        UNIT_ASSERT(event.Operation == Operation);
        UNIT_ASSERT(event.Result == Callback.Result);
        return event.Result;
    }
};

} // namespace

Y_UNIT_TEST_SUITE(TAsyncIoCompletion) {
    Y_UNIT_TEST(CompletionRequiresAllBytes) {
        UNIT_ASSERT_VALUES_EQUAL(NIoDetail::CheckIoCompletion(4096, 4096), 4096);
        UNIT_ASSERT_VALUES_EQUAL(NIoDetail::CheckIoCompletion(2048, 4096), -EIO);
        UNIT_ASSERT_VALUES_EQUAL(NIoDetail::CheckIoCompletion(0, 4096), -EIO);
        UNIT_ASSERT_VALUES_EQUAL(NIoDetail::CheckIoCompletion(8192, 4096), -EIO);
        UNIT_ASSERT_VALUES_EQUAL(NIoDetail::CheckIoCompletion(-EIO, 4096), -EIO);
        UNIT_ASSERT_VALUES_EQUAL(NIoDetail::CheckIoCompletion(-ENOSPC, 4096), -ENOSPC);
        UNIT_ASSERT_VALUES_EQUAL(NIoDetail::CheckIoCompletion(0, 0), 0);
    }

    Y_UNIT_TEST(ShortWritesContinueAtNextByte) {
        const char data[] = "abcdefgh";
        TString written;
        size_t calls = 0;
        const auto result = NIoDetail::WriteAll(data, 8, 100,
            [&](const void* part, ui32 size, ui64 offset) -> i64 {
                UNIT_ASSERT_VALUES_EQUAL(offset, 100 + written.size());
                UNIT_ASSERT_VALUES_EQUAL(size, 8 - written.size());
                const ui32 count = std::min<ui32>(size, 3);
                written.append(static_cast<const char*>(part), count);
                ++calls;
                return count;
            });
        UNIT_ASSERT_VALUES_EQUAL(result, 8);
        UNIT_ASSERT_VALUES_EQUAL(written, "abcdefgh");
        UNIT_ASSERT_VALUES_EQUAL(calls, 3);
    }

    Y_UNIT_TEST(ErrorAfterPartialWriteStopsWithoutSkipping) {
        std::array<char, 8192> data{};
        size_t calls = 0;
        const auto result = NIoDetail::WriteAll(data.data(), data.size(), 4096,
            [&](const void* part, ui32 size, ui64 offset) -> i64 {
                ++calls;
                if (calls == 1) {
                    return 512;
                }
                UNIT_ASSERT_VALUES_EQUAL(calls, 2);
                UNIT_ASSERT(part == data.data() + 512);
                UNIT_ASSERT_VALUES_EQUAL(offset, 4608);
                UNIT_ASSERT_VALUES_EQUAL(size, 7680);
                return -ENOSPC;
            });
        UNIT_ASSERT_VALUES_EQUAL(result, -ENOSPC);
        UNIT_ASSERT_VALUES_EQUAL(calls, 2);
    }

    Y_UNIT_TEST(ZeroProgressIsAnError) {
        char data[16]{};
        size_t calls = 0;
        const auto result = NIoDetail::WriteAll(data, sizeof(data), 0,
            [&](const void*, ui32, ui64) -> i64 {
                ++calls;
                return 0;
            });
        UNIT_ASSERT_VALUES_EQUAL(result, -EIO);
        UNIT_ASSERT_VALUES_EQUAL(calls, 1);
    }

    Y_UNIT_TEST(NativeWriteReadAndBarrier) {
        TContext context;
        memset(context.Buffer.data(), 'x', 4096);
        context.Io->PreparePWrite(context.Operation, context.Buffer.data(), 4096, 0);
        UNIT_ASSERT(context.Complete() == EIoResult::Ok);
        context.Io->PreparePRead(context.Operation, nullptr, 0, 0);
        UNIT_ASSERT(context.Complete() == EIoResult::Ok);
        memset(context.Buffer.data(), 0, 4096);
        context.Io->PreparePRead(context.Operation, context.Buffer.data(), 4096, 0);
        UNIT_ASSERT(context.Complete() == EIoResult::Ok);
        UNIT_ASSERT_VALUES_EQUAL(TString(context.Buffer.data(), 4096), TString(4096, 'x'));
    }

#if defined(_linux_)
    Y_UNIT_TEST(NativeShortReadIsAnError) {
        TContext context;
        context.Io->PreparePRead(context.Operation, context.Buffer.data(), 8192, 0);
        UNIT_ASSERT(context.Complete() == EIoResult::IOError);
    }

    Y_UNIT_TEST(NativeReadAtEofIsAnError) {
        TContext context;
        context.Io->PreparePRead(context.Operation, context.Buffer.data(), 4096, 4096);
        UNIT_ASSERT(context.Complete() == EIoResult::IOError);
    }
#else
    Y_UNIT_TEST(NativeWriteErrorReachesCallback) {
        TContext context;
        // Only the worker thread touches this closed handle after submission.
        // This produces a real OS write error without filling a disk or changing
        // process-wide resource limits.
        UNIT_ASSERT(context.Io->GetFileHandle()->Close());
        context.Io->PreparePWrite(context.Operation, context.Buffer.data(), 4096, 0);
        UNIT_ASSERT(context.Complete() == EIoResult::IOError);
        UNIT_ASSERT(context.Io->GetLastErrno() != 0);
    }
#endif
}
