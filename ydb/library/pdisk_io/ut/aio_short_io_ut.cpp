#include <ydb/library/pdisk_io/aio.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/system/tempfile.h>
#include <util/generic/scope.h>

#include <cstring>
#include <sys/resource.h>

using namespace NKikimr::NPDisk;

namespace {

class TCallback final : public ICallback {
public:
    ui32 Calls = 0;
    EIoResult Result = EIoResult::Unknown;

    void Exec(TAsyncIoOperationResult* result) override {
        ++Calls;
        Result = result->Result;
    }
};

}

Y_UNIT_TEST_SUITE(PDiskAsyncIoShortOperation) {
    Y_UNIT_TEST(ShortWriteIsNotReportedAsSuccess) {
        TTempFile temp(MakeTempName(nullptr, "pdisk_short_write"));
        TFile file(temp.Name(), CreateAlways | RdWr);
        auto context = CreateAsyncIoContextReal(temp.Name(), 0, TDeviceMode::None);
        UNIT_ASSERT(context->Setup(16, false) == EIoResult::Ok);
        alignas(4096) char buffer[8192];
        std::memset(buffer, 0x5a, sizeof(buffer));
        auto* operation = context->CreateAsyncIoOperation(nullptr, {}, nullptr);
        context->PreparePWrite(operation, buffer, sizeof(buffer), 0);

        struct rlimit original;
        UNIT_ASSERT_VALUES_EQUAL(getrlimit(RLIMIT_FSIZE, &original), 0);
        struct rlimit limited = original;
        limited.rlim_cur = 4096;
        UNIT_ASSERT_VALUES_EQUAL(setrlimit(RLIMIT_FSIZE, &limited), 0);
        TCallback callback;
        EIoResult submitted;
        i64 completed;
        TAsyncIoOperationResult result;
        int restored = -1;
        {
            Y_DEFER { restored = setrlimit(RLIMIT_FSIZE, &original); };
            submitted = context->Submit(operation, &callback);
            completed = submitted == EIoResult::Ok
                ? context->GetEvents(1, 1, &result, TDuration::Seconds(10)) : 0;
        }
        context->DestroyAsyncIoOperation(operation);
        const auto destroyed = context->Destroy();
        UNIT_ASSERT_VALUES_EQUAL(restored, 0);
        UNIT_ASSERT(submitted == EIoResult::Ok);
        UNIT_ASSERT_VALUES_EQUAL(completed, 1);
        UNIT_ASSERT_VALUES_EQUAL(callback.Calls, 1);
        UNIT_ASSERT(callback.Result == result.Result);
        UNIT_ASSERT(destroyed == EIoResult::Ok);
        UNIT_ASSERT_VALUES_EQUAL(file.GetLength(), 4096);
        UNIT_ASSERT_C(result.Result == EIoResult::IOError,
            "An incomplete 8192-byte write was reported as successful");
    }

    Y_UNIT_TEST(ShortReadIsNotReportedAsSuccess) {
        TTempFile temp(MakeTempName(nullptr, "pdisk_short_read"));
        TFile file(temp.Name(), CreateAlways | RdWr);
        file.Resize(4096);
        auto context = CreateAsyncIoContextReal(temp.Name(), 0, TDeviceMode::None);
        UNIT_ASSERT(context->Setup(16, false) == EIoResult::Ok);

        // A real direct-I/O read spans the file's EOF. The kernel may modify
        // the buffer beyond the returned byte count; only the error status
        // is part of our contract for this incomplete operation.
        alignas(4096) char buffer[8192];
        std::memset(buffer, 0x5a, sizeof(buffer));
        auto* operation = context->CreateAsyncIoOperation(nullptr, {}, nullptr);
        context->PreparePRead(operation, buffer, sizeof(buffer), 0);
        TCallback callback;
        UNIT_ASSERT(context->Submit(operation, &callback) == EIoResult::Ok);
        TAsyncIoOperationResult result;
        UNIT_ASSERT_VALUES_EQUAL(context->GetEvents(1, 1, &result, TDuration::Seconds(10)), 1);
        UNIT_ASSERT_VALUES_EQUAL(callback.Calls, 1);
        UNIT_ASSERT(callback.Result == result.Result);
        context->DestroyAsyncIoOperation(operation);
        UNIT_ASSERT(context->Destroy() == EIoResult::Ok);
        UNIT_ASSERT_VALUES_EQUAL(file.GetLength(), 4096);
        UNIT_ASSERT_C(result.Result == EIoResult::IOError,
            "An incomplete 8192-byte read was reported as successful");
    }

    Y_UNIT_TEST(FullWriteAndReadSucceedButEofFails) {
        TTempFile temp(MakeTempName(nullptr, "pdisk_full_io"));
        TFile file(temp.Name(), CreateAlways | RdWr);
        auto context = CreateAsyncIoContextReal(temp.Name(), 0, TDeviceMode::None);
        UNIT_ASSERT(context->Setup(16, false) == EIoResult::Ok);
        Y_DEFER { context->Destroy(); };
        alignas(4096) char buffer[8192];
        std::memset(buffer, 0x5a, sizeof(buffer));
        auto* operation = context->CreateAsyncIoOperation(nullptr, {}, nullptr);
        Y_DEFER { context->DestroyAsyncIoOperation(operation); };
        auto complete = [&](EIoResult expected) {
            TCallback callback;
            UNIT_ASSERT(context->Submit(operation, &callback) == EIoResult::Ok);
            TAsyncIoOperationResult result;
            UNIT_ASSERT_VALUES_EQUAL(context->GetEvents(1, 1, &result, TDuration::Seconds(10)), 1);
            UNIT_ASSERT_VALUES_EQUAL(callback.Calls, 1);
            UNIT_ASSERT(result.Result == expected);
            UNIT_ASSERT(callback.Result == expected);
        };
        context->PreparePWrite(operation, buffer, sizeof(buffer), 0);
        complete(EIoResult::Ok);
        UNIT_ASSERT_VALUES_EQUAL(file.GetLength(), sizeof(buffer));
        std::memset(buffer, 0, sizeof(buffer));
        context->PreparePRead(operation, buffer, sizeof(buffer), 0);
        complete(EIoResult::Ok);
        for (char byte : buffer) {
            UNIT_ASSERT_VALUES_EQUAL(byte, char(0x5a));
        }
        context->PreparePRead(operation, buffer, sizeof(buffer), sizeof(buffer));
        complete(EIoResult::IOError);
    }

}
