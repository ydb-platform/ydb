#include <ydb/services/udf_store/wasm/host.h>
#include <ydb/services/udf_store/wasm/registry_helpers.h>

#include <ydb/library/wasm/api/compartment.h>

#include <library/cpp/resource/resource.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/string.h>
#include <util/string/builder.h>

using namespace NKikimr::NUdfStore::NWasm;
using namespace NYdb::NWasm;

namespace {

// Nested wasm frames: fail -> boom_middle -> boom_leaf -> ThrowException.
constexpr TStringBuf FailUdfWast = R"(
    (module
        (import "env" "memory" (memory i64 1))
        (import "env" "ThrowException" (func $throw (param i64)))

        (data (i64.const 64) "boom-from-wasm\00")

        (func $boom_leaf
            (call $throw (i64.const 64))
        )

        (func $boom_middle
            (call $boom_leaf)
        )

        (func $fail (param $context i64) (param $result i64)
            (call $boom_middle)
        )

        (export "fail" (func $fail))
    )
)";

//! Mirrors TWasmUdfFunction::Run → WasmError message that becomes the query
//! failure reason after UdfTerminate.
TString FormatQueryFailureReason(TStringBuf functionName, const std::exception& ex)
{
    return TStringBuilder() << functionName << "(); ex: " << ex.what();
}

TString RunFailAndCaptureReason(IWebAssemblyCompartment* compartment)
{
    TString reason;
    try {
        InvokeUdfExport(
            compartment,
            "fail",
            /*context*/ 0,
            /*result*/ 0,
            /*args*/ {});
        ythrow yexception() << "expected ThrowException from wasm UDF";
    } catch (const std::exception& ex) {
        reason = FormatQueryFailureReason("fail", ex);
    }
    return reason;
}

} // namespace

Y_UNIT_TEST_SUITE(TWasmUdfThrowExceptionTest) {
    Y_UNIT_TEST(ThrowExceptionBecomesQueryFailureReason) {
        EnsureUdfHostIntrinsicsRegistered();

        auto compartment = CreateEmptyImage();
        TCurrentCompartmentGuard compartmentGuard(compartment.get());
        compartment->AddModule(FailUdfWast);

        const TString reason = RunFailAndCaptureReason(compartment.get());

        UNIT_ASSERT_C(
            reason.Contains("fail(); ex:"),
            TStringBuilder() << "missing UDF wrapper prefix in: " << reason);
        UNIT_ASSERT_C(
            reason.Contains("Error while executing UDF"),
            TStringBuilder() << "missing host ThrowException prefix in: " << reason);
        UNIT_ASSERT_C(
            !reason.Contains("host.cpp"),
            TStringBuilder() << "internal host.cpp:line must not appear in: " << reason);
        UNIT_ASSERT_C(
            reason.Contains("boom-from-wasm"),
            TStringBuilder() << "missing wasm error text in: " << reason);
        // User wasm frames keep native IP, module, function, and op/line offset
        // (not stripped to bare names). Host / env frames stay filtered out.
        UNIT_ASSERT_C(
            reason.Contains("0x"),
            TStringBuilder() << "missing frame addresses in: " << reason);
        UNIT_ASSERT_C(
            reason.Contains("wasm!"),
            TStringBuilder() << "missing wasm!Module!func frames in: " << reason);
        UNIT_ASSERT_C(
            reason.Contains("boom_leaf"),
            TStringBuilder() << "missing boom_leaf frame in: " << reason);
        UNIT_ASSERT_C(
            reason.Contains("boom_middle"),
            TStringBuilder() << "missing boom_middle frame in: " << reason);
        UNIT_ASSERT_C(
            reason.Contains("fail"),
            TStringBuilder() << "missing fail frame in: " << reason);
        UNIT_ASSERT_C(
            reason.Contains('+'),
            TStringBuilder() << "missing op/line offsets in: " << reason);
        UNIT_ASSERT_C(
            reason.Contains(" at ") && reason.Contains(".wat:"),
            TStringBuilder() << "missing source path:line in: " << reason);
        UNIT_ASSERT_C(
            !reason.Contains("host!"),
            TStringBuilder() << "host frames must be filtered out: " << reason);
    }

    // Emscripten throw example built with -g (DWARF in .wasm). Expects real
    // main.cpp:line in the call stack, not only synthetic .wat paths.
    Y_UNIT_TEST(ThrowExceptionWasmDwarfMapsToMainCpp) {
        EnsureUdfHostIntrinsicsRegistered();

        const TString wasmBytes = NResource::Find("/throw_with_dwarf.wasm");
        UNIT_ASSERT(!wasmBytes.empty());

        auto compartment = CreateEmptyImage();
        TCurrentCompartmentGuard compartmentGuard(compartment.get());
        compartment->AddModule(TRef::FromString(wasmBytes), "Throw");

        const TString reason = RunFailAndCaptureReason(compartment.get());

        UNIT_ASSERT_C(
            reason.Contains("boom-from-wasm"),
            TStringBuilder() << "missing wasm error text in: " << reason);
        UNIT_ASSERT_C(
            !reason.Contains("host.cpp"),
            TStringBuilder() << "internal host.cpp:line must not appear in: " << reason);
        UNIT_ASSERT_C(
            reason.Contains("fail"),
            TStringBuilder() << "missing fail frame in: " << reason);
        UNIT_ASSERT_C(
            reason.Contains("boom_leaf"),
            TStringBuilder() << "missing boom_leaf frame in: " << reason);
        UNIT_ASSERT_C(
            reason.Contains("boom_middle"),
            TStringBuilder() << "missing boom_middle frame in: " << reason);
        UNIT_ASSERT_C(
            reason.Contains(" at ") && reason.Contains("main.cpp:"),
            TStringBuilder() << "missing main.cpp:line from wasm DWARF in: " << reason);
        UNIT_ASSERT_C(
            !reason.Contains("host!"),
            TStringBuilder() << "host frames must be filtered out: " << reason);
    }
}
