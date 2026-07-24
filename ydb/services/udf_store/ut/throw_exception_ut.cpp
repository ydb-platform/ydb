#include <ydb/services/udf_store/wasm/host.h>
#include <ydb/services/udf_store/wasm/registry_helpers.h>

#include <ydb/library/wasm/api/compartment.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/string.h>
#include <util/string/builder.h>

using namespace NKikimr::NUdfStore::NWasm;
using namespace NYdb::NWasm;

namespace {

// Minimal UDF-shaped module: imports host ThrowException and always fails.
constexpr TStringBuf FailUdfWast = R"(
    (module
        (import "env" "memory" (memory i64 1))
        (import "env" "ThrowException" (func $throw (param i64)))

        (data (i64.const 64) "boom-from-wasm\00")

        (func $fail (param $context i64) (param $result i64)
            (call $throw (i64.const 64))
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

} // namespace

Y_UNIT_TEST_SUITE(TWasmUdfThrowExceptionTest) {
    Y_UNIT_TEST(ThrowExceptionBecomesQueryFailureReason) {
        EnsureUdfHostIntrinsicsRegistered();

        auto compartment = CreateEmptyImage();
        TCurrentCompartmentGuard compartmentGuard(compartment.get());
        compartment->AddModule(FailUdfWast);

        TString reason;
        try {
            InvokeUdfExport(
                compartment.get(),
                "fail",
                /*context*/ 0,
                /*result*/ 0,
                /*args*/ {});
            UNIT_FAIL("expected ThrowException from wasm UDF");
        } catch (const std::exception& ex) {
            reason = FormatQueryFailureReason("fail", ex);
        }

        UNIT_ASSERT_C(
            reason.Contains("fail(); ex:"),
            TStringBuilder() << "missing UDF wrapper prefix in: " << reason);
        UNIT_ASSERT_C(
            reason.Contains("Error while executing UDF"),
            TStringBuilder() << "missing host ThrowException prefix in: " << reason);
        UNIT_ASSERT_C(
            reason.Contains("boom-from-wasm"),
            TStringBuilder() << "missing wasm error text in: " << reason);
    }
}
