#include <ydb/services/udf_store/wasm/compile.h>
#include <ydb/services/udf_store/wasm/host.h>
#include <ydb/services/udf_store/wasm/registry_helpers.h>

#include <ydb/library/wasm/api/compartment.h>
#include <ydb/library/wasm/api/pointer.h>

#include <library/cpp/testing/unittest/registar.h>

#include <bit>

using namespace NKikimr::NUdfStore::NWasm;
using namespace NYdb::NWasm;

namespace {

constexpr TStringBuf SdkStubWast = R"(
    (module
        (import "env" "memory" (memory i64 8 2097152))
        (global $heap (mut i64) (i64.const 1024))
        (func $malloc (param $n i64) (result i64)
            (local $p i64)
            (local.set $p (global.get $heap))
            (global.set $heap
                (i64.and
                    (i64.add (i64.add (local.get $p) (local.get $n)) (i64.const 7))
                    (i64.const -8)))
            (local.get $p)
        )
        (func $free (param $p i64))
        (export "malloc" (func $malloc))
        (export "free" (func $free))
    )
)";

constexpr TStringBuf HelpersWast = R"(
    (module
        (func $helpers_scale (param $value i64) (result i64)
            (i64.mul (local.get $value) (i64.const 3))
        )
        (export "helpers_scale" (func $helpers_scale))
    )
)";

constexpr TStringBuf WithHelpersWast = R"(
    (module
        (import "env" "memory" (memory i64 8 2097152))
        (import "helpers" "helpers_scale" (func $helpers_scale (param i64) (result i64)))

        (func $scale (param $context i64) (param $result i64) (param $arg0 i64)
            (i32.store8
                (i64.add (local.get $result) (i64.const 2))
                (i32.const 3))
            (i64.store
                (i64.add (local.get $result) (i64.const 8))
                (call $helpers_scale
                    (i64.load (i64.add (local.get $arg0) (i64.const 8)))))
        )

        (export "scale" (func $scale))
    )
)";

TNamedModuleBytecode MakeNamedLibrary(TStringBuf name, TStringBuf wast)
{
    const auto objectCode = CompileModuleObjectCode(wast, EBytecodeFormat::HumanReadable);
    return TNamedModuleBytecode{
        .Name = TString(name),
        .Bytecode = MakeModuleBytecode(wast, objectCode, EBytecodeFormat::HumanReadable),
    };
}

} // namespace

Y_UNIT_TEST_SUITE(TWasmUdfWithHelpersTest) {
    Y_UNIT_TEST(SdkThenHelpersThenModule) {
        EnsureUdfHostIntrinsicsRegistered();

        // Mirror CreateRegistryCompartment without CreateImageFromSdk's process-wide
        // cache (its static dtor races WAVM teardown under unittest).
        auto compartment = CreateEmptyImage();
        compartment->AddSdk(MakeNamedLibrary("sdk", SdkStubWast).Bytecode);
        AddPrecompiledModule(
            compartment.get(),
            MakeNamedLibrary("helpers", HelpersWast).Bytecode,
            "helpers");

        TCurrentCompartmentGuard compartmentGuard(compartment.get());

        const auto moduleObjectCode = CompileModuleObjectCode(
            WithHelpersWast,
            EBytecodeFormat::HumanReadable);
        AddPrecompiledModule(
            compartment.get(),
            MakeModuleBytecode(WithHelpersWast, moduleObjectCode, EBytecodeFormat::HumanReadable),
            "WithHelpers");

        const auto argOffset = compartment->AllocateBytes(sizeof(TUnversionedValue));
        const auto resultOffset = compartment->AllocateBytes(sizeof(TUnversionedValue));

        TUnversionedValue arg = MakeEmptyValue();
        arg.Type = EAbiValueType::Int64;
        arg.Data.Int64 = 7;
        StoreValue(compartment.get(), argOffset, arg);
        StoreValue(compartment.get(), resultOffset, MakeEmptyValue());

        InvokeUdfExport(
            compartment.get(),
            "scale",
            /*context*/ 0,
            resultOffset,
            {argOffset});

        const auto result = *PtrFromVM(
            compartment.get(),
            std::bit_cast<TUnversionedValue*>(resultOffset));
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(result.Type), static_cast<int>(EAbiValueType::Int64));
        UNIT_ASSERT_VALUES_EQUAL(result.Data.Int64, 21);
    }
}
