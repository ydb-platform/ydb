#include <ydb/services/udf_store/wasm/compile.h>
#include <ydb/services/udf_store/wasm/host.h>
#include <ydb/services/udf_store/wasm/registry_helpers.h>

#include <ydb/library/wasm/api/compartment.h>
#include <ydb/library/wasm/api/pointer.h>

#include <library/cpp/testing/unittest/registar.h>

#include <bit>
#include <cstring>

using namespace NKikimr::NUdfStore::NWasm;
using namespace NYdb::NWasm;
using EAbiValueType = NYdb::NUdfStore::NAbi::EValueType;

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

// Minimal object ABI: create returns monotonic ui64; call returns handle as int64;
// destroy is a no-op. Mirrors host TypeConfig create/call/destroy exports.
constexpr TStringBuf ObjectsWast = R"(
    (module
        (import "env" "memory" (memory i64 8 2097152))
        (global $next (mut i64) (i64.const 1))

        (func $prefix_create (param $context i64) (param $result i64) (param $config i64)
            (local $h i64)
            (local.set $h (global.get $next))
            (global.set $next (i64.add (local.get $h) (i64.const 1)))
            (i32.store8
                (i64.add (local.get $result) (i64.const 2))
                (i32.const 4))
            (i64.store
                (i64.add (local.get $result) (i64.const 8))
                (local.get $h))
        )

        (func $prefix_apply (param $context i64) (param $result i64)
                (param $handle i64) (param $input i64)
            (i32.store8
                (i64.add (local.get $result) (i64.const 2))
                (i32.const 3))
            (i64.store
                (i64.add (local.get $result) (i64.const 8))
                (i64.load (i64.add (local.get $handle) (i64.const 8))))
        )

        (func $prefix_destroy (param $context i64) (param $result i64) (param $handle i64)
            (i32.store8
                (i64.add (local.get $result) (i64.const 2))
                (i32.const 2))
        )

        (export "prefix_create" (func $prefix_create))
        (export "prefix_apply" (func $prefix_apply))
        (export "prefix_destroy" (func $prefix_destroy))
    )
)";

TNamedModuleBytecode MakeNamedLibrary(TStringBuf name, TStringBuf wast) {
    const auto objectCode = CompileModuleObjectCode(wast, EBytecodeFormat::HumanReadable);
    return TNamedModuleBytecode{
        .Name = TString(name),
        .Bytecode = MakeModuleBytecode(wast, objectCode, EBytecodeFormat::HumanReadable),
    };
}

uintptr_t AllocValue(IWebAssemblyCompartment* compartment, const TUnversionedValue& value) {
    const auto offset = compartment->AllocateBytes(sizeof(TUnversionedValue));
    StoreValue(compartment, offset, value);
    return offset;
}

} // namespace

Y_UNIT_TEST_SUITE(TWasmUdfObjectsAbiTest) {
    Y_UNIT_TEST(CreateTwoHandlesAndCall) {
        EnsureUdfHostIntrinsicsRegistered();

        auto compartment = CreateEmptyImage();
        compartment->AddSdk(MakeNamedLibrary("sdk", SdkStubWast).Bytecode);
        TCurrentCompartmentGuard compartmentGuard(compartment.get());

        const auto moduleObjectCode = CompileModuleObjectCode(
            ObjectsWast,
            EBytecodeFormat::HumanReadable);
        AddPrecompiledModule(
            compartment.get(),
            MakeModuleBytecode(ObjectsWast, moduleObjectCode, EBytecodeFormat::HumanReadable),
            "Prefix");

        auto createOnce = [&]() -> ui64 {
            TUnversionedValue config = MakeEmptyValue();
            config.Type = EAbiValueType::String;
            config.Length = 0;
            config.Data.String = nullptr;

            const auto configOffset = AllocValue(compartment.get(), config);
            const auto resultOffset = AllocValue(compartment.get(), MakeEmptyValue());
            InvokeUdfExport(
                compartment.get(),
                "prefix_create",
                /*context*/ 0,
                resultOffset,
                {configOffset});

            const auto created = *PtrFromVM(
                compartment.get(),
                std::bit_cast<TUnversionedValue*>(resultOffset));
            UNIT_ASSERT_EQUAL(created.Type, EAbiValueType::Uint64);
            UNIT_ASSERT(created.Data.Uint64 != 0);
            return created.Data.Uint64;
        };

        const ui64 h1 = createOnce();
        const ui64 h2 = createOnce();
        UNIT_ASSERT_VALUES_UNEQUAL(h1, h2);

        TUnversionedValue handleValue = MakeEmptyValue();
        handleValue.Type = EAbiValueType::Uint64;
        handleValue.Data.Uint64 = h2;
        TUnversionedValue input = MakeEmptyValue();
        input.Type = EAbiValueType::String;
        input.Length = 0;
        input.Data.String = nullptr;

        const auto handleOffset = AllocValue(compartment.get(), handleValue);
        const auto inputOffset = AllocValue(compartment.get(), input);
        const auto callResultOffset = AllocValue(compartment.get(), MakeEmptyValue());
        InvokeUdfExport(
            compartment.get(),
            "prefix_apply",
            /*context*/ 0,
            callResultOffset,
            {handleOffset, inputOffset});

        const auto called = *PtrFromVM(
            compartment.get(),
            std::bit_cast<TUnversionedValue*>(callResultOffset));
        UNIT_ASSERT_EQUAL(called.Type, EAbiValueType::Int64);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui64>(called.Data.Int64), h2);
    }
}
