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

// Shared context + two filters in one module (MVP of examples/ctx semantics).
// Handles index into a flat counter table in linear memory.
// Snapshot returns ASCII "a=<n>;b=<m>".
constexpr TStringBuf SharedCtxWast = R"(
    (module
        (import "env" "memory" (memory i64 8 2097152))
        (import "env" "malloc" (func $malloc (param i64) (result i64)))

        (global $next (mut i64) (i64.const 1))
        ;; slot = (handle-1)*16: i64 a_count, i64 b_count
        (global $table (mut i64) (i64.const 0))

        (func $ensure_table
            (if (i64.eqz (global.get $table))
                (then
                    (global.set $table
                        (call $malloc (i64.const 4096))))))

        (func $slot (param $h i64) (result i64)
            (i64.add
                (global.get $table)
                (i64.mul (i64.sub (local.get $h) (i64.const 1)) (i64.const 16))))

        (func $ctx_create (param $context i64) (param $result i64)
            (local $h i64)
            (call $ensure_table)
            (local.set $h (global.get $next))
            (global.set $next (i64.add (local.get $h) (i64.const 1)))
            (i64.store (call $slot (local.get $h)) (i64.const 0))
            (i64.store
                (i64.add (call $slot (local.get $h)) (i64.const 8))
                (i64.const 0))
            (i32.store8
                (i64.add (local.get $result) (i64.const 2))
                (i32.const 4))
            (i64.store
                (i64.add (local.get $result) (i64.const 8))
                (local.get $h))
        )

        (func $filter_a (param $context i64) (param $result i64)
                (param $handle i64) (param $input i64)
            (local $h i64) (local $p i64) (local $c i64)
            (local.set $h (i64.load (i64.add (local.get $handle) (i64.const 8))))
            (local.set $p (call $slot (local.get $h)))
            (local.set $c (i64.add (i64.load (local.get $p)) (i64.const 1)))
            (i64.store (local.get $p) (local.get $c))
            (i32.store8
                (i64.add (local.get $result) (i64.const 2))
                (i32.const 3))
            (i64.store
                (i64.add (local.get $result) (i64.const 8))
                (local.get $c))
        )

        (func $filter_b (param $context i64) (param $result i64)
                (param $handle i64) (param $input i64)
            (local $h i64) (local $p i64) (local $c i64)
            (local.set $h (i64.load (i64.add (local.get $handle) (i64.const 8))))
            (local.set $p (i64.add (call $slot (local.get $h)) (i64.const 8)))
            (local.set $c (i64.add (i64.load (local.get $p)) (i64.const 1)))
            (i64.store (local.get $p) (local.get $c))
            (i32.store8
                (i64.add (local.get $result) (i64.const 2))
                (i32.const 3))
            (i64.store
                (i64.add (local.get $result) (i64.const 8))
                (local.get $c))
        )

        ;; Write "a=X;b=Y" for single-digit counts (test only).
        (func $ctx_snapshot (param $context i64) (param $result i64) (param $handle i64)
            (local $h i64) (local $base i64) (local $buf i64)
            (local $a i64) (local $b i64)
            (local.set $h (i64.load (i64.add (local.get $handle) (i64.const 8))))
            (local.set $base (call $slot (local.get $h)))
            (local.set $a (i64.load (local.get $base)))
            (local.set $b (i64.load (i64.add (local.get $base) (i64.const 8))))
            (local.set $buf (call $malloc (i64.const 8)))
            (i32.store8 (local.get $buf) (i32.const 97))
            (i32.store8 (i64.add (local.get $buf) (i64.const 1)) (i32.const 61))
            (i32.store8
                (i64.add (local.get $buf) (i64.const 2))
                (i32.add (i32.const 48) (i32.wrap_i64 (local.get $a))))
            (i32.store8 (i64.add (local.get $buf) (i64.const 3)) (i32.const 59))
            (i32.store8 (i64.add (local.get $buf) (i64.const 4)) (i32.const 98))
            (i32.store8 (i64.add (local.get $buf) (i64.const 5)) (i32.const 61))
            (i32.store8
                (i64.add (local.get $buf) (i64.const 6))
                (i32.add (i32.const 48) (i32.wrap_i64 (local.get $b))))
            (i32.store8
                (i64.add (local.get $result) (i64.const 2))
                (i32.const 16))
            (i32.store
                (i64.add (local.get $result) (i64.const 4))
                (i32.const 7))
            (i64.store
                (i64.add (local.get $result) (i64.const 8))
                (local.get $buf))
        )

        (export "ctx_create" (func $ctx_create))
        (export "ctx_snapshot" (func $ctx_snapshot))
        (export "filter_a" (func $filter_a))
        (export "filter_b" (func $filter_b))
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

Y_UNIT_TEST_SUITE(TWasmSharedCtxSnapshotTest) {
    Y_UNIT_TEST(TwoFiltersThenSnapshot) {
        EnsureUdfHostIntrinsicsRegistered();

        auto compartment = CreateEmptyImage();
        compartment->AddSdk(MakeNamedLibrary("sdk", SdkStubWast).Bytecode);
        TCurrentCompartmentGuard compartmentGuard(compartment.get());

        const auto moduleObjectCode = CompileModuleObjectCode(
            SharedCtxWast,
            EBytecodeFormat::HumanReadable);
        AddPrecompiledModule(
            compartment.get(),
            MakeModuleBytecode(SharedCtxWast, moduleObjectCode, EBytecodeFormat::HumanReadable),
            "Ctx");

        const auto resultOffset = AllocValue(compartment.get(), MakeEmptyValue());
        InvokeUdfExport(
            compartment.get(),
            "ctx_create",
            /*context*/ 0,
            resultOffset,
            {});
        const auto created = *PtrFromVM(
            compartment.get(),
            std::bit_cast<TUnversionedValue*>(resultOffset));
        UNIT_ASSERT_EQUAL(created.Type, EAbiValueType::Uint64);
        const ui64 handle = created.Data.Uint64;
        UNIT_ASSERT(handle != 0);

        auto runFilter = [&](const char* name) {
            TUnversionedValue handleValue = MakeEmptyValue();
            handleValue.Type = EAbiValueType::Uint64;
            handleValue.Data.Uint64 = handle;
            TUnversionedValue input = MakeEmptyValue();
            input.Type = EAbiValueType::Int64;
            input.Data.Int64 = 0;
            const auto hOff = AllocValue(compartment.get(), handleValue);
            const auto iOff = AllocValue(compartment.get(), input);
            const auto rOff = AllocValue(compartment.get(), MakeEmptyValue());
            InvokeUdfExport(compartment.get(), name, 0, rOff, {hOff, iOff});
        };

        runFilter("filter_a");
        runFilter("filter_a");
        runFilter("filter_b");

        TUnversionedValue handleValue = MakeEmptyValue();
        handleValue.Type = EAbiValueType::Uint64;
        handleValue.Data.Uint64 = handle;
        const auto hOff = AllocValue(compartment.get(), handleValue);
        const auto snapOff = AllocValue(compartment.get(), MakeEmptyValue());
        InvokeUdfExport(compartment.get(), "ctx_snapshot", 0, snapOff, {hOff});

        const auto snap = *PtrFromVM(
            compartment.get(),
            std::bit_cast<TUnversionedValue*>(snapOff));
        UNIT_ASSERT_EQUAL(snap.Type, EAbiValueType::String);
        UNIT_ASSERT_VALUES_EQUAL(snap.Length, 7u);
        const auto* bytes = PtrFromVM(compartment.get(), snap.Data.String, snap.Length);
        const TString text(reinterpret_cast<const char*>(bytes), snap.Length);
        UNIT_ASSERT_VALUES_EQUAL(text, "a=2;b=1");
    }
}
