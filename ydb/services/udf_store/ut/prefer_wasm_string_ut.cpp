#include <ydb/services/udf_store/wasm/compartment_manager.h>
#include <ydb/services/udf_store/wasm/compile.h>
#include <ydb/services/udf_store/wasm/prefer_wasm_stats.h>
#include <ydb/services/udf_store/wasm/registry_helpers.h>
#include <ydb/services/udf_store/wasm/wasm_string.h>

#include <ydb/library/wasm/api/allocation_registry.h>
#include <ydb/library/wasm/api/compartment.h>
#include <ydb/library/wasm/api/data_transfer.h>

#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/mkql_string_util.h>

#include <library/cpp/testing/unittest/registar.h>

#include <bit>

using namespace NKikimr::NUdfStore::NWasm;
using namespace NYdb::NWasm;

//! Refcount release emitted by the MiniKQL LLVM codegen (mkql_computation_node_codegen).
extern "C" void DeleteString(void* strData);

using NYql::NUdf::TStringRef;
using NYql::NUdf::TUnboxedValue;
using NYql::NUdf::TUnboxedValuePod;
using EAbiValueType = NYdb::NUdfStore::NAbi::EValueType;

namespace {

//! Bump allocator over 8 pages of linear memory: enough for a few blobs, and it
//! keeps the test free of the real runtime image (see with_helpers_ut.cpp).
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

std::unique_ptr<IWebAssemblyCompartment> MakeAllocatingCompartment() {
    const auto objectCode = CompileModuleObjectCode(SdkStubWast, EBytecodeFormat::HumanReadable);
    auto compartment = CreateEmptyImage();
    compartment->AddSdk(MakeModuleBytecode(SdkStubWast, objectCode, EBytecodeFormat::HumanReadable));
    return compartment;
}

TQueryCompartmentHandlePtr MakeQueryCompartment(ui64 generation) {
    auto handle = std::make_shared<TQueryCompartmentHandle>();
    handle->Compartment = MakeAllocatingCompartment();
    handle->Generation = generation;
    return handle;
}

TString MakeBlob(size_t size) {
    TString blob = TString::Uninitialized(size);
    for (size_t i = 0; i < size; ++i) {
        blob[i] = 'a' + (i % 26);
    }
    return blob;
}

TStringBuf ReadGuestBytes(IWebAssemblyCompartment* compartment, const TUnversionedValue& value) {
    const auto offset = std::bit_cast<uintptr_t>(value.Data.String);
    return TStringBuf(
        static_cast<const char*>(compartment->GetHostPointer(offset, value.Length)),
        value.Length);
}

struct TFilledArg {
    TUnversionedValue Value;
    TCopyGuard Guard;
};

TFilledArg FillArg(IWebAssemblyCompartment* compartment, const TUnboxedValuePod& arg) {
    TFilledArg filled;
    TWasmStringValue::FillAbiStringArg(compartment, arg, filled.Value, filled.Guard);
    return filled;
}

constexpr size_t BlobSize = 4096;

} // namespace

Y_UNIT_TEST_SUITE(TPreferWasmStringTest) {

//! The whole point of PreferWasm: the scan writes the column into linear memory
//! once, and every UDF call reuses those bytes instead of copying them again.
Y_UNIT_TEST(ResidentColumnIsReusedByUdfArg) {
    NKikimr::NMiniKQL::TScopedAlloc alloc(__LOCATION__);
    auto handle = MakeQueryCompartment(/*generation*/ 1);
    TCurrentQueryCompartmentGuard queryGuard(handle.get());
    TPreferWasmStats::Instance().Reset();

    const TString blob = MakeBlob(BlobSize);
    TUnboxedValue value(TWasmStringValue::MakePreferWasm(TStringRef(blob.data(), blob.size())));

    auto stats = TPreferWasmStats::Instance().GetSnapshot();
    UNIT_ASSERT_VALUES_EQUAL(stats.MaterializedInWasm, 1);
    UNIT_ASSERT_VALUES_EQUAL(stats.FallbackNoCompartment, 0);

    // Three UDF calls over the same column value: all of them reuse the bytes.
    for (int call = 0; call < 3; ++call) {
        const auto filled = FillArg(handle->Compartment.get(), value);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(filled.Value.Type), static_cast<int>(EAbiValueType::String));
        UNIT_ASSERT_VALUES_EQUAL(filled.Value.Length, BlobSize);
        UNIT_ASSERT_VALUES_EQUAL(ReadGuestBytes(handle->Compartment.get(), filled.Value), TStringBuf(blob));
    }

    stats = TPreferWasmStats::Instance().GetSnapshot();
    UNIT_ASSERT_VALUES_EQUAL(stats.ResidentReused, 3);
    UNIT_ASSERT_VALUES_EQUAL(stats.CopiedIntoCompartment, 0);

    UNIT_ASSERT_VALUES_EQUAL(TWasmAllocationRegistry::Instance().CountGeneration(1), 1);
    UNIT_ASSERT_VALUES_EQUAL(value.AsStringValue().RefCount(), 2);
    value.Clear();
    UNIT_ASSERT_VALUES_EQUAL(TWasmAllocationRegistry::Instance().CountGeneration(1), 0);
}

//! What KqpWasmResidentString achieves for a loop-invariant argument (e.g. a
//! dictionary from a scalar subquery): materialize once, then every per-row UDF
//! call reuses the resident bytes. The query-scoped counters must reflect it so
//! the compute actor log shows the technology engaged even with columns=[].
Y_UNIT_TEST(ResidentConstArgIsMaterializedOnceAndReused) {
    NKikimr::NMiniKQL::TScopedAlloc alloc(__LOCATION__);
    auto handle = MakeQueryCompartment(/*generation*/ 1);
    TCurrentQueryCompartmentGuard queryGuard(handle.get());
    TPreferWasmStats::Instance().Reset();

    const TString dict = MakeBlob(BlobSize);
    // The computation node materializes the invariant arg exactly once...
    TUnboxedValue value(TWasmStringValue::MakePreferWasm(TStringRef(dict.data(), dict.size())));
    handle->PreferWasm.OnResidentConstArg();

    constexpr int Rows = 5;
    for (int row = 0; row < Rows; ++row) {
        const auto filled = FillArg(handle->Compartment.get(), value);
        UNIT_ASSERT_VALUES_EQUAL(filled.Value.Length, BlobSize);
        UNIT_ASSERT_VALUES_EQUAL(ReadGuestBytes(handle->Compartment.get(), filled.Value), TStringBuf(dict));
    }

    const auto queryStats = handle->PreferWasm.GetSnapshot();
    UNIT_ASSERT_VALUES_EQUAL(queryStats.ResidentConstArgs, 1);
    UNIT_ASSERT_VALUES_EQUAL(queryStats.MaterializedInWasm, 1);
    UNIT_ASSERT_VALUES_EQUAL(queryStats.ResidentReused, Rows);
    UNIT_ASSERT_VALUES_EQUAL(queryStats.CopiedIntoCompartment, 0);

    value.Clear();
    UNIT_ASSERT_VALUES_EQUAL(TWasmAllocationRegistry::Instance().CountGeneration(1), 0);
}

//! Baseline (EnableWasmUdfResidentStringColumns off, or a value that never came
//! from a marked column): a host string is copied into the compartment per call.
Y_UNIT_TEST(HostStringIsCopiedPerUdfCall) {
    NKikimr::NMiniKQL::TScopedAlloc alloc(__LOCATION__);
    auto handle = MakeQueryCompartment(/*generation*/ 1);
    TCurrentQueryCompartmentGuard queryGuard(handle.get());
    TPreferWasmStats::Instance().Reset();

    const TString blob = MakeBlob(BlobSize);
    TUnboxedValue value(NKikimr::NMiniKQL::MakeString(TStringRef(blob.data(), blob.size())));

    for (int call = 0; call < 3; ++call) {
        const auto filled = FillArg(handle->Compartment.get(), value);
        UNIT_ASSERT_VALUES_EQUAL(filled.Value.Length, BlobSize);
        UNIT_ASSERT_VALUES_EQUAL(ReadGuestBytes(handle->Compartment.get(), filled.Value), TStringBuf(blob));
    }

    const auto stats = TPreferWasmStats::Instance().GetSnapshot();
    UNIT_ASSERT_VALUES_EQUAL(stats.CopiedIntoCompartment, 3);
    UNIT_ASSERT_VALUES_EQUAL(stats.ResidentReused, 0);
    UNIT_ASSERT_VALUES_EQUAL(stats.MaterializedInWasm, 0);
}

//! Read and UDF in different tasks: nothing to materialize into, so the value
//! stays on the host and the counter flags the planning mistake.
Y_UNIT_TEST(NoQueryCompartmentFallsBackToHost) {
    NKikimr::NMiniKQL::TScopedAlloc alloc(__LOCATION__);
    UNIT_ASSERT_EQUAL(GetCurrentQueryCompartment(), nullptr);
    TPreferWasmStats::Instance().Reset();

    const TString blob = MakeBlob(BlobSize);
    TUnboxedValue value(TWasmStringValue::MakePreferWasm(TStringRef(blob.data(), blob.size())));

    const auto stats = TPreferWasmStats::Instance().GetSnapshot();
    UNIT_ASSERT_VALUES_EQUAL(stats.FallbackNoCompartment, 1);
    UNIT_ASSERT_VALUES_EQUAL(stats.MaterializedInWasm, 0);
    UNIT_ASSERT_VALUES_EQUAL(TStringBuf(value.AsStringRef()), TStringBuf(blob));
}

//! A compute actor is destroyed before its task runner tears the computation
//! graph down, so a value left in a graph slot outlives the query scope. Its
//! refcount header lives in linear memory, so the compartment has to stay mapped
//! until that last value is released.
Y_UNIT_TEST(ResidentValueOutlivesQueryScope) {
    NKikimr::NMiniKQL::TScopedAlloc alloc(__LOCATION__);
    constexpr ui64 generation = 42;
    const TString blob = MakeBlob(BlobSize);

    std::weak_ptr<TQueryCompartmentHandle> weakHandle;
    TUnboxedValue value;
    {
        auto handle = MakeQueryCompartment(generation);
        weakHandle = handle;
        // What TQueryCompartmentScope does on acquire.
        TWasmAllocationRegistry::Instance().RetainOwner(generation, handle);
        TCurrentQueryCompartmentGuard queryGuard(handle.get());
        value = TUnboxedValue(TWasmStringValue::MakePreferWasm(TStringRef(blob.data(), blob.size())));
        // ... and what it does when the actor goes away.
        TWasmAllocationRegistry::Instance().ReleaseOwner(generation);
    }

    UNIT_ASSERT(!weakHandle.expired());
    UNIT_ASSERT_VALUES_EQUAL(TStringBuf(value.AsStringRef()), TStringBuf(blob));
    UNIT_ASSERT_VALUES_EQUAL(TWasmAllocationRegistry::Instance().CountGeneration(generation), 1);

    value.Clear();
    UNIT_ASSERT(weakHandle.expired());
    UNIT_ASSERT_VALUES_EQUAL(TWasmAllocationRegistry::Instance().CountGeneration(generation), 0);
}

//! The LLVM codegen path does not call TStringValue::TData::UnRef: it decrements
//! the refcount inline and calls DeleteString. That entry point has to consult
//! the registry too, otherwise a resident value released inside JIT-compiled code
//! is handed to the MiniKQL allocator, which never allocated those bytes.
Y_UNIT_TEST(CodegenDeleteStringReachesRegistry) {
    NKikimr::NMiniKQL::TScopedAlloc alloc(__LOCATION__);
    constexpr ui64 generation = 11;
    auto handle = MakeQueryCompartment(generation);
    TCurrentQueryCompartmentGuard queryGuard(handle.get());

    const TString blob = MakeBlob(BlobSize);
    // Deliberately a bare pod: nothing else owns the value, so DeleteString below
    // is the only release, exactly as in generated code.
    const auto value = TWasmStringValue::MakePreferWasm(TStringRef(blob.data(), blob.size()));
    UNIT_ASSERT_VALUES_EQUAL(TWasmAllocationRegistry::Instance().CountGeneration(generation), 1);

    const auto headerBytes = NYql::NUdf::TStringValue::AllocationBytes(0);
    void* header = const_cast<char*>(value.AsStringRef().Data()) - headerBytes;
    DeleteString(header);

    UNIT_ASSERT_VALUES_EQUAL(TWasmAllocationRegistry::Instance().CountGeneration(generation), 0);
}

//! Bytes of another compartment must never be passed as an offset.
Y_UNIT_TEST(ForeignCompartmentBytesAreCopied) {
    NKikimr::NMiniKQL::TScopedAlloc alloc(__LOCATION__);
    auto handle = MakeQueryCompartment(/*generation*/ 1);
    auto other = MakeAllocatingCompartment();
    TCurrentQueryCompartmentGuard queryGuard(handle.get());
    TPreferWasmStats::Instance().Reset();

    const TString blob = MakeBlob(BlobSize);
    TUnboxedValue value(TWasmStringValue::MakePreferWasm(TStringRef(blob.data(), blob.size())));

    const auto filled = FillArg(other.get(), value);
    UNIT_ASSERT_VALUES_EQUAL(ReadGuestBytes(other.get(), filled.Value), TStringBuf(blob));

    const auto stats = TPreferWasmStats::Instance().GetSnapshot();
    UNIT_ASSERT_VALUES_EQUAL(stats.CopiedIntoCompartment, 1);
    UNIT_ASSERT_VALUES_EQUAL(stats.ResidentReused, 0);
}

//! Strings up to the embedded buffer never reach linear memory, so PreferWasm
//! wins nothing on short values: the arg is copied on every call.
Y_UNIT_TEST(EmbeddedStringGainsNothing) {
    NKikimr::NMiniKQL::TScopedAlloc alloc(__LOCATION__);
    auto handle = MakeQueryCompartment(/*generation*/ 1);
    TCurrentQueryCompartmentGuard queryGuard(handle.get());
    TPreferWasmStats::Instance().Reset();

    const TString blob = MakeBlob(TUnboxedValuePod::InternalBufferSize);
    TUnboxedValue value(TWasmStringValue::MakePreferWasm(TStringRef(blob.data(), blob.size())));
    UNIT_ASSERT(!value.IsString());

    const auto filled = FillArg(handle->Compartment.get(), value);
    UNIT_ASSERT_VALUES_EQUAL(ReadGuestBytes(handle->Compartment.get(), filled.Value), TStringBuf(blob));

    const auto stats = TPreferWasmStats::Instance().GetSnapshot();
    UNIT_ASSERT_VALUES_EQUAL(stats.MaterializedInWasm, 0);
    UNIT_ASSERT_VALUES_EQUAL(stats.CopiedIntoCompartment, 1);
}

}
