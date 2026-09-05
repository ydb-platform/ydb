#include <ydb/services/udf_store/wasm/bridge_node_table.h>
#include <ydb/services/udf_store/wasm/bridge_types.h>
#include <ydb/services/udf_store/wasm/compartment_manager.h>
#include <ydb/services/udf_store/wasm/compile.h>
#include <ydb/services/udf_store/wasm/host.h>
#include <ydb/services/udf_store/wasm/invocation_context.h>
#include <ydb/services/udf_store/wasm/registry_helpers.h>

#include <ydb/library/wasm/api/compartment.h>
#include <ydb/library/wasm/api/pointer.h>

#include <yql/essentials/minikql/computation/mkql_value_builder.h>
#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/public/udf/udf_data_type.h>

#include <library/cpp/testing/unittest/registar.h>

#include <bit>

using namespace NKikimr::NUdfStore::NWasm;
using namespace NYdb::NWasm;
using namespace NYql::NUdf;
using namespace NKikimr::NMiniKQL;

namespace {

constexpr TStringBuf SdkStubWast = R"(
    (module
        (import "env" "memory" (memory i64 8 2097152))
        (global $heap (mut i64) (i64.const 65536))
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

constexpr TStringBuf BridgeDictLookupWast = R"(
    (module
        (import "env" "memory" (memory i64 8 2097152))
        (import "env" "BridgeDictLookup" (func $lookup (param i64 i64) (result i64)))
        (import "env" "BridgeIsNull" (func $is_null (param i64) (result i32)))
        (import "env" "BridgeMakeNull" (func $make_null (result i64)))
        (import "env" "BridgeGetInt64" (func $get_i64 (param i64) (result i64)))
        (import "env" "BridgeMakeInt64" (func $make_i64 (param i64) (result i64)))
        (import "env" "BridgeMakeOptional" (func $make_opt (param i64) (result i64)))
        (import "env" "BridgeUnref" (func $unref (param i64)))
        (func $dict_lookup (param $ctx i64) (param $result i64) (param $dict i64) (param $key i64)
            (local $payload i64)
            (local $inner i64)
            (local.set $payload (call $lookup (local.get $dict) (local.get $key)))
            (if (i32.eqz (call $is_null (local.get $payload)))
                (then
                    (local.set $inner (call $make_i64 (call $get_i64 (local.get $payload))))
                    (call $unref (local.get $payload))
                    (i64.store (local.get $result) (call $make_opt (local.get $inner)))
                    (call $unref (local.get $inner)))
                (else
                    (i64.store (local.get $result) (call $make_null))))
        )
        (export "dict_lookup" (func $dict_lookup))
        (func $lookup_raw (param $ctx i64) (param $result i64) (param $dict i64) (param $key i64)
            (i64.store (local.get $result) (call $lookup (local.get $dict) (local.get $key)))
        )
        (export "lookup_raw" (func $lookup_raw))
        (func $wrap_optional (param $ctx i64) (param $result i64) (param $dict i64)
            (i64.store (local.get $result) (call $make_opt (local.get $dict)))
        )
        (export "wrap_optional" (func $wrap_optional))
    )
)";

TNamedModuleBytecode MakeNamedLibrary(TStringBuf name, TStringBuf wast) {
    const auto objectCode = CompileModuleObjectCode(wast, EBytecodeFormat::HumanReadable);
    return TNamedModuleBytecode{
        .Name = TString(name),
        .Bytecode = MakeModuleBytecode(wast, objectCode, EBytecodeFormat::HumanReadable),
    };
}

struct TMiniKqlEnv {
    TScopedAlloc Alloc;
    TTypeEnvironment Env;
    TMemoryUsageInfo MemInfo;
    THolderFactory HolderFactory;
    TDefaultValueBuilder ValueBuilder;

    TMiniKqlEnv()
        : Alloc(__LOCATION__)
        , Env(Alloc)
        , MemInfo("bridge_dict_ut")
        , HolderFactory(Alloc.Ref(), MemInfo)
        , ValueBuilder(HolderFactory)
    {
    }
};

TUnboxedValue MakeStringIntDict(TMiniKqlEnv& mkql, TStringBuf key, i64 value) {
    TKeyTypes keyTypes;
    keyTypes.emplace_back(EDataSlot::String, false);
    auto keyValue = mkql.ValueBuilder.NewString(TStringRef(key.data(), key.size()));
    return mkql.HolderFactory.CreateDirectHashedDictHolder(
        [&](TValuesDictHashMap& map) {
            map.emplace(keyValue, TUnboxedValuePod(value));
        },
        keyTypes,
        /*isTuple=*/false,
        /*eagerFill=*/true,
        /*encodedType=*/nullptr,
        /*hash=*/nullptr,
        /*equate=*/nullptr);
}

//! Dict<String, Int64?> whose single key carries no payload.
TUnboxedValue MakeStringNullDict(TMiniKqlEnv& mkql, TStringBuf key) {
    TKeyTypes keyTypes;
    keyTypes.emplace_back(EDataSlot::String, false);
    auto keyValue = mkql.ValueBuilder.NewString(TStringRef(key.data(), key.size()));
    return mkql.HolderFactory.CreateDirectHashedDictHolder(
        [&](TValuesDictHashMap& map) {
            map.emplace(keyValue, TUnboxedValuePod());
        },
        keyTypes,
        /*isTuple=*/false,
        /*eagerFill=*/true,
        /*encodedType=*/nullptr,
        /*hash=*/nullptr,
        /*equate=*/nullptr);
}

} // namespace

Y_UNIT_TEST_SUITE(TWasmBridgeDictTest) {

Y_UNIT_TEST(IdentityReuseSameDict) {
    TMiniKqlEnv mkql;
    TWasmBridgeNodeTable table(/*generation*/ 5);
    auto dict = MakeStringIntDict(mkql, "a", 1);

    const ui64 h1 = table.Register(
        EBridgeNodeKind::Dict,
        EBridgeValueKind::Dict,
        nullptr,
        TUnboxedValue(dict));
    const ui64 h2 = table.TryReuse(dict);
    UNIT_ASSERT_VALUES_EQUAL(h1, h2);
    UNIT_ASSERT_VALUES_EQUAL(table.DebugSize(), 1u);

    table.Unref(h1);
    UNIT_ASSERT_VALUES_EQUAL(table.DebugSize(), 0u);
}

Y_UNIT_TEST(RunScopeDropsOnlyTheRefItTookOnReuse) {
    // RegisterOrReuse on an identity the table already knows bumps a ref
    // instead of making a second node, and the scope must give back exactly
    // that one ref -- otherwise a precompute the guest holds across rows dies
    // on the second row.
    TMiniKqlEnv mkql;
    TWasmBridgeNodeTable table(/*generation*/ 15);
    auto dict = MakeStringIntDict(mkql, "a", 1);

    ui64 first = NullBridgeHandle;
    {
        TBridgeRunScopeGuard scope(table);
        first = table.RegisterOrReuse(
            EBridgeNodeKind::Dict,
            EBridgeValueKind::Dict,
            nullptr,
            dict);
        table.Ref(first); // guest keeps the handle across rows
    }
    UNIT_ASSERT_VALUES_EQUAL(table.DebugSize(), 1u);

    {
        TBridgeRunScopeGuard scope(table);
        UNIT_ASSERT_VALUES_EQUAL(
            table.RegisterOrReuse(
                EBridgeNodeKind::Dict,
                EBridgeValueKind::Dict,
                nullptr,
                dict),
            first);
        UNIT_ASSERT_VALUES_EQUAL(table.DebugSize(), 1u);
    }
    UNIT_ASSERT_VALUES_EQUAL(table.DebugSize(), 1u);

    table.Unref(first);
    UNIT_ASSERT_VALUES_EQUAL(table.DebugSize(), 0u);
}

Y_UNIT_TEST(DictLookupViaIntrinsics) {
    EnsureUdfHostIntrinsicsRegistered();
    TMiniKqlEnv mkql;

    auto dict = MakeStringIntDict(mkql, "a", 1);

    auto compartment = CreateEmptyImage();
    compartment->AddSdk(MakeNamedLibrary("sdk", SdkStubWast).Bytecode);

    auto handle = std::make_unique<TQueryCompartmentHandle>();
    handle->Generation = 21;
    handle->BridgeNodes = std::make_unique<TWasmBridgeNodeTable>(handle->Generation);
    handle->Compartment = std::move(compartment);

    TCurrentQueryCompartmentGuard queryGuard(handle.get());
    TCurrentCompartmentGuard compartmentGuard(handle->Compartment.get());
    TWasmUdfInvocationContext context(handle->Compartment.get());
    TCurrentInvocationContextGuard invocationGuard(&context);
    TBridgeValueBuilderGuard vbGuard(*handle->BridgeNodes, &mkql.ValueBuilder);

    const auto moduleObjectCode = CompileModuleObjectCode(
        BridgeDictLookupWast,
        EBytecodeFormat::HumanReadable);
    AddPrecompiledModule(
        handle->Compartment.get(),
        MakeModuleBytecode(BridgeDictLookupWast, moduleObjectCode, EBytecodeFormat::HumanReadable),
        "DictUdf");

    auto& table = *handle->BridgeNodes;
    const ui64 dictHandle = table.Register(
        EBridgeNodeKind::Dict,
        EBridgeValueKind::Dict,
        nullptr,
        TUnboxedValue(dict));
    UNIT_ASSERT_VALUES_EQUAL(table.TryReuse(dict), dictHandle);

    auto key2 = mkql.ValueBuilder.NewString(TStringRef("a", 1));
    const ui64 keyHandle = table.Register(
        EBridgeNodeKind::String,
        EBridgeValueKind::String,
        nullptr,
        std::move(key2));

    const auto resultOffset = handle->Compartment->AllocateBytes(sizeof(ui64));
    *PtrFromVM(handle->Compartment.get(), std::bit_cast<ui64*>(resultOffset)) = 0;

    InvokeUdfExport(
        handle->Compartment.get(),
        "dict_lookup",
        std::bit_cast<uintptr_t>(&context),
        resultOffset,
        {dictHandle, keyHandle});

    const ui64 resultHandle = *PtrFromVM(
        handle->Compartment.get(),
        std::bit_cast<ui64*>(resultOffset));
    UNIT_ASSERT(resultHandle != NullBridgeHandle);
    const auto& resultNode = table.Resolve(resultHandle);
    UNIT_ASSERT(resultNode.ValueKind == EBridgeValueKind::Optional);
    UNIT_ASSERT(static_cast<bool>(resultNode.Value));
    UNIT_ASSERT_VALUES_EQUAL(resultNode.Value.GetOptionalValue().Get<i64>(), 1);

    table.Unref(keyHandle);
    table.Unref(resultHandle);
    table.Unref(dictHandle);
    UNIT_ASSERT_VALUES_EQUAL(table.DebugSize(), 0u);
}

Y_UNIT_TEST(DictLookupSeparatesAMissingKeyFromANullPayload) {
    // Both used to answer with handle 0, which left a Dict<K, V?> guest unable
    // to tell "no such key" from "the key is there and holds null".
    EnsureUdfHostIntrinsicsRegistered();
    TMiniKqlEnv mkql;

    auto dict = MakeStringNullDict(mkql, "a");

    auto compartment = CreateEmptyImage();
    compartment->AddSdk(MakeNamedLibrary("sdk", SdkStubWast).Bytecode);

    auto handle = std::make_unique<TQueryCompartmentHandle>();
    handle->Generation = 27;
    handle->BridgeNodes = std::make_unique<TWasmBridgeNodeTable>(handle->Generation);
    handle->Compartment = std::move(compartment);

    TCurrentQueryCompartmentGuard queryGuard(handle.get());
    TCurrentCompartmentGuard compartmentGuard(handle->Compartment.get());
    TWasmUdfInvocationContext context(handle->Compartment.get());
    TCurrentInvocationContextGuard invocationGuard(&context);
    TBridgeValueBuilderGuard vbGuard(*handle->BridgeNodes, &mkql.ValueBuilder);

    const auto moduleObjectCode = CompileModuleObjectCode(
        BridgeDictLookupWast,
        EBytecodeFormat::HumanReadable);
    AddPrecompiledModule(
        handle->Compartment.get(),
        MakeModuleBytecode(BridgeDictLookupWast, moduleObjectCode, EBytecodeFormat::HumanReadable),
        "DictUdf");

    auto& table = *handle->BridgeNodes;
    const ui64 dictHandle = table.Register(
        EBridgeNodeKind::Dict,
        EBridgeValueKind::Dict,
        nullptr,
        TUnboxedValue(dict));

    const auto resultOffset = handle->Compartment->AllocateBytes(sizeof(ui64));
    auto lookup = [&](TStringBuf key) {
        auto keyValue = mkql.ValueBuilder.NewString(TStringRef(key.data(), key.size()));
        const ui64 keyHandle = table.Register(
            EBridgeNodeKind::String,
            EBridgeValueKind::String,
            nullptr,
            std::move(keyValue));
        *PtrFromVM(handle->Compartment.get(), std::bit_cast<ui64*>(resultOffset)) = 0;
        InvokeUdfExport(
            handle->Compartment.get(),
            "lookup_raw",
            std::bit_cast<uintptr_t>(&context),
            resultOffset,
            {dictHandle, keyHandle});
        table.Unref(keyHandle);
        return *PtrFromVM(handle->Compartment.get(), std::bit_cast<ui64*>(resultOffset));
    };

    const ui64 present = lookup("a");
    UNIT_ASSERT_C(present != NullBridgeHandle, "a key holding null still exists");
    UNIT_ASSERT(table.Resolve(present).ValueKind == EBridgeValueKind::Null);
    table.Unref(present);

    UNIT_ASSERT_VALUES_EQUAL(lookup("zzz"), NullBridgeHandle);

    table.Unref(dictHandle);
    UNIT_ASSERT_VALUES_EQUAL(table.DebugSize(), 0u);
}

Y_UNIT_TEST(MakeOptionalReusesMarkerRepresentationNode) {
    // Optional<Dict> is the dict pod itself in MiniKQL, so BridgeMakeOptional
    // must hand back the node the guest passed in rather than open a second
    // node on the same identity -- two nodes would key the resident cache and
    // the user-data slot twice.
    EnsureUdfHostIntrinsicsRegistered();
    TMiniKqlEnv mkql;

    auto dict = MakeStringIntDict(mkql, "a", 1);

    auto compartment = CreateEmptyImage();
    compartment->AddSdk(MakeNamedLibrary("sdk", SdkStubWast).Bytecode);

    auto handle = std::make_unique<TQueryCompartmentHandle>();
    handle->Generation = 22;
    handle->BridgeNodes = std::make_unique<TWasmBridgeNodeTable>(handle->Generation);
    handle->Compartment = std::move(compartment);

    TCurrentQueryCompartmentGuard queryGuard(handle.get());
    TCurrentCompartmentGuard compartmentGuard(handle->Compartment.get());
    TWasmUdfInvocationContext context(handle->Compartment.get());
    TCurrentInvocationContextGuard invocationGuard(&context);
    TBridgeValueBuilderGuard vbGuard(*handle->BridgeNodes, &mkql.ValueBuilder);

    const auto moduleObjectCode = CompileModuleObjectCode(
        BridgeDictLookupWast,
        EBytecodeFormat::HumanReadable);
    AddPrecompiledModule(
        handle->Compartment.get(),
        MakeModuleBytecode(BridgeDictLookupWast, moduleObjectCode, EBytecodeFormat::HumanReadable),
        "DictUdf");

    auto& table = *handle->BridgeNodes;
    const ui64 dictHandle = table.Register(
        EBridgeNodeKind::Dict,
        EBridgeValueKind::Dict,
        nullptr,
        TUnboxedValue(dict));

    const auto resultOffset = handle->Compartment->AllocateBytes(sizeof(ui64));
    *PtrFromVM(handle->Compartment.get(), std::bit_cast<ui64*>(resultOffset)) = 0;

    InvokeUdfExport(
        handle->Compartment.get(),
        "wrap_optional",
        std::bit_cast<uintptr_t>(&context),
        resultOffset,
        {dictHandle});

    const ui64 resultHandle = *PtrFromVM(
        handle->Compartment.get(),
        std::bit_cast<ui64*>(resultOffset));
    UNIT_ASSERT_VALUES_EQUAL(resultHandle, dictHandle);
    UNIT_ASSERT_VALUES_EQUAL(table.DebugSize(), 1u);

    table.Unref(resultHandle);
    table.Unref(dictHandle);
    UNIT_ASSERT_VALUES_EQUAL(table.DebugSize(), 0u);
}

} // Y_UNIT_TEST_SUITE
