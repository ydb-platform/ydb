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
#include <yql/essentials/minikql/mkql_type_builder.h>
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

//! Guest calls that hand a handle to MiniKQL as a value of a declared type: a
//! dict key, a member of a dict the guest builds, an argument of a callable.
//! Every export leaves the handles it was given alone, so the host retires
//! them itself.
constexpr TStringBuf BridgeTypeCheckWast = R"(
    (module
        (import "env" "memory" (memory i64 8 2097152))
        (import "env" "BridgeDictContains" (func $contains (param i64 i64) (result i32)))
        (import "env" "BridgeDictLookup" (func $lookup (param i64 i64) (result i64)))
        (import "env" "BridgeGetResultType" (func $result_type (result i64)))
        (import "env" "BridgeMakeDict" (func $make_dict (param i64 i64 i32) (result i64)))
        (import "env" "BridgeRun" (func $run (param i64 i64 i32) (result i64)))
        (import "env" "BridgeUnref" (func $unref (param i64)))

        (func $contains_raw (param $ctx i64) (param $result i64) (param $dict i64) (param $key i64)
            (i64.store (local.get $result)
                (i64.extend_i32_u (call $contains (local.get $dict) (local.get $key))))
        )
        (export "contains_raw" (func $contains_raw))

        (func $make_dict_one (param $ctx i64) (param $result i64)
                (param $key i64) (param $payload i64) (param $pairs i64)
            (local $type i64)
            (local.set $type (call $result_type))
            (i64.store (local.get $pairs) (local.get $key))
            (i64.store (i64.add (local.get $pairs) (i64.const 8)) (local.get $payload))
            (i64.store (local.get $result)
                (call $make_dict (local.get $type) (local.get $pairs) (i32.const 1)))
            (call $unref (local.get $type))
        )
        (export "make_dict_one" (func $make_dict_one))

        (func $make_dict_then_lookup (param $ctx i64) (param $result i64)
                (param $key i64) (param $payload i64) (param $bad_key i64) (param $pairs i64)
            (local $type i64)
            (local $dict i64)
            (local.set $type (call $result_type))
            (i64.store (local.get $pairs) (local.get $key))
            (i64.store (i64.add (local.get $pairs) (i64.const 8)) (local.get $payload))
            (local.set $dict
                (call $make_dict (local.get $type) (local.get $pairs) (i32.const 1)))
            (call $unref (local.get $type))
            (i64.store (local.get $result) (call $lookup (local.get $dict) (local.get $bad_key)))
            (call $unref (local.get $dict))
        )
        (export "make_dict_then_lookup" (func $make_dict_then_lookup))

        (func $run_one (param $ctx i64) (param $result i64)
                (param $callable i64) (param $arg i64) (param $argv i64)
            (i64.store (local.get $argv) (local.get $arg))
            (i64.store (local.get $result)
                (call $run (local.get $callable) (local.get $argv) (i32.const 1)))
        )
        (export "run_one" (func $run_one))
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
        , MemInfo("bridge_typecheck_ut")
        , HolderFactory(Alloc.Ref(), MemInfo)
        , ValueBuilder(HolderFactory)
    {
    }
};

// Both namespaces in scope here spell TType and TDataType, so the MiniKQL
// types the tests declare are built through these.
using TMkqlType = NKikimr::NMiniKQL::TType;

const NYql::NUdf::TType* AsBridgeType(TMkqlType* type) {
    return static_cast<const NYql::NUdf::TType*>(type);
}

TMkqlType* MkqlStringType(TMiniKqlEnv& mkql) {
    return NKikimr::NMiniKQL::TDataType::Create(NYql::NUdf::TDataType<char*>::Id, mkql.Env);
}

TMkqlType* MkqlInt64Type(TMiniKqlEnv& mkql) {
    return NKikimr::NMiniKQL::TDataType::Create(NYql::NUdf::TDataType<i64>::Id, mkql.Env);
}

//! Callable of one argument returning Int64: the declaration that goes with
//! TArgumentLengthCallable below.
TMkqlType* LengthCallableType(TMiniKqlEnv& mkql, TMkqlType* argType) {
    TMkqlType* args[] = {argType};
    return NKikimr::NMiniKQL::TCallableType::Create(
        MkqlInt64Type(mkql),
        "Length",
        Y_ARRAY_SIZE(args),
        args,
        /*payload=*/nullptr,
        mkql.Env);
}

//! Answers the length of its only argument, or -1 when that argument is
//! absent. Reading an argument is what a handle of the wrong family aborts on,
//! which is why the guard has to refuse one before the call gets here.
class TArgumentLengthCallable: public TManagedBoxedValue {
public:
    TUnboxedValue Run(const IValueBuilder*, const TUnboxedValuePod* args) const override {
        if (!args[0]) {
            return TUnboxedValuePod(i64{-1});
        }
        return TUnboxedValuePod(static_cast<i64>(args[0].AsStringRef().Size()));
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

std::unique_ptr<TQueryCompartmentHandle> MakeQueryCompartment(ui64 generation) {
    EnsureUdfHostIntrinsicsRegistered();

    auto compartment = CreateEmptyImage();
    compartment->AddSdk(MakeNamedLibrary("sdk", SdkStubWast).Bytecode);

    auto handle = std::make_unique<TQueryCompartmentHandle>();
    handle->Generation = generation;
    handle->BridgeNodes = std::make_unique<TWasmBridgeNodeTable>(generation);
    handle->BridgeNodes->SetTypeInfoHelper(new NKikimr::NMiniKQL::TTypeInfoHelper());
    handle->Compartment = std::move(compartment);

    const auto objectCode = CompileModuleObjectCode(
        BridgeTypeCheckWast,
        EBytecodeFormat::HumanReadable);
    AddPrecompiledModule(
        handle->Compartment.get(),
        MakeModuleBytecode(BridgeTypeCheckWast, objectCode, EBytecodeFormat::HumanReadable),
        "TypeCheckUdf");
    return handle;
}

//! Query compartment with the module above loaded, plus everything a host
//! intrinsic looks up in thread-local state while the guest runs.
struct TTypeCheckUdf {
    std::unique_ptr<TQueryCompartmentHandle> Handle;
    TWasmUdfInvocationContext Context;
    TCurrentQueryCompartmentGuard QueryGuard;
    TCurrentCompartmentGuard CompartmentGuard;
    TBridgeValueBuilderGuard ValueBuilderGuard;
    TCurrentInvocationContextGuard InvocationGuard;
    uintptr_t ResultOffset;

    TTypeCheckUdf(TMiniKqlEnv& mkql, ui64 generation, TMkqlType* resultType)
        : Handle(MakeQueryCompartment(generation))
        , Context(Handle->Compartment.get())
        , QueryGuard(Handle.get())
        , CompartmentGuard(Handle->Compartment.get())
        , ValueBuilderGuard(*Handle->BridgeNodes, &mkql.ValueBuilder)
        , InvocationGuard(&Context)
        , ResultOffset(Handle->Compartment->AllocateBytes(sizeof(ui64)))
    {
        Context.ResultType = AsBridgeType(resultType);
    }

    TWasmBridgeNodeTable& Table() {
        return *Handle->BridgeNodes;
    }

    //! Room in linear memory for the handle array an export passes on.
    uintptr_t Scratch(size_t handles) {
        return Handle->Compartment->AllocateBytes(sizeof(ui64) * handles);
    }

    //! Run one export and answer the handle it left in the result cell.
    ui64 Invoke(const TString& name, const TVector<uintptr_t>& args) {
        auto* compartment = Handle->Compartment.get();
        *PtrFromVM(compartment, std::bit_cast<ui64*>(ResultOffset)) = NullBridgeHandle;
        InvokeUdfExport(
            compartment,
            name,
            std::bit_cast<uintptr_t>(&Context),
            ResultOffset,
            args);
        return *PtrFromVM(compartment, std::bit_cast<ui64*>(ResultOffset));
    }
};

} // namespace

Y_UNIT_TEST_SUITE(TWasmBridgeTypeCheckTest) {

Y_UNIT_TEST(DictContainsRejectsAKeyOfTheWrongKind) {
    // Contains hashes the key the way Lookup does, so a boxed handle where the
    // dict declares a String key takes the process down unless it is refused
    // on the way in.
    TMiniKqlEnv mkql;
    auto* dictType = NKikimr::NMiniKQL::TDictType::Create(
        MkqlStringType(mkql),
        MkqlInt64Type(mkql),
        mkql.Env);

    TTypeCheckUdf udf(mkql, 41, MkqlInt64Type(mkql));
    auto& table = udf.Table();

    auto dict = MakeStringIntDict(mkql, "a", 1);
    const ui64 dictHandle = table.Register(
        EBridgeNodeKind::Dict,
        EBridgeValueKind::Dict,
        AsBridgeType(dictType),
        TUnboxedValue(dict));

    TUnboxedValue items[] = {TUnboxedValuePod(i64{7})};
    const ui64 keyHandle = table.Register(
        EBridgeNodeKind::List,
        EBridgeValueKind::List,
        nullptr,
        mkql.ValueBuilder.NewList(items, 1));

    UNIT_ASSERT_EXCEPTION_CONTAINS(
        udf.Invoke("contains_raw", {dictHandle, keyHandle}),
        yexception,
        "BridgeDictContains key");

    table.Unref(keyHandle);
    table.Unref(dictHandle);
    UNIT_ASSERT_VALUES_EQUAL(table.DebugSize(), 0u);
}

Y_UNIT_TEST(MakeDictRejectsAKeyOfTheWrongKind) {
    // Keys reach the hasher of the dict being built, which reads them as the
    // declared key type.
    TMiniKqlEnv mkql;
    auto* dictType = NKikimr::NMiniKQL::TDictType::Create(
        MkqlStringType(mkql),
        MkqlInt64Type(mkql),
        mkql.Env);

    TTypeCheckUdf udf(mkql, 42, dictType);
    auto& table = udf.Table();

    TUnboxedValue items[] = {TUnboxedValuePod(i64{7})};
    const ui64 keyHandle = table.Register(
        EBridgeNodeKind::List,
        EBridgeValueKind::List,
        nullptr,
        mkql.ValueBuilder.NewList(items, 1));
    const ui64 payloadHandle = table.Register(
        EBridgeNodeKind::Scalar,
        EBridgeValueKind::Int64,
        nullptr,
        TUnboxedValuePod(i64{1}));

    UNIT_ASSERT_EXCEPTION_CONTAINS(
        udf.Invoke("make_dict_one", {keyHandle, payloadHandle, udf.Scratch(2)}),
        yexception,
        "BridgeMakeDict expected a string value, got list");

    table.Unref(payloadHandle);
    table.Unref(keyHandle);
}

Y_UNIT_TEST(GuestBuiltDictChecksTheKeyOnLookup) {
    // A dict the guest built is registered with the type it was built from, or
    // a later Lookup has nothing to check the key against and the mistyped one
    // reaches the hasher after all.
    TMiniKqlEnv mkql;
    auto* dictType = NKikimr::NMiniKQL::TDictType::Create(
        MkqlStringType(mkql),
        MkqlInt64Type(mkql),
        mkql.Env);

    TTypeCheckUdf udf(mkql, 43, dictType);
    auto& table = udf.Table();

    const ui64 keyHandle = table.Register(
        EBridgeNodeKind::String,
        EBridgeValueKind::String,
        nullptr,
        mkql.ValueBuilder.NewString(TStringRef("a", 1)));
    const ui64 payloadHandle = table.Register(
        EBridgeNodeKind::Scalar,
        EBridgeValueKind::Int64,
        nullptr,
        TUnboxedValuePod(i64{1}));

    TUnboxedValue items[] = {TUnboxedValuePod(i64{7})};
    const ui64 badKeyHandle = table.Register(
        EBridgeNodeKind::List,
        EBridgeValueKind::List,
        nullptr,
        mkql.ValueBuilder.NewList(items, 1));

    UNIT_ASSERT_EXCEPTION_CONTAINS(
        udf.Invoke(
            "make_dict_then_lookup",
            {keyHandle, payloadHandle, badKeyHandle, udf.Scratch(2)}),
        yexception,
        "BridgeDictLookup key");

    table.Unref(badKeyHandle);
    table.Unref(payloadHandle);
    table.Unref(keyHandle);
}

Y_UNIT_TEST(MakeDictTakesAListWhereThePayloadIsOptional) {
    // Optional<List<Int64>> is the list itself in MiniKQL, so a plain list is
    // what the guest has to pass for such a payload. Naming the family of the
    // declared type without looking through the Optional first refused it.
    TMiniKqlEnv mkql;
    auto* listType = NKikimr::NMiniKQL::TListType::Create(MkqlInt64Type(mkql), mkql.Env);
    auto* dictType = NKikimr::NMiniKQL::TDictType::Create(
        MkqlStringType(mkql),
        NKikimr::NMiniKQL::TOptionalType::Create(listType, mkql.Env),
        mkql.Env);

    TTypeCheckUdf udf(mkql, 44, dictType);
    auto& table = udf.Table();

    const ui64 keyHandle = table.Register(
        EBridgeNodeKind::String,
        EBridgeValueKind::String,
        nullptr,
        mkql.ValueBuilder.NewString(TStringRef("a", 1)));

    TUnboxedValue items[] = {TUnboxedValuePod(i64{7})};
    const ui64 payloadHandle = table.Register(
        EBridgeNodeKind::List,
        EBridgeValueKind::List,
        AsBridgeType(listType),
        mkql.ValueBuilder.NewList(items, 1));

    const ui64 dictHandle = udf.Invoke(
        "make_dict_one",
        {keyHandle, payloadHandle, udf.Scratch(2)});
    UNIT_ASSERT(dictHandle != NullBridgeHandle);
    UNIT_ASSERT_VALUES_EQUAL(table.Resolve(dictHandle).Value.GetDictLength(), 1u);

    table.Unref(dictHandle);
    table.Unref(payloadHandle);
    table.Unref(keyHandle);
    UNIT_ASSERT_VALUES_EQUAL(table.DebugSize(), 0u);
}

Y_UNIT_TEST(RunReadsAnArgumentOfTheDeclaredFamily) {
    TMiniKqlEnv mkql;

    TTypeCheckUdf udf(mkql, 45, MkqlInt64Type(mkql));
    auto& table = udf.Table();

    const ui64 callableHandle = table.Register(
        EBridgeNodeKind::Callable,
        EBridgeValueKind::Callable,
        AsBridgeType(LengthCallableType(mkql, MkqlStringType(mkql))),
        TUnboxedValuePod(new TArgumentLengthCallable()));
    const ui64 argHandle = table.Register(
        EBridgeNodeKind::String,
        EBridgeValueKind::String,
        nullptr,
        mkql.ValueBuilder.NewString(TStringRef("hello", 5)));

    const ui64 resultHandle = udf.Invoke(
        "run_one",
        {callableHandle, argHandle, udf.Scratch(1)});
    UNIT_ASSERT(resultHandle != NullBridgeHandle);
    UNIT_ASSERT_VALUES_EQUAL(table.Resolve(resultHandle).Value.Get<i64>(), 5);

    table.Unref(resultHandle);
    table.Unref(argHandle);
    table.Unref(callableHandle);
    UNIT_ASSERT_VALUES_EQUAL(table.DebugSize(), 0u);
}

Y_UNIT_TEST(RunRejectsAnArgumentOfTheWrongKind) {
    // The callee reads its arguments as the types it declares, so a boxed
    // handle in a String slot aborts inside the accessor.
    TMiniKqlEnv mkql;

    TTypeCheckUdf udf(mkql, 46, MkqlInt64Type(mkql));
    auto& table = udf.Table();

    const ui64 callableHandle = table.Register(
        EBridgeNodeKind::Callable,
        EBridgeValueKind::Callable,
        AsBridgeType(LengthCallableType(mkql, MkqlStringType(mkql))),
        TUnboxedValuePod(new TArgumentLengthCallable()));

    TUnboxedValue items[] = {TUnboxedValuePod(i64{7})};
    const ui64 argHandle = table.Register(
        EBridgeNodeKind::List,
        EBridgeValueKind::List,
        nullptr,
        mkql.ValueBuilder.NewList(items, 1));

    UNIT_ASSERT_EXCEPTION_CONTAINS(
        udf.Invoke("run_one", {callableHandle, argHandle, udf.Scratch(1)}),
        yexception,
        "BridgeRun argument");

    table.Unref(argHandle);
    table.Unref(callableHandle);
    UNIT_ASSERT_VALUES_EQUAL(table.DebugSize(), 0u);
}

Y_UNIT_TEST(RunRejectsANullMandatoryArgument) {
    // An absent value is how an omitted argument is spelled; a mandatory slot
    // is read regardless, and reading an absent value as a string aborts.
    TMiniKqlEnv mkql;

    TTypeCheckUdf udf(mkql, 47, MkqlInt64Type(mkql));
    auto& table = udf.Table();

    const ui64 callableHandle = table.Register(
        EBridgeNodeKind::Callable,
        EBridgeValueKind::Callable,
        AsBridgeType(LengthCallableType(mkql, MkqlStringType(mkql))),
        TUnboxedValuePod(new TArgumentLengthCallable()));

    UNIT_ASSERT_EXCEPTION_CONTAINS(
        udf.Invoke("run_one", {callableHandle, NullBridgeHandle, udf.Scratch(1)}),
        yexception,
        "declares it mandatory");

    table.Unref(callableHandle);
    UNIT_ASSERT_VALUES_EQUAL(table.DebugSize(), 0u);
}

Y_UNIT_TEST(RunTakesANullWhereTheArgumentIsOptional) {
    TMiniKqlEnv mkql;

    TTypeCheckUdf udf(mkql, 48, MkqlInt64Type(mkql));
    auto& table = udf.Table();

    auto* argType = NKikimr::NMiniKQL::TOptionalType::Create(MkqlStringType(mkql), mkql.Env);
    const ui64 callableHandle = table.Register(
        EBridgeNodeKind::Callable,
        EBridgeValueKind::Callable,
        AsBridgeType(LengthCallableType(mkql, argType)),
        TUnboxedValuePod(new TArgumentLengthCallable()));

    const ui64 resultHandle = udf.Invoke(
        "run_one",
        {callableHandle, NullBridgeHandle, udf.Scratch(1)});
    UNIT_ASSERT(resultHandle != NullBridgeHandle);
    UNIT_ASSERT_VALUES_EQUAL(table.Resolve(resultHandle).Value.Get<i64>(), -1);

    table.Unref(resultHandle);
    table.Unref(callableHandle);
    UNIT_ASSERT_VALUES_EQUAL(table.DebugSize(), 0u);
}

} // Y_UNIT_TEST_SUITE
