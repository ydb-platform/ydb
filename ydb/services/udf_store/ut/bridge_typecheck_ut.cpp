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
        (import "env" "BridgeDictIterNext" (func $dict_iter_next (param i64 i64 i64) (result i32)))
        (import "env" "BridgeDictLength" (func $dict_length (param i64) (result i64)))
        (import "env" "BridgeDictLookup" (func $lookup (param i64 i64) (result i64)))
        (import "env" "BridgeDictMakeIterator" (func $dict_iter (param i64) (result i64)))
        (import "env" "BridgeGetResultType" (func $result_type (result i64)))
        (import "env" "BridgeListLength" (func $list_length (param i64) (result i64)))
        (import "env" "BridgeListMakeIterator" (func $list_iter (param i64) (result i64)))
        (import "env" "BridgeMakeArray" (func $make_array (param i64 i32) (result i64)))
        (import "env" "BridgeMakeArrayTyped" (func $make_array_typed (param i64 i64 i32) (result i64)))
        (import "env" "BridgeMakeDict" (func $make_dict (param i64 i64 i32) (result i64)))
        (import "env" "BridgeMakeList" (func $make_list (param i64 i32) (result i64)))
        (import "env" "BridgeMakeListTyped" (func $make_list_typed (param i64 i64 i32) (result i64)))
        (import "env" "BridgeMakeOptional" (func $make_optional (param i64) (result i64)))
        (import "env" "BridgeRun" (func $run (param i64 i64 i32) (result i64)))
        (import "env" "BridgeTypeListItem" (func $type_list_item (param i64) (result i64)))
        (import "env" "BridgeTypeMember" (func $type_member (param i64 i32) (result i64)))
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

        (func $make_array_one (param $ctx i64) (param $result i64) (param $a i64) (param $elems i64)
            (i64.store (local.get $elems) (local.get $a))
            (i64.store (local.get $result) (call $make_array (local.get $elems) (i32.const 1)))
        )
        (export "make_array_one" (func $make_array_one))

        (func $make_array_two (param $ctx i64) (param $result i64)
                (param $a i64) (param $b i64) (param $elems i64)
            (i64.store (local.get $elems) (local.get $a))
            (i64.store (i64.add (local.get $elems) (i64.const 8)) (local.get $b))
            (i64.store (local.get $result) (call $make_array (local.get $elems) (i32.const 2)))
        )
        (export "make_array_two" (func $make_array_two))

        (func $make_optional_then_dict (param $ctx i64) (param $result i64)
                (param $key i64) (param $payload i64) (param $pairs i64)
            (local $type i64)
            (local $opt i64)
            (local.set $opt (call $make_optional (local.get $payload)))
            (local.set $type (call $result_type))
            (i64.store (local.get $pairs) (local.get $key))
            (i64.store (i64.add (local.get $pairs) (i64.const 8)) (local.get $opt))
            (i64.store (local.get $result)
                (call $make_dict (local.get $type) (local.get $pairs) (i32.const 1)))
            (call $unref (local.get $type))
            (call $unref (local.get $opt))
        )
        (export "make_optional_then_dict" (func $make_optional_then_dict))

        (func $make_list_two (param $ctx i64) (param $result i64)
                (param $a i64) (param $b i64) (param $items i64)
            (i64.store (local.get $items) (local.get $a))
            (i64.store (i64.add (local.get $items) (i64.const 8)) (local.get $b))
            (i64.store (local.get $result) (call $make_list (local.get $items) (i32.const 2)))
        )
        (export "make_list_two" (func $make_list_two))

        ;; Build a two-item list of the type named by BridgeTypeListItem on the
        ;; result type -- the inner List of List<List<...>>, or any other list
        ;; the guest reaches by walking the type tree itself.
        (func $make_inner_list_typed (param $ctx i64) (param $result i64)
                (param $a i64) (param $b i64) (param $items i64)
            (local $outer i64)
            (local $inner i64)
            (local.set $outer (call $result_type))
            (local.set $inner (call $type_list_item (local.get $outer)))
            (i64.store (local.get $items) (local.get $a))
            (i64.store (i64.add (local.get $items) (i64.const 8)) (local.get $b))
            (i64.store (local.get $result)
                (call $make_list_typed (local.get $inner) (local.get $items) (i32.const 2)))
            (call $unref (local.get $inner))
            (call $unref (local.get $outer))
        )
        (export "make_inner_list_typed" (func $make_inner_list_typed))

        ;; Build the second Tuple member of a two-member result via typed Make.
        (func $make_second_tuple_typed (param $ctx i64) (param $result i64)
                (param $a i64) (param $b i64) (param $elems i64)
            (local $outer i64)
            (local $inner i64)
            (local.set $outer (call $result_type))
            (local.set $inner (call $type_member (local.get $outer) (i32.const 1)))
            (i64.store (local.get $elems) (local.get $a))
            (i64.store (i64.add (local.get $elems) (i64.const 8)) (local.get $b))
            (i64.store (local.get $result)
                (call $make_array_typed (local.get $inner) (local.get $elems) (i32.const 2)))
            (call $unref (local.get $inner))
            (call $unref (local.get $outer))
        )
        (export "make_second_tuple_typed" (func $make_second_tuple_typed))

        (func $dict_iter_then_lookup (param $ctx i64) (param $result i64)
                (param $dict i64) (param $key i64)
            (local $iter i64)
            (local.set $iter (call $dict_iter (local.get $dict)))
            (i64.store (local.get $result) (call $lookup (local.get $iter) (local.get $key)))
            (call $unref (local.get $iter))
        )
        (export "dict_iter_then_lookup" (func $dict_iter_then_lookup))

        (func $dict_iter_then_length (param $ctx i64) (param $result i64) (param $dict i64)
            (local $iter i64)
            (local.set $iter (call $dict_iter (local.get $dict)))
            (i64.store (local.get $result) (call $dict_length (local.get $iter)))
            (call $unref (local.get $iter))
        )
        (export "dict_iter_then_length" (func $dict_iter_then_length))

        (func $list_iter_then_length (param $ctx i64) (param $result i64) (param $list i64)
            (local $iter i64)
            (local.set $iter (call $list_iter (local.get $list)))
            (i64.store (local.get $result) (call $list_length (local.get $iter)))
            (call $unref (local.get $iter))
        )
        (export "list_iter_then_length" (func $list_iter_then_length))

        (func $dict_iter_then_next (param $ctx i64) (param $result i64)
                (param $dict i64) (param $out i64)
            (local $iter i64)
            (local.set $iter (call $dict_iter (local.get $dict)))
            (i64.store (local.get $result)
                (i64.extend_i32_u
                    (call $dict_iter_next
                        (local.get $iter)
                        (local.get $out)
                        (i64.add (local.get $out) (i64.const 8)))))
            (call $unref (local.get $iter))
        )
        (export "dict_iter_then_next" (func $dict_iter_then_next))

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

TMkqlType* MkqlTupleType(TMiniKqlEnv& mkql, const TVector<TMkqlType*>& elements) {
    return NKikimr::NMiniKQL::TTupleType::Create(
        elements.size(),
        elements.data(),
        mkql.Env);
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

Y_UNIT_TEST(MakeArrayRejectsANullInAMandatoryMemberSlot) {
    // A member the guest leaves null is stored the way an absent value always
    // is, and the declared String is read back with AsStringRef, which takes
    // the process down on one.
    TMiniKqlEnv mkql;

    TTypeCheckUdf udf(mkql, 51, MkqlTupleType(mkql, {MkqlStringType(mkql)}));

    UNIT_ASSERT_EXCEPTION_CONTAINS(
        udf.Invoke("make_array_one", {NullBridgeHandle, udf.Scratch(1)}),
        yexception,
        "BridgeMakeArray slot 0 is null");

    UNIT_ASSERT_VALUES_EQUAL(udf.Table().DebugSize(), 0u);
}

Y_UNIT_TEST(MakeArrayTakesANullWhereTheMemberIsOptional) {
    TMiniKqlEnv mkql;

    auto* memberType = NKikimr::NMiniKQL::TOptionalType::Create(MkqlStringType(mkql), mkql.Env);
    TTypeCheckUdf udf(mkql, 52, MkqlTupleType(mkql, {memberType}));
    auto& table = udf.Table();

    const ui64 resultHandle = udf.Invoke("make_array_one", {NullBridgeHandle, udf.Scratch(1)});
    UNIT_ASSERT(resultHandle != NullBridgeHandle);
    UNIT_ASSERT(!table.Resolve(resultHandle).Value.GetElement(0));

    table.Unref(resultHandle);
    UNIT_ASSERT_VALUES_EQUAL(table.DebugSize(), 0u);
}

Y_UNIT_TEST(MakeArrayRejectsAnArityMismatch) {
    // A width the declaration does not name used to drop the declared type
    // and every per-member check with it, so a wrong count was all it took to
    // get an unchecked member into a declared slot.
    TMiniKqlEnv mkql;

    TTypeCheckUdf udf(mkql, 53, MkqlTupleType(mkql, {MkqlStringType(mkql)}));
    auto& table = udf.Table();

    const ui64 textHandle = table.Register(
        EBridgeNodeKind::String,
        EBridgeValueKind::String,
        AsBridgeType(MkqlStringType(mkql)),
        mkql.ValueBuilder.NewString(TStringRef("a", 1)));

    UNIT_ASSERT_EXCEPTION_CONTAINS(
        udf.Invoke("make_array_two", {textHandle, textHandle, udf.Scratch(2)}),
        yexception,
        "matches no Tuple in the declared result type");

    table.Unref(textHandle);
    UNIT_ASSERT_VALUES_EQUAL(table.DebugSize(), 0u);
}

Y_UNIT_TEST(MakeArrayChecksMembersOfANestedTuple) {
    // The guest builds a nested container bottom-up, so the width it is
    // building is what picks the declared type its members are checked
    // against -- here the inner Tuple, not the one-member outer.
    TMiniKqlEnv mkql;

    auto* innerType = MkqlTupleType(mkql, {MkqlStringType(mkql), MkqlInt64Type(mkql)});
    TTypeCheckUdf udf(mkql, 54, MkqlTupleType(mkql, {innerType}));
    auto& table = udf.Table();

    TUnboxedValue items[] = {TUnboxedValuePod(i64{7})};
    const ui64 listHandle = table.Register(
        EBridgeNodeKind::List,
        EBridgeValueKind::List,
        nullptr,
        mkql.ValueBuilder.NewList(items, 1));
    const ui64 numberHandle = table.Register(
        EBridgeNodeKind::Scalar,
        EBridgeValueKind::Int64,
        AsBridgeType(MkqlInt64Type(mkql)),
        TUnboxedValuePod(i64{1}));

    UNIT_ASSERT_EXCEPTION_CONTAINS(
        udf.Invoke("make_array_two", {listHandle, numberHandle, udf.Scratch(2)}),
        yexception,
        "BridgeMakeArray expected a string value, got list");

    table.Unref(numberHandle);
    table.Unref(listHandle);
    UNIT_ASSERT_VALUES_EQUAL(table.DebugSize(), 0u);
}

Y_UNIT_TEST(MakeListRejectsAnItemOfTheWrongKind) {
    // Items of a declared List<String> are read as strings by whoever reads
    // the list, the same way a dict payload is.
    TMiniKqlEnv mkql;

    auto* listType = NKikimr::NMiniKQL::TListType::Create(MkqlStringType(mkql), mkql.Env);
    TTypeCheckUdf udf(mkql, 55, listType);
    auto& table = udf.Table();

    TUnboxedValue items[] = {TUnboxedValuePod(i64{7})};
    const ui64 innerListHandle = table.Register(
        EBridgeNodeKind::List,
        EBridgeValueKind::List,
        nullptr,
        mkql.ValueBuilder.NewList(items, 1));
    const ui64 textHandle = table.Register(
        EBridgeNodeKind::String,
        EBridgeValueKind::String,
        AsBridgeType(MkqlStringType(mkql)),
        mkql.ValueBuilder.NewString(TStringRef("a", 1)));

    UNIT_ASSERT_EXCEPTION_CONTAINS(
        udf.Invoke("make_list_two", {innerListHandle, textHandle, udf.Scratch(2)}),
        yexception,
        "BridgeMakeList expected a string value, got list");

    table.Unref(textHandle);
    table.Unref(innerListHandle);
    UNIT_ASSERT_VALUES_EQUAL(table.DebugSize(), 0u);
}

Y_UNIT_TEST(MakeListTypesTheNodeItBuilds) {
    // Untyped, the list the guest just built has nothing for the item check
    // to compare against the next time it is read.
    TMiniKqlEnv mkql;

    auto* listType = NKikimr::NMiniKQL::TListType::Create(MkqlStringType(mkql), mkql.Env);
    TTypeCheckUdf udf(mkql, 56, listType);
    auto& table = udf.Table();

    const ui64 textHandle = table.Register(
        EBridgeNodeKind::String,
        EBridgeValueKind::String,
        AsBridgeType(MkqlStringType(mkql)),
        mkql.ValueBuilder.NewString(TStringRef("a", 1)));

    const ui64 resultHandle = udf.Invoke(
        "make_list_two",
        {textHandle, textHandle, udf.Scratch(2)});
    UNIT_ASSERT(resultHandle != NullBridgeHandle);
    UNIT_ASSERT_VALUES_EQUAL(table.Resolve(resultHandle).Type, AsBridgeType(listType));

    table.Unref(resultHandle);
    table.Unref(textHandle);
    UNIT_ASSERT_VALUES_EQUAL(table.DebugSize(), 0u);
}

Y_UNIT_TEST(MakeOptionalKeepsAReusedContainerReadable) {
    // The payload came from a declared Optional<List<Int64>>: its kind stopped
    // at the wrapper and only its type names the list. MiniKQL represents an
    // Optional over a boxed value as the value itself, so wrapping it again
    // hands back the very same node -- and writing an inner kind onto that one
    // used to rename it an optional-over-nothing, closing the slot it came
    // out of.
    TMiniKqlEnv mkql;

    auto* listType = NKikimr::NMiniKQL::TListType::Create(MkqlInt64Type(mkql), mkql.Env);
    auto* payloadType = NKikimr::NMiniKQL::TOptionalType::Create(listType, mkql.Env);
    auto* dictType = NKikimr::NMiniKQL::TDictType::Create(
        MkqlStringType(mkql),
        payloadType,
        mkql.Env);

    TTypeCheckUdf udf(mkql, 57, dictType);
    auto& table = udf.Table();

    TUnboxedValue items[] = {TUnboxedValuePod(i64{7})};
    const ui64 payloadHandle = table.Register(
        EBridgeNodeKind::Optional,
        EBridgeValueKind::Optional,
        AsBridgeType(payloadType),
        mkql.ValueBuilder.NewList(items, 1));
    const ui64 keyHandle = table.Register(
        EBridgeNodeKind::String,
        EBridgeValueKind::String,
        AsBridgeType(MkqlStringType(mkql)),
        mkql.ValueBuilder.NewString(TStringRef("a", 1)));

    const ui64 resultHandle = udf.Invoke(
        "make_optional_then_dict",
        {keyHandle, payloadHandle, udf.Scratch(2)});
    UNIT_ASSERT(resultHandle != NullBridgeHandle);
    UNIT_ASSERT(!table.Resolve(payloadHandle).InnerValueKind);

    table.Unref(resultHandle);
    table.Unref(keyHandle);
    table.Unref(payloadHandle);
    UNIT_ASSERT_VALUES_EQUAL(table.DebugSize(), 0u);
}

Y_UNIT_TEST(DictLookupRejectsAnIteratorInPlaceOfTheDict) {
    // An iterator is registered with the value kind of the container it walks,
    // so it used to pass for that container everywhere. For a dict iterator
    // that also disarmed the key check: its Type is the key type, so the
    // DictKeyTypeOf() of it is empty and the guard took its untyped-slot
    // early return.
    TMiniKqlEnv mkql;
    auto* dictType = NKikimr::NMiniKQL::TDictType::Create(
        MkqlStringType(mkql),
        MkqlInt64Type(mkql),
        mkql.Env);

    TTypeCheckUdf udf(mkql, 58, MkqlInt64Type(mkql));
    auto& table = udf.Table();

    auto dict = MakeStringIntDict(mkql, "a", 1);
    const ui64 dictHandle = table.Register(
        EBridgeNodeKind::Dict,
        EBridgeValueKind::Dict,
        AsBridgeType(dictType),
        TUnboxedValue(dict));
    const ui64 keyHandle = table.Register(
        EBridgeNodeKind::String,
        EBridgeValueKind::String,
        AsBridgeType(MkqlStringType(mkql)),
        mkql.ValueBuilder.NewString(TStringRef("a", 1)));

    UNIT_ASSERT_EXCEPTION_CONTAINS(
        udf.Invoke("dict_iter_then_lookup", {dictHandle, keyHandle}),
        yexception,
        "BridgeDictLookup got an iterator");

    table.Unref(keyHandle);
    table.Unref(dictHandle);
}

Y_UNIT_TEST(DictLengthRejectsAnIterator) {
    TMiniKqlEnv mkql;
    auto* dictType = NKikimr::NMiniKQL::TDictType::Create(
        MkqlStringType(mkql),
        MkqlInt64Type(mkql),
        mkql.Env);

    TTypeCheckUdf udf(mkql, 59, MkqlInt64Type(mkql));
    auto& table = udf.Table();

    auto dict = MakeStringIntDict(mkql, "a", 1);
    const ui64 dictHandle = table.Register(
        EBridgeNodeKind::Dict,
        EBridgeValueKind::Dict,
        AsBridgeType(dictType),
        TUnboxedValue(dict));

    UNIT_ASSERT_EXCEPTION_CONTAINS(
        udf.Invoke("dict_iter_then_length", {dictHandle}),
        yexception,
        "BridgeDictLength got an iterator");

    table.Unref(dictHandle);
}

Y_UNIT_TEST(ListLengthRejectsAnIterator) {
    TMiniKqlEnv mkql;
    auto* listType = NKikimr::NMiniKQL::TListType::Create(MkqlInt64Type(mkql), mkql.Env);

    TTypeCheckUdf udf(mkql, 60, MkqlInt64Type(mkql));
    auto& table = udf.Table();

    TUnboxedValue items[] = {TUnboxedValuePod(i64{7})};
    const ui64 listHandle = table.Register(
        EBridgeNodeKind::List,
        EBridgeValueKind::List,
        AsBridgeType(listType),
        mkql.ValueBuilder.NewList(items, 1));

    UNIT_ASSERT_EXCEPTION_CONTAINS(
        udf.Invoke("list_iter_then_length", {listHandle}),
        yexception,
        "BridgeListLength got an iterator");

    table.Unref(listHandle);
}

Y_UNIT_TEST(AnIteratorStillWalksThroughItsOwnIntrinsic) {
    // The control for the three above: what the guard refuses is an iterator
    // standing in for a container, not iteration itself.
    TMiniKqlEnv mkql;
    auto* dictType = NKikimr::NMiniKQL::TDictType::Create(
        MkqlStringType(mkql),
        MkqlInt64Type(mkql),
        mkql.Env);

    TTypeCheckUdf udf(mkql, 61, MkqlInt64Type(mkql));
    auto& table = udf.Table();

    auto dict = MakeStringIntDict(mkql, "a", 1);
    const ui64 dictHandle = table.Register(
        EBridgeNodeKind::Dict,
        EBridgeValueKind::Dict,
        AsBridgeType(dictType),
        TUnboxedValue(dict));

    const ui64 has = udf.Invoke("dict_iter_then_next", {dictHandle, udf.Scratch(2)});
    UNIT_ASSERT_VALUES_EQUAL(has, 1u);

    table.Unref(dictHandle);
}

Y_UNIT_TEST(MakeListBuildsTheInnerListOfANestedListResult) {
    // FindListTypeIn used to accept the outer List first, so building the
    // inner List<Int64> under List<List<Int64>> checked items as lists and
    // failed every row. Item families now pick the inner candidate.
    TMiniKqlEnv mkql;

    auto* innerType = NKikimr::NMiniKQL::TListType::Create(MkqlInt64Type(mkql), mkql.Env);
    auto* outerType = NKikimr::NMiniKQL::TListType::Create(innerType, mkql.Env);
    TTypeCheckUdf udf(mkql, 62, outerType);
    auto& table = udf.Table();

    const ui64 a = table.Register(
        EBridgeNodeKind::Scalar,
        EBridgeValueKind::Int64,
        AsBridgeType(MkqlInt64Type(mkql)),
        TUnboxedValuePod(i64{1}));
    const ui64 b = table.Register(
        EBridgeNodeKind::Scalar,
        EBridgeValueKind::Int64,
        AsBridgeType(MkqlInt64Type(mkql)),
        TUnboxedValuePod(i64{2}));

    const ui64 resultHandle = udf.Invoke("make_list_two", {a, b, udf.Scratch(2)});
    UNIT_ASSERT(resultHandle != NullBridgeHandle);
    UNIT_ASSERT_VALUES_EQUAL(table.Resolve(resultHandle).Type, AsBridgeType(innerType));

    table.Unref(resultHandle);
    table.Unref(b);
    table.Unref(a);
    UNIT_ASSERT_VALUES_EQUAL(table.DebugSize(), 0u);
}

Y_UNIT_TEST(MakeListBuildsTheSecondListOfATupleResult) {
    // Sibling Lists of different item families: the first match was
    // List<Int64>, so a list of strings failed the item check. Families pick
    // List<String>.
    TMiniKqlEnv mkql;

    auto* intList = NKikimr::NMiniKQL::TListType::Create(MkqlInt64Type(mkql), mkql.Env);
    auto* stringList = NKikimr::NMiniKQL::TListType::Create(MkqlStringType(mkql), mkql.Env);
    TTypeCheckUdf udf(mkql, 63, MkqlTupleType(mkql, {intList, stringList}));
    auto& table = udf.Table();

    const ui64 a = table.Register(
        EBridgeNodeKind::String,
        EBridgeValueKind::String,
        AsBridgeType(MkqlStringType(mkql)),
        mkql.ValueBuilder.NewString(TStringRef("a", 1)));
    const ui64 b = table.Register(
        EBridgeNodeKind::String,
        EBridgeValueKind::String,
        AsBridgeType(MkqlStringType(mkql)),
        mkql.ValueBuilder.NewString(TStringRef("b", 1)));

    const ui64 resultHandle = udf.Invoke("make_list_two", {a, b, udf.Scratch(2)});
    UNIT_ASSERT(resultHandle != NullBridgeHandle);
    UNIT_ASSERT_VALUES_EQUAL(table.Resolve(resultHandle).Type, AsBridgeType(stringList));

    table.Unref(resultHandle);
    table.Unref(b);
    table.Unref(a);
    UNIT_ASSERT_VALUES_EQUAL(table.DebugSize(), 0u);
}

Y_UNIT_TEST(MakeArrayBuildsAnInnerTupleOfTheOuterArity) {
    // Outer and inner Tuples share arity 2, so the first match was the outer
    // one and string members failed against Tuple slots. Families pick the
    // Tuple<String,String> member.
    TMiniKqlEnv mkql;

    auto* ints = MkqlTupleType(mkql, {MkqlInt64Type(mkql), MkqlInt64Type(mkql)});
    auto* strings = MkqlTupleType(mkql, {MkqlStringType(mkql), MkqlStringType(mkql)});
    TTypeCheckUdf udf(mkql, 64, MkqlTupleType(mkql, {ints, strings}));
    auto& table = udf.Table();

    const ui64 a = table.Register(
        EBridgeNodeKind::String,
        EBridgeValueKind::String,
        AsBridgeType(MkqlStringType(mkql)),
        mkql.ValueBuilder.NewString(TStringRef("a", 1)));
    const ui64 b = table.Register(
        EBridgeNodeKind::String,
        EBridgeValueKind::String,
        AsBridgeType(MkqlStringType(mkql)),
        mkql.ValueBuilder.NewString(TStringRef("b", 1)));

    const ui64 resultHandle = udf.Invoke("make_array_two", {a, b, udf.Scratch(2)});
    UNIT_ASSERT(resultHandle != NullBridgeHandle);
    UNIT_ASSERT_VALUES_EQUAL(table.Resolve(resultHandle).Type, AsBridgeType(strings));

    table.Unref(resultHandle);
    table.Unref(b);
    table.Unref(a);
    UNIT_ASSERT_VALUES_EQUAL(table.DebugSize(), 0u);
}

Y_UNIT_TEST(MakeListTypedNamesTheInnerListExplicitly) {
    // Preferred path for nested containers: the guest walks BridgeGetResultType
    // with BridgeTypeListItem and names the list, the way BridgeMakeDict does.
    TMiniKqlEnv mkql;

    auto* innerType = NKikimr::NMiniKQL::TListType::Create(MkqlInt64Type(mkql), mkql.Env);
    auto* outerType = NKikimr::NMiniKQL::TListType::Create(innerType, mkql.Env);
    TTypeCheckUdf udf(mkql, 65, outerType);
    auto& table = udf.Table();

    const ui64 a = table.Register(
        EBridgeNodeKind::Scalar,
        EBridgeValueKind::Int64,
        AsBridgeType(MkqlInt64Type(mkql)),
        TUnboxedValuePod(i64{1}));
    const ui64 b = table.Register(
        EBridgeNodeKind::Scalar,
        EBridgeValueKind::Int64,
        AsBridgeType(MkqlInt64Type(mkql)),
        TUnboxedValuePod(i64{2}));

    const ui64 resultHandle = udf.Invoke("make_inner_list_typed", {a, b, udf.Scratch(2)});
    UNIT_ASSERT(resultHandle != NullBridgeHandle);
    UNIT_ASSERT_VALUES_EQUAL(table.Resolve(resultHandle).Type, AsBridgeType(innerType));

    table.Unref(resultHandle);
    table.Unref(b);
    table.Unref(a);
    UNIT_ASSERT_VALUES_EQUAL(table.DebugSize(), 0u);
}

Y_UNIT_TEST(MakeArrayTypedNamesTheSecondTupleExplicitly) {
    TMiniKqlEnv mkql;

    auto* ints = MkqlTupleType(mkql, {MkqlInt64Type(mkql), MkqlInt64Type(mkql)});
    auto* strings = MkqlTupleType(mkql, {MkqlStringType(mkql), MkqlStringType(mkql)});
    TTypeCheckUdf udf(mkql, 66, MkqlTupleType(mkql, {ints, strings}));
    auto& table = udf.Table();

    const ui64 a = table.Register(
        EBridgeNodeKind::String,
        EBridgeValueKind::String,
        AsBridgeType(MkqlStringType(mkql)),
        mkql.ValueBuilder.NewString(TStringRef("a", 1)));
    const ui64 b = table.Register(
        EBridgeNodeKind::String,
        EBridgeValueKind::String,
        AsBridgeType(MkqlStringType(mkql)),
        mkql.ValueBuilder.NewString(TStringRef("b", 1)));

    const ui64 resultHandle = udf.Invoke("make_second_tuple_typed", {a, b, udf.Scratch(2)});
    UNIT_ASSERT(resultHandle != NullBridgeHandle);
    UNIT_ASSERT_VALUES_EQUAL(table.Resolve(resultHandle).Type, AsBridgeType(strings));

    table.Unref(resultHandle);
    table.Unref(b);
    table.Unref(a);
    UNIT_ASSERT_VALUES_EQUAL(table.DebugSize(), 0u);
}

} // Y_UNIT_TEST_SUITE
