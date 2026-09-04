#include <ydb/services/udf_store/wasm/bridge_node_table.h>
#include <ydb/services/udf_store/wasm/bridge_resident.h>
#include <ydb/services/udf_store/wasm/bridge_types.h>
#include <ydb/services/udf_store/wasm/compartment_manager.h>
#include <ydb/services/udf_store/wasm/compile.h>
#include <ydb/services/udf_store/wasm/host.h>
#include <ydb/services/udf_store/wasm/invocation_context.h>
#include <ydb/services/udf_store/wasm/manifest.h>
#include <ydb/services/udf_store/wasm/registry_helpers.h>
#include <ydb/services/udf_store/wasm/udf_function.h>

#include <ydb/library/wasm/api/compartment.h>
#include <ydb/library/wasm/api/pointer.h>

#include <yql/essentials/minikql/computation/mkql_value_builder.h>
#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/public/udf/udf_type_printer.h>

#include <library/cpp/testing/unittest/registar.h>

#include <bit>
#include <cstring>

using namespace NKikimr::NUdfStore::NWasm;
using namespace NYdb::NWasm;
using namespace NYql::NUdf;
using namespace NKikimr::NMiniKQL;

namespace {

//! Bump allocator behind the POSIX "sbrk" export the host uses to fence off
//! grown regions, mirroring the default SDK stub the registry installs. malloc
//! goes through the same break, so a missing fence shows up as malloc handing
//! back host-owned bytes, and a break left above the last mapped page shows up
//! as malloc handing back offsets the guest cannot store to.
constexpr TStringBuf SdkStubWast = R"(
    (module
        (import "env" "memory" (memory i64 8 2097152))
        (global $break (mut i64) (i64.const 65536))
        (func $sbrk (param $n i64) (result i64)
            (local $old i64)
            (local $new i64)
            (local $pages i64)
            (local.set $old (global.get $break))
            (local.set $new
                (i64.and
                    (i64.add (i64.add (local.get $old) (local.get $n)) (i64.const 7))
                    (i64.const -8)))
            (local.set $pages
                (i64.sub
                    (i64.shr_u
                        (i64.add (local.get $new) (i64.const 65535))
                        (i64.const 16))
                    (memory.size)))
            (if (i64.gt_s (local.get $pages) (i64.const 0))
                (then
                    (if (i64.eq (memory.grow (local.get $pages)) (i64.const -1))
                        (then (return (i64.const -1))))))
            (global.set $break (local.get $new))
            (local.get $old)
        )
        (func $malloc (param $n i64) (result i64)
            (call $sbrk (local.get $n))
        )
        (func $free (param $p i64))
        (export "sbrk" (func $sbrk))
        (export "malloc" (func $malloc))
        (export "free" (func $free))
    )
)";

//! A runtime library that keeps its break to itself. The resident cache cannot
//! fence anything off such a guest, so it must refuse to hand out memory.
constexpr TStringBuf SdkStubNoSbrkWast = R"(
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

// Bridge echo: resultHandle* <- BridgeMakeInt64(BridgeGetInt64(arg0))
constexpr TStringBuf BridgeEchoIntWast = R"(
    (module
        (import "env" "memory" (memory i64 8 2097152))
        (import "env" "BridgeGetInt64" (func $get_i64 (param i64) (result i64)))
        (import "env" "BridgeMakeInt64" (func $make_i64 (param i64) (result i64)))
        (func $echo_int (param $ctx i64) (param $result i64) (param $arg0 i64)
            (i64.store (local.get $result)
                (call $make_i64 (call $get_i64 (local.get $arg0))))
        )
        (export "echo_int" (func $echo_int))
    )
)";

// Bridge string: copy arg string into guest buffer via BridgeCopyString, then BridgeMakeString.
constexpr TStringBuf BridgeEchoStringWast = R"(
    (module
        (import "env" "memory" (memory i64 8 2097152))
        (import "env" "BridgeGetStringLen" (func $strlen (param i64) (result i64)))
        (import "env" "BridgeCopyString" (func $copy (param i64 i64 i64) (result i64)))
        (import "env" "BridgeMakeString" (func $make (param i64 i64) (result i64)))
        (import "env" "AllocateBytes" (func $alloc (param i64 i64) (result i64)))
        (func $echo_str (param $ctx i64) (param $result i64) (param $arg0 i64)
            (local $len i64)
            (local $buf i64)
            (local.set $len (call $strlen (local.get $arg0)))
            (local.set $buf (call $alloc (local.get $ctx) (local.get $len)))
            (drop (call $copy (local.get $arg0) (local.get $buf) (local.get $len)))
            (i64.store (local.get $result)
                (call $make (local.get $buf) (local.get $len)))
        )
        (export "echo_str" (func $echo_str))
    )
)";

//! Ref + EnsureString twice; write MakeInt64(offset) if both pins match, else 0 handle.
constexpr TStringBuf BridgeEnsureStringWast = R"(
    (module
        (import "env" "memory" (memory i64 8 2097152))
        (import "env" "BridgeRef" (func $ref (param i64)))
        (import "env" "BridgeEnsureString" (func $ensure (param i64) (result i64)))
        (import "env" "BridgeMakeInt64" (func $make_i64 (param i64) (result i64)))
        (func $pin_twice (param $ctx i64) (param $result i64) (param $arg0 i64)
            (local $o1 i64)
            (local $o2 i64)
            (call $ref (local.get $arg0))
            (local.set $o1 (call $ensure (local.get $arg0)))
            (local.set $o2 (call $ensure (local.get $arg0)))
            (if (i64.eq (local.get $o1) (local.get $o2))
                (then
                    (i64.store (local.get $result)
                        (call $make_i64 (local.get $o1))))
                (else
                    (i64.store (local.get $result) (i64.const 0))))
        )
        (export "pin_twice" (func $pin_twice))
    )
)";

//! Build a Struct from two handles and read it straight back: both the member
//! count and the element need the result node to carry a type.
constexpr TStringBuf BridgeStructRoundTripWast = R"(
    (module
        (import "env" "memory" (memory i64 8 2097152))
        (import "env" "AllocateBytes" (func $alloc (param i64 i64) (result i64)))
        (import "env" "BridgeMakeStruct" (func $make_struct (param i64 i32) (result i64)))
        (import "env" "BridgeGetMemberCount" (func $member_count (param i64) (result i32)))
        (import "env" "BridgeGetElement" (func $get_elem (param i64 i32) (result i64)))
        (import "env" "BridgeGetInt64" (func $get_i64 (param i64) (result i64)))
        (import "env" "BridgeMakeInt64" (func $make_i64 (param i64) (result i64)))
        (func $round_trip (param $ctx i64) (param $result i64) (param $a i64) (param $b i64)
            (local $members i64)
            (local $s i64)
            (local.set $members (call $alloc (local.get $ctx) (i64.const 16)))
            (i64.store (local.get $members) (local.get $a))
            (i64.store offset=8 (local.get $members) (local.get $b))
            (local.set $s (call $make_struct (local.get $members) (i32.const 2)))
            (i64.store (local.get $result)
                (call $make_i64
                    (i64.add
                        (i64.mul
                            (i64.extend_i32_s (call $member_count (local.get $s)))
                            (i64.const 100))
                        (call $get_i64 (call $get_elem (local.get $s) (i32.const 1))))))
        )
        (export "round_trip" (func $round_trip))
    )
)";

//! Wrap arg0 as alternative 0 of the declared Variant result type.
constexpr TStringBuf BridgeMakeVariantWast = R"(
    (module
        (import "env" "memory" (memory i64 8 2097152))
        (import "env" "BridgeMakeVariant" (func $make_variant (param i32 i64) (result i64)))
        (func $wrap_variant (param $ctx i64) (param $result i64) (param $arg0 i64)
            (i64.store (local.get $result)
                (call $make_variant (i32.const 0) (local.get $arg0)))
        )
        (export "wrap_variant" (func $wrap_variant))
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
        , MemInfo("bridge_abi_ut")
        , HolderFactory(Alloc.Ref(), MemInfo)
        , ValueBuilder(HolderFactory)
    {
    }
};

//! Enough of a UDF registration context to build TType* out of manifest nodes
//! and print them back.
struct TTypeBuilderEnv {
    TScopedAlloc Alloc;
    TTypeEnvironment Env;
    ITypeInfoHelper::TPtr TypeInfoHelper;
    NYql::TRuntimeSettings::TConstPtr RuntimeSettings;
    TFunctionTypeInfoBuilder Builder;

    TTypeBuilderEnv()
        : Alloc(__LOCATION__)
        , Env(Alloc)
        , TypeInfoHelper(new TTypeInfoHelper())
        , RuntimeSettings(NYql::MakeRuntimeSettings())
        , Builder(
              NYql::UnknownLangVersion,
              *RuntimeSettings,
              Env,
              TypeInfoHelper,
              "",
              /*countersProvider=*/nullptr,
              NYql::NUdf::TSourcePosition())
    {
    }

    TString Format(const TWasmTypeNode& node) {
        TStringBuilder output;
        TTypePrinter printer(*TypeInfoHelper, BuildTypeFromWasmTypeNode(Builder, node));
        printer.Out(output.Out);
        return output;
    }
};

} // namespace

Y_UNIT_TEST_SUITE(TWasmBridgeAbiTest) {

Y_UNIT_TEST(ParseBridgeManifest) {
    const TString manifest = R"({
        "module_name": "Echo",
        "calling_convention": "bridge",
        "required_libraries": ["sdk"],
        "functions": [
            {
                "name": "EchoInt",
                "export": "echo_int",
                "argument_types": [
                    {"value": "int64", "tag": "concrete_type"}
                ],
                "result_type": {"value": "int64", "tag": "concrete_type"}
            }
        ]
    })";
    const auto parsed = ParseManifest(manifest);
    UNIT_ASSERT(parsed.CallingConventionEnum == EWasmCallingConvention::Bridge);
    UNIT_ASSERT(parsed.Functions[0].CallingConvention == EWasmCallingConvention::Bridge);
}

Y_UNIT_TEST(ParseDictType) {
    const TString manifest = R"({
        "module_name": "D",
        "calling_convention": "bridge",
        "functions": [
            {
                "name": "Lookup",
                "argument_types": [
                    {
                        "value": "dict",
                        "tag": "concrete_type",
                        "key": {"value": "string", "tag": "concrete_type"},
                        "payload": {"value": "int64", "tag": "concrete_type"}
                    },
                    {"value": "string", "tag": "concrete_type"}
                ],
                "result_type": {
                    "value": "optional",
                    "tag": "concrete_type",
                    "item": {"value": "int64", "tag": "concrete_type"}
                }
            }
        ]
    })";
    const auto parsed = ParseManifest(manifest);
    UNIT_ASSERT_VALUES_EQUAL(parsed.Functions[0].ArgTypes.size(), 2u);
    UNIT_ASSERT(parsed.Functions[0].ArgTypes[0]->Kind == TWasmTypeNode::EKind::Dict);
    UNIT_ASSERT(parsed.Functions[0].ResultType->Kind == TWasmTypeNode::EKind::Optional);
}

Y_UNIT_TEST(ParseCallableType) {
    const TString manifest = R"({
        "module_name": "C",
        "calling_convention": "bridge",
        "functions": [
            {
                "name": "Run",
                "argument_types": [
                    {
                        "value": "callable",
                        "tag": "concrete_type",
                        "arguments": [
                            {"value": "int64", "tag": "concrete_type"}
                        ],
                        "returns": {"value": "int64", "tag": "concrete_type"}
                    },
                    {"value": "int64", "tag": "concrete_type"}
                ],
                "result_type": {"value": "int64", "tag": "concrete_type"}
            }
        ]
    })";
    const auto parsed = ParseManifest(manifest);
    UNIT_ASSERT(parsed.Functions[0].ArgTypes[0]->Kind == TWasmTypeNode::EKind::Callable);
    UNIT_ASSERT(parsed.Functions[0].ArgTypes[0]->CallableReturns->Leaf == EUdfValueType::Int64);
    UNIT_ASSERT_VALUES_EQUAL(parsed.Functions[0].ArgTypes[0]->Members.size(), 1u);
}

Y_UNIT_TEST(ParseStructuredAndWideLeafTypes) {
    const TString manifest = R"({
        "module_name": "Wide",
        "calling_convention": "bridge",
        "functions": [
            {
                "name": "Wide",
                "argument_types": [
                    {
                        "value": "struct",
                        "tag": "concrete_type",
                        "members": [
                            {"name": "id", "type": {"value": "int32", "tag": "concrete_type"}},
                            {"name": "score", "type": {"value": "float", "tag": "concrete_type"}},
                            {"name": "name", "type": {"value": "utf8", "tag": "concrete_type"}}
                        ]
                    },
                    {
                        "value": "tuple",
                        "tag": "concrete_type",
                        "elements": [
                            {"value": "date", "tag": "concrete_type"},
                            {"value": "decimal", "tag": "concrete_type"}
                        ]
                    },
                    {"value": "resource", "tag": "concrete_type", "resource_tag": "Trie"}
                ],
                "result_type": {
                    "value": "variant",
                    "tag": "concrete_type",
                    "elements": [
                        {"value": "uint32", "tag": "concrete_type"},
                        {"value": "timestamp", "tag": "concrete_type"}
                    ]
                }
            }
        ]
    })";
    const auto parsed = ParseManifest(manifest);
    const auto& function = parsed.Functions[0];
    UNIT_ASSERT_VALUES_EQUAL(function.ArgTypes.size(), 3u);

    const auto& structNode = *function.ArgTypes[0];
    UNIT_ASSERT(structNode.Kind == TWasmTypeNode::EKind::Struct);
    UNIT_ASSERT_VALUES_EQUAL(structNode.Members.size(), 3u);
    UNIT_ASSERT_VALUES_EQUAL(structNode.Members[1].Name, "score");
    UNIT_ASSERT(structNode.Members[1].Type->Leaf == EUdfValueType::Float);
    UNIT_ASSERT(structNode.Members[2].Type->Leaf == EUdfValueType::Utf8);

    const auto& tupleNode = *function.ArgTypes[1];
    UNIT_ASSERT(tupleNode.Kind == TWasmTypeNode::EKind::Tuple);
    UNIT_ASSERT(tupleNode.Members[0].Type->Leaf == EUdfValueType::Date);
    UNIT_ASSERT(tupleNode.Members[1].Type->Leaf == EUdfValueType::Decimal);

    UNIT_ASSERT(function.ArgTypes[2]->Kind == TWasmTypeNode::EKind::Resource);
    UNIT_ASSERT_VALUES_EQUAL(function.ArgTypes[2]->Tag, "Trie");

    UNIT_ASSERT(function.ResultType->Kind == TWasmTypeNode::EKind::Variant);
    UNIT_ASSERT(function.ResultType->Members[0].Type->Leaf == EUdfValueType::Uint32);
    UNIT_ASSERT(function.ResultType->Members[1].Type->Leaf == EUdfValueType::Timestamp);
}

Y_UNIT_TEST(NestedLeavesKeepTheirDeclaredType) {
    const TString manifest = R"({
        "module_name": "Nested",
        "calling_convention": "bridge",
        "functions": [
            {
                "name": "Nested",
                "argument_types": [
                    {
                        "value": "dict",
                        "tag": "concrete_type",
                        "key": {"value": "string", "tag": "concrete_type"},
                        "payload": {"value": "int64", "tag": "concrete_type"}
                    },
                    {
                        "value": "list",
                        "tag": "concrete_type",
                        "item": {
                            "value": "optional",
                            "tag": "concrete_type",
                            "item": {"value": "utf8", "tag": "concrete_type"}
                        }
                    },
                    {
                        "value": "struct",
                        "tag": "concrete_type",
                        "members": [
                            {"name": "id", "type": {"value": "int32", "tag": "concrete_type"}},
                            {"name": "name", "type": {"value": "utf8", "tag": "concrete_type"}}
                        ]
                    },
                    {"value": "string", "tag": "concrete_type"}
                ],
                "result_type": {
                    "value": "optional",
                    "tag": "concrete_type",
                    "item": {"value": "int64", "tag": "concrete_type"}
                }
            }
        ]
    })";
    const auto parsed = ParseManifest(manifest);
    const auto& function = parsed.Functions[0];
    TTypeBuilderEnv types;

    // A leaf inside a container is exactly what the manifest declared: an
    // extra Optional here would stop Dict<String,Int64> from matching the
    // dict a query actually passes.
    UNIT_ASSERT_VALUES_EQUAL(types.Format(*function.ArgTypes[0]), "Dict<String,Int64>");
    UNIT_ASSERT_VALUES_EQUAL(types.Format(*function.ArgTypes[1]), "List<Utf8?>");
    UNIT_ASSERT_VALUES_EQUAL(
        types.Format(*function.ArgTypes[2]),
        "Struct<'id':Int32,'name':Utf8>");
    // A bare leaf argument / result keeps the historical Optional<data> shape.
    UNIT_ASSERT_VALUES_EQUAL(types.Format(*function.ArgTypes[3]), "String?");
    UNIT_ASSERT_VALUES_EQUAL(types.Format(*function.ResultType), "Int64?");
}

Y_UNIT_TEST(RejectBridgeWithTypeConfigCallable) {
    const TString manifest = R"({
        "module_name": "Bad",
        "calling_convention": "bridge",
        "objects": [
            {
                "name": "X",
                "create_export": "x_create",
                "methods": [
                    {
                        "name": "Apply",
                        "export": "x_apply",
                        "yql_binding": "type_config_callable",
                        "argument_types": [],
                        "result_type": {"value": "int64", "tag": "concrete_type"}
                    }
                ]
            }
        ]
    })";
    UNIT_ASSERT_EXCEPTION(ParseManifest(manifest), yexception);
}

Y_UNIT_TEST(EchoIntViaBridgeIntrinsics) {
    EnsureUdfHostIntrinsicsRegistered();
    TMiniKqlEnv mkql;

    auto compartment = CreateEmptyImage();
    compartment->AddSdk(MakeNamedLibrary("sdk", SdkStubWast).Bytecode);

    auto handle = std::make_unique<TQueryCompartmentHandle>();
    handle->Generation = 11;
    handle->BridgeNodes = std::make_unique<TWasmBridgeNodeTable>(handle->Generation);
    handle->Compartment = std::move(compartment);

    TCurrentQueryCompartmentGuard queryGuard(handle.get());
    TCurrentCompartmentGuard compartmentGuard(handle->Compartment.get());
    TWasmUdfInvocationContext context(handle->Compartment.get());
    TCurrentInvocationContextGuard invocationGuard(&context);
    TBridgeValueBuilderGuard vbGuard(*handle->BridgeNodes, &mkql.ValueBuilder);

    const auto moduleObjectCode = CompileModuleObjectCode(
        BridgeEchoIntWast,
        EBytecodeFormat::HumanReadable);
    AddPrecompiledModule(
        handle->Compartment.get(),
        MakeModuleBytecode(BridgeEchoIntWast, moduleObjectCode, EBytecodeFormat::HumanReadable),
        "Echo");

    auto& table = *handle->BridgeNodes;
    const ui64 argHandle = table.Register(
        EBridgeNodeKind::Scalar,
        EBridgeValueKind::Int64,
        nullptr,
        TUnboxedValuePod(i64{123}));

    const auto resultOffset = handle->Compartment->AllocateBytes(sizeof(ui64));
    *PtrFromVM(handle->Compartment.get(), std::bit_cast<ui64*>(resultOffset)) = 0;

    InvokeUdfExport(
        handle->Compartment.get(),
        "echo_int",
        std::bit_cast<uintptr_t>(&context),
        resultOffset,
        {argHandle});

    const ui64 resultHandle = *PtrFromVM(
        handle->Compartment.get(),
        std::bit_cast<ui64*>(resultOffset));
    UNIT_ASSERT(resultHandle != NullBridgeHandle);
    UNIT_ASSERT_VALUES_EQUAL(table.Resolve(resultHandle).Value.Get<i64>(), 123);

    table.Unref(argHandle);
    table.Unref(resultHandle);
    UNIT_ASSERT_VALUES_EQUAL(table.DebugSize(), 0u);
}

Y_UNIT_TEST(EchoStringViaBridgeIntrinsics) {
    EnsureUdfHostIntrinsicsRegistered();
    TMiniKqlEnv mkql;

    auto compartment = CreateEmptyImage();
    compartment->AddSdk(MakeNamedLibrary("sdk", SdkStubWast).Bytecode);

    auto handle = std::make_unique<TQueryCompartmentHandle>();
    handle->Generation = 12;
    handle->BridgeNodes = std::make_unique<TWasmBridgeNodeTable>(handle->Generation);
    handle->Compartment = std::move(compartment);

    TCurrentQueryCompartmentGuard queryGuard(handle.get());
    TCurrentCompartmentGuard compartmentGuard(handle->Compartment.get());
    TWasmUdfInvocationContext context(handle->Compartment.get());
    TCurrentInvocationContextGuard invocationGuard(&context);
    TBridgeValueBuilderGuard vbGuard(*handle->BridgeNodes, &mkql.ValueBuilder);

    const auto moduleObjectCode = CompileModuleObjectCode(
        BridgeEchoStringWast,
        EBytecodeFormat::HumanReadable);
    AddPrecompiledModule(
        handle->Compartment.get(),
        MakeModuleBytecode(BridgeEchoStringWast, moduleObjectCode, EBytecodeFormat::HumanReadable),
        "Echo");

    auto& table = *handle->BridgeNodes;
    auto strValue = mkql.ValueBuilder.NewString(TStringRef("hello-bridge", 12));
    const ui64 argHandle = table.Register(
        EBridgeNodeKind::String,
        EBridgeValueKind::String,
        nullptr,
        std::move(strValue));

    const auto resultOffset = handle->Compartment->AllocateBytes(sizeof(ui64));
    *PtrFromVM(handle->Compartment.get(), std::bit_cast<ui64*>(resultOffset)) = 0;

    InvokeUdfExport(
        handle->Compartment.get(),
        "echo_str",
        std::bit_cast<uintptr_t>(&context),
        resultOffset,
        {argHandle});

    const ui64 resultHandle = *PtrFromVM(
        handle->Compartment.get(),
        std::bit_cast<ui64*>(resultOffset));
    UNIT_ASSERT(resultHandle != NullBridgeHandle);
    const TStringRef out = table.Resolve(resultHandle).Value.AsStringRef();
    UNIT_ASSERT_VALUES_EQUAL(TString(out.Data(), out.Size()), "hello-bridge");

    table.Unref(argHandle);
    table.Unref(resultHandle);
    UNIT_ASSERT_VALUES_EQUAL(table.DebugSize(), 0u);
}

Y_UNIT_TEST(EnsureStringPinsOnceWithRef) {
    EnsureUdfHostIntrinsicsRegistered();
    TMiniKqlEnv mkql;

    auto compartment = CreateEmptyImage();
    compartment->AddSdk(MakeNamedLibrary("sdk", SdkStubWast).Bytecode);

    auto handle = std::make_unique<TQueryCompartmentHandle>();
    handle->Generation = 13;
    handle->BridgeNodes = std::make_unique<TWasmBridgeNodeTable>(handle->Generation);
    handle->Compartment = std::move(compartment);

    TCurrentQueryCompartmentGuard queryGuard(handle.get());
    TCurrentCompartmentGuard compartmentGuard(handle->Compartment.get());
    TWasmUdfInvocationContext context(handle->Compartment.get());
    TCurrentInvocationContextGuard invocationGuard(&context);
    TBridgeValueBuilderGuard vbGuard(*handle->BridgeNodes, &mkql.ValueBuilder);

    const auto moduleObjectCode = CompileModuleObjectCode(
        BridgeEnsureStringWast,
        EBytecodeFormat::HumanReadable);
    AddPrecompiledModule(
        handle->Compartment.get(),
        MakeModuleBytecode(BridgeEnsureStringWast, moduleObjectCode, EBytecodeFormat::HumanReadable),
        "Pin");

    auto& table = *handle->BridgeNodes;
    auto strValue = mkql.ValueBuilder.NewString(
        TStringRef("pin-me-once-long-enough-to-be-boxed-!!!!!!", 42));
    const ui64 argHandle = table.Register(
        EBridgeNodeKind::String,
        EBridgeValueKind::String,
        nullptr,
        std::move(strValue));

    const auto resultOffset = handle->Compartment->AllocateBytes(sizeof(ui64));
    *PtrFromVM(handle->Compartment.get(), std::bit_cast<ui64*>(resultOffset)) = 0;

    InvokeUdfExport(
        handle->Compartment.get(),
        "pin_twice",
        std::bit_cast<uintptr_t>(&context),
        resultOffset,
        {argHandle});

    const ui64 resultHandle = *PtrFromVM(
        handle->Compartment.get(),
        std::bit_cast<ui64*>(resultOffset));
    UNIT_ASSERT(resultHandle != NullBridgeHandle);
    const ui64 firstOffset = static_cast<ui64>(table.Resolve(resultHandle).Value.Get<i64>());
    UNIT_ASSERT(firstOffset != 0);
    UNIT_ASSERT(handle->Resident);
    UNIT_ASSERT_VALUES_EQUAL(handle->Resident->PinCount(), 1u);

    // Simulate end-of-Run Unref while guest BridgeRef keeps the node alive.
    table.Unref(argHandle);
    UNIT_ASSERT(table.DebugSize() >= 1u);

    *PtrFromVM(handle->Compartment.get(), std::bit_cast<ui64*>(resultOffset)) = 0;
    InvokeUdfExport(
        handle->Compartment.get(),
        "pin_twice",
        std::bit_cast<uintptr_t>(&context),
        resultOffset,
        {argHandle});
    const ui64 resultHandle2 = *PtrFromVM(
        handle->Compartment.get(),
        std::bit_cast<ui64*>(resultOffset));
    UNIT_ASSERT(resultHandle2 != NullBridgeHandle);
    const ui64 secondOffset = static_cast<ui64>(table.Resolve(resultHandle2).Value.Get<i64>());
    UNIT_ASSERT_VALUES_EQUAL(firstOffset, secondOffset);

    table.Unref(resultHandle);
    table.Unref(resultHandle2);
    // Drop guest Refs from both pin_twice calls.
    table.Unref(argHandle);
    table.Unref(argHandle);
    UNIT_ASSERT_VALUES_EQUAL(table.DebugSize(), 0u);
}

Y_UNIT_TEST(MakeVariantLeavesItsItemToTheGuest) {
    // Every other Make* leaves its inputs alone, so a guest holding the item in
    // an RAII handle would double-Unref if this one consumed it.
    EnsureUdfHostIntrinsicsRegistered();
    TMiniKqlEnv mkql;

    auto compartment = CreateEmptyImage();
    compartment->AddSdk(MakeNamedLibrary("sdk", SdkStubWast).Bytecode);

    auto handle = std::make_unique<TQueryCompartmentHandle>();
    handle->Generation = 23;
    handle->BridgeNodes = std::make_unique<TWasmBridgeNodeTable>(handle->Generation);
    handle->BridgeNodes->SetTypeInfoHelper(new NKikimr::NMiniKQL::TTypeInfoHelper());
    handle->Compartment = std::move(compartment);

    using NKikimr::NMiniKQL::TDataType;
    auto* i64Type = TDataType::Create(NYql::NUdf::TDataType<i64>::Id, mkql.Env);
    auto* stringType = TDataType::Create(NYql::NUdf::TDataType<char*>::Id, mkql.Env);
    NKikimr::NMiniKQL::TType* elements[] = {i64Type, stringType};
    auto* variantType = NKikimr::NMiniKQL::TVariantType::Create(
        NKikimr::NMiniKQL::TTupleType::Create(2, elements, mkql.Env),
        mkql.Env);

    TCurrentQueryCompartmentGuard queryGuard(handle.get());
    TCurrentCompartmentGuard compartmentGuard(handle->Compartment.get());
    TWasmUdfInvocationContext context(handle->Compartment.get());
    context.ResultType = static_cast<const NYql::NUdf::TType*>(variantType);
    TCurrentInvocationContextGuard invocationGuard(&context);
    TBridgeValueBuilderGuard vbGuard(*handle->BridgeNodes, &mkql.ValueBuilder);

    const auto moduleObjectCode = CompileModuleObjectCode(
        BridgeMakeVariantWast,
        EBytecodeFormat::HumanReadable);
    AddPrecompiledModule(
        handle->Compartment.get(),
        MakeModuleBytecode(BridgeMakeVariantWast, moduleObjectCode, EBytecodeFormat::HumanReadable),
        "Variant");

    auto& table = *handle->BridgeNodes;
    const ui64 itemHandle = table.Register(
        EBridgeNodeKind::Scalar,
        EBridgeValueKind::Int64,
        nullptr,
        TUnboxedValuePod(i64{7}));

    const auto resultOffset = handle->Compartment->AllocateBytes(sizeof(ui64));
    *PtrFromVM(handle->Compartment.get(), std::bit_cast<ui64*>(resultOffset)) = 0;

    InvokeUdfExport(
        handle->Compartment.get(),
        "wrap_variant",
        std::bit_cast<uintptr_t>(&context),
        resultOffset,
        {itemHandle});

    const ui64 resultHandle = *PtrFromVM(
        handle->Compartment.get(),
        std::bit_cast<ui64*>(resultOffset));
    UNIT_ASSERT(resultHandle != NullBridgeHandle);
    UNIT_ASSERT_VALUES_EQUAL(
        table.Resolve(resultHandle).Value.GetVariantItem().Get<i64>(),
        7);

    // The item survived the call, and the single Unref below is what retires it.
    UNIT_ASSERT_VALUES_EQUAL(table.Resolve(itemHandle).Value.Get<i64>(), 7);
    table.Unref(itemHandle);
    table.Unref(resultHandle);
    UNIT_ASSERT_VALUES_EQUAL(table.DebugSize(), 0u);
}

Y_UNIT_TEST(MakeStructResultIsReadableBack) {
    // The node MakeStruct hands out has to carry a type, or the guest cannot
    // inspect what it just built: GetMemberCount and GetElement both need one.
    EnsureUdfHostIntrinsicsRegistered();
    TMiniKqlEnv mkql;

    auto compartment = CreateEmptyImage();
    compartment->AddSdk(MakeNamedLibrary("sdk", SdkStubWast).Bytecode);

    auto handle = std::make_unique<TQueryCompartmentHandle>();
    handle->Generation = 25;
    handle->BridgeNodes = std::make_unique<TWasmBridgeNodeTable>(handle->Generation);
    handle->BridgeNodes->SetTypeInfoHelper(new NKikimr::NMiniKQL::TTypeInfoHelper());
    handle->Compartment = std::move(compartment);

    auto* i64Type = NKikimr::NMiniKQL::TDataType::Create(
        NYql::NUdf::TDataType<i64>::Id, mkql.Env);
    const std::array members{
        NKikimr::NMiniKQL::TStructMember("first", i64Type),
        NKikimr::NMiniKQL::TStructMember("second", i64Type),
    };
    auto* structType = NKikimr::NMiniKQL::TStructType::Create(
        members.size(), members.data(), mkql.Env);

    TCurrentQueryCompartmentGuard queryGuard(handle.get());
    TCurrentCompartmentGuard compartmentGuard(handle->Compartment.get());
    TWasmUdfInvocationContext context(handle->Compartment.get());
    context.ResultType = static_cast<const NYql::NUdf::TType*>(structType);
    TCurrentInvocationContextGuard invocationGuard(&context);
    TBridgeValueBuilderGuard vbGuard(*handle->BridgeNodes, &mkql.ValueBuilder);

    const auto moduleObjectCode = CompileModuleObjectCode(
        BridgeStructRoundTripWast,
        EBytecodeFormat::HumanReadable);
    AddPrecompiledModule(
        handle->Compartment.get(),
        MakeModuleBytecode(BridgeStructRoundTripWast, moduleObjectCode, EBytecodeFormat::HumanReadable),
        "Struct");

    auto& table = *handle->BridgeNodes;
    const ui64 firstHandle = table.Register(
        EBridgeNodeKind::Scalar,
        EBridgeValueKind::Int64,
        nullptr,
        TUnboxedValuePod(i64{4}));
    const ui64 secondHandle = table.Register(
        EBridgeNodeKind::Scalar,
        EBridgeValueKind::Int64,
        nullptr,
        TUnboxedValuePod(i64{7}));

    const auto resultOffset = handle->Compartment->AllocateBytes(sizeof(ui64));
    *PtrFromVM(handle->Compartment.get(), std::bit_cast<ui64*>(resultOffset)) = 0;

    InvokeUdfExport(
        handle->Compartment.get(),
        "round_trip",
        std::bit_cast<uintptr_t>(&context),
        resultOffset,
        {firstHandle, secondHandle});

    const ui64 resultHandle = *PtrFromVM(
        handle->Compartment.get(),
        std::bit_cast<ui64*>(resultOffset));
    UNIT_ASSERT(resultHandle != NullBridgeHandle);
    // members * 100 + second element.
    UNIT_ASSERT_VALUES_EQUAL(table.Resolve(resultHandle).Value.Get<i64>(), 207);
}

Y_UNIT_TEST(GrowingTheArenaRecyclesTheOldChunkTail) {
    auto compartment = CreateEmptyImage();
    compartment->AddSdk(MakeNamedLibrary("sdk", SdkStubWast).Bytecode);
    TCompartmentResidentCache resident(compartment.get());

    const ui64 small = resident.Alloc(64);
    UNIT_ASSERT(small != 0);
    // Larger than what is left in the first chunk, so a second one is opened.
    const ui64 big = resident.Alloc(8ull << 20);
    UNIT_ASSERT(big > small);

    // The tail of the first chunk went back to the free lists, so a block that
    // fits there comes from below the fresh chunk instead of eating into it.
    const ui64 recycled = resident.Alloc(1ull << 20);
    UNIT_ASSERT_C(
        recycled < big,
        TStringBuilder() << "block at " << recycled << " came from the new chunk at " << big
            << " while the tail of the old one was dropped");
}

Y_UNIT_TEST(BridgeRunDepthOutlivesTheInvocationContext) {
    // A callable passed to BridgeRun can lead back into another bridge UDF,
    // which opens a fresh invocation context. Keeping the recursion counter on
    // the query compartment is what stops that from restarting the count.
    auto compartment = CreateEmptyImage();

    auto handle = std::make_unique<TQueryCompartmentHandle>();
    handle->Generation = 24;
    handle->Compartment = std::move(compartment);
    handle->BridgeRunDepth = 3;

    TCurrentQueryCompartmentGuard queryGuard(handle.get());
    TWasmUdfInvocationContext nested(handle->Compartment.get());
    TCurrentInvocationContextGuard invocationGuard(&nested);

    UNIT_ASSERT_VALUES_EQUAL(GetCurrentQueryCompartment()->BridgeRunDepth, 3u);
}

Y_UNIT_TEST(TryReuseRefcountedString) {
    EnsureUdfHostIntrinsicsRegistered();
    TMiniKqlEnv mkql;

    auto handle = std::make_unique<TQueryCompartmentHandle>();
    handle->Generation = 16;
    handle->BridgeNodes = std::make_unique<TWasmBridgeNodeTable>(handle->Generation);

    auto& table = *handle->BridgeNodes;
    // Long enough to be EMarkers::String (not Embedded).
    auto strValue = mkql.ValueBuilder.NewString(
        TStringRef("pin-me-once-long-enough-to-be-a-refcounted-string!!!!", 52));
    const TUnboxedValue keepAlive = strValue;

    const ui64 h1 = table.Register(
        EBridgeNodeKind::String,
        EBridgeValueKind::String,
        nullptr,
        TUnboxedValue(strValue));
    UNIT_ASSERT(keepAlive.IsString());
    UNIT_ASSERT(!keepAlive.IsBoxed());

    // Simulate BridgeRef + end-of-Run Unref: node survives, TryReuse hits.
    table.Ref(h1);
    table.Unref(h1);
    const ui64 h2 = table.TryReuse(keepAlive);
    UNIT_ASSERT_VALUES_EQUAL(h1, h2);

    table.Unref(h1); // drop surviving ref
    UNIT_ASSERT_VALUES_EQUAL(table.DebugSize(), 0u);
    UNIT_ASSERT_VALUES_EQUAL(table.TryReuse(keepAlive), NullBridgeHandle);
}

Y_UNIT_TEST(SubstringsOfOneBufferGetDistinctNodes) {
    EnsureUdfHostIntrinsicsRegistered();
    TMiniKqlEnv mkql;

    auto handle = std::make_unique<TQueryCompartmentHandle>();
    handle->Generation = 99;
    handle->BridgeNodes = std::make_unique<TWasmBridgeNodeTable>(handle->Generation);
    auto& table = *handle->BridgeNodes;

    const TUnboxedValue whole = mkql.ValueBuilder.NewString(
        TStringRef("AAAAAAAAAAAAAAAAAAAAAAAAbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 52));
    // Same TStringValue buffer, different offset/size; each > 14 bytes so
    // SubString does not collapse either into an embedded copy.
    const TUnboxedValue head = mkql.ValueBuilder.SubString(whole, 0, 24);
    const TUnboxedValue tail = mkql.ValueBuilder.SubString(whole, 24, 28);
    UNIT_ASSERT(head.IsString() && tail.IsString());
    UNIT_ASSERT_VALUES_EQUAL(head.AsRawStringValue(), tail.AsRawStringValue());

    const ui64 h1 = table.RegisterOrReuse(
        EBridgeNodeKind::String, EBridgeValueKind::String, nullptr, head);
    const ui64 h2 = table.RegisterOrReuse(
        EBridgeNodeKind::String, EBridgeValueKind::String, nullptr, tail);

    // Distinct values must never alias one node: a node holds exactly one
    // TUnboxedValue, so sharing a handle hands the guest the wrong bytes.
    UNIT_ASSERT_VALUES_UNEQUAL(h1, h2);
    UNIT_ASSERT_VALUES_EQUAL(table.DebugSize(), 2u);

    // And the same value must still be reused.
    UNIT_ASSERT_VALUES_EQUAL(table.TryReuse(tail), h2);
}

Y_UNIT_TEST(AliasingNodeKeepsIdentityOwner) {
    TMiniKqlEnv mkql;

    TWasmBridgeNodeTable table(21);
    auto strValue = mkql.ValueBuilder.NewString(
        TStringRef("identity-owner-string-long-enough-to-be-refcounted!!", 51));
    const TUnboxedValue keepAlive = strValue;
    UNIT_ASSERT(keepAlive.IsString());

    const ui64 owner = table.Register(
        EBridgeNodeKind::String,
        EBridgeValueKind::String,
        nullptr,
        TUnboxedValue(strValue));

    // MakeOptional() over a marker pod yields the very same pod, so the
    // Optional node aliases the string identity without owning it.
    const ui64 alias = table.Register(
        EBridgeNodeKind::Optional,
        EBridgeValueKind::Optional,
        nullptr,
        TUnboxedValue(strValue.MakeOptional()));
    UNIT_ASSERT(owner != alias);

    table.Unref(alias);
    UNIT_ASSERT_VALUES_EQUAL(table.TryReuse(keepAlive), owner);

    table.Unref(owner);
    UNIT_ASSERT_VALUES_EQUAL(table.TryReuse(keepAlive), NullBridgeHandle);
    UNIT_ASSERT_VALUES_EQUAL(table.DebugSize(), 0u);
}

Y_UNIT_TEST(RegisterOrReuseNeverDuplicatesIdentity) {
    TMiniKqlEnv mkql;

    TWasmBridgeNodeTable table(22);
    auto strValue = mkql.ValueBuilder.NewString(
        TStringRef("reuse-me-long-enough-to-be-a-refcounted-string!!!!!!", 51));

    const ui64 first = table.RegisterOrReuse(
        EBridgeNodeKind::String,
        EBridgeValueKind::String,
        nullptr,
        strValue);
    const ui64 second = table.RegisterOrReuse(
        EBridgeNodeKind::String,
        EBridgeValueKind::String,
        nullptr,
        strValue);
    UNIT_ASSERT_VALUES_EQUAL(first, second);
    UNIT_ASSERT_VALUES_EQUAL(table.DebugSize(), 1u);

    table.Unref(first);
    UNIT_ASSERT_VALUES_EQUAL(table.DebugSize(), 1u);
    table.Unref(second);
    UNIT_ASSERT_VALUES_EQUAL(table.DebugSize(), 0u);
}

Y_UNIT_TEST(EnsureStringWithoutRefLosesIdentity) {
    EnsureUdfHostIntrinsicsRegistered();
    TMiniKqlEnv mkql;

    auto compartment = CreateEmptyImage();
    compartment->AddSdk(MakeNamedLibrary("sdk", SdkStubWast).Bytecode);

    auto handle = std::make_unique<TQueryCompartmentHandle>();
    handle->Generation = 14;
    handle->BridgeNodes = std::make_unique<TWasmBridgeNodeTable>(handle->Generation);
    handle->Compartment = std::move(compartment);

    TCurrentQueryCompartmentGuard queryGuard(handle.get());
    TCurrentCompartmentGuard compartmentGuard(handle->Compartment.get());
    TWasmUdfInvocationContext context(handle->Compartment.get());
    TCurrentInvocationContextGuard invocationGuard(&context);
    TBridgeValueBuilderGuard vbGuard(*handle->BridgeNodes, &mkql.ValueBuilder);

    auto& table = *handle->BridgeNodes;
    auto strValue = mkql.ValueBuilder.NewString(TStringRef("ephemeral", 9));
    const TUnboxedValue keepAlive = strValue;
    const ui64 h1 = table.Register(
        EBridgeNodeKind::String,
        EBridgeValueKind::String,
        nullptr,
        TUnboxedValue(strValue));
    table.Unref(h1);
    UNIT_ASSERT_VALUES_EQUAL(table.DebugSize(), 0u);
    UNIT_ASSERT_VALUES_EQUAL(table.TryReuse(keepAlive), NullBridgeHandle);
}

Y_UNIT_TEST(EnsureStringLargeUsesDetachedGrow) {
    EnsureUdfHostIntrinsicsRegistered();
    TMiniKqlEnv mkql;

    auto compartment = CreateEmptyImage();
    compartment->AddSdk(MakeNamedLibrary("sdk", SdkStubWast).Bytecode);

    auto handle = std::make_unique<TQueryCompartmentHandle>();
    handle->Generation = 15;
    handle->BridgeNodes = std::make_unique<TWasmBridgeNodeTable>(handle->Generation);
    handle->Compartment = std::move(compartment);
    handle->Resident = std::make_unique<TCompartmentResidentCache>(handle->Compartment.get());

    TCurrentQueryCompartmentGuard queryGuard(handle.get());
    TCurrentCompartmentGuard compartmentGuard(handle->Compartment.get());
    TBridgeValueBuilderGuard vbGuard(*handle->BridgeNodes, &mkql.ValueBuilder);

    constexpr size_t kSize = 256 * 1024;
    TString blob(kSize, 'Z');
    auto& table = *handle->BridgeNodes;
    auto& resident = *handle->Resident;
    auto strValue = mkql.ValueBuilder.NewString(TStringRef(blob.data(), blob.size()));
    const ui64 argHandle = table.Register(
        EBridgeNodeKind::String,
        EBridgeValueKind::String,
        nullptr,
        std::move(strValue));

    const size_t before = handle->Compartment->GetLinearMemorySize();
    const ui64 offset = EnsureBridgeStringResident(table.Resolve(argHandle), resident);
    UNIT_ASSERT(offset != 0);
    UNIT_ASSERT_VALUES_EQUAL(resident.PinCount(), 1u);
    UNIT_ASSERT(handle->Compartment->GetLinearMemorySize() > before);

    const char* host = PtrFromVM(
        handle->Compartment.get(),
        std::bit_cast<char*>(static_cast<uintptr_t>(offset)),
        kSize);
    UNIT_ASSERT_VALUES_EQUAL(host[0], 'Z');
    UNIT_ASSERT_VALUES_EQUAL(host[kSize - 1], 'Z');

    const ui64 again = EnsureBridgeStringResident(table.Resolve(argHandle), resident);
    UNIT_ASSERT_VALUES_EQUAL(offset, again);

    // The pin outlives the node: the next row reuses the same bytes even if
    // the guest never called BridgeRef.
    table.Unref(argHandle);
    UNIT_ASSERT_VALUES_EQUAL(resident.PinCount(), 1u);
}

Y_UNIT_TEST(ResidentCacheEvictsBeyondBudget) {
    TMiniKqlEnv mkql;

    auto compartment = CreateEmptyImage();
    compartment->AddSdk(MakeNamedLibrary("sdk", SdkStubWast).Bytecode);

    constexpr ui64 kBudget = 4ull << 20;
    constexpr size_t kBlob = 1024 * 1024;
    TCompartmentResidentCache resident(compartment.get(), kBudget);

    TVector<TUnboxedValue> blobs;
    for (int i = 0; i < 12; ++i) {
        TString payload(kBlob, static_cast<char>('a' + i));
        blobs.push_back(mkql.ValueBuilder.NewString(TStringRef(payload.data(), payload.size())));
    }

    for (const auto& blob : blobs) {
        resident.BeginRun();
        const void* key = BridgeIdentityKey(blob);
        UNIT_ASSERT(key != nullptr);
        UNIT_ASSERT(resident.Pin(key, blob, blob.AsStringRef()) != 0);
    }

    UNIT_ASSERT(resident.EvictionCount() > 0);
    UNIT_ASSERT(resident.PinnedBytes() <= kBudget);
    // Evicted blocks come back through the free lists instead of growing.
    UNIT_ASSERT(resident.ArenaBytes() <= 2 * kBudget);
}

Y_UNIT_TEST(PinFencesTheGuestHeapAboveHostBytes) {
    // Pinning grows linear memory on the host and then calls the guest's own
    // "sbrk" to push the allocator break past the new region. That sbrk call is
    // the only guest code the resident cache runs, and this is what it buys:
    // the guest allocator can no longer hand out bytes the host is using.
    TMiniKqlEnv mkql;

    auto compartment = CreateEmptyImage();
    compartment->AddSdk(MakeNamedLibrary("sdk", SdkStubWast).Bytecode);

    const auto memoryBefore = compartment->GetLinearMemorySize();

    TCompartmentResidentCache resident(compartment.get());
    TString blob(4u << 20, 'Z');
    const TUnboxedValue value = mkql.ValueBuilder.NewString(
        TStringRef(blob.data(), blob.size()));
    const void* key = BridgeIdentityKey(value);
    UNIT_ASSERT(key != nullptr);

    const ui64 offset = resident.Pin(key, value, value.AsStringRef());
    UNIT_ASSERT(offset != 0);
    UNIT_ASSERT(compartment->GetLinearMemorySize() > memoryBefore);

    const ui64 guestBlock = compartment->AllocateBytes(1024);
    UNIT_ASSERT_C(
        guestBlock >= offset + blob.size(),
        TStringBuilder() << "guest malloc returned " << guestBlock
            << ", inside the pin at " << offset << ".." << (offset + blob.size()));

    // Pushing the break above the arena leaves it at the top of linear memory,
    // so the block malloc just handed out is only usable if "sbrk" grew memory
    // on the way. PtrFromVM bounds-checks, so this is where a pure pointer bump
    // stops being a plausible sbrk.
    char* guestBytes = PtrFromVM(
        compartment.get(),
        std::bit_cast<char*>(static_cast<uintptr_t>(guestBlock)),
        1024);
    std::memset(guestBytes, 'g', 1024);

    // Nothing the guest wrote may land inside the pin.
    const char* pinned = PtrFromVM(
        compartment.get(),
        std::bit_cast<char*>(static_cast<uintptr_t>(offset)),
        blob.size());
    UNIT_ASSERT_VALUES_EQUAL(TStringBuf(pinned, blob.size()), TStringBuf(blob));
}

Y_UNIT_TEST(PinRefusesAGuestItCannotFenceOff) {
    // Without "sbrk" there is no way to keep guest malloc out of the arena, and
    // silently sharing the bytes would corrupt whichever side writes second.
    TMiniKqlEnv mkql;

    auto compartment = CreateEmptyImage();
    compartment->AddSdk(MakeNamedLibrary("sdk", SdkStubNoSbrkWast).Bytecode);

    TCompartmentResidentCache resident(compartment.get());
    const TUnboxedValue value = mkql.ValueBuilder.NewString(
        TStringRef("pin-me-if-you-can-but-you-cannot-fence-me", 41));
    const void* key = BridgeIdentityKey(value);
    UNIT_ASSERT(key != nullptr);

    UNIT_ASSERT_EXCEPTION_CONTAINS(
        resident.Pin(key, value, value.AsStringRef()),
        yexception,
        "must export \"sbrk\"");
}

Y_UNIT_TEST(RowLoopWithoutRefDoesNotGrowMemory) {
    TMiniKqlEnv mkql;

    auto compartment = CreateEmptyImage();
    compartment->AddSdk(MakeNamedLibrary("sdk", SdkStubWast).Bytecode);

    auto handle = std::make_unique<TQueryCompartmentHandle>();
    handle->Generation = 17;
    handle->BridgeNodes = std::make_unique<TWasmBridgeNodeTable>(handle->Generation);
    handle->Compartment = std::move(compartment);
    handle->Resident = std::make_unique<TCompartmentResidentCache>(handle->Compartment.get());

    TCurrentQueryCompartmentGuard queryGuard(handle.get());
    TCurrentCompartmentGuard compartmentGuard(handle->Compartment.get());
    TBridgeValueBuilderGuard vbGuard(*handle->BridgeNodes, &mkql.ValueBuilder);

    TString blob(512 * 1024, 'Q');
    // One column value shared by every row, as a scan over a constant argument
    // hands it out.
    const TUnboxedValue column = mkql.ValueBuilder.NewString(
        TStringRef(blob.data(), blob.size()));

    auto& table = *handle->BridgeNodes;
    auto& resident = *handle->Resident;

    size_t memoryAfterFirstRow = 0;
    ui64 firstOffset = 0;
    // The guest never calls BridgeRef, so the node dies at the end of each row.
    for (int row = 0; row < 64; ++row) {
        resident.BeginRun();
        const ui64 argHandle = table.RegisterOrReuse(
            EBridgeNodeKind::String,
            EBridgeValueKind::String,
            nullptr,
            TUnboxedValue(column));
        const ui64 offset = EnsureBridgeStringResident(table.Resolve(argHandle), resident);
        UNIT_ASSERT(offset != 0);
        table.Unref(argHandle);

        if (row == 0) {
            firstOffset = offset;
            memoryAfterFirstRow = handle->Compartment->GetLinearMemorySize();
        } else {
            UNIT_ASSERT_VALUES_EQUAL(offset, firstOffset);
            UNIT_ASSERT_VALUES_EQUAL(
                handle->Compartment->GetLinearMemorySize(),
                memoryAfterFirstRow);
        }
    }

    UNIT_ASSERT_VALUES_EQUAL(resident.PinCount(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(resident.EvictionCount(), 0u);
}

Y_UNIT_TEST(KindsFollowDeclaredTypes) {
    TMiniKqlEnv mkql;
    NYql::NUdf::ITypeInfoHelper::TPtr helper = new NKikimr::NMiniKQL::TTypeInfoHelper();

    using NKikimr::NMiniKQL::TDataType;
    auto* i64Type = TDataType::Create(NYql::NUdf::TDataType<i64>::Id, mkql.Env);
    auto* ui32Type = TDataType::Create(NYql::NUdf::TDataType<ui32>::Id, mkql.Env);
    auto* floatType = TDataType::Create(NYql::NUdf::TDataType<float>::Id, mkql.Env);
    auto* utf8Type = TDataType::Create(NYql::NUdf::TDataType<NYql::NUdf::TUtf8>::Id, mkql.Env);
    auto* dateType = TDataType::Create(NYql::NUdf::TDataType<NYql::NUdf::TDate>::Id, mkql.Env);
    auto* stringType = TDataType::Create(NYql::NUdf::TDataType<char*>::Id, mkql.Env);

    const auto kindOf = [&](NKikimr::NMiniKQL::TType* type) {
        return BridgeKindsFromType(static_cast<const NYql::NUdf::TType*>(type), helper.Get()).Value;
    };

    // Leaf data types keep their own kind instead of collapsing to Int64.
    UNIT_ASSERT(kindOf(ui32Type) == EBridgeValueKind::Uint32);
    UNIT_ASSERT(kindOf(floatType) == EBridgeValueKind::Float);
    UNIT_ASSERT(kindOf(utf8Type) == EBridgeValueKind::Utf8);
    UNIT_ASSERT(kindOf(dateType) == EBridgeValueKind::Date);

    // Nested containers stay traversable instead of turning into Callable.
    auto* innerDict = NKikimr::NMiniKQL::TDictType::Create(stringType, i64Type, mkql.Env);
    auto* outerDict = NKikimr::NMiniKQL::TDictType::Create(stringType, innerDict, mkql.Env);
    UNIT_ASSERT(kindOf(outerDict) == EBridgeValueKind::Dict);
    UNIT_ASSERT(kindOf(NKikimr::NMiniKQL::TListType::Create(
        NKikimr::NMiniKQL::TOptionalType::Create(i64Type, mkql.Env), mkql.Env)) == EBridgeValueKind::List);

    NKikimr::NMiniKQL::TType* elements[] = {i64Type, stringType, floatType};
    auto* tuple = NKikimr::NMiniKQL::TTupleType::Create(3, elements, mkql.Env);
    UNIT_ASSERT(kindOf(tuple) == EBridgeValueKind::Tuple);
    UNIT_ASSERT(kindOf(NKikimr::NMiniKQL::TVariantType::Create(tuple, mkql.Env)) == EBridgeValueKind::Variant);
    UNIT_ASSERT(kindOf(NKikimr::NMiniKQL::TResourceType::Create("Trie", mkql.Env)) == EBridgeValueKind::Resource);

    // Optional over data is represented like the data itself, and reported so.
    UNIT_ASSERT(kindOf(NKikimr::NMiniKQL::TOptionalType::Create(stringType, mkql.Env))
        == EBridgeValueKind::String);
    UNIT_ASSERT(kindOf(NKikimr::NMiniKQL::TOptionalType::Create(innerDict, mkql.Env))
        == EBridgeValueKind::Optional);
}

Y_UNIT_TEST(UserDataSurvivesNodeDeath) {
    TMiniKqlEnv mkql;

    auto compartment = CreateEmptyImage();
    compartment->AddSdk(MakeNamedLibrary("sdk", SdkStubWast).Bytecode);
    TCompartmentResidentCache resident(compartment.get());

    TWasmBridgeNodeTable table(31);
    auto blob = mkql.ValueBuilder.NewString(
        TStringRef("dictionary-payload-long-enough-to-be-refcounted!!!!!", 51));

    const ui64 firstRow = table.Register(
        EBridgeNodeKind::String,
        EBridgeValueKind::String,
        nullptr,
        TUnboxedValue(blob));
    const void* key = BridgeIdentityKey(table.Resolve(firstRow).Value);
    UNIT_ASSERT(key != nullptr);
    UNIT_ASSERT_VALUES_EQUAL(resident.GetUserData(key), 0u);
    resident.SetUserData(key, table.Resolve(firstRow).Value, 0xC0FFEEull);

    // End of Run: the node is gone, the guest never called BridgeRef.
    table.Unref(firstRow);
    UNIT_ASSERT_VALUES_EQUAL(table.DebugSize(), 0u);

    const ui64 secondRow = table.RegisterOrReuse(
        EBridgeNodeKind::String,
        EBridgeValueKind::String,
        nullptr,
        blob);
    UNIT_ASSERT(secondRow != firstRow);
    UNIT_ASSERT_VALUES_EQUAL(
        resident.GetUserData(BridgeIdentityKey(table.Resolve(secondRow).Value)),
        0xC0FFEEull);
    table.Unref(secondRow);
}

Y_UNIT_TEST(DroppedUserDataIsQueuedForGuest) {
    TMiniKqlEnv mkql;

    auto compartment = CreateEmptyImage();
    compartment->AddSdk(MakeNamedLibrary("sdk", SdkStubWast).Bytecode);
    TCompartmentResidentCache resident(compartment.get());

    TVector<TUnboxedValue> values;
    for (int i = 0; i < 1100; ++i) {
        const TString payload = TStringBuilder()
            << "user-data-owner-long-enough-to-be-refcounted-" << i;
        values.push_back(mkql.ValueBuilder.NewString(TStringRef(payload.data(), payload.size())));
        resident.SetUserData(
            BridgeIdentityKey(values.back()),
            values.back(),
            static_cast<ui64>(i + 1));
    }

    ui64 released = 0;
    UNIT_ASSERT(resident.PopReleasedUserData(released));
    // Oldest entries are dropped first, so the guest sees them in order.
    UNIT_ASSERT_VALUES_EQUAL(released, 1u);
    UNIT_ASSERT(resident.UserDataCount() <= 1024u);
}

Y_UNIT_TEST(EnsureStringAcceptsEveryStringKind) {
    TMiniKqlEnv mkql;

    auto compartment = CreateEmptyImage();
    compartment->AddSdk(MakeNamedLibrary("sdk", SdkStubWast).Bytecode);
    TCompartmentResidentCache resident(compartment.get());

    TWasmBridgeNodeTable table(41);
    const TStringBuf text = "utf8-payload-long-enough-to-be-a-refcounted-string!!";
    for (const auto kind : {
             EBridgeValueKind::String,
             EBridgeValueKind::Utf8,
             EBridgeValueKind::Yson,
             EBridgeValueKind::Json,
         })
    {
        const ui64 h = table.Register(
            EBridgeNodeKind::String,
            kind,
            nullptr,
            mkql.ValueBuilder.NewString(TStringRef(text.data(), text.size())));
        UNIT_ASSERT(EnsureBridgeStringResident(table.Resolve(h), resident) != 0);
        table.Unref(h);
    }

    const ui64 scalar = table.Register(
        EBridgeNodeKind::Scalar,
        EBridgeValueKind::Int64,
        nullptr,
        TUnboxedValuePod(i64{1}));
    UNIT_ASSERT_EXCEPTION(
        EnsureBridgeStringResident(table.Resolve(scalar), resident),
        yexception);
    table.Unref(scalar);
}

Y_UNIT_TEST(ResidentFreeRejectsDoubleAndForeignOffsets) {
    auto compartment = CreateEmptyImage();
    compartment->AddSdk(MakeNamedLibrary("sdk", SdkStubWast).Bytecode);
    TCompartmentResidentCache resident(compartment.get());

    const ui64 guestBlock = resident.AllocGuest(128);
    UNIT_ASSERT(guestBlock != 0);
    resident.FreeGuest(guestBlock);
    // A second release must not put the same offset on the free list twice:
    // two owners would then be handed the very same bytes.
    UNIT_ASSERT_EXCEPTION(resident.FreeGuest(guestBlock), yexception);

    // Host-owned blocks (the per-Run result slot) are not the guest's to free.
    const ui64 hostBlock = resident.Alloc(128);
    UNIT_ASSERT(hostBlock != 0);
    UNIT_ASSERT_EXCEPTION(resident.FreeGuest(hostBlock), yexception);
    resident.Free(hostBlock);
    UNIT_ASSERT_EXCEPTION(resident.Free(hostBlock), yexception);
}

Y_UNIT_TEST(ResidentFreeRejectsPinnedOffsets) {
    TMiniKqlEnv mkql;

    auto compartment = CreateEmptyImage();
    compartment->AddSdk(MakeNamedLibrary("sdk", SdkStubWast).Bytecode);
    TCompartmentResidentCache resident(compartment.get());

    const TString blob(4096, 'P');
    const TUnboxedValue value = mkql.ValueBuilder.NewString(
        TStringRef(blob.data(), blob.size()));
    const void* key = BridgeIdentityKey(value);
    UNIT_ASSERT(key != nullptr);

    // BridgeEnsureString hands this offset to the guest, so the guest knows it.
    const ui64 pinned = resident.Pin(key, value, value.AsStringRef());
    UNIT_ASSERT(pinned != 0);
    UNIT_ASSERT_EXCEPTION(resident.FreeGuest(pinned), yexception);
    UNIT_ASSERT_VALUES_EQUAL(resident.Pin(key, value, value.AsStringRef()), pinned);
}

Y_UNIT_TEST(ResidentScratchIsReusedAcrossRuns) {
    TMiniKqlEnv mkql;

    auto compartment = CreateEmptyImage();
    compartment->AddSdk(MakeNamedLibrary("sdk", SdkStubWast).Bytecode);

    TCompartmentResidentCache resident(compartment.get());
    const TString row = "short-per-row-value";

    resident.BeginRun();
    const ui64 first = resident.PinScratch(TStringRef(row.data(), row.size()));
    UNIT_ASSERT(first != 0);

    const ui64 arenaAfterFirst = resident.ArenaBytes();
    for (int i = 0; i < 1000; ++i) {
        resident.BeginRun();
        UNIT_ASSERT_VALUES_EQUAL(resident.PinScratch(TStringRef(row.data(), row.size())), first);
    }
    UNIT_ASSERT_VALUES_EQUAL(resident.ArenaBytes(), arenaAfterFirst);
    UNIT_ASSERT_VALUES_EQUAL(resident.PinCount(), 0u);
}

Y_UNIT_TEST(AllocGuestRespectsResidentBudget) {
    // Pins evict under pressure; guest AllocResident has nothing to evict, so
    // the same budget has to refuse rather than grow linear memory forever.
    auto compartment = CreateEmptyImage();
    compartment->AddSdk(MakeNamedLibrary("sdk", SdkStubWast).Bytecode);

    constexpr ui64 kBudget = 1ull << 20;
    TCompartmentResidentCache resident(compartment.get(), kBudget);

    const ui64 first = resident.AllocGuest(kBudget / 2);
    UNIT_ASSERT(first != 0);
    UNIT_ASSERT(resident.GuestBytes() <= kBudget);

    UNIT_ASSERT_EXCEPTION_CONTAINS(
        resident.AllocGuest(kBudget),
        yexception,
        "resident budget");

    resident.FreeGuest(first);
    UNIT_ASSERT_VALUES_EQUAL(resident.GuestBytes(), 0u);
    // After freeing, the same budget is available again.
    UNIT_ASSERT(resident.AllocGuest(kBudget / 2) != 0);
}

Y_UNIT_TEST(PinScratchRespectsResidentBudget) {
    auto compartment = CreateEmptyImage();
    compartment->AddSdk(MakeNamedLibrary("sdk", SdkStubWast).Bytecode);

    constexpr ui64 kBudget = 256ull << 10;
    TCompartmentResidentCache resident(compartment.get(), kBudget);
    resident.BeginRun();

    const TString blob(kBudget + 1, 'S');
    UNIT_ASSERT_EXCEPTION_CONTAINS(
        resident.PinScratch(TStringRef(blob.data(), blob.size())),
        yexception,
        "resident budget");
}

} // Y_UNIT_TEST_SUITE
