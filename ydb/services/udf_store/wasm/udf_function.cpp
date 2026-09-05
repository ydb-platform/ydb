#include "udf_function.h"

#include "bridge_node_table.h"
#include "bridge_resident.h"
#include "bridge_types.h"
#include "compartment_manager.h"
#include "invocation_context.h"
#include "registry_helpers.h"
#include "udf_configured_callable.h"

#include <yql/essentials/public/decimal/yql_decimal.h>
#include <yql/essentials/public/udf/udf_type_builder.h>
#include <yql/essentials/public/udf/udf_data_type.h>
#include <yql/essentials/minikql/mkql_terminator.h>
#include <yql/essentials/public/issue/yql_issue.h>

#include <ydb/services/udf_store/wasm/abi/udf_cpp_abi.h>
#include <ydb/library/wasm/api/compartment.h>
#include <ydb/library/wasm/api/data_transfer.h>
#include <ydb/library/wasm/api/pointer.h>

#include <util/generic/scope.h>
#include <util/generic/yexception.h>
#include <util/string/builder.h>

#include <algorithm>

namespace NKikimr::NUdfStore::NWasm {

using namespace NYql::NUdf;
using namespace NYdb::NWasm;
using EAbiValueType = NYdb::NUdfStore::NAbi::EValueType;
using EAbiValueFlags = NYdb::NUdfStore::NAbi::EValueFlags;

namespace {

void WasmError(const std::exception& ex, TStringRef name, const IValueBuilder* valueBuilder) {
    Y_UNUSED(valueBuilder);
    const auto msg = TStringBuilder() << name << "(); ex: " << ex.what();
    UdfTerminate(msg.c_str());
}

TType* BuildLeafDataType(IFunctionTypeInfoBuilder& builder, EUdfValueType type) {
    switch (type) {
        case EUdfValueType::Null:
            return builder.Null();
        case EUdfValueType::Int64:
            return builder.Primitive(TDataType<i64>::Id);
        case EUdfValueType::Uint64:
            return builder.Primitive(TDataType<ui64>::Id);
        case EUdfValueType::Double:
            return builder.Primitive(TDataType<double>::Id);
        case EUdfValueType::Boolean:
            return builder.Primitive(TDataType<bool>::Id);
        case EUdfValueType::String:
            return builder.Primitive(TDataType<char*>::Id);
        case EUdfValueType::Int32:
            return builder.Primitive(TDataType<i32>::Id);
        case EUdfValueType::Uint32:
            return builder.Primitive(TDataType<ui32>::Id);
        case EUdfValueType::Float:
            return builder.Primitive(TDataType<float>::Id);
        case EUdfValueType::Utf8:
            return builder.Primitive(TDataType<TUtf8>::Id);
        case EUdfValueType::Date:
            return builder.Primitive(TDataType<TDate>::Id);
        case EUdfValueType::Datetime:
            return builder.Primitive(TDataType<TDatetime>::Id);
        case EUdfValueType::Timestamp:
            return builder.Primitive(TDataType<TTimestamp>::Id);
        case EUdfValueType::Decimal:
            // The manifest carries no precision/scale, so scale 0 is what a
            // UDF gets: BridgeCopyDecimal hands over the raw unscaled 128-bit
            // payload and there is no BridgeMakeDecimal, so the declared scale
            // never changes what crosses the boundary — only how YQL reads the
            // argument it passes in. A UDF that needs a real scale needs a
            // manifest field for it first.
            return builder.Decimal(NYql::NDecimal::MaxPrecision, 0);
    }
    return builder.Null();
}

//! Keep in sync with MaxManifestTypeDepth in manifest.cpp: a tree that got
//! past the parser (or was built by hand in a test) still must not recurse
//! without bound while constructing MiniKQL types.
constexpr ui32 MaxWasmTypeNodeDepth = 32;

} // namespace

TType* BuildTypeFromWasmTypeNode(
    IFunctionTypeInfoBuilder& builder,
    const TWasmTypeNode& node,
    bool topLevel,
    ui32 depth)
{
    if (depth > MaxWasmTypeNodeDepth) {
        ythrow yexception()
            << "Wasm type nesting exceeds " << MaxWasmTypeNodeDepth << " levels";
    }
    switch (node.Kind) {
        case TWasmTypeNode::EKind::Leaf:
            if (node.Leaf == EUdfValueType::Null) {
                return builder.Null();
            }
            if (!topLevel) {
                // Nested leaves are exactly what the manifest says: wrapping
                // them too would turn Dict<String,Int64> into
                // Dict<Optional<String>,Optional<Int64>>.
                return BuildLeafDataType(builder, node.Leaf);
            }
            // A bare leaf argument / result stays Optional<data> so that
            // unversioned_value signatures keep their historical shape.
            return builder.Optional()->Item(BuildLeafDataType(builder, node.Leaf)).Build();
        case TWasmTypeNode::EKind::Optional: {
            Y_ENSURE(node.Item);
            return builder.Optional()
                ->Item(BuildTypeFromWasmTypeNode(builder, *node.Item, /*topLevel*/ false, depth + 1))
                .Build();
        }
        case TWasmTypeNode::EKind::List: {
            Y_ENSURE(node.Item);
            return builder.List()
                ->Item(BuildTypeFromWasmTypeNode(builder, *node.Item, /*topLevel*/ false, depth + 1))
                .Build();
        }
        case TWasmTypeNode::EKind::Dict: {
            Y_ENSURE(node.Key && node.Payload);
            return builder.Dict()
                ->Key(BuildTypeFromWasmTypeNode(builder, *node.Key, /*topLevel*/ false, depth + 1))
                .Value(BuildTypeFromWasmTypeNode(builder, *node.Payload, /*topLevel*/ false, depth + 1))
                .Build();
        }
        case TWasmTypeNode::EKind::Tuple: {
            auto tuple = builder.Tuple(node.Members.size());
            for (const auto& member : node.Members) {
                Y_ENSURE(member.Type);
                tuple->Add(BuildTypeFromWasmTypeNode(
                    builder, *member.Type, /*topLevel*/ false, depth + 1));
            }
            return tuple->Build();
        }
        case TWasmTypeNode::EKind::Struct: {
            auto members = builder.Struct(node.Members.size());
            for (const auto& member : node.Members) {
                Y_ENSURE(member.Type);
                members->AddField(
                    TStringRef(member.Name.data(), member.Name.size()),
                    BuildTypeFromWasmTypeNode(
                        builder, *member.Type, /*topLevel*/ false, depth + 1),
                    nullptr);
            }
            return members->Build();
        }
        case TWasmTypeNode::EKind::Variant: {
            Y_ENSURE(!node.Members.empty());
            const bool named = !node.Members.front().Name.empty();
            TType* underlying = nullptr;
            if (named) {
                auto members = builder.Struct(node.Members.size());
                for (const auto& member : node.Members) {
                    Y_ENSURE(member.Type);
                    members->AddField(
                        TStringRef(member.Name.data(), member.Name.size()),
                        BuildTypeFromWasmTypeNode(
                            builder, *member.Type, /*topLevel*/ false, depth + 1),
                        nullptr);
                }
                underlying = members->Build();
            } else {
                auto tuple = builder.Tuple(node.Members.size());
                for (const auto& member : node.Members) {
                    Y_ENSURE(member.Type);
                    tuple->Add(BuildTypeFromWasmTypeNode(
                        builder, *member.Type, /*topLevel*/ false, depth + 1));
                }
                underlying = tuple->Build();
            }
            return builder.Variant()->Over(underlying).Build();
        }
        case TWasmTypeNode::EKind::Resource:
            return builder.Resource(TStringRef(node.Tag.data(), node.Tag.size()));
        case TWasmTypeNode::EKind::Callable: {
            Y_ENSURE(node.CallableReturns);
            auto callable = builder.Callable(node.Members.size());
            callable->Returns(BuildTypeFromWasmTypeNode(
                builder, *node.CallableReturns, /*topLevel*/ false, depth + 1));
            for (const auto& arg : node.Members) {
                Y_ENSURE(arg.Type);
                callable->Arg(BuildTypeFromWasmTypeNode(
                    builder, *arg.Type, /*topLevel*/ false, depth + 1));
            }
            return callable->Build();
        }
    }
    return builder.Null();
}

void BridgeKindsFromTypeNode(
    const TWasmTypeNode& node,
    EBridgeNodeKind& outNodeKind,
    EBridgeValueKind& outValueKind)
{
    switch (node.Kind) {
        case TWasmTypeNode::EKind::Leaf:
            switch (node.Leaf) {
                case EUdfValueType::Null:
                    outNodeKind = EBridgeNodeKind::Scalar;
                    outValueKind = EBridgeValueKind::Null;
                    return;
                case EUdfValueType::Int64:
                    outNodeKind = EBridgeNodeKind::Scalar;
                    outValueKind = EBridgeValueKind::Int64;
                    return;
                case EUdfValueType::Uint64:
                    outNodeKind = EBridgeNodeKind::Scalar;
                    outValueKind = EBridgeValueKind::Uint64;
                    return;
                case EUdfValueType::Double:
                    outNodeKind = EBridgeNodeKind::Scalar;
                    outValueKind = EBridgeValueKind::Double;
                    return;
                case EUdfValueType::Boolean:
                    outNodeKind = EBridgeNodeKind::Scalar;
                    outValueKind = EBridgeValueKind::Boolean;
                    return;
                case EUdfValueType::String:
                    outNodeKind = EBridgeNodeKind::String;
                    outValueKind = EBridgeValueKind::String;
                    return;
                case EUdfValueType::Int32:
                    outNodeKind = EBridgeNodeKind::Scalar;
                    outValueKind = EBridgeValueKind::Int32;
                    return;
                case EUdfValueType::Uint32:
                    outNodeKind = EBridgeNodeKind::Scalar;
                    outValueKind = EBridgeValueKind::Uint32;
                    return;
                case EUdfValueType::Float:
                    outNodeKind = EBridgeNodeKind::Scalar;
                    outValueKind = EBridgeValueKind::Float;
                    return;
                case EUdfValueType::Utf8:
                    outNodeKind = EBridgeNodeKind::String;
                    outValueKind = EBridgeValueKind::Utf8;
                    return;
                case EUdfValueType::Date:
                    outNodeKind = EBridgeNodeKind::Scalar;
                    outValueKind = EBridgeValueKind::Date;
                    return;
                case EUdfValueType::Datetime:
                    outNodeKind = EBridgeNodeKind::Scalar;
                    outValueKind = EBridgeValueKind::Datetime;
                    return;
                case EUdfValueType::Timestamp:
                    outNodeKind = EBridgeNodeKind::Scalar;
                    outValueKind = EBridgeValueKind::Timestamp;
                    return;
                case EUdfValueType::Decimal:
                    outNodeKind = EBridgeNodeKind::Scalar;
                    outValueKind = EBridgeValueKind::Decimal;
                    return;
            }
            return;
        case TWasmTypeNode::EKind::Optional:
            outNodeKind = EBridgeNodeKind::Optional;
            outValueKind = EBridgeValueKind::Optional;
            return;
        case TWasmTypeNode::EKind::List:
            outNodeKind = EBridgeNodeKind::List;
            outValueKind = EBridgeValueKind::List;
            return;
        case TWasmTypeNode::EKind::Dict:
            outNodeKind = EBridgeNodeKind::Dict;
            outValueKind = EBridgeValueKind::Dict;
            return;
        case TWasmTypeNode::EKind::Tuple:
            outNodeKind = EBridgeNodeKind::Tuple;
            outValueKind = EBridgeValueKind::Tuple;
            return;
        case TWasmTypeNode::EKind::Struct:
            outNodeKind = EBridgeNodeKind::Struct;
            outValueKind = EBridgeValueKind::Struct;
            return;
        case TWasmTypeNode::EKind::Variant:
            outNodeKind = EBridgeNodeKind::Variant;
            outValueKind = EBridgeValueKind::Variant;
            return;
        case TWasmTypeNode::EKind::Resource:
            outNodeKind = EBridgeNodeKind::Resource;
            outValueKind = EBridgeValueKind::Resource;
            return;
        case TWasmTypeNode::EKind::Callable:
            outNodeKind = EBridgeNodeKind::Callable;
            outValueKind = EBridgeValueKind::Callable;
            return;
    }
    outNodeKind = EBridgeNodeKind::Unknown;
    outValueKind = EBridgeValueKind::Null;
}

namespace {

struct TPreparedArg {
    TPreparedUdfArg Storage;
};

TPreparedArg PrepareArgFromUnboxed(
    IWebAssemblyCompartment* compartment,
    const TUnboxedValuePod& arg,
    EUdfValueType expectedType)
{
    TPreparedArg prepared;
    auto value = MakeEmptyValue();

    if (!arg) {
        value.Type = EAbiValueType::Null;
    } else {
        switch (expectedType) {
            case EUdfValueType::Null:
                value.Type = EAbiValueType::Null;
                break;
            case EUdfValueType::Int64:
                value.Type = EAbiValueType::Int64;
                value.Data.Int64 = arg.Get<i64>();
                break;
            case EUdfValueType::Uint64:
                value.Type = EAbiValueType::Uint64;
                value.Data.Uint64 = arg.Get<ui64>();
                break;
            case EUdfValueType::Double:
                value.Type = EAbiValueType::Double;
                value.Data.Double = arg.Get<double>();
                break;
            case EUdfValueType::Boolean:
                value.Type = EAbiValueType::Boolean;
                value.Data.Boolean = arg.Get<bool>() ? 1 : 0;
                break;
            case EUdfValueType::String: {
                const TStringBuf string = arg.AsStringRef();
                prepared.Storage.StringGuard = CopyIntoCompartment(string, compartment);
                value.Type = EAbiValueType::String;
                value.Length = static_cast<ui32>(string.size());
                value.Data.String = std::bit_cast<char*>(prepared.Storage.StringGuard.GetCopiedOffset());
                break;
            }
            default:
                ythrow yexception()
                    << "Wasm UDF argument type " << ValueTypeToString(expectedType)
                    << " requires calling_convention=bridge";
        }
    }

    const auto offset = compartment->AllocateBytes(sizeof(TUnversionedValue));
    prepared.Storage.ValueGuard = TCopyGuard(compartment, offset);
    prepared.Storage.Offset = offset;
    StoreValue(compartment, offset, value);
    return prepared;
}

ui64 RegisterBridgeArg(
    TWasmBridgeNodeTable& table,
    const TUnboxedValuePod& arg,
    const TWasmTypeNode* typeNode,
    TType* mkqlType)
{
    EBridgeNodeKind nodeKind = EBridgeNodeKind::Scalar;
    EBridgeValueKind valueKind = EBridgeValueKind::Null;
    if (typeNode) {
        BridgeKindsFromTypeNode(*typeNode, nodeKind, valueKind);
    } else if (!arg) {
        valueKind = EBridgeValueKind::Null;
    } else if (arg.IsString()) {
        nodeKind = EBridgeNodeKind::String;
        valueKind = EBridgeValueKind::String;
    } else if (arg.IsBoxed()) {
        nodeKind = EBridgeNodeKind::Callable;
        valueKind = EBridgeValueKind::Callable;
    } else {
        valueKind = EBridgeValueKind::Int64;
    }

    // Leaf Optional<data> from BuildTypeFromWasmTypeNode: empty → null handle
    // is wrong for Optional kind; empty optional is a real node with empty value.
    if (typeNode && typeNode->Kind == TWasmTypeNode::EKind::Leaf && !arg) {
        // Historical optional leaf: empty means Null.
        return table.Register(
            EBridgeNodeKind::Scalar,
            EBridgeValueKind::Null,
            mkqlType,
            {});
    }

    return table.RegisterOrReuse(nodeKind, valueKind, mkqlType, arg);
}

} // namespace

TUnboxedValue ReadResultUnboxed(
    const IValueBuilder* valueBuilder,
    IWebAssemblyCompartment* compartment,
    uintptr_t resultOffset,
    EUdfValueType expectedType)
{
    const auto result = *PtrFromVM(compartment, std::bit_cast<TUnversionedValue*>(resultOffset));
    if (result.Type == EAbiValueType::Null) {
        return {};
    }

    switch (expectedType) {
        case EUdfValueType::Null:
            return {};
        case EUdfValueType::Int64:
            if (result.Type != EAbiValueType::Int64) {
                ythrow yexception()
                    << "Wasm UDF returned wrong value type for Int64 result: "
                    << static_cast<int>(result.Type);
            }
            return TUnboxedValuePod(result.Data.Int64);
        case EUdfValueType::Uint64:
            if (result.Type != EAbiValueType::Uint64) {
                ythrow yexception()
                    << "Wasm UDF returned wrong value type for Uint64 result: "
                    << static_cast<int>(result.Type);
            }
            return TUnboxedValuePod(result.Data.Uint64);
        case EUdfValueType::Double:
            if (result.Type != EAbiValueType::Double) {
                ythrow yexception()
                    << "Wasm UDF returned wrong value type for Double result: "
                    << static_cast<int>(result.Type);
            }
            return TUnboxedValuePod(result.Data.Double);
        case EUdfValueType::Boolean:
            if (result.Type != EAbiValueType::Boolean) {
                ythrow yexception()
                    << "Wasm UDF returned wrong value type for Boolean result: "
                    << static_cast<int>(result.Type);
            }
            return TUnboxedValuePod(static_cast<bool>(result.Data.Boolean));
        case EUdfValueType::String: {
            if (result.Type != EAbiValueType::String) {
                ythrow yexception()
                    << "Wasm UDF returned wrong value type for String result: "
                    << static_cast<int>(result.Type);
            }
            const auto* hostData = PtrFromVM(compartment, result.Data.String, result.Length);
            return valueBuilder->NewString(TStringRef(hostData, result.Length));
        }
        default:
            ythrow yexception()
                << "Wasm UDF result type " << ValueTypeToString(expectedType)
                << " requires calling_convention=bridge";
    }

    return {};
}

TType* TWasmUdfFunction::BuildYqlType(IFunctionTypeInfoBuilder& builder, EUdfValueType type) {
    switch (type) {
        case EUdfValueType::Null:
            return builder.Null();
        case EUdfValueType::Int64:
            return builder.Optional()->Item<i64>().Build();
        case EUdfValueType::Uint64:
            return builder.Optional()->Item<ui64>().Build();
        case EUdfValueType::Double:
            return builder.Optional()->Item<double>().Build();
        case EUdfValueType::Boolean:
            return builder.Optional()->Item<bool>().Build();
        case EUdfValueType::String:
            return builder.Optional()->Item<char*>().Build();
        default:
            // Only reachable for bridge functions, which build their types
            // from TWasmTypeNode instead.
            return builder.Optional()->Item(BuildLeafDataType(builder, type)).Build();
    }
    return builder.Null();
}

TType* TWasmUdfFunction::BuildFunctionType(
    IFunctionTypeInfoBuilder& builder,
    const TWasmUdfDescriptor& descriptor)
{
    auto callable = builder.Callable(descriptor.Args.size());
    callable->Returns(BuildYqlType(builder, descriptor.Result));
    for (const auto argType : descriptor.Args) {
        callable->Arg(BuildYqlType(builder, argType));
    }
    return callable->Build();
}

void TWasmUdfFunction::Register(
    IFunctionTypeInfoBuilder& builder,
    bool typesOnly,
    TWasmCompartmentStatePtr state,
    const TWasmUdfDescriptor& descriptor)
{
    builder.Returns(BuildYqlType(builder, descriptor.Result));
    auto args = builder.Args(descriptor.Args.size());
    for (const auto argType : descriptor.Args) {
        args->Add(BuildYqlType(builder, argType));
    }

    if (!typesOnly) {
        builder.Implementation(new TWasmUdfFunction(std::move(state), descriptor));
    }
}

TWasmUdfFunction::TWasmUdfFunction(TWasmCompartmentStatePtr state, const TWasmUdfDescriptor& descriptor)
    : State_(std::move(state))
    , Descriptor_(descriptor)
{
}

TUnboxedValue TWasmUdfFunction::Run(
    const IValueBuilder* valueBuilder,
    const TUnboxedValuePod* args) const
{
    try {
        auto* queryHandle = GetCurrentQueryCompartment();
        Y_ENSURE(queryHandle && queryHandle->Compartment,
            "Query WASM compartment is not initialized");

        auto* compartment = queryHandle->Compartment.get();
        const TString exportName(PlainWasmExport(Descriptor_));
        const auto exportKey = MakeExportKey(State_->ModuleName, exportName);
        auto* exportIt = queryHandle->Exports.FindPtr(exportKey);
        Y_ENSURE(exportIt, "Missing WASM export binding for " << exportKey);

        StartUdfDeadlineUnlessNested(compartment);
        TCurrentCompartmentGuard compartmentGuard(compartment);
        TWasmUdfInvocationContext context(compartment);
        TCurrentInvocationContextGuard invocationGuard(&context);

        TVector<TPreparedArg> preparedArgs;
        preparedArgs.reserve(Descriptor_.Args.size());
        TVector<uintptr_t> argOffsets;
        argOffsets.reserve(Descriptor_.Args.size());
        for (size_t i = 0; i < Descriptor_.Args.size(); ++i) {
            preparedArgs.push_back(PrepareArgFromUnboxed(compartment, args[i], Descriptor_.Args[i]));
            argOffsets.push_back(preparedArgs.back().Storage.Offset);
        }

        const auto resultOffset = compartment->AllocateBytes(sizeof(TUnversionedValue));
        auto resultGuard = TCopyGuard(compartment, resultOffset);
        StoreValue(compartment, resultOffset, MakeEmptyValue());

        InvokeUdfExport(
            compartment,
            *exportIt,
            exportName,
            std::bit_cast<uintptr_t>(&context),
            resultOffset,
            argOffsets);

        return ReadResultUnboxed(valueBuilder, compartment, resultOffset, Descriptor_.Result);
    } catch (const std::exception& ex) {
        WasmError(ex, TStringRef(Descriptor_.Name), valueBuilder);
    }
    return {};
}

void TWasmBridgeFunction::Register(
    IFunctionTypeInfoBuilder& builder,
    bool typesOnly,
    TWasmCompartmentStatePtr state,
    const TWasmUdfDescriptor& descriptor)
{
    TVector<TType*> argTypes;
    argTypes.reserve(descriptor.ArgTypes.size());

    const size_t argc = !descriptor.ArgTypes.empty()
        ? descriptor.ArgTypes.size()
        : descriptor.Args.size();

    auto argsBuilder = builder.Args(argc);
    if (!descriptor.ArgTypes.empty()) {
        for (const auto& argType : descriptor.ArgTypes) {
            auto* t = BuildTypeFromWasmTypeNode(builder, *argType);
            argTypes.push_back(t);
            argsBuilder->Add(t);
        }
    } else {
        for (const auto argType : descriptor.Args) {
            auto* t = TWasmUdfFunction::BuildYqlType(builder, argType);
            argTypes.push_back(t);
            argsBuilder->Add(t);
        }
    }

    TType* resultType = nullptr;
    if (descriptor.ResultType) {
        resultType = BuildTypeFromWasmTypeNode(builder, *descriptor.ResultType);
    } else {
        resultType = TWasmUdfFunction::BuildYqlType(builder, descriptor.Result);
    }
    builder.Returns(resultType);

    if (!typesOnly) {
        builder.Implementation(new TWasmBridgeFunction(
            std::move(state),
            descriptor,
            std::move(argTypes),
            resultType,
            builder.TypeInfoHelper()));
    }
}

TWasmBridgeFunction::TWasmBridgeFunction(
    TWasmCompartmentStatePtr state,
    const TWasmUdfDescriptor& descriptor,
    TVector<TType*> argTypes,
    TType* resultType,
    ITypeInfoHelper::TPtr typeInfoHelper)
    : State_(std::move(state))
    , Descriptor_(descriptor)
    , ArgTypes_(std::move(argTypes))
    , ResultType_(resultType)
    , TypeInfoHelper_(std::move(typeInfoHelper))
{
}

TUnboxedValue TWasmBridgeFunction::Run(
    const IValueBuilder* valueBuilder,
    const TUnboxedValuePod* args) const
{
    try {
        auto* queryHandle = GetCurrentQueryCompartment();
        Y_ENSURE(queryHandle && queryHandle->Compartment,
            "Query WASM compartment is not initialized");
        Y_ENSURE(queryHandle->BridgeNodes, "Bridge node table is not initialized");

        auto* compartment = queryHandle->Compartment.get();
        auto& table = *queryHandle->BridgeNodes;
        if (!table.GetTypeInfoHelper() && TypeInfoHelper_) {
            table.SetTypeInfoHelper(TypeInfoHelper_);
        }
        if (!queryHandle->Resident) {
            queryHandle->Resident = std::make_unique<TCompartmentResidentCache>(compartment);
        }
        auto& resident = *queryHandle->Resident;
        // Releases the previous row's scratch and makes older pins evictable.
        // Only the outermost Run may do that: a bridge UDF reached through
        // BridgeRun runs while the caller still holds its own scratch offsets.
        const bool outermostRun = GetCurrentInvocationContext() == nullptr;
        if (outermostRun) {
            resident.BeginRun();
        }

        const TString exportName(PlainWasmExport(Descriptor_));
        const auto exportKey = MakeExportKey(State_->ModuleName, exportName);
        auto* exportIt = queryHandle->Exports.FindPtr(exportKey);
        Y_ENSURE(exportIt, "Missing WASM export binding for " << exportKey);

        StartUdfDeadlineUnlessNested(compartment);
        TCurrentCompartmentGuard compartmentGuard(compartment);
        TWasmUdfInvocationContext context(compartment);
        context.ResultType = ResultType_;
        TCurrentInvocationContextGuard invocationGuard(&context);
        TBridgeValueBuilderGuard valueBuilderGuard(table, valueBuilder);

        const size_t argc = !Descriptor_.ArgTypes.empty()
            ? Descriptor_.ArgTypes.size()
            : Descriptor_.Args.size();

        // Args and everything the guest registers through the intrinsics carry
        // a host ref that this scope drops on the way out, on the normal path
        // and while unwinding alike. A guest keeps a handle past the row only
        // by taking its own ref with BridgeRef.
        TBridgeRunScopeGuard runScope(table);

        TVector<uintptr_t> argHandles;
        argHandles.reserve(argc);

        for (size_t i = 0; i < argc; ++i) {
            const TWasmTypeNode* typeNode = (i < Descriptor_.ArgTypes.size())
                ? Descriptor_.ArgTypes[i].get()
                : nullptr;
            TType* mkqlType = (i < ArgTypes_.size()) ? ArgTypes_[i] : nullptr;
            argHandles.push_back(RegisterBridgeArg(table, args[i], typeNode, mkqlType));
        }

        // Result slot: 8-byte ui64 in linear memory for guest to write the result
        // handle. Taken from the resident arena, so no guest malloc per row.
        const ui64 resultOffset = resident.Alloc(sizeof(ui64));
        Y_DEFER {
            resident.Free(resultOffset);
        };
        *PtrFromVM(compartment, std::bit_cast<ui64*>(resultOffset)) = NullBridgeHandle;

        InvokeUdfExport(
            compartment,
            *exportIt,
            exportName,
            std::bit_cast<uintptr_t>(&context),
            resultOffset,
            argHandles);

        const ui64 resultHandle = *PtrFromVM(compartment, std::bit_cast<ui64*>(resultOffset));
        TUnboxedValue result;
        if (resultHandle != NullBridgeHandle) {
            // Copying the value out takes a MiniKQL ref of its own, so the
            // node behind the result handle may die with the scope.
            result = table.Resolve(resultHandle).Value;
        }

        return result;
    } catch (const NKikimr::NMiniKQL::TTerminateException&) {
        // Preserve terminate classification from nested BridgeRun / UdfTerminate.
        throw;
    } catch (const NYql::TErrorException&) {
        throw;
    } catch (const std::exception& ex) {
        WasmError(ex, TStringRef(Descriptor_.Name), valueBuilder);
    }
    return {};
}

TWasmSoModule::TWasmSoModule(TWasmCompartmentStatePtr state, TString moduleName)
    : State_(std::move(state))
    , ModuleName_(std::move(moduleName))
{
}

void TWasmSoModule::CleanupOnTerminate() const {
}

void TWasmSoModule::GetAllFunctions(IFunctionsSink& sink) const {
    if (State_->ModuleName != ModuleName_) {
        return;
    }
    for (const auto& name : State_->FunctionOrder) {
        const auto* descriptor = State_->Functions.FindPtr(name);
        auto entry = sink.Add(TStringRef(name));
        if (descriptor && descriptor->Binding == EWasmUdfBinding::TypeConfigCallable) {
            entry->SetTypeAwareness();
        }
    }
}

void TWasmSoModule::BuildFunctionTypeInfo(
    const TStringRef& name,
    TType* /*userType*/,
    const TStringRef& typeConfig,
    ui32 flags,
    IFunctionTypeInfoBuilder& builder) const
{
    try {
        if (State_->ModuleName != ModuleName_) {
            builder.SetError(TStringRef::Of("Unknown wasm UDF module"));
            return;
        }
        const TString functionName(name.Data(), name.Size());
        const auto* descriptor = State_->Functions.FindPtr(functionName);
        if (!descriptor) {
            builder.SetError(TStringRef::Of("Unknown wasm UDF function"));
            return;
        }

        const bool typesOnly = (flags & TFlags::TypesOnly) != 0;
        if (descriptor->Binding == EWasmUdfBinding::TypeConfigCallable) {
            TWasmConfiguredCallable::Register(
                builder,
                typesOnly,
                State_,
                *descriptor,
                TString(typeConfig.Data(), typeConfig.Size()));
        } else if (descriptor->CallingConvention == EWasmCallingConvention::Bridge) {
            TWasmBridgeFunction::Register(builder, typesOnly, State_, *descriptor);
        } else {
            TWasmUdfFunction::Register(builder, typesOnly, State_, *descriptor);
        }
    } catch (const std::exception&) {
        builder.SetError(CurrentExceptionMessage());
    }
}

TUniquePtr<IUdfModule> BuildWasmSoModule(TWasmCompartmentStatePtr state) {
    TString moduleName = state->ModuleName;
    return TUniquePtr<IUdfModule>(new TWasmSoModule(std::move(state), std::move(moduleName)));
}

} // namespace NKikimr::NUdfStore::NWasm
