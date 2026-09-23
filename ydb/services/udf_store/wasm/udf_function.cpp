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
#include <yql/essentials/public/udf/udf_type_inspection.h>
#include <yql/essentials/public/udf/udf_data_type.h>
#include <yql/essentials/minikql/mkql_terminator.h>
#include <yql/essentials/public/issue/yql_issue.h>

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
            ythrow yexception() << "Decimal requires precision and scale";
    }
    ythrow yexception() << "Unsupported WASM type descriptor";
}

//! Keep in sync with MaxManifestTypeDepth in manifest.cpp: a tree that got
//! past the parser (or was built by hand in a test) still must not recurse
//! without bound while constructing MiniKQL types.
constexpr ui32 MaxWasmTypeNodeDepth = 32;

} // namespace

TType* BuildTypeFromWasmTypeNode(
    IFunctionTypeInfoBuilder& builder,
    const TWasmTypeNode& node,
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
            if (node.Leaf == EUdfValueType::Decimal) {
                Y_ENSURE(node.Precision > 0 && node.Precision <= NYql::NDecimal::MaxPrecision && node.Scale <= node.Precision,
                    "Invalid Decimal precision/scale");
                return builder.Decimal(node.Precision, node.Scale);
            }
            return BuildLeafDataType(builder, node.Leaf);
        case TWasmTypeNode::EKind::Optional: {
            Y_ENSURE(node.Item);
            return builder.Optional()
                ->Item(BuildTypeFromWasmTypeNode(builder, *node.Item, depth + 1))
                .Build();
        }
        case TWasmTypeNode::EKind::List: {
            Y_ENSURE(node.Item);
            return builder.List()
                ->Item(BuildTypeFromWasmTypeNode(builder, *node.Item, depth + 1))
                .Build();
        }
        case TWasmTypeNode::EKind::Dict: {
            Y_ENSURE(node.Key && node.Payload);
            return builder.Dict()
                ->Key(BuildTypeFromWasmTypeNode(builder, *node.Key, depth + 1))
                .Value(BuildTypeFromWasmTypeNode(builder, *node.Payload, depth + 1))
                .Build();
        }
        case TWasmTypeNode::EKind::Tuple: {
            auto tuple = builder.Tuple(node.Members.size());
            for (const auto& member : node.Members) {
                Y_ENSURE(member.Type);
                tuple->Add(BuildTypeFromWasmTypeNode(
                    builder, *member.Type, depth + 1));
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
                        builder, *member.Type, depth + 1),
                    nullptr);
            }
            return members->Build();
        }
        case TWasmTypeNode::EKind::Variant: {
            Y_ENSURE(!node.Members.empty());
            const bool named = node.NamedVariant;
            TType* underlying = nullptr;
            if (named) {
                auto members = builder.Struct(node.Members.size());
                for (const auto& member : node.Members) {
                    Y_ENSURE(member.Type);
                    members->AddField(
                        TStringRef(member.Name.data(), member.Name.size()),
                        BuildTypeFromWasmTypeNode(
                            builder, *member.Type, depth + 1),
                        nullptr);
                }
                underlying = members->Build();
            } else {
                auto tuple = builder.Tuple(node.Members.size());
                for (const auto& member : node.Members) {
                    Y_ENSURE(member.Type);
                    tuple->Add(BuildTypeFromWasmTypeNode(
                        builder, *member.Type, depth + 1));
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
                builder, *node.CallableReturns, depth + 1));
            for (const auto& arg : node.Members) {
                Y_ENSURE(arg.Type);
                callable->Arg(BuildTypeFromWasmTypeNode(
                    builder, *arg.Type, depth + 1));
            }
            return callable->Build();
        }
    }
    ythrow yexception() << "Unsupported WASM type descriptor";
}

bool TDeclaredResultShape::Accepts(
    EBridgeValueKind kind,
    std::optional<EBridgeValueKind> payload) const
{
    auto family = BridgeKindFamily(kind);
    // BridgeMakeOptional registers an Optional node whenever the payload has
    // no identity of its own to reuse. Such a node is a wrapper: MiniKQL reads
    // what is inside it, so that is what has to match the declared family --
    // a Just(scalar) is not a list, however optional the declaration is.
    if (family == EBridgeKindFamily::Optional && payload) {
        family = BridgeKindFamily(*payload);
    }
    return AcceptsFamily(family);
}

bool TDeclaredResultShape::AcceptsFamily(std::optional<EBridgeKindFamily> family) const {
    if (Family == EBridgeKindFamily::Null) {
        // The declared type told us nothing to check against.
        return true;
    }
    if (!family) {
        // Nothing named to compare against, which only happens for an Optional
        // over a payload no one recorded -- readable as a declared container
        // and nothing else.
        return IsBridgeBoxedFamily(Family);
    }
    if (*family == Family) {
        return true;
    }
    // An Optional the host registered for a declared Optional<container>: the
    // value is the container itself, so the kind stopped at the wrapper and
    // there is nothing cheap to look at. A scalar or a string under an
    // Optional keeps a representation of its own, so a wrapper over one of
    // those always carries the payload kind -- a node that does not is not
    // one of them, and cannot pass for a declared scalar or string.
    if (*family == EBridgeKindFamily::Optional && IsBridgeBoxedFamily(Family)) {
        return true;
    }
    return *family == EBridgeKindFamily::Null && Optional;
}

//! Alternatives of a Variant, counted on the Tuple or Struct underneath it.
ui32 AlternativeCountOf(const TType* underlying, const ITypeInfoHelper* helper) {
    if (!underlying || !helper) {
        return 0;
    }
    if (const TStructTypeInspector members(*helper, underlying); members) {
        return members.GetMembersCount();
    }
    if (const TTupleTypeInspector elements(*helper, underlying); elements) {
        return elements.GetElementsCount();
    }
    return 0;
}

TDeclaredResultShape DeclaredResultShape(const TType* type, const ITypeInfoHelper* helper) {
    if (!type || !helper) {
        return {};
    }
    TDeclaredResultShape shape;
    const TType* payload = type;
    // BridgeKindsFromType reports Optional for Optional<container>, which
    // would leave the payload unchecked, so peel the wrappers here instead.
    for (ui32 depth = 0; payload && depth <= MaxWasmTypeNodeDepth; ++depth) {
        switch (helper->GetTypeKind(payload)) {
            case ETypeKind::Optional:
                shape.Optional = true;
                payload = TOptionalTypeInspector(*helper, payload).GetItemType();
                continue;
            case ETypeKind::Tagged:
                payload = TTaggedTypeInspector(*helper, payload).GetBaseType();
                continue;
            default:
                shape.Family = BridgeKindFamily(BridgeKindsFromType(payload, helper).Value);
                if (const TVariantTypeInspector variant(*helper, payload); variant) {
                    shape.VariantAlternatives = AlternativeCountOf(variant.GetUnderlyingType(), helper);
                }
                if (const TResourceTypeInspector resource(*helper, payload); resource) {
                    const TStringRef tag = resource.GetTag();
                    shape.ResourceTag.assign(tag.Data(), tag.Size());
                }
                return shape;
        }
    }
    return {};
}

std::unique_ptr<TWasmBridgeFunction> TWasmBridgeFunction::Create(
    IFunctionTypeInfoBuilder& builder,
    TWasmCompartmentStatePtr state,
    const TWasmUdfDescriptor& descriptor)
{
    TVector<TType*> args;
    for (const auto& type : descriptor.ArgTypes) {
        args.push_back(BuildTypeFromWasmTypeNode(builder, *type));
    }
    Y_ENSURE(descriptor.ResultType, "Missing bridge result type");
    auto* result = BuildTypeFromWasmTypeNode(builder, *descriptor.ResultType);
    return std::unique_ptr<TWasmBridgeFunction>(new TWasmBridgeFunction(
        std::move(state), descriptor, std::move(args), result, builder.TypeInfoHelper()));
}

void TWasmBridgeFunction::Register(
    IFunctionTypeInfoBuilder& builder,
    bool typesOnly,
    TWasmCompartmentStatePtr state,
    const TWasmUdfDescriptor& descriptor)
{
    auto function = Create(builder, std::move(state), descriptor);
    builder.Returns(function->ResultType_);
    auto args = builder.Args(function->ArgTypes_.size());
    for (auto* type : function->ArgTypes_) {
        args->Add(type);
    }
    args->Done();
    if (!typesOnly) {
        builder.Implementation(function.release());
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
    , ResultShape_(DeclaredResultShape(ResultType_, TypeInfoHelper_.Get()))
{
}

void TWasmBridgeFunction::EnsureResultFamily(const TWasmBridgeNodeTable::TNode& node) const {
    // Check every declared Optional layer before comparing payload kinds.
    // A present optional can contain Nothing; its marker is not a scalar.
    auto value = TUnboxedValuePod(node.Value);
    const TType* valueType = ResultType_;
    for (ui32 depth = 0; depth <= MaxWasmTypeNodeDepth; ++depth) {
        const TOptionalTypeInspector optional(*TypeInfoHelper_, valueType);
        if (!optional) {
            break;
        }
        if (!value) {
            return;
        }
        value = value.GetOptionalValue();
        valueType = optional.GetItemType();
    }
    if (TypeInfoHelper_->GetTypeKind(valueType) == ETypeKind::Null) {
        Y_ENSURE(!value, "Wasm UDF '" << Descriptor_.Name << "' returned a value for Null");
        return;
    }
    Y_ENSURE(value.HasValue(), "Wasm UDF '" << Descriptor_.Name
        << "' returned NULL for a non-optional payload");

    // An empty value is a null whatever kind the node it came in carries.
    // Anything else is worth what the node's own type says about it, so that a
    // UDF handed an optional argument may return it unchanged: such a node
    // wears the Optional kind and names its payload only in its type.
    const auto family = node.Value
        ? BridgeNodeValueFamily(node, TypeInfoHelper_.Get())
        : std::make_optional(EBridgeKindFamily::Null);
    if (!ResultShape_.AcceptsFamily(family)) {
        ythrow yexception()
            << "Wasm UDF '" << Descriptor_.Name << "' returned a "
            << (family ? BridgeKindFamilyAsStr(*family) : "optional")
            << " value, but its result type is "
            << (ResultShape_.Optional ? "optional " : "")
            << BridgeKindFamilyAsStr(ResultShape_.Family);
    }

    const auto* payload = BridgePeelOptional(ResultType_, TypeInfoHelper_.Get());
    if (node.Value && family != EBridgeKindFamily::Null && payload
        && TypeInfoHelper_->GetTypeKind(payload) == ETypeKind::Data) {
        const auto expectedKind = BridgeKindsFromType(payload, TypeInfoHelper_.Get()).Value;
        const auto actualKind = BridgeNodeValueKind(node, TypeInfoHelper_.Get());
        Y_ENSURE(actualKind && *actualKind == expectedKind,
            "Wasm UDF '" << Descriptor_.Name << "' returned a different scalar type");
        const TType* actualType = node.Type ? node.Type : node.AuxType;
        if (expectedKind == EBridgeValueKind::Decimal && actualType) {
            const TDataAndDecimalTypeInspector expected(*TypeInfoHelper_, payload);
            const TDataAndDecimalTypeInspector actual(*TypeInfoHelper_, BridgePeelOptional(actualType, TypeInfoHelper_.Get()));
            Y_ENSURE(actual && actual.GetPrecision() == expected.GetPrecision() && actual.GetScale() == expected.GetScale(),
                "Wasm UDF '" << Descriptor_.Name << "' returned different Decimal precision/scale");
        }
    }

    // The family is as far as a kind goes. A Variant and a Resource carry one
    // more thing MiniKQL reads without a check of its own, and a value that
    // did not come from BridgeMakeVariant -- an argument returned unchanged,
    // say -- never passed the check that intrinsic does.
    if (node.ValueKind == EBridgeValueKind::Variant && ResultShape_.VariantAlternatives != 0) {
        const ui32 index = node.Value.GetVariantIndex();
        if (index >= ResultShape_.VariantAlternatives) {
            ythrow yexception()
                << "Wasm UDF '" << Descriptor_.Name << "' returned alternative " << index
                << " of a Variant, but its result type declares only "
                << ResultShape_.VariantAlternatives;
        }
    }
    if (node.ValueKind == EBridgeValueKind::Resource && !ResultShape_.ResourceTag.empty()) {
        const TStringRef tag = node.Value.GetResourceTag();
        if (TStringBuf(tag.Data(), tag.Size()) != ResultShape_.ResourceTag) {
            ythrow yexception()
                << "Wasm UDF '" << Descriptor_.Name << "' returned a Resource tagged '"
                << TStringBuf(tag.Data(), tag.Size())
                << "', but its result type declares '" << ResultShape_.ResourceTag << "'";
        }
    }
}

TUnboxedValue TWasmBridgeFunction::Run(
    const IValueBuilder* valueBuilder,
    const TUnboxedValuePod* args) const
{
    try {
        return Invoke(valueBuilder, args);
    } catch (const NKikimr::NMiniKQL::TTerminateException&) {
        throw;
    } catch (const NYql::TErrorException&) {
        throw;
    } catch (const std::exception& ex) {
        WasmError(ex, TStringRef(Descriptor_.Name), valueBuilder);
    }
    return {};
}

TUnboxedValue TWasmBridgeFunction::Invoke(
    const IValueBuilder* valueBuilder,
    const TUnboxedValuePod* args) const
{
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

    const size_t argc = ArgTypes_.size();

    // Args and everything the guest registers through the intrinsics carry
    // a host ref that this scope drops on the way out, on the normal path
    // and while unwinding alike. A guest keeps a handle past the row only
    // by taking its own ref with BridgeRef.
    TBridgeRunScopeGuard runScope(table);

    TVector<uintptr_t> argHandles;
    argHandles.reserve(argc);

    for (size_t i = 0; i < argc; ++i) {
        const auto kinds = args[i]
            ? BridgeKindsFromType(ArgTypes_[i], TypeInfoHelper_.Get())
            : TBridgeKinds{EBridgeNodeKind::Scalar, EBridgeValueKind::Null};
        argHandles.push_back(table.RegisterOrReuse(kinds.Node, kinds.Value, ArgTypes_[i], args[i]));
    }

    if (Descriptor_.IsObjectConstructor) {
        argHandles.push_back(table.RegisterOrReuse(EBridgeNodeKind::String, EBridgeValueKind::String,
            nullptr, valueBuilder->NewString(TStringRef::Of(""))));
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
        const auto& resultNode = table.Resolve(resultHandle);
        EnsureResultFamily(resultNode);
        result = resultNode.Value;
    }

    Y_ENSURE(result || ResultShape_.Optional || ResultShape_.Family == EBridgeKindFamily::Null,
        "Wasm UDF '" << Descriptor_.Name << "' returned NULL for a non-optional result");
    return result;
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
        } else {
            TWasmBridgeFunction::Register(builder, typesOnly, State_, *descriptor);
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
