#include "mkql_program_builder.h"
#include "mkql_opt_literal.h"
#include "mkql_node_visitor.h"
#include "mkql_node_cast.h"
#include "mkql_runtime_version.h"
#include "ydb/library/yql/minikql/mkql_node_printer.h"
#include "ydb/library/yql/minikql/mkql_function_registry.h"
#include "ydb/library/yql/minikql/mkql_utils.h"
#include "ydb/library/yql/minikql/mkql_type_builder.h"
#include "ydb/library/yql/core/sql_types/match_recognize.h"
#include "ydb/library/yql/core/sql_types/time_order_recover.h"
#include <ydb/library/yql/parser/pg_catalog/catalog.h>

#include <util/string/cast.h>
#include <util/string/printf.h>
#include <array>

using namespace std::string_view_literals;

namespace NKikimr {
namespace NMiniKQL {

namespace {

struct TDataFunctionFlags {
    enum {
        HasBooleanResult = 0x01,
        RequiresBooleanArgs = 0x02,
        HasOptionalResult = 0x04,
        AllowOptionalArgs = 0x08,
        HasUi32Result = 0x10,
        RequiresCompare = 0x20,
        HasStringResult = 0x40,
        RequiresStringArgs = 0x80,
        RequiresHash = 0x100,
        RequiresEquals = 0x200,
        AllowNull = 0x400,
        CommonOptionalResult = 0x800,
        SupportsTuple = 0x1000,
        SameOptionalArgs = 0x2000,
        Default = 0x00
    };
};

#define MKQL_BAD_TYPE_VISIT(NodeType, ScriptName) \
    void Visit(NodeType& node) override { \
        Y_UNUSED(node); \
        MKQL_ENSURE(false, "Can't convert " #NodeType " to " ScriptName " object"); \
    }

class TPythonTypeChecker : public TExploringNodeVisitor {
    using TExploringNodeVisitor::Visit;
    MKQL_BAD_TYPE_VISIT(TAnyType, "Python");
};

class TLuaTypeChecker : public TExploringNodeVisitor {
    using TExploringNodeVisitor::Visit;
    MKQL_BAD_TYPE_VISIT(TVoidType, "Lua");
    MKQL_BAD_TYPE_VISIT(TAnyType, "Lua");
    MKQL_BAD_TYPE_VISIT(TVariantType, "Lua");
};

class TJavascriptTypeChecker : public TExploringNodeVisitor {
    using TExploringNodeVisitor::Visit;
    MKQL_BAD_TYPE_VISIT(TAnyType, "Javascript");
};

#undef MKQL_BAD_TYPE_VISIT

void EnsureScriptSpecificTypes(
        EScriptType scriptType,
        TCallableType* funcType,
        const TTypeEnvironment& env)
{
    switch (scriptType) {
        case EScriptType::Lua:
            return TLuaTypeChecker().Walk(funcType, env);
        case EScriptType::Python:
        case EScriptType::Python2:
        case EScriptType::Python3:
        case EScriptType::ArcPython:
        case EScriptType::ArcPython2:
        case EScriptType::ArcPython3:
        case EScriptType::CustomPython:
        case EScriptType::CustomPython2:
        case EScriptType::CustomPython3:
        case EScriptType::SystemPython2:
        case EScriptType::SystemPython3:
        case EScriptType::SystemPython3_8:
        case EScriptType::SystemPython3_9:
        case EScriptType::SystemPython3_10:
        case EScriptType::SystemPython3_11:
        case EScriptType::SystemPython3_12:
        case EScriptType::SystemPython3_13:
            return TPythonTypeChecker().Walk(funcType, env);
        case EScriptType::Javascript:
            return TJavascriptTypeChecker().Walk(funcType, env);
    default:
        MKQL_ENSURE(false, "Unknown script type " << static_cast<ui32>(scriptType));
    }
}

ui32 GetNumericSchemeTypeLevel(NUdf::TDataTypeId typeId) {
    switch (typeId) {
    case NUdf::TDataType<ui8>::Id:
        return 0;
    case NUdf::TDataType<i8>::Id:
        return 1;
    case NUdf::TDataType<ui16>::Id:
        return 2;
    case NUdf::TDataType<i16>::Id:
        return 3;
    case NUdf::TDataType<ui32>::Id:
        return 4;
    case NUdf::TDataType<i32>::Id:
        return 5;
    case NUdf::TDataType<ui64>::Id:
        return 6;
    case NUdf::TDataType<i64>::Id:
        return 7;
    case NUdf::TDataType<float>::Id:
        return 8;
    case NUdf::TDataType<double>::Id:
        return 9;
    default:
        ythrow yexception() << "Unknown numeric type: " << typeId;
    }
}

NUdf::TDataTypeId GetNumericSchemeTypeByLevel(ui32 level) {
    switch (level) {
    case 0:
        return NUdf::TDataType<ui8>::Id;
    case 1:
        return NUdf::TDataType<i8>::Id;
    case 2:
        return NUdf::TDataType<ui16>::Id;
    case 3:
        return NUdf::TDataType<i16>::Id;
    case 4:
        return NUdf::TDataType<ui32>::Id;
    case 5:
        return NUdf::TDataType<i32>::Id;
    case 6:
        return NUdf::TDataType<ui64>::Id;
    case 7:
        return NUdf::TDataType<i64>::Id;
    case 8:
        return NUdf::TDataType<float>::Id;
    case 9:
        return NUdf::TDataType<double>::Id;
    default:
        ythrow yexception() << "Unknown numeric level: " << level;
    }
}

NUdf::TDataTypeId MakeNumericDataSuperType(NUdf::TDataTypeId typeId1, NUdf::TDataTypeId typeId2) {
    return typeId1 == typeId2 ? typeId1 :
        GetNumericSchemeTypeByLevel(std::max(GetNumericSchemeTypeLevel(typeId1), GetNumericSchemeTypeLevel(typeId2)));
}

template<bool IsFilter>
bool CollectOptionalElements(const TType* type, std::vector<std::string_view>& test, std::vector<std::pair<std::string_view, TType*>>& output) {
    const auto structType = AS_TYPE(TStructType, type);
    test.reserve(structType->GetMembersCount());
    output.reserve(structType->GetMembersCount());
    bool multiOptional = false;
    for (ui32 i = 0; i < structType->GetMembersCount(); ++i) {
        output.emplace_back(structType->GetMemberName(i), structType->GetMemberType(i));
        auto& memberType = output.back().second;
        if (memberType->IsOptional()) {
            test.emplace_back(output.back().first);
            if constexpr (IsFilter) {
                memberType = AS_TYPE(TOptionalType, memberType)->GetItemType();
                multiOptional = multiOptional || memberType->IsOptional();
            }
        }
    }

    return multiOptional;
}

template<bool IsFilter>
bool CollectOptionalElements(const TType* type, std::vector<ui32>& test, std::vector<TType*>& output) {
    const auto typleType = AS_TYPE(TTupleType, type);
    test.reserve(typleType->GetElementsCount());
    output.reserve(typleType->GetElementsCount());
    bool multiOptional = false;
    for (ui32 i = 0; i < typleType->GetElementsCount(); ++i) {
        output.emplace_back(typleType->GetElementType(i));
        auto& elementType = output.back();
        if (elementType->IsOptional()) {
            test.emplace_back(i);
            if constexpr (IsFilter) {
                elementType = AS_TYPE(TOptionalType, elementType)->GetItemType();
                multiOptional = multiOptional || elementType->IsOptional();
            }
        }
    }

    return multiOptional;
}

bool ReduceOptionalElements(const TType* type, const TArrayRef<const std::string_view>& test, std::vector<std::pair<std::string_view, TType*>>& output) {
    const auto structType = AS_TYPE(TStructType, type);
    output.reserve(structType->GetMembersCount());
    for (ui32 i = 0U; i < structType->GetMembersCount(); ++i) {
        output.emplace_back(structType->GetMemberName(i), structType->GetMemberType(i));
    }

    bool multiOptional = false;
    for (const auto& member : test) {
        auto& memberType = output[structType->GetMemberIndex(member)].second;
        MKQL_ENSURE(memberType->IsOptional(), "Required optional column type");
        memberType = AS_TYPE(TOptionalType, memberType)->GetItemType();
        multiOptional = multiOptional || memberType->IsOptional();
    }

    return multiOptional;
}

bool ReduceOptionalElements(const TType* type, const TArrayRef<const ui32>& test, std::vector<TType*>& output) {
    const auto typleType = AS_TYPE(TTupleType, type);
    output.reserve(typleType->GetElementsCount());
    for (ui32 i = 0U; i < typleType->GetElementsCount(); ++i) {
        output.emplace_back(typleType->GetElementType(i));
    }

    bool multiOptional = false;
    for (const auto& member : test) {
        auto& memberType = output[member];
        MKQL_ENSURE(memberType->IsOptional(), "Required optional column type");
        memberType = AS_TYPE(TOptionalType, memberType)->GetItemType();
        multiOptional = multiOptional || memberType->IsOptional();
    }

    return multiOptional;
}

static std::vector<TType*> ValidateBlockItems(const TArrayRef<TType* const>& wideComponents, bool unwrap) {
    MKQL_ENSURE(wideComponents.size() > 0, "Expected at least one column");
    std::vector<TType*> items;
    items.reserve(wideComponents.size());
    // XXX: Declare these variables outside the loop body to use for the last
    // item (i.e. block length column) in the assertions below.
    bool isScalar;
    TType* itemType;
    for (const auto& wideComponent : wideComponents) {
        auto blockType = AS_TYPE(TBlockType, wideComponent);
        isScalar = blockType->GetShape() == TBlockType::EShape::Scalar;
        itemType = blockType->GetItemType();
        items.push_back(unwrap ? itemType : blockType);
    }

    MKQL_ENSURE(isScalar, "Last column should be scalar");
    MKQL_ENSURE(AS_TYPE(TDataType, itemType)->GetSchemeType() == NUdf::TDataType<ui64>::Id, "Expected Uint64");
    return items;
}

std::vector<TType*> ValidateBlockStreamType(const TType* streamType, bool unwrap = true) {
    const auto wideComponents = GetWideComponents(AS_TYPE(TStreamType, streamType));
    return ValidateBlockItems(wideComponents, unwrap);
}

std::vector<TType*> ValidateBlockFlowType(const TType* flowType, bool unwrap = true) {
    const auto wideComponents = GetWideComponents(AS_TYPE(TFlowType, flowType));
    return ValidateBlockItems(wideComponents, unwrap);
}

} // namespace

std::string_view ScriptTypeAsStr(EScriptType type) {
    switch (type) {
#define MKQL_SCRIPT_TYPE_CASE(name, value, ...) \
    case EScriptType::name: return std::string_view(#name);

        MKQL_SCRIPT_TYPES(MKQL_SCRIPT_TYPE_CASE)

#undef MKQL_SCRIPT_TYPE_CASE
    } // switch

    return std::string_view("Unknown");
}

EScriptType ScriptTypeFromStr(std::string_view str) {
    TString lowerStr = TString(str);
    lowerStr.to_lower();
#define MKQL_SCRIPT_TYPE_FROM_STR(name, value, lowerName, allowSuffix) \
    if ((allowSuffix && lowerStr.StartsWith(#lowerName)) || lowerStr == #lowerName) return EScriptType::name;

    MKQL_SCRIPT_TYPES(MKQL_SCRIPT_TYPE_FROM_STR)
#undef MKQL_SCRIPT_TYPE_FROM_STR

    return EScriptType::Unknown;
}

bool IsCustomPython(EScriptType type) {
    return type == EScriptType::CustomPython ||
        type == EScriptType::CustomPython2 ||
        type == EScriptType::CustomPython3;
}

bool IsSystemPython(EScriptType type) {
    return type == EScriptType::SystemPython2
        || type == EScriptType::SystemPython3
        || type == EScriptType::SystemPython3_8
        || type == EScriptType::SystemPython3_9
        || type == EScriptType::SystemPython3_10
        || type == EScriptType::SystemPython3_11
        || type == EScriptType::SystemPython3_12
        || type == EScriptType::SystemPython3_13
        || type == EScriptType::Python
        || type == EScriptType::Python2;
}

EScriptType CanonizeScriptType(EScriptType type) {
    if (type == EScriptType::Python) {
        return EScriptType::Python2;
    }

    if (type == EScriptType::ArcPython) {
        return EScriptType::ArcPython2;
    }

    return type;
}

void EnsureDataOrOptionalOfData(TRuntimeNode node) {
    MKQL_ENSURE(node.GetStaticType()->IsData() ||
        node.GetStaticType()->IsOptional() && AS_TYPE(TOptionalType, node.GetStaticType())
        ->GetItemType()->IsData(), "Expected data or optional of data");
}

TProgramBuilder::TProgramBuilder(const TTypeEnvironment& env, const IFunctionRegistry& functionRegistry, bool voidWithEffects)
    : TTypeBuilder(env)
    , FunctionRegistry(functionRegistry)
    , VoidWithEffects(voidWithEffects)
{}

const TTypeEnvironment& TProgramBuilder::GetTypeEnvironment() const {
    return Env;
}

const IFunctionRegistry& TProgramBuilder::GetFunctionRegistry() const {
    return FunctionRegistry;
}

TType* TProgramBuilder::ChooseCommonType(TType* type1, TType* type2) {
    bool isOptional1, isOptional2;
    const auto data1 = UnpackOptionalData(type1, isOptional1);
    const auto data2 = UnpackOptionalData(type2, isOptional2);
    if (data1->IsSameType(*data2)) {
        return isOptional1 ? type1 : type2;
    }

    MKQL_ENSURE(!
        ((NUdf::GetDataTypeInfo(*data1->GetDataSlot()).Features | NUdf::GetDataTypeInfo(*data2->GetDataSlot()).Features) & (NUdf::EDataTypeFeatures::DateType | NUdf::EDataTypeFeatures::TzDateType)),
        "Not same date types: " << *type1 << " and " << *type2
    );

    const auto data = NewDataType(MakeNumericDataSuperType(data1->GetSchemeType(), data2->GetSchemeType()));
    return isOptional1 || isOptional2 ? NewOptionalType(data) : data;
}

TType* TProgramBuilder::BuildArithmeticCommonType(TType* type1, TType* type2) {
    bool isOptional1, isOptional2;
    const auto data1 = UnpackOptionalData(type1, isOptional1);
    const auto data2 = UnpackOptionalData(type2, isOptional2);
    const auto features1 = NUdf::GetDataTypeInfo(*data1->GetDataSlot()).Features;
    const auto features2 = NUdf::GetDataTypeInfo(*data2->GetDataSlot()).Features;
    const bool isOptional = isOptional1 || isOptional2;
    if (features1 & features2 & NUdf::EDataTypeFeatures::TimeIntervalType) {
        return NewOptionalType(features1 & NUdf::EDataTypeFeatures::BigDateType ? data1 : data2);
    } else if (features1 & NUdf::EDataTypeFeatures::TimeIntervalType) {
        return NewOptionalType(features2 & NUdf::EDataTypeFeatures::IntegralType ? data1 : data2);
    } else if (features2 & NUdf::EDataTypeFeatures::TimeIntervalType) {
        return NewOptionalType(features1 & NUdf::EDataTypeFeatures::IntegralType ? data2 : data1);
    } else if (
        features1 & (NUdf::EDataTypeFeatures::DateType | NUdf::EDataTypeFeatures::TzDateType) &&
        features2 & (NUdf::EDataTypeFeatures::DateType | NUdf::EDataTypeFeatures::TzDateType)
    ) {
        const auto used = ((features1 | features2) & NUdf::EDataTypeFeatures::BigDateType)
            ? NewDataType(NUdf::EDataSlot::Interval64)
            : NewDataType(NUdf::EDataSlot::Interval);
        return isOptional ? NewOptionalType(used) : used;
    } else if (data1->GetSchemeType() == NUdf::TDataType<NUdf::TDecimal>::Id) {
        MKQL_ENSURE(data1->IsSameType(*data2), "Must be same type.");
        return isOptional ? NewOptionalType(data1) : data2;
    }

    const auto data = NewDataType(MakeNumericDataSuperType(data1->GetSchemeType(), data2->GetSchemeType()));
    return isOptional ? NewOptionalType(data) : data;
}

TRuntimeNode TProgramBuilder::Arg(TType* type) const {
    TCallableBuilder builder(Env, __func__, type, true);
    return TRuntimeNode(builder.Build(), false);
}

TRuntimeNode TProgramBuilder::WideFlowArg(TType* type) const {
    if constexpr (RuntimeVersion < 18U) {
        THROW yexception() << "Runtime version (" << RuntimeVersion << ") too old for " << __func__;
    }

    TCallableBuilder builder(Env, __func__, type, true);
    return TRuntimeNode(builder.Build(), false);
}

TRuntimeNode TProgramBuilder::Member(TRuntimeNode structObj, const std::string_view& memberName) {
    bool isOptional;
    const auto type = AS_TYPE(TStructType, UnpackOptional(structObj.GetStaticType(), isOptional));
    const auto memberIndex = type->GetMemberIndex(memberName);
    auto memberType = type->GetMemberType(memberIndex);
    if (isOptional && !memberType->IsOptional() && !memberType->IsNull() && !memberType->IsPg()) {
        memberType = NewOptionalType(memberType);
    }

    TCallableBuilder callableBuilder(Env, __func__, memberType);
    callableBuilder.Add(structObj);
    callableBuilder.Add(NewDataLiteral<ui32>(memberIndex));
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::Element(TRuntimeNode structObj, const std::string_view& memberName) {
    return Member(structObj, memberName);
}

TRuntimeNode TProgramBuilder::AddMember(TRuntimeNode structObj, const std::string_view& memberName, TRuntimeNode memberValue) {
    auto oldType = structObj.GetStaticType();
    MKQL_ENSURE(oldType->IsStruct(), "Expected struct");

    const auto& oldTypeDetailed = static_cast<const TStructType&>(*oldType);
    TStructTypeBuilder newTypeBuilder(Env);
    newTypeBuilder.Reserve(oldTypeDetailed.GetMembersCount() + 1);
    for (ui32 i = 0, e = oldTypeDetailed.GetMembersCount(); i < e; ++i) {
        newTypeBuilder.Add(oldTypeDetailed.GetMemberName(i), oldTypeDetailed.GetMemberType(i));
    }

    newTypeBuilder.Add(memberName, memberValue.GetStaticType());
    auto newType = newTypeBuilder.Build();
    for (ui32 i = 0, e = newType->GetMembersCount(); i < e; ++i) {
        if (newType->GetMemberName(i) == memberName) {
            // insert at position i in the struct
            TCallableBuilder callableBuilder(Env, __func__, newType);
            callableBuilder.Add(structObj);
            callableBuilder.Add(memberValue);
            callableBuilder.Add(NewDataLiteral<ui32>(i));
            return TRuntimeNode(callableBuilder.Build(), false);
        }
    }

    Y_ABORT();
}

TRuntimeNode TProgramBuilder::RemoveMember(TRuntimeNode structObj, const std::string_view& memberName, bool forced) {
    auto oldType = structObj.GetStaticType();
    MKQL_ENSURE(oldType->IsStruct(), "Expected struct");

    const auto& oldTypeDetailed = static_cast<const TStructType&>(*oldType);
    MKQL_ENSURE(oldTypeDetailed.GetMembersCount() > 0, "Expected non-empty struct");

    TStructTypeBuilder newTypeBuilder(Env);
    newTypeBuilder.Reserve(oldTypeDetailed.GetMembersCount() - 1);
    std::optional<ui32> memberIndex;
    for (ui32 i = 0, e = oldTypeDetailed.GetMembersCount(); i < e; ++i) {
        if (oldTypeDetailed.GetMemberName(i) != memberName) {
            newTypeBuilder.Add(oldTypeDetailed.GetMemberName(i), oldTypeDetailed.GetMemberType(i));
        }
        else {
            memberIndex = i;
        }
    }
    if (!memberIndex && forced) {
        return structObj;
    }

    MKQL_ENSURE(memberIndex, "Unknown member name: " << memberName);

    // remove at position i in the struct
    auto newType = newTypeBuilder.Build();
    TCallableBuilder callableBuilder(Env, __func__, newType);
    callableBuilder.Add(structObj);
    callableBuilder.Add(NewDataLiteral<ui32>(*memberIndex));
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::Zip(const TArrayRef<const TRuntimeNode>& lists) {
    if (lists.empty()) {
        return NewEmptyList(Env.GetEmptyTupleLazy()->GetGenericType());
    }

    std::vector<TType*> tupleTypes;
    tupleTypes.reserve(lists.size());
    for (auto& list : lists) {
        if (list.GetStaticType()->IsEmptyList()) {
            tupleTypes.push_back(Env.GetTypeOfVoidLazy());
            continue;
        }

        AS_TYPE(TListType, list.GetStaticType());
        auto itemType = static_cast<const TListType&>(*list.GetStaticType()).GetItemType();
        tupleTypes.push_back(itemType);
    }

    auto returnType = TListType::Create(TTupleType::Create(tupleTypes.size(), tupleTypes.data(), Env), Env);
    TCallableBuilder callableBuilder(Env, __func__, returnType);
    for (auto& list : lists) {
        callableBuilder.Add(list);
    }

    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::ZipAll(const TArrayRef<const TRuntimeNode>& lists) {
    if (lists.empty()) {
        return NewEmptyList(Env.GetEmptyTupleLazy()->GetGenericType());
    }

    std::vector<TType*> tupleTypes;
    tupleTypes.reserve(lists.size());
    for (auto& list : lists) {
        if (list.GetStaticType()->IsEmptyList()) {
            tupleTypes.push_back(TOptionalType::Create(Env.GetTypeOfVoidLazy(), Env));
            continue;
        }

        AS_TYPE(TListType, list.GetStaticType());
        auto itemType = static_cast<const TListType&>(*list.GetStaticType()).GetItemType();
        tupleTypes.push_back(TOptionalType::Create(itemType, Env));
    }

    auto returnType = TListType::Create(TTupleType::Create(tupleTypes.size(), tupleTypes.data(), Env), Env);
    TCallableBuilder callableBuilder(Env, __func__, returnType);
    for (auto& list : lists) {
        callableBuilder.Add(list);
    }

    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::Enumerate(TRuntimeNode list, TRuntimeNode start, TRuntimeNode step) {
    const auto itemType = AS_TYPE(TListType, list.GetStaticType())->GetItemType();
    ThrowIfListOfVoid(itemType);

    MKQL_ENSURE(AS_TYPE(TDataType, start)->GetSchemeType() == NUdf::TDataType<ui64>::Id, "Expected Uint64 as start");
    MKQL_ENSURE(AS_TYPE(TDataType, step)->GetSchemeType() == NUdf::TDataType<ui64>::Id, "Expected Uint64 as step");

    const std::array<TType*, 2U> tupleTypes = {{ NewDataType(NUdf::EDataSlot::Uint64), itemType }};
    const auto returnType = NewListType(NewTupleType(tupleTypes));

    TCallableBuilder callableBuilder(Env, __func__, returnType);
    callableBuilder.Add(list);
    callableBuilder.Add(start);
    callableBuilder.Add(step);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::Enumerate(TRuntimeNode list) {
    return TProgramBuilder::Enumerate(list, NewDataLiteral<ui64>(0), NewDataLiteral<ui64>(1));
}

TRuntimeNode TProgramBuilder::Fold(TRuntimeNode list, TRuntimeNode state, const TBinaryLambda& handler) {
    const auto itemType = AS_TYPE(TListType, list.GetStaticType())->GetItemType();
    ThrowIfListOfVoid(itemType);

    const auto stateNodeArg = Arg(state.GetStaticType());
    const auto itemArg = Arg(itemType);
    const auto newState = handler(itemArg, stateNodeArg);
    MKQL_ENSURE(newState.GetStaticType()->IsSameType(*state.GetStaticType()), "State type is changed by the handler");

    TCallableBuilder callableBuilder(Env, __func__, state.GetStaticType());
    callableBuilder.Add(list);
    callableBuilder.Add(state);
    callableBuilder.Add(itemArg);
    callableBuilder.Add(stateNodeArg);
    callableBuilder.Add(newState);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::Fold1(TRuntimeNode list, const TUnaryLambda& init, const TBinaryLambda& handler) {
    const auto itemType = AS_TYPE(TListType, list.GetStaticType())->GetItemType();
    ThrowIfListOfVoid(itemType);

    const auto itemArg = Arg(itemType);
    const auto initState = init(itemArg);
    const auto stateNodeArg = Arg(initState.GetStaticType());
    const auto newState = handler(itemArg, stateNodeArg);

    MKQL_ENSURE(newState.GetStaticType()->IsSameType(*initState.GetStaticType()), "State type is changed by the handler");

    TCallableBuilder callableBuilder(Env, __func__, NewOptionalType(newState.GetStaticType()));
    callableBuilder.Add(list);
    callableBuilder.Add(itemArg);
    callableBuilder.Add(initState);
    callableBuilder.Add(stateNodeArg);
    callableBuilder.Add(newState);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::Reduce(TRuntimeNode list, TRuntimeNode state1,
    const TBinaryLambda& handler1,
    const TUnaryLambda& handler2,
    TRuntimeNode state3,
    const TBinaryLambda& handler3) {
    const auto listType = list.GetStaticType();
    MKQL_ENSURE(listType->IsList() || listType->IsStream(), "Expected list or stream");
    const auto itemType = listType->IsList()?
        static_cast<const TListType&>(*listType).GetItemType():
        static_cast<const TStreamType&>(*listType).GetItemType();
    ThrowIfListOfVoid(itemType);

    const auto state1NodeArg = Arg(state1.GetStaticType());
    const auto state3NodeArg = Arg(state3.GetStaticType());
    const auto itemArg = Arg(itemType);
    const auto newState1 = handler1(itemArg, state1NodeArg);
    MKQL_ENSURE(newState1.GetStaticType()->IsSameType(*state1.GetStaticType()), "State 1 type is changed by the handler");

    const auto newState2 = handler2(state1NodeArg);
    TRuntimeNode itemState2Arg = Arg(newState2.GetStaticType());

    const auto newState3 = handler3(itemState2Arg, state3NodeArg);
    MKQL_ENSURE(newState3.GetStaticType()->IsSameType(*state3.GetStaticType()), "State 3 type is changed by the handler");

    TCallableBuilder callableBuilder(Env, __func__, newState3.GetStaticType());
    callableBuilder.Add(list);
    callableBuilder.Add(state1);
    callableBuilder.Add(state3);
    callableBuilder.Add(itemArg);
    callableBuilder.Add(state1NodeArg);
    callableBuilder.Add(newState1);
    callableBuilder.Add(newState2);
    callableBuilder.Add(itemState2Arg);
    callableBuilder.Add(state3NodeArg);
    callableBuilder.Add(newState3);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::Condense(TRuntimeNode flow, TRuntimeNode state,
    const TBinaryLambda& switcher,
    const TBinaryLambda& handler, bool useCtx) {
    const auto flowType = flow.GetStaticType();

    if (flowType->IsList()) {
        // TODO: Native implementation for list.
        return Collect(Condense(ToFlow(flow), state, switcher, handler));
    }

    MKQL_ENSURE(flowType->IsFlow() || flowType->IsStream(), "Expected flow or stream.");

    const auto itemType = flowType->IsFlow() ?
        static_cast<const TFlowType&>(*flowType).GetItemType():
        static_cast<const TStreamType&>(*flowType).GetItemType();

    const auto itemArg = Arg(itemType);
    const auto stateArg = Arg(state.GetStaticType());
    const auto outSwitch = switcher(itemArg, stateArg);
    const auto newState = handler(itemArg, stateArg);
    MKQL_ENSURE(newState.GetStaticType()->IsSameType(*state.GetStaticType()), "State type is changed by the handler");

    TCallableBuilder callableBuilder(Env, __func__, flowType->IsFlow() ? NewFlowType(state.GetStaticType()) : NewStreamType(state.GetStaticType()));
    callableBuilder.Add(flow);
    callableBuilder.Add(state);
    callableBuilder.Add(itemArg);
    callableBuilder.Add(stateArg);
    callableBuilder.Add(outSwitch);
    callableBuilder.Add(newState);
    if (useCtx) {
        MKQL_ENSURE(RuntimeVersion >= 30U, "Too old runtime version");
        callableBuilder.Add(NewDataLiteral<bool>(useCtx));
    }

    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::Condense1(TRuntimeNode flow, const TUnaryLambda& init,
    const TBinaryLambda& switcher,
    const TBinaryLambda& handler, bool useCtx) {
    const auto flowType = flow.GetStaticType();

    if (flowType->IsList()) {
        // TODO: Native implementation for list.
        return Collect(Condense1(ToFlow(flow), init, switcher, handler));
    }

    MKQL_ENSURE(flowType->IsFlow() || flowType->IsStream(), "Expected flow or stream.");

    const auto itemType = flowType->IsFlow() ?
        static_cast<const TFlowType&>(*flowType).GetItemType():
        static_cast<const TStreamType&>(*flowType).GetItemType();

    const auto itemArg = Arg(itemType);
    const auto initState = init(itemArg);
    const auto stateArg = Arg(initState.GetStaticType());
    const auto outSwitch = switcher(itemArg, stateArg);
    const auto newState = handler(itemArg, stateArg);

    MKQL_ENSURE(newState.GetStaticType()->IsSameType(*initState.GetStaticType()), "State type is changed by the handler");

    TCallableBuilder callableBuilder(Env, __func__, flowType->IsFlow() ? NewFlowType(newState.GetStaticType()) : NewStreamType(newState.GetStaticType()));
    callableBuilder.Add(flow);
    callableBuilder.Add(itemArg);
    callableBuilder.Add(initState);
    callableBuilder.Add(stateArg);
    callableBuilder.Add(outSwitch);
    callableBuilder.Add(newState);
    if (useCtx) {
        MKQL_ENSURE(RuntimeVersion >= 30U, "Too old runtime version");
        callableBuilder.Add(NewDataLiteral<bool>(useCtx));
    }

    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::Squeeze(TRuntimeNode stream, TRuntimeNode state,
    const TBinaryLambda& handler,
    const TUnaryLambda& save,
    const TUnaryLambda& load) {
    const auto streamType = stream.GetStaticType();
    MKQL_ENSURE(streamType->IsStream(), "Expected stream");

    const auto& streamDetailedType = static_cast<const TStreamType&>(*streamType);
    const auto itemType = streamDetailedType.GetItemType();
    ThrowIfListOfVoid(itemType);

    const auto stateNodeArg = Arg(state.GetStaticType());
    const auto itemArg = Arg(itemType);
    const auto newState = handler(itemArg, stateNodeArg);
    MKQL_ENSURE(newState.GetStaticType()->IsSameType(*state.GetStaticType()), "State type is changed by the handler");

    TRuntimeNode saveArg, outSave, loadArg, outLoad;

    if (save && load) {
        outSave = save(saveArg = Arg(state.GetStaticType()));
        outLoad = load(loadArg = Arg(outSave.GetStaticType()));
        MKQL_ENSURE(outLoad.GetStaticType()->IsSameType(*state.GetStaticType()), "Loaded type is changed by the load handler");
    } else {
        saveArg = outSave = loadArg = outLoad = NewVoid();
    }

    TCallableBuilder callableBuilder(Env, __func__, TStreamType::Create(state.GetStaticType(), Env));
    callableBuilder.Add(stream);
    callableBuilder.Add(state);
    callableBuilder.Add(itemArg);
    callableBuilder.Add(stateNodeArg);
    callableBuilder.Add(newState);
    callableBuilder.Add(saveArg);
    callableBuilder.Add(outSave);
    callableBuilder.Add(loadArg);
    callableBuilder.Add(outLoad);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::Squeeze1(TRuntimeNode stream, const TUnaryLambda& init,
    const TBinaryLambda& handler,
    const TUnaryLambda& save,
    const TUnaryLambda& load) {
    const auto streamType = stream.GetStaticType();
    MKQL_ENSURE(streamType->IsStream(), "Expected stream");

    const auto& streamDetailedType = static_cast<const TStreamType&>(*streamType);
    const auto itemType = streamDetailedType.GetItemType();
    ThrowIfListOfVoid(itemType);

    const auto itemArg = Arg(itemType);
    const auto initState = init(itemArg);
    const auto stateNodeArg = Arg(initState.GetStaticType());
    const auto newState = handler(itemArg, stateNodeArg);
    MKQL_ENSURE(newState.GetStaticType()->IsSameType(*initState.GetStaticType()), "State type is changed by the handler");

    TRuntimeNode saveArg, outSave, loadArg, outLoad;

    if (save && load) {
        outSave = save(saveArg = Arg(initState.GetStaticType()));
        outLoad = load(loadArg = Arg(outSave.GetStaticType()));
        MKQL_ENSURE(outLoad.GetStaticType()->IsSameType(*initState.GetStaticType()), "Loaded type is changed by the load handler");
    } else {
        saveArg = outSave = loadArg = outLoad = NewVoid();
    }

    TCallableBuilder callableBuilder(Env, __func__, NewStreamType(newState.GetStaticType()));
    callableBuilder.Add(stream);
    callableBuilder.Add(itemArg);
    callableBuilder.Add(initState);
    callableBuilder.Add(stateNodeArg);
    callableBuilder.Add(newState);
    callableBuilder.Add(saveArg);
    callableBuilder.Add(outSave);
    callableBuilder.Add(loadArg);
    callableBuilder.Add(outLoad);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::Discard(TRuntimeNode stream) {
    const auto streamType = stream.GetStaticType();
    MKQL_ENSURE(streamType->IsStream() || streamType->IsFlow(), "Expected stream or flow.");

    TCallableBuilder callableBuilder(Env, __func__, streamType);
    callableBuilder.Add(stream);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::Map(TRuntimeNode list, const TUnaryLambda& handler) {
    return BuildMap(__func__, list, handler);
}

TRuntimeNode TProgramBuilder::OrderedMap(TRuntimeNode list, const TUnaryLambda& handler) {
    return BuildMap(__func__, list, handler);
}

TRuntimeNode TProgramBuilder::MapNext(TRuntimeNode list, const TBinaryLambda& handler) {
    const auto listType = list.GetStaticType();
    MKQL_ENSURE(listType->IsStream() || listType->IsFlow(), "Expected stream or flow");

    const auto itemType = listType->IsFlow() ?
        AS_TYPE(TFlowType, listType)->GetItemType():
        AS_TYPE(TStreamType, listType)->GetItemType();

    ThrowIfListOfVoid(itemType);

    TType* nextItemType = TOptionalType::Create(itemType, Env);

    const auto itemArg = Arg(itemType);
    const auto nextItemArg = Arg(nextItemType);

    const auto newItem = handler(itemArg, nextItemArg);

    const auto resultListType = listType->IsFlow() ?
        (TType*)TFlowType::Create(newItem.GetStaticType(), Env):
        (TType*)TStreamType::Create(newItem.GetStaticType(), Env);

    TCallableBuilder callableBuilder(Env, __func__, resultListType);
    callableBuilder.Add(list);
    callableBuilder.Add(itemArg);
    callableBuilder.Add(nextItemArg);
    callableBuilder.Add(newItem);
    return TRuntimeNode(callableBuilder.Build(), false);
}

template <bool Ordered>
TRuntimeNode TProgramBuilder::BuildExtract(TRuntimeNode list, const std::string_view& name) {
    const auto listType = list.GetStaticType();
    MKQL_ENSURE(listType->IsList() || listType->IsOptional(), "Expected list or optional.");

    const auto itemType = listType->IsList() ?
            AS_TYPE(TListType, listType)->GetItemType():
            AS_TYPE(TOptionalType, listType)->GetItemType();

    const auto lambda = [&](TRuntimeNode item) {
        return itemType->IsStruct() ? Member(item, name) : Nth(item, ::FromString<ui32>(name));
    };

    return Ordered ? OrderedMap(list, lambda) : Map(list, lambda);
}

TRuntimeNode TProgramBuilder::Extract(TRuntimeNode list, const std::string_view& name) {
    return BuildExtract<false>(list, name);
}

TRuntimeNode TProgramBuilder::OrderedExtract(TRuntimeNode list, const std::string_view& name) {
    return BuildExtract<true>(list, name);
}

TRuntimeNode TProgramBuilder::ChainMap(TRuntimeNode list, TRuntimeNode state, const TBinaryLambda& handler) {
    return ChainMap(list, state, [&](TRuntimeNode item, TRuntimeNode state) -> TRuntimeNodePair {
        const auto result = handler(item, state);
        return {result, result};
    });
}

TRuntimeNode TProgramBuilder::ChainMap(TRuntimeNode list, TRuntimeNode state, const TBinarySplitLambda& handler) {
    const auto listType = list.GetStaticType();
    MKQL_ENSURE(listType->IsFlow() || listType->IsList() || listType->IsStream(), "Expected flow, list or stream");

    const auto itemType = listType->IsFlow() ?
        AS_TYPE(TFlowType, listType)->GetItemType():
        listType->IsList() ?
            AS_TYPE(TListType, listType)->GetItemType():
            AS_TYPE(TStreamType, listType)->GetItemType();

    ThrowIfListOfVoid(itemType);

    const auto stateNodeArg = Arg(state.GetStaticType());
    const auto itemArg = Arg(itemType);
    const auto newItemAndState = handler(itemArg, stateNodeArg);

    MKQL_ENSURE(std::get<1U>(newItemAndState).GetStaticType()->IsSameType(*state.GetStaticType()), "State type is changed by the handler");
    const auto resultItemType = std::get<0U>(newItemAndState).GetStaticType();
    TType* resultListType = nullptr;
    if (listType->IsFlow()) {
        resultListType = TFlowType::Create(resultItemType, Env);
    } else if (listType->IsList()) {
        resultListType = TListType::Create(resultItemType, Env);
    } else if (listType->IsStream()) {
        resultListType = TStreamType::Create(resultItemType, Env);
    }

    TCallableBuilder callableBuilder(Env, __func__, resultListType);
    callableBuilder.Add(list);
    callableBuilder.Add(state);
    callableBuilder.Add(itemArg);
    callableBuilder.Add(stateNodeArg);
    callableBuilder.Add(std::get<0U>(newItemAndState));
    callableBuilder.Add(std::get<1U>(newItemAndState));
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::Chain1Map(TRuntimeNode list, const TUnaryLambda& init, const TBinaryLambda& handler) {
    return Chain1Map(list,
        [&](TRuntimeNode item) -> TRuntimeNodePair {
            const auto result = init(item);
            return {result, result};
        },
        [&](TRuntimeNode item, TRuntimeNode state) -> TRuntimeNodePair {
            const auto result = handler(item, state);
            return {result, result};
        }
    );
}

TRuntimeNode TProgramBuilder::Chain1Map(TRuntimeNode list, const TUnarySplitLambda& init, const TBinarySplitLambda& handler) {
    const auto listType = list.GetStaticType();
    MKQL_ENSURE(listType->IsFlow() || listType->IsList() || listType->IsStream(), "Expected flow, list or stream");

    const auto itemType = listType->IsFlow() ?
        AS_TYPE(TFlowType, listType)->GetItemType():
        listType->IsList() ?
            AS_TYPE(TListType, listType)->GetItemType():
            AS_TYPE(TStreamType, listType)->GetItemType();

    ThrowIfListOfVoid(itemType);

    const auto itemArg = Arg(itemType);
    const auto initItemAndState = init(itemArg);
    const auto resultItemType = std::get<0U>(initItemAndState).GetStaticType();
    const auto stateType = std::get<1U>(initItemAndState).GetStaticType();;
    TType* resultListType = nullptr;
    if (listType->IsFlow()) {
        resultListType = TFlowType::Create(resultItemType, Env);
    } else if (listType->IsList()) {
        resultListType = TListType::Create(resultItemType, Env);
    } else if (listType->IsStream()) {
        resultListType = TStreamType::Create(resultItemType, Env);
    }

    const auto stateArg = Arg(stateType);
    const auto updateItemAndState = handler(itemArg, stateArg);
    MKQL_ENSURE(std::get<0U>(updateItemAndState).GetStaticType()->IsSameType(*resultItemType), "Item type is changed by the handler");
    MKQL_ENSURE(std::get<1U>(updateItemAndState).GetStaticType()->IsSameType(*stateType), "State type is changed by the handler");

    TCallableBuilder callableBuilder(Env, __func__, resultListType);
    callableBuilder.Add(list);
    callableBuilder.Add(itemArg);
    callableBuilder.Add(std::get<0U>(initItemAndState));
    callableBuilder.Add(std::get<1U>(initItemAndState));
    callableBuilder.Add(stateArg);
    callableBuilder.Add(std::get<0U>(updateItemAndState));
    callableBuilder.Add(std::get<1U>(updateItemAndState));
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::ToList(TRuntimeNode optional) {
    const auto optionalType = optional.GetStaticType();
    MKQL_ENSURE(optionalType->IsOptional(), "Expected optional");

    const auto& optionalDetailedType = static_cast<const TOptionalType&>(*optionalType);
    const auto itemType = optionalDetailedType.GetItemType();
    return IfPresent(optional, [&](TRuntimeNode item) { return AsList(item); }, NewEmptyList(itemType));
}

TRuntimeNode TProgramBuilder::Iterable(TZeroLambda lambda) {
    if constexpr (RuntimeVersion < 19U) {
        THROW yexception() << "Runtime version (" << RuntimeVersion << ") too old for " << __func__;
    }

    const auto itemArg = Arg(NewNull().GetStaticType());
    auto lambdaRes = lambda();
    const auto resultType = NewListType(AS_TYPE(TStreamType, lambdaRes.GetStaticType())->GetItemType());
    TCallableBuilder callableBuilder(Env, __func__, resultType);
    callableBuilder.Add(lambdaRes);
    callableBuilder.Add(itemArg);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::ToOptional(TRuntimeNode list) {
    return Head(list);
}

TRuntimeNode TProgramBuilder::Head(TRuntimeNode list) {
    const auto resultType = NewOptionalType(AS_TYPE(TListType, list.GetStaticType())->GetItemType());
    TCallableBuilder callableBuilder(Env, __func__, resultType);
    callableBuilder.Add(list);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::Last(TRuntimeNode list) {
    const auto resultType = NewOptionalType(AS_TYPE(TListType, list.GetStaticType())->GetItemType());
    TCallableBuilder callableBuilder(Env, __func__, resultType);
    callableBuilder.Add(list);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::Nanvl(TRuntimeNode data, TRuntimeNode dataIfNaN) {
    const std::array<TRuntimeNode, 2> args = {{ data, dataIfNaN }};
    return Invoke(__func__, BuildArithmeticCommonType(data.GetStaticType(), dataIfNaN.GetStaticType()), args);
}

TRuntimeNode TProgramBuilder::FlatMap(TRuntimeNode list, const TUnaryLambda& handler)
{
    return BuildFlatMap(__func__, list, handler);
}

TRuntimeNode TProgramBuilder::OrderedFlatMap(TRuntimeNode list, const TUnaryLambda& handler)
{
    return BuildFlatMap(__func__, list, handler);
}

TRuntimeNode TProgramBuilder::Filter(TRuntimeNode list, const TUnaryLambda& handler)
{
    return BuildFilter(__func__, list, handler);
}

TRuntimeNode TProgramBuilder::Filter(TRuntimeNode list, TRuntimeNode limit, const TUnaryLambda& handler)
{
    return BuildFilter(__func__, list, limit, handler);
}

TRuntimeNode TProgramBuilder::OrderedFilter(TRuntimeNode list, const TUnaryLambda& handler)
{
    return BuildFilter(__func__, list, handler);
}

TRuntimeNode TProgramBuilder::OrderedFilter(TRuntimeNode list, TRuntimeNode limit, const TUnaryLambda& handler)
{
    return BuildFilter(__func__, list, limit, handler);
}

TRuntimeNode TProgramBuilder::TakeWhile(TRuntimeNode list, const TUnaryLambda& handler)
{
    return BuildFilter(__func__, list, handler);
}

TRuntimeNode TProgramBuilder::SkipWhile(TRuntimeNode list, const TUnaryLambda& handler)
{
    return BuildFilter(__func__, list, handler);
}

TRuntimeNode TProgramBuilder::TakeWhileInclusive(TRuntimeNode list, const TUnaryLambda& handler)
{
    return BuildFilter(__func__, list, handler);
}

TRuntimeNode TProgramBuilder::SkipWhileInclusive(TRuntimeNode list, const TUnaryLambda& handler)
{
    return BuildFilter(__func__, list, handler);
}

TRuntimeNode TProgramBuilder::BuildListSort(const std::string_view& callableName, TRuntimeNode list, TRuntimeNode ascending,
    const TUnaryLambda& keyExtractor)
{
    const auto listType = list.GetStaticType();
    MKQL_ENSURE(listType->IsList(), "Expected list.");

    const auto itemType = static_cast<const TListType&>(*listType).GetItemType();

    ThrowIfListOfVoid(itemType);

    const auto ascendingType = ascending.GetStaticType();
    const auto itemArg = Arg(itemType);

    auto key = keyExtractor(itemArg);

    if (ascendingType->IsTuple()) {
        const auto ascendingTuple = AS_TYPE(TTupleType, ascendingType);
        if (ascendingTuple->GetElementsCount() == 0) {
            return list;
        }

        if (ascendingTuple->GetElementsCount() == 1) {
            ascending = Nth(ascending, 0);
            key = Nth(key, 0);
        }
    }

    TCallableBuilder callableBuilder(Env, callableName, listType);
    callableBuilder.Add(list);
    callableBuilder.Add(itemArg);
    callableBuilder.Add(key);
    callableBuilder.Add(ascending);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::BuildListNth(const std::string_view& callableName, TRuntimeNode list, TRuntimeNode n, TRuntimeNode ascending,
    const TUnaryLambda& keyExtractor)
{
    const auto listType = list.GetStaticType();
    MKQL_ENSURE(listType->IsList(), "Expected list.");

    const auto itemType = static_cast<const TListType&>(*listType).GetItemType();

    ThrowIfListOfVoid(itemType);
    MKQL_ENSURE(n.GetStaticType()->IsData(), "Expected data");
    MKQL_ENSURE(static_cast<const TDataType&>(*n.GetStaticType()).GetSchemeType() == NUdf::TDataType<ui64>::Id, "Expected ui64");

    const auto ascendingType = ascending.GetStaticType();
    const auto itemArg = Arg(itemType);

    auto key = keyExtractor(itemArg);

    if (ascendingType->IsTuple()) {
        const auto ascendingTuple = AS_TYPE(TTupleType, ascendingType);
        if (ascendingTuple->GetElementsCount() == 0) {
            return Take(list, n);
        }

        if (ascendingTuple->GetElementsCount() == 1) {
            ascending = Nth(ascending, 0);
            key = Nth(key, 0);
        }
    }

    TCallableBuilder callableBuilder(Env, callableName, listType);
    callableBuilder.Add(list);
    callableBuilder.Add(n);
    callableBuilder.Add(itemArg);
    callableBuilder.Add(key);
    callableBuilder.Add(ascending);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::BuildSort(const std::string_view& callableName, TRuntimeNode flow, TRuntimeNode ascending,
    const TUnaryLambda& keyExtractor)
{
    if (const auto flowType = flow.GetStaticType(); flowType->IsFlow() || flowType->IsStream()) {
        const bool newVersion = RuntimeVersion >= 25U && flowType->IsFlow();
        const auto condense = newVersion ?
            SqueezeToList(Map(flow, [&](TRuntimeNode item) { return Pickle(item); }), NewEmptyOptionalDataLiteral(NUdf::TDataType<ui64>::Id)) :
            Condense1(flow,
                [this](TRuntimeNode item) { return AsList(item); },
                [this](TRuntimeNode, TRuntimeNode) { return NewDataLiteral<bool>(false); },
                [this](TRuntimeNode item, TRuntimeNode state) { return Append(state, item); }
            );

        const auto finalKeyExtractor = newVersion ? [&](TRuntimeNode item) {
                auto itemType = AS_TYPE(TFlowType, flowType)->GetItemType();
                return keyExtractor(Unpickle(itemType, item));
            } : keyExtractor;

        return FlatMap(condense, [&](TRuntimeNode list) {
            auto stealed = RuntimeVersion >= 27U ? Steal(list) : list;
            auto sorted = BuildSort(RuntimeVersion >= 26U ? "UnstableSort" : callableName, stealed, ascending, finalKeyExtractor);
            return newVersion ? Map(LazyList(sorted), [&](TRuntimeNode item) {
                auto itemType = AS_TYPE(TFlowType, flowType)->GetItemType();
                return Unpickle(itemType, item);
            }) : sorted;
        });
    }

    return BuildListSort(callableName, flow, ascending, keyExtractor);
}

TRuntimeNode TProgramBuilder::BuildNth(const std::string_view& callableName, TRuntimeNode flow, TRuntimeNode n, TRuntimeNode ascending,
    const TUnaryLambda& keyExtractor)
{
    if (const auto flowType = flow.GetStaticType(); flowType->IsFlow() || flowType->IsStream()) {
        return FlatMap(Condense1(flow,
                [this](TRuntimeNode item) { return AsList(item); },
                [this](TRuntimeNode, TRuntimeNode) { return NewDataLiteral<bool>(false); },
                [this](TRuntimeNode item, TRuntimeNode state) { return Append(state, item); }
            ),
            [&](TRuntimeNode list) { return BuildNth(callableName, list, n, ascending, keyExtractor); }
        );
    }

    return BuildListNth(callableName, flow, n, ascending, keyExtractor);
}

TRuntimeNode TProgramBuilder::BuildTake(const std::string_view& callableName, TRuntimeNode flow, TRuntimeNode count) {
    const auto listType = flow.GetStaticType();

    TType* itemType = nullptr;
    if (listType->IsFlow()) {
        itemType = AS_TYPE(TFlowType, listType)->GetItemType();
    } else if (listType->IsList()) {
        itemType = AS_TYPE(TListType, listType)->GetItemType();
    } else if (listType->IsStream()) {
        itemType = AS_TYPE(TStreamType, listType)->GetItemType();
    }

    MKQL_ENSURE(itemType, "Expected flow, list or stream.");
    ThrowIfListOfVoid(itemType);

    MKQL_ENSURE(count.GetStaticType()->IsData(), "Expected data");
    MKQL_ENSURE(static_cast<const TDataType&>(*count.GetStaticType()).GetSchemeType() == NUdf::TDataType<ui64>::Id, "Expected ui64");

    TCallableBuilder callableBuilder(Env, callableName, listType);
    callableBuilder.Add(flow);
    callableBuilder.Add(count);
    return TRuntimeNode(callableBuilder.Build(), false);
}

template<bool IsFilter, bool OnStruct>
TRuntimeNode TProgramBuilder::BuildFilterNulls(TRuntimeNode list) {
    const auto listType = list.GetStaticType();

    TType* itemType;
    if (listType->IsFlow()) {
        itemType = AS_TYPE(TFlowType, listType)->GetItemType();
    } else if (listType->IsList()) {
        itemType = AS_TYPE(TListType, listType)->GetItemType();
    } else if (listType->IsStream()) {
        itemType = AS_TYPE(TStreamType, listType)->GetItemType();
    } else if (listType->IsOptional()) {
        itemType = AS_TYPE(TOptionalType, listType)->GetItemType();
    } else {
        THROW yexception() << "Expected flow or list or stream or optional of " << (OnStruct ? "struct." : "tuple.");
    }

    std::conditional_t<OnStruct, std::vector<std::pair<std::string_view, TType*>>, std::vector<TType*>> filteredItems;
    std::vector<std::conditional_t<OnStruct, std::string_view, ui32>> members;
    const bool multiOptional = CollectOptionalElements<IsFilter>(itemType, members, filteredItems);

    const auto predicate = [=](TRuntimeNode item) {
        std::vector<TRuntimeNode> checkMembers;
        checkMembers.reserve(members.size());
        std::transform(members.cbegin(), members.cend(), std::back_inserter(checkMembers),
            [=](const auto& i){ return Exists(Element(item, i)); });
        return And(checkMembers);
    };

    auto resultType = listType;

    if constexpr (IsFilter) {
        if (const auto filteredItemType = NewArrayType(filteredItems); multiOptional) {
            return BuildFilterNulls<OnStruct>(list, members, filteredItems);
        } else {
            resultType = listType->IsFlow() ?
                NewFlowType(filteredItemType):
                listType->IsList() ?
                    NewListType(filteredItemType):
                    listType->IsStream() ? NewStreamType(filteredItemType) : NewOptionalType(filteredItemType);
        }
    }

    return Filter(list, predicate, resultType);
}

template<bool IsFilter, bool OnStruct>
TRuntimeNode TProgramBuilder::BuildFilterNulls(TRuntimeNode list, const TArrayRef<std::conditional_t<OnStruct, const std::string_view, const ui32>>& members) {
    if (members.empty()) {
        return list;
    }

    const auto listType = list.GetStaticType();

    TType* itemType;
    if (listType->IsFlow()) {
        itemType = AS_TYPE(TFlowType, listType)->GetItemType();
    } else if (listType->IsList()) {
        itemType = AS_TYPE(TListType, listType)->GetItemType();
    } else if (listType->IsStream()) {
        itemType = AS_TYPE(TStreamType, listType)->GetItemType();
    } else if (listType->IsOptional()) {
        itemType = AS_TYPE(TOptionalType, listType)->GetItemType();
    } else {
        THROW yexception() << "Expected flow or list or stream or optional of struct.";
    }

    const auto predicate = [=](TRuntimeNode item) {
        TRuntimeNode::TList checkMembers;
        checkMembers.reserve(members.size());
        std::transform(members.cbegin(), members.cend(), std::back_inserter(checkMembers),
            [=](const auto& i){ return Exists(Element(item, i)); });
        return And(checkMembers);
    };

    auto resultType = listType;

    if constexpr (IsFilter) {
        if (std::conditional_t<OnStruct, std::vector<std::pair<std::string_view, TType*>>, std::vector<TType*>> filteredItems;
            ReduceOptionalElements(itemType, members, filteredItems)) {
            return BuildFilterNulls<OnStruct>(list, members, filteredItems);
        } else {
            const auto filteredItemType = NewArrayType(filteredItems);
            resultType = listType->IsFlow() ?
                NewFlowType(filteredItemType):
                listType->IsList() ?
                    NewListType(filteredItemType):
                    listType->IsStream() ? NewStreamType(filteredItemType) : NewOptionalType(filteredItemType);
        }
    }

    return Filter(list, predicate, resultType);
}

template<bool OnStruct>
TRuntimeNode TProgramBuilder::BuildFilterNulls(TRuntimeNode list, const TArrayRef<std::conditional_t<OnStruct, const std::string_view, const ui32>>& members,
    const std::conditional_t<OnStruct, std::vector<std::pair<std::string_view, TType*>>, std::vector<TType*>>& filteredItems) {
    return FlatMap(list, [&](TRuntimeNode item) {
        TRuntimeNode::TList checkMembers;
        checkMembers.reserve(members.size());
        std::transform(members.cbegin(), members.cend(), std::back_inserter(checkMembers),
            [=](const auto& i){ return Element(item, i); });

        return IfPresent(checkMembers, [&](TRuntimeNode::TList items) {
            std::conditional_t<OnStruct, std::vector<std::pair<std::string_view, TRuntimeNode>>, TRuntimeNode::TList> row;
            row.reserve(filteredItems.size());
            auto j = 0U;
            if constexpr (OnStruct) {
                std::transform(filteredItems.cbegin(), filteredItems.cend(), std::back_inserter(row),
                    [&](const std::pair<std::string_view, TType*>& i) {
                        const auto& member = i.first;
                        const bool passtrought = members.cend() == std::find(members.cbegin(), members.cend(), member);
                        return std::make_pair(member, passtrought ?  Element(item, member) : items[j++]);
                    }
                );
                return NewOptional(NewStruct(row));
            } else {
                auto i = 0U;
                std::generate_n(std::back_inserter(row), filteredItems.size(),
                    [&]() {
                        const auto index = i++;
                        const bool passtrought = members.cend() == std::find(members.cbegin(), members.cend(), index);
                        return passtrought ? Element(item, index) : items[j++];
                    }
                );
                return NewOptional(NewTuple(row));
            }
        },  NewEmptyOptional(NewOptionalType(NewArrayType(filteredItems))));
    });
}

TRuntimeNode TProgramBuilder::SkipNullMembers(TRuntimeNode list) {
    return BuildFilterNulls<false, true>(list);
}

TRuntimeNode TProgramBuilder::FilterNullMembers(TRuntimeNode list) {
    return BuildFilterNulls<true, true>(list);
}

TRuntimeNode TProgramBuilder::SkipNullMembers(TRuntimeNode list, const TArrayRef<const std::string_view>& members) {
    return BuildFilterNulls<false, true>(list, members);
}

TRuntimeNode TProgramBuilder::FilterNullMembers(TRuntimeNode list, const TArrayRef<const std::string_view>& members) {
    return BuildFilterNulls<true, true>(list, members);
}

TRuntimeNode TProgramBuilder::FilterNullElements(TRuntimeNode list) {
    return BuildFilterNulls<true, false>(list);
}

TRuntimeNode TProgramBuilder::SkipNullElements(TRuntimeNode list) {
    return BuildFilterNulls<false, false>(list);
}

TRuntimeNode TProgramBuilder::FilterNullElements(TRuntimeNode list, const TArrayRef<const ui32>& elements) {
    return BuildFilterNulls<true, false>(list, elements);
}

TRuntimeNode TProgramBuilder::SkipNullElements(TRuntimeNode list, const TArrayRef<const ui32>& elements) {
    return BuildFilterNulls<false, false>(list, elements);
}

template <typename ResultType>
TRuntimeNode TProgramBuilder::BuildContainerProperty(const std::string_view& callableName, TRuntimeNode listOrDict) {
    const auto type = listOrDict.GetStaticType();
    MKQL_ENSURE(type->IsList() || type->IsDict() || type->IsEmptyList() || type->IsEmptyDict(), "Expected list or dict.");

    if (type->IsList()) {
        const auto itemType = AS_TYPE(TListType, type)->GetItemType();
        ThrowIfListOfVoid(itemType);
    }

    TCallableBuilder callableBuilder(Env, callableName, NewDataType(NUdf::TDataType<ResultType>::Id));
    callableBuilder.Add(listOrDict);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::Length(TRuntimeNode listOrDict) {
    return BuildContainerProperty<ui64>(__func__, listOrDict);
}

TRuntimeNode TProgramBuilder::Iterator(TRuntimeNode list, const TArrayRef<const TRuntimeNode>& dependentNodes) {
    const auto streamType = NewStreamType(AS_TYPE(TListType, list.GetStaticType())->GetItemType());
    TCallableBuilder callableBuilder(Env, __func__, streamType);
    callableBuilder.Add(list);
    for (auto node : dependentNodes) {
        callableBuilder.Add(node);
    }
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::EmptyIterator(TType* streamType) {
    MKQL_ENSURE(streamType->IsStream() || streamType->IsFlow(), "Expected stream or flow.");
    if (RuntimeVersion < 7U && streamType->IsFlow()) {
        return ToFlow(EmptyIterator(NewStreamType(AS_TYPE(TFlowType, streamType)->GetItemType())));
    }
    TCallableBuilder callableBuilder(Env, __func__, streamType);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::Collect(TRuntimeNode flow) {
    const auto seqType = flow.GetStaticType();
    TType* itemType = nullptr;
    if (seqType->IsFlow()) {
        itemType = AS_TYPE(TFlowType, seqType)->GetItemType();
    } else if (seqType->IsList()) {
        itemType = AS_TYPE(TListType, seqType)->GetItemType();
    } else if (seqType->IsStream()) {
        itemType = AS_TYPE(TStreamType, seqType)->GetItemType();
    } else {
        THROW yexception() << "Expected flow, list or stream.";
    }

    TCallableBuilder callableBuilder(Env, __func__, NewListType(itemType));
    callableBuilder.Add(flow);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::LazyList(TRuntimeNode list) {
    const auto type = list.GetStaticType();
    bool isOptional;
    const auto listType = UnpackOptional(type, isOptional);
    MKQL_ENSURE(listType->IsList(), "Expected list");
    TCallableBuilder callableBuilder(Env, __func__, type);
    callableBuilder.Add(list);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::ForwardList(TRuntimeNode stream) {
    const auto type = stream.GetStaticType();
    MKQL_ENSURE(type->IsStream() || type->IsFlow(), "Expected flow or stream.");
    if constexpr (RuntimeVersion < 10U) {
        if (type->IsFlow()) {
            return ForwardList(FromFlow(stream));
        }
    }
    TCallableBuilder callableBuilder(Env, __func__, NewListType(type->IsFlow() ? AS_TYPE(TFlowType, stream)->GetItemType() : AS_TYPE(TStreamType, stream)->GetItemType()));
    callableBuilder.Add(stream);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::ToFlow(TRuntimeNode stream) {
    const auto type = stream.GetStaticType();
    MKQL_ENSURE(type->IsStream() || type->IsList() || type->IsOptional(), "Expected stream, list or optional.");
    const auto itemType = type->IsStream() ? AS_TYPE(TStreamType, stream)->GetItemType() :
        type->IsList() ? AS_TYPE(TListType, stream)->GetItemType() : AS_TYPE(TOptionalType, stream)->GetItemType();
    TCallableBuilder callableBuilder(Env, __func__, NewFlowType(itemType));
    callableBuilder.Add(stream);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::FromFlow(TRuntimeNode flow) {
    MKQL_ENSURE(flow.GetStaticType()->IsFlow(), "Expected flow.");
    TCallableBuilder callableBuilder(Env, __func__, NewStreamType(AS_TYPE(TFlowType, flow)->GetItemType()));
    callableBuilder.Add(flow);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::Steal(TRuntimeNode input) {
    if constexpr (RuntimeVersion < 27U) {
        THROW yexception() << "Runtime version (" << RuntimeVersion << ") too old for " << __func__;
    }

    TCallableBuilder callableBuilder(Env, __func__, input.GetStaticType(), true);
    callableBuilder.Add(input);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::ToBlocks(TRuntimeNode flow) {
    auto* flowType = AS_TYPE(TFlowType, flow.GetStaticType());
    auto* blockType = NewBlockType(flowType->GetItemType(), TBlockType::EShape::Many);

    TCallableBuilder callableBuilder(Env, __func__, NewFlowType(blockType));
    callableBuilder.Add(flow);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::WideToBlocks(TRuntimeNode flow) {
    TType* outputItemType;
    {
        const auto wideComponents = GetWideComponents(AS_TYPE(TFlowType, flow.GetStaticType()));
        std::vector<TType*> outputItems;
        outputItems.reserve(wideComponents.size());
        for (size_t i = 0; i < wideComponents.size(); ++i) {
            outputItems.push_back(NewBlockType(wideComponents[i], TBlockType::EShape::Many));
        }
        outputItems.push_back(NewBlockType(NewDataType(NUdf::TDataType<ui64>::Id), TBlockType::EShape::Scalar));
        outputItemType = NewMultiType(outputItems);
    }

    TCallableBuilder callableBuilder(Env, __func__, NewFlowType(outputItemType));
    callableBuilder.Add(flow);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::FromBlocks(TRuntimeNode flow) {
    auto* flowType = AS_TYPE(TFlowType, flow.GetStaticType());
    auto* blockType = AS_TYPE(TBlockType, flowType->GetItemType());

    TCallableBuilder callableBuilder(Env, __func__, NewFlowType(blockType->GetItemType()));
    callableBuilder.Add(flow);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::WideFromBlocks(TRuntimeNode flow) {
    auto outputItems = ValidateBlockFlowType(flow.GetStaticType());
    outputItems.pop_back();
    TType* outputMultiType = NewMultiType(outputItems);
    TCallableBuilder callableBuilder(Env, __func__, NewFlowType(outputMultiType));
    callableBuilder.Add(flow);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::WideSkipBlocks(TRuntimeNode flow, TRuntimeNode count) {
    return BuildWideSkipTakeBlocks(__func__, flow, count);
}

TRuntimeNode TProgramBuilder::WideTakeBlocks(TRuntimeNode flow, TRuntimeNode count) {
    return BuildWideSkipTakeBlocks(__func__, flow, count);
}

TRuntimeNode TProgramBuilder::WideTopBlocks(TRuntimeNode flow, TRuntimeNode count, const std::vector<std::pair<ui32, TRuntimeNode>>& keys) {
    return BuildWideTopOrSort(__func__, flow, count, keys);
}

TRuntimeNode TProgramBuilder::WideTopSortBlocks(TRuntimeNode flow, TRuntimeNode count, const std::vector<std::pair<ui32, TRuntimeNode>>& keys) {
    return BuildWideTopOrSort(__func__, flow, count, keys);
}

TRuntimeNode TProgramBuilder::WideSortBlocks(TRuntimeNode flow, const std::vector<std::pair<ui32, TRuntimeNode>>& keys) {
    return BuildWideTopOrSort(__func__, flow, Nothing(), keys);
}

TRuntimeNode TProgramBuilder::AsScalar(TRuntimeNode value) {
    TCallableBuilder callableBuilder(Env, __func__, NewBlockType(value.GetStaticType(), TBlockType::EShape::Scalar));
    callableBuilder.Add(value);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::ReplicateScalar(TRuntimeNode value, TRuntimeNode count) {
    if constexpr (RuntimeVersion < 43U) {
        THROW yexception() << "Runtime version (" << RuntimeVersion << ") too old for " << __func__;
    }

    auto valueType = AS_TYPE(TBlockType, value.GetStaticType());
    auto countType = AS_TYPE(TBlockType, count.GetStaticType());

    MKQL_ENSURE(valueType->GetShape() == TBlockType::EShape::Scalar, "Expecting scalar as first arguemnt");
    MKQL_ENSURE(countType->GetShape() == TBlockType::EShape::Scalar, "Expecting scalar as second arguemnt");

    MKQL_ENSURE(countType->GetItemType()->IsData(), "Expected scalar data as second argument");
    MKQL_ENSURE(AS_TYPE(TDataType, countType->GetItemType())->GetSchemeType() ==
        NUdf::TDataType<ui64>::Id, "Expected scalar ui64 as second argument");

    auto outputType = NewBlockType(valueType->GetItemType(), TBlockType::EShape::Many);

    TCallableBuilder callableBuilder(Env, __func__, outputType);
    callableBuilder.Add(value);
    callableBuilder.Add(count);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::BlockCompress(TRuntimeNode flow, ui32 bitmapIndex) {
    auto blockItemTypes = ValidateBlockFlowType(flow.GetStaticType());

    MKQL_ENSURE(blockItemTypes.size() >= 2, "Expected at least two input columns");
    MKQL_ENSURE(bitmapIndex < blockItemTypes.size() - 1, "Invalid bitmap index");
    MKQL_ENSURE(AS_TYPE(TDataType, blockItemTypes[bitmapIndex])->GetSchemeType() == NUdf::TDataType<bool>::Id, "Expected Bool as bitmap column type");


    const auto wideComponents = GetWideComponents(AS_TYPE(TFlowType, flow.GetStaticType()));
    MKQL_ENSURE(wideComponents.size() == blockItemTypes.size(), "Unexpected tuple size");
    std::vector<TType*> flowItems;
    for (size_t i = 0; i < wideComponents.size(); ++i) {
        if (i == bitmapIndex) {
            continue;
        }
        flowItems.push_back(wideComponents[i]);
    }

    TCallableBuilder callableBuilder(Env, __func__, NewFlowType(NewMultiType(flowItems)));
    callableBuilder.Add(flow);
    callableBuilder.Add(NewDataLiteral<ui32>(bitmapIndex));
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::BlockExpandChunked(TRuntimeNode comp) {
    if (comp.GetStaticType()->IsStream()) {
        ValidateBlockStreamType(comp.GetStaticType());
    } else {
        ValidateBlockFlowType(comp.GetStaticType());
    }
    TCallableBuilder callableBuilder(Env, __func__, comp.GetStaticType());
    callableBuilder.Add(comp);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::BlockCoalesce(TRuntimeNode first, TRuntimeNode second) {
    auto firstType = AS_TYPE(TBlockType, first.GetStaticType());
    auto secondType = AS_TYPE(TBlockType, second.GetStaticType());

    auto firstItemType = firstType->GetItemType();
    auto secondItemType = secondType->GetItemType();

    MKQL_ENSURE(firstItemType->IsOptional() || firstItemType->IsPg(), "Expecting Optional or Pg type as first argument");

    if (!firstItemType->IsSameType(*secondItemType)) {
        bool firstOptional;
        firstItemType = UnpackOptional(firstItemType, firstOptional);
        MKQL_ENSURE(firstItemType->IsSameType(*secondItemType), "Uncompatible arguemnt types");
    }

    auto outputType = NewBlockType(secondType->GetItemType(), GetResultShape({firstType, secondType}));

    TCallableBuilder callableBuilder(Env, __func__, outputType);
    callableBuilder.Add(first);
    callableBuilder.Add(second);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::BlockExists(TRuntimeNode data) {
    auto dataType = AS_TYPE(TBlockType, data.GetStaticType());
    auto outputType = NewBlockType(NewDataType(NUdf::TDataType<bool>::Id), dataType->GetShape());

    TCallableBuilder callableBuilder(Env, __func__, outputType);
    callableBuilder.Add(data);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::BlockMember(TRuntimeNode structObj, const std::string_view& memberName) {
    auto blockType = AS_TYPE(TBlockType, structObj.GetStaticType());
    bool isOptional;
    const auto type = AS_TYPE(TStructType, UnpackOptional(blockType->GetItemType(), isOptional));

    const auto memberIndex = type->GetMemberIndex(memberName);
    auto memberType = type->GetMemberType(memberIndex);
    if (isOptional && !memberType->IsOptional() && !memberType->IsNull() && !memberType->IsPg()) {
        memberType = NewOptionalType(memberType);
    }

    auto returnType = NewBlockType(memberType, blockType->GetShape());
    TCallableBuilder callableBuilder(Env, __func__, returnType);
    callableBuilder.Add(structObj);
    callableBuilder.Add(NewDataLiteral<ui32>(memberIndex));
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::BlockNth(TRuntimeNode tuple, ui32 index) {
    auto blockType = AS_TYPE(TBlockType, tuple.GetStaticType());
    bool isOptional;
    const auto type = AS_TYPE(TTupleType, UnpackOptional(blockType->GetItemType(), isOptional));

    MKQL_ENSURE(index < type->GetElementsCount(), "Index out of range: " << index <<
        " is not less than " << type->GetElementsCount());
    auto itemType = type->GetElementType(index);
    if (isOptional && !itemType->IsOptional() && !itemType->IsNull() && !itemType->IsPg()) {
        itemType = TOptionalType::Create(itemType, Env);
    }

    auto returnType = NewBlockType(itemType, blockType->GetShape());
    TCallableBuilder callableBuilder(Env, __func__, returnType);
    callableBuilder.Add(tuple);
    callableBuilder.Add(NewDataLiteral<ui32>(index));
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::BlockAsStruct(const TArrayRef<std::pair<std::string_view, TRuntimeNode>>& args) {
    MKQL_ENSURE(!args.empty(), "Expected at least one argument");

    TBlockType::EShape resultShape = TBlockType::EShape::Scalar;
    TVector<std::pair<std::string_view, TType*>> members;
    for (const auto& x : args) {
        auto blockType = AS_TYPE(TBlockType, x.second.GetStaticType());
        members.emplace_back(x.first, blockType->GetItemType());
        if (blockType->GetShape() == TBlockType::EShape::Many) {
            resultShape = TBlockType::EShape::Many;
        }
    }

    auto returnType = NewBlockType(NewStructType(members), resultShape);
    TCallableBuilder callableBuilder(Env, __func__, returnType);
    for (const auto& x : args) {
        callableBuilder.Add(x.second);
    }

    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::BlockAsTuple(const TArrayRef<const TRuntimeNode>& args) {
    MKQL_ENSURE(!args.empty(), "Expected at least one argument");

    TBlockType::EShape resultShape = TBlockType::EShape::Scalar;
    TVector<TType*> types;
    for (const auto& x : args) {
        auto blockType = AS_TYPE(TBlockType, x.GetStaticType());
        types.push_back(blockType->GetItemType());
        if (blockType->GetShape() == TBlockType::EShape::Many) {
            resultShape = TBlockType::EShape::Many;
        }
    }

    auto tupleType = NewTupleType(types);
    auto returnType = NewBlockType(tupleType, resultShape);
    TCallableBuilder callableBuilder(Env, __func__, returnType);
    for (const auto& x : args) {
        callableBuilder.Add(x);
    }

    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::BlockToPg(TRuntimeNode input, TType* returnType) {
    if constexpr (RuntimeVersion < 37U) {
        THROW yexception() << "Runtime version (" << RuntimeVersion << ") too old for " << __func__;
    }

    TCallableBuilder callableBuilder(Env, __func__, returnType);
    callableBuilder.Add(input);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::BlockFromPg(TRuntimeNode input, TType* returnType) {
    if constexpr (RuntimeVersion < 37U) {
        THROW yexception() << "Runtime version (" << RuntimeVersion << ") too old for " << __func__;
    }

    TCallableBuilder callableBuilder(Env, __func__, returnType);
    callableBuilder.Add(input);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::BlockNot(TRuntimeNode data) {
    auto dataType = AS_TYPE(TBlockType, data.GetStaticType());

    bool isOpt;
    MKQL_ENSURE(UnpackOptionalData(dataType->GetItemType(), isOpt)->GetSchemeType() == NUdf::TDataType<bool>::Id, "Requires boolean args.");

    TCallableBuilder callableBuilder(Env, __func__, data.GetStaticType());
    callableBuilder.Add(data);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::BlockAnd(TRuntimeNode first, TRuntimeNode second) {
    return BuildBlockLogical(__func__, first, second);
}

TRuntimeNode TProgramBuilder::BlockOr(TRuntimeNode first, TRuntimeNode second) {
    return BuildBlockLogical(__func__, first, second);
}

TRuntimeNode TProgramBuilder::BlockXor(TRuntimeNode first, TRuntimeNode second) {
    return BuildBlockLogical(__func__, first, second);
}

TRuntimeNode TProgramBuilder::ListFromRange(TRuntimeNode start, TRuntimeNode end, TRuntimeNode step) {
    MKQL_ENSURE(start.GetStaticType()->IsData(), "Expected data");
    MKQL_ENSURE(end.GetStaticType()->IsSameType(*start.GetStaticType()), "Mismatch type");

    if constexpr (RuntimeVersion < 24U) {
        MKQL_ENSURE(IsNumericType(AS_TYPE(TDataType, start)->GetSchemeType()), "Expected numeric");
    } else {
        MKQL_ENSURE(IsNumericType(AS_TYPE(TDataType, start)->GetSchemeType()) ||
            IsDateType(AS_TYPE(TDataType, start)->GetSchemeType()) ||
            IsTzDateType(AS_TYPE(TDataType, start)->GetSchemeType()) ||
            IsIntervalType(AS_TYPE(TDataType, start)->GetSchemeType()),
            "Expected numeric, date or tzdate");

        if (IsNumericType(AS_TYPE(TDataType, start)->GetSchemeType())) {
            MKQL_ENSURE(IsNumericType(AS_TYPE(TDataType, step)->GetSchemeType()), "Expected numeric");
        } else {
            MKQL_ENSURE(IsIntervalType(AS_TYPE(TDataType, step)->GetSchemeType()), "Expected interval");
        }
    }

    TCallableBuilder callableBuilder(Env, __func__, TListType::Create(start.GetStaticType(), Env));
    callableBuilder.Add(start);
    callableBuilder.Add(end);
    callableBuilder.Add(step);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::Switch(TRuntimeNode stream,
    const TArrayRef<const TSwitchInput>& handlerInputs,
    std::function<TRuntimeNode(ui32 index, TRuntimeNode item)> handler,
    ui64 memoryLimitBytes, TType* returnType) {
    MKQL_ENSURE(stream.GetStaticType()->IsStream() || stream.GetStaticType()->IsFlow(), "Expected stream or flow.");
    std::vector<TRuntimeNode> argNodes(handlerInputs.size());
    std::vector<TRuntimeNode> outputNodes(handlerInputs.size());
    for (ui32 i = 0; i < handlerInputs.size(); ++i) {
        TRuntimeNode arg = Arg(handlerInputs[i].InputType);
        argNodes[i] = arg;
        outputNodes[i] = handler(i, arg);
    }

    TCallableBuilder callableBuilder(Env, __func__, returnType);
    callableBuilder.Add(stream);
    callableBuilder.Add(NewDataLiteral<ui64>(memoryLimitBytes));
    for (ui32 i = 0; i < handlerInputs.size(); ++i) {
        std::vector<TRuntimeNode> tupleElems;
        for (auto index : handlerInputs[i].Indicies) {
            tupleElems.push_back(NewDataLiteral<ui32>(index));
        }

        auto indiciesTuple = NewTuple(tupleElems);
        callableBuilder.Add(indiciesTuple);
        callableBuilder.Add(argNodes[i]);
        callableBuilder.Add(outputNodes[i]);
        if (!handlerInputs[i].ResultVariantOffset) {
            callableBuilder.Add(NewVoid());
        } else {
            callableBuilder.Add(NewDataLiteral<ui32>(*handlerInputs[i].ResultVariantOffset));
        }
    }

    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::HasItems(TRuntimeNode listOrDict) {
    return BuildContainerProperty<bool>(__func__, listOrDict);
}

TRuntimeNode TProgramBuilder::Reverse(TRuntimeNode list) {
    bool isOptional = false;
    const auto listType = UnpackOptional(list, isOptional);

    if (isOptional) {
        return Map(list, [&](TRuntimeNode unpacked) { return Reverse(unpacked); } );
    }

    const auto listDetailedType = AS_TYPE(TListType, listType);
    const auto itemType = listDetailedType->GetItemType();
    ThrowIfListOfVoid(itemType);

    TCallableBuilder callableBuilder(Env, __func__, listType);
    callableBuilder.Add(list);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::Skip(TRuntimeNode list, TRuntimeNode count) {
    return BuildTake(__func__, list, count);
}

TRuntimeNode TProgramBuilder::Take(TRuntimeNode list, TRuntimeNode count) {
    return BuildTake(__func__, list, count);
}

TRuntimeNode TProgramBuilder::Sort(TRuntimeNode list, TRuntimeNode ascending, const TUnaryLambda& keyExtractor)
{
    return BuildSort(__func__, list, ascending, keyExtractor);
}

TRuntimeNode TProgramBuilder::WideTop(TRuntimeNode flow, TRuntimeNode count, const std::vector<std::pair<ui32, TRuntimeNode>>& keys)
{
    return BuildWideTopOrSort(__func__, flow, count, keys);
}

TRuntimeNode TProgramBuilder::WideTopSort(TRuntimeNode flow, TRuntimeNode count, const std::vector<std::pair<ui32, TRuntimeNode>>& keys)
{
    return BuildWideTopOrSort(__func__, flow, count, keys);
}

TRuntimeNode TProgramBuilder::WideSort(TRuntimeNode flow, const std::vector<std::pair<ui32, TRuntimeNode>>& keys)
{
    return BuildWideTopOrSort(__func__, flow, Nothing(), keys);
}

TRuntimeNode TProgramBuilder::BuildWideTopOrSort(const std::string_view& callableName, TRuntimeNode flow, TMaybe<TRuntimeNode> count, const std::vector<std::pair<ui32, TRuntimeNode>>& keys) {
    if (count) {
        if constexpr (RuntimeVersion < 33U) {
            THROW yexception() << "Runtime version (" << RuntimeVersion << ") too old for " << callableName;
        }
    } else {
        if constexpr (RuntimeVersion < 34U) {
            THROW yexception() << "Runtime version (" << RuntimeVersion << ") too old for " << callableName;
        }
    }

    const auto width = GetWideComponentsCount(AS_TYPE(TFlowType, flow.GetStaticType()));
    MKQL_ENSURE(!keys.empty() && keys.size() <= width, "Unexpected keys count: " << keys.size());

    TCallableBuilder callableBuilder(Env, callableName, flow.GetStaticType());
    callableBuilder.Add(flow);
    if (count) {
        callableBuilder.Add(*count);
    }

    std::for_each(keys.cbegin(), keys.cend(), [&](const std::pair<ui32, TRuntimeNode>& key) {
        MKQL_ENSURE(key.first < width, "Key index too large: " << key.first);
        callableBuilder.Add(NewDataLiteral(key.first));
        callableBuilder.Add(key.second);
    });
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::Top(TRuntimeNode flow, TRuntimeNode count, TRuntimeNode ascending, const TUnaryLambda& keyExtractor) {
    if (const auto flowType = flow.GetStaticType(); flowType->IsFlow() || flowType->IsStream()) {
        const TUnaryLambda getKey = [&](TRuntimeNode item) { return Nth(item, 0U); };
        const TUnaryLambda getItem = [&](TRuntimeNode item) { return Nth(item, 1U); };
        const TUnaryLambda cacheKeyExtractor = [&](TRuntimeNode item) {
            return NewTuple({keyExtractor(item), item});
        };

        return FlatMap(Condense1(Map(flow, cacheKeyExtractor),
                [&](TRuntimeNode item) { return AsList(item); },
                [this](TRuntimeNode, TRuntimeNode) { return NewDataLiteral<bool>(false); },
                [&](TRuntimeNode item, TRuntimeNode state) {
                    return KeepTop(count, state, item, ascending, getKey);
                }
            ),
            [&](TRuntimeNode list) { return Map(Top(list, count, ascending, getKey), getItem); }
        );
    }

    return BuildListNth(__func__, flow, count, ascending, keyExtractor);
}

TRuntimeNode TProgramBuilder::TopSort(TRuntimeNode flow, TRuntimeNode count, TRuntimeNode ascending, const TUnaryLambda& keyExtractor) {
    if (const auto flowType = flow.GetStaticType(); flowType->IsFlow() || flowType->IsStream()) {
        const TUnaryLambda getKey = [&](TRuntimeNode item) { return Nth(item, 0U); };
        const TUnaryLambda getItem = [&](TRuntimeNode item) { return Nth(item, 1U); };
        const TUnaryLambda cacheKeyExtractor = [&](TRuntimeNode item) {
            return NewTuple({keyExtractor(item), item});
        };

        return FlatMap(Condense1(Map(flow, cacheKeyExtractor),
                [&](TRuntimeNode item) { return AsList(item); },
                [this](TRuntimeNode, TRuntimeNode) { return NewDataLiteral<bool>(false); },
                [&](TRuntimeNode item, TRuntimeNode state) {
                    return KeepTop(count, state, item, ascending, getKey);
                }
            ),
            [&](TRuntimeNode list) { return Map(TopSort(list, count, ascending, getKey), getItem); }
        );
    }

    if constexpr (RuntimeVersion >= 25U)
        return BuildListNth(__func__, flow, count, ascending, keyExtractor);
    else
        return BuildListSort("Sort", BuildListNth("Top", flow, count, ascending, keyExtractor), ascending, keyExtractor);
}

TRuntimeNode TProgramBuilder::KeepTop(TRuntimeNode count, TRuntimeNode list, TRuntimeNode item, TRuntimeNode ascending, const TUnaryLambda& keyExtractor) {
    const auto listType = list.GetStaticType();
    MKQL_ENSURE(listType->IsList(), "Expected list.");

    const auto itemType = static_cast<const TListType&>(*listType).GetItemType();

    ThrowIfListOfVoid(itemType);
    MKQL_ENSURE(count.GetStaticType()->IsData(), "Expected data");
    MKQL_ENSURE(static_cast<const TDataType&>(*count.GetStaticType()).GetSchemeType() == NUdf::TDataType<ui64>::Id, "Expected ui64");

    MKQL_ENSURE(itemType->IsSameType(*item.GetStaticType()), "Types of list and item are different.");

    const auto ascendingType = ascending.GetStaticType();
    const auto itemArg = Arg(itemType);

    auto key = keyExtractor(itemArg);
    const auto hotkey = Arg(key.GetStaticType());

    if (ascendingType->IsTuple()) {
        const auto ascendingTuple = AS_TYPE(TTupleType, ascendingType);
        if (ascendingTuple->GetElementsCount() == 0) {
            return If(AggrLess(Length(list), count), Append(list, item), list);
        }

        if (ascendingTuple->GetElementsCount() == 1) {
            ascending = Nth(ascending, 0);
            key = Nth(key, 0);
        }
    }

    TCallableBuilder callableBuilder(Env, __func__, listType);
    callableBuilder.Add(count);
    callableBuilder.Add(list);
    callableBuilder.Add(item);
    callableBuilder.Add(itemArg);
    callableBuilder.Add(key);
    callableBuilder.Add(ascending);
    callableBuilder.Add(hotkey);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::Contains(TRuntimeNode dict, TRuntimeNode key) {
    if constexpr (RuntimeVersion >= 25U)
        if (!dict.GetStaticType()->IsDict())
            return DataCompare(__func__, dict, key);

    const auto keyType = AS_TYPE(TDictType, dict.GetStaticType())->GetKeyType();
    MKQL_ENSURE(keyType->IsSameType(*key.GetStaticType()), "Key type mismatch. Requred: " << *keyType << ", but got: " << *key.GetStaticType());

    TCallableBuilder callableBuilder(Env, __func__, NewDataType(NUdf::TDataType<bool>::Id));
    callableBuilder.Add(dict);
    callableBuilder.Add(key);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::Lookup(TRuntimeNode dict, TRuntimeNode key) {
    const auto dictType = AS_TYPE(TDictType, dict.GetStaticType());
    const auto keyType = dictType->GetKeyType();
    MKQL_ENSURE(keyType->IsSameType(*key.GetStaticType()), "Key type mismatch. Requred: " << *keyType << ", but got: " << *key.GetStaticType());

    TCallableBuilder callableBuilder(Env, __func__, NewOptionalType(dictType->GetPayloadType()));
    callableBuilder.Add(dict);
    callableBuilder.Add(key);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::DictItems(TRuntimeNode dict, EDictItems mode) {
    const auto dictTypeChecked = AS_TYPE(TDictType, dict.GetStaticType());
    TType* itemType;
    switch (mode) {
        case EDictItems::Both: {
            const std::array<TType*, 2U> tupleTypes = {{ dictTypeChecked->GetKeyType(), dictTypeChecked->GetPayloadType() }};
            itemType = NewTupleType(tupleTypes);
            break;
        }
        case EDictItems::Keys: itemType = dictTypeChecked->GetKeyType(); break;
        case EDictItems::Payloads: itemType = dictTypeChecked->GetPayloadType(); break;
    }

    TCallableBuilder callableBuilder(Env, __func__, NewListType(itemType));
    callableBuilder.Add(dict);
    callableBuilder.Add(NewDataLiteral((ui32)mode));
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::DictItems(TRuntimeNode dict) {
    if constexpr (RuntimeVersion < 6U) {
        return DictItems(dict, EDictItems::Both);
    }
    const auto dictTypeChecked = AS_TYPE(TDictType, dict.GetStaticType());
    const auto itemType = NewTupleType({ dictTypeChecked->GetKeyType(), dictTypeChecked->GetPayloadType() });
    TCallableBuilder callableBuilder(Env, __func__, NewListType(itemType));
    callableBuilder.Add(dict);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::DictKeys(TRuntimeNode dict) {
    if constexpr (RuntimeVersion < 6U) {
        return DictItems(dict, EDictItems::Keys);
    }
    const auto dictTypeChecked = AS_TYPE(TDictType, dict.GetStaticType());
    TCallableBuilder callableBuilder(Env, __func__, NewListType(dictTypeChecked->GetKeyType()));
    callableBuilder.Add(dict);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::DictPayloads(TRuntimeNode dict) {
    if constexpr (RuntimeVersion < 6U) {
        return DictItems(dict, EDictItems::Payloads);
    }
    const auto dictTypeChecked = AS_TYPE(TDictType, dict.GetStaticType());
    TCallableBuilder callableBuilder(Env, __func__, NewListType(dictTypeChecked->GetPayloadType()));
    callableBuilder.Add(dict);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::ToIndexDict(TRuntimeNode list) {
    const auto itemType = AS_TYPE(TListType, list.GetStaticType())->GetItemType();
    ThrowIfListOfVoid(itemType);
    const auto keyType = NewDataType(NUdf::TDataType<ui64>::Id);
    const auto dictType = NewDictType(keyType, itemType, false);
    TCallableBuilder callableBuilder(Env, __func__, dictType);
    callableBuilder.Add(list);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::JoinDict(TRuntimeNode dict1, bool isMulti1, TRuntimeNode dict2, bool isMulti2, EJoinKind joinKind) {
    const auto dict1type = AS_TYPE(TDictType, dict1);
    const auto dict2type = AS_TYPE(TDictType, dict2);
    MKQL_ENSURE(dict1type->GetKeyType()->IsSameType(*dict2type->GetKeyType()), "Dict key types must be the same");

    if (joinKind == EJoinKind::RightOnly || joinKind == EJoinKind::RightSemi)
        MKQL_ENSURE(dict1type->GetPayloadType()->IsVoid(), "Void required for first dict payload.");
    else if (isMulti1)
        MKQL_ENSURE(dict1type->GetPayloadType()->IsList(), "List required for first dict payload.");

    if (joinKind == EJoinKind::LeftOnly || joinKind == EJoinKind::LeftSemi)
        MKQL_ENSURE(dict2type->GetPayloadType()->IsVoid(), "Void required for second dict payload.");
    else if (isMulti2)
        MKQL_ENSURE(dict2type->GetPayloadType()->IsList(), "List required for second dict payload.");

    std::array<TType*, 2> tupleItems = {{ dict1type->GetPayloadType(), dict2type->GetPayloadType() }};
    if (isMulti1 && tupleItems.front()->IsList())
        tupleItems.front() = AS_TYPE(TListType, tupleItems.front())->GetItemType();

    if (isMulti2 && tupleItems.back()->IsList())
        tupleItems.back() = AS_TYPE(TListType, tupleItems.back())->GetItemType();

    if (IsLeftOptional(joinKind))
        tupleItems.front() = NewOptionalType(tupleItems.front());

    if (IsRightOptional(joinKind))
        tupleItems.back() = NewOptionalType(tupleItems.back());

    TType* itemType;
    if (joinKind == EJoinKind::LeftOnly || joinKind == EJoinKind::LeftSemi)
        itemType = tupleItems.front();
    else if (joinKind == EJoinKind::RightOnly || joinKind == EJoinKind::RightSemi)
        itemType = tupleItems.back();
    else
        itemType = NewTupleType(tupleItems);

    const auto returnType = NewListType(itemType);
    TCallableBuilder callableBuilder(Env, __func__, returnType);
    callableBuilder.Add(dict1);
    callableBuilder.Add(dict2);
    callableBuilder.Add(NewDataLiteral(isMulti1));
    callableBuilder.Add(NewDataLiteral(isMulti2));
    callableBuilder.Add(NewDataLiteral(ui32(joinKind)));
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::GraceJoinCommon(const TStringBuf& funcName, TRuntimeNode flowLeft, TRuntimeNode flowRight, EJoinKind joinKind,
        const TArrayRef<const ui32>& leftKeyColumns, const TArrayRef<const ui32>& rightKeyColumns,
        const TArrayRef<const ui32>& leftRenames, const TArrayRef<const ui32>& rightRenames, TType* returnType, EAnyJoinSettings anyJoinSettings ) {

    MKQL_ENSURE(!leftKeyColumns.empty(), "At least one key column must be specified");
    if (flowRight) {
        MKQL_ENSURE(!rightKeyColumns.empty(), "At least one key column must be specified");
    }

    TRuntimeNode::TList leftKeyColumnsNodes, rightKeyColumnsNodes, leftRenamesNodes, rightRenamesNodes;

    leftKeyColumnsNodes.reserve(leftKeyColumns.size());
    std::transform(leftKeyColumns.cbegin(), leftKeyColumns.cend(), std::back_inserter(leftKeyColumnsNodes), [this](const ui32 idx) { return NewDataLiteral(idx); });

    rightKeyColumnsNodes.reserve(rightKeyColumns.size());
    std::transform(rightKeyColumns.cbegin(), rightKeyColumns.cend(), std::back_inserter(rightKeyColumnsNodes), [this](const ui32 idx) { return NewDataLiteral(idx); });

    leftRenamesNodes.reserve(leftRenames.size());
    std::transform(leftRenames.cbegin(), leftRenames.cend(), std::back_inserter(leftRenamesNodes), [this](const ui32 idx) { return NewDataLiteral(idx); });

    rightRenamesNodes.reserve(rightRenames.size());
    std::transform(rightRenames.cbegin(), rightRenames.cend(), std::back_inserter(rightRenamesNodes), [this](const ui32 idx) { return NewDataLiteral(idx); });


    TCallableBuilder callableBuilder(Env, funcName, returnType);
    callableBuilder.Add(flowLeft);
    if (flowRight) {
        callableBuilder.Add(flowRight);
    }
    callableBuilder.Add(NewDataLiteral((ui32)joinKind));
    callableBuilder.Add(NewTuple(leftKeyColumnsNodes));
    callableBuilder.Add(NewTuple(rightKeyColumnsNodes));
    callableBuilder.Add(NewTuple(leftRenamesNodes));
    callableBuilder.Add(NewTuple(rightRenamesNodes));
    callableBuilder.Add(NewDataLiteral((ui32)anyJoinSettings));

    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::GraceJoin(TRuntimeNode flowLeft, TRuntimeNode flowRight, EJoinKind joinKind,
        const TArrayRef<const ui32>& leftKeyColumns, const TArrayRef<const ui32>& rightKeyColumns,
        const TArrayRef<const ui32>& leftRenames, const TArrayRef<const ui32>& rightRenames, TType* returnType, EAnyJoinSettings anyJoinSettings ) {

    return GraceJoinCommon(__func__, flowLeft, flowRight, joinKind, leftKeyColumns, rightKeyColumns, leftRenames, rightRenames, returnType, anyJoinSettings);
}

TRuntimeNode TProgramBuilder::GraceSelfJoin(TRuntimeNode flowLeft,  EJoinKind joinKind, const TArrayRef<const ui32>& leftKeyColumns, const TArrayRef<const ui32>& rightKeyColumns,
        const TArrayRef<const ui32>& leftRenames, const TArrayRef<const ui32>& rightRenames, TType* returnType, EAnyJoinSettings anyJoinSettings ) {

    if constexpr (RuntimeVersion < 40U) {
        THROW yexception() << "Runtime version (" << RuntimeVersion << ") too old for " << __func__;
    }

    return GraceJoinCommon(__func__, flowLeft, {}, joinKind, leftKeyColumns, rightKeyColumns, leftRenames, rightRenames, returnType, anyJoinSettings);
}

TRuntimeNode TProgramBuilder::ToSortedDict(TRuntimeNode list, bool all, const TUnaryLambda& keySelector,
    const TUnaryLambda& payloadSelector, bool isCompact, ui64 itemsCountHint) {
    return ToDict(list, all, keySelector, payloadSelector, __func__, isCompact, itemsCountHint);
}

TRuntimeNode TProgramBuilder::ToHashedDict(TRuntimeNode list, bool all, const TUnaryLambda& keySelector,
    const TUnaryLambda& payloadSelector, bool isCompact, ui64 itemsCountHint) {
    return ToDict(list, all, keySelector, payloadSelector, __func__, isCompact, itemsCountHint);
}

TRuntimeNode TProgramBuilder::SqueezeToSortedDict(TRuntimeNode stream, bool all, const TUnaryLambda& keySelector,
    const TUnaryLambda& payloadSelector, bool isCompact, ui64 itemsCountHint) {
    return SqueezeToDict(stream, all, keySelector, payloadSelector, __func__, isCompact, itemsCountHint);
}

TRuntimeNode TProgramBuilder::SqueezeToHashedDict(TRuntimeNode stream, bool all, const TUnaryLambda& keySelector,
    const TUnaryLambda& payloadSelector, bool isCompact, ui64 itemsCountHint) {
    return SqueezeToDict(stream, all, keySelector, payloadSelector, __func__, isCompact, itemsCountHint);
}

TRuntimeNode TProgramBuilder::NarrowSqueezeToSortedDict(TRuntimeNode stream, bool all, const TNarrowLambda& keySelector,
    const TNarrowLambda& payloadSelector, bool isCompact, ui64 itemsCountHint) {
    return NarrowSqueezeToDict(stream, all, keySelector, payloadSelector, __func__, isCompact, itemsCountHint);
}

TRuntimeNode TProgramBuilder::NarrowSqueezeToHashedDict(TRuntimeNode stream, bool all, const TNarrowLambda& keySelector,
    const TNarrowLambda& payloadSelector, bool isCompact, ui64 itemsCountHint) {
    return NarrowSqueezeToDict(stream, all, keySelector, payloadSelector, __func__, isCompact, itemsCountHint);
}

TRuntimeNode TProgramBuilder::SqueezeToList(TRuntimeNode flow, TRuntimeNode limit) {
    if constexpr (RuntimeVersion < 25U) {
        THROW yexception() << "Runtime version (" << RuntimeVersion << ") too old for " << __func__;
    }

    const auto itemType = AS_TYPE(TFlowType, flow.GetStaticType())->GetItemType();
    TCallableBuilder callableBuilder(Env, __func__, NewFlowType(NewListType(itemType)));
    callableBuilder.Add(flow);
    callableBuilder.Add(limit);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::Append(TRuntimeNode list, TRuntimeNode item) {
    auto listType = list.GetStaticType();
    AS_TYPE(TListType, listType);

    const auto& listDetailedType = static_cast<const TListType&>(*listType);
    auto itemType = item.GetStaticType();
    MKQL_ENSURE(itemType->IsSameType(*listDetailedType.GetItemType()), "Types of list and item are different");

    TCallableBuilder callableBuilder(Env, __func__, listType);
    callableBuilder.Add(list);
    callableBuilder.Add(item);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::Prepend(TRuntimeNode item, TRuntimeNode list) {
    auto listType = list.GetStaticType();
    AS_TYPE(TListType, listType);

    const auto& listDetailedType = static_cast<const TListType&>(*listType);
    auto itemType = item.GetStaticType();
    MKQL_ENSURE(itemType->IsSameType(*listDetailedType.GetItemType()), "Types of list and item are different");

    TCallableBuilder callableBuilder(Env, __func__, listType);
    callableBuilder.Add(item);
    callableBuilder.Add(list);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::BuildExtend(const std::string_view& callableName, const TArrayRef<const TRuntimeNode>& lists) {
    MKQL_ENSURE(lists.size() > 0, "Expected at least 1 list or flow");
    if (lists.size() == 1) {
        return lists.front();
    }

    auto listType = lists.front().GetStaticType();
    MKQL_ENSURE(listType->IsFlow() || listType->IsList() || listType->IsStream(), "Expected either flow, list or stream");
    for (ui32 i = 1; i < lists.size(); ++i) {
        auto listType2 = lists[i].GetStaticType();
        MKQL_ENSURE(listType->IsSameType(*listType2), "Types of flows are different, left: " <<
            PrintNode(listType, true) << ", right: " <<
            PrintNode(listType2, true));
    }

    TCallableBuilder callableBuilder(Env, callableName, listType);
    for (auto list : lists) {
        callableBuilder.Add(list);
    }

    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::Extend(const TArrayRef<const TRuntimeNode>& lists) {
    return BuildExtend(__func__, lists);
}

TRuntimeNode TProgramBuilder::OrderedExtend(const TArrayRef<const TRuntimeNode>& lists) {
    return BuildExtend(__func__, lists);
}

template<>
TRuntimeNode TProgramBuilder::NewDataLiteral<NUdf::EDataSlot::String>(const NUdf::TStringRef& data) const {
    return TRuntimeNode(BuildDataLiteral(data, NUdf::TDataType<const char*>::Id, Env), true);
}

template<>
TRuntimeNode TProgramBuilder::NewDataLiteral<NUdf::EDataSlot::Utf8>(const NUdf::TStringRef& data) const {
    return TRuntimeNode(BuildDataLiteral(data, NUdf::TDataType<NUdf::TUtf8>::Id, Env), true);
}

template<>
TRuntimeNode TProgramBuilder::NewDataLiteral<NUdf::EDataSlot::Yson>(const NUdf::TStringRef& data) const {
    return TRuntimeNode(BuildDataLiteral(data, NUdf::TDataType<NUdf::TYson>::Id, Env), true);
}

template<>
TRuntimeNode TProgramBuilder::NewDataLiteral<NUdf::EDataSlot::Json>(const NUdf::TStringRef& data) const {
    return TRuntimeNode(BuildDataLiteral(data, NUdf::TDataType<NUdf::TJson>::Id, Env), true);
}

template<>
TRuntimeNode TProgramBuilder::NewDataLiteral<NUdf::EDataSlot::JsonDocument>(const NUdf::TStringRef& data) const {
    return TRuntimeNode(BuildDataLiteral(data, NUdf::TDataType<NUdf::TJsonDocument>::Id, Env), true);
}

template<>
TRuntimeNode TProgramBuilder::NewDataLiteral<NUdf::EDataSlot::Uuid>(const NUdf::TStringRef& data) const {
    return TRuntimeNode(BuildDataLiteral(data, NUdf::TDataType<NUdf::TUuid>::Id, Env), true);
}

template<>
TRuntimeNode TProgramBuilder::NewDataLiteral<NUdf::EDataSlot::Date>(const NUdf::TStringRef& data) const {
    return TRuntimeNode(BuildDataLiteral(data, NUdf::TDataType<NUdf::TDate>::Id, Env), true);
}

template<>
TRuntimeNode TProgramBuilder::NewDataLiteral<NUdf::EDataSlot::Datetime>(const NUdf::TStringRef& data) const {
    return TRuntimeNode(BuildDataLiteral(data, NUdf::TDataType<NUdf::TDatetime>::Id, Env), true);
}

template<>
TRuntimeNode TProgramBuilder::NewDataLiteral<NUdf::EDataSlot::Timestamp>(const NUdf::TStringRef& data) const {
    return TRuntimeNode(BuildDataLiteral(data, NUdf::TDataType<NUdf::TTimestamp>::Id, Env), true);
}

template<>
TRuntimeNode TProgramBuilder::NewDataLiteral<NUdf::EDataSlot::Interval>(const NUdf::TStringRef& data) const {
    return TRuntimeNode(BuildDataLiteral(data, NUdf::TDataType<NUdf::TInterval>::Id, Env), true);
}

template<>
TRuntimeNode TProgramBuilder::NewDataLiteral<NUdf::EDataSlot::DyNumber>(const NUdf::TStringRef& data) const {
    return TRuntimeNode(BuildDataLiteral(data, NUdf::TDataType<NUdf::TDyNumber>::Id, Env), true);
}

TRuntimeNode TProgramBuilder::NewDecimalLiteral(NYql::NDecimal::TInt128 data, ui8 precision, ui8 scale) const {
    return TRuntimeNode(TDataLiteral::Create(NUdf::TUnboxedValuePod(data), TDataDecimalType::Create(precision, scale, Env), Env), true);
}

template<>
TRuntimeNode TProgramBuilder::NewDataLiteral<NUdf::EDataSlot::Date32>(const NUdf::TStringRef& data) const {
    return TRuntimeNode(BuildDataLiteral(data, NUdf::TDataType<NUdf::TDate32>::Id, Env), true);
}

template<>
TRuntimeNode TProgramBuilder::NewDataLiteral<NUdf::EDataSlot::Datetime64>(const NUdf::TStringRef& data) const {
    return TRuntimeNode(BuildDataLiteral(data, NUdf::TDataType<NUdf::TDatetime64>::Id, Env), true);
}

template<>
TRuntimeNode TProgramBuilder::NewDataLiteral<NUdf::EDataSlot::Timestamp64>(const NUdf::TStringRef& data) const {
    return TRuntimeNode(BuildDataLiteral(data, NUdf::TDataType<NUdf::TTimestamp64>::Id, Env), true);
}

template<>
TRuntimeNode TProgramBuilder::NewDataLiteral<NUdf::EDataSlot::Interval64>(const NUdf::TStringRef& data) const {
    return TRuntimeNode(BuildDataLiteral(data, NUdf::TDataType<NUdf::TInterval64>::Id, Env), true);
}

TRuntimeNode TProgramBuilder::NewOptional(TRuntimeNode data) {
    auto type = TOptionalType::Create(data.GetStaticType(), Env);
    return TRuntimeNode(TOptionalLiteral::Create(data, type, Env), true);
}

TRuntimeNode TProgramBuilder::NewOptional(TType* optionalType, TRuntimeNode data) {
    auto type = AS_TYPE(TOptionalType, optionalType);
    return TRuntimeNode(TOptionalLiteral::Create(data, type, Env), true);
}

TRuntimeNode TProgramBuilder::NewVoid() {
    return TRuntimeNode(Env.GetVoidLazy(), true);
}

TRuntimeNode TProgramBuilder::NewEmptyListOfVoid() {
    return TRuntimeNode(Env.GetListOfVoidLazy(), true);
}

TRuntimeNode TProgramBuilder::NewEmptyOptional(TType* optionalOrPgType) {
    MKQL_ENSURE(optionalOrPgType->IsOptional() || optionalOrPgType->IsPg(), "Expected optional or pg type");

    if (optionalOrPgType->IsOptional()) {
        return TRuntimeNode(TOptionalLiteral::Create(static_cast<TOptionalType*>(optionalOrPgType), Env), true);
    }

    return PgCast(NewNull(), optionalOrPgType);
}

TRuntimeNode TProgramBuilder::NewEmptyOptionalDataLiteral(NUdf::TDataTypeId schemeType) {
    return TRuntimeNode(BuildEmptyOptionalDataLiteral(schemeType, Env), true);
}

TRuntimeNode TProgramBuilder::NewEmptyStruct() {
    return TRuntimeNode(Env.GetEmptyStructLazy(), true);
}

TRuntimeNode TProgramBuilder::NewStruct(const TArrayRef<const std::pair<std::string_view, TRuntimeNode>>& members) {
    if (members.empty()) {
        return NewEmptyStruct();
    }

    TStructLiteralBuilder builder(Env);
    for (auto x : members) {
        builder.Add(x.first, x.second);
    }

    return TRuntimeNode(builder.Build(), true);
}

TRuntimeNode TProgramBuilder::NewStruct(TType* structType, const TArrayRef<const std::pair<std::string_view, TRuntimeNode>>& members) {
    const auto detailedStructType = AS_TYPE(TStructType, structType);
    MKQL_ENSURE(members.size() == detailedStructType->GetMembersCount(), "Mismatch count of members");
    if (members.empty()) {
        return NewEmptyStruct();
    }

    std::vector<TRuntimeNode> values(detailedStructType->GetMembersCount());
    for (ui32 i = 0; i < detailedStructType->GetMembersCount(); ++i) {
        const auto& name = members[i].first;
        ui32 index = detailedStructType->GetMemberIndex(name);
        MKQL_ENSURE(!values[index], "Duplicate of member: " << name);
        values[index] = members[i].second;
    }

    return TRuntimeNode(TStructLiteral::Create(values.size(), values.data(), detailedStructType, Env), true);
}

TRuntimeNode TProgramBuilder::NewEmptyList() {
    return TRuntimeNode(Env.GetEmptyListLazy(), true);
}

TRuntimeNode TProgramBuilder::NewEmptyList(TType* itemType) {
    TListLiteralBuilder builder(Env, itemType);
    return TRuntimeNode(builder.Build(), true);
}

TRuntimeNode TProgramBuilder::NewList(TType* itemType, const TArrayRef<const TRuntimeNode>& items) {
    TListLiteralBuilder builder(Env, itemType);
    for (auto item : items) {
        builder.Add(item);
    }

    return TRuntimeNode(builder.Build(), true);
}

TRuntimeNode TProgramBuilder::NewEmptyDict() {
    return TRuntimeNode(Env.GetEmptyDictLazy(), true);
}

TRuntimeNode TProgramBuilder::NewDict(TType* dictType, const TArrayRef<const std::pair<TRuntimeNode, TRuntimeNode>>& items) {
    MKQL_ENSURE(dictType->IsDict(), "Expected dict type");

    return TRuntimeNode(TDictLiteral::Create(items.size(), items.data(), static_cast<TDictType*>(dictType), Env), true);
}

TRuntimeNode TProgramBuilder::NewEmptyTuple() {
    return TRuntimeNode(Env.GetEmptyTupleLazy(), true);
}


TRuntimeNode TProgramBuilder::NewTuple(TType* tupleType, const TArrayRef<const TRuntimeNode>& elements) {
    MKQL_ENSURE(tupleType->IsTuple(), "Expected tuple type");

    return TRuntimeNode(TTupleLiteral::Create(elements.size(), elements.data(), static_cast<TTupleType*>(tupleType), Env), true);
}

TRuntimeNode TProgramBuilder::NewTuple(const TArrayRef<const TRuntimeNode>& elements) {
    std::vector<TType*> types;
    types.reserve(elements.size());
    for (auto elem : elements) {
        types.push_back(elem.GetStaticType());
    }

    return NewTuple(NewTupleType(types), elements);
}


TRuntimeNode TProgramBuilder::NewVariant(TRuntimeNode item, ui32 index, TType* variantType) {
    const auto type = AS_TYPE(TVariantType, variantType);
    MKQL_ENSURE(type->GetUnderlyingType()->IsTuple(), "Expected tuple as underlying type");
    return TRuntimeNode(TVariantLiteral::Create(item, index, type, Env), true);
}

TRuntimeNode TProgramBuilder::NewVariant(TRuntimeNode item, const std::string_view& member, TType* variantType) {
    const auto type = AS_TYPE(TVariantType, variantType);
    MKQL_ENSURE(type->GetUnderlyingType()->IsStruct(), "Expected struct as underlying type");
    ui32 index = AS_TYPE(TStructType, type->GetUnderlyingType())->GetMemberIndex(member);
    return TRuntimeNode(TVariantLiteral::Create(item, index, type, Env), true);
}

TRuntimeNode TProgramBuilder::Coalesce(TRuntimeNode data, TRuntimeNode defaultData) {
    bool isOptional = false;
    const auto dataType = UnpackOptional(data, isOptional);
    if (!isOptional && !data.GetStaticType()->IsPg()) {
        MKQL_ENSURE(data.GetStaticType()->IsSameType(*defaultData.GetStaticType()), "Mismatch operand types");
        return data;
    }

    if (!dataType->IsSameType(*defaultData.GetStaticType())) {
        bool isOptionalDefault;
        const auto defaultDataType = UnpackOptional(defaultData, isOptionalDefault);
        MKQL_ENSURE(dataType->IsSameType(*defaultDataType), "Mismatch operand types");
    }

    TCallableBuilder callableBuilder(Env, __func__, defaultData.GetStaticType());
    callableBuilder.Add(data);
    callableBuilder.Add(defaultData);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::Unwrap(TRuntimeNode optional, TRuntimeNode message, const std::string_view& file, ui32 row, ui32 column) {
    bool isOptional;
    auto underlyingType = UnpackOptional(optional, isOptional);
    MKQL_ENSURE(isOptional, "Expected optional");

    const auto& messageType = message.GetStaticType();
    MKQL_ENSURE(messageType->IsData(), "Expected data");

    const auto& messageTypeData = static_cast<const TDataType&>(*messageType);
    MKQL_ENSURE(messageTypeData.GetSchemeType() == NUdf::TDataType<char*>::Id || messageTypeData.GetSchemeType() == NUdf::TDataType<NUdf::TUtf8>::Id, "Expected string or utf8.");

    TCallableBuilder callableBuilder(Env, __func__, underlyingType);
    callableBuilder.Add(optional);
    callableBuilder.Add(message);
    callableBuilder.Add(NewDataLiteral<NUdf::EDataSlot::String>(file));
    callableBuilder.Add(NewDataLiteral(row));
    callableBuilder.Add(NewDataLiteral(column));
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::Increment(TRuntimeNode data) {
    const std::array<TRuntimeNode, 1> args = {{ data }};
    bool isOptional;
    const auto type = UnpackOptionalData(data, isOptional);

    if (type->GetSchemeType() != NUdf::TDataType<NUdf::TDecimal>::Id)
        return Invoke(__func__, data.GetStaticType(), args);

    return Invoke(TString("Inc_") += ::ToString(static_cast<TDataDecimalType*>(type)->GetParams().first), data.GetStaticType(), args);
}

TRuntimeNode TProgramBuilder::Decrement(TRuntimeNode data) {
    const std::array<TRuntimeNode, 1> args = {{ data }};
    bool isOptional;
    const auto type = UnpackOptionalData(data, isOptional);

    if (type->GetSchemeType() != NUdf::TDataType<NUdf::TDecimal>::Id)
        return Invoke(__func__, data.GetStaticType(), args);

    return Invoke(TString("Dec_") += ::ToString(static_cast<TDataDecimalType*>(type)->GetParams().first), data.GetStaticType(), args);
}

TRuntimeNode TProgramBuilder::Abs(TRuntimeNode data) {
    const std::array<TRuntimeNode, 1> args = {{ data }};
    return Invoke(__func__, data.GetStaticType(), args);
}

TRuntimeNode TProgramBuilder::Plus(TRuntimeNode data) {
    const std::array<TRuntimeNode, 1> args = {{ data }};
    return Invoke(__func__, data.GetStaticType(), args);
}

TRuntimeNode TProgramBuilder::Minus(TRuntimeNode data) {
    const std::array<TRuntimeNode, 1> args = {{ data }};
    return Invoke(__func__, data.GetStaticType(), args);
}

TRuntimeNode TProgramBuilder::Add(TRuntimeNode data1, TRuntimeNode data2) {
    const std::array<TRuntimeNode, 2> args = {{ data1, data2 }};

    bool isOptionalLeft;
    const auto leftType = UnpackOptionalData(data1, isOptionalLeft);

    if (leftType->GetSchemeType() != NUdf::TDataType<NUdf::TDecimal>::Id)
        return Invoke(__func__, BuildArithmeticCommonType(data1.GetStaticType(), data2.GetStaticType()), args);

    const auto decimalType = static_cast<TDataDecimalType*>(leftType);
    bool isOptionalRight;
    const auto rightType = static_cast<TDataDecimalType*>(UnpackOptionalData(data2, isOptionalRight));
    MKQL_ENSURE(rightType->IsSameType(*decimalType), "Operands type mismatch");
    const auto resultType = isOptionalLeft || isOptionalRight ? NewOptionalType(decimalType) : decimalType;
    return Invoke(TString("Add_") += ::ToString(decimalType->GetParams().first), resultType, args);
}

TRuntimeNode TProgramBuilder::Sub(TRuntimeNode data1, TRuntimeNode data2) {
    const std::array<TRuntimeNode, 2> args = {{ data1, data2 }};

    bool isOptionalLeft;
    const auto leftType = UnpackOptionalData(data1, isOptionalLeft);

    if (leftType->GetSchemeType() != NUdf::TDataType<NUdf::TDecimal>::Id)
        return Invoke(__func__, BuildArithmeticCommonType(data1.GetStaticType(), data2.GetStaticType()), args);

    const auto decimalType = static_cast<TDataDecimalType*>(leftType);
    bool isOptionalRight;
    const auto rightType = static_cast<TDataDecimalType*>(UnpackOptionalData(data2, isOptionalRight));
    MKQL_ENSURE(rightType->IsSameType(*decimalType), "Operands type mismatch");
    const auto resultType = isOptionalLeft || isOptionalRight ? NewOptionalType(decimalType) : decimalType;
    return Invoke(TString("Sub_") += ::ToString(decimalType->GetParams().first), resultType, args);
}

TRuntimeNode TProgramBuilder::Mul(TRuntimeNode data1, TRuntimeNode data2) {
    const std::array<TRuntimeNode, 2> args = {{ data1, data2 }};
    return Invoke(__func__, BuildArithmeticCommonType(data1.GetStaticType(), data2.GetStaticType()), args);
}

TRuntimeNode TProgramBuilder::Div(TRuntimeNode data1, TRuntimeNode data2) {
    const std::array<TRuntimeNode, 2> args = {{ data1, data2 }};
    auto resultType = BuildArithmeticCommonType(data1.GetStaticType(), data2.GetStaticType());
    if (resultType->IsData() && !(NUdf::GetDataTypeInfo(*static_cast<TDataType*>(resultType)->GetDataSlot()).Features & (NUdf::EDataTypeFeatures::FloatType | NUdf::EDataTypeFeatures::DecimalType))) {
        resultType = NewOptionalType(resultType);
    }
    return Invoke(__func__, resultType, args);
}

TRuntimeNode TProgramBuilder::DecimalDiv(TRuntimeNode data1, TRuntimeNode data2) {
    bool isOptionalLeft, isOptionalRight;
    const auto leftType = static_cast<TDataDecimalType*>(UnpackOptionalData(data1, isOptionalLeft));
    const auto rightType = UnpackOptionalData(data2, isOptionalRight);

    if (rightType->GetSchemeType() == NUdf::TDataType<NUdf::TDecimal>::Id)
        MKQL_ENSURE(static_cast<TDataDecimalType*>(rightType)->IsSameType(*leftType), "Operands type mismatch");
    else
        MKQL_ENSURE(NUdf::GetDataTypeInfo(*rightType->GetDataSlot()).Features & NUdf::IntegralType, "Operands type mismatch");

    const auto returnType = isOptionalLeft || isOptionalRight ? NewOptionalType(leftType) : leftType;

    TCallableBuilder callableBuilder(Env, __func__, returnType);
    callableBuilder.Add(data1);
    callableBuilder.Add(data2);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::DecimalMod(TRuntimeNode data1, TRuntimeNode data2) {
    bool isOptionalLeft, isOptionalRight;
    const auto leftType = static_cast<TDataDecimalType*>(UnpackOptionalData(data1, isOptionalLeft));
    const auto rightType = UnpackOptionalData(data2, isOptionalRight);

    if (rightType->GetSchemeType() == NUdf::TDataType<NUdf::TDecimal>::Id)
        MKQL_ENSURE(static_cast<TDataDecimalType*>(rightType)->IsSameType(*leftType), "Operands type mismatch");
    else
        MKQL_ENSURE(NUdf::GetDataTypeInfo(*rightType->GetDataSlot()).Features & NUdf::IntegralType, "Operands type mismatch");

    const auto returnType = isOptionalLeft || isOptionalRight ? NewOptionalType(leftType) : leftType;

    TCallableBuilder callableBuilder(Env, __func__, returnType);
    callableBuilder.Add(data1);
    callableBuilder.Add(data2);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::DecimalMul(TRuntimeNode data1, TRuntimeNode data2) {
    bool isOptionalLeft, isOptionalRight;
    const auto leftType = static_cast<TDataDecimalType*>(UnpackOptionalData(data1, isOptionalLeft));
    const auto rightType = UnpackOptionalData(data2, isOptionalRight);

    if (rightType->GetSchemeType() == NUdf::TDataType<NUdf::TDecimal>::Id)
        MKQL_ENSURE(static_cast<TDataDecimalType*>(rightType)->IsSameType(*leftType), "Operands type mismatch");
    else
        MKQL_ENSURE(NUdf::GetDataTypeInfo(*rightType->GetDataSlot()).Features & NUdf::IntegralType, "Operands type mismatch");

    const auto returnType = isOptionalLeft || isOptionalRight ? NewOptionalType(leftType) : leftType;

    TCallableBuilder callableBuilder(Env, __func__, returnType);
    callableBuilder.Add(data1);
    callableBuilder.Add(data2);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::AllOf(TRuntimeNode list, const TUnaryLambda& predicate) {
    return Not(NotAllOf(list, predicate));
}

TRuntimeNode TProgramBuilder::NotAllOf(TRuntimeNode list, const TUnaryLambda& predicate) {
    return Exists(ToOptional(SkipWhile(list, predicate)));
}

TRuntimeNode TProgramBuilder::BitNot(TRuntimeNode data) {
    const std::array<TRuntimeNode, 1> args = {{ data }};
    return Invoke(__func__, data.GetStaticType(), args);
}

TRuntimeNode TProgramBuilder::CountBits(TRuntimeNode data) {
    const std::array<TRuntimeNode, 1> args = {{ data }};
    return Invoke(__func__, data.GetStaticType(), args);
}

TRuntimeNode TProgramBuilder::BitAnd(TRuntimeNode data1, TRuntimeNode data2) {
    const std::array<TRuntimeNode, 2> args = {{ data1, data2 }};
    return Invoke(__func__, BuildArithmeticCommonType(data1.GetStaticType(), data2.GetStaticType()), args);
}

TRuntimeNode TProgramBuilder::BitOr(TRuntimeNode data1, TRuntimeNode data2) {
    const std::array<TRuntimeNode, 2> args = {{ data1, data2 }};
    return Invoke(__func__, BuildArithmeticCommonType(data1.GetStaticType(), data2.GetStaticType()), args);
}

TRuntimeNode TProgramBuilder::BitXor(TRuntimeNode data1, TRuntimeNode data2) {
    const std::array<TRuntimeNode, 2> args = {{ data1, data2 }};
    return Invoke(__func__, BuildArithmeticCommonType(data1.GetStaticType(), data2.GetStaticType()), args);
}

TRuntimeNode TProgramBuilder::ShiftLeft(TRuntimeNode arg, TRuntimeNode bits) {
    const std::array<TRuntimeNode, 2> args = {{ arg, bits }};
    return Invoke(__func__, arg.GetStaticType(), args);
}

TRuntimeNode TProgramBuilder::RotLeft(TRuntimeNode arg, TRuntimeNode bits) {
    const std::array<TRuntimeNode, 2> args = {{ arg, bits }};
    return Invoke(__func__, arg.GetStaticType(), args);
}

TRuntimeNode TProgramBuilder::ShiftRight(TRuntimeNode arg, TRuntimeNode bits) {
    const std::array<TRuntimeNode, 2> args = {{ arg, bits }};
    return Invoke(__func__, arg.GetStaticType(), args);
}

TRuntimeNode TProgramBuilder::RotRight(TRuntimeNode arg, TRuntimeNode bits) {
    const std::array<TRuntimeNode, 2> args = {{ arg, bits }};
    return Invoke(__func__, arg.GetStaticType(), args);
}

TRuntimeNode TProgramBuilder::Mod(TRuntimeNode data1, TRuntimeNode data2) {
    const std::array<TRuntimeNode, 2> args = {{ data1, data2 }};
    auto resultType = BuildArithmeticCommonType(data1.GetStaticType(), data2.GetStaticType());
    if (resultType->IsData() && !(NUdf::GetDataTypeInfo(*static_cast<TDataType*>(resultType)->GetDataSlot()).Features & (NUdf::EDataTypeFeatures::FloatType | NUdf::EDataTypeFeatures::DecimalType))) {
        resultType = NewOptionalType(resultType);
    }
    return Invoke(__func__, resultType, args);
}

TRuntimeNode TProgramBuilder::BuildMinMax(const std::string_view& callableName, const TRuntimeNode* data, size_t size) {
    switch (size) {
        case 0U: return NewNull();
        case 1U: return *data;
        case 2U: return InvokeBinary(callableName, ChooseCommonType(data[0U].GetStaticType(), data[1U].GetStaticType()), data[0U], data[1U]);
        default: break;
    }

    const auto half = size >> 1U;
    const std::array<TRuntimeNode, 2U> args = {{ BuildMinMax(callableName, data, half), BuildMinMax(callableName, data + half, size - half) }};
    return BuildMinMax(callableName, args.data(), args.size());
}

TRuntimeNode TProgramBuilder::BuildWideSkipTakeBlocks(const std::string_view& callableName, TRuntimeNode flow, TRuntimeNode count) {
    ValidateBlockFlowType(flow.GetStaticType());

    MKQL_ENSURE(count.GetStaticType()->IsData(), "Expected data");
    MKQL_ENSURE(static_cast<const TDataType&>(*count.GetStaticType()).GetSchemeType() == NUdf::TDataType<ui64>::Id, "Expected ui64");

    TCallableBuilder callableBuilder(Env, callableName, flow.GetStaticType());
    callableBuilder.Add(flow);
    callableBuilder.Add(count);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::BuildBlockLogical(const std::string_view& callableName, TRuntimeNode first, TRuntimeNode second) {
    auto firstType = AS_TYPE(TBlockType, first.GetStaticType());
    auto secondType = AS_TYPE(TBlockType, second.GetStaticType());

    bool isOpt1, isOpt2;
    MKQL_ENSURE(UnpackOptionalData(firstType->GetItemType(), isOpt1)->GetSchemeType() == NUdf::TDataType<bool>::Id, "Requires boolean args.");
    MKQL_ENSURE(UnpackOptionalData(secondType->GetItemType(), isOpt2)->GetSchemeType() == NUdf::TDataType<bool>::Id, "Requires boolean args.");

    const auto itemType = NewDataType(NUdf::TDataType<bool>::Id, isOpt1 || isOpt2);
    auto outputType = NewBlockType(itemType, GetResultShape({firstType, secondType}));

    TCallableBuilder callableBuilder(Env, callableName, outputType);
    callableBuilder.Add(first);
    callableBuilder.Add(second);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::Min(const TArrayRef<const TRuntimeNode>& args) {
    return BuildMinMax(__func__,  args.data(), args.size());
}

TRuntimeNode TProgramBuilder::Max(const TArrayRef<const TRuntimeNode>& args) {
    return BuildMinMax(__func__,  args.data(), args.size());
}

TRuntimeNode TProgramBuilder::Min(TRuntimeNode data1, TRuntimeNode data2) {
    const std::array<TRuntimeNode, 2U> args = {{ data1, data2 }};
    return Min(args);
}

TRuntimeNode TProgramBuilder::Max(TRuntimeNode data1, TRuntimeNode data2) {
    const std::array<TRuntimeNode, 2U> args = {{ data1, data2 }};
    return Max(args);
}

TRuntimeNode TProgramBuilder::Equals(TRuntimeNode data1, TRuntimeNode data2) {
    return DataCompare(__func__, data1, data2);
}

TRuntimeNode TProgramBuilder::NotEquals(TRuntimeNode data1, TRuntimeNode data2) {
    return DataCompare(__func__, data1, data2);
}

TRuntimeNode TProgramBuilder::Less(TRuntimeNode data1, TRuntimeNode data2) {
    return DataCompare(__func__, data1, data2);
}

TRuntimeNode TProgramBuilder::LessOrEqual(TRuntimeNode data1, TRuntimeNode data2) {
    return DataCompare(__func__, data1, data2);
}

TRuntimeNode TProgramBuilder::Greater(TRuntimeNode data1, TRuntimeNode data2) {
    return DataCompare(__func__, data1, data2);
}

TRuntimeNode TProgramBuilder::GreaterOrEqual(TRuntimeNode data1, TRuntimeNode data2) {
    return DataCompare(__func__, data1, data2);
}

TRuntimeNode TProgramBuilder::InvokeBinary(const std::string_view& callableName, TType* type, TRuntimeNode data1, TRuntimeNode data2) {
    const std::array<TRuntimeNode, 2> args = {{ data1, data2 }};
    return Invoke(callableName, type, args);
}

TRuntimeNode TProgramBuilder::AggrCompare(const std::string_view& callableName, TRuntimeNode data1, TRuntimeNode data2) {
    return InvokeBinary(callableName, NewDataType(NUdf::TDataType<bool>::Id), data1, data2);
}

TRuntimeNode TProgramBuilder::DataCompare(const std::string_view& callableName, TRuntimeNode left, TRuntimeNode right) {
    bool isOptionalLeft, isOptionalRight;
    const auto leftType = UnpackOptionalData(left, isOptionalLeft);
    const auto rightType = UnpackOptionalData(right, isOptionalRight);

    const auto lId = leftType->GetSchemeType();
    const auto rId = rightType->GetSchemeType();

    if (lId == NUdf::TDataType<NUdf::TDecimal>::Id && rId == NUdf::TDataType<NUdf::TDecimal>::Id) {
        const auto& lDec = static_cast<TDataDecimalType*>(leftType)->GetParams();
        const auto& rDec = static_cast<TDataDecimalType*>(rightType)->GetParams();
        if (lDec.second < rDec.second) {
            left = ToDecimal(left, std::min<ui8>(lDec.first + rDec.second - lDec.second, NYql::NDecimal::MaxPrecision), rDec.second);
        } else if (lDec.second > rDec.second) {
            right = ToDecimal(right, std::min<ui8>(rDec.first + lDec.second - rDec.second, NYql::NDecimal::MaxPrecision), lDec.second);
        }
    } else if (lId == NUdf::TDataType<NUdf::TDecimal>::Id && NUdf::GetDataTypeInfo(NUdf::GetDataSlot(rId)).Features & NUdf::EDataTypeFeatures::IntegralType) {
        const auto scale = static_cast<TDataDecimalType*>(leftType)->GetParams().second;
        right = ToDecimal(right, std::min<ui8>(NYql::NDecimal::MaxPrecision, NUdf::GetDataTypeInfo(NUdf::GetDataSlot(rId)).DecimalDigits + scale), scale);
    } else if (rId == NUdf::TDataType<NUdf::TDecimal>::Id && NUdf::GetDataTypeInfo(NUdf::GetDataSlot(lId)).Features & NUdf::EDataTypeFeatures::IntegralType) {
        const auto scale = static_cast<TDataDecimalType*>(rightType)->GetParams().second;
        left = ToDecimal(left, std::min<ui8>(NYql::NDecimal::MaxPrecision, NUdf::GetDataTypeInfo(NUdf::GetDataSlot(lId)).DecimalDigits + scale), scale);
    }

    const std::array<TRuntimeNode, 2> args = {{ left, right }};
    const auto resultType = isOptionalLeft || isOptionalRight ? NewOptionalType(NewDataType(NUdf::TDataType<bool>::Id)) : NewDataType(NUdf::TDataType<bool>::Id);
    return Invoke(callableName, resultType, args);
}

TRuntimeNode TProgramBuilder::BuildRangeLogical(const std::string_view& callableName, const TArrayRef<const TRuntimeNode>& lists) {
    MKQL_ENSURE(!lists.empty(), "Expecting at least one argument");

    for (auto& list : lists) {
        MKQL_ENSURE(list.GetStaticType()->IsList(), "Expecting lists");
        MKQL_ENSURE(list.GetStaticType()->IsSameType(*lists.front().GetStaticType()), "Expecting arguments of same type");
    }

    TCallableBuilder callableBuilder(Env, callableName, lists.front().GetStaticType());
    for (auto& list : lists) {
        callableBuilder.Add(list);
    }

    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::AggrEquals(TRuntimeNode data1, TRuntimeNode data2) {
    return AggrCompare(__func__, data1, data2);
}

TRuntimeNode TProgramBuilder::AggrNotEquals(TRuntimeNode data1, TRuntimeNode data2) {
    return AggrCompare(__func__, data1, data2);
}

TRuntimeNode TProgramBuilder::AggrLess(TRuntimeNode data1, TRuntimeNode data2) {
    return AggrCompare(__func__, data1, data2);
}

TRuntimeNode TProgramBuilder::AggrLessOrEqual(TRuntimeNode data1, TRuntimeNode data2) {
    return AggrCompare(__func__, data1, data2);
}

TRuntimeNode TProgramBuilder::AggrGreater(TRuntimeNode data1, TRuntimeNode data2) {
    return AggrCompare(__func__, data1, data2);
}

TRuntimeNode TProgramBuilder::AggrGreaterOrEqual(TRuntimeNode data1, TRuntimeNode data2) {
    return AggrCompare(__func__, data1, data2);
}

TRuntimeNode TProgramBuilder::If(TRuntimeNode condition, TRuntimeNode thenBranch, TRuntimeNode elseBranch) {
    bool condOpt, thenOpt, elseOpt;
    const auto conditionType = UnpackOptionalData(condition, condOpt);
    MKQL_ENSURE(conditionType->GetSchemeType() == NUdf::TDataType<bool>::Id, "Expected bool");

    const auto thenUnpacked = UnpackOptional(thenBranch, thenOpt);
    const auto elseUnpacked = UnpackOptional(elseBranch, elseOpt);
    MKQL_ENSURE(thenUnpacked->IsSameType(*elseUnpacked), "Different return types in branches.");

    const bool isOptional = condOpt || thenOpt || elseOpt;

    TCallableBuilder callableBuilder(Env, __func__, isOptional ? NewOptionalType(thenUnpacked) : thenUnpacked);
    callableBuilder.Add(condition);
    callableBuilder.Add(thenBranch);
    callableBuilder.Add(elseBranch);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::If(const TArrayRef<const TRuntimeNode>& args) {
    MKQL_ENSURE(args.size() % 2U, "Expected odd arguments.");
    MKQL_ENSURE(args.size() >= 3U, "Expected at least three arguments.");
    return If(args.front(), args[1U], 3U == args.size() ? args.back() : If(args.last(args.size() - 2U)));
}

TRuntimeNode TProgramBuilder::If(TRuntimeNode condition, TRuntimeNode thenBranch, TRuntimeNode elseBranch, TType* resultType) {
    bool condOpt;
    const auto conditionType = UnpackOptionalData(condition, condOpt);
    MKQL_ENSURE(conditionType->GetSchemeType() == NUdf::TDataType<bool>::Id, "Expected bool");

    TCallableBuilder callableBuilder(Env, __func__, resultType);
    callableBuilder.Add(condition);
    callableBuilder.Add(thenBranch);
    callableBuilder.Add(elseBranch);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::Ensure(TRuntimeNode value, TRuntimeNode predicate, TRuntimeNode message, const std::string_view& file, ui32 row, ui32 column) {
    bool isOptional;
    const auto unpackedType = UnpackOptionalData(predicate, isOptional);
    MKQL_ENSURE(unpackedType->GetSchemeType() == NUdf::TDataType<bool>::Id, "Expected bool");

    const auto& messageType = message.GetStaticType();
    MKQL_ENSURE(messageType->IsData(), "Expected data");

    const auto& messageTypeData = static_cast<const TDataType&>(*messageType);
    MKQL_ENSURE(messageTypeData.GetSchemeType() == NUdf::TDataType<char*>::Id || messageTypeData.GetSchemeType() == NUdf::TDataType<NUdf::TUtf8>::Id, "Expected string or utf8.");

    TCallableBuilder callableBuilder(Env, __func__, value.GetStaticType());
    callableBuilder.Add(value);
    callableBuilder.Add(predicate);
    callableBuilder.Add(message);
    callableBuilder.Add(NewDataLiteral<NUdf::EDataSlot::String>(file));
    callableBuilder.Add(NewDataLiteral(row));
    callableBuilder.Add(NewDataLiteral(column));
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::SourceOf(TType* returnType) {
    MKQL_ENSURE(returnType->IsFlow() || returnType->IsStream(), "Expected flow or stream.");
    TCallableBuilder callableBuilder(Env, __func__, returnType);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::Source() {
    if constexpr (RuntimeVersion < 18U) {
        THROW yexception() << "Runtime version (" << RuntimeVersion << ") too old for " << __func__;
    }

    TCallableBuilder callableBuilder(Env, __func__, NewFlowType(NewMultiType({})));
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::IfPresent(TRuntimeNode optional, const TUnaryLambda& thenBranch, TRuntimeNode elseBranch) {
    bool isOptional;
    const auto unpackedType = UnpackOptional(optional, isOptional);
    if (!isOptional) {
        return thenBranch(optional);
    }

    const auto itemArg = Arg(unpackedType);
    const auto then = thenBranch(itemArg);
    bool thenOpt, elseOpt;
    const auto thenUnpacked = UnpackOptional(then, thenOpt);
    const auto elseUnpacked = UnpackOptional(elseBranch, elseOpt);

    MKQL_ENSURE(thenUnpacked->IsSameType(*elseUnpacked), "Different return types in branches.");

    TCallableBuilder callableBuilder(Env, __func__, (thenOpt || elseOpt) ? NewOptionalType(thenUnpacked) : thenUnpacked);
    callableBuilder.Add(optional);
    callableBuilder.Add(itemArg);
    callableBuilder.Add(then);
    callableBuilder.Add(elseBranch);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::IfPresent(TRuntimeNode::TList optionals, const TNarrowLambda& thenBranch, TRuntimeNode elseBranch) {
    switch (optionals.size()) {
        case 0U:
            return thenBranch({});
        case 1U:
            return IfPresent(optionals.front(), [&](TRuntimeNode unwrap){ return thenBranch({unwrap}); }, elseBranch);
        default:
            break;

    }

    const auto first = optionals.front();
    optionals.erase(optionals.cbegin());
    return IfPresent(first,
        [&](TRuntimeNode head) {
            return IfPresent(optionals,
                [&](TRuntimeNode::TList tail) {
                    tail.insert(tail.cbegin(), head);
                    return thenBranch(tail);
                },
                elseBranch
            );
        },
        elseBranch
    );
}

TRuntimeNode TProgramBuilder::Not(TRuntimeNode data) {
    return UnaryDataFunction(data, __func__, TDataFunctionFlags::CommonOptionalResult | TDataFunctionFlags::RequiresBooleanArgs | TDataFunctionFlags::AllowOptionalArgs);
}

TRuntimeNode TProgramBuilder::BuildBinaryLogical(const std::string_view& callableName, TRuntimeNode data1, TRuntimeNode data2) {
    bool isOpt1, isOpt2;
    MKQL_ENSURE(UnpackOptionalData(data1, isOpt1)->GetSchemeType() == NUdf::TDataType<bool>::Id, "Requires boolean args.");
    MKQL_ENSURE(UnpackOptionalData(data2, isOpt2)->GetSchemeType() == NUdf::TDataType<bool>::Id, "Requires boolean args.");
    const auto resultType = NewDataType(NUdf::TDataType<bool>::Id, isOpt1 || isOpt2);
    TCallableBuilder callableBuilder(Env, callableName, resultType);
    callableBuilder.Add(data1);
    callableBuilder.Add(data2);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::BuildLogical(const std::string_view& callableName, const TArrayRef<const TRuntimeNode>& args) {
    MKQL_ENSURE(!args.empty(), "Empty logical args.");

    switch (args.size()) {
        case 1U: return args.front();
        case 2U: return BuildBinaryLogical(callableName, args.front(), args.back());
    }

    const auto half = (args.size() + 1U) >> 1U;
    const TArrayRef<const TRuntimeNode> one(args.data(), half), two(args.data() + half, args.size() - half);
    return BuildBinaryLogical(callableName, BuildLogical(callableName, one), BuildLogical(callableName, two));
}

TRuntimeNode TProgramBuilder::And(const TArrayRef<const TRuntimeNode>& args) {
    return BuildLogical(__func__, args);
}

TRuntimeNode TProgramBuilder::Or(const TArrayRef<const TRuntimeNode>& args) {
    return BuildLogical(__func__, args);
}

TRuntimeNode TProgramBuilder::Xor(const TArrayRef<const TRuntimeNode>& args) {
    return BuildLogical(__func__, args);
}

TRuntimeNode TProgramBuilder::Exists(TRuntimeNode data) {
    const auto& nodeType = data.GetStaticType();
    if (nodeType->IsVoid()) {
        return NewDataLiteral(false);
    }

    if (!nodeType->IsOptional() && !nodeType->IsPg()) {
        return NewDataLiteral(true);
    }

    TCallableBuilder callableBuilder(Env, __func__, NewDataType(NUdf::TDataType<bool>::Id));
    callableBuilder.Add(data);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::NewMTRand(TRuntimeNode seed) {
    auto seedData = AS_TYPE(TDataType, seed);
    MKQL_ENSURE(seedData->GetSchemeType() == NUdf::TDataType<ui64>::Id, "seed must be ui64");
    TCallableBuilder callableBuilder(Env, __func__, NewResourceType(RandomMTResource), true);
    callableBuilder.Add(seed);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::NextMTRand(TRuntimeNode rand) {
    auto resType = AS_TYPE(TResourceType, rand);
    MKQL_ENSURE(resType->GetTag() == RandomMTResource, "Expected MTRand resource");

    const std::array<TType*, 2U> tupleTypes = {{ NewDataType(NUdf::TDataType<ui64>::Id), rand.GetStaticType() }};
    auto returnType = NewTupleType(tupleTypes);

    TCallableBuilder callableBuilder(Env, __func__, returnType);
    callableBuilder.Add(rand);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::AggrCountInit(TRuntimeNode value) {
    TCallableBuilder callableBuilder(Env, __func__, NewDataType(NUdf::TDataType<ui64>::Id));
    callableBuilder.Add(value);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::AggrCountUpdate(TRuntimeNode value, TRuntimeNode state) {
    MKQL_ENSURE(AS_TYPE(TDataType, state)->GetSchemeType() == NUdf::TDataType<ui64>::Id, "Expected ui64 type");
    TCallableBuilder callableBuilder(Env, __func__, NewDataType(NUdf::TDataType<ui64>::Id));
    callableBuilder.Add(value);
    callableBuilder.Add(state);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::AggrMin(TRuntimeNode data1, TRuntimeNode data2) {
    const auto type = data1.GetStaticType();
    MKQL_ENSURE(type->IsSameType(*data2.GetStaticType()), "Must be same type.");
    return InvokeBinary(__func__, type, data1, data2);
}

TRuntimeNode TProgramBuilder::AggrMax(TRuntimeNode data1, TRuntimeNode data2) {
    const auto type = data1.GetStaticType();
    MKQL_ENSURE(type->IsSameType(*data2.GetStaticType()), "Must be same type.");
    return InvokeBinary(__func__, type, data1, data2);
}

TRuntimeNode TProgramBuilder::AggrAdd(TRuntimeNode data1, TRuntimeNode data2) {
    const std::array<TRuntimeNode, 2> args = {{ data1, data2 }};
    bool isOptionalLeft;
    const auto leftType = UnpackOptionalData(data1, isOptionalLeft);

    if (leftType->GetSchemeType() !=   NUdf::TDataType<NUdf::TDecimal>::Id)
        return Invoke(__func__, data1.GetStaticType(), args);

    const auto decimalType = static_cast<TDataDecimalType*>(leftType);
    bool isOptionalRight;
    const auto rightType = static_cast<TDataDecimalType*>(UnpackOptionalData(data2, isOptionalRight));
    MKQL_ENSURE(rightType->IsSameType(*decimalType), "Operands type mismatch");
    return Invoke(TString("AggrAdd_") += ::ToString(decimalType->GetParams().first), data1.GetStaticType(), args);
}

TRuntimeNode TProgramBuilder::QueueCreate(TRuntimeNode initCapacity, TRuntimeNode initSize, const TArrayRef<const TRuntimeNode>& dependentNodes, TType* returnType) {
    auto resType = AS_TYPE(TResourceType, returnType);
    const auto tag = resType->GetTag();

    if (initCapacity.GetStaticType()->IsVoid()) {
        MKQL_ENSURE(RuntimeVersion >= 13, "Unbounded queue is not supported in runtime version " << RuntimeVersion);
    } else {
        auto initCapacityType = AS_TYPE(TDataType, initCapacity);
        MKQL_ENSURE(initCapacityType->GetSchemeType() == NUdf::TDataType<ui64>::Id, "init capcity must be ui64");
    }

    auto initSizeType = AS_TYPE(TDataType, initSize);
    MKQL_ENSURE(initSizeType->GetSchemeType() == NUdf::TDataType<ui64>::Id, "init size must be ui64");

    TCallableBuilder callableBuilder(Env, __func__, returnType, true);
    callableBuilder.Add(NewDataLiteral<NUdf::EDataSlot::String>(tag));
    callableBuilder.Add(initCapacity);
    callableBuilder.Add(initSize);
    for (auto node : dependentNodes) {
        callableBuilder.Add(node);
    }
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::QueuePush(TRuntimeNode resource, TRuntimeNode value) {
    auto resType = AS_TYPE(TResourceType, resource);
    const auto tag = resType->GetTag();
    MKQL_ENSURE(tag.StartsWith(ResourceQueuePrefix), "Expected Queue resource");
    TCallableBuilder callableBuilder(Env, __func__, resource.GetStaticType());
    callableBuilder.Add(resource);
    callableBuilder.Add(value);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::QueuePop(TRuntimeNode resource) {
    auto resType = AS_TYPE(TResourceType, resource);
    const auto tag = resType->GetTag();
    MKQL_ENSURE(tag.StartsWith(ResourceQueuePrefix), "Expected Queue resource");
    TCallableBuilder callableBuilder(Env, __func__, resource.GetStaticType());
    callableBuilder.Add(resource);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::QueuePeek(TRuntimeNode resource, TRuntimeNode index, const TArrayRef<const TRuntimeNode>& dependentNodes, TType* returnType) {
    MKQL_ENSURE(returnType->IsOptional(), "Expected optional type as result of QueuePeek");
    auto resType = AS_TYPE(TResourceType, resource);
    auto indexType = AS_TYPE(TDataType, index);
    MKQL_ENSURE(indexType->GetSchemeType() == NUdf::TDataType<ui64>::Id, "index size must be ui64");
    const auto tag = resType->GetTag();
    MKQL_ENSURE(tag.StartsWith(ResourceQueuePrefix), "Expected Queue resource");
    TCallableBuilder callableBuilder(Env, __func__, returnType);
    callableBuilder.Add(resource);
    callableBuilder.Add(index);
    for (auto node : dependentNodes) {
        callableBuilder.Add(node);
    }
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::QueueRange(TRuntimeNode resource, TRuntimeNode begin, TRuntimeNode end, const TArrayRef<const TRuntimeNode>& dependentNodes, TType* returnType) {
    MKQL_ENSURE(RuntimeVersion >= 14, "QueueRange is not supported in runtime version " << RuntimeVersion);

    MKQL_ENSURE(returnType->IsList(), "Expected list type as result of QueueRange");
    auto resType = AS_TYPE(TResourceType, resource);

    auto beginType = AS_TYPE(TDataType, begin);
    MKQL_ENSURE(beginType->GetSchemeType() == NUdf::TDataType<ui64>::Id, "begin index must be ui64");

    auto endType = AS_TYPE(TDataType, end);
    MKQL_ENSURE(endType->GetSchemeType() == NUdf::TDataType<ui64>::Id, "end index must be ui64");

    const auto tag = resType->GetTag();
    MKQL_ENSURE(tag.StartsWith(ResourceQueuePrefix), "Expected Queue resource");
    TCallableBuilder callableBuilder(Env, __func__, returnType);
    callableBuilder.Add(resource);
    callableBuilder.Add(begin);
    callableBuilder.Add(end);
    for (auto node : dependentNodes) {
        callableBuilder.Add(node);
    }
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::PreserveStream(TRuntimeNode stream, TRuntimeNode queue, TRuntimeNode outpace) {
    auto streamType = AS_TYPE(TStreamType, stream);
    auto resType = AS_TYPE(TResourceType, queue);
    auto outpaceType = AS_TYPE(TDataType, outpace);
    MKQL_ENSURE(outpaceType->GetSchemeType() == NUdf::TDataType<ui64>::Id, "PreserveStream: outpace size must be ui64");
    const auto tag = resType->GetTag();
    MKQL_ENSURE(tag.StartsWith(ResourceQueuePrefix), "PreserveStream: Expected Queue resource");
    TCallableBuilder callableBuilder(Env, __func__, streamType);
    callableBuilder.Add(stream);
    callableBuilder.Add(queue);
    callableBuilder.Add(outpace);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::Seq(const TArrayRef<const TRuntimeNode>& args, TType* returnType) {
    MKQL_ENSURE(RuntimeVersion >= 15, "Seq is not supported in runtime version " << RuntimeVersion);

    TCallableBuilder callableBuilder(Env, __func__, returnType);
    for (auto node : args) {
        callableBuilder.Add(node);
    }
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::FromYsonSimpleType(TRuntimeNode input, NUdf::TDataTypeId schemeType) {
    auto type = input.GetStaticType();
    if (type->IsOptional()) {
        type = static_cast<const TOptionalType&>(*type).GetItemType();
    }
    MKQL_ENSURE(type->IsData(), "Expected data type");
    auto resDataType = NewDataType(schemeType);
    auto resultType = NewOptionalType(resDataType);
    TCallableBuilder callableBuilder(Env, __func__, resultType);
    callableBuilder.Add(input);
    callableBuilder.Add(NewDataLiteral(static_cast<ui32>(schemeType)));
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::TryWeakMemberFromDict(TRuntimeNode other, TRuntimeNode rest, NUdf::TDataTypeId schemeType, const std::string_view& memberName) {
    auto resDataType = NewDataType(schemeType);
    auto resultType = NewOptionalType(resDataType);

    TCallableBuilder callableBuilder(Env, __func__, resultType);
    callableBuilder.Add(other);
    callableBuilder.Add(rest);
    callableBuilder.Add(NewDataLiteral(static_cast<ui32>(schemeType)));
    callableBuilder.Add(NewDataLiteral<NUdf::EDataSlot::String>(memberName));
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::TimezoneId(TRuntimeNode name) {
    bool isOptional;
    auto dataType = UnpackOptionalData(name, isOptional);
    MKQL_ENSURE(dataType->GetSchemeType() == NUdf::TDataType<char*>::Id, "Expected string");
    auto resultType = NewOptionalType(NewDataType(NUdf::EDataSlot::Uint16));
    TCallableBuilder callableBuilder(Env, __func__, resultType);
    callableBuilder.Add(name);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::TimezoneName(TRuntimeNode id) {
    bool isOptional;
    auto dataType = UnpackOptionalData(id, isOptional);
    MKQL_ENSURE(dataType->GetSchemeType() == NUdf::TDataType<ui16>::Id, "Expected ui32");
    auto resultType = NewOptionalType(NewDataType(NUdf::EDataSlot::String));
    TCallableBuilder callableBuilder(Env, __func__, resultType);
    callableBuilder.Add(id);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::AddTimezone(TRuntimeNode utc, TRuntimeNode id) {
    bool isOptional1;
    auto dataType1 = UnpackOptionalData(utc, isOptional1);
    MKQL_ENSURE(NUdf::GetDataTypeInfo(*dataType1->GetDataSlot()).Features & NUdf::DateType, "Expected date type");

    bool isOptional2;
    auto dataType2 = UnpackOptionalData(id, isOptional2);
    MKQL_ENSURE(dataType2->GetSchemeType() == NUdf::TDataType<ui16>::Id, "Expected ui16");
    NUdf::EDataSlot tzType;
    switch (*dataType1->GetDataSlot()) {
    case NUdf::EDataSlot::Date: tzType = NUdf::EDataSlot::TzDate; break;
    case NUdf::EDataSlot::Datetime: tzType = NUdf::EDataSlot::TzDatetime; break;
    case NUdf::EDataSlot::Timestamp: tzType = NUdf::EDataSlot::TzTimestamp; break;
    case NUdf::EDataSlot::Date32: tzType = NUdf::EDataSlot::TzDate32; break;
    case NUdf::EDataSlot::Datetime64: tzType = NUdf::EDataSlot::TzDatetime64; break;
    case NUdf::EDataSlot::Timestamp64: tzType = NUdf::EDataSlot::TzTimestamp64; break;
    default:
        ythrow yexception() << "Unknown date type: " << *dataType1->GetDataSlot();
    }

    auto resultType = NewOptionalType(NewDataType(tzType));
    TCallableBuilder callableBuilder(Env, __func__, resultType);
    callableBuilder.Add(utc);
    callableBuilder.Add(id);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::RemoveTimezone(TRuntimeNode local) {
    bool isOptional1;
    const auto dataType1 = UnpackOptionalData(local, isOptional1);
    MKQL_ENSURE((NUdf::GetDataTypeInfo(*dataType1->GetDataSlot()).Features & NUdf::TzDateType), "Expected date with timezone type");

    NUdf::EDataSlot type;
    switch (*dataType1->GetDataSlot()) {
    case NUdf::EDataSlot::TzDate: type = NUdf::EDataSlot::Date; break;
    case NUdf::EDataSlot::TzDatetime: type = NUdf::EDataSlot::Datetime; break;
    case NUdf::EDataSlot::TzTimestamp: type = NUdf::EDataSlot::Timestamp; break;
    case NUdf::EDataSlot::TzDate32: type = NUdf::EDataSlot::Date32; break;
    case NUdf::EDataSlot::TzDatetime64: type = NUdf::EDataSlot::Datetime64; break;
    case NUdf::EDataSlot::TzTimestamp64: type = NUdf::EDataSlot::Timestamp64; break;
    default:
        ythrow yexception() << "Unknown date with timezone type: " << *dataType1->GetDataSlot();
    }

    return Convert(local, NewDataType(type, isOptional1));
}

TRuntimeNode TProgramBuilder::Nth(TRuntimeNode tuple, ui32 index) {
    bool isOptional;
    const auto type = AS_TYPE(TTupleType, UnpackOptional(tuple.GetStaticType(), isOptional));

    MKQL_ENSURE(index < type->GetElementsCount(), "Index out of range: " << index <<
        " is not less than " << type->GetElementsCount());
    auto itemType = type->GetElementType(index);
    if (isOptional && !itemType->IsOptional() && !itemType->IsNull() && !itemType->IsPg()) {
        itemType = TOptionalType::Create(itemType, Env);
    }

    TCallableBuilder callableBuilder(Env, __func__, itemType);
    callableBuilder.Add(tuple);
    callableBuilder.Add(NewDataLiteral<ui32>(index));
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::Element(TRuntimeNode tuple, ui32 index) {
    return Nth(tuple, index);
}

TRuntimeNode TProgramBuilder::Guess(TRuntimeNode variant, ui32 tupleIndex) {
    bool isOptional;
    auto unpacked = UnpackOptional(variant, isOptional);
    auto type = AS_TYPE(TVariantType, unpacked);
    auto underlyingType = AS_TYPE(TTupleType, type->GetUnderlyingType());
    MKQL_ENSURE(tupleIndex < underlyingType->GetElementsCount(), "Wrong tuple index");
    auto resType = TOptionalType::Create(underlyingType->GetElementType(tupleIndex), Env);

    TCallableBuilder callableBuilder(Env, __func__, resType);
    callableBuilder.Add(variant);
    callableBuilder.Add(NewDataLiteral<ui32>(tupleIndex));
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::Guess(TRuntimeNode variant, const std::string_view& memberName) {
    bool isOptional;
    auto unpacked = UnpackOptional(variant, isOptional);
    auto type = AS_TYPE(TVariantType, unpacked);
    auto underlyingType = AS_TYPE(TStructType, type->GetUnderlyingType());
    auto structIndex = underlyingType->GetMemberIndex(memberName);
    auto resType = TOptionalType::Create(underlyingType->GetMemberType(structIndex), Env);

    TCallableBuilder callableBuilder(Env, __func__, resType);
    callableBuilder.Add(variant);
    callableBuilder.Add(NewDataLiteral<ui32>(structIndex));
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::Way(TRuntimeNode variant) {
    bool isOptional;
    auto unpacked = UnpackOptional(variant, isOptional);
    auto type = AS_TYPE(TVariantType, unpacked);
    auto underlyingType = type->GetUnderlyingType();
    auto dataType = NewDataType(underlyingType->IsTuple() ? NUdf::EDataSlot::Uint32 : NUdf::EDataSlot::Utf8);
    auto resType = isOptional ? TOptionalType::Create(dataType, Env) : dataType;

    TCallableBuilder callableBuilder(Env, __func__, resType);
    callableBuilder.Add(variant);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::VariantItem(TRuntimeNode variant) {
    bool isOptional;
    auto unpacked = UnpackOptional(variant, isOptional);
    auto type = AS_TYPE(TVariantType, unpacked);
    auto underlyingType = type->GetAlternativeType(0);
    auto resType = isOptional ? TOptionalType::Create(underlyingType, Env) : underlyingType;

    TCallableBuilder callableBuilder(Env, __func__, resType);
    callableBuilder.Add(variant);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::VisitAll(TRuntimeNode variant, std::function<TRuntimeNode(ui32, TRuntimeNode)> handler) {
    const auto type = AS_TYPE(TVariantType, variant);
    std::vector<TRuntimeNode> items;
    std::vector<TRuntimeNode> newItems;

    for (ui32 i = 0; i < type->GetAlternativesCount(); ++i) {
        const auto itemType = type->GetAlternativeType(i);
        const auto itemArg = Arg(itemType);

        const auto res = handler(i, itemArg);
        items.emplace_back(itemArg);
        newItems.emplace_back(res);
    }

    bool hasOptional;
    const auto firstUnpacked = UnpackOptional(newItems.front(), hasOptional);
    bool allOptional = hasOptional;

    for (size_t i = 1U; i < newItems.size(); ++i) {
        bool isOptional;
        const auto unpacked = UnpackOptional(newItems[i].GetStaticType(), isOptional);
        MKQL_ENSURE(unpacked->IsSameType(*firstUnpacked), "Different return types in branches.");
        hasOptional = hasOptional || isOptional;
        allOptional = allOptional && isOptional;
    }

    if (hasOptional && !allOptional) {
        for (auto& item : newItems) {
            if (!item.GetStaticType()->IsOptional()) {
                item = NewOptional(item);
            }
        }
    }

    TCallableBuilder callableBuilder(Env, __func__, newItems.front().GetStaticType());
    callableBuilder.Add(variant);
    for (ui32 i = 0; i < type->GetAlternativesCount(); ++i) {
        callableBuilder.Add(items[i]);
        callableBuilder.Add(newItems[i]);
    }

    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::UnaryDataFunction(TRuntimeNode data, const std::string_view& callableName, ui32 flags) {
    bool isOptional;
    auto type = UnpackOptionalData(data, isOptional);
    if (!(flags & TDataFunctionFlags::AllowOptionalArgs)) {
        MKQL_ENSURE(!isOptional, "Optional data is not allowed");
    }

    auto schemeType = type->GetSchemeType();
    if (flags & TDataFunctionFlags::RequiresBooleanArgs) {
        MKQL_ENSURE(schemeType == NUdf::TDataType<bool>::Id, "Boolean data is required");
    } else if (flags & TDataFunctionFlags::RequiresStringArgs) {
        MKQL_ENSURE(schemeType == NUdf::TDataType<char*>::Id, "String data is required");
    }

    if (!schemeType) {
        MKQL_ENSURE((flags & TDataFunctionFlags::AllowNull) != 0, "Null is not allowed");
    }

    TType* resultType;
    if (flags & TDataFunctionFlags::HasBooleanResult) {
        resultType = TDataType::Create(NUdf::TDataType<bool>::Id, Env);
    } else if (flags & TDataFunctionFlags::HasUi32Result) {
        resultType = TDataType::Create(NUdf::TDataType<ui32>::Id, Env);
    } else if (flags & TDataFunctionFlags::HasStringResult) {
        resultType = TDataType::Create(NUdf::TDataType<char*>::Id, Env);
    } else if (flags & TDataFunctionFlags::HasOptionalResult) {
        resultType = TOptionalType::Create(type, Env);
    } else {
        resultType = type;
    }

    if ((flags & TDataFunctionFlags::CommonOptionalResult) && isOptional) {
        resultType = TOptionalType::Create(resultType, Env);
    }

    TCallableBuilder callableBuilder(Env, callableName, resultType);
    callableBuilder.Add(data);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::ToDict(TRuntimeNode list, bool multi, const TUnaryLambda& keySelector,
    const TUnaryLambda& payloadSelector, std::string_view callableName, bool isCompact, ui64 itemsCountHint)
{
    bool isOptional;
    const auto type = UnpackOptional(list, isOptional);
    MKQL_ENSURE(type->IsList(), "Expected list.");

    if (isOptional) {
        return Map(list, [&](TRuntimeNode unpacked) { return ToDict(unpacked, multi, keySelector, payloadSelector, callableName, isCompact, itemsCountHint); } );
    }

    const auto itemType = AS_TYPE(TListType, type)->GetItemType();
    ThrowIfListOfVoid(itemType);
    const auto itemArg = Arg(itemType);

    const auto key = keySelector(itemArg);
    const auto keyType = key.GetStaticType();

    auto payload = payloadSelector(itemArg);
    auto payloadType = payload.GetStaticType();
    if (multi) {
        payloadType = TListType::Create(payloadType, Env);
    }

    auto dictType = TDictType::Create(keyType, payloadType, Env);
    TCallableBuilder callableBuilder(Env, callableName, dictType);
    callableBuilder.Add(list);
    callableBuilder.Add(itemArg);
    callableBuilder.Add(key);
    callableBuilder.Add(payload);
    callableBuilder.Add(NewDataLiteral(multi));
    callableBuilder.Add(NewDataLiteral(isCompact));
    callableBuilder.Add(NewDataLiteral(itemsCountHint));
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::SqueezeToDict(TRuntimeNode stream, bool multi, const TUnaryLambda& keySelector,
    const TUnaryLambda& payloadSelector, std::string_view callableName, bool isCompact, ui64 itemsCountHint)
{
    if constexpr (RuntimeVersion < 21U) {
        THROW yexception() << "Runtime version (" << RuntimeVersion << ") too old for " << __func__;
    }

    const auto type = stream.GetStaticType();
    MKQL_ENSURE(type->IsStream() || type->IsFlow(), "Expected stream or flow.");

    const auto itemType = type->IsFlow() ? AS_TYPE(TFlowType, type)->GetItemType() : AS_TYPE(TStreamType, type)->GetItemType();
    ThrowIfListOfVoid(itemType);
    const auto itemArg = Arg(itemType);

    const auto key = keySelector(itemArg);
    const auto keyType = key.GetStaticType();

    auto payload = payloadSelector(itemArg);
    auto payloadType = payload.GetStaticType();
    if (multi) {
        payloadType = TListType::Create(payloadType, Env);
    }

    auto dictType = TDictType::Create(keyType, payloadType, Env);
    auto returnType = type->IsFlow()
        ? (TType*) TFlowType::Create(dictType, Env)
        : (TType*) TStreamType::Create(dictType, Env);
    TCallableBuilder callableBuilder(Env, callableName, returnType);
    callableBuilder.Add(stream);
    callableBuilder.Add(itemArg);
    callableBuilder.Add(key);
    callableBuilder.Add(payload);
    callableBuilder.Add(NewDataLiteral(multi));
    callableBuilder.Add(NewDataLiteral(isCompact));
    callableBuilder.Add(NewDataLiteral(itemsCountHint));
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::NarrowSqueezeToDict(TRuntimeNode flow, bool multi, const TNarrowLambda& keySelector,
    const TNarrowLambda& payloadSelector, std::string_view callableName, bool isCompact, ui64 itemsCountHint)
{
    if constexpr (RuntimeVersion < 23U) {
        THROW yexception() << "Runtime version (" << RuntimeVersion << ") too old for " << __func__;
    }

    const auto wideComponents = GetWideComponents(AS_TYPE(TFlowType, flow.GetStaticType()));

    TRuntimeNode::TList itemArgs;
    itemArgs.reserve(wideComponents.size());
    auto i = 0U;
    std::generate_n(std::back_inserter(itemArgs), wideComponents.size(), [&](){ return Arg(wideComponents[i++]); });

    const auto key = keySelector(itemArgs);
    const auto keyType = key.GetStaticType();

    auto payload = payloadSelector(itemArgs);
    auto payloadType = payload.GetStaticType();
    if (multi) {
        payloadType = TListType::Create(payloadType, Env);
    }

    const auto dictType = TDictType::Create(keyType, payloadType, Env);
    const auto returnType = TFlowType::Create(dictType, Env);
    TCallableBuilder callableBuilder(Env, callableName, returnType);
    callableBuilder.Add(flow);
    std::for_each(itemArgs.cbegin(), itemArgs.cend(), std::bind(&TCallableBuilder::Add, std::ref(callableBuilder), std::placeholders::_1));
    callableBuilder.Add(key);
    callableBuilder.Add(payload);
    callableBuilder.Add(NewDataLiteral(multi));
    callableBuilder.Add(NewDataLiteral(isCompact));
    callableBuilder.Add(NewDataLiteral(itemsCountHint));
    return TRuntimeNode(callableBuilder.Build(), false);
}

void TProgramBuilder::ThrowIfListOfVoid(TType* type) {
    MKQL_ENSURE(!VoidWithEffects || !type->IsVoid(), "List of void is forbidden for current function");
}

TRuntimeNode TProgramBuilder::BuildFlatMap(const std::string_view& callableName, TRuntimeNode list, const TUnaryLambda& handler)
{
    const auto listType = list.GetStaticType();
    MKQL_ENSURE(listType->IsFlow() || listType->IsList() || listType->IsOptional() || listType->IsStream(), "Expected flow, list, stream or optional");

    if (listType->IsOptional()) {
        const auto itemArg = Arg(AS_TYPE(TOptionalType, listType)->GetItemType());
        const auto newList = handler(itemArg);
        const auto type = newList.GetStaticType();
        MKQL_ENSURE(type->IsList() || type->IsOptional() || type->IsStream() || type->IsFlow(), "Expected flow, list, stream or optional");
        return IfPresent(list, [&](TRuntimeNode item) {
            return handler(item);
        }, type->IsOptional() ? NewEmptyOptional(type) : type->IsList() ? NewEmptyList(AS_TYPE(TListType, type)->GetItemType()) : EmptyIterator(type));
    }

    const auto itemType = listType->IsFlow() ?
            AS_TYPE(TFlowType, listType)->GetItemType():
            listType->IsList() ?
                AS_TYPE(TListType, listType)->GetItemType():
                AS_TYPE(TStreamType, listType)->GetItemType();

    ThrowIfListOfVoid(itemType);
    const auto itemArg = Arg(itemType);
    const auto newList = handler(itemArg);
    const auto type = newList.GetStaticType();

    TType* retItemType = nullptr;
    if (type->IsOptional()) {
        retItemType = AS_TYPE(TOptionalType, type)->GetItemType();
    } else if (type->IsFlow()) {
        retItemType = AS_TYPE(TFlowType, type)->GetItemType();
    } else if (type->IsList()) {
        retItemType = AS_TYPE(TListType, type)->GetItemType();
    } else if (type->IsStream()) {
        retItemType = AS_TYPE(TStreamType, type)->GetItemType();
    } else {
        THROW yexception() << "Expected flow, list or stream.";
    }

    const auto resultListType = listType->IsFlow() || type->IsFlow() ?
        TFlowType::Create(retItemType, Env):
        listType->IsList() ?
            (TType*)TListType::Create(retItemType, Env):
            (TType*)TStreamType::Create(retItemType, Env);
    TCallableBuilder callableBuilder(Env, callableName, resultListType);
    callableBuilder.Add(list);
    callableBuilder.Add(itemArg);
    callableBuilder.Add(newList);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::MultiMap(TRuntimeNode list, const TExpandLambda& handler)
{
    if constexpr (RuntimeVersion < 16U) {
        const auto single = [=](TRuntimeNode item) -> TRuntimeNode {
            const auto newList = handler(item);
            const auto retItemType = newList.front().GetStaticType();
            MKQL_ENSURE(retItemType->IsSameType(*newList.back().GetStaticType()), "Must be same type.");
            return NewList(retItemType, newList);
        };
        return OrderedFlatMap(list, single);
    }

    const auto listType = list.GetStaticType();
    MKQL_ENSURE(listType->IsFlow() || listType->IsList(), "Expected flow, list, stream or optional");

    const auto itemType = listType->IsFlow() ? AS_TYPE(TFlowType, listType)->GetItemType() : AS_TYPE(TListType, listType)->GetItemType();

    const auto itemArg = Arg(itemType);
    const auto newList = handler(itemArg);

    MKQL_ENSURE(newList.size() > 1U, "Expected many items.");
    const auto retItemType = newList.front().GetStaticType();
    MKQL_ENSURE(retItemType->IsSameType(*newList.back().GetStaticType()), "Must be same type.");

    const auto resultListType = listType->IsFlow() ?
        (TType*)TFlowType::Create(retItemType, Env) : (TType*)TListType::Create(retItemType, Env);
    TCallableBuilder callableBuilder(Env, __func__, resultListType);
    callableBuilder.Add(list);
    callableBuilder.Add(itemArg);
    std::for_each(newList.cbegin(), newList.cend(), std::bind(&TCallableBuilder::Add, std::ref(callableBuilder), std::placeholders::_1));
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::NarrowMultiMap(TRuntimeNode flow, const TWideLambda& handler) {
    if constexpr (RuntimeVersion < 18U) {
        THROW yexception() << "Runtime version (" << RuntimeVersion << ") too old for " << __func__;
    }

    const auto wideComponents = GetWideComponents(AS_TYPE(TFlowType, flow.GetStaticType()));

    TRuntimeNode::TList itemArgs;
    itemArgs.reserve(wideComponents.size());
    auto i = 0U;
    std::generate_n(std::back_inserter(itemArgs), wideComponents.size(), [&](){ return Arg(wideComponents[i++]); });

    const auto newList = handler(itemArgs);

    MKQL_ENSURE(newList.size() > 1U, "Expected many items.");
    const auto retItemType = newList.front().GetStaticType();
    MKQL_ENSURE(retItemType->IsSameType(*newList.back().GetStaticType()), "Must be same type.");

    TCallableBuilder callableBuilder(Env, __func__, NewFlowType(newList.front().GetStaticType()));
    callableBuilder.Add(flow);
    std::for_each(itemArgs.cbegin(), itemArgs.cend(), std::bind(&TCallableBuilder::Add, std::ref(callableBuilder), std::placeholders::_1));
    std::for_each(newList.cbegin(), newList.cend(), std::bind(&TCallableBuilder::Add, std::ref(callableBuilder), std::placeholders::_1));
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::ExpandMap(TRuntimeNode flow, const TExpandLambda& handler) {
    if constexpr (RuntimeVersion < 18U) {
        THROW yexception() << "Runtime version (" << RuntimeVersion << ") too old for " << __func__;
    }

    const auto itemType = AS_TYPE(TFlowType, flow.GetStaticType())->GetItemType();
    const auto itemArg = Arg(itemType);
    const auto newItems = handler(itemArg);

    std::vector<TType*> tupleItems;
    tupleItems.reserve(newItems.size());
    std::transform(newItems.cbegin(), newItems.cend(), std::back_inserter(tupleItems), std::bind(&TRuntimeNode::GetStaticType, std::placeholders::_1));

    TCallableBuilder callableBuilder(Env, __func__, NewFlowType(NewMultiType(tupleItems)));
    callableBuilder.Add(flow);
    callableBuilder.Add(itemArg);
    std::for_each(newItems.cbegin(), newItems.cend(), std::bind(&TCallableBuilder::Add, std::ref(callableBuilder), std::placeholders::_1));
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::WideMap(TRuntimeNode flow, const TWideLambda& handler) {
    if constexpr (RuntimeVersion < 18U) {
        THROW yexception() << "Runtime version (" << RuntimeVersion << ") too old for " << __func__;
    }

    const auto wideComponents = GetWideComponents(AS_TYPE(TFlowType, flow.GetStaticType()));

    TRuntimeNode::TList itemArgs;
    itemArgs.reserve(wideComponents.size());
    auto i = 0U;
    std::generate_n(std::back_inserter(itemArgs), wideComponents.size(), [&](){ return Arg(wideComponents[i++]); });

    const auto newItems = handler(itemArgs);

    std::vector<TType*> tupleItems;
    tupleItems.reserve(newItems.size());
    std::transform(newItems.cbegin(), newItems.cend(), std::back_inserter(tupleItems), std::bind(&TRuntimeNode::GetStaticType, std::placeholders::_1));

    TCallableBuilder callableBuilder(Env, __func__, NewFlowType(NewMultiType(tupleItems)));
    callableBuilder.Add(flow);
    std::for_each(itemArgs.cbegin(), itemArgs.cend(), std::bind(&TCallableBuilder::Add, std::ref(callableBuilder), std::placeholders::_1));
    std::for_each(newItems.cbegin(), newItems.cend(), std::bind(&TCallableBuilder::Add, std::ref(callableBuilder), std::placeholders::_1));
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::WideChain1Map(TRuntimeNode flow, const TWideLambda& init, const TBinaryWideLambda& update) {
    if constexpr (RuntimeVersion < 23U) {
        THROW yexception() << "Runtime version (" << RuntimeVersion << ") too old for " << __func__;
    }

    const auto wideComponents = GetWideComponents(AS_TYPE(TFlowType, flow.GetStaticType()));

    TRuntimeNode::TList inputArgs;
    inputArgs.reserve(wideComponents.size());
    auto i = 0U;
    std::generate_n(std::back_inserter(inputArgs), wideComponents.size(), [&](){ return Arg(wideComponents[i++]); });

    const auto initItems = init(inputArgs);

    std::vector<TType*> tupleItems;
    tupleItems.reserve(initItems.size());
    std::transform(initItems.cbegin(), initItems.cend(), std::back_inserter(tupleItems), std::bind(&TRuntimeNode::GetStaticType, std::placeholders::_1));

    TRuntimeNode::TList outputArgs;
    outputArgs.reserve(tupleItems.size());
    std::transform(tupleItems.cbegin(), tupleItems.cend(), std::back_inserter(outputArgs), std::bind(&TProgramBuilder::Arg, this, std::placeholders::_1));

    const auto updateItems = update(inputArgs, outputArgs);

    MKQL_ENSURE(initItems.size() == updateItems.size(), "Expected same width.");

    TCallableBuilder callableBuilder(Env, __func__, NewFlowType(NewMultiType(tupleItems)));
    callableBuilder.Add(flow);
    std::for_each(inputArgs.cbegin(), inputArgs.cend(), std::bind(&TCallableBuilder::Add, std::ref(callableBuilder), std::placeholders::_1));
    std::for_each(initItems.cbegin(), initItems.cend(), std::bind(&TCallableBuilder::Add, std::ref(callableBuilder), std::placeholders::_1));
    std::for_each(outputArgs.cbegin(), outputArgs.cend(), std::bind(&TCallableBuilder::Add, std::ref(callableBuilder), std::placeholders::_1));
    std::for_each(updateItems.cbegin(), updateItems.cend(), std::bind(&TCallableBuilder::Add, std::ref(callableBuilder), std::placeholders::_1));
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::NarrowMap(TRuntimeNode flow, const TNarrowLambda& handler) {
    if constexpr (RuntimeVersion < 18U) {
        THROW yexception() << "Runtime version (" << RuntimeVersion << ") too old for " << __func__;
    }

    const auto wideComponents = GetWideComponents(AS_TYPE(TFlowType, flow.GetStaticType()));

    TRuntimeNode::TList itemArgs;
    itemArgs.reserve(wideComponents.size());
    auto i = 0U;
    std::generate_n(std::back_inserter(itemArgs), wideComponents.size(), [&](){ return Arg(wideComponents[i++]); });

    const auto newItem = handler(itemArgs);

    TCallableBuilder callableBuilder(Env, __func__, NewFlowType(newItem.GetStaticType()));
    callableBuilder.Add(flow);
    std::for_each(itemArgs.cbegin(), itemArgs.cend(), std::bind(&TCallableBuilder::Add, std::ref(callableBuilder), std::placeholders::_1));
    callableBuilder.Add(newItem);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::NarrowFlatMap(TRuntimeNode flow, const TNarrowLambda& handler) {
    if constexpr (RuntimeVersion < 18U) {
        THROW yexception() << "Runtime version (" << RuntimeVersion << ") too old for " << __func__;
    }

    const auto wideComponents = GetWideComponents(AS_TYPE(TFlowType, flow.GetStaticType()));

    TRuntimeNode::TList itemArgs;
    itemArgs.reserve(wideComponents.size());
    auto i = 0U;
    std::generate_n(std::back_inserter(itemArgs), wideComponents.size(), [&](){ return Arg(wideComponents[i++]); });

    const auto newList = handler(itemArgs);
    const auto type = newList.GetStaticType();

    TType* retItemType = nullptr;
    if (type->IsOptional()) {
        retItemType = AS_TYPE(TOptionalType, type)->GetItemType();
    } else if (type->IsFlow()) {
        retItemType = AS_TYPE(TFlowType, type)->GetItemType();
    } else if (type->IsList()) {
        retItemType = AS_TYPE(TListType, type)->GetItemType();
    } else if (type->IsStream()) {
        retItemType = AS_TYPE(TStreamType, type)->GetItemType();
    } else {
        THROW yexception() << "Expected flow, list or stream.";
    }

    TCallableBuilder callableBuilder(Env, __func__, NewFlowType(retItemType));
    callableBuilder.Add(flow);
    std::for_each(itemArgs.cbegin(), itemArgs.cend(), std::bind(&TCallableBuilder::Add, std::ref(callableBuilder), std::placeholders::_1));
    callableBuilder.Add(newList);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::BuildWideFilter(const std::string_view& callableName, TRuntimeNode flow, const TNarrowLambda& handler) {
    if constexpr (RuntimeVersion < 18U) {
        THROW yexception() << "Runtime version (" << RuntimeVersion << ") too old for " << __func__;
    }

    const auto wideComponents = GetWideComponents(AS_TYPE(TFlowType, flow.GetStaticType()));

    TRuntimeNode::TList itemArgs;
    itemArgs.reserve(wideComponents.size());
    auto i = 0U;
    std::generate_n(std::back_inserter(itemArgs), wideComponents.size(), [&](){ return Arg(wideComponents[i++]); });

    const auto predicate = handler(itemArgs);

    TCallableBuilder callableBuilder(Env, callableName, flow.GetStaticType());
    callableBuilder.Add(flow);
    std::for_each(itemArgs.cbegin(), itemArgs.cend(), std::bind(&TCallableBuilder::Add, std::ref(callableBuilder), std::placeholders::_1));
    callableBuilder.Add(predicate);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::WideFilter(TRuntimeNode flow, const TNarrowLambda& handler) {
    return BuildWideFilter(__func__, flow, handler);
}

TRuntimeNode TProgramBuilder::WideTakeWhile(TRuntimeNode flow, const TNarrowLambda& handler) {
    return BuildWideFilter(__func__, flow, handler);
}

TRuntimeNode TProgramBuilder::WideSkipWhile(TRuntimeNode flow, const TNarrowLambda& handler) {
    return BuildWideFilter(__func__, flow, handler);
}

TRuntimeNode TProgramBuilder::WideTakeWhileInclusive(TRuntimeNode flow, const TNarrowLambda& handler) {
    return BuildWideFilter(__func__, flow, handler);
}

TRuntimeNode TProgramBuilder::WideSkipWhileInclusive(TRuntimeNode flow, const TNarrowLambda& handler) {
    return BuildWideFilter(__func__, flow, handler);
}

TRuntimeNode TProgramBuilder::WideFilter(TRuntimeNode flow, TRuntimeNode limit, const TNarrowLambda& handler) {
    const auto wideComponents = GetWideComponents(AS_TYPE(TFlowType, flow.GetStaticType()));

    TRuntimeNode::TList itemArgs;
    itemArgs.reserve(wideComponents.size());
    auto i = 0U;
    std::generate_n(std::back_inserter(itemArgs), wideComponents.size(), [&](){ return Arg(wideComponents[i++]); });

    const auto predicate = handler(itemArgs);

    TCallableBuilder callableBuilder(Env, __func__, flow.GetStaticType());
    callableBuilder.Add(flow);
    std::for_each(itemArgs.cbegin(), itemArgs.cend(), std::bind(&TCallableBuilder::Add, std::ref(callableBuilder), std::placeholders::_1));
    callableBuilder.Add(predicate);
    callableBuilder.Add(limit);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::BuildFilter(const std::string_view& callableName, TRuntimeNode list, const TUnaryLambda& handler, TType* resultType)
{
    const auto listType = list.GetStaticType();
    MKQL_ENSURE(listType->IsFlow() || listType->IsList() || listType->IsStream(), "Expected flow, list or stream.");
    const auto outputType = resultType ? resultType : listType;

    const auto itemType = listType->IsFlow() ?
        AS_TYPE(TFlowType, listType)->GetItemType():
        listType->IsList() ?
            AS_TYPE(TListType, listType)->GetItemType():
            AS_TYPE(TStreamType, listType)->GetItemType();
    ThrowIfListOfVoid(itemType);

    const auto itemArg = Arg(itemType);
    const auto predicate = handler(itemArg);

    MKQL_ENSURE(predicate.GetStaticType()->IsData(), "Expected boolean data");

    const auto& detailedPredicateType = static_cast<const TDataType&>(*predicate.GetStaticType());
    MKQL_ENSURE(detailedPredicateType.GetSchemeType() == NUdf::TDataType<bool>::Id, "Expected boolean data");

    TCallableBuilder callableBuilder(Env, callableName, outputType);
    callableBuilder.Add(list);
    callableBuilder.Add(itemArg);
    callableBuilder.Add(predicate);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::BuildFilter(const std::string_view& callableName, TRuntimeNode list, TRuntimeNode limit, const TUnaryLambda& handler, TType* resultType)
{
    if constexpr (RuntimeVersion < 4U) {
        return Take(BuildFilter(callableName, list, handler, resultType), limit);
    }

    const auto listType = list.GetStaticType();
    MKQL_ENSURE(listType->IsFlow() || listType->IsList() || listType->IsStream(), "Expected flow, list or stream.");
    MKQL_ENSURE(limit.GetStaticType()->IsData(), "Expected data");
    const auto outputType = resultType ? resultType : listType;

    const auto itemType = listType->IsFlow() ?
        AS_TYPE(TFlowType, listType)->GetItemType():
        listType->IsList() ?
            AS_TYPE(TListType, listType)->GetItemType():
            AS_TYPE(TStreamType, listType)->GetItemType();
    ThrowIfListOfVoid(itemType);

    const auto itemArg = Arg(itemType);
    const auto predicate = handler(itemArg);

    MKQL_ENSURE(predicate.GetStaticType()->IsData(), "Expected boolean data");

    const auto& detailedPredicateType = static_cast<const TDataType&>(*predicate.GetStaticType());
    MKQL_ENSURE(detailedPredicateType.GetSchemeType() == NUdf::TDataType<bool>::Id, "Expected boolean data");

    TCallableBuilder callableBuilder(Env, callableName, outputType);
    callableBuilder.Add(list);
    callableBuilder.Add(limit);
    callableBuilder.Add(itemArg);
    callableBuilder.Add(predicate);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::Filter(TRuntimeNode list, const TUnaryLambda& handler, TType* resultType)
{
    const auto type = list.GetStaticType();

    if (type->IsOptional()) {
        return
            IfPresent(list,
                [&](TRuntimeNode item) {
                    return If(handler(item), item, NewEmptyOptional(resultType), resultType);
                },
                NewEmptyOptional(resultType)
            );
    }

    return BuildFilter(__func__, list, handler, resultType);
}

TRuntimeNode TProgramBuilder::BuildHeap(const std::string_view& callableName, TRuntimeNode list, const TBinaryLambda& comparator) {
    const auto listType = list.GetStaticType();
    MKQL_ENSURE(listType->IsList(), "Expected list.");
    const auto itemType = AS_TYPE(TListType, listType)->GetItemType();

    const auto leftArg = Arg(itemType);
    const auto rightArg = Arg(itemType);
    const auto predicate = comparator(leftArg, rightArg);

    TCallableBuilder callableBuilder(Env, callableName, listType);
    callableBuilder.Add(list);
    callableBuilder.Add(leftArg);
    callableBuilder.Add(rightArg);
    callableBuilder.Add(predicate);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::BuildNth(const std::string_view& callableName, TRuntimeNode list, TRuntimeNode n, const TBinaryLambda& comparator) {
    const auto listType = list.GetStaticType();
    MKQL_ENSURE(listType->IsList(), "Expected list.");
    const auto itemType = AS_TYPE(TListType, listType)->GetItemType();

    MKQL_ENSURE(n.GetStaticType()->IsData(), "Expected data");
    MKQL_ENSURE(static_cast<const TDataType&>(*n.GetStaticType()).GetSchemeType() == NUdf::TDataType<ui64>::Id, "Expected ui64");

    const auto leftArg = Arg(itemType);
    const auto rightArg = Arg(itemType);
    const auto predicate = comparator(leftArg, rightArg);

    TCallableBuilder callableBuilder(Env, callableName, listType);
    callableBuilder.Add(list);
    callableBuilder.Add(n);
    callableBuilder.Add(leftArg);
    callableBuilder.Add(rightArg);
    callableBuilder.Add(predicate);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::MakeHeap(TRuntimeNode list, const TBinaryLambda& comparator) {
    return BuildHeap(__func__, list, std::move(comparator));
}

TRuntimeNode TProgramBuilder::PushHeap(TRuntimeNode list, const TBinaryLambda& comparator) {
    return BuildHeap(__func__, list, std::move(comparator));
}

TRuntimeNode TProgramBuilder::PopHeap(TRuntimeNode list, const TBinaryLambda& comparator) {
    return BuildHeap(__func__, list, std::move(comparator));
}

TRuntimeNode TProgramBuilder::SortHeap(TRuntimeNode list, const TBinaryLambda& comparator) {
    return BuildHeap(__func__, list, std::move(comparator));
}

TRuntimeNode TProgramBuilder::StableSort(TRuntimeNode list, const TBinaryLambda& comparator) {
    return BuildHeap(__func__, list, std::move(comparator));
}

TRuntimeNode TProgramBuilder::NthElement(TRuntimeNode list, TRuntimeNode n, const TBinaryLambda& comparator) {
    return BuildNth(__func__, list, n, std::move(comparator));
}

TRuntimeNode TProgramBuilder::PartialSort(TRuntimeNode list, TRuntimeNode n, const TBinaryLambda& comparator) {
    return BuildNth(__func__, list, n, std::move(comparator));
}

TRuntimeNode TProgramBuilder::BuildMap(const std::string_view& callableName, TRuntimeNode list, const TUnaryLambda& handler)
{
    const auto listType = list.GetStaticType();
    MKQL_ENSURE(listType->IsFlow() || listType->IsList() || listType->IsStream() || listType->IsOptional(), "Expected flow, list, stream or optional");

    if (listType->IsOptional()) {
        const auto itemArg = Arg(AS_TYPE(TOptionalType, listType)->GetItemType());
        const auto newItem = handler(itemArg);

        return IfPresent(list,
            [&](TRuntimeNode item) { return NewOptional(handler(item)); },
            NewEmptyOptional(NewOptionalType(newItem.GetStaticType()))
        );
    }

    const auto itemType = listType->IsFlow() ?
        AS_TYPE(TFlowType, listType)->GetItemType():
        listType->IsList() ?
            AS_TYPE(TListType, listType)->GetItemType():
            AS_TYPE(TStreamType, listType)->GetItemType();

    ThrowIfListOfVoid(itemType);

    const auto itemArg = Arg(itemType);
    const auto newItem = handler(itemArg);

    const auto resultListType = listType->IsFlow() ?
        (TType*)TFlowType::Create(newItem.GetStaticType(), Env):
        listType->IsList() ?
            (TType*)TListType::Create(newItem.GetStaticType(), Env):
            (TType*)TStreamType::Create(newItem.GetStaticType(), Env);
    TCallableBuilder callableBuilder(Env, callableName, resultListType);
    callableBuilder.Add(list);
    callableBuilder.Add(itemArg);
    callableBuilder.Add(newItem);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::Invoke(const std::string_view& funcName, TType* resultType, const TArrayRef<const TRuntimeNode>& args) {
    MKQL_ENSURE(args.size() >= 1U && args.size() <= 3U, "Expected from one to three arguments.");
    std::array<TArgType, 4U> argTypes;
    argTypes.front().first = UnpackOptionalData(resultType, argTypes.front().second)->GetSchemeType();
    auto i = 0U;
    for (const auto& arg : args) {
        ++i;
        argTypes[i].first = UnpackOptionalData(arg, argTypes[i].second)->GetSchemeType();
    }

    FunctionRegistry.GetBuiltins()->GetBuiltin(funcName, argTypes.data(), 1U + args.size());

    TCallableBuilder callableBuilder(Env, __func__, resultType);
    callableBuilder.Add(NewDataLiteral<NUdf::EDataSlot::String>(funcName));
    for (const auto& arg : args) {
        callableBuilder.Add(arg);
    }

    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::Udf(
    const std::string_view& funcName,
    TRuntimeNode runConfig,
    TType* userType,
    const std::string_view& typeConfig
)
{
    TRuntimeNode userTypeNode = userType ? TRuntimeNode(userType, true) : TRuntimeNode(Env.GetVoidLazy()->GetType(), true);
    const ui32 flags = NUdf::IUdfModule::TFlags::TypesOnly;

    if (!TypeInfoHelper) {
        TypeInfoHelper = new TTypeInfoHelper();
    }

    TFunctionTypeInfo funcInfo;
    TStatus status = FunctionRegistry.FindFunctionTypeInfo(
        Env, TypeInfoHelper, nullptr, funcName, userType, typeConfig, flags, {}, nullptr, &funcInfo);
    MKQL_ENSURE(status.IsOk(), status.GetError());

    auto runConfigType = funcInfo.RunConfigType;
    if (runConfig) {
        bool typesMatch = runConfigType->IsSameType(*runConfig.GetStaticType());
        MKQL_ENSURE(typesMatch, "RunConfig type mismatch");
    } else {
        MKQL_ENSURE(runConfigType->IsVoid() || runConfigType->IsOptional(), "RunConfig must be void or optional");
        if (runConfigType->IsVoid()) {
            runConfig = NewVoid();
        } else {
            runConfig = NewEmptyOptional(const_cast<TType*>(runConfigType));
        }
    }

    auto funNameNode = NewDataLiteral<NUdf::EDataSlot::String>(funcName);
    auto typeConfigNode = NewDataLiteral<NUdf::EDataSlot::String>(typeConfig);

    TCallableBuilder callableBuilder(Env, __func__, funcInfo.FunctionType);
    callableBuilder.Add(funNameNode);
    callableBuilder.Add(userTypeNode);
    callableBuilder.Add(typeConfigNode);
    callableBuilder.Add(runConfig);

    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::TypedUdf(
    const std::string_view& funcName,
    TType* funcType,
    TRuntimeNode runConfig,
    TType* userType,
    const std::string_view& typeConfig,
    const std::string_view& file,
    ui32 row,
    ui32 column)
{
    auto funNameNode = NewDataLiteral<NUdf::EDataSlot::String>(funcName);
    auto typeConfigNode = NewDataLiteral<NUdf::EDataSlot::String>(typeConfig);
    TRuntimeNode userTypeNode = userType ? TRuntimeNode(userType, true) : TRuntimeNode(Env.GetVoidLazy(), true);

    TCallableBuilder callableBuilder(Env, "Udf", funcType);
    callableBuilder.Add(funNameNode);
    callableBuilder.Add(userTypeNode);
    callableBuilder.Add(typeConfigNode);
    callableBuilder.Add(runConfig);
    callableBuilder.Add(NewDataLiteral<NUdf::EDataSlot::String>(file));
    callableBuilder.Add(NewDataLiteral(row));
    callableBuilder.Add(NewDataLiteral(column));

    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::ScriptUdf(
    const std::string_view& moduleName,
    const std::string_view& funcName,
    TType* funcType,
    TRuntimeNode script,
    const std::string_view& file,
    ui32 row,
    ui32 column)
{
    MKQL_ENSURE(funcType, "UDF callable type must not be empty");
    MKQL_ENSURE(funcType->IsCallable(), "type must be callable");
    auto scriptType = NKikimr::NMiniKQL::ScriptTypeFromStr(moduleName);
    MKQL_ENSURE(scriptType != EScriptType::Unknown, "unknown script type '" << moduleName << "'");
    EnsureScriptSpecificTypes(scriptType, static_cast<TCallableType*>(funcType), Env);

    auto scriptTypeStr = IsCustomPython(scriptType) ? moduleName : ScriptTypeAsStr(CanonizeScriptType(scriptType));

    TStringBuilder name;
    name.reserve(scriptTypeStr.size() + funcName.size() + 1);
    name << scriptTypeStr << '.' << funcName;
    auto funcNameNode = NewDataLiteral<NUdf::EDataSlot::String>(name);
    TRuntimeNode userTypeNode(funcType, true);
    auto typeConfigNode = NewDataLiteral<NUdf::EDataSlot::String>("");

    TCallableBuilder callableBuilder(Env, __func__, funcType);
    callableBuilder.Add(funcNameNode);
    callableBuilder.Add(userTypeNode);
    callableBuilder.Add(typeConfigNode);
    callableBuilder.Add(script);
    callableBuilder.Add(NewDataLiteral<NUdf::EDataSlot::String>(file));
    callableBuilder.Add(NewDataLiteral(row));
    callableBuilder.Add(NewDataLiteral(column));

    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::Apply(TRuntimeNode callableNode, const TArrayRef<const TRuntimeNode>& args,
         const std::string_view& file, ui32 row, ui32 column, ui32 dependentCount) {
    MKQL_ENSURE(dependentCount <= args.size(), "Too many dependent nodes");
    ui32 usedArgs = args.size() - dependentCount;
    MKQL_ENSURE(!callableNode.IsImmediate() && callableNode.GetNode()->GetType()->IsCallable(),
            "Expected callable");

    auto callable = static_cast<TCallable*>(callableNode.GetNode());
    TType* returnType = callable->GetType()->GetReturnType();
    MKQL_ENSURE(returnType->IsCallable(), "Expected callable as return type");

    auto callableType = static_cast<TCallableType*>(returnType);
    MKQL_ENSURE(usedArgs <= callableType->GetArgumentsCount(), "Too many arguments");
    MKQL_ENSURE(usedArgs >= callableType->GetArgumentsCount() - callableType->GetOptionalArgumentsCount(), "Too few arguments");

    for (ui32 i = 0; i < usedArgs; i++) {
        TType* argType = callableType->GetArgumentType(i);
        TRuntimeNode arg = args[i];
        MKQL_ENSURE(arg.GetStaticType()->IsConvertableTo(*argType),
                    "Argument type mismatch for argument " << i << ": runtime " << argType->GetKindAsStr()
                                   << " with static " << arg.GetStaticType()->GetKindAsStr());
    }

    TCallableBuilder callableBuilder(Env, RuntimeVersion >= 8 ? "Apply2" : "Apply", callableType->GetReturnType());
    callableBuilder.Add(callableNode);
    callableBuilder.Add(NewDataLiteral<ui32>(dependentCount));

    if constexpr (RuntimeVersion >= 8) {
        callableBuilder.Add(NewDataLiteral<NUdf::EDataSlot::String>(file));
        callableBuilder.Add(NewDataLiteral(row));
        callableBuilder.Add(NewDataLiteral(column));
    }

    for (const auto& arg: args) {
        callableBuilder.Add(arg);
    }

    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::Apply(
        TRuntimeNode callableNode,
        const TArrayRef<const TRuntimeNode>& args,
        ui32 dependentCount) {
    return Apply(callableNode, args, {}, 0, 0, dependentCount);
}

TRuntimeNode TProgramBuilder::Callable(TType* callableType, const TArrayLambda& handler) {
    auto castedCallableType = AS_TYPE(TCallableType, callableType);
    std::vector<TRuntimeNode> args;
    args.reserve(castedCallableType->GetArgumentsCount());
    for (ui32 i = 0; i < castedCallableType->GetArgumentsCount(); ++i) {
        args.push_back(Arg(castedCallableType->GetArgumentType(i)));
    }

    auto res = handler(args);
    TCallableBuilder callableBuilder(Env, __func__, callableType);
    for (ui32 i = 0; i < castedCallableType->GetArgumentsCount(); ++i) {
        callableBuilder.Add(args[i]);
    }

    callableBuilder.Add(res);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::NewNull() {
    if (!UseNullType || RuntimeVersion < 11) {
        TCallableBuilder callableBuilder(Env, "Null", NewOptionalType(Env.GetVoidLazy()->GetType()));
        return TRuntimeNode(callableBuilder.Build(), false);
    } else {
        return TRuntimeNode(Env.GetNullLazy(), true);
    }
}

TRuntimeNode TProgramBuilder::Concat(TRuntimeNode data1, TRuntimeNode data2) {
    bool isOpt1, isOpt2;
    const auto type1 = UnpackOptionalData(data1, isOpt1)->GetSchemeType();
    const auto type2 = UnpackOptionalData(data2, isOpt2)->GetSchemeType();
    const auto resultType = NewDataType(type1 == type2 ? type1 : NUdf::TDataType<char*>::Id);
    return InvokeBinary(__func__, isOpt1 || isOpt2 ? NewOptionalType(resultType) : resultType, data1, data2);
}

TRuntimeNode TProgramBuilder::AggrConcat(TRuntimeNode data1, TRuntimeNode data2) {
    MKQL_ENSURE(data1.GetStaticType()->IsSameType(*data2.GetStaticType()), "Operands type mismatch.");
    const std::array<TRuntimeNode, 2> args = {{ data1, data2 }};
    return Invoke(__func__, data1.GetStaticType(), args);
}

TRuntimeNode TProgramBuilder::Substring(TRuntimeNode data, TRuntimeNode start, TRuntimeNode count) {
    const std::array<TRuntimeNode, 3U> args = {{ data, start, count }};
    return Invoke(__func__, data.GetStaticType(), args);
}

TRuntimeNode TProgramBuilder::Find(TRuntimeNode haystack, TRuntimeNode needle, TRuntimeNode pos) {
    const std::array<TRuntimeNode, 3U> args = {{ haystack, needle, pos }};
    return Invoke(__func__, NewOptionalType(NewDataType(NUdf::TDataType<ui32>::Id)), args);
}

TRuntimeNode TProgramBuilder::RFind(TRuntimeNode haystack, TRuntimeNode needle, TRuntimeNode pos) {
    const std::array<TRuntimeNode, 3U> args = {{ haystack, needle, pos }};
    return Invoke(__func__, NewOptionalType(NewDataType(NUdf::TDataType<ui32>::Id)), args);
}

TRuntimeNode TProgramBuilder::StartsWith(TRuntimeNode string, TRuntimeNode prefix) {
    if constexpr (RuntimeVersion < 19U) {
        THROW yexception() << "Runtime version (" << RuntimeVersion << ") too old for " << __func__;
    }

    return DataCompare(__func__, string, prefix);
}

TRuntimeNode TProgramBuilder::EndsWith(TRuntimeNode string, TRuntimeNode suffix) {
    if constexpr (RuntimeVersion < 19U) {
        THROW yexception() << "Runtime version (" << RuntimeVersion << ") too old for " << __func__;
    }

    return DataCompare(__func__, string, suffix);
}

TRuntimeNode TProgramBuilder::StringContains(TRuntimeNode string, TRuntimeNode pattern) {
    bool isOpt1, isOpt2;
    TDataType* type1 = UnpackOptionalData(string, isOpt1);
    TDataType* type2 = UnpackOptionalData(pattern, isOpt2);
    MKQL_ENSURE(type1->GetSchemeType() == NUdf::TDataType<NUdf::TUtf8>::Id ||
                type1->GetSchemeType() == NUdf::TDataType<char*>::Id, "Expecting string as first argument");
    MKQL_ENSURE(type2->GetSchemeType() == NUdf::TDataType<NUdf::TUtf8>::Id ||
                type2->GetSchemeType() == NUdf::TDataType<char*>::Id, "Expecting string as second argument");
    if constexpr (RuntimeVersion < 32U) {
        auto stringCasted = (type1->GetSchemeType() == NUdf::TDataType<NUdf::TUtf8>::Id) ? ToString(string) : string;
        auto patternCasted = (type2->GetSchemeType() == NUdf::TDataType<NUdf::TUtf8>::Id) ? ToString(pattern) : pattern;
        auto found = Exists(Find(stringCasted, patternCasted, NewDataLiteral(ui32(0))));
        if (!isOpt1 && !isOpt2) {
            return found;
        }
        TVector<TRuntimeNode> predicates;
        if (isOpt1) {
            predicates.push_back(Exists(string));
        }
        if (isOpt2) {
            predicates.push_back(Exists(pattern));
        }

        TRuntimeNode argsNotNull = (predicates.size() == 1) ? predicates.front() : And(predicates);
        return If(argsNotNull, NewOptional(found), NewEmptyOptionalDataLiteral(NUdf::TDataType<bool>::Id));
    }

    return DataCompare(__func__, string, pattern);
}

TRuntimeNode TProgramBuilder::ByteAt(TRuntimeNode data, TRuntimeNode index) {
    const std::array<TRuntimeNode, 2U> args = {{ data, index }};
    return Invoke(__func__, NewOptionalType(NewDataType(NUdf::TDataType<ui8>::Id)), args);
}

TRuntimeNode TProgramBuilder::Size(TRuntimeNode data) {
    return UnaryDataFunction(data, __func__, TDataFunctionFlags::HasUi32Result | TDataFunctionFlags::AllowNull | TDataFunctionFlags::AllowOptionalArgs | TDataFunctionFlags::CommonOptionalResult);
}

template <bool Utf8>
TRuntimeNode TProgramBuilder::ToString(TRuntimeNode data) {
    bool isOptional;
    UnpackOptionalData(data, isOptional);
    const auto resultType = NewDataType(Utf8 ? NUdf::EDataSlot::Utf8 : NUdf::EDataSlot::String, isOptional);

    TCallableBuilder callableBuilder(Env, __func__, resultType);
    callableBuilder.Add(data);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::FromString(TRuntimeNode data, TType* type) {
    bool isOptional;
    const auto sourceType = UnpackOptionalData(data, isOptional);
    const auto targetType = UnpackOptionalData(type, isOptional);
    MKQL_ENSURE(sourceType->GetSchemeType() == NUdf::TDataType<char*>::Id || sourceType->GetSchemeType() == NUdf::TDataType<NUdf::TUtf8>::Id, "Expected String");
    MKQL_ENSURE(targetType->GetSchemeType() != 0, "Null is not allowed");
    TCallableBuilder callableBuilder(Env, __func__, type);
    callableBuilder.Add(data);
    callableBuilder.Add(NewDataLiteral(static_cast<ui32>(targetType->GetSchemeType())));
    if (targetType->GetSchemeType() == NUdf::TDataType<NUdf::TDecimal>::Id) {
        const auto& params = static_cast<const TDataDecimalType*>(targetType)->GetParams();
        callableBuilder.Add(NewDataLiteral(params.first));
        callableBuilder.Add(NewDataLiteral(params.second));
    }
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::StrictFromString(TRuntimeNode data, TType* type) {
    bool isOptional;
    const auto sourceType = UnpackOptionalData(data, isOptional);
    const auto targetType = UnpackOptionalData(type, isOptional);
    MKQL_ENSURE(sourceType->GetSchemeType() == NUdf::TDataType<char*>::Id || sourceType->GetSchemeType() == NUdf::TDataType<NUdf::TUtf8>::Id, "Expected String");
    MKQL_ENSURE(targetType->GetSchemeType() != 0, "Null is not allowed");
    TCallableBuilder callableBuilder(Env, __func__, type);
    callableBuilder.Add(data);
    callableBuilder.Add(NewDataLiteral(static_cast<ui32>(targetType->GetSchemeType())));
    if (targetType->GetSchemeType() == NUdf::TDataType<NUdf::TDecimal>::Id) {
        const auto& params = static_cast<const TDataDecimalType*>(targetType)->GetParams();
        callableBuilder.Add(NewDataLiteral(params.first));
        callableBuilder.Add(NewDataLiteral(params.second));
    }
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::ToBytes(TRuntimeNode data) {
    return UnaryDataFunction(data, __func__, TDataFunctionFlags::HasStringResult | TDataFunctionFlags::AllowOptionalArgs | TDataFunctionFlags::CommonOptionalResult);
}

TRuntimeNode TProgramBuilder::FromBytes(TRuntimeNode data, NUdf::TDataTypeId schemeType) {
    auto type = data.GetStaticType();
    bool isOptional;
    auto dataType = UnpackOptionalData(type, isOptional);
    MKQL_ENSURE(dataType->GetSchemeType() == NUdf::TDataType<char*>::Id, "Expected String");

    auto outDataType = NewDataType(schemeType);
    auto resultType = NewOptionalType(outDataType);
    TCallableBuilder callableBuilder(Env, __func__, resultType);
    callableBuilder.Add(data);
    callableBuilder.Add(NewDataLiteral(static_cast<ui32>(schemeType)));
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::InversePresortString(TRuntimeNode data) {
    const std::array<TRuntimeNode, 1U> args = {{ data }};
    return Invoke(__func__, NewDataType(NUdf::TDataType<char*>::Id), args);
}

TRuntimeNode TProgramBuilder::InverseString(TRuntimeNode data) {
    const std::array<TRuntimeNode, 1U> args = {{ data }};
    return Invoke(__func__, NewDataType(NUdf::TDataType<char*>::Id), args);
}

TRuntimeNode TProgramBuilder::Random(const TArrayRef<const TRuntimeNode>& dependentNodes) {
    TCallableBuilder callableBuilder(Env, __func__, NewDataType(NUdf::TDataType<double>::Id));
    for (auto& x : dependentNodes) {
        callableBuilder.Add(x);
    }

    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::RandomNumber(const TArrayRef<const TRuntimeNode>& dependentNodes) {
    TCallableBuilder callableBuilder(Env, __func__, NewDataType(NUdf::TDataType<ui64>::Id));
    for (auto& x : dependentNodes) {
        callableBuilder.Add(x);
    }

    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::RandomUuid(const TArrayRef<const TRuntimeNode>& dependentNodes) {
    TCallableBuilder callableBuilder(Env, __func__, NewDataType(NUdf::TDataType<NUdf::TUuid>::Id));
    for (auto& x : dependentNodes) {
        callableBuilder.Add(x);
    }

    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::Now(const TArrayRef<const TRuntimeNode>& args) {
    TCallableBuilder callableBuilder(Env, __func__, NewDataType(NUdf::TDataType<ui64>::Id));
    for (const auto& x : args) {
        callableBuilder.Add(x);
    }

    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::CurrentUtcDate(const TArrayRef<const TRuntimeNode>& args) {
    return Cast(CurrentUtcTimestamp(args), NewDataType(NUdf::TDataType<NUdf::TDate>::Id));
}

TRuntimeNode TProgramBuilder::CurrentUtcDatetime(const TArrayRef<const TRuntimeNode>& args) {
    return Cast(CurrentUtcTimestamp(args), NewDataType(NUdf::TDataType<NUdf::TDatetime>::Id));
}

TRuntimeNode TProgramBuilder::CurrentUtcTimestamp(const TArrayRef<const TRuntimeNode>& args) {
    return Coalesce(ToIntegral(Now(args), NewDataType(NUdf::TDataType<NUdf::TTimestamp>::Id, true)),
        TRuntimeNode(BuildDataLiteral(NUdf::TUnboxedValuePod(ui64(NUdf::MAX_TIMESTAMP - 1ULL)), NUdf::TDataType<NUdf::TTimestamp>::Id, Env), true));
}

TRuntimeNode TProgramBuilder::Pickle(TRuntimeNode data) {
    TCallableBuilder callableBuilder(Env, __func__, NewDataType(NUdf::EDataSlot::String));
    callableBuilder.Add(data);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::StablePickle(TRuntimeNode data) {
    TCallableBuilder callableBuilder(Env, __func__, NewDataType(NUdf::EDataSlot::String));
    callableBuilder.Add(data);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::Unpickle(TType* type, TRuntimeNode serialized) {
    MKQL_ENSURE(AS_TYPE(TDataType, serialized)->GetSchemeType() == NUdf::TDataType<char*>::Id, "Expected String");
    TCallableBuilder callableBuilder(Env, __func__, type);
    callableBuilder.Add(TRuntimeNode(type, true));
    callableBuilder.Add(serialized);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::Ascending(TRuntimeNode data) {
    auto dataType = NewDataType(NUdf::EDataSlot::String);
    TCallableBuilder callableBuilder(Env, __func__, dataType);
    callableBuilder.Add(data);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::Descending(TRuntimeNode data) {
    auto dataType = NewDataType(NUdf::EDataSlot::String);
    TCallableBuilder callableBuilder(Env, __func__, dataType);
    callableBuilder.Add(data);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::Convert(TRuntimeNode data, TType* type) {
    if (data.GetStaticType()->IsSameType(*type)) {
        return data;
    }

    bool isOptional;
    const auto dataType = UnpackOptionalData(data, isOptional);

    const std::array<TRuntimeNode, 1> args = {{ data }};

    if (dataType->GetSchemeType() == NUdf::TDataType<NUdf::TDecimal>::Id) {
        const auto targetSchemeType = UnpackOptionalData(type, isOptional)->GetSchemeType();
        TStringStream str;
        str << "To" << NUdf::GetDataTypeInfo(NUdf::GetDataSlot(targetSchemeType)).Name
            << '_' << ::ToString(static_cast<const TDataDecimalType*>(dataType)->GetParams().second);
        return Invoke(str.Str().c_str(), type, args);
    }

    return Invoke(__func__, type, args);
}

TRuntimeNode TProgramBuilder::ToDecimal(TRuntimeNode data, ui8 precision, ui8 scale) {
    bool isOptional;
    auto dataType = UnpackOptionalData(data, isOptional);

    TType* decimal = TDataDecimalType::Create(precision, scale, Env);
    if (isOptional)
        decimal = TOptionalType::Create(decimal, Env);
    const std::array<TRuntimeNode, 1> args = {{ data }};

    if (dataType->GetSchemeType() == NUdf::TDataType<NUdf::TDecimal>::Id) {
        const auto& params = static_cast<const TDataDecimalType*>(dataType)->GetParams();
        if (precision - scale < params.first - params.second && scale != params.second) {
            return ToDecimal(ToDecimal(data, precision - scale + params.second, params.second), precision, scale);
        } else if (params.second < scale) {
            return Invoke("ScaleUp_" + ::ToString(scale - params.second), decimal, args);
        } else if (params.second > scale) {
            return Invoke("ScaleDown_" + ::ToString(params.second - scale), decimal, args);
        } else if (precision < params.first) {
            return Invoke("CheckBounds_" + ::ToString(precision), decimal, args);
        } else if (precision > params.first) {
            return Invoke("Plus", decimal, args);
        } else {
            return data;
        }
    } else {
        const auto digits = NUdf::GetDataTypeInfo(*dataType->GetDataSlot()).DecimalDigits;
        MKQL_ENSURE(digits, "Can't cast into Decimal.");
        if (digits <= precision && !scale)
            return Invoke(__func__, decimal, args);
        else
            return ToDecimal(ToDecimal(data, digits, 0), precision, scale);
    }
}

TRuntimeNode TProgramBuilder::ToIntegral(TRuntimeNode data, TType* type) {
    bool isOptional;
    auto dataType = UnpackOptionalData(data, isOptional);

    if (dataType->GetSchemeType() == NUdf::TDataType<NUdf::TDecimal>::Id) {
        const auto& params = static_cast<const TDataDecimalType*>(dataType)->GetParams();
        if (params.second)
            return ToIntegral(ToDecimal(data, params.first - params.second, 0), type);
    }

    const std::array<TRuntimeNode, 1> args = {{ data }};
    return Invoke(__func__, type, args);
}

TRuntimeNode TProgramBuilder::ListIf(TRuntimeNode predicate, TRuntimeNode item) {
    return If(predicate, NewList(item.GetStaticType(), {item}), NewEmptyList(item.GetStaticType()));
}

TRuntimeNode TProgramBuilder::AsList(TRuntimeNode item) {
    TListLiteralBuilder builder(Env, item.GetStaticType());
    builder.Add(item);
    return TRuntimeNode(builder.Build(), true);
}

TRuntimeNode TProgramBuilder::AsList(const TArrayRef<const TRuntimeNode>& items) {
    MKQL_ENSURE(!items.empty(), "required not empty list of items");
    TListLiteralBuilder builder(Env, items[0].GetStaticType());
    for (auto item : items) {
        builder.Add(item);
    }

    return TRuntimeNode(builder.Build(), true);
}

TRuntimeNode TProgramBuilder::MapJoinCore(TRuntimeNode flow, TRuntimeNode dict, EJoinKind joinKind,
    const TArrayRef<const ui32>& leftKeyColumns, const TArrayRef<const ui32>& leftRenames,
    const TArrayRef<const ui32>& rightRenames, TType* returnType) {

    MKQL_ENSURE(joinKind == EJoinKind::Inner || joinKind == EJoinKind::Left || joinKind == EJoinKind::LeftSemi || joinKind == EJoinKind::LeftOnly, "Unsupported join kind");
    MKQL_ENSURE(!leftKeyColumns.empty(), "At least one key column must be specified");
    MKQL_ENSURE(leftRenames.size() % 2U == 0U, "Expected even count");
    MKQL_ENSURE(rightRenames.size() % 2U == 0U, "Expected even count");

    TRuntimeNode::TList leftKeyColumnsNodes, leftRenamesNodes, rightRenamesNodes;

    leftKeyColumnsNodes.reserve(leftKeyColumns.size());
    std::transform(leftKeyColumns.cbegin(), leftKeyColumns.cend(), std::back_inserter(leftKeyColumnsNodes), [this](const ui32 idx) { return NewDataLiteral(idx); });

    leftRenamesNodes.reserve(leftRenames.size());
    std::transform(leftRenames.cbegin(), leftRenames.cend(), std::back_inserter(leftRenamesNodes), [this](const ui32 idx) { return NewDataLiteral(idx); });

    rightRenamesNodes.reserve(rightRenames.size());
    std::transform(rightRenames.cbegin(), rightRenames.cend(), std::back_inserter(rightRenamesNodes), [this](const ui32 idx) { return NewDataLiteral(idx); });

    TCallableBuilder callableBuilder(Env, __func__, returnType);
    callableBuilder.Add(flow);
    callableBuilder.Add(dict);
    callableBuilder.Add(NewDataLiteral((ui32)joinKind));
    callableBuilder.Add(NewTuple(leftKeyColumnsNodes));
    callableBuilder.Add(NewTuple(leftRenamesNodes));
    callableBuilder.Add(NewTuple(rightRenamesNodes));

    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::CommonJoinCore(TRuntimeNode flow, EJoinKind joinKind,
    const TArrayRef<const ui32>& leftColumns, const TArrayRef<const ui32>& rightColumns,
    const TArrayRef<const ui32>& requiredColumns, const TArrayRef<const ui32>& keyColumns,
    ui64 memLimit, std::optional<ui32> sortedTableOrder,
    EAnyJoinSettings anyJoinSettings, const ui32 tableIndexField, TType* returnType) {

    if constexpr (RuntimeVersion < 17U) {
        THROW yexception() << "Runtime version (" << RuntimeVersion << ") too old for " << __func__;
    }

    MKQL_ENSURE(leftColumns.size() % 2U == 0U, "Expected even count");
    MKQL_ENSURE(rightColumns.size() % 2U == 0U, "Expected even count");

    TRuntimeNode::TList leftInputColumnsNodes, rightInputColumnsNodes, requiredColumnsNodes,
        leftOutputColumnsNodes, rightOutputColumnsNodes, keyColumnsNodes;

    bool s = false;
    for (const auto idx :  leftColumns) {
        ((s = !s) ? leftInputColumnsNodes : leftOutputColumnsNodes).emplace_back(NewDataLiteral(idx));
    }

    for (const auto idx :  rightColumns) {
        ((s = !s) ? rightInputColumnsNodes : rightOutputColumnsNodes).emplace_back(NewDataLiteral(idx));
    }

    const std::unordered_set<ui32> requiredIndices(requiredColumns.cbegin(), requiredColumns.cend());
    MKQL_ENSURE(requiredIndices.size() == requiredColumns.size(), "Duplication of requred columns.");

    requiredColumnsNodes.reserve(requiredColumns.size());
    std::transform(requiredColumns.cbegin(), requiredColumns.cend(), std::back_inserter(requiredColumnsNodes),
        std::bind(&TProgramBuilder::NewDataLiteral<ui32>, this, std::placeholders::_1));

    const std::unordered_set<ui32> keyIndices(keyColumns.cbegin(), keyColumns.cend());
    MKQL_ENSURE(keyIndices.size() == keyColumns.size(), "Duplication of key columns.");

    keyColumnsNodes.reserve(keyColumns.size());
    std::transform(keyColumns.cbegin(), keyColumns.cend(), std::back_inserter(keyColumnsNodes),
        std::bind(&TProgramBuilder::NewDataLiteral<ui32>, this, std::placeholders::_1));

    TCallableBuilder callableBuilder(Env, __func__, returnType);
    callableBuilder.Add(flow);
    callableBuilder.Add(NewDataLiteral((ui32)joinKind));
    callableBuilder.Add(NewTuple(leftInputColumnsNodes));
    callableBuilder.Add(NewTuple(rightInputColumnsNodes));
    callableBuilder.Add(NewTuple(requiredColumnsNodes));
    callableBuilder.Add(NewTuple(leftOutputColumnsNodes));
    callableBuilder.Add(NewTuple(rightOutputColumnsNodes));
    callableBuilder.Add(NewTuple(keyColumnsNodes));
    callableBuilder.Add(NewDataLiteral(memLimit));
    callableBuilder.Add(sortedTableOrder ? NewDataLiteral(*sortedTableOrder) : NewVoid());
    callableBuilder.Add(NewDataLiteral((ui32)anyJoinSettings));
    callableBuilder.Add(NewDataLiteral(tableIndexField));
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::WideCombiner(TRuntimeNode flow, i64 memLimit, const TWideLambda& extractor, const TBinaryWideLambda& init, const TTernaryWideLambda& update, const TBinaryWideLambda& finish) {
    if constexpr (RuntimeVersion < 18U) {
        THROW yexception() << "Runtime version (" << RuntimeVersion << ") too old for " << __func__;
    }

    if (memLimit < 0) {
        if constexpr (RuntimeVersion < 46U) {
            THROW yexception() << "Runtime version (" << RuntimeVersion << ") too old for " << __func__ << " with limit " << memLimit;
        }
    }

    const auto wideComponents = GetWideComponents(AS_TYPE(TFlowType, flow.GetStaticType()));

    TRuntimeNode::TList itemArgs;
    itemArgs.reserve(wideComponents.size());

    auto i = 0U;
    std::generate_n(std::back_inserter(itemArgs), wideComponents.size(), [&](){ return Arg(wideComponents[i++]); });

    const auto keys = extractor(itemArgs);

    TRuntimeNode::TList keyArgs;
    keyArgs.reserve(keys.size());
    std::transform(keys.cbegin(), keys.cend(), std::back_inserter(keyArgs), [&](TRuntimeNode key){ return Arg(key.GetStaticType()); }  );

    const auto first = init(keyArgs, itemArgs);

    TRuntimeNode::TList stateArgs;
    stateArgs.reserve(first.size());
    std::transform(first.cbegin(), first.cend(), std::back_inserter(stateArgs), [&](TRuntimeNode state){ return Arg(state.GetStaticType()); }  );

    const auto next = update(keyArgs, itemArgs, stateArgs);
    MKQL_ENSURE(next.size() == first.size(), "Mismatch init and update state size.");

    TRuntimeNode::TList finishKeyArgs;
    finishKeyArgs.reserve(keys.size());
    std::transform(keys.cbegin(), keys.cend(), std::back_inserter(finishKeyArgs), [&](TRuntimeNode key){ return Arg(key.GetStaticType()); }  );

    TRuntimeNode::TList finishStateArgs;
    finishStateArgs.reserve(next.size());
    std::transform(next.cbegin(), next.cend(), std::back_inserter(finishStateArgs), [&](TRuntimeNode state){ return Arg(state.GetStaticType()); }  );

    const auto output = finish(finishKeyArgs, finishStateArgs);

    std::vector<TType*> tupleItems;
    tupleItems.reserve(output.size());
    std::transform(output.cbegin(), output.cend(), std::back_inserter(tupleItems), std::bind(&TRuntimeNode::GetStaticType, std::placeholders::_1));

    TCallableBuilder callableBuilder(Env, __func__, NewFlowType(NewMultiType(tupleItems)));
    callableBuilder.Add(flow);
    if constexpr (RuntimeVersion < 46U)
        callableBuilder.Add(NewDataLiteral(ui64(memLimit)));
    else
        callableBuilder.Add(NewDataLiteral(memLimit));
    callableBuilder.Add(NewDataLiteral(ui32(keyArgs.size())));
    callableBuilder.Add(NewDataLiteral(ui32(stateArgs.size())));
    std::for_each(itemArgs.cbegin(), itemArgs.cend(), std::bind(&TCallableBuilder::Add, std::ref(callableBuilder), std::placeholders::_1));
    std::for_each(keys.cbegin(), keys.cend(), std::bind(&TCallableBuilder::Add, std::ref(callableBuilder), std::placeholders::_1));
    std::for_each(keyArgs.cbegin(), keyArgs.cend(), std::bind(&TCallableBuilder::Add, std::ref(callableBuilder), std::placeholders::_1));
    std::for_each(first.cbegin(), first.cend(), std::bind(&TCallableBuilder::Add, std::ref(callableBuilder), std::placeholders::_1));
    std::for_each(stateArgs.cbegin(), stateArgs.cend(), std::bind(&TCallableBuilder::Add, std::ref(callableBuilder), std::placeholders::_1));
    std::for_each(next.cbegin(), next.cend(), std::bind(&TCallableBuilder::Add, std::ref(callableBuilder), std::placeholders::_1));
    std::for_each(finishKeyArgs.cbegin(), finishKeyArgs.cend(), std::bind(&TCallableBuilder::Add, std::ref(callableBuilder), std::placeholders::_1));
    std::for_each(finishStateArgs.cbegin(), finishStateArgs.cend(), std::bind(&TCallableBuilder::Add, std::ref(callableBuilder), std::placeholders::_1));
    std::for_each(output.cbegin(), output.cend(), std::bind(&TCallableBuilder::Add, std::ref(callableBuilder), std::placeholders::_1));
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::WideLastCombinerCommon(const TStringBuf& funcName, TRuntimeNode flow, const TWideLambda& extractor, const TBinaryWideLambda& init, const TTernaryWideLambda& update, const TBinaryWideLambda& finish) {
    const auto wideComponents = GetWideComponents(AS_TYPE(TFlowType, flow.GetStaticType()));

    TRuntimeNode::TList itemArgs;
    itemArgs.reserve(wideComponents.size());

    auto i = 0U;
    std::generate_n(std::back_inserter(itemArgs), wideComponents.size(), [&](){ return Arg(wideComponents[i++]); });

    const auto keys = extractor(itemArgs);

    TRuntimeNode::TList keyArgs;
    keyArgs.reserve(keys.size());
    std::transform(keys.cbegin(), keys.cend(), std::back_inserter(keyArgs), [&](TRuntimeNode key){ return Arg(key.GetStaticType()); }  );

    const auto first = init(keyArgs, itemArgs);

    TRuntimeNode::TList stateArgs;
    stateArgs.reserve(first.size());
    std::transform(first.cbegin(), first.cend(), std::back_inserter(stateArgs), [&](TRuntimeNode state){ return Arg(state.GetStaticType()); }  );

    const auto next = update(keyArgs, itemArgs, stateArgs);
    MKQL_ENSURE(next.size() == first.size(), "Mismatch init and update state size.");

    TRuntimeNode::TList finishKeyArgs;
    finishKeyArgs.reserve(keys.size());
    std::transform(keys.cbegin(), keys.cend(), std::back_inserter(finishKeyArgs), [&](TRuntimeNode key){ return Arg(key.GetStaticType()); }  );

    TRuntimeNode::TList finishStateArgs;
    finishStateArgs.reserve(next.size());
    std::transform(next.cbegin(), next.cend(), std::back_inserter(finishStateArgs), [&](TRuntimeNode state){ return Arg(state.GetStaticType()); }  );

    const auto output = finish(finishKeyArgs, finishStateArgs);

    std::vector<TType*> tupleItems;
    tupleItems.reserve(output.size());
    std::transform(output.cbegin(), output.cend(), std::back_inserter(tupleItems), std::bind(&TRuntimeNode::GetStaticType, std::placeholders::_1));

    TCallableBuilder callableBuilder(Env, funcName, NewFlowType(NewMultiType(tupleItems)));
    callableBuilder.Add(flow);
    callableBuilder.Add(NewDataLiteral(ui32(keyArgs.size())));
    callableBuilder.Add(NewDataLiteral(ui32(stateArgs.size())));
    std::for_each(itemArgs.cbegin(), itemArgs.cend(), std::bind(&TCallableBuilder::Add, std::ref(callableBuilder), std::placeholders::_1));
    std::for_each(keys.cbegin(), keys.cend(), std::bind(&TCallableBuilder::Add, std::ref(callableBuilder), std::placeholders::_1));
    std::for_each(keyArgs.cbegin(), keyArgs.cend(), std::bind(&TCallableBuilder::Add, std::ref(callableBuilder), std::placeholders::_1));
    std::for_each(first.cbegin(), first.cend(), std::bind(&TCallableBuilder::Add, std::ref(callableBuilder), std::placeholders::_1));
    std::for_each(stateArgs.cbegin(), stateArgs.cend(), std::bind(&TCallableBuilder::Add, std::ref(callableBuilder), std::placeholders::_1));
    std::for_each(next.cbegin(), next.cend(), std::bind(&TCallableBuilder::Add, std::ref(callableBuilder), std::placeholders::_1));
    std::for_each(finishKeyArgs.cbegin(), finishKeyArgs.cend(), std::bind(&TCallableBuilder::Add, std::ref(callableBuilder), std::placeholders::_1));
    std::for_each(finishStateArgs.cbegin(), finishStateArgs.cend(), std::bind(&TCallableBuilder::Add, std::ref(callableBuilder), std::placeholders::_1));
    std::for_each(output.cbegin(), output.cend(), std::bind(&TCallableBuilder::Add, std::ref(callableBuilder), std::placeholders::_1));
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::WideLastCombiner(TRuntimeNode flow, const TWideLambda& extractor, const TBinaryWideLambda& init, const TTernaryWideLambda& update, const TBinaryWideLambda& finish) {
    if constexpr (RuntimeVersion < 29U) {
        THROW yexception() << "Runtime version (" << RuntimeVersion << ") too old for " << __func__;
    }

    return WideLastCombinerCommon(__func__, flow, extractor, init, update, finish);
}

TRuntimeNode TProgramBuilder::WideLastCombinerCommonWithSpilling(const TStringBuf& funcName, TRuntimeNode flow, const TWideLambda& extractor, const TBinaryWideLambda& init, const TTernaryWideLambda& update, const TBinaryWideLambda& finish, const TBinaryWideLambda& serialize, const TBinaryWideLambda& deserialize) {
    const auto wideComponents = GetWideComponents(AS_TYPE(TFlowType, flow.GetStaticType()));

    TRuntimeNode::TList itemArgs;
    itemArgs.reserve(wideComponents.size());

    auto i = 0U;
    std::generate_n(std::back_inserter(itemArgs), wideComponents.size(), [&](){ return Arg(wideComponents[i++]); });

    const auto keys = extractor(itemArgs);

    TRuntimeNode::TList keyArgs;
    keyArgs.reserve(keys.size());
    std::transform(keys.cbegin(), keys.cend(), std::back_inserter(keyArgs), [&](TRuntimeNode key){ return Arg(key.GetStaticType()); }  );

    const auto first = init(keyArgs, itemArgs);

    TRuntimeNode::TList stateArgs;
    stateArgs.reserve(first.size());
    std::transform(first.cbegin(), first.cend(), std::back_inserter(stateArgs), [&](TRuntimeNode state){ return Arg(state.GetStaticType()); }  );

    const auto next = update(keyArgs, itemArgs, stateArgs);
    MKQL_ENSURE(next.size() == first.size(), "Mismatch init and update state size.");

    TRuntimeNode::TList finishKeyArgs;
    finishKeyArgs.reserve(keys.size());
    std::transform(keys.cbegin(), keys.cend(), std::back_inserter(finishKeyArgs), [&](TRuntimeNode key){ return Arg(key.GetStaticType()); }  );

    TRuntimeNode::TList finishStateArgs;
    finishStateArgs.reserve(next.size());
    std::transform(next.cbegin(), next.cend(), std::back_inserter(finishStateArgs), [&](TRuntimeNode state){ return Arg(state.GetStaticType()); }  );

    const auto output = finish(finishKeyArgs, finishStateArgs);

    TRuntimeNode::TList serializeKeyArgs;
    serializeKeyArgs.reserve(keys.size());
    std::transform(keys.cbegin(), keys.cend(), std::back_inserter(serializeKeyArgs), [&](TRuntimeNode key){ return Arg(key.GetStaticType()); }  );

    TRuntimeNode::TList serializeStateArgs;
    serializeStateArgs.reserve(next.size());
    std::transform(next.cbegin(), next.cend(), std::back_inserter(serializeStateArgs), [&](TRuntimeNode state){ return Arg(state.GetStaticType()); }  );

    const auto serializeOutput = serialize(serializeKeyArgs, serializeStateArgs);

    TRuntimeNode::TList deserializeKeyArgs;
    deserializeKeyArgs.reserve(keys.size());
    std::transform(keys.cbegin(), keys.cend(), std::back_inserter(deserializeKeyArgs), [&](TRuntimeNode key){ return Arg(key.GetStaticType()); }  );

    TRuntimeNode::TList deserializeStateArgs;
    deserializeStateArgs.reserve(serializeOutput.size());
    std::transform(serializeOutput.cbegin(), serializeOutput.cend(), std::back_inserter(deserializeStateArgs),
        [&](TRuntimeNode arg){
            TType* actualType = arg.GetStaticType();
            if (actualType->IsOptional()) {
                actualType = AS_TYPE(TOptionalType, actualType)->GetItemType();
            }
            return Arg(actualType);
        }
    );

    const auto deserializeOutput = deserialize(deserializeKeyArgs, deserializeStateArgs);

    std::vector<TType*> tupleItems;
    tupleItems.reserve(output.size());
    std::transform(output.cbegin(), output.cend(), std::back_inserter(tupleItems), std::bind(&TRuntimeNode::GetStaticType, std::placeholders::_1));

    TCallableBuilder callableBuilder(Env, funcName, NewFlowType(NewMultiType(tupleItems)));
    callableBuilder.Add(flow);
    callableBuilder.Add(NewDataLiteral(ui32(keyArgs.size())));
    callableBuilder.Add(NewDataLiteral(ui32(stateArgs.size())));
    std::for_each(itemArgs.cbegin(), itemArgs.cend(), std::bind(&TCallableBuilder::Add, std::ref(callableBuilder), std::placeholders::_1));
    std::for_each(keys.cbegin(), keys.cend(), std::bind(&TCallableBuilder::Add, std::ref(callableBuilder), std::placeholders::_1));
    std::for_each(keyArgs.cbegin(), keyArgs.cend(), std::bind(&TCallableBuilder::Add, std::ref(callableBuilder), std::placeholders::_1));
    std::for_each(first.cbegin(), first.cend(), std::bind(&TCallableBuilder::Add, std::ref(callableBuilder), std::placeholders::_1));
    std::for_each(stateArgs.cbegin(), stateArgs.cend(), std::bind(&TCallableBuilder::Add, std::ref(callableBuilder), std::placeholders::_1));
    std::for_each(next.cbegin(), next.cend(), std::bind(&TCallableBuilder::Add, std::ref(callableBuilder), std::placeholders::_1));
    std::for_each(finishKeyArgs.cbegin(), finishKeyArgs.cend(), std::bind(&TCallableBuilder::Add, std::ref(callableBuilder), std::placeholders::_1));
    std::for_each(finishStateArgs.cbegin(), finishStateArgs.cend(), std::bind(&TCallableBuilder::Add, std::ref(callableBuilder), std::placeholders::_1));
    std::for_each(output.cbegin(), output.cend(), std::bind(&TCallableBuilder::Add, std::ref(callableBuilder), std::placeholders::_1));
    std::for_each(serializeKeyArgs.cbegin(), serializeKeyArgs.cend(), std::bind(&TCallableBuilder::Add, std::ref(callableBuilder), std::placeholders::_1));
    std::for_each(serializeStateArgs.cbegin(), serializeStateArgs.cend(), std::bind(&TCallableBuilder::Add, std::ref(callableBuilder), std::placeholders::_1));
    std::for_each(serializeOutput.cbegin(), serializeOutput.cend(), std::bind(&TCallableBuilder::Add, std::ref(callableBuilder), std::placeholders::_1));
    std::for_each(deserializeKeyArgs.cbegin(), deserializeKeyArgs.cend(), std::bind(&TCallableBuilder::Add, std::ref(callableBuilder), std::placeholders::_1));
    std::for_each(deserializeStateArgs.cbegin(), deserializeStateArgs.cend(), std::bind(&TCallableBuilder::Add, std::ref(callableBuilder), std::placeholders::_1));
    std::for_each(deserializeOutput.cbegin(), deserializeOutput.cend(), std::bind(&TCallableBuilder::Add, std::ref(callableBuilder), std::placeholders::_1));
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::WideLastCombinerWithSpilling(TRuntimeNode flow, const TWideLambda& extractor, const TBinaryWideLambda& init, const TTernaryWideLambda& update, const TBinaryWideLambda& finish, const TBinaryWideLambda& serialize, const TBinaryWideLambda& deserialize) {
    if constexpr (RuntimeVersion < 51U) {
        THROW yexception() << "Runtime version (" << RuntimeVersion << ") too old for " << __func__;
    }

    return WideLastCombinerCommonWithSpilling(__func__, flow, extractor, init, update, finish, serialize, deserialize);
}

TRuntimeNode TProgramBuilder::WideCondense1(TRuntimeNode flow, const TWideLambda& init, const TWideSwitchLambda& switcher, const TBinaryWideLambda& update, bool useCtx) {
    if constexpr (RuntimeVersion < 18U) {
        THROW yexception() << "Runtime version (" << RuntimeVersion << ") too old for " << __func__;
    }

    const auto wideComponents = GetWideComponents(AS_TYPE(TFlowType, flow.GetStaticType()));

    TRuntimeNode::TList itemArgs;
    itemArgs.reserve(wideComponents.size());

    auto i = 0U;
    std::generate_n(std::back_inserter(itemArgs), wideComponents.size(), [&](){ return Arg(wideComponents[i++]); });

    const auto first = init(itemArgs);

    TRuntimeNode::TList stateArgs;
    stateArgs.reserve(first.size());
    std::transform(first.cbegin(), first.cend(), std::back_inserter(stateArgs), [&](TRuntimeNode state){ return Arg(state.GetStaticType()); }  );

    const auto chop = switcher(itemArgs, stateArgs);

    const auto next = update(itemArgs, stateArgs);
    MKQL_ENSURE(next.size() == first.size(), "Mismatch init and update state size.");

    std::vector<TType*> tupleItems;
    tupleItems.reserve(next.size());
    std::transform(next.cbegin(), next.cend(), std::back_inserter(tupleItems), std::bind(&TRuntimeNode::GetStaticType, std::placeholders::_1));

    TCallableBuilder callableBuilder(Env, __func__, NewFlowType(NewMultiType(tupleItems)));
    callableBuilder.Add(flow);
    std::for_each(itemArgs.cbegin(), itemArgs.cend(), std::bind(&TCallableBuilder::Add, std::ref(callableBuilder), std::placeholders::_1));
    std::for_each(first.cbegin(), first.cend(), std::bind(&TCallableBuilder::Add, std::ref(callableBuilder), std::placeholders::_1));
    std::for_each(stateArgs.cbegin(), stateArgs.cend(), std::bind(&TCallableBuilder::Add, std::ref(callableBuilder), std::placeholders::_1));
    callableBuilder.Add(chop);
    std::for_each(next.cbegin(), next.cend(), std::bind(&TCallableBuilder::Add, std::ref(callableBuilder), std::placeholders::_1));
    if (useCtx) {
        MKQL_ENSURE(RuntimeVersion >= 30U, "Too old runtime version");
        callableBuilder.Add(NewDataLiteral<bool>(useCtx));
    }

    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::CombineCore(TRuntimeNode stream,
    const TUnaryLambda& keyExtractor,
    const TBinaryLambda& init,
    const TTernaryLambda& update,
    const TBinaryLambda& finish,
    ui64 memLimit)
{
    if constexpr (RuntimeVersion < 3U) {
        THROW yexception() << "Runtime version (" << RuntimeVersion << ") too old for " << __func__;
    }

    const bool isStream = stream.GetStaticType()->IsStream();
    const auto itemType = isStream ? AS_TYPE(TStreamType, stream)->GetItemType() : AS_TYPE(TFlowType, stream)->GetItemType();

    const auto itemArg = Arg(itemType);

    const auto key = keyExtractor(itemArg);
    const auto keyType = key.GetStaticType();

    const auto keyArg = Arg(keyType);

    const auto stateInit = init(keyArg, itemArg);
    const auto stateType = stateInit.GetStaticType();

    const auto stateArg = Arg(stateType);

    const auto stateUpdate = update(keyArg, itemArg, stateArg);
    const auto finishItem = finish(keyArg, stateArg);

    const auto finishType = finishItem.GetStaticType();
    MKQL_ENSURE(finishType->IsList() || finishType->IsStream() || finishType->IsOptional(), "Expected list, stream or optional");

    TType* retItemType = nullptr;
    if (finishType->IsOptional()) {
        retItemType = AS_TYPE(TOptionalType, finishType)->GetItemType();
    } else if (finishType->IsList()) {
        retItemType = AS_TYPE(TListType, finishType)->GetItemType();
    } else if (finishType->IsStream()) {
        retItemType = AS_TYPE(TStreamType, finishType)->GetItemType();
    }

    const auto resultStreamType = isStream ? NewStreamType(retItemType) : NewFlowType(retItemType);
    TCallableBuilder callableBuilder(Env, __func__, resultStreamType);
    callableBuilder.Add(stream);
    callableBuilder.Add(itemArg);
    callableBuilder.Add(key);
    callableBuilder.Add(keyArg);
    callableBuilder.Add(stateInit);
    callableBuilder.Add(stateArg);
    callableBuilder.Add(stateUpdate);
    callableBuilder.Add(finishItem);
    callableBuilder.Add(NewDataLiteral(memLimit));

    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::CombineCoreWithSpilling(TRuntimeNode stream,
    const TUnaryLambda& keyExtractor,
    const TBinaryLambda& init,
    const TTernaryLambda& update,
    const TBinaryLambda& finish,
    const TUnaryLambda& load,
    ui64 memLimit)
{
    if constexpr (RuntimeVersion < 51U) {
        THROW yexception() << "Runtime version (" << RuntimeVersion << ") too old for " << __func__;
    }

    const bool isStream = stream.GetStaticType()->IsStream();
    const auto itemType = isStream ? AS_TYPE(TStreamType, stream)->GetItemType() : AS_TYPE(TFlowType, stream)->GetItemType();

    const auto itemArg = Arg(itemType);

    const auto key = keyExtractor(itemArg);
    const auto keyType = key.GetStaticType();

    const auto keyArg = Arg(keyType);

    const auto stateInit = init(keyArg, itemArg);
    const auto stateType = stateInit.GetStaticType();

    const auto stateArg = Arg(stateType);

    const auto stateUpdate = update(keyArg, itemArg, stateArg);
    const auto finishItem = finish(keyArg, stateArg);

    const auto finishType = finishItem.GetStaticType();
    MKQL_ENSURE(finishType->IsList() || finishType->IsStream() || finishType->IsOptional(), "Expected list, stream or optional");

    const auto stateLoad = load(stateArg);

    TType* retItemType = nullptr;
    if (finishType->IsOptional()) {
        retItemType = AS_TYPE(TOptionalType, finishType)->GetItemType();
    } else if (finishType->IsList()) {
        retItemType = AS_TYPE(TListType, finishType)->GetItemType();
    } else if (finishType->IsStream()) {
        retItemType = AS_TYPE(TStreamType, finishType)->GetItemType();
    }

    const auto resultStreamType = isStream ? NewStreamType(retItemType) : NewFlowType(retItemType);
    TCallableBuilder callableBuilder(Env, __func__, resultStreamType);
    callableBuilder.Add(stream);
    callableBuilder.Add(itemArg);
    callableBuilder.Add(key);
    callableBuilder.Add(keyArg);
    callableBuilder.Add(stateInit);
    callableBuilder.Add(stateArg);
    callableBuilder.Add(stateUpdate);
    callableBuilder.Add(finishItem);
    callableBuilder.Add(stateLoad);
    callableBuilder.Add(NewDataLiteral(memLimit));

    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::GroupingCore(TRuntimeNode stream,
    const TBinaryLambda& groupSwitch,
    const TUnaryLambda& keyExtractor,
    const TUnaryLambda& handler)
{
    if (handler && RuntimeVersion < 20U) {
        THROW yexception() << "Runtime version (" << RuntimeVersion << ") too old for " << __func__ << " with handler";
    }

    auto itemType = AS_TYPE(TStreamType, stream)->GetItemType();

    TRuntimeNode keyExtractorItemArg = Arg(itemType);
    TRuntimeNode keyExtractorResult = keyExtractor(keyExtractorItemArg);

    TRuntimeNode groupSwitchKeyArg = Arg(keyExtractorResult.GetStaticType());
    TRuntimeNode groupSwitchItemArg = Arg(itemType);

    TRuntimeNode groupSwitchResult = groupSwitch(groupSwitchKeyArg, groupSwitchItemArg);
    MKQL_ENSURE(AS_TYPE(TDataType, groupSwitchResult)->GetSchemeType() == NUdf::TDataType<bool>::Id,
        "Expected bool type");

    TRuntimeNode handlerItemArg;
    TRuntimeNode handlerResult;

    if (handler) {
        handlerItemArg = Arg(itemType);
        handlerResult = handler(handlerItemArg);
        itemType = handlerResult.GetStaticType();
    }

    const std::array<TType*, 2U> tupleItems = {{ keyExtractorResult.GetStaticType(), NewStreamType(itemType) }};
    const auto finishType = NewStreamType(NewTupleType(tupleItems));

    TCallableBuilder callableBuilder(Env, __func__, finishType);
    callableBuilder.Add(stream);
    callableBuilder.Add(keyExtractorResult);
    callableBuilder.Add(groupSwitchResult);
    callableBuilder.Add(keyExtractorItemArg);
    callableBuilder.Add(groupSwitchKeyArg);
    callableBuilder.Add(groupSwitchItemArg);
    if (handler) {
        callableBuilder.Add(handlerResult);
        callableBuilder.Add(handlerItemArg);
    }

    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::Chopper(TRuntimeNode flow, const TUnaryLambda& keyExtractor, const TBinaryLambda& groupSwitch, const TBinaryLambda& groupHandler) {
    const auto flowType = flow.GetStaticType();
    MKQL_ENSURE(flowType->IsFlow() || flowType->IsStream(), "Expected flow or stream.");


    if constexpr (RuntimeVersion < 9U) {
        return FlatMap(GroupingCore(flow, groupSwitch, keyExtractor),
            [&](TRuntimeNode item) -> TRuntimeNode { return groupHandler(Nth(item, 0U), Nth(item, 1U)); }
        );
    }

    const bool isStream = flowType->IsStream();
    const auto itemType = isStream ? AS_TYPE(TStreamType, flow)->GetItemType() : AS_TYPE(TFlowType, flow)->GetItemType();

    const auto itemArg = Arg(itemType);
    const auto keyExtractorResult = keyExtractor(itemArg);
    const auto keyArg = Arg(keyExtractorResult.GetStaticType());
    const auto groupSwitchResult = groupSwitch(keyArg, itemArg);

    const auto input = Arg(flowType);
    const auto output = groupHandler(keyArg, input);

    TCallableBuilder callableBuilder(Env, __func__, output.GetStaticType());
    callableBuilder.Add(flow);

    callableBuilder.Add(itemArg);
    callableBuilder.Add(keyExtractorResult);
    callableBuilder.Add(keyArg);
    callableBuilder.Add(groupSwitchResult);

    callableBuilder.Add(input);
    callableBuilder.Add(output);

    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::WideChopper(TRuntimeNode flow, const TWideLambda& extractor, const TWideSwitchLambda& groupSwitch,
    const std::function<TRuntimeNode (TRuntimeNode::TList, TRuntimeNode)>& groupHandler
) {
    if constexpr (RuntimeVersion < 18U) {
        THROW yexception() << "Runtime version (" << RuntimeVersion << ") too old for " << __func__;
    }

    const auto wideComponents = GetWideComponents(AS_TYPE(TFlowType, flow.GetStaticType()));

    TRuntimeNode::TList itemArgs, keyArgs;
    itemArgs.reserve(wideComponents.size());

    auto i = 0U;
    std::generate_n(std::back_inserter(itemArgs), wideComponents.size(), [&](){ return Arg(wideComponents[i++]); });

    const auto keys = extractor(itemArgs);

    keyArgs.reserve(keys.size());
    std::transform(keys.cbegin(), keys.cend(), std::back_inserter(keyArgs), [&](TRuntimeNode key){ return Arg(key.GetStaticType()); }  );

    const auto groupSwitchResult = groupSwitch(keyArgs, itemArgs);

    const auto input = WideFlowArg(flow.GetStaticType());
    const auto output = groupHandler(keyArgs, input);

    TCallableBuilder callableBuilder(Env, __func__, output.GetStaticType());
    callableBuilder.Add(flow);
    std::for_each(itemArgs.cbegin(), itemArgs.cend(), std::bind(&TCallableBuilder::Add, std::ref(callableBuilder), std::placeholders::_1));
    std::for_each(keys.cbegin(), keys.cend(), std::bind(&TCallableBuilder::Add, std::ref(callableBuilder), std::placeholders::_1));
    std::for_each(keyArgs.cbegin(), keyArgs.cend(), std::bind(&TCallableBuilder::Add, std::ref(callableBuilder), std::placeholders::_1));
    callableBuilder.Add(groupSwitchResult);
    callableBuilder.Add(input);
    callableBuilder.Add(output);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::HoppingCore(TRuntimeNode list,
    const TUnaryLambda& timeExtractor,
    const TUnaryLambda& init,
    const TBinaryLambda& update,
    const TUnaryLambda& save,
    const TUnaryLambda& load,
    const TBinaryLambda& merge,
    const TBinaryLambda& finish,
    TRuntimeNode hop, TRuntimeNode interval, TRuntimeNode delay)
{
    auto streamType = AS_TYPE(TStreamType, list);
    auto itemType = AS_TYPE(TStructType, streamType->GetItemType());
    auto timestampType = TOptionalType::Create(TDataType::Create(NUdf::TDataType<NUdf::TTimestamp>::Id, Env), Env);

    TRuntimeNode itemArg = Arg(itemType);

    auto outTime = timeExtractor(itemArg);
    auto outStateInit = init(itemArg);

    auto stateType = outStateInit.GetStaticType();
    TRuntimeNode stateArg = Arg(stateType);

    auto outStateUpdate = update(itemArg, stateArg);

    auto hasSaveLoad = (bool)save;
    TRuntimeNode saveArg, outSave, loadArg, outLoad;
    if (hasSaveLoad) {
        saveArg = Arg(stateType);
        outSave = save(saveArg);

        loadArg = Arg(outSave.GetStaticType());
        outLoad = load(loadArg);

        MKQL_ENSURE(outLoad.GetStaticType()->IsSameType(*stateType), "Loaded type is changed by the load handler");
    } else {
        saveArg = outSave = loadArg = outLoad = NewVoid();
    }

    TRuntimeNode state2Arg = Arg(stateType);
    TRuntimeNode timeArg = Arg(timestampType);

    auto outStateMerge = merge(stateArg, state2Arg);
    auto outItemFinish = finish(stateArg, timeArg);

    auto finishType = outItemFinish.GetStaticType();
    MKQL_ENSURE(finishType->IsStruct(), "Expected struct type as finish lambda output");

    auto resultType = TStreamType::Create(outItemFinish.GetStaticType(), Env);

    TCallableBuilder callableBuilder(Env, __func__, resultType);
    callableBuilder.Add(list);
    callableBuilder.Add(itemArg);
    callableBuilder.Add(stateArg);
    callableBuilder.Add(state2Arg);
    callableBuilder.Add(timeArg);
    callableBuilder.Add(saveArg);
    callableBuilder.Add(loadArg);
    callableBuilder.Add(outTime);
    callableBuilder.Add(outStateInit);
    callableBuilder.Add(outStateUpdate);
    callableBuilder.Add(outSave);
    callableBuilder.Add(outLoad);
    callableBuilder.Add(outStateMerge);
    callableBuilder.Add(outItemFinish);
    callableBuilder.Add(hop);
    callableBuilder.Add(interval);
    callableBuilder.Add(delay);

    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::MultiHoppingCore(TRuntimeNode list,
    const TUnaryLambda& keyExtractor,
    const TUnaryLambda& timeExtractor,
    const TUnaryLambda& init,
    const TBinaryLambda& update,
    const TUnaryLambda& save,
    const TUnaryLambda& load,
    const TBinaryLambda& merge,
    const TTernaryLambda& finish,
    TRuntimeNode hop, TRuntimeNode interval, TRuntimeNode delay,
    TRuntimeNode dataWatermarks, TRuntimeNode watermarksMode)
{
    if constexpr (RuntimeVersion < 22U) {
        THROW yexception() << "Runtime version (" << RuntimeVersion << ") too old for " << __func__;
    }

    auto streamType = AS_TYPE(TStreamType, list);
    auto itemType = AS_TYPE(TStructType, streamType->GetItemType());
    auto timestampType = TOptionalType::Create(TDataType::Create(NUdf::TDataType<NUdf::TTimestamp>::Id, Env), Env);

    TRuntimeNode itemArg = Arg(itemType);

    auto keyExtract = keyExtractor(itemArg);
    auto keyType = keyExtract.GetStaticType();
    TRuntimeNode keyArg = Arg(keyType);

    auto outTime = timeExtractor(itemArg);
    auto outStateInit = init(itemArg);

    auto stateType = outStateInit.GetStaticType();
    TRuntimeNode stateArg = Arg(stateType);

    auto outStateUpdate = update(itemArg, stateArg);

    auto hasSaveLoad = (bool)save;
    TRuntimeNode saveArg, outSave, loadArg, outLoad;
    if (hasSaveLoad) {
        saveArg = Arg(stateType);
        outSave = save(saveArg);

        loadArg = Arg(outSave.GetStaticType());
        outLoad = load(loadArg);

        MKQL_ENSURE(outLoad.GetStaticType()->IsSameType(*stateType), "Loaded type is changed by the load handler");
    } else {
        saveArg = outSave = loadArg = outLoad = NewVoid();
    }

    TRuntimeNode state2Arg = Arg(stateType);
    TRuntimeNode timeArg = Arg(timestampType);

    auto outStateMerge = merge(stateArg, state2Arg);
    auto outItemFinish = finish(keyArg, stateArg, timeArg);

    auto finishType = outItemFinish.GetStaticType();
    MKQL_ENSURE(finishType->IsStruct(), "Expected struct type as finish lambda output");

    auto resultType = TStreamType::Create(outItemFinish.GetStaticType(), Env);

    TCallableBuilder callableBuilder(Env, __func__, resultType);
    callableBuilder.Add(list);
    callableBuilder.Add(itemArg);
    callableBuilder.Add(keyArg);
    callableBuilder.Add(stateArg);
    callableBuilder.Add(state2Arg);
    callableBuilder.Add(timeArg);
    callableBuilder.Add(saveArg);
    callableBuilder.Add(loadArg);
    callableBuilder.Add(keyExtract);
    callableBuilder.Add(outTime);
    callableBuilder.Add(outStateInit);
    callableBuilder.Add(outStateUpdate);
    callableBuilder.Add(outSave);
    callableBuilder.Add(outLoad);
    callableBuilder.Add(outStateMerge);
    callableBuilder.Add(outItemFinish);
    callableBuilder.Add(hop);
    callableBuilder.Add(interval);
    callableBuilder.Add(delay);
    callableBuilder.Add(dataWatermarks);
    callableBuilder.Add(watermarksMode);

    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::Default(TType* type) {
    bool isOptional;
    const auto targetType = UnpackOptionalData(type, isOptional);
    if (isOptional) {
        return NewOptional(Default(targetType));
    }

    const auto scheme = targetType->GetSchemeType();
    const auto value = scheme == NUdf::TDataType<NUdf::TUuid>::Id ?
        Env.NewStringValue("\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"sv) :
        scheme == NUdf::TDataType<NUdf::TDyNumber>::Id ? NUdf::TUnboxedValuePod::Embedded("\1") : NUdf::TUnboxedValuePod::Zero();
    return TRuntimeNode(TDataLiteral::Create(value, targetType, Env), true);
}

TRuntimeNode TProgramBuilder::Cast(TRuntimeNode arg, TType* type) {
    if (arg.GetStaticType()->IsSameType(*type)) {
        return arg;
    }

    bool isOptional;
    const auto targetType = UnpackOptionalData(type, isOptional);
    const auto sourceType = UnpackOptionalData(arg, isOptional);

    const auto sId = sourceType->GetSchemeType();
    const auto tId = targetType->GetSchemeType();

    if (sId == NUdf::TDataType<char*>::Id) {
        if (tId != NUdf::TDataType<char*>::Id) {
            return FromString(arg, type);
        } else {
            return arg;
        }
    }

    if (sId == NUdf::TDataType<NUdf::TUtf8>::Id) {
        if (tId != NUdf::TDataType<char*>::Id) {
            return FromString(arg, type);
        } else {
            return ToString(arg);
        }
    }

    if (tId == NUdf::TDataType<char*>::Id) {
        return ToString(arg);
    }

    if (tId == NUdf::TDataType<NUdf::TUtf8>::Id) {
        return ToString<true>(arg);
    }

    if (tId == NUdf::TDataType<NUdf::TDecimal>::Id) {
        const auto& params = static_cast<const TDataDecimalType*>(targetType)->GetParams();
        return ToDecimal(arg, params.first, params.second);
    }

    const auto options = NKikimr::NUdf::GetCastResult(*sourceType->GetDataSlot(), *targetType->GetDataSlot());
    MKQL_ENSURE((*options & NKikimr::NUdf::ECastOptions::Undefined) ||
        !(*options & NKikimr::NUdf::ECastOptions::Impossible),
        "Impossible to cast " <<  *static_cast<TType*>(sourceType) << " into " << *static_cast<TType*>(targetType));

    const bool useToIntegral = (*options & NKikimr::NUdf::ECastOptions::Undefined) ||
        (*options & NKikimr::NUdf::ECastOptions::MayFail);
    return useToIntegral ? ToIntegral(arg, type) : Convert(arg, type);
}

TRuntimeNode TProgramBuilder::RangeCreate(TRuntimeNode list) {
    MKQL_ENSURE(list.GetStaticType()->IsList(), "Expecting list");
    auto itemType = static_cast<TListType*>(list.GetStaticType())->GetItemType();
    MKQL_ENSURE(itemType->IsTuple(), "Expecting list of tuples");

    auto tupleType = static_cast<TTupleType*>(itemType);
    MKQL_ENSURE(tupleType->GetElementsCount() == 2,
        "Expecting list ot 2-element tuples, got: " << tupleType->GetElementsCount() << " elements");

    MKQL_ENSURE(tupleType->GetElementType(0)->IsSameType(*tupleType->GetElementType(1)),
        "Expecting list ot 2-element tuples of same type");

    MKQL_ENSURE(tupleType->GetElementType(0)->IsTuple(),
        "Expecting range boundary to be tuple");

    auto boundaryType = static_cast<TTupleType*>(tupleType->GetElementType(0));
    MKQL_ENSURE(boundaryType->GetElementsCount() >= 2,
        "Range boundary should have at least 2 components, got: " << boundaryType->GetElementsCount());

    auto lastComp = boundaryType->GetElementType(boundaryType->GetElementsCount() - 1);
    std::vector<TType*> outputComponents;
    for (ui32 i = 0; i < boundaryType->GetElementsCount() - 1; ++i) {
        outputComponents.push_back(lastComp);
        outputComponents.push_back(boundaryType->GetElementType(i));
    }
    outputComponents.push_back(lastComp);

    auto outputBoundary = TTupleType::Create(outputComponents.size(), &outputComponents.front(), Env);
    std::vector<TType*> outputRangeComps(2, outputBoundary);
    auto outputRange = TTupleType::Create(outputRangeComps.size(), &outputRangeComps.front(), Env);

    TCallableBuilder callableBuilder(Env, __func__, TListType::Create(outputRange, Env));
    callableBuilder.Add(list);

    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::RangeUnion(const TArrayRef<const TRuntimeNode>& lists) {
    return BuildRangeLogical(__func__, lists);
}

TRuntimeNode TProgramBuilder::RangeIntersect(const TArrayRef<const TRuntimeNode>& lists) {
    return BuildRangeLogical(__func__, lists);
}

TRuntimeNode TProgramBuilder::RangeMultiply(const TArrayRef<const TRuntimeNode>& args) {
    MKQL_ENSURE(args.size() >= 2, "Expecting at least two arguments");

    bool unlimited = false;
    if (args.front().GetStaticType()->IsVoid()) {
        unlimited = true;
    } else {
        MKQL_ENSURE(args.front().GetStaticType()->IsData() &&
            static_cast<TDataType*>(args.front().GetStaticType())->GetSchemeType() == NUdf::TDataType<ui64>::Id,
                "Expected ui64 as first argument");
    }

    std::vector<TType*> outputComponents;
    for (size_t i = 1; i < args.size(); ++i) {
        const auto& list = args[i];

        MKQL_ENSURE(list.GetStaticType()->IsList(), "Expecting list");

        auto listItemType = static_cast<TListType*>(list.GetStaticType())->GetItemType();
        MKQL_ENSURE(listItemType->IsTuple(), "Expecting list of tuples");

        auto rangeType = static_cast<TTupleType*>(listItemType);
        MKQL_ENSURE(rangeType->GetElementsCount() == 2, "Expecting list of 2-element tuples");
        MKQL_ENSURE(rangeType->GetElementType(0)->IsTuple(), "Range boundary should be tuple");

        auto boundaryType = static_cast<TTupleType*>(rangeType->GetElementType(0));

        ui32 elementsCount = boundaryType->GetElementsCount();
        MKQL_ENSURE(elementsCount >= 3 && elementsCount % 2 == 1, "Range boundary should have odd number components (at least 3)");

        for (size_t j = 0; j < elementsCount - 1; ++j) {
            outputComponents.push_back(boundaryType->GetElementType(j));
        }
    }
    outputComponents.push_back(TDataType::Create(NUdf::TDataType<i32>::Id, Env));

    auto outputBoundary = TTupleType::Create(outputComponents.size(), &outputComponents.front(), Env);
    std::vector<TType*> outputRangeComps(2, outputBoundary);
    auto outputRange = TTupleType::Create(outputRangeComps.size(), &outputRangeComps.front(), Env);

    TCallableBuilder callableBuilder(Env, __func__, TListType::Create(outputRange, Env));
    if (unlimited) {
        callableBuilder.Add(NewDataLiteral<ui64>(std::numeric_limits<ui64>::max()));
    } else {
        callableBuilder.Add(args[0]);
    }

    for (size_t i = 1; i < args.size(); ++i) {
        callableBuilder.Add(args[i]);
    }

    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::RangeFinalize(TRuntimeNode list) {
    MKQL_ENSURE(list.GetStaticType()->IsList(), "Expecting list");

    auto listItemType = static_cast<TListType*>(list.GetStaticType())->GetItemType();
    MKQL_ENSURE(listItemType->IsTuple(), "Expecting list of tuples");

    auto rangeType = static_cast<TTupleType*>(listItemType);
    MKQL_ENSURE(rangeType->GetElementsCount() == 2, "Expecting list of 2-element tuples");
    MKQL_ENSURE(rangeType->GetElementType(0)->IsTuple(), "Range boundary should be tuple");

    auto boundaryType = static_cast<TTupleType*>(rangeType->GetElementType(0));

    ui32 elementsCount = boundaryType->GetElementsCount();
    MKQL_ENSURE(elementsCount >= 3 && elementsCount % 2 == 1, "Range boundary should have odd number components (at least 3)");

    std::vector<TType*> outputComponents;
    for (ui32 i = 0; i < elementsCount; ++i) {
        if (i % 2 == 1 || i + 1 == elementsCount) {
            outputComponents.push_back(boundaryType->GetElementType(i));
        }
    }

    auto outputBoundary = TTupleType::Create(outputComponents.size(), &outputComponents.front(), Env);
    std::vector<TType*> outputRangeComps(2, outputBoundary);
    auto outputRange = TTupleType::Create(outputRangeComps.size(), &outputRangeComps.front(), Env);

    TCallableBuilder callableBuilder(Env, __func__, TListType::Create(outputRange, Env));
    callableBuilder.Add(list);

    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::Round(const std::string_view& callableName, TRuntimeNode source, TType* targetType) {
    const auto sourceType = source.GetStaticType();
    MKQL_ENSURE(sourceType->IsData(), "Expecting first arg to be of Data type");
    MKQL_ENSURE(targetType->IsData(), "Expecting second arg to be Data type");


    const auto ss = *static_cast<TDataType*>(sourceType)->GetDataSlot();
    const auto ts = *static_cast<TDataType*>(targetType)->GetDataSlot();

    const auto options = NKikimr::NUdf::GetCastResult(ss, ts);
    MKQL_ENSURE(!(*options & NKikimr::NUdf::ECastOptions::Impossible),
        "Impossible to cast " <<  *sourceType << " into " << *targetType);

    MKQL_ENSURE(*options & (NKikimr::NUdf::ECastOptions::MayFail |
                            NKikimr::NUdf::ECastOptions::MayLoseData |
                            NKikimr::NUdf::ECastOptions::AnywayLoseData),
        "Rounding from " <<  *sourceType << " to " << *targetType << " is trivial");

    TCallableBuilder callableBuilder(Env, callableName, TOptionalType::Create(targetType, Env));
    callableBuilder.Add(source);

    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::NextValue(TRuntimeNode value) {
    const auto valueType = value.GetStaticType();
    MKQL_ENSURE(valueType->IsData(), "Expecting argument of Data type");

    const auto slot = *static_cast<TDataType*>(valueType)->GetDataSlot();
    MKQL_ENSURE(slot == NUdf::EDataSlot::String || slot == NUdf::EDataSlot::Utf8,
                "Unsupported type: " << *valueType);

    TCallableBuilder callableBuilder(Env, __func__, TOptionalType::Create(valueType, Env));
    callableBuilder.Add(value);

    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::Nop(TRuntimeNode value, TType* returnType) {
    if constexpr (RuntimeVersion < 35U) {
        THROW yexception() << "Runtime version (" << RuntimeVersion << ") too old for " << __func__;
    }

    TCallableBuilder callableBuilder(Env, __func__, returnType);
    callableBuilder.Add(value);
    return TRuntimeNode(callableBuilder.Build(), false);
}

bool TProgramBuilder::IsNull(TRuntimeNode arg) {
    return arg.GetStaticType()->IsSameType(*NewNull().GetStaticType()); // TODO ->IsNull();
}

TRuntimeNode TProgramBuilder::Replicate(TRuntimeNode item, TRuntimeNode count, const std::string_view& file, ui32 row, ui32 column) {
    MKQL_ENSURE(count.GetStaticType()->IsData(), "Expected data");
    MKQL_ENSURE(static_cast<const TDataType&>(*count.GetStaticType()).GetSchemeType() == NUdf::TDataType<ui64>::Id, "Expected ui64");

    const auto listType = TListType::Create(item.GetStaticType(), Env);
    TCallableBuilder callableBuilder(Env, __func__, listType);
    callableBuilder.Add(item);
    callableBuilder.Add(count);
    if constexpr (RuntimeVersion >= 2) {
        callableBuilder.Add(NewDataLiteral<NUdf::EDataSlot::String>(file));
        callableBuilder.Add(NewDataLiteral(row));
        callableBuilder.Add(NewDataLiteral(column));
    }

    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::PgConst(TPgType* pgType, const std::string_view& value, TRuntimeNode typeMod) {
    if constexpr (RuntimeVersion < 30U) {
        THROW yexception() << "Runtime version (" << RuntimeVersion << ") too old for " << __func__;
    }

    TCallableBuilder callableBuilder(Env, __func__, pgType);
    callableBuilder.Add(NewDataLiteral(pgType->GetTypeId()));
    callableBuilder.Add(NewDataLiteral<NUdf::EDataSlot::String>(value));
    if (typeMod) {
        callableBuilder.Add(typeMod);
    }

    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::PgResolvedCall(bool useContext, const std::string_view& name,
    ui32 id, const TArrayRef<const TRuntimeNode>& args,
    TType* returnType, bool rangeFunction) {
    if constexpr (RuntimeVersion < 45U) {
        THROW yexception() << "Runtime version (" << RuntimeVersion << ") too old for " << __func__;
    }

    TCallableBuilder callableBuilder(Env, __func__, returnType);
    callableBuilder.Add(NewDataLiteral(useContext));
    callableBuilder.Add(NewDataLiteral(rangeFunction));
    callableBuilder.Add(NewDataLiteral<NUdf::EDataSlot::String>(name));
    callableBuilder.Add(NewDataLiteral(id));
    for (const auto& arg : args) {
        callableBuilder.Add(arg);
    }
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::BlockPgResolvedCall(const std::string_view& name, ui32 id,
    const TArrayRef<const TRuntimeNode>& args, TType* returnType) {
    if constexpr (RuntimeVersion < 30U) {
        THROW yexception() << "Runtime version (" << RuntimeVersion << ") too old for " << __func__;
    }

    TCallableBuilder callableBuilder(Env, __func__, returnType);
    callableBuilder.Add(NewDataLiteral<NUdf::EDataSlot::String>(name));
    callableBuilder.Add(NewDataLiteral(id));
    for (const auto& arg : args) {
        callableBuilder.Add(arg);
    }
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::PgArray(const TArrayRef<const TRuntimeNode>& args, TType* returnType) {
    if constexpr (RuntimeVersion < 30U) {
        THROW yexception() << "Runtime version (" << RuntimeVersion << ") too old for " << __func__;
    }

    TCallableBuilder callableBuilder(Env, __func__, returnType);
    for (const auto& arg : args) {
        callableBuilder.Add(arg);
    }

    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::PgTableContent(
    const std::string_view& cluster,
    const std::string_view& table,
    TType* returnType) {
    if constexpr (RuntimeVersion < 47U) {
        THROW yexception() << "Runtime version (" << RuntimeVersion << ") too old for " << __func__;
    }

    TCallableBuilder callableBuilder(Env, __func__, returnType);
    callableBuilder.Add(NewDataLiteral<NUdf::EDataSlot::String>(cluster));
    callableBuilder.Add(NewDataLiteral<NUdf::EDataSlot::String>(table));
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::PgToRecord(TRuntimeNode input, const TArrayRef<std::pair<std::string_view, std::string_view>>& members) {
    if constexpr (RuntimeVersion < 48U) {
        THROW yexception() << "Runtime version (" << RuntimeVersion << ") too old for " << __func__;
    }

    MKQL_ENSURE(input.GetStaticType()->IsStruct(), "Expected struct");
    auto structType = AS_TYPE(TStructType, input.GetStaticType());
    for (ui32 i = 0; i < structType->GetMembersCount(); ++i) {
        auto itemType = structType->GetMemberType(i);
        MKQL_ENSURE(itemType->IsNull() || itemType->IsPg(), "Expected null or pg");
    }

    auto returnType = NewPgType(NYql::NPg::LookupType("record").TypeId);
    TCallableBuilder callableBuilder(Env, __func__, returnType);
    callableBuilder.Add(input);
    TVector<TRuntimeNode> names;
    for (const auto& x : members) {
        names.push_back(NewDataLiteral<NUdf::EDataSlot::String>(x.first));
        names.push_back(NewDataLiteral<NUdf::EDataSlot::String>(x.second));
    }

    callableBuilder.Add(NewTuple(names));
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::PgCast(TRuntimeNode input, TType* returnType, TRuntimeNode typeMod) {
    if constexpr (RuntimeVersion < 30U) {
        THROW yexception() << "Runtime version (" << RuntimeVersion << ") too old for " << __func__;
    }

    TCallableBuilder callableBuilder(Env, __func__, returnType);
    callableBuilder.Add(input);
    if (typeMod) {
        callableBuilder.Add(typeMod);
    }

    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::FromPg(TRuntimeNode input, TType* returnType) {
    if constexpr (RuntimeVersion < 30U) {
        THROW yexception() << "Runtime version (" << RuntimeVersion << ") too old for " << __func__;
    }

    TCallableBuilder callableBuilder(Env, __func__, returnType);
    callableBuilder.Add(input);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::ToPg(TRuntimeNode input, TType* returnType) {
    if constexpr (RuntimeVersion < 30U) {
        THROW yexception() << "Runtime version (" << RuntimeVersion << ") too old for " << __func__;
    }

    TCallableBuilder callableBuilder(Env, __func__, returnType);
    callableBuilder.Add(input);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::PgClone(TRuntimeNode input, const TArrayRef<const TRuntimeNode>& dependentNodes) {
    if constexpr (RuntimeVersion < 38U) {
        THROW yexception() << "Runtime version (" << RuntimeVersion << ") too old for " << __func__;
    }

    TCallableBuilder callableBuilder(Env, __func__, input.GetStaticType());
    callableBuilder.Add(input);
    for (const auto& node : dependentNodes) {
        callableBuilder.Add(node);
    }

    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::WithContext(TRuntimeNode input, const std::string_view& contextType) {
    if constexpr (RuntimeVersion < 30U) {
        THROW yexception() << "Runtime version (" << RuntimeVersion << ") too old for " << __func__;
    }

    TCallableBuilder callableBuilder(Env, __func__, input.GetStaticType());
    callableBuilder.Add(NewDataLiteral<NUdf::EDataSlot::String>(contextType));
    callableBuilder.Add(input);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::PgInternal0(TType* returnType) {
    TCallableBuilder callableBuilder(Env, __func__, returnType);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::BlockIf(TRuntimeNode condition, TRuntimeNode thenBranch, TRuntimeNode elseBranch) {
    const auto conditionType = AS_TYPE(TBlockType, condition.GetStaticType());
    MKQL_ENSURE(AS_TYPE(TDataType, conditionType->GetItemType())->GetSchemeType() == NUdf::TDataType<bool>::Id,
                "Expected bool as first argument");

    const auto thenType = AS_TYPE(TBlockType, thenBranch.GetStaticType());
    const auto elseType = AS_TYPE(TBlockType, elseBranch.GetStaticType());
    MKQL_ENSURE(thenType->GetItemType()->IsSameType(*elseType->GetItemType()), "Different return types in branches.");

    auto returnType = NewBlockType(thenType->GetItemType(), GetResultShape({conditionType, thenType, elseType}));
    TCallableBuilder callableBuilder(Env, __func__, returnType);
    callableBuilder.Add(condition);
    callableBuilder.Add(thenBranch);
    callableBuilder.Add(elseBranch);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::BlockJust(TRuntimeNode data) {
    const auto initialType = AS_TYPE(TBlockType, data.GetStaticType());
    auto returnType = NewBlockType(NewOptionalType(initialType->GetItemType()), initialType->GetShape());
    TCallableBuilder callableBuilder(Env, __func__, returnType);
    callableBuilder.Add(data);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::BlockFunc(const std::string_view& funcName, TType* returnType, const TArrayRef<const TRuntimeNode>& args) {
    for (const auto& arg : args) {
        MKQL_ENSURE(arg.GetStaticType()->IsBlock(), "Expected Block type");
    }

    TCallableBuilder builder(Env, __func__, returnType);
    builder.Add(NewDataLiteral<NUdf::EDataSlot::String>(funcName));
    for (const auto& arg : args) {
        builder.Add(arg);
    }

    return TRuntimeNode(builder.Build(), false);
}

TRuntimeNode TProgramBuilder::BlockBitCast(TRuntimeNode value, TType* targetType) {
    MKQL_ENSURE(value.GetStaticType()->IsBlock(), "Expected Block type");

    auto returnType = TBlockType::Create(targetType, AS_TYPE(TBlockType, value.GetStaticType())->GetShape(), Env);
    TCallableBuilder builder(Env, __func__, returnType);
    builder.Add(value);
    builder.Add(TRuntimeNode(targetType, true));

    return TRuntimeNode(builder.Build(), false);
}

TRuntimeNode TProgramBuilder::BlockCombineAll(TRuntimeNode flow, std::optional<ui32> filterColumn,
    const TArrayRef<const TAggInfo>& aggs, TType* returnType) {
    if constexpr (RuntimeVersion < 31U) {
        THROW yexception() << "Runtime version (" << RuntimeVersion << ") too old for " << __func__;
    }

    TCallableBuilder builder(Env, __func__, returnType);
    builder.Add(flow);
    if (!filterColumn) {
        builder.Add(NewEmptyOptionalDataLiteral(NUdf::TDataType<ui32>::Id));
    } else {
        builder.Add(NewOptional(NewDataLiteral<ui32>(*filterColumn)));
    }

    TVector<TRuntimeNode> aggsNodes;
    for (const auto& agg : aggs) {
        TVector<TRuntimeNode> params;
        params.push_back(NewDataLiteral<NUdf::EDataSlot::String>(agg.Name));
        for (const auto& col : agg.ArgsColumns) {
            params.push_back(NewDataLiteral<ui32>(col));
        }

        aggsNodes.push_back(NewTuple(params));
    }

    builder.Add(NewTuple(aggsNodes));
    return TRuntimeNode(builder.Build(), false);
}

TRuntimeNode TProgramBuilder::BlockCombineHashed(TRuntimeNode flow, std::optional<ui32> filterColumn, const TArrayRef<ui32>& keys,
    const TArrayRef<const TAggInfo>& aggs, TType* returnType) {
    if constexpr (RuntimeVersion < 31U) {
        THROW yexception() << "Runtime version (" << RuntimeVersion << ") too old for " << __func__;
    }

    TCallableBuilder builder(Env, __func__, returnType);
    builder.Add(flow);
    if (!filterColumn) {
        builder.Add(NewEmptyOptionalDataLiteral(NUdf::TDataType<ui32>::Id));
    } else {
        builder.Add(NewOptional(NewDataLiteral<ui32>(*filterColumn)));
    }

    TVector<TRuntimeNode> keyNodes;
    for (const auto& key : keys) {
        keyNodes.push_back(NewDataLiteral<ui32>(key));
    }

    builder.Add(NewTuple(keyNodes));
    TVector<TRuntimeNode> aggsNodes;
    for (const auto& agg : aggs) {
        TVector<TRuntimeNode> params;
        params.push_back(NewDataLiteral<NUdf::EDataSlot::String>(agg.Name));
        for (const auto& col : agg.ArgsColumns) {
            params.push_back(NewDataLiteral<ui32>(col));
        }

        aggsNodes.push_back(NewTuple(params));
    }

    builder.Add(NewTuple(aggsNodes));
    return TRuntimeNode(builder.Build(), false);
}

TRuntimeNode TProgramBuilder::BlockMergeFinalizeHashed(TRuntimeNode flow, const TArrayRef<ui32>& keys,
    const TArrayRef<const TAggInfo>& aggs, TType* returnType) {
    if constexpr (RuntimeVersion < 31U) {
        THROW yexception() << "Runtime version (" << RuntimeVersion << ") too old for " << __func__;
    }

    TCallableBuilder builder(Env, __func__, returnType);
    builder.Add(flow);

    TVector<TRuntimeNode> keyNodes;
    for (const auto& key : keys) {
        keyNodes.push_back(NewDataLiteral<ui32>(key));
    }

    builder.Add(NewTuple(keyNodes));
    TVector<TRuntimeNode> aggsNodes;
    for (const auto& agg : aggs) {
        TVector<TRuntimeNode> params;
        params.push_back(NewDataLiteral<NUdf::EDataSlot::String>(agg.Name));
        for (const auto& col : agg.ArgsColumns) {
            params.push_back(NewDataLiteral<ui32>(col));
        }

        aggsNodes.push_back(NewTuple(params));
    }

    builder.Add(NewTuple(aggsNodes));
    return TRuntimeNode(builder.Build(), false);
}

TRuntimeNode TProgramBuilder::BlockMergeManyFinalizeHashed(TRuntimeNode flow, const TArrayRef<ui32>& keys,
    const TArrayRef<const TAggInfo>& aggs, ui32 streamIndex, const TVector<TVector<ui32>>& streams, TType* returnType) {
    if constexpr (RuntimeVersion < 31U) {
        THROW yexception() << "Runtime version (" << RuntimeVersion << ") too old for " << __func__;
    }

    TCallableBuilder builder(Env, __func__, returnType);
    builder.Add(flow);

    TVector<TRuntimeNode> keyNodes;
    for (const auto& key : keys) {
        keyNodes.push_back(NewDataLiteral<ui32>(key));
    }

    builder.Add(NewTuple(keyNodes));
    TVector<TRuntimeNode> aggsNodes;
    for (const auto& agg : aggs) {
        TVector<TRuntimeNode> params;
        params.push_back(NewDataLiteral<NUdf::EDataSlot::String>(agg.Name));
        for (const auto& col : agg.ArgsColumns) {
            params.push_back(NewDataLiteral<ui32>(col));
        }

        aggsNodes.push_back(NewTuple(params));
    }

    builder.Add(NewTuple(aggsNodes));
    builder.Add(NewDataLiteral<ui32>(streamIndex));
    TVector<TRuntimeNode> streamsNodes;
    for (const auto& s : streams) {
        TVector<TRuntimeNode> streamNodes;
        for (const auto& i : s) {
            streamNodes.push_back(NewDataLiteral<ui32>(i));
        }

        streamsNodes.push_back(NewTuple(streamNodes));
    }

    builder.Add(NewTuple(streamsNodes));
    return TRuntimeNode(builder.Build(), false);
}

TRuntimeNode TProgramBuilder::ScalarApply(const TArrayRef<const TRuntimeNode>& args, const TArrayLambda& handler) {
    if constexpr (RuntimeVersion < 39U) {
        THROW yexception() << "Runtime version (" << RuntimeVersion << ") too old for " << __func__;
    }

    MKQL_ENSURE(!args.empty(), "Required at least one argument");
    TVector<TRuntimeNode> lambdaArgs;
    bool scalarOnly = true;
    std::shared_ptr<arrow::DataType> arrowType;
    for (const auto& arg : args) {
        auto blockType = AS_TYPE(TBlockType, arg.GetStaticType());
        scalarOnly = scalarOnly && blockType->GetShape() == TBlockType::EShape::Scalar;
        MKQL_ENSURE(ConvertArrowType(blockType->GetItemType(), arrowType), "Unsupported arrow type");
        lambdaArgs.emplace_back(Arg(blockType->GetItemType()));
    }

    auto ret = handler(lambdaArgs);
    MKQL_ENSURE(ConvertArrowType(ret.GetStaticType(), arrowType), "Unsupported arrow type");
    auto returnType = NewBlockType(ret.GetStaticType(), scalarOnly ? TBlockType::EShape::Scalar : TBlockType::EShape::Many);
    TCallableBuilder builder(Env, __func__, returnType);
    for (const auto& arg : args) {
        builder.Add(arg);
    }

    for (const auto& arg : lambdaArgs) {
        builder.Add(arg);
    }

    builder.Add(ret);
    return TRuntimeNode(builder.Build(), false);
}

TRuntimeNode TProgramBuilder::BlockMapJoinCore(TRuntimeNode flow, TRuntimeNode dict,
    EJoinKind joinKind, const TArrayRef<const ui32>& leftKeyColumns
) {
    if constexpr (RuntimeVersion < 51U) {
        THROW yexception() << "Runtime version (" << RuntimeVersion << ") too old for " << __func__;
    }
    MKQL_ENSURE(joinKind == EJoinKind::LeftSemi || joinKind == EJoinKind::LeftOnly,
                "Unsupported join kind");
    MKQL_ENSURE(!leftKeyColumns.empty(), "At least one key column must be specified");

    TRuntimeNode::TList leftKeyColumnsNodes;
    leftKeyColumnsNodes.reserve(leftKeyColumns.size());
    std::transform(leftKeyColumns.cbegin(), leftKeyColumns.cend(),
        std::back_inserter(leftKeyColumnsNodes), [this](const ui32 idx) {
            return NewDataLiteral(idx);
        });

    auto returnJoinItems = ValidateBlockFlowType(flow.GetStaticType(), false);
    const auto payloadType = AS_TYPE(TDictType, dict.GetStaticType())->GetPayloadType();
    // XXX: This is the contract ensured by the expression compiler and
    // optimizers for join types that don't require the right (i.e. dict) part.
    MKQL_ENSURE(payloadType->IsVoid(), "Dict payload has to be Void");
    TType* returnJoinType = NewFlowType(NewMultiType(returnJoinItems));

    TCallableBuilder callableBuilder(Env, __func__, returnJoinType);
    callableBuilder.Add(flow);
    callableBuilder.Add(dict);
    callableBuilder.Add(NewDataLiteral((ui32)joinKind));
    callableBuilder.Add(NewTuple(leftKeyColumnsNodes));

    return TRuntimeNode(callableBuilder.Build(), false);
}

namespace {
using namespace NYql::NMatchRecognize;
TRuntimeNode PatternToRuntimeNode(const TRowPattern& pattern, const TProgramBuilder& programBuilder) {
    const auto& env = programBuilder.GetTypeEnvironment();
    TTupleLiteralBuilder patternBuilder(env);
    for (const auto& term: pattern) {
        TTupleLiteralBuilder termBuilder(env);
        for (const auto& factor: term) {
            TTupleLiteralBuilder factorBuilder(env);
            factorBuilder.Add(factor.Primary.index() == 0 ?
                programBuilder.NewDataLiteral<NUdf::EDataSlot::String>(std::get<0>(factor.Primary)) :
                PatternToRuntimeNode(std::get<1>(factor.Primary), programBuilder)
            );
            factorBuilder.Add(programBuilder.NewDataLiteral<ui64>(factor.QuantityMin));
            factorBuilder.Add(programBuilder.NewDataLiteral<ui64>(factor.QuantityMax));
            factorBuilder.Add(programBuilder.NewDataLiteral<bool>(factor.Greedy));
            factorBuilder.Add(programBuilder.NewDataLiteral<bool>(factor.Output));
            factorBuilder.Add(programBuilder.NewDataLiteral<bool>(factor.Unused));
            termBuilder.Add({factorBuilder.Build(), true});
        }
        patternBuilder.Add({termBuilder.Build(), true});
    }
    return {patternBuilder.Build(), true};
};

} //namespace

TRuntimeNode TProgramBuilder::MatchRecognizeCore(
    TRuntimeNode inputStream,
    const TUnaryLambda& getPartitionKeySelectorNode,
    const TArrayRef<TStringBuf>& partitionColumns,
    const TArrayRef<std::pair<TStringBuf, TBinaryLambda>>& getMeasures,
    const NYql::NMatchRecognize::TRowPattern& pattern,
    const TArrayRef<std::pair<TStringBuf, TTernaryLambda>>& getDefines,
    bool streamingMode
) {
    MKQL_ENSURE(RuntimeVersion >= 42, "MatchRecognize is not supported in runtime version " << RuntimeVersion);

    const auto inputRowType = AS_TYPE(TStructType, AS_TYPE(TFlowType, inputStream.GetStaticType())->GetItemType());
    const auto inputRowArg = Arg(inputRowType);
    const auto partitionKeySelectorNode = getPartitionKeySelectorNode(inputRowArg);

    TStructTypeBuilder indexRangeTypeBuilder(Env);
    indexRangeTypeBuilder.Add("From", TDataType::Create(NUdf::TDataType<ui64>::Id, Env));
    indexRangeTypeBuilder.Add("To", TDataType::Create(NUdf::TDataType<ui64>::Id, Env));
    const auto& rangeList = TListType::Create(indexRangeTypeBuilder.Build(), Env);
    TStructTypeBuilder matchedVarsTypeBuilder(Env);
    for (const auto& var: GetPatternVars(pattern)) {
        matchedVarsTypeBuilder.Add(var, rangeList);
    }
    TRuntimeNode matchedVarsArg = Arg(matchedVarsTypeBuilder.Build());

    //---These vars may be empty in case of no measures
    TRuntimeNode measureInputDataArg;
    std::vector<TRuntimeNode> specialColumnIndexesInMeasureInputDataRow;
    TVector<TRuntimeNode> measures;
    TVector<TType*> measureTypes;
    //---
    if (getMeasures.empty()) {
        measureInputDataArg = Arg(Env.GetTypeOfVoidLazy());
    } else {
        using NYql::NMatchRecognize::EMeasureInputDataSpecialColumns;
        measures.reserve(getMeasures.size());
        measureTypes.reserve(getMeasures.size());
        specialColumnIndexesInMeasureInputDataRow.resize(static_cast<size_t>(NYql::NMatchRecognize::EMeasureInputDataSpecialColumns::Last));
        TStructTypeBuilder measureInputDataRowTypeBuilder(Env);
        for (ui32 i = 0; i != inputRowType->GetMembersCount(); ++i) {
            measureInputDataRowTypeBuilder.Add(inputRowType->GetMemberName(i), inputRowType->GetMemberType(i));
        }
        measureInputDataRowTypeBuilder.Add(
                MeasureInputDataSpecialColumnName(EMeasureInputDataSpecialColumns::Classifier),
                TDataType::Create(NUdf::TDataType<NYql::NUdf::TUtf8>::Id, Env)
        );
        measureInputDataRowTypeBuilder.Add(
                MeasureInputDataSpecialColumnName(EMeasureInputDataSpecialColumns::MatchNumber),
                TDataType::Create(NUdf::TDataType<ui64>::Id, Env)
        );
        const auto measureInputDataRowType = measureInputDataRowTypeBuilder.Build();

        for (ui32 i = 0; i != measureInputDataRowType->GetMembersCount(); ++i) {
            //assume a few, if grows, it's better to use a lookup table here
            static_assert(static_cast<size_t>(EMeasureInputDataSpecialColumns::Last) < 5);
            for (size_t j = 0; j != static_cast<size_t>(EMeasureInputDataSpecialColumns::Last); ++j) {
                if (measureInputDataRowType->GetMemberName(i) ==
                        NYql::NMatchRecognize::MeasureInputDataSpecialColumnName(static_cast<EMeasureInputDataSpecialColumns>(j)))
                    specialColumnIndexesInMeasureInputDataRow[j] = NewDataLiteral<ui32>(i);
            }
        }

        measureInputDataArg = Arg(TListType::Create(measureInputDataRowType, Env));
        for (size_t i = 0; i != getMeasures.size(); ++i) {
            measures.push_back(getMeasures[i].second(measureInputDataArg, matchedVarsArg));
            measureTypes.push_back(measures[i].GetStaticType());
        }
    }

    TStructTypeBuilder outputRowTypeBuilder(Env);
    THashMap<TStringBuf, size_t> partitionColumnLookup;
    for (size_t i = 0; i != partitionColumns.size(); ++i) {
        const auto& name = partitionColumns[i];
        partitionColumnLookup[name] = i;
        outputRowTypeBuilder.Add(
                name,
                AS_TYPE(TTupleType, partitionKeySelectorNode.GetStaticType())->GetElementType(i)
        );
    }
    THashMap<TStringBuf, size_t> measureColumnLookup;
    for (size_t i = 0; i != measures.size(); ++i) {
        const auto& name = getMeasures[i].first;
        measureColumnLookup[name] = i;
        outputRowTypeBuilder.Add(
                name,
                measures[i].GetStaticType()
        );
    }
    auto outputRowType = outputRowTypeBuilder.Build();

    std::vector<TRuntimeNode> partitionColumnIndexes(partitionColumnLookup.size());
    std::vector<TRuntimeNode> measureColumnIndexes(measureColumnLookup.size());
    for (ui32 i = 0; i != outputRowType->GetMembersCount(); ++i) {
        if (auto it = partitionColumnLookup.find(outputRowType->GetMemberName(i)); it != partitionColumnLookup.end()) {
            partitionColumnIndexes[it->second] = NewDataLiteral<ui32>(i);
        }
        else if (auto it = measureColumnLookup.find(outputRowType->GetMemberName(i)); it != measureColumnLookup.end()) {
            measureColumnIndexes[it->second] = NewDataLiteral<ui32>(i);
        }
    }
    auto outputType = (TType*)TFlowType::Create(outputRowType, Env);

    THashMap<TStringBuf , size_t> patternVarLookup;
    for (ui32 i = 0; i != AS_TYPE(TStructType, matchedVarsArg.GetStaticType())->GetMembersCount(); ++i){
        patternVarLookup[AS_TYPE(TStructType, matchedVarsArg.GetStaticType())->GetMemberName(i)] = i;
    }

    THashMap<TStringBuf , size_t> defineLookup;
    for (size_t i = 0; i != getDefines.size(); ++i) {
        defineLookup[getDefines[i].first] = i;
    }
    std::vector<TRuntimeNode> defineNames(patternVarLookup.size());
    std::vector<TRuntimeNode> defineNodes(patternVarLookup.size());

    const auto& inputDataArg = Arg(TListType::Create(inputRowType, Env));
    const auto& currentRowIndexArg = Arg(TDataType::Create(NUdf::TDataType<ui64>::Id, Env));
    for (const auto& [v, i]: patternVarLookup) {
        defineNames[i] = NewDataLiteral<NUdf::EDataSlot::String>(v);
        if (const auto it = defineLookup.find(v); it != defineLookup.end()) {
            defineNodes[i] = getDefines[it->second].second(inputDataArg, matchedVarsArg, currentRowIndexArg);
        }
        else { //no predicate for var
            if ("$" == v || "^" == v) {
                //DO nothing, //will be handled in a specific way
            }
            else { // a var without a predicate matches any row
                defineNodes[i] = NewDataLiteral<bool>(true);
            }
        }
    }

    TCallableBuilder callableBuilder(GetTypeEnvironment(), "MatchRecognizeCore", outputType);
    auto indexType = TDataType::Create(NUdf::TDataType<ui32>::Id, Env);
    auto indexListType = TListType::Create(indexType, Env);
    callableBuilder.Add(inputStream);
    callableBuilder.Add(inputRowArg);
    callableBuilder.Add(partitionKeySelectorNode);
    callableBuilder.Add(TRuntimeNode(TListLiteral::Create(partitionColumnIndexes.data(), partitionColumnIndexes.size(), indexListType, Env), true));
    callableBuilder.Add(measureInputDataArg);
    callableBuilder.Add(TRuntimeNode(TListLiteral::Create(
            specialColumnIndexesInMeasureInputDataRow.data(), specialColumnIndexesInMeasureInputDataRow.size(),
            indexListType, Env
            ),
    true));
    callableBuilder.Add(NewDataLiteral<ui32>(inputRowType->GetMembersCount()));
    callableBuilder.Add(matchedVarsArg);
    callableBuilder.Add(TRuntimeNode(TListLiteral::Create(measureColumnIndexes.data(), measureColumnIndexes.size(), indexListType, Env), true));
    for (const auto& m: measures) {
        callableBuilder.Add(m);
    }

    callableBuilder.Add(PatternToRuntimeNode(pattern, *this));

    callableBuilder.Add(currentRowIndexArg);
    callableBuilder.Add(inputDataArg);
    const auto stringType = NewDataType(NUdf::EDataSlot::String);
    callableBuilder.Add(TRuntimeNode(TListLiteral::Create(defineNames.begin(), defineNames.size(), TListType::Create(stringType, Env), Env), true));
    for (const auto& d: defineNodes) {
        callableBuilder.Add(d);
    }
    callableBuilder.Add(NewDataLiteral(streamingMode));
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TProgramBuilder::TimeOrderRecover(
    TRuntimeNode inputStream,
    const TUnaryLambda& getTimeExtractor,
    TRuntimeNode delay,
    TRuntimeNode ahead,
    TRuntimeNode rowLimit
    )
{
    MKQL_ENSURE(RuntimeVersion >= 44, "TimeOrderRecover is not supported in runtime version " << RuntimeVersion);

    auto& inputRowType = *static_cast<TStructType*>(AS_TYPE(TStructType, AS_TYPE(TFlowType, inputStream.GetStaticType())->GetItemType()));
    const auto inputRowArg = Arg(&inputRowType);
    TStructTypeBuilder outputRowTypeBuilder(Env);
    outputRowTypeBuilder.Reserve(inputRowType.GetMembersCount() + 1);
    const ui32 inputRowColumnCount = inputRowType.GetMembersCount();
    for (ui32 i = 0; i != inputRowColumnCount; ++i) {
        outputRowTypeBuilder.Add(inputRowType.GetMemberName(i), inputRowType.GetMemberType(i));
    }
    using NYql::NTimeOrderRecover::OUT_OF_ORDER_MARKER;
    outputRowTypeBuilder.Add(OUT_OF_ORDER_MARKER, TDataType::Create(NUdf::TDataType<bool>::Id, Env));
    const auto outputRowType = outputRowTypeBuilder.Build();
    const auto outOfOrderColumnIndex = outputRowType->GetMemberIndex(OUT_OF_ORDER_MARKER);
    TCallableBuilder callableBuilder(GetTypeEnvironment(), "TimeOrderRecover", TFlowType::Create(outputRowType, Env));

    callableBuilder.Add(inputStream);
    callableBuilder.Add(inputRowArg);
    callableBuilder.Add(getTimeExtractor(inputRowArg));
    callableBuilder.Add(NewDataLiteral(inputRowColumnCount));
    callableBuilder.Add(NewDataLiteral(outOfOrderColumnIndex));
    callableBuilder.Add(delay),
    callableBuilder.Add(ahead),
    callableBuilder.Add(rowLimit);
    return TRuntimeNode(callableBuilder.Build(), false);
}

bool CanExportType(TType* type, const TTypeEnvironment& env) {
    if (type->GetKind() == TType::EKind::Type) {
        return false; // Type of Type
    }

    TExploringNodeVisitor explorer;
    explorer.Walk(type, env);
    bool canExport = true;
    for (auto& node : explorer.GetNodes()) {
        switch (static_cast<TType*>(node)->GetKind()) {
        case TType::EKind::Void:
            node->SetCookie(1);
            break;

        case TType::EKind::Data:
            node->SetCookie(1);
            break;

        case TType::EKind::Pg:
            node->SetCookie(1);
            break;

        case TType::EKind::Optional: {
            auto optionalType = static_cast<TOptionalType*>(node);
            if (!optionalType->GetItemType()->GetCookie()) {
                canExport = false;
            } else {
                node->SetCookie(1);
            }

            break;
        }

        case TType::EKind::List: {
            auto listType = static_cast<TListType*>(node);
            if (!listType->GetItemType()->GetCookie()) {
                canExport = false;
            } else {
                node->SetCookie(1);
            }

            break;
        }

        case TType::EKind::Struct: {
            auto structType = static_cast<TStructType*>(node);
            for (ui32 index = 0; index < structType->GetMembersCount(); ++index) {
                if (!structType->GetMemberType(index)->GetCookie()) {
                    canExport = false;
                    break;
                }
            }

            if (canExport) {
                node->SetCookie(1);
            }

            break;
        }

        case TType::EKind::Tuple: {
            auto tupleType = static_cast<TTupleType*>(node);
            for (ui32 index = 0; index < tupleType->GetElementsCount(); ++index) {
                if (!tupleType->GetElementType(index)->GetCookie()) {
                    canExport = false;
                    break;
                }
            }

            if (canExport) {
                node->SetCookie(1);
            }

            break;
        }

        case TType::EKind::Dict:  {
            auto dictType = static_cast<TDictType*>(node);
            if (!dictType->GetKeyType()->GetCookie() || !dictType->GetPayloadType()->GetCookie()) {
                canExport = false;
            } else {
                node->SetCookie(1);
            }

            break;
        }

        case TType::EKind::Variant:  {
            auto variantType = static_cast<TVariantType*>(node);
            TType* innerType = variantType->GetUnderlyingType();

            if (innerType->IsStruct()) {
                auto structType = static_cast<TStructType*>(innerType);
                for (ui32 index = 0; index < structType->GetMembersCount(); ++index) {
                    if (!structType->GetMemberType(index)->GetCookie()) {
                        canExport = false;
                        break;
                    }
                }
            }

            if (innerType->IsTuple()) {
                auto tupleType = static_cast<TTupleType*>(innerType);
                for (ui32 index = 0; index < tupleType->GetElementsCount(); ++index) {
                    if (!tupleType->GetElementType(index)->GetCookie()) {
                        canExport = false;
                        break;
                    }
                }
            }

            if (canExport) {
                node->SetCookie(1);
            }

            break;
        }

        case TType::EKind::Type:
            break;

        default:
            canExport = false;
        }

        if (!canExport) {
            break;
        }
    }

    for (auto& node : explorer.GetNodes()) {
        node->SetCookie(0);
    }

    return canExport;
}

}
}
