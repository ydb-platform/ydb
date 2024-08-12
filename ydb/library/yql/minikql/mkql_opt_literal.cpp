#include "mkql_opt_literal.h"
#include "mkql_node_cast.h"
#include "mkql_node_builder.h"
#include "mkql_node_visitor.h"
#include "mkql_program_builder.h"
#include "mkql_node_printer.h"

#include <library/cpp/containers/stack_vector/stack_vec.h>

#include <util/generic/singleton.h>


namespace NKikimr {
namespace NMiniKQL {

using namespace NDetail;

namespace {

TNode* LiteralAddMember(
        const TStructLiteral& oldStruct,
        const TStructType& newStructType,
        TRuntimeNode newMember,
        TRuntimeNode position,
        const TTypeEnvironment& env)
{
    TStructLiteralBuilder resultBuilder(env);

    TDataLiteral* positionData = AS_VALUE(TDataLiteral, position);
    const ui32 positionValue = positionData->AsValue().Get<ui32>();
    MKQL_ENSURE(positionValue <= oldStruct.GetType()->GetMembersCount(), "Bad member index");

    for (ui32 i = 0; i < positionValue; ++i) {
        resultBuilder.Add(TString(oldStruct.GetType()->GetMemberName(i)), oldStruct.GetValue(i));
    }

    resultBuilder.Add(TString(newStructType.GetMemberName(positionValue)), newMember);
    for (ui32 i = positionValue; i < oldStruct.GetValuesCount(); ++i) {
        resultBuilder.Add(TString(oldStruct.GetType()->GetMemberName(i)), oldStruct.GetValue(i));
    }

    return resultBuilder.Build();
}

TNode* LiteralRemoveMember(
    const TStructLiteral& oldStruct,
    TRuntimeNode position,
    const TTypeEnvironment& env)
{
    TStructLiteralBuilder resultBuilder(env);

    TDataLiteral* positionData = AS_VALUE(TDataLiteral, position);
    const ui32 positionValue = positionData->AsValue().Get<ui32>();
    MKQL_ENSURE(positionValue < oldStruct.GetType()->GetMembersCount(), "Bad member index");

    for (ui32 i = 0; i < positionValue; ++i) {
        resultBuilder.Add(TString(oldStruct.GetType()->GetMemberName(i)), oldStruct.GetValue(i));
    }

    for (ui32 i = positionValue + 1; i < oldStruct.GetValuesCount(); ++i) {
        resultBuilder.Add(TString(oldStruct.GetType()->GetMemberName(i)), oldStruct.GetValue(i));
    }

    return resultBuilder.Build();
}

TRuntimeNode OptimizeIf(TCallable& callable, const TTypeEnvironment& env) {
    Y_UNUSED(env);
    MKQL_ENSURE(callable.GetInputsCount() == 3, "Expected 3 arguments");

    auto predicateInput = callable.GetInput(0);
    auto thenInput = callable.GetInput(1);
    auto elseInput = callable.GetInput(2);
    if (predicateInput.HasValue()) {
        TDataLiteral* data = AS_VALUE(TDataLiteral, predicateInput);
        const bool predicateValue = data->AsValue().Get<bool>();
        return predicateValue ? thenInput : elseInput;
    }

    if (thenInput == elseInput) {
        return thenInput;
    }

    return TRuntimeNode(&callable, false);
}

TRuntimeNode OptimizeSize(TCallable& callable, const TTypeEnvironment& env) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 arguments");

    auto dataInput = callable.GetInput(0);
    if (dataInput.HasValue()) {
        if (dataInput.GetStaticType()->IsData()) {
            auto slot = *AS_TYPE(TDataType, dataInput.GetStaticType())->GetDataSlot();
            if (NYql::NUdf::GetDataTypeInfo(slot).Features & NYql::NUdf::EDataTypeFeatures::StringType) {
                TDataLiteral* value = AS_VALUE(TDataLiteral, dataInput);
                return TRuntimeNode(BuildDataLiteral(NUdf::TUnboxedValuePod((ui32)value->AsValue().AsStringRef().Size()), NUdf::EDataSlot::Uint32, env), true);
            }
        }
    }

    return TRuntimeNode(&callable, false);
}

TRuntimeNode OptimizeLength(TCallable& callable, const TTypeEnvironment& env) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 arguments");

    auto listOrDictInput = callable.GetInput(0);
    if (listOrDictInput.HasValue()) {
        if (listOrDictInput.GetStaticType()->IsList()) {
            TListLiteral* value = AS_VALUE(TListLiteral, listOrDictInput);
            return TRuntimeNode(BuildDataLiteral(NUdf::TUnboxedValuePod((ui64)value->GetItemsCount()), NUdf::EDataSlot::Uint64, env), true);
        }
    }

    return TRuntimeNode(&callable, false);
}

TRuntimeNode OptimizeAddMember(TCallable& callable, const TTypeEnvironment& env) {
    MKQL_ENSURE(callable.GetInputsCount() == 3, "Expected 3 arguments");

    auto callableReturnType = callable.GetType()->GetReturnType();
    MKQL_ENSURE(callableReturnType->IsStruct(), "Expected struct");
    const auto& newType = static_cast<TStructType&>(*callableReturnType);

    auto structInput = callable.GetInput(0);
    if (structInput.HasValue()) {
        TStructLiteral* value = AS_VALUE(TStructLiteral, structInput);
        return TRuntimeNode(LiteralAddMember(*value, newType, callable.GetInput(1), callable.GetInput(2), env), true);
    }

    return TRuntimeNode(&callable, false);
}

TRuntimeNode OptimizeRemoveMember(TCallable& callable, const TTypeEnvironment& env) {
    MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected 2 arguments");

    auto callableReturnType = callable.GetType()->GetReturnType();
    MKQL_ENSURE(callableReturnType->IsStruct(), "Expected struct");
    auto structInput = callable.GetInput(0);
    if (structInput.HasValue()) {
        TStructLiteral* value = AS_VALUE(TStructLiteral, structInput);
        return TRuntimeNode(LiteralRemoveMember(*value, callable.GetInput(1), env), true);
    }

    return TRuntimeNode(&callable, false);
}

TRuntimeNode OptimizeMember(TCallable& callable, const TTypeEnvironment& env) {
    Y_UNUSED(env);
    MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected 2 arguments");

    auto structInput = callable.GetInput(0);
    if (structInput.HasValue() && structInput.GetStaticType()->IsStruct()) {
        TStructLiteral* value = AS_VALUE(TStructLiteral, structInput);

        auto position = callable.GetInput(1);
        TDataLiteral* positionData = AS_VALUE(TDataLiteral, position);
        const ui32 positionValue = positionData->AsValue().Get<ui32>();

        MKQL_ENSURE(positionValue < value->GetValuesCount(), "Bad member index");
        return value->GetValue(positionValue);
    }

    return TRuntimeNode(&callable, false);
}

TRuntimeNode OptimizeFilter(TCallable& callable, const TTypeEnvironment& env) {
    if (callable.GetInputsCount() == 3U) {
        auto listInput = callable.GetInput(0);
        if (!listInput.GetStaticType()->IsList()) {
            return TRuntimeNode(&callable, false);
        }

        auto listType = static_cast<TListType*>(listInput.GetStaticType());
        auto predicateInput = callable.GetInput(2);
        if (predicateInput.HasValue()) {
            auto predicate = predicateInput.GetValue();
            MKQL_ENSURE(predicate->GetType()->IsData(), "Expected data");

            const auto& data = static_cast<const TDataLiteral&>(*predicate);
            const bool predicateValue = data.AsValue().Get<bool>();
            if (predicateValue) {
                return listInput;
            } else {
                return TRuntimeNode(TListLiteral::Create(nullptr, 0, listType, env), true);
            }
        }
    }

    return TRuntimeNode(&callable, false);
}

TRuntimeNode OptimizeMap(TCallable& callable, const TTypeEnvironment& env) {
    MKQL_ENSURE(callable.GetInputsCount() == 3, "Expected 3 arguments");

    auto returnType = callable.GetType()->GetReturnType();
    if (!returnType->IsList()) {
        return TRuntimeNode(&callable, false);
    }

    auto listType = static_cast<TListType*>(returnType);
    auto newItemInput = callable.GetInput(2);
    if (listType->GetItemType()->IsVoid() && newItemInput.HasValue()) {
        return TRuntimeNode(env.GetListOfVoidLazy(), true);
    }

    return TRuntimeNode(&callable, false);
}

TRuntimeNode OptimizeFlatMap(TCallable& callable, const TTypeEnvironment& env) {
    MKQL_ENSURE(callable.GetInputsCount() > 2U, "Expected 3 or more arguments");

    const auto returnType = callable.GetType()->GetReturnType();
    if (!returnType->IsList() || callable.GetInputsCount() > 3U) {
        return TRuntimeNode(&callable, false);
    }

    const auto listType = static_cast<TListType*>(returnType);
    const auto newItemInput = callable.GetInput(2);
    if (listType->GetItemType()->IsVoid() && newItemInput.HasValue()) {
        if (newItemInput.GetStaticType()->IsList()) {
            TListLiteral* list = AS_VALUE(TListLiteral, newItemInput);
            if (list->GetItemsCount() == 0) {
                return TRuntimeNode(env.GetListOfVoidLazy(), true);
            }
        } else {
            TOptionalLiteral* opt = AS_VALUE(TOptionalLiteral, newItemInput);
            if (!opt->HasItem()) {
                return TRuntimeNode(env.GetListOfVoidLazy(), true);
            }
        }
    }

    return TRuntimeNode(&callable, false);
}

TRuntimeNode OptimizeCoalesce(TCallable& callable, const TTypeEnvironment& env) {
    Y_UNUSED(env);
    MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected 2 arguments");

    auto optionalInput = callable.GetInput(0);
    auto defaultInput = callable.GetInput(1);
    if (optionalInput.HasValue()) {
        auto optionalData = AS_VALUE(TOptionalLiteral, optionalInput);
        if (optionalData->HasItem()) {
            return optionalInput.GetStaticType()->IsSameType(*defaultInput.GetStaticType())  ? optionalInput : optionalData->GetItem();
        } else {
            return defaultInput;
        }
    }

    return TRuntimeNode(&callable, false);
}

TRuntimeNode OptimizeExists(TCallable& callable, const TTypeEnvironment& env) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 arguments");

    auto optionalInput = callable.GetInput(0);
    if (optionalInput.HasValue()) {
        const bool has = AS_VALUE(TOptionalLiteral, optionalInput)->HasItem();
        return TRuntimeNode(BuildDataLiteral(NUdf::TUnboxedValuePod(has), NUdf::EDataSlot::Bool, env), true);
    }

    return TRuntimeNode(&callable, false);
}

TRuntimeNode OptimizeNth(TCallable& callable, const TTypeEnvironment& env) {
    Y_UNUSED(env);
    MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected 2 arguments");

    auto tupleInput = callable.GetInput(0);
    if (tupleInput.HasValue() && tupleInput.GetStaticType()->IsTuple()) {
        auto tuple = tupleInput.GetValue();
        auto indexData = AS_VALUE(TDataLiteral, callable.GetInput(1));
        const ui32 index = indexData->AsValue().Get<ui32>();

        const auto& value = static_cast<const TTupleLiteral&>(*tuple);
        MKQL_ENSURE(index < value.GetValuesCount(), "Index out of range");
        return value.GetValue(index);
    }

    return TRuntimeNode(&callable, false);
}

TRuntimeNode OptimizeExtend(TCallable& callable, const TTypeEnvironment& env) {
    auto returnType = callable.GetType()->GetReturnType();
    if (!returnType->IsList()) {
        return TRuntimeNode(&callable, false);
    }

    auto itemType = static_cast<TListType*>(returnType)->GetItemType();
    if (!itemType->IsVoid()) {
        return TRuntimeNode(&callable, false);
    }

    for (ui32 i = 0; i < callable.GetInputsCount(); ++i) {
        auto seq = callable.GetInput(i);
        auto seqType = seq.GetStaticType();

        MKQL_ENSURE(seqType->IsList(), "Expected list type in extend");
        MKQL_ENSURE(static_cast<TListType*>(seqType)->GetItemType()->IsVoid(), "Expected list of void");

        if (!seq.HasValue()) {
            return TRuntimeNode(&callable, false);
        }

        TListLiteral* listValue = AS_VALUE(TListLiteral, seq);
        if (listValue->GetItemsCount() != 0) {
            return TRuntimeNode(&callable, false);
        }
    }

    return TRuntimeNode(env.GetListOfVoidLazy(), true);
}

struct TOptimizationFuncMapFiller {
    THashMap<TString, TCallableVisitFunc> Map;
    TCallableVisitFuncProvider Provider;

    TOptimizationFuncMapFiller()
    {
        Map["If"] = &OptimizeIf;
        Map["Size"] = &OptimizeSize;
        Map["Length"] = &OptimizeLength;
        Map["AddMember"] = &OptimizeAddMember;
        Map["RemoveMember"] = &OptimizeRemoveMember;
        Map["Member"] = &OptimizeMember;
        Map["Filter"] = &OptimizeFilter;
        Map["Map"] = &OptimizeMap;
        Map["FlatMap"] = &OptimizeFlatMap;
        Map["Coalesce"] = &OptimizeCoalesce;
        Map["Exists"] = &OptimizeExists;
        Map["Nth"] = &OptimizeNth;
        Map["Extend"] = &OptimizeExtend;

        Provider = [&](TInternName name) {
            auto it = Map.find(name.Str());
            if (it != Map.end())
                return it->second;

            return TCallableVisitFunc();
        };
    }
};

} // namespace

TCallableVisitFuncProvider GetLiteralPropagationOptimizationFuncProvider() {
    return Singleton<TOptimizationFuncMapFiller>()->Provider;
}

TRuntimeNode LiteralPropagationOptimization(TRuntimeNode root, const TTypeEnvironment& env, bool inPlace) {
    TExploringNodeVisitor explorer;
    explorer.Walk(root.GetNode(), env);
    bool wereChanges = false;
    return SinglePassVisitCallables(root, explorer, GetLiteralPropagationOptimizationFuncProvider(), env, inPlace, wereChanges);
}

} // namespace NMiniKQL
} // namespace NKikimr
