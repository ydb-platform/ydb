#include "kqp_opt_impl.h"

#include <ydb/library/yql/utils/utf8.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NNodes;
using namespace NYql::NCommon;

namespace {

template<typename T>
TTableLookup::TCompareResult::TResult CompareValues(const T& left, const T& right) {
    if (left == right) {
        return TTableLookup::TCompareResult::Equal;
    } else {
        return left > right
            ? TTableLookup::TCompareResult::Greater
            : TTableLookup::TCompareResult::Less;
    }
}

template<typename T>
TTableLookup::TCompareResult CompareIntegralNodes(TCoAtom left, TCoAtom right, NKikimr::NUdf::EDataSlot slot) {
    T leftValue = FromString<T>(left.Ref(), slot);
    T rightValue = FromString<T>(right.Ref(), slot);
    auto compareResult = CompareValues(leftValue, rightValue);

    TMaybe<bool> adjacent;
    switch (compareResult) {
        case TTableLookup::TCompareResult::Equal:
            break;

        case TTableLookup::TCompareResult::Greater:
            adjacent = leftValue == rightValue + 1;
            break;

        case TTableLookup::TCompareResult::Less:
            adjacent = rightValue == leftValue + 1;
            break;
    }

    return TTableLookup::TCompareResult(compareResult, adjacent);
}

template<typename T>
TTableLookup::TCompareResult CompareNodes(TCoAtom left, TCoAtom right, NKikimr::NUdf::EDataSlot slot) {
    T leftValue = FromString<T>(left.Ref(), slot);
    T rightValue = FromString<T>(right.Ref(), slot);
    return TTableLookup::TCompareResult(CompareValues(leftValue, rightValue));
}

template<>
TTableLookup::TCompareResult CompareNodes<bool>(TCoAtom left, TCoAtom right, NKikimr::NUdf::EDataSlot slot) {
    bool leftValue = FromString<bool>(left.Ref(), slot);
    bool rightValue = FromString<bool>(right.Ref(), slot);
    auto compareResult = CompareValues(leftValue, rightValue);

    return TTableLookup::TCompareResult(compareResult);
}

template<>
TTableLookup::TCompareResult CompareNodes<ui64>(TCoAtom left, TCoAtom right, NKikimr::NUdf::EDataSlot slot) {
    return CompareIntegralNodes<ui64>(left, right, slot);
}

template<>
TTableLookup::TCompareResult CompareNodes<i64>(TCoAtom left, TCoAtom right, NKikimr::NUdf::EDataSlot slot) {
    return CompareIntegralNodes<i64>(left, right, slot);
}

template<>
TTableLookup::TCompareResult CompareNodes<TString>(TCoAtom left, TCoAtom right, NKikimr::NUdf::EDataSlot slot) {
    Y_UNUSED(slot);

    const auto& leftValue = left.Value();
    const auto& rightValue = right.Value();
    return TTableLookup::TCompareResult(CompareValues(leftValue, rightValue));
}

} // namespace

bool KqpTableLookupCanCompare(TExprBase node) {
    if (node.Maybe<TCoBool>()) {
        return true;
    }

    if (node.Maybe<TCoIntegralCtor>()) {
        return true;
    }

    if (node.Maybe<TCoString>()) {
        return true;
    }

    if (node.Maybe<TCoUtf8>()) {
        return true;
    }

    return false;
}

TMaybeNode<TExprBase> KqpTableLookupGetValue(TExprBase node, const TTypeAnnotationNode* type,
    TExprContext& ctx)
{
    const TTypeAnnotationNode* targetType = type;
    bool isTargetOptional = false;
    if (type->GetKind() == ETypeAnnotationKind::Optional) {
        targetType = type->Cast<TOptionalExprType>()->GetItemType();
        isTargetOptional = true;
    }

    if (targetType->GetKind() != ETypeAnnotationKind::Data) {
        return TMaybeNode<TExprBase>();
    }

    THashSet<const TExprNode*> knownArgs;
    bool canPush = true;
    VisitExpr(node.Ptr(), [&knownArgs, &canPush] (const TExprNode::TPtr& exprNode) {
        auto node = TExprBase(exprNode);

        if (!canPush) {
            return false;
        }

        if (auto maybeLambda = node.Maybe<TCoLambda>()) {
            for (const auto& arg : maybeLambda.Cast().Args()) {
                knownArgs.emplace(arg.Raw());
            }
        }

        if (auto maybeArg = node.Maybe<TCoArgument>()) {
            if (!knownArgs.contains(maybeArg.Cast().Raw())) {
                canPush = false;
                return false;
            }
        }

        return true;
    });

    if (!canPush) {
        return TMaybeNode<TExprBase>();
    }

    const auto& dataTypeName = targetType->Cast<TDataExprType>()->GetName();

    TExprBase valueNode = node;
    if (isTargetOptional) {
        if (auto maybeJust = node.Maybe<TCoJust>()) {
            valueNode = maybeJust.Cast().Input();
        }

        if (node.Maybe<TCoNothing>()) {
            return Build<TCoNothing>(ctx, node.Pos())
                .OptionalType(ExpandType(node.Pos(), *type, ctx))
                .Done()
                .Ptr();
        }
    }

    TExprNode::TPtr literal;
    if (auto maybeInt = valueNode.Maybe<TCoIntegralCtor>()) {
        if (maybeInt.Cast().CallableName() == dataTypeName) {
            return valueNode;
        }

        if (AllowIntegralConversion(maybeInt.Cast(), false, NKikimr::NUdf::GetDataSlot(dataTypeName))) {
            literal = maybeInt.Cast().Literal().Ptr();
        }
    }

    if (auto maybeString = valueNode.Maybe<TCoString>()) {
        if (dataTypeName == "String") {
            return valueNode;
        }

        if (dataTypeName == "Utf8") {
            auto atom = maybeString.Cast().Literal();
            auto value = atom.Value();
            if (!IsUtf8(value)) {
                return {};
            }

            literal = atom.Ptr();
        }
    }

    if (auto maybeUtf8 = valueNode.Maybe<TCoUtf8>()) {
        if (dataTypeName == "String" || dataTypeName == "Utf8") {
            literal = maybeUtf8.Cast().Literal().Ptr();
        }
    }

    if (auto maybeBool = valueNode.Maybe<TCoBool>()) {
        if (dataTypeName == "Bool") {
            literal = maybeBool.Cast().Literal().Ptr();
        }
    }

    if (literal) {
        auto ret = ctx.Builder(valueNode.Pos())
            .Callable(dataTypeName)
                .Add(0, literal)
                .Seal()
            .Build();

        return ret;
    }

    auto valueType = valueNode.Ref().GetTypeAnn();
    if (isTargetOptional && valueType->GetKind() == ETypeAnnotationKind::Optional) {
        valueType = valueType->Cast<TOptionalExprType>()->GetItemType();
    }

    if (valueType->GetKind() == ETypeAnnotationKind::Data &&
        valueType->Cast<TDataExprType>()->GetName() == dataTypeName)
    {
        return node;
    }

    return Build<TCoConvert>(ctx, node.Pos())
        .Input(node)
        .Type().Build(dataTypeName)
        .Done();
}

TTableLookup::TCompareResult KqpTableLookupCompare(TExprBase left, TExprBase right) {
    if (left.Maybe<TCoBool>() && right.Maybe<TCoBool>()) {
        return CompareNodes<bool>(left.Cast<TCoBool>().Literal(),
            right.Cast<TCoBool>().Literal(), NKikimr::NUdf::EDataSlot::Bool);
    }

    if (left.Maybe<TCoUint64>() && right.Maybe<TCoUint64>()) {
        return CompareNodes<ui64>(left.Cast<TCoUint64>().Literal(),
            right.Cast<TCoUint64>().Literal(), NKikimr::NUdf::EDataSlot::Uint64);
    }

    if (left.Maybe<TCoIntegralCtor>() && right.Maybe<TCoIntegralCtor>()) {
        return CompareNodes<i64>(left.Cast<TCoIntegralCtor>().Literal(),
            right.Cast<TCoIntegralCtor>().Literal(), NKikimr::NUdf::EDataSlot::Int64);
    }

    if (left.Maybe<TCoString>() && right.Maybe<TCoString>() ||
        left.Maybe<TCoUtf8>() && right.Maybe<TCoUtf8>())
    {
        return CompareNodes<TString>(left.Cast<TCoDataCtor>().Literal(),
            right.Cast<TCoDataCtor>().Literal(), NKikimr::NUdf::EDataSlot::String);
    }

    YQL_ENSURE(false, "Unexpected nodes in Kikimr TableLookupCompare: (" << left.Ref().Content()
        << ", " << right.Ref().Content() << ")");
}

} // namespace NKikimr::NKqp::NOpt
