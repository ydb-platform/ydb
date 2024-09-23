#include "predicate_collector.h"

#include <ydb/core/formats/arrow/ssa_runtime_version.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NNodes;

namespace {

bool IsSupportedPredicate(const TCoCompare& predicate) {
    return !predicate.Ref().Content().starts_with("Aggr");
}

bool IsSupportedDataType(const TCoDataCtor& node) {
    if (node.Maybe<TCoBool>() ||
        node.Maybe<TCoFloat>() ||
        node.Maybe<TCoDouble>() ||
        node.Maybe<TCoInt8>() ||
        node.Maybe<TCoInt16>() ||
        node.Maybe<TCoInt32>() ||
        node.Maybe<TCoInt64>() ||
        node.Maybe<TCoUint8>() ||
        node.Maybe<TCoUint16>() ||
        node.Maybe<TCoUint32>() ||
        node.Maybe<TCoUint64>() ||
        node.Maybe<TCoUtf8>() ||
        node.Maybe<TCoString>()) {
        return true;
    }

    if constexpr (NKikimr::NSsa::RuntimeVersion >= 4U) {
        if (node.Maybe<TCoTimestamp>()) {
            return true;
        }
    }

    if constexpr (NKikimr::NSsa::RuntimeVersion >= 5U) {
        if (node.Maybe<TCoDate32>() ||  node.Maybe<TCoDatetime64>() || node.Maybe<TCoTimestamp64>() || node.Maybe<TCoInterval64>()) {
            return true;
        }
    }

    return false;
}

bool IsSupportedCast(const TCoSafeCast& cast) {
    auto maybeDataType = cast.Type().Maybe<TCoDataType>();
    if (!maybeDataType) {
        if (const auto maybeOptionalType = cast.Type().Maybe<TCoOptionalType>()) {
            maybeDataType = maybeOptionalType.Cast().ItemType().Maybe<TCoDataType>();
        }
    }
    YQL_ENSURE(maybeDataType.IsValid());

    const auto dataType = maybeDataType.Cast();
    if (dataType.Type().Value() == "Int32") { // TODO: Support any numeric casts.
        return cast.Value().Maybe<TCoString>() || cast.Value().Maybe<TCoUtf8>();
    }
    return false;
}

std::vector<TExprBase> GetComparisonNodes(const TExprBase& node) {
    std::vector<TExprBase> res;
    if (node.Maybe<TExprList>()) {
        auto nodeList = node.Cast<TExprList>();
        res.reserve(nodeList.Size());
        for (size_t i = 0; i < nodeList.Size(); ++i) {
            res.emplace_back(nodeList.Item(i));
        }
    } else {
        res.emplace_back(node);
    }
    return res;
}

bool IsMemberColumn(const TCoMember& member, const TExprNode* lambdaArg) {
    return member.Struct().Raw() == lambdaArg;
}

bool IsMemberColumn(const TExprBase& node, const TExprNode* lambdaArg) {
    if (const auto member = node.Maybe<TCoMember>()) {
        return IsMemberColumn(member.Cast(), lambdaArg);
    }
    return false;
}

bool IsGoodTypeForArithmeticPushdown(const TTypeAnnotationNode& type) {
    const auto fatures = NUdf::GetDataTypeInfo(RemoveOptionality(type).Cast<TDataExprType>()->GetSlot()).Features;
    return NUdf::EDataTypeFeatures::NumericType & fatures
        || (NKikimr::NSsa::RuntimeVersion >= 5U && (NUdf::EDataTypeFeatures::BigDateType & fatures) && !(NUdf::EDataTypeFeatures::TzDateType & fatures));
}

bool IsGoodTypeForComparsionPushdown(const TTypeAnnotationNode& type) {
    const auto fatures = NUdf::GetDataTypeInfo(RemoveOptionality(type).Cast<TDataExprType>()->GetSlot()).Features;
    return (NUdf::EDataTypeFeatures::CanCompare  & fatures)
        && (((NUdf::EDataTypeFeatures::NumericType | NUdf::EDataTypeFeatures::StringType) & fatures) ||
            (NKikimr::NSsa::RuntimeVersion >= 5U && (NUdf::EDataTypeFeatures::BigDateType & fatures) && !(NUdf::EDataTypeFeatures::TzDateType & fatures)));
}

[[maybe_unused]]
bool AbstractTreeCanBePushed(const TExprBase& expr, const TExprNode* ) {
    if (!expr.Ref().IsCallable({"Apply", "NamedApply", "IfPresent", "Visit"})) {
        return false;
    }

    if (FindNode(expr.Ptr(), [] (const TExprNode::TPtr& node) { return node->IsCallable({"Ensure", "Unwrap"}); })) {
        return false;
    }

    const auto applies = FindNodes(expr.Ptr(), [] (const TExprNode::TPtr& node) {
        return node->IsCallable({"Apply", "NamedApply"}) && (node->Head().IsCallable("Udf") || (node->Head().IsCallable("AssumeStrict") && node->Head().Head().IsCallable("Udf") ));
    });

    if (applies.empty()) {
        return false;
    }

    for (const auto& apply : applies) {
        const auto& udf = SkipCallables(apply->Head(), {"AssumeStrict"});
        const auto& udfName = udf.Head();
        if (!(udfName.Content().starts_with("Json2.") || udfName.Content().starts_with("Re2."))) {
            return false;
        }

        if (udfName.IsAtom("Json2.CompilePath") && !apply->Tail().IsCallable("Utf8")) {
            return false;
        }

        // Pushdonw only SQL LIKE or ILIKE.
        constexpr auto like = "Re2.PatternFromLike"sv;
        if (udfName.Content().starts_with("Re2.") && !udfName.IsAtom({like, "Re2.Options"}) &&
            !FindNode(udf.ChildPtr(1U), [like] (const TExprNode::TPtr& node) { return node->IsCallable("Udf") && node->Head().IsAtom(like); })) {
            return false;
        }
    }

    return true;
}

bool CheckExpressionNodeForPushdown(const TExprBase& node, const TExprNode* lambdaArg) {
    if constexpr (NKikimr::NSsa::RuntimeVersion >= 5U) {
        if (node.Maybe<TCoIf>() || node.Maybe<TCoJust>() || node.Maybe<TCoCoalesce>()) {
            return true;
        }
    }

    if (const auto maybeSafeCast = node.Maybe<TCoSafeCast>()) {
        return IsSupportedCast(maybeSafeCast.Cast());
    } else if (const auto maybeData = node.Maybe<TCoDataCtor>()) {
        return IsSupportedDataType(maybeData.Cast());
    } else if (const auto maybeMember = node.Maybe<TCoMember>()) {
        return IsMemberColumn(maybeMember.Cast(), lambdaArg);
    } else if (const auto maybeJsonValue = node.Maybe<TCoJsonValue>()) {
        const auto jsonOp = maybeJsonValue.Cast();
        return jsonOp.Json().Maybe<TCoMember>() && jsonOp.JsonPath().Maybe<TCoUtf8>();
    } else if (node.Maybe<TCoNull>() || node.Maybe<TCoParameter>()) {
        return true;
    }

    if constexpr (NKikimr::NSsa::RuntimeVersion >= 4U) {
        if (const auto op = node.Maybe<TCoUnaryArithmetic>()) {
            return CheckExpressionNodeForPushdown(op.Cast().Arg(), lambdaArg) && IsGoodTypeForArithmeticPushdown(*op.Cast().Ref().GetTypeAnn());
        } else if (const auto op = node.Maybe<TCoBinaryArithmetic>()) {
            return CheckExpressionNodeForPushdown(op.Cast().Left(), lambdaArg) && CheckExpressionNodeForPushdown(op.Cast().Right(), lambdaArg)
                && IsGoodTypeForArithmeticPushdown(*op.Cast().Ref().GetTypeAnn()) && !op.Cast().Maybe<TCoAggrAdd>();
        }
    }

    if constexpr (NKikimr::NSsa::RuntimeVersion >= 5U) {
        return AbstractTreeCanBePushed(node, lambdaArg);
    }

    return false;
}

bool IsGoodTypesForPushdownCompare(const TTypeAnnotationNode& typeOne, const TTypeAnnotationNode& typeTwo) {
    const auto& rawOne = RemoveOptionality(typeOne);
    const auto& rawTwo = RemoveOptionality(typeTwo);
    if (IsSameAnnotation(rawOne, rawTwo))
        return true;

    const auto kindOne = rawOne.GetKind();
    const auto kindTwo = rawTwo.GetKind();
    if (ETypeAnnotationKind::Null == kindOne || ETypeAnnotationKind::Null == kindTwo)
        return true;

    if (kindTwo != kindOne)
        return false;

    switch (kindOne) {
        case ETypeAnnotationKind::Tuple: {
            const auto& itemsOne = rawOne.Cast<TTupleExprType>()->GetItems();
            const auto& itemsTwo = rawTwo.Cast<TTupleExprType>()->GetItems();
            const auto size = itemsOne.size();
            if (size != itemsTwo.size())
                return false;
            for (auto i = 0U; i < size; ++i) {
                if (!IsGoodTypesForPushdownCompare(*itemsOne[i], *itemsTwo[i])) {
                    return false;
                }
            }
            return true;
        }
        case ETypeAnnotationKind::Data:
            return IsGoodTypeForComparsionPushdown(typeOne) && IsGoodTypeForComparsionPushdown(typeTwo);
        default:
            break;
    }
    return false;
}

bool CheckComparisonParametersForPushdown(const TCoCompare& compare, const TExprNode* lambdaArg, const TExprBase& input) {
    const auto* inputType = input.Ref().GetTypeAnn();
    switch (inputType->GetKind()) {
        case ETypeAnnotationKind::Flow:
            inputType = inputType->Cast<TFlowExprType>()->GetItemType();
            break;
        case ETypeAnnotationKind::Stream:
            inputType = inputType->Cast<TStreamExprType>()->GetItemType();
            break;
        case ETypeAnnotationKind::Struct:
            break;
        default:
            YQL_ENSURE(false, "Unsupported type of incoming data: " << (ui32)inputType->GetKind());
            // We do not know how process input that is not a sequence of elements
            return false;
    }
    YQL_ENSURE(inputType->GetKind() == ETypeAnnotationKind::Struct);

    if (inputType->GetKind() != ETypeAnnotationKind::Struct) {
        // We do not know how process input that is not a sequence of elements
        return false;
    }

    if (!IsGoodTypesForPushdownCompare(*compare.Left().Ref().GetTypeAnn(), *compare.Right().Ref().GetTypeAnn())) {
        return false;
    }

    const auto leftList = GetComparisonNodes(compare.Left());
    const auto rightList = GetComparisonNodes(compare.Right());
    YQL_ENSURE(leftList.size() == rightList.size(), "Different sizes of lists in comparison!");

    for (size_t i = 0; i < leftList.size(); ++i) {
        if (!CheckExpressionNodeForPushdown(leftList[i], lambdaArg) || !CheckExpressionNodeForPushdown(rightList[i], lambdaArg)) {
            return false;
        }
    }

    return true;
}

bool CompareCanBePushed(const TCoCompare& compare, const TExprNode* lambdaArg, const TExprBase& lambdaBody) {
    return IsSupportedPredicate(compare) && CheckComparisonParametersForPushdown(compare, lambdaArg, lambdaBody);
}

bool SafeCastCanBePushed(const TCoFlatMap& flatmap, const TExprNode* lambdaArg) {
    /*
     * There are three ways of comparison in following format:
     *
     * FlatMap (LeftArgument, FlatMap(RightArgument(), Just(Predicate))
     *
     * Examples:
     * FlatMap (SafeCast(), FlatMap(Member(), Just(Comparison))
     * FlatMap (Member(), FlatMap(SafeCast(), Just(Comparison))
     * FlatMap (SafeCast(), FlatMap(SafeCast(), Just(Comparison))
     */
    const auto maybeFlatmap = flatmap.Lambda().Body().Maybe<TCoFlatMap>();
    if (!maybeFlatmap) {
        return false;
    }

    const auto leftList = GetComparisonNodes(flatmap.Input());
    const auto rightList = GetComparisonNodes(maybeFlatmap.Cast().Input());
    YQL_ENSURE(leftList.size() == rightList.size(), "Different sizes of lists in comparison!");

    for (size_t i = 0; i < leftList.size(); ++i) {
        if (!CheckExpressionNodeForPushdown(leftList[i], lambdaArg) || !CheckExpressionNodeForPushdown(rightList[i], lambdaArg)) {
            return false;
        }
    }

    const auto maybeJust = maybeFlatmap.Cast().Lambda().Body().Maybe<TCoJust>();
    if (!maybeJust) {
        return false;
    }

    const auto maybePredicate = maybeJust.Cast().Input().Maybe<TCoCompare>();
    if (!maybePredicate) {
        return false;
    }

    if (!IsSupportedPredicate(maybePredicate.Cast())) {
        return false;
    }

    return true;
}

bool JsonExistsCanBePushed(const TCoJsonExists& jsonExists, const TExprNode* lambdaArg) {
    const auto maybeMember = jsonExists.Json().Maybe<TCoMember>();
    if (!maybeMember || !jsonExists.JsonPath().Maybe<TCoUtf8>()) {
        // Currently we support only simple columns in pushdown
        return false;
    }
    if (!IsMemberColumn(maybeMember.Cast(), lambdaArg)) {
        return false;
    }
    return true;
}

bool CoalesceCanBePushed(const TCoCoalesce& coalesce, const TExprNode* lambdaArg, const TExprBase& lambdaBody) {
    if (!coalesce.Value().Maybe<TCoBool>()) {
        return false;
    }

    const auto predicate = coalesce.Predicate();
    if (const auto maybeCompare = predicate.Maybe<TCoCompare>()) {
        return CompareCanBePushed(maybeCompare.Cast(), lambdaArg, lambdaBody);
    } else if (const auto maybeFlatmap = predicate.Maybe<TCoFlatMap>()) {
        return SafeCastCanBePushed(maybeFlatmap.Cast(), lambdaArg);
    } else if (const auto maybeJsonExists = predicate.Maybe<TCoJsonExists>()) {
        return JsonExistsCanBePushed(maybeJsonExists.Cast(), lambdaArg);
    }

    return false;
}

bool ExistsCanBePushed(const TCoExists& exists, const TExprNode* lambdaArg) {
    return IsMemberColumn(exists.Optional(), lambdaArg);
}

void CollectChildrenPredicates(const TExprNode& opNode, TOLAPPredicateNode& predicateTree, const TExprNode* lambdaArg, const TExprBase& lambdaBody) {
    predicateTree.Children.reserve(opNode.ChildrenSize());
    predicateTree.CanBePushed = true;
    for (const auto& childNodePtr: opNode.Children()) {
        TOLAPPredicateNode child;
        child.ExprNode = childNodePtr;
        if (const auto maybeCtor = TMaybeNode<TCoDataCtor>(child.ExprNode))
            child.CanBePushed = IsSupportedDataType(maybeCtor.Cast());
        else
            CollectPredicates(TExprBase(child.ExprNode), child, lambdaArg, lambdaBody);
        predicateTree.Children.emplace_back(child);
        predicateTree.CanBePushed &= child.CanBePushed;
    }
}

}

void CollectPredicates(const TExprBase& predicate, TOLAPPredicateNode& predicateTree, const TExprNode* lambdaArg, const TExprBase& lambdaBody) {
    if constexpr (NKikimr::NSsa::RuntimeVersion >= 5U) {
        if (predicate.Maybe<TCoIf>() || predicate.Maybe<TCoJust>() || predicate.Maybe<TCoCoalesce>()) {
            return CollectChildrenPredicates(predicate.Ref(), predicateTree, lambdaArg, lambdaBody);
        }
    }

    if (predicate.Maybe<TCoNot>() || predicate.Maybe<TCoAnd>() || predicate.Maybe<TCoOr>() || predicate.Maybe<TCoXor>()) {
        return CollectChildrenPredicates(predicate.Ref(), predicateTree, lambdaArg, lambdaBody);
    } else if (const auto maybeCoalesce = predicate.Maybe<TCoCoalesce>()) {
        predicateTree.CanBePushed = CoalesceCanBePushed(maybeCoalesce.Cast(), lambdaArg, lambdaBody);
    } else if (const auto maybeCompare = predicate.Maybe<TCoCompare>()) {
        predicateTree.CanBePushed = CompareCanBePushed(maybeCompare.Cast(), lambdaArg, lambdaBody);
    } else if (const auto maybeExists = predicate.Maybe<TCoExists>()) {
        predicateTree.CanBePushed = ExistsCanBePushed(maybeExists.Cast(), lambdaArg);
    } else if (const auto maybeJsonExists = predicate.Maybe<TCoJsonExists>()) {
        predicateTree.CanBePushed = JsonExistsCanBePushed(maybeJsonExists.Cast(), lambdaArg);
    } else {
        if constexpr (NKikimr::NSsa::RuntimeVersion >= 5U) {
            predicateTree.CanBePushed = AbstractTreeCanBePushed(predicate, lambdaArg);
        } else {
            predicateTree.CanBePushed = false;
        }
    }
}

}
