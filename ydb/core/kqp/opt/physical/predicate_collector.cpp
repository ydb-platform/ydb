#include "predicate_collector.h"

#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/utils/log/log.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NNodes;

THashMap<TString, TString> IgnoreCaseSubstringMatchFunctions = {
    {"EqualsIgnoreCase", "String._yql_AsciiEqualsIgnoreCase"},
    {"StartsWithIgnoreCase", "String._yql_AsciiStartsWithIgnoreCase"},
    {"EndsWithIgnoreCase", "String._yql_AsciiEndsWithIgnoreCase"},
    {"StringContainsIgnoreCase", "String._yql_AsciiContainsIgnoreCase"}
};

namespace {

bool IsSupportedPredicate(const TCoCompare& predicate) {
    return !predicate.Ref().Content().starts_with("Aggr");
}

bool CheckSameColumn(const TExprBase &left, const TExprBase &right) {
    if (auto leftMember = left.Maybe<TCoMember>()) {
        if (auto rightMember = right.Maybe<TCoMember>()) {
            return (leftMember.Cast().Name() == rightMember.Cast().Name());
        }
    }
    return false;
}

bool IsSupportedDataType(const TCoDataCtor& node, bool allowOlapApply) {
    Y_UNUSED(allowOlapApply);
    if (node.Maybe<TCoBool>() || node.Maybe<TCoFloat>() || node.Maybe<TCoDouble>() || node.Maybe<TCoInt8>() || node.Maybe<TCoInt16>() ||
        node.Maybe<TCoInt32>() || node.Maybe<TCoInt64>() || node.Maybe<TCoUint8>() || node.Maybe<TCoUint16>() || node.Maybe<TCoUint32>() ||
        node.Maybe<TCoUint64>() || node.Maybe<TCoUtf8>() || node.Maybe<TCoString>() || node.Maybe<TCoDate>() || node.Maybe<TCoDate32>() ||
        node.Maybe<TCoDatetime>() || node.Maybe<TCoDatetime64>() || node.Maybe<TCoTimestamp64>() || node.Maybe<TCoInterval64>() || node.Maybe<TCoInterval>() ||
        node.Maybe<TCoTimestamp>()) {
        return true;
    }
    return false;
}

bool IsSupportedCast(const TCoSafeCast& cast, bool allowOlapApply) {
    auto maybeDataType = cast.Type().Maybe<TCoDataType>();
    if (!maybeDataType) {
        if (const auto maybeOptionalType = cast.Type().Maybe<TCoOptionalType>()) {
            maybeDataType = maybeOptionalType.Cast().ItemType().Maybe<TCoDataType>();
        }
    }
    YQL_ENSURE(maybeDataType.IsValid());

    if (allowOlapApply) {
        return true;
    }

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

bool IsGoodTypeForUnaryArithmeticPushdown(const TTypeAnnotationNode& type, bool allowOlapApply) {
    const auto features = NUdf::GetDataTypeInfo(RemoveOptionality(type).Cast<TDataExprType>()->GetSlot()).Features;
    return ((NUdf::EDataTypeFeatures::NumericType) & features)
        || (allowOlapApply && ((NUdf::EDataTypeFeatures::ExtDateType |
            NUdf::EDataTypeFeatures::DateType |
            NUdf::EDataTypeFeatures::TimeIntervalType) & features) && !(NUdf::EDataTypeFeatures::TzDateType & features));
}

bool IsGoodTypeForBinaryArithmeticPushdown(const TTypeAnnotationNode& type, bool allowOlapApply) {
    const auto features = NUdf::GetDataTypeInfo(RemoveOptionality(type).Cast<TDataExprType>()->GetSlot()).Features;
    return ((NUdf::EDataTypeFeatures::NumericType | NUdf::EDataTypeFeatures::DateType | NUdf::EDataTypeFeatures::TimeIntervalType) & features)
        || (allowOlapApply && ((NUdf::EDataTypeFeatures::ExtDateType |
            NUdf::EDataTypeFeatures::DateType |
            NUdf::EDataTypeFeatures::TimeIntervalType) & features) && !(NUdf::EDataTypeFeatures::TzDateType & features));
}

bool IsGoodTypeForComparsionPushdown(const TTypeAnnotationNode& type, bool allowOlapApply) {
    const auto features = NUdf::GetDataTypeInfo(RemoveOptionality(type).Cast<TDataExprType>()->GetSlot()).Features;
    if (features & NUdf::EDataTypeFeatures::DecimalType) {
        return false;
    }
    return (NUdf::EDataTypeFeatures::CanCompare & features) &&
           (((NUdf::EDataTypeFeatures::NumericType | NUdf::EDataTypeFeatures::StringType | NUdf::EDataTypeFeatures::DateType |
              NUdf::EDataTypeFeatures::TimeIntervalType) & features) ||
            (allowOlapApply &&
             ((NUdf::EDataTypeFeatures::ExtDateType | NUdf::EDataTypeFeatures::DateType | NUdf::EDataTypeFeatures::TimeIntervalType) & features) &&
             !(NUdf::EDataTypeFeatures::TzDateType & features)));
}

bool CanPushdownStringUdf(const TExprNode& udf, bool pushdownSubstring) {
    if (!pushdownSubstring) {
        return false;
    }
    const auto& name = udf.Head().Content();
    static const THashSet<TString> substringMatchUdfs = {
        "String._yql_AsciiEqualsIgnoreCase",

        "String.Contains",
        "String._yql_AsciiContainsIgnoreCase",
        "String.StartsWith",
        "String._yql_AsciiStartsWithIgnoreCase",
        "String.EndsWith",
        "String._yql_AsciiEndsWithIgnoreCase"
    };
    return substringMatchUdfs.contains(name);
}

bool AbstractTreeCanBePushed(const TExprBase& expr, const TExprNode*, bool pushdownSubstring) {
    if (!expr.Ref().IsCallable({"Apply", "Coalesce", "NamedApply", "IfPresent", "Visit"})) {
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
        if (!(udfName.Content().starts_with("Json2.") || udfName.Content().starts_with("Re2.") || CanPushdownStringUdf(udf, pushdownSubstring))) {
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

bool IfPresentCanBePushed(const TCoIfPresent& ifPresent, const TExprNode* lambdaArg, bool allowOlapApply) {

    Y_UNUSED(ifPresent);
    Y_UNUSED(lambdaArg);

    return allowOlapApply;
}

bool CheckExpressionNodeForPushdown(const TExprBase& node, const TExprNode* lambdaArg, const TPushdownOptions& options) {
    if (options.AllowOlapApply) {
        if (node.Maybe<TCoJust>() || node.Maybe<TCoCoalesce>()) {
            return true;
        }
        // Temporary fix for https://github.com/ydb-platform/ydb/issues/7967
        else if (auto ifPred = node.Maybe<TCoIf>()) {
            if (ifPred.ThenValue().Maybe<TCoNothing>() || ifPred.ElseValue().Maybe<TCoNothing>()) {
                return false;
            }
            return true;
        }
    }

    if (const auto maybeSafeCast = node.Maybe<TCoSafeCast>()) {
        return IsSupportedCast(maybeSafeCast.Cast(), options.AllowOlapApply);
    } else if (const auto maybeData = node.Maybe<TCoDataCtor>()) {
        return IsSupportedDataType(maybeData.Cast(), options.AllowOlapApply);
    } else if (const auto maybeMember = node.Maybe<TCoMember>()) {
        return IsMemberColumn(maybeMember.Cast(), lambdaArg);
    } else if (const auto maybeJsonValue = node.Maybe<TCoJsonValue>()) {
        const auto jsonOp = maybeJsonValue.Cast();
        return jsonOp.Json().Maybe<TCoMember>() && jsonOp.JsonPath().Maybe<TCoUtf8>();
    } else if (node.Maybe<TCoNull>() || node.Maybe<TCoParameter>() || node.Maybe<TCoJust>()) {
        return true;
    }

    if (const auto op = node.Maybe<TCoUnaryArithmetic>()) {
        return CheckExpressionNodeForPushdown(op.Cast().Arg(), lambdaArg, options) &&
               IsGoodTypeForUnaryArithmeticPushdown(*op.Cast().Ref().GetTypeAnn(), options.AllowOlapApply);
    } else if (const auto op = node.Maybe<TCoBinaryArithmetic>()) {
        // FIXME: CS should be able to handle bin arithmetic op with the same column.
        if (!options.AllowOlapApply && CheckSameColumn(op.Cast().Left(), op.Cast().Right())) {
            return false;
        }

        return CheckExpressionNodeForPushdown(op.Cast().Left(), lambdaArg, options) &&
               CheckExpressionNodeForPushdown(op.Cast().Right(), lambdaArg, options) &&
               IsGoodTypeForBinaryArithmeticPushdown(*op.Cast().Ref().GetTypeAnn(), options.AllowOlapApply) &&
               !op.Cast().Maybe<TCoAggrAdd>();
    }

    if (options.AllowOlapApply) {
        if (const auto maybeIfPresent = node.Maybe<TCoIfPresent>()) {
            return IfPresentCanBePushed(maybeIfPresent.Cast(), lambdaArg, options.AllowOlapApply);
        }
        return AbstractTreeCanBePushed(node, lambdaArg, options.PushdownSubstring);
    }

    return false;
}

bool IsGoodTypesForPushdownCompare(const TTypeAnnotationNode& typeOne, const TTypeAnnotationNode& typeTwo, const TPushdownOptions& options) {
    const auto& rawOne = RemoveOptionality(typeOne);
    const auto& rawTwo = RemoveOptionality(typeTwo);
    if (IsSameAnnotation(rawOne, rawTwo))
        return IsGoodTypeForComparsionPushdown(rawOne, options.AllowOlapApply);

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
                if (!IsGoodTypesForPushdownCompare(*itemsOne[i], *itemsTwo[i], options)) {
                    return false;
                }
            }
            return true;
        }
        case ETypeAnnotationKind::Data: {
            return IsGoodTypeForComparsionPushdown(typeOne, options.AllowOlapApply) && IsGoodTypeForComparsionPushdown(typeTwo, options.AllowOlapApply);
        }
        default:
            break;
    }
    return false;
}

bool CheckComparisonParametersForPushdown(const TCoCompare& compare, const TExprNode* lambdaArg, const TExprBase& input, const TPushdownOptions& options) {

    const auto* inputType = input.Ref().GetTypeAnn();
    switch (inputType->GetKind()) {
        case ETypeAnnotationKind::Flow:
            inputType = inputType->Cast<TFlowExprType>()->GetItemType();
            break;
        case ETypeAnnotationKind::Stream:
            inputType = inputType->Cast<TStreamExprType>()->GetItemType();
            break;
        case ETypeAnnotationKind::Optional:
            inputType = inputType->Cast<TOptionalExprType>()->GetItemType();
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
    // FIXME: CS should be able to handle bin cmp op with the same column.
    if (!options.AllowOlapApply && CheckSameColumn(compare.Left(), compare.Right())) {
        return false;
    }

    if (!IsGoodTypesForPushdownCompare(*compare.Left().Ref().GetTypeAnn(), *compare.Right().Ref().GetTypeAnn(), options)) {
        return false;
    }

    const auto leftList = GetComparisonNodes(compare.Left());
    const auto rightList = GetComparisonNodes(compare.Right());
    YQL_ENSURE(leftList.size() == rightList.size(), "Different sizes of lists in comparison!");

    for (size_t i = 0; i < leftList.size(); ++i) {
        if (!CheckExpressionNodeForPushdown(leftList[i], lambdaArg, options) || !CheckExpressionNodeForPushdown(rightList[i], lambdaArg, options)) {
            return false;
        }
    }

    if (options.PushdownSubstring) {
        if (IgnoreCaseSubstringMatchFunctions.contains(compare.CallableName())) {
            const auto& right = compare.Right().Ref();
            YQL_ENSURE(right.IsCallable("String") || right.IsCallable("Utf8"));
            const auto pattern = right.Child(0);
            YQL_ENSURE(pattern->IsAtom());
            if (UTF8Detect(pattern->Content()) != ASCII) {
                return false;
            }
        }
    }

    return true;
}

bool CompareCanBePushed(const TCoCompare& compare, const TExprNode* lambdaArg, const TExprBase& lambdaBody, const TPushdownOptions& options) {
    return IsSupportedPredicate(compare) && CheckComparisonParametersForPushdown(compare, lambdaArg, lambdaBody, options);
}

bool SafeCastCanBePushed(const TCoFlatMap& flatmap, const TExprNode* lambdaArg, const TPushdownOptions& options) {
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
        if (!CheckExpressionNodeForPushdown(leftList[i], lambdaArg, options) || !CheckExpressionNodeForPushdown(rightList[i], lambdaArg, options)) {
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

bool CoalesceCanBePushed(const TCoCoalesce& coalesce, const TExprNode* lambdaArg, const TExprBase& lambdaBody, const TPushdownOptions& options) {
    if (!coalesce.Value().Maybe<TCoBool>()) {
        return false;
    }

    const auto predicate = coalesce.Predicate();
    if (const auto maybeCompare = predicate.Maybe<TCoCompare>()) {
        return CompareCanBePushed(maybeCompare.Cast(), lambdaArg, lambdaBody, options);
    } else if (const auto maybeFlatmap = predicate.Maybe<TCoFlatMap>()) {
        return SafeCastCanBePushed(maybeFlatmap.Cast(), lambdaArg, options);
    } else if (const auto maybeJsonExists = predicate.Maybe<TCoJsonExists>()) {
        return JsonExistsCanBePushed(maybeJsonExists.Cast(), lambdaArg);
    } else if (const auto maybeIfPresent = predicate.Maybe<TCoIfPresent>()) {
        return IfPresentCanBePushed(maybeIfPresent.Cast(), lambdaArg, options.AllowOlapApply);
    }

    return false;
}

bool ExistsCanBePushed(const TCoExists& exists, const TExprNode* lambdaArg) {
    return IsMemberColumn(exists.Optional(), lambdaArg);
}

void CollectChildrenPredicates(const TExprNode& opNode, TOLAPPredicateNode& predicateTree, const TExprNode* lambdaArg, const TExprBase& lambdaBody, const TPushdownOptions& options) {
    predicateTree.Children.reserve(opNode.ChildrenSize());
    predicateTree.CanBePushed = true;
    predicateTree.CanBePushedApply = true;

    for (const auto& childNodePtr: opNode.Children()) {
        TOLAPPredicateNode child;
        child.ExprNode = childNodePtr;
        if (const auto maybeCtor = TMaybeNode<TCoDataCtor>(child.ExprNode)) {
            child.CanBePushed = IsSupportedDataType(maybeCtor.Cast(), false);
            child.CanBePushedApply = IsSupportedDataType(maybeCtor.Cast(), true);
        }
        else {
            CollectPredicates(TExprBase(child.ExprNode), child, lambdaArg, lambdaBody, options);
        }
        predicateTree.Children.emplace_back(child);
        predicateTree.CanBePushed &= child.CanBePushed;
        predicateTree.CanBePushedApply &= child.CanBePushedApply;
    }
}

} // namespace

void CollectPredicates(const TExprBase& predicate, TOLAPPredicateNode& predicateTree, const TExprNode* lambdaArg, const TExprBase& lambdaBody, const TPushdownOptions& options) {
    if (predicate.Maybe<TCoNot>() || predicate.Maybe<TCoAnd>() || predicate.Maybe<TCoOr>() || predicate.Maybe<TCoXor>()) {
        CollectChildrenPredicates(predicate.Ref(), predicateTree, lambdaArg, lambdaBody, options);
    } else if (const auto maybeCoalesce = predicate.Maybe<TCoCoalesce>()) {
        predicateTree.CanBePushed = CoalesceCanBePushed(maybeCoalesce.Cast(), lambdaArg, lambdaBody, {false, options.PushdownSubstring});
        predicateTree.CanBePushedApply = CoalesceCanBePushed(maybeCoalesce.Cast(), lambdaArg, lambdaBody, {true, options.PushdownSubstring});
    } else if (const auto maybeCompare = predicate.Maybe<TCoCompare>()) {
        predicateTree.CanBePushed = CompareCanBePushed(maybeCompare.Cast(), lambdaArg, lambdaBody, {false, options.PushdownSubstring});
        predicateTree.CanBePushedApply = CompareCanBePushed(maybeCompare.Cast(), lambdaArg, lambdaBody, {true, options.PushdownSubstring});
    } else if (const auto maybeExists = predicate.Maybe<TCoExists>()) {
        predicateTree.CanBePushed = ExistsCanBePushed(maybeExists.Cast(), lambdaArg);
        predicateTree.CanBePushedApply = predicateTree.CanBePushed;
    } else if (const auto maybeJsonExists = predicate.Maybe<TCoJsonExists>()) {
        predicateTree.CanBePushed = JsonExistsCanBePushed(maybeJsonExists.Cast(), lambdaArg);
        predicateTree.CanBePushedApply = predicateTree.CanBePushed;
    }

    if (options.AllowOlapApply && !predicateTree.CanBePushedApply){
        if (predicate.Maybe<TCoIf>() || predicate.Maybe<TCoJust>() || predicate.Maybe<TCoCoalesce>()) {
            CollectChildrenPredicates(predicate.Ref(), predicateTree, lambdaArg, lambdaBody, {true, options.PushdownSubstring});    
        }
        if (!predicateTree.CanBePushedApply) {
            predicateTree.CanBePushedApply =  AbstractTreeCanBePushed(predicate, lambdaArg, options.PushdownSubstring);
        }
    }
}
} //namespace NKikimr::NKqp::NOpt
