#include "kqp_opt_phy_rules.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/library/yql/core/extract_predicate/extract_predicate.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NNodes;

namespace {

static TMaybeNode<TExprBase> NullNode = TMaybeNode<TExprBase>();

bool IsFalseLiteral(TExprBase node) {
    return node.Maybe<TCoBool>() && !FromString<bool>(node.Cast<TCoBool>().Literal().Value());
}

bool IsSupportedPredicate(const TCoCompare& predicate) {
    if (predicate.Maybe<TCoCmpEqual>()) {
        return true;
    }

    if (predicate.Maybe<TCoCmpLess>()) {
        return true;
    }

    if (predicate.Maybe<TCoCmpGreater>()) {
        return true;
    }

    if (predicate.Maybe<TCoCmpNotEqual>()) {
        return true;
    }

    if (predicate.Maybe<TCoCmpGreaterOrEqual>()) {
        return true;
    }

    if (predicate.Maybe<TCoCmpLessOrEqual>()) {
        return true;
    }

    return false;
}

bool ValidateIfArgument(const TCoOptionalIf& optionalIf, const TExprNode* rawLambdaArg) {
    // Check it is SELECT * or SELECT `field1`, `field2`...
    if (optionalIf.Value().Raw() == rawLambdaArg) {
        return true;
    }

    // Ok, maybe it is SELECT `field1`, `field2` ?
    auto maybeAsStruct = optionalIf.Value().Maybe<TCoAsStruct>();
    if (!maybeAsStruct) {
        return false;
    }

    for (auto arg : maybeAsStruct.Cast()) {
        // Check that second tuple element is Member(lambda arg)
        auto tuple = arg.Maybe<TExprList>().Cast();
        if (tuple.Size() != 2) {
            return false;
        }

        auto maybeMember = tuple.Item(1).Maybe<TCoMember>();
        if (!maybeMember) {
            return false;
        }

        auto member = maybeMember.Cast();
        if (member.Struct().Raw() != rawLambdaArg) {
            return false;
        }
    }

    return true;
}

bool IsSupportedDataType(const TCoDataCtor& node) {
    if (node.Maybe<TCoUtf8>() ||
        node.Maybe<TCoString>() ||
        node.Maybe<TCoBool>() ||
        node.Maybe<TCoFloat>() ||
        node.Maybe<TCoDouble>() ||
        node.Maybe<TCoInt8>() ||
        node.Maybe<TCoInt16>() ||
        node.Maybe<TCoInt32>() ||
        node.Maybe<TCoInt64>() ||
        node.Maybe<TCoUint8>() ||
        node.Maybe<TCoUint16>() ||
        node.Maybe<TCoUint32>() ||
        node.Maybe<TCoUint64>())
    {
        return true;
    }

    return false;
}

bool IsSupportedCast(const TCoSafeCast& cast) {
    auto maybeDataType = cast.Type().Maybe<TCoDataType>();
    YQL_ENSURE(maybeDataType.IsValid());

    auto dataType = maybeDataType.Cast();
    if (dataType.Type().Value() == "Int32") {
        return cast.Value().Maybe<TCoString>().IsValid();
    } else if (dataType.Type().Value() == "Timestamp") {
        return cast.Value().Maybe<TCoUint32>().IsValid();
    }
    return false;
}

bool IsComparableTypes(const TExprBase& leftNode, const TExprBase& rightNode, bool equality,
    const TTypeAnnotationNode* inputType)
{
    const TExprNode::TPtr leftPtr = leftNode.Ptr();
    const TExprNode::TPtr rightPtr = rightNode.Ptr();

    auto getDataType = [inputType](const TExprNode::TPtr& node) {
        auto type = node->GetTypeAnn();

        if (type->GetKind() == ETypeAnnotationKind::Unit) {
            auto rowType = inputType->Cast<TStructExprType>();
            type = rowType->FindItemType(node->Content());
        }

        if (type->GetKind() == ETypeAnnotationKind::Optional) {
            type = type->Cast<TOptionalExprType>()->GetItemType();
        }

        return type;
    };

    auto defaultCompare = [equality](const TTypeAnnotationNode* left, const TTypeAnnotationNode* right) {
        if (equality) {
            return CanCompare<true>(left, right);
        }

        return CanCompare<false>(left, right);
    };

    auto canCompare = [&defaultCompare](const TTypeAnnotationNode* left, const TTypeAnnotationNode* right) {
        if (left->GetKind() != ETypeAnnotationKind::Data ||
            right->GetKind() != ETypeAnnotationKind::Data)
        {
            return defaultCompare(left, right);
        }

        auto leftTypeId = GetDataTypeInfo(left->Cast<TDataExprType>()->GetSlot()).TypeId;
        auto rightTypeId = GetDataTypeInfo(right->Cast<TDataExprType>()->GetSlot()).TypeId;

        if (leftTypeId == rightTypeId) {
            return ECompareOptions::Comparable;
        }

        /*
         * Check special case UInt32 <-> Datetime in case i can't put it inside switch without lot of copypaste
         */
        if (leftTypeId == NYql::NProto::Uint32 && rightTypeId == NYql::NProto::Date) {
            return ECompareOptions::Comparable;
        }

        /*
         * SSA program requires strict equality of some types, otherwise columnshard fails to execute comparison
         */
        switch (leftTypeId) {
            case NYql::NProto::Int8:
            case NYql::NProto::Int16:
            case NYql::NProto::Int32:
                // SSA program cast those values to Int32
                if (rightTypeId == NYql::NProto::Int8 ||
                    rightTypeId == NYql::NProto::Int16 ||
                    rightTypeId == NYql::NProto::Int32)
                {
                    return ECompareOptions::Comparable;
                }
                break;
            case NYql::NProto::Uint16:
                if (rightTypeId == NYql::NProto::Date) {
                    return ECompareOptions::Comparable;
                }
                [[fallthrough]];
            case NYql::NProto::Uint8:
            case NYql::NProto::Uint32:
                // SSA program cast those values to Uint32
                if (rightTypeId == NYql::NProto::Uint8 ||
                    rightTypeId == NYql::NProto::Uint16 ||
                    rightTypeId == NYql::NProto::Uint32)
                {
                    return ECompareOptions::Comparable;
                }
                break;
            case NYql::NProto::Date:
                // See arcadia/ydb/library/yql/dq/runtime/dq_arrow_helpers.cpp SwitchMiniKQLDataTypeToArrowType
                if (rightTypeId == NYql::NProto::Uint16) {
                    return ECompareOptions::Comparable;
                }
                break;
            case NYql::NProto::Datetime:
                // See arcadia/ydb/library/yql/dq/runtime/dq_arrow_helpers.cpp SwitchMiniKQLDataTypeToArrowType
                if (rightTypeId == NYql::NProto::Uint32) {
                    return ECompareOptions::Comparable;
                }
                break;
            case NYql::NProto::Bool:
            case NYql::NProto::Int64:
            case NYql::NProto::Uint64:
            case NYql::NProto::Float:
            case NYql::NProto::Double:
            case NYql::NProto::Decimal:
            case NYql::NProto::Timestamp:
            case NYql::NProto::Interval:
                // Obviosly here right node has not same type as left one
                break;
            default:
                return defaultCompare(left, right);
        }

        return ECompareOptions::Uncomparable;
    };

    auto leftType = getDataType(leftPtr);
    auto rightType = getDataType(rightPtr);

    if (canCompare(leftType, rightType) == ECompareOptions::Uncomparable) {
        YQL_CLOG(DEBUG, ProviderKqp) << "OLAP Pushdown: "
            << "Uncompatible types in compare of nodes: "
            << leftPtr->Content() << " of type " << FormatType(leftType)
            << " and "
            << rightPtr->Content() << " of type " << FormatType(rightType);

        return false;
    }

    return true;
}

TVector<TExprBase> ConvertComparisonNode(const TExprBase& nodeIn, const TExprNode* rawLambdaArg)
{
    TVector<TExprBase> out;

    auto convertNode = [rawLambdaArg](const TExprBase& node) -> TMaybeNode<TExprBase> {
        if (node.Maybe<TCoNull>()) {
            return node;
        }

        if (auto maybeSafeCast = node.Maybe<TCoSafeCast>()) {
            if (!IsSupportedCast(maybeSafeCast.Cast())) {
                return NullNode;
            }

            return node;
        }

        if (auto maybeParameter = node.Maybe<TCoParameter>()) {
            return maybeParameter.Cast();
        }

        if (auto maybeData = node.Maybe<TCoDataCtor>()) {
            if (!IsSupportedDataType(maybeData.Cast())) {
                return NullNode;
            }

            return node;
        }

        if (auto maybeMember = node.Maybe<TCoMember>()) {
            if (maybeMember.Cast().Struct().Raw() != rawLambdaArg) {
                return NullNode;
            }

            return maybeMember.Cast().Name();
        }

        return NullNode;
    };

    // Columns & values may be single element
    TMaybeNode<TExprBase> node = convertNode(nodeIn);

    if (node.IsValid()) {
        out.emplace_back(std::move(node.Cast()));
        return out;
    }

    // Or columns and values can be Tuple
    if (!nodeIn.Maybe<TExprList>()) {
        // something unusual found, return empty vector
        return out;
    }

    auto tuple = nodeIn.Cast<TExprList>();

    out.reserve(tuple.Size());

    for (ui32 i = 0; i < tuple.Size(); ++i) {
        TMaybeNode<TExprBase> node = convertNode(tuple.Item(i));

        if (!node.IsValid()) {
            // Return empty vector
            return TVector<TExprBase>();
        }

        out.emplace_back(node.Cast());
    }

    return out;
}

TVector<std::pair<TExprBase, TExprBase>> ExtractComparisonParameters(const TCoCompare& predicate,
    const TExprNode* rawLambdaArg, const TExprBase& input)
{
    TVector<std::pair<TExprBase, TExprBase>> out;

    auto left = ConvertComparisonNode(predicate.Left(), rawLambdaArg);

    if (left.empty()) {
        return out;
    }

    auto right = ConvertComparisonNode(predicate.Right(), rawLambdaArg);

    if (left.size() != right.size()) {
        return out;
    }

    TMaybeNode<TCoCmpEqual> maybeEqual = predicate.Maybe<TCoCmpEqual>();
    TMaybeNode<TCoCmpNotEqual> maybeNotEqual = predicate.Maybe<TCoCmpNotEqual>();

    bool equality = maybeEqual.IsValid() || maybeNotEqual.IsValid();
    const TTypeAnnotationNode* inputType = input.Ptr()->GetTypeAnn();

    switch (inputType->GetKind()) {
        case ETypeAnnotationKind::Flow:
            inputType = inputType->Cast<TFlowExprType>()->GetItemType();
            break;
        case ETypeAnnotationKind::Stream:
            inputType = inputType->Cast<TStreamExprType>()->GetItemType();
            break;
        default:
            YQL_ENSURE(false, "Unsupported type of incoming data: " << (ui32)inputType->GetKind());
            // We do not know how process input that is not a sequence of elements
            return out;
    }

    YQL_ENSURE(inputType->GetKind() == ETypeAnnotationKind::Struct);

    if (inputType->GetKind() != ETypeAnnotationKind::Struct) {
        // We do not know how process input that is not a sequence of elements
        return out;
    }

    for (ui32 i = 0; i < left.size(); ++i) {
        if (!IsComparableTypes(left[i], right[i], equality, inputType)) {
            // Return empty vector
            return TVector<std::pair<TExprBase, TExprBase>>();
        }
        out.emplace_back(std::move(std::make_pair(left[i], right[i])));
    }

    return out;
}

TExprBase BuildOneElementComparison(const std::pair<TExprBase, TExprBase>& parameter, const TCoCompare& predicate,
    TExprContext& ctx, TPositionHandle pos, bool forceStrictComparison)
{
    auto isNull = [](const TExprBase& node) {
        if (node.Maybe<TCoNull>()) {
            return true;
        }

        if (node.Maybe<TCoNothing>()) {
            return true;
        }

        return false;
    };

    // Any comparison with NULL should return false even if NULL is uncomparable
    // See postgres documentation https://www.postgresql.org/docs/13/functions-comparisons.html
    // 9.24.5. Row Constructor Comparison
    if (isNull(parameter.first) || isNull(parameter.second)) {
        return Build<TCoBool>(ctx, pos)
            .Literal().Build("false")
            .Done();
    }

    std::string compareOperator = "";

    if (predicate.Maybe<TCoCmpEqual>()) {
        compareOperator = "eq";
    } else if (predicate.Maybe<TCoCmpNotEqual>()) {
        compareOperator = "neq";
    } else if (predicate.Maybe<TCoCmpLess>() || (predicate.Maybe<TCoCmpLessOrEqual>() && forceStrictComparison)) {
        compareOperator = "lt";
    } else if (predicate.Maybe<TCoCmpLessOrEqual>() && !forceStrictComparison) {
        compareOperator = "lte";
    } else if (predicate.Maybe<TCoCmpGreater>() || (predicate.Maybe<TCoCmpGreaterOrEqual>() && forceStrictComparison)) {
        compareOperator = "gt";
    } else if (predicate.Maybe<TCoCmpGreaterOrEqual>() && !forceStrictComparison) {
        compareOperator = "gte";
    }

    YQL_ENSURE(!compareOperator.empty(), "Unsupported comparison node: " << predicate.Ptr()->Content());

    return Build<TKqpOlapFilterCompare>(ctx, pos)
        .Operator(ctx.NewAtom(pos, compareOperator))
        .Left(parameter.first)
        .Right(parameter.second)
        .Done();
}

TMaybeNode<TExprBase> ComparisonPushdown(const TVector<std::pair<TExprBase, TExprBase>>& parameters, const TCoCompare& predicate,
    TExprContext& ctx, TPositionHandle pos)
{
    ui32 conditionsCount = parameters.size();

    if (conditionsCount == 1) {
        auto condition = BuildOneElementComparison(parameters[0], predicate, ctx, pos, false);
        return IsFalseLiteral(condition) ? NullNode : condition;
    }

    if (predicate.Maybe<TCoCmpEqual>() || predicate.Maybe<TCoCmpNotEqual>()) {
        TVector<TExprBase> conditions;
        conditions.reserve(conditionsCount);
        bool hasFalseCondition = false;

        for (ui32 i = 0; i < conditionsCount; ++i) {
            auto condition = BuildOneElementComparison(parameters[i], predicate, ctx, pos, false);
            if (IsFalseLiteral(condition)) {
                hasFalseCondition = true;
            } else {
                conditions.emplace_back(condition);
            }
        }

        if (predicate.Maybe<TCoCmpEqual>()) {
            if (hasFalseCondition) {
                return NullNode;
            }
            return Build<TKqpOlapAnd>(ctx, pos)
                .Add(conditions)
                .Done();
        }

        return Build<TKqpOlapOr>(ctx, pos)
            .Add(conditions)
            .Done();
    }

    TVector<TExprBase> orConditions;
    orConditions.reserve(conditionsCount);

    // Here we can be only when comparing tuples lexicographically
    for (ui32 i = 0; i < conditionsCount; ++i) {
        TVector<TExprBase> andConditions;
        andConditions.reserve(conditionsCount);

        // We need strict < and > in beginning columns except the last one
        // For example: (c1, c2, c3) >= (1, 2, 3) ==> (c1 > 1) OR (c2 > 2 AND c1 = 1) OR (c3 >= 3 AND c2 = 2 AND c1 = 1)
        auto condition = BuildOneElementComparison(parameters[i], predicate, ctx, pos, i < conditionsCount - 1);
        if (IsFalseLiteral(condition)) {
            continue;
        }
        andConditions.emplace_back(condition);

        for (ui32 j = 0; j < i; ++j) {
            andConditions.emplace_back(Build<TKqpOlapFilterCompare>(ctx, pos)
                .Operator(ctx.NewAtom(pos, "eq"))
                .Left(parameters[j].first)
                .Right(parameters[j].second)
                .Done());
        }

        orConditions.emplace_back(
            Build<TKqpOlapAnd>(ctx, pos)
                .Add(std::move(andConditions))
                .Done()
        );
    }

    return Build<TKqpOlapOr>(ctx, pos)
        .Add(std::move(orConditions))
        .Done();
}

// TODO: Check how to reduce columns if they are not needed. Unfortunately columnshard need columns list
// for every column present in program even if it is not used in result set.
//#define ENABLE_COLUMNS_PRUNING
#ifdef ENABLE_COLUMNS_PRUNING
TMaybeNode<TCoAtomList> BuildColumnsFromLambda(const TCoLambda& lambda, TExprContext& ctx, TPositionHandle pos)
{
    auto exprType = lambda.Ptr()->GetTypeAnn();

    if (exprType->GetKind() == ETypeAnnotationKind::Optional) {
        exprType = exprType->Cast<TOptionalExprType>()->GetItemType();
    }

    if (exprType->GetKind() != ETypeAnnotationKind::Struct) {
        return nullptr;
    }

    auto items = exprType->Cast<TStructExprType>()->GetItems();

    auto columnsList = Build<TCoAtomList>(ctx, pos);

    for (auto& item: items) {
        columnsList.Add(ctx.NewAtom(pos, item->GetName()));
    }

    return columnsList.Done();
}
#endif

TMaybeNode<TExprBase> ExistsPushdown(const TCoExists& exists, TExprContext& ctx, TPositionHandle pos, const TExprNode* lambdaArg)
{
    auto maybeMember = exists.Optional().Maybe<TCoMember>();

    if (!maybeMember.IsValid()) {
        return NullNode;
    }

    if (maybeMember.Cast().Struct().Raw() != lambdaArg) {
        return NullNode;
    }

    auto columnName = maybeMember.Cast().Name();

    return Build<TKqpOlapFilterExists>(ctx, pos)
        .Column(columnName)
        .Done();
}

TMaybeNode<TExprBase> SafeCastPredicatePushdown(const TCoFlatMap& flatmap,
    TExprContext& ctx, TPositionHandle pos, const TExprNode* lambdaArg)
{
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
    TVector<std::pair<TExprBase, TExprBase>> out;

    auto maybeFlatmap = flatmap.Lambda().Body().Maybe<TCoFlatMap>();

    if (!maybeFlatmap.IsValid()) {
        return NullNode;
    }

    auto right = ConvertComparisonNode(maybeFlatmap.Cast().Input(), lambdaArg);

    if (right.empty()) {
        return NullNode;
    }

    auto left = ConvertComparisonNode(flatmap.Input(), lambdaArg);

    if (left.empty()) {
        return NullNode;
    }

    auto maybeJust = maybeFlatmap.Cast().Lambda().Body().Maybe<TCoJust>();

    if (!maybeJust.IsValid()) {
        return NullNode;
    }

    auto maybePredicate = maybeJust.Cast().Input().Maybe<TCoCompare>();

    if (!maybePredicate.IsValid()) {
        return NullNode;
    }

    auto predicate = maybePredicate.Cast();

    if (!IsSupportedPredicate(predicate)) {
        return NullNode;
    }

    TVector<std::pair<TExprBase, TExprBase>> parameters;

    if (left.size() != right.size()) {
        return NullNode;
    }

    for (ui32 i = 0; i < left.size(); ++i) {
        out.emplace_back(std::move(std::make_pair(left[i], right[i])));
    }

    return ComparisonPushdown(parameters, predicate, ctx, pos);
}

TMaybeNode<TExprBase> SimplePredicatePushdown(const TCoCompare& predicate, TExprContext& ctx, TPositionHandle pos,
    const TExprNode* lambdaArg, const TExprBase& input)
{
    if (!IsSupportedPredicate(predicate)) {
        return NullNode;
    }

    auto parameters = ExtractComparisonParameters(predicate, lambdaArg, input);

    if (parameters.empty()) {
        return NullNode;
    }

    return ComparisonPushdown(parameters, predicate, ctx, pos);
}


TMaybeNode<TExprBase> CoalescePushdown(const TCoCoalesce& coalesce, TExprContext& ctx, TPositionHandle pos,
    const TExprNode* lambdaArg, const TExprBase& input)
{
    if (!coalesce.Value().Maybe<TCoBool>()) {
        return NullNode;
    }

    if (coalesce.Value().Cast<TCoBool>().Literal().Value() != "false") {
        return NullNode;
    }

    auto maybeFlatmap = coalesce.Predicate().Maybe<TCoFlatMap>();

    if (maybeFlatmap.IsValid()) {
        return SafeCastPredicatePushdown(maybeFlatmap.Cast(), ctx, pos, lambdaArg);
    }

    auto maybePredicate = coalesce.Predicate().Maybe<TCoCompare>();

    if (maybePredicate.IsValid()) {
        return SimplePredicatePushdown(maybePredicate.Cast(), ctx, pos, lambdaArg, input);
    }

    return NullNode;
}

TMaybeNode<TExprBase> PredicatePushdown(const TExprBase& predicate, TExprContext& ctx, TPositionHandle pos,
    const TExprNode* lambdaArg, const TExprBase& input)
{
    auto maybeCoalesce = predicate.Maybe<TCoCoalesce>();
    if (maybeCoalesce.IsValid()) {
        return CoalescePushdown(maybeCoalesce.Cast(), ctx, pos, lambdaArg, input);
    }

    auto maybeExists = predicate.Maybe<TCoExists>();
    if (maybeExists.IsValid()) {
        return ExistsPushdown(maybeExists.Cast(), ctx, pos, lambdaArg);
    }

    auto maybePredicate = predicate.Maybe<TCoCompare>();
    if (maybePredicate.IsValid()) {
        return SimplePredicatePushdown(maybePredicate.Cast(), ctx, pos, lambdaArg, input);
    }

    if (predicate.Maybe<TCoNot>()) {
        auto notNode = predicate.Cast<TCoNot>();
        auto pushedNot = PredicatePushdown(notNode.Value(), ctx, pos, lambdaArg, input);

        if (!pushedNot.IsValid()) {
            return NullNode;
        }

        return Build<TKqpOlapNot>(ctx, pos)
            .Value(pushedNot.Cast())
            .Done();
    }

    if (!predicate.Maybe<TCoAnd>() && !predicate.Maybe<TCoOr>() && !predicate.Maybe<TCoXor>()) {
        return NullNode;
    }

    TVector<TExprBase> pushedOps;
    pushedOps.reserve(predicate.Ptr()->ChildrenSize());

    for (auto& child: predicate.Ptr()->Children()) {
        auto pushedChild = PredicatePushdown(TExprBase(child), ctx, pos, lambdaArg, input);

        if (!pushedChild.IsValid()) {
            return NullNode;
        }

        pushedOps.emplace_back(pushedChild.Cast());
    }

    if (predicate.Maybe<TCoAnd>()) {
        return Build<TKqpOlapAnd>(ctx, pos)
            .Add(pushedOps)
            .Done();
    }

    if (predicate.Maybe<TCoOr>()) {
        return Build<TKqpOlapOr>(ctx, pos)
            .Add(pushedOps)
            .Done();
    }

    Y_VERIFY_DEBUG(predicate.Maybe<TCoXor>());

    return Build<TKqpOlapXor>(ctx, pos)
        .Add(pushedOps)
        .Done();
}

} // annymous namespace end

TExprBase KqpPushOlapFilter(TExprBase node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx,
    TTypeAnnotationContext& typesCtx)
{
    Y_UNUSED(typesCtx);

    if (!kqpCtx.Config->HasOptEnableOlapPushdown()) {
        return node;
    }

    if (!node.Maybe<TCoFlatMap>().Input().Maybe<TKqpReadOlapTableRanges>()) {
        return node;
    }

    auto flatmap = node.Cast<TCoFlatMap>();
    auto read = flatmap.Input().Cast<TKqpReadOlapTableRanges>();

    if (read.Process().Body().Raw() != read.Process().Args().Arg(0).Raw()) {
        return node;
    }

    const auto& lambda = flatmap.Lambda();
    auto lambdaArg = lambda.Args().Arg(0).Raw();

    YQL_CLOG(TRACE, ProviderKqp) << "Initial OLAP lambda: " << KqpExprToPrettyString(lambda, ctx);

    auto maybeOptionalIf = lambda.Body().Maybe<TCoOptionalIf>();

    if (!maybeOptionalIf.IsValid()) {
        return node;
    }

    auto optionalIf = maybeOptionalIf.Cast();

    if (!ValidateIfArgument(optionalIf, lambdaArg)) {
        return node;
    }

    auto pushedPredicate = PredicatePushdown(
        optionalIf.Predicate(), ctx, node.Pos(), lambdaArg, read.Process().Body()
    );

    if (!pushedPredicate.IsValid()) {
        return node;
    }

    auto olapFilter = Build<TKqpOlapFilter>(ctx, node.Pos())
        .Input(read.Process().Body())
        .Condition(pushedPredicate.Cast())
        .Done();

    auto newProcessLambda = Build<TCoLambda>(ctx, node.Pos())
        .Args({"olap_filter_row"})
        .Body<TExprApplier>()
            .Apply(olapFilter)
            .With(read.Process().Args().Arg(0), "olap_filter_row")
            .Build()
        .Done();

    YQL_CLOG(TRACE, ProviderKqp) << "Pushed OLAP lambda: " << KqpExprToPrettyString(newProcessLambda, ctx);

#ifdef ENABLE_COLUMNS_PRUNING
    TMaybeNode<TCoAtomList> readColumns = BuildColumnsFromLambda(lambda, ctx, node.Pos());

    if (!readColumns.IsValid()) {
        readColumns = read.Columns();
    }
#endif

    auto newRead = Build<TKqpReadOlapTableRanges>(ctx, node.Pos())
        .Table(read.Table())
        .Ranges(read.Ranges())
#ifdef ENABLE_COLUMNS_PRUNING
        .Columns(readColumns.Cast())
#else
        .Columns(read.Columns())
#endif
        .Settings(read.Settings())
        .ExplainPrompt(read.ExplainPrompt())
        .Process(newProcessLambda)
        .Done();

#ifdef ENABLE_COLUMNS_PRUNING
    return newRead;
#else
    auto newFlatmap = Build<TCoFlatMap>(ctx, node.Pos())
        .Input(newRead)
        .Lambda<TCoLambda>()
            .Args({"new_arg"})
            .Body<TCoOptionalIf>()
                .Predicate<TCoBool>()
                    .Literal().Build("true")
                    .Build()
                .Value<TExprApplier>()
                    .Apply(optionalIf.Value())
                    .With(lambda.Args().Arg(0), "new_arg")
                    .Build()
                .Build()
            .Build()
        .Done();

    return newFlatmap;
#endif
}

} // namespace NKikimr::NKqp::NOpt
