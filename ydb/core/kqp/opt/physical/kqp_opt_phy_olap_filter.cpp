#include "kqp_opt_phy_rules.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/library/yql/core/extract_predicate/extract_predicate.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NNodes;

namespace {

static TMaybeNode<TExprBase> NullNode = TMaybeNode<TExprBase>();

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

    // Ok, maybe it is SELECT `field` ?
    auto maybeAsStruct = optionalIf.Value().Maybe<TCoAsStruct>();

    if (!maybeAsStruct) {
        return false;
    }

    auto asStruct = maybeAsStruct.Cast();

    // SELECT `field` has only one item
    if (asStruct.ArgCount() != 1) {
        return false;
    }

    // Check that second tuple element is Member(lambda arg)
    auto tuple = asStruct.Arg(0).Maybe<TExprList>().Cast();

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


TVector<std::pair<TExprBase, TExprBase>> ExtractComparisonParameters(const TCoCompare& predicate,
    const TExprNode* rawLambdaArg, const TExprBase& input)
{
    TVector<std::pair<TExprBase, TExprBase>> out;

    auto convertNode = [rawLambdaArg](const TExprBase& node) -> TMaybeNode<TExprBase> {
        if (node.Maybe<TCoNull>()) {
            return node;
        }

        if (auto maybeParameter = node.Maybe<TCoParameter>()) {
            return maybeParameter.Cast();
        }

        if (auto maybeData = node.Maybe<TCoDataCtor>()) {
            if (IsSupportedDataType(maybeData.Cast())) {
                return node;
            }

            return NullNode;
        }

        if (auto maybeMember = node.Maybe<TCoMember>()) {
            if (maybeMember.Cast().Struct().Raw() == rawLambdaArg) {
                return maybeMember.Cast().Name();
            }

            return NullNode;
        }

        return NullNode;
    };

    // Columns & values may be single element
    TMaybeNode<TExprBase> left = convertNode(predicate.Left());
    TMaybeNode<TExprBase> right = convertNode(predicate.Right());

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

    if (left.IsValid() && right.IsValid()) {
        if (!IsComparableTypes(left.Cast(), right.Cast(), equality, inputType)) {
            return out;
        }

        out.emplace_back(std::move(std::make_pair(left.Cast(), right.Cast())));
        return out;
    }

    // Or columns and values can be Tuple
    if (!predicate.Left().Maybe<TExprList>() || !predicate.Right().Maybe<TExprList>()) {
        // something unusual found, return empty vector
        return out;
    }

    auto tupleLeft = predicate.Left().Cast<TExprList>();
    auto tupleRight = predicate.Right().Cast<TExprList>();

    if (tupleLeft.Size() != tupleRight.Size()) {
        return out;
    }

    out.reserve(tupleLeft.Size());

    for (ui32 i = 0; i < tupleLeft.Size(); ++i) {
        TMaybeNode<TExprBase> left = convertNode(tupleLeft.Item(i));
        TMaybeNode<TExprBase> right = convertNode(tupleRight.Item(i));

        if (!left.IsValid() || !right.IsValid()) {
            // Return empty vector
            return TVector<std::pair<TExprBase, TExprBase>>();
        }

        if (!IsComparableTypes(left.Cast(), right.Cast(), equality, inputType)) {
            // Return empty vector
            return TVector<std::pair<TExprBase, TExprBase>>();
        }

        out.emplace_back(std::move(std::make_pair(left.Cast(), right.Cast())));
    }

    return out;
}

TExprBase BuildOneElementComparison(const std::pair<TExprBase, TExprBase>& parameter, const TCoCompare& predicate,
    TExprContext& ctx, TPositionHandle pos, const TExprBase& input)
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

    if (predicate.Maybe<TCoCmpEqual>()) {
        return Build<TKqpOlapFilterEqual>(ctx, pos)
            .Input(input)
            .Left(parameter.first)
            .Right(parameter.second)
            .Done();
    }

    if (predicate.Maybe<TCoCmpLess>()) {
        return Build<TKqpOlapFilterLess>(ctx, pos)
            .Input(input)
            .Left(parameter.first)
            .Right(parameter.second)
            .Done();
    }

    if (predicate.Maybe<TCoCmpLessOrEqual>()) {
        return Build<TKqpOlapFilterLessOrEqual>(ctx, pos)
            .Input(input)
            .Left(parameter.first)
            .Right(parameter.second)
            .Done();
    }

    if (predicate.Maybe<TCoCmpGreater>()) {
        return Build<TKqpOlapFilterGreater>(ctx, pos)
            .Input(input)
            .Left(parameter.first)
            .Right(parameter.second)
            .Done();
    }

    if (predicate.Maybe<TCoCmpGreaterOrEqual>()) {
        return Build<TKqpOlapFilterGreaterOrEqual>(ctx, pos)
            .Input(input)
            .Left(parameter.first)
            .Right(parameter.second)
            .Done();
    }

    YQL_ENSURE(predicate.Maybe<TCoCmpNotEqual>(), "Unsupported comparison node: " << predicate.Ptr()->Content());

    return Build<TCoNot>(ctx, pos)
        .Value<TKqpOlapFilterEqual>()
            .Input(input)
            .Left(parameter.first)
            .Right(parameter.second)
            .Build()
        .Done();
}

TExprBase ComparisonPushdown(const TVector<std::pair<TExprBase, TExprBase>>& parameters, const TCoCompare& predicate,
    TExprContext& ctx, TPositionHandle pos, const TExprBase& input)
{
    ui32 conditionsCount = parameters.size();

    if (conditionsCount == 1) {
        return BuildOneElementComparison(parameters[0], predicate, ctx, pos, input);
    }

    if (predicate.Maybe<TCoCmpEqual>() || predicate.Maybe<TCoCmpNotEqual>()) {
        TVector<TExprBase> conditions;
        conditions.reserve(conditionsCount);

        for (ui32 i = 0; i < conditionsCount; ++i) {
            conditions.emplace_back(BuildOneElementComparison(parameters[i], predicate, ctx, pos, input));
        }

        if (predicate.Maybe<TCoCmpEqual>()) {
            return Build<TCoAnd>(ctx, pos)
                .Add(conditions)
                .Done();
        }

        return Build<TCoOr>(ctx, pos)
            .Add(conditions)
            .Done();
    }

    TVector<TExprBase> orConditions;
    orConditions.reserve(conditionsCount);

    // Here we can be only whe comparing tuples lexicographically
    for (ui32 i = 0; i < conditionsCount; ++i) {
        TVector<TExprBase> andConditions;
        andConditions.reserve(conditionsCount);

        andConditions.emplace_back(BuildOneElementComparison(parameters[i], predicate, ctx, pos, input));

        for (ui32 j = 0; j < i; ++j) {
            andConditions.emplace_back(Build<TKqpOlapFilterEqual>(ctx, pos)
                .Input(input)
                .Left(parameters[j].first)
                .Right(parameters[j].second)
                .Done());
        }

        orConditions.emplace_back(Build<TCoAnd>(ctx, pos)
            .Add(std::move(andConditions))
            .Done());
    }

    return Build<TCoOr>(ctx, pos)
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

TMaybeNode<TExprBase> ExistsPushdown(const TCoExists& exists, TExprContext& ctx, TPositionHandle pos,
    const TExprNode* lambdaArg, const TExprBase& input)
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
        .Input(input)
        .Column(columnName)
        .Done();
}

TMaybeNode<TExprBase> CoalescePushdown(const TCoCoalesce& coalesce, TExprContext& ctx, TPositionHandle pos,
    const TExprNode* lambdaArg, const TExprBase& input)
{
    auto maybePredicate = coalesce.Predicate().Maybe<TCoCompare>();

    if (!maybePredicate.IsValid()) {
        return NullNode;
    }

    auto predicate = maybePredicate.Cast();

    if (!IsSupportedPredicate(predicate)) {
        return NullNode;
    }

    if (!coalesce.Value().Maybe<TCoBool>()) {
        return NullNode;
    }

    if (coalesce.Value().Cast<TCoBool>().Literal().Value() != "false") {
        return NullNode;
    }

    auto parameters = ExtractComparisonParameters(predicate, lambdaArg, input);

    if (parameters.empty()) {
        return NullNode;
    }

    return ComparisonPushdown(parameters, predicate, ctx, pos, input);
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
        return ExistsPushdown(maybeExists.Cast(), ctx, pos, lambdaArg, input);
    }

    if (predicate.Maybe<TCoNot>()) {
        auto notNode = predicate.Cast<TCoNot>();
        auto pushedNot = PredicatePushdown(notNode.Value(), ctx, pos, lambdaArg, input);

        if (!pushedNot.IsValid()) {
            return NullNode;
        }

        return Build<TCoNot>(ctx, pos)
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
        return Build<TCoAnd>(ctx, pos)
            .Add(pushedOps)
            .Done();
    }

    if (predicate.Maybe<TCoOr>()) {
        return Build<TCoOr>(ctx, pos)
            .Add(pushedOps)
            .Done();
    }

    Y_VERIFY_DEBUG(predicate.Maybe<TCoXor>());

    return Build<TCoXor>(ctx, pos)
        .Add(pushedOps)
        .Done();
}

} // annymous namespace end

TExprBase KqpPushOlapFilter(TExprBase node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx,
    TTypeAnnotationContext& typesCtx)
{
    Y_UNUSED(typesCtx);

    if (!kqpCtx.Config->PushOlapProcess()) {
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
        .Args({"row"})
        .Body<TExprApplier>()
            .Apply(olapFilter)
            .With(read.Process().Args().Arg(0), "row")
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
