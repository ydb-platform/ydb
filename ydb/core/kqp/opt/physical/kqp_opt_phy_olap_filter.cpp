#include "kqp_opt_phy_rules.h"

#include <ydb/core/formats/arrow/ssa_runtime_version.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/library/yql/core/extract_predicate/extract_predicate.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/providers/common/pushdown/collection.h>
#include <ydb/library/yql/providers/common/pushdown/predicate_node.h>

#include <unordered_set>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NNodes;

namespace {

static TMaybeNode<TExprBase> NullNode = TMaybeNode<TExprBase>();

static const std::unordered_set<std::string> SecondLevelFilters = {
    "string_contains",
    "starts_with",
    "ends_with"
};

struct TPushdownSettings : public NPushdown::TSettings {
    TPushdownSettings()
        : NPushdown::TSettings(NYql::NLog::EComponent::ProviderKqp)
    {
        using EFlag = NPushdown::TSettings::EFeatureFlag;
        Enable(EFlag::LikeOperator, NSsa::RuntimeVersion >= 2U);
        Enable(EFlag::LikeOperatorOnlyForUtf8, NSsa::RuntimeVersion < 3U);
        Enable(EFlag::JsonQueryOperators | EFlag::JsonExistsOperator, NSsa::RuntimeVersion >= 3U);
        Enable(EFlag::ArithmeticalExpressions
            | EFlag::UnaryOperators
            | EFlag::DoNotCheckCompareArgumentsTypes
            | EFlag::TimestampCtor, NSsa::RuntimeVersion >= 4U);
        Enable(EFlag::LogicalXorOperator
            | EFlag::ParameterExpression
            | EFlag::CastExpression
            | EFlag::StringTypes
            | EFlag::DateTimeTypes
            | EFlag::UuidType
            | EFlag::DecimalType
            | EFlag::DyNumberType);
    }
};

struct TFilterOpsLevels {
    TFilterOpsLevels(const TMaybeNode<TExprBase>& firstLevel, const TMaybeNode<TExprBase>& secondLevel)
        : FirstLevelOps(firstLevel)
        , SecondLevelOps(secondLevel)
    {}

    TFilterOpsLevels(const TMaybeNode<TExprBase>& predicate)
        : FirstLevelOps(predicate)
        , SecondLevelOps(NullNode)
    {
        if (IsSecondLevelOp(predicate)) {
            FirstLevelOps = NullNode;
            SecondLevelOps = predicate;
        }
    }

    bool IsValid() const {
        return FirstLevelOps.IsValid() || SecondLevelOps.IsValid();
    }

    bool IsSecondLevelOp(const TMaybeNode<TExprBase>& predicate) {
        if (const auto maybeBinaryOp = predicate.Maybe<TKqpOlapFilterBinaryOp>()) {
            auto op = maybeBinaryOp.Cast().Operator().StringValue();
            if (SecondLevelFilters.find(op) != SecondLevelFilters.end()) {
                return true;
            }
        }
        return false;
    }

    void WrapToNotOp(TExprContext& ctx, TPositionHandle pos) {
        if (FirstLevelOps.IsValid()) {
            FirstLevelOps = Build<TKqpOlapNot>(ctx, pos)
                .Value(FirstLevelOps.Cast())
                .Done();
        }

        if (SecondLevelOps.IsValid()) {
            SecondLevelOps = Build<TKqpOlapNot>(ctx, pos)
                .Value(SecondLevelOps.Cast())
                .Done();
        }
    }


    TMaybeNode<TExprBase> FirstLevelOps;
    TMaybeNode<TExprBase> SecondLevelOps;
};

static TFilterOpsLevels NullFilterOpsLevels = TFilterOpsLevels(NullNode, NullNode);

bool IsFalseLiteral(TExprBase node) {
    return node.Maybe<TCoBool>() && !FromString<bool>(node.Cast<TCoBool>().Literal().Value());
}

std::vector<TExprBase> ConvertComparisonNode(const TExprBase& nodeIn, TExprContext& ctx, TPositionHandle pos);

std::optional<std::pair<TExprBase, TExprBase>> ExtractBinaryFunctionParameters(const TExprBase& op, TExprContext& ctx, TPositionHandle pos)
{
    const auto left = ConvertComparisonNode(TExprBase(op.Ref().HeadPtr()), ctx, pos);
    if (left.size() != 1U) {
        return std::nullopt;
    }

    const auto right = ConvertComparisonNode(TExprBase(op.Ref().TailPtr()), ctx, pos);
    if (right.size() != 1U) {
        return std::nullopt;
    }

    return std::make_pair(left.front(), right.front());
}

std::vector<std::pair<TExprBase, TExprBase>> ExtractComparisonParameters(const TCoCompare& predicate, TExprContext& ctx, TPositionHandle pos)
{
    std::vector<std::pair<TExprBase, TExprBase>> out;
    auto left = ConvertComparisonNode(predicate.Left(), ctx, pos);

    if (left.empty()) {
        return out;
    }

    auto right = ConvertComparisonNode(predicate.Right(), ctx, pos);
    if (left.size() != right.size()) {
        return out;
    }

    for (ui32 i = 0; i < left.size(); ++i) {
        out.emplace_back(std::move(std::make_pair(left[i], right[i])));
    }

    return out;
}

TMaybeNode<TExprBase> ComparisonPushdown(const std::vector<std::pair<TExprBase, TExprBase>>& parameters, const TCoCompare& predicate, TExprContext& ctx, TPositionHandle pos);

[[maybe_unused]]
TMaybeNode<TExprBase> YqlCoalescePushdown(const TCoCoalesce& coalesce, TExprContext& ctx) {
    if (const auto params = ExtractBinaryFunctionParameters(coalesce, ctx, coalesce.Pos())) {
        return Build<TKqpOlapFilterBinaryOp>(ctx, coalesce.Pos())
                .Operator().Value("??", TNodeFlags::Default).Build()
                .Left(params->first)
                .Right(params->second)
                .Done();
    }

    return NullNode;
}

std::vector<TExprBase> ConvertComparisonNode(const TExprBase& nodeIn, TExprContext& ctx, TPositionHandle pos)
{
    std::vector<TExprBase> out;
    const auto convertNode = [&ctx, &pos](const TExprBase& node) -> TMaybeNode<TExprBase> {
        if (node.Maybe<TCoNull>()) {
            return node;
        }

        if (auto maybeSafeCast = node.Maybe<TCoSafeCast>()) {
            return node;
        }

        if (auto maybeParameter = node.Maybe<TCoParameter>()) {
            return maybeParameter.Cast();
        }

        if (auto maybeData = node.Maybe<TCoDataCtor>()) {
            return node;
        }

        if (auto maybeMember = node.Maybe<TCoMember>()) {
            return maybeMember.Cast().Name();
        }

        if (auto maybeJsonValue = node.Maybe<TCoJsonValue>()) {
            auto maybeColMember = maybeJsonValue.Cast().Json().Maybe<TCoMember>();
            auto maybePathUtf8 = maybeJsonValue.Cast().JsonPath().Maybe<TCoUtf8>();
            auto maybeReturningType = maybeJsonValue.Cast().ReturningType();

            YQL_ENSURE(maybeColMember, "Expected TCoMember in column field of JSON_VALUE function for pushdown");
            YQL_ENSURE(maybePathUtf8, "Expected TCoUtf8 in path of JSON_VALUE function for pushdown");

            auto builder = Build<TKqpOlapJsonValue>(ctx, pos)
                .Column(maybeColMember.Cast().Name())
                .Path(maybePathUtf8.Cast());
            if (maybeReturningType) {
                builder.ReturningType(maybeReturningType.Cast());
            } else {
                builder.ReturningType<TCoDataType>()
                    .Type().Value("Utf8", TNodeFlags::Default).Build()
                    .Build();
            }
            return builder.Done();
        }

        if constexpr (NKikimr::NSsa::RuntimeVersion >= 4U) {
            if (const auto maybeArithmetic = node.Maybe<TCoBinaryArithmetic>()) {
                const auto arithmetic = maybeArithmetic.Cast();
                if (const auto params = ExtractBinaryFunctionParameters(arithmetic, ctx, pos)) {
                    return Build<TKqpOlapFilterBinaryOp>(ctx, pos)
                            .Operator().Value(arithmetic.Ref().Content(), TNodeFlags::Default).Build()
                            .Left(params->first)
                            .Right(params->second)
                            .Done();
                }
            }

            if (const auto maybeArithmetic = node.Maybe<TCoUnaryArithmetic>()) {
                const auto arithmetic = maybeArithmetic.Cast();
                if (const auto params = ConvertComparisonNode(arithmetic.Arg(), ctx, pos); 1U == params.size()) {
                    TString oper(arithmetic.Ref().Content());
                    YQL_ENSURE(oper.to_lower());
                    return Build<TKqpOlapFilterUnaryOp>(ctx, pos)
                            .Operator().Value(oper, TNodeFlags::Default).Build()
                            .Arg(params.front())
                            .Done();
                }
            }

            if (const auto maybeCoalesce = node.Maybe<TCoCoalesce>()) {
                return YqlCoalescePushdown(maybeCoalesce.Cast(), ctx);
            }
        }

        if (const auto maybeCompare = node.Maybe<TCoCompare>()) {
            if (const auto params = ExtractComparisonParameters(maybeCompare.Cast(), ctx, pos); !params.empty()) {
                return ComparisonPushdown(params, maybeCompare.Cast(), ctx, pos);
            }
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
    } else if (NKikimr::NSsa::RuntimeVersion >= 2U) {
        // We introduced LIKE pushdown in v2 of SSA program
        if (predicate.Maybe<TCoCmpStringContains>()) {
            compareOperator = "string_contains";
        } else if (predicate.Maybe<TCoCmpStartsWith>()) {
            compareOperator = "starts_with";
        } else if (predicate.Maybe<TCoCmpEndsWith>()) {
            compareOperator = "ends_with";
        }
    }

    YQL_ENSURE(!compareOperator.empty(), "Unsupported comparison node: " << predicate.Ptr()->Content());

    return Build<TKqpOlapFilterBinaryOp>(ctx, pos)
        .Operator().Value(compareOperator, TNodeFlags::Default).Build()
        .Left(parameter.first)
        .Right(parameter.second)
        .Done();
}

TMaybeNode<TExprBase> ComparisonPushdown(const std::vector<std::pair<TExprBase, TExprBase>>& parameters, const TCoCompare& predicate,
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
            andConditions.emplace_back(Build<TKqpOlapFilterBinaryOp>(ctx, pos)
                .Operator().Value("eq", TNodeFlags::Default).Build()
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

TMaybeNode<TExprBase> SimplePredicatePushdown(const TCoCompare& predicate, TExprContext& ctx, TPositionHandle pos)
{
    auto parameters = ExtractComparisonParameters(predicate, ctx, pos);
    if (parameters.empty()) {
        return NullNode;
    }

    return ComparisonPushdown(parameters, predicate, ctx, pos);
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

template<bool Empty>
TMaybeNode<TExprBase> ExistsPushdown(const TCoExists& exists, TExprContext& ctx, TPositionHandle pos)
{
    const auto columnName = exists.Optional().Cast<TCoMember>().Name();
    if constexpr (NSsa::RuntimeVersion >= 4U) {
        return Build<TKqpOlapFilterUnaryOp>(ctx, pos)
                .Operator().Value(Empty ? "empty" : "exists", TNodeFlags::Default).Build()
                .Arg(columnName)
                .Done();
    } else {
        return Build<TKqpOlapFilterExists>(ctx, pos)
            .Column(columnName)
            .Done();
    }
}

TMaybeNode<TExprBase> JsonExistsPushdown(const TCoJsonExists& jsonExists, TExprContext& ctx, TPositionHandle pos)
{
    auto columnName = jsonExists.Json().Cast<TCoMember>().Name();
    return Build<TKqpOlapJsonExists>(ctx, pos)
        .Column(columnName)
        .Path(jsonExists.JsonPath().Cast<TCoUtf8>())
        .Done();
}

TMaybeNode<TExprBase> SafeCastPredicatePushdown(const TCoFlatMap& inputFlatmap,
    TExprContext& ctx, TPositionHandle pos)
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
    auto left = ConvertComparisonNode(inputFlatmap.Input(), ctx, pos);
    if (left.empty()) {
        return NullNode;
    }

    auto flatmap = inputFlatmap.Lambda().Body().Cast<TCoFlatMap>();
    auto right = ConvertComparisonNode(flatmap.Input(), ctx, pos);
    if (right.empty()) {
        return NullNode;
    }

    auto predicate = flatmap.Lambda().Body().Cast<TCoJust>().Input().Cast<TCoCompare>();

    std::vector<std::pair<TExprBase, TExprBase>> parameters;
    if (left.size() != right.size()) {
        return NullNode;
    }

    for (ui32 i = 0; i < left.size(); ++i) {
        parameters.emplace_back(std::move(std::make_pair(left[i], right[i])));
    }

    return ComparisonPushdown(parameters, predicate, ctx, pos);
}

TMaybeNode<TExprBase> CoalescePushdown(const TCoCoalesce& coalesce, TExprContext& ctx, TPositionHandle pos)
{
    if constexpr (NSsa::RuntimeVersion >= 4U) {
        if (const auto node = YqlCoalescePushdown(coalesce, ctx)) {
            return node;
        }
    }

    auto predicate = coalesce.Predicate();
    if (auto maybeFlatmap = predicate.Maybe<TCoFlatMap>()) {
        return SafeCastPredicatePushdown(maybeFlatmap.Cast(), ctx, pos);
    } else if (auto maybePredicate = predicate.Maybe<TCoCompare>()) {
        return SimplePredicatePushdown(maybePredicate.Cast(), ctx, pos);
    } else if (auto maybeJsonExists = predicate.Maybe<TCoJsonExists>()) {
        return JsonExistsPushdown(maybeJsonExists.Cast(), ctx, pos);
    }

    return NullNode;
}

TFilterOpsLevels PredicatePushdown(const TExprBase& predicate, TExprContext& ctx, TPositionHandle pos)
{
    if (const auto maybeCoalesce = predicate.Maybe<TCoCoalesce>()) {
        auto coalescePred = CoalescePushdown(maybeCoalesce.Cast(), ctx, pos);
        return TFilterOpsLevels(coalescePred);
    }

    if (const auto maybeExists = predicate.Maybe<TCoExists>()) {
        auto existsPred = ExistsPushdown<false>(maybeExists.Cast(), ctx, pos);
        return TFilterOpsLevels(existsPred);
    }

    if (const auto maybeJsonExists = predicate.Maybe<TCoJsonExists>()) {
        auto jsonExistsPred = JsonExistsPushdown(maybeJsonExists.Cast(), ctx, pos);
        return TFilterOpsLevels(jsonExistsPred);
    }

    if (const auto maybePredicate = predicate.Maybe<TCoCompare>()) {
        auto pred = SimplePredicatePushdown(maybePredicate.Cast(), ctx, pos);
        return TFilterOpsLevels(pred);
    }

    if (const auto maybeNot = predicate.Maybe<TCoNot>()) {
        const auto notNode = maybeNot.Cast();
        if constexpr (NSsa::RuntimeVersion >= 4U) {
            if (const auto maybeExists = notNode.Value().Maybe<TCoExists>()) {
                return TFilterOpsLevels(ExistsPushdown<true>(maybeExists.Cast(), ctx, pos));
            }
        }
        auto pushedFilters = PredicatePushdown(notNode.Value(), ctx, pos);
        pushedFilters.WrapToNotOp(ctx, pos);
        return pushedFilters;
    }

    if (!predicate.Maybe<TCoAnd>() && !predicate.Maybe<TCoOr>() && !predicate.Maybe<TCoXor>()) {
        return NullFilterOpsLevels;
    }

    TVector<TExprBase> firstLvlOps;
    TVector<TExprBase> secondLvlOps;
    firstLvlOps.reserve(predicate.Ptr()->ChildrenSize());
    secondLvlOps.reserve(predicate.Ptr()->ChildrenSize());

    for (auto& child: predicate.Ptr()->Children()) {
        auto pushedChild = PredicatePushdown(TExprBase(child), ctx, pos);

        if (!pushedChild.IsValid()) {
            return NullFilterOpsLevels;
        }

        if (pushedChild.FirstLevelOps.IsValid()) {
            firstLvlOps.emplace_back(pushedChild.FirstLevelOps.Cast());
        }
        if (pushedChild.SecondLevelOps.IsValid()) {
            secondLvlOps.emplace_back(pushedChild.SecondLevelOps.Cast());
        }
    }

    if (predicate.Maybe<TCoAnd>()) {
        auto firstLvl = NullNode;
        if (!firstLvlOps.empty()) {
            firstLvl = Build<TKqpOlapAnd>(ctx, pos)
                .Add(firstLvlOps)
                .Done();
        }
        auto secondLvl = NullNode;
        if (!secondLvlOps.empty()) {
            secondLvl = Build<TKqpOlapAnd>(ctx, pos)
                .Add(secondLvlOps)
                .Done();
        }
        return TFilterOpsLevels(firstLvl, secondLvl);
    }

    if (predicate.Maybe<TCoOr>()) {
        auto ops = Build<TKqpOlapOr>(ctx, pos)
            .Add(firstLvlOps)
            .Add(secondLvlOps)
            .Done();
        return TFilterOpsLevels(ops, NullNode);
    }

    Y_DEBUG_ABORT_UNLESS(predicate.Maybe<TCoXor>());

    auto ops = Build<TKqpOlapXor>(ctx, pos)
        .Add(firstLvlOps)
        .Add(secondLvlOps)
        .Done();
    return TFilterOpsLevels(ops, NullNode);
}

void SplitForPartialPushdown(const NPushdown::TPredicateNode& predicateTree, NPushdown::TPredicateNode& predicatesToPush, NPushdown::TPredicateNode& remainingPredicates,
    TExprContext& ctx, TPositionHandle pos)
{
    if (predicateTree.CanBePushed) {
        predicatesToPush = predicateTree;
        remainingPredicates.ExprNode = Build<TCoBool>(ctx, pos).Literal().Build("true").Done();
        return;
    }

    if (predicateTree.Op != NPushdown::EBoolOp::And) {
        // We can partially pushdown predicates from AND operator only.
        // For OR operator we would need to have several read operators which is not acceptable.
        // TODO: Add support for NOT(op1 OR op2), because it expands to (!op1 AND !op2).
        remainingPredicates = predicateTree;
        return;
    }

    bool isFoundNotStrictOp = false;
    std::vector<NPushdown::TPredicateNode> pushable;
    std::vector<NPushdown::TPredicateNode> remaining;
    for (auto& predicate : predicateTree.Children) {
        if (predicate.CanBePushed && !isFoundNotStrictOp) {
            pushable.emplace_back(predicate);
        } else {
            if (!IsStrict(predicate.ExprNode.Cast().Ptr())) {
                isFoundNotStrictOp = true;
            }
            remaining.emplace_back(predicate);
        }
    }
    predicatesToPush.SetPredicates(pushable, ctx, pos);
    remainingPredicates.SetPredicates(remaining, ctx, pos);
}

bool IsGoodTypeForPushdown(const TTypeAnnotationNode& type) {
    return NUdf::EDataTypeFeatures::NumericType & NUdf::GetDataTypeInfo(RemoveOptionality(type).Cast<TDataExprType>()->GetSlot()).Features;
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
        case ETypeAnnotationKind::Data: {
            const auto fOne = NUdf::GetDataTypeInfo(rawOne.Cast<TDataExprType>()->GetSlot()).Features;
            const auto fTwo = NUdf::GetDataTypeInfo(rawTwo.Cast<TDataExprType>()->GetSlot()).Features;
            return ((NUdf::EDataTypeFeatures::NumericType | NUdf::EDataTypeFeatures::StringType) & fOne) && (NUdf::EDataTypeFeatures::CanCompare  & fOne)
                && ((NUdf::EDataTypeFeatures::NumericType | NUdf::EDataTypeFeatures::StringType) & fTwo) && (NUdf::EDataTypeFeatures::CanCompare  & fTwo);
        }
        default: break;
    }
    return false;
}

bool IsGoodNodeForPushdown(const TExprBase& node) {
    if (const auto maybeCompare = node.Maybe<TCoCompare>()) {
        const auto compare = maybeCompare.Cast();
        return IsGoodTypesForPushdownCompare(*compare.Left().Ref().GetTypeAnn(), *compare.Right().Ref().GetTypeAnn())
            && IsGoodNodeForPushdown(compare.Left()) && IsGoodNodeForPushdown(compare.Right());
    } else if (const auto maybeUnaryOp = node.Maybe<TCoUnaryArithmetic>()) {
        return IsGoodTypeForPushdown(*node.Ref().GetTypeAnn()) && IsGoodNodeForPushdown(maybeUnaryOp.Cast().Arg());
    } else if (const auto maybeBinaryOp = node.Maybe<TCoBinaryArithmetic>()) {
        const auto binaryOp = maybeBinaryOp.Cast();
        return IsGoodTypeForPushdown(*binaryOp.Ref().GetTypeAnn()) && !binaryOp.Maybe<TCoAggrAdd>()
            && IsGoodNodeForPushdown(binaryOp.Left()) && IsGoodNodeForPushdown(binaryOp.Right());
    } else if (const auto maybeCoalesce = node.Maybe<TCoCoalesce>()) {
        const auto coalesce = maybeCoalesce.Cast();
        return IsGoodNodeForPushdown(coalesce.Predicate()) && IsGoodNodeForPushdown(coalesce.Value());
    }

    return true;
}

void UpdatePushableFlagWithOlapSpecific(NPushdown::TPredicateNode& tree) {
    if constexpr (NSsa::RuntimeVersion < 4U)
        return;

    std::for_each(tree.Children.begin(), tree.Children.end(), std::bind(&UpdatePushableFlagWithOlapSpecific, std::placeholders::_1));
    tree.CanBePushed = tree.CanBePushed && std::all_of(tree.Children.cbegin(), tree.Children.cend(), [](const NPushdown::TPredicateNode& node) { return node.CanBePushed; });

    if (tree.CanBePushed && NPushdown::EBoolOp::Undefined == tree.Op) {
        tree.CanBePushed = IsGoodNodeForPushdown(tree.ExprNode.Cast());
    }
}

} // anonymous namespace end

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

    const auto optionalIf = maybeOptionalIf.Cast();
    NPushdown::TPredicateNode predicateTree(optionalIf.Predicate());
    CollectPredicates(optionalIf.Predicate(), predicateTree, lambdaArg, read.Process().Body(), TPushdownSettings());
    YQL_ENSURE(predicateTree.IsValid(), "Collected OLAP predicates are invalid");
    UpdatePushableFlagWithOlapSpecific(predicateTree);

    NPushdown::TPredicateNode predicatesToPush;
    NPushdown::TPredicateNode remainingPredicates;
    SplitForPartialPushdown(predicateTree, predicatesToPush, remainingPredicates, ctx, node.Pos());
    if (!predicatesToPush.IsValid()) {
        return node;
    }

    YQL_ENSURE(predicatesToPush.IsValid(), "Predicates to push is invalid");
    YQL_ENSURE(remainingPredicates.IsValid(), "Remaining predicates is invalid");

    const auto pushedFilters = PredicatePushdown(predicatesToPush.ExprNode.Cast(), ctx, node.Pos());
    YQL_ENSURE(pushedFilters.IsValid(), "Pushed predicate should be always valid!");

    TMaybeNode<TExprBase> olapFilter;
    if (pushedFilters.FirstLevelOps.IsValid()) {
        olapFilter = Build<TKqpOlapFilter>(ctx, node.Pos())
            .Input(read.Process().Body())
            .Condition(pushedFilters.FirstLevelOps.Cast())
            .Done();
    }

    if (pushedFilters.SecondLevelOps.IsValid()) {
        olapFilter = Build<TKqpOlapFilter>(ctx, node.Pos())
            .Input(olapFilter.IsValid() ? olapFilter.Cast() : read.Process().Body())
            .Condition(pushedFilters.SecondLevelOps.Cast())
            .Done();
    }

    auto newProcessLambda = Build<TCoLambda>(ctx, node.Pos())
        .Args({"olap_filter_row"})
        .Body<TExprApplier>()
            .Apply(olapFilter.Cast())
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
                .Predicate<TExprApplier>()
                    .Apply(remainingPredicates.ExprNode.Cast())
                    .With(lambda.Args().Arg(0), "new_arg")
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
