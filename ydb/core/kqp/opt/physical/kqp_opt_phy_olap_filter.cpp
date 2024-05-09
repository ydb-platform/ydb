#include "kqp_opt_phy_rules.h"
#include "predicate_collector.h"

#include <ydb/core/formats/arrow/ssa_runtime_version.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/core/peephole_opt/yql_opt_peephole_physical.h>

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

std::vector<TExprBase> ConvertComparisonNode(const TExprBase& nodeIn, const TExprNode& argument, TExprContext& ctx, TPositionHandle pos);

std::optional<std::pair<TExprBase, TExprBase>> ExtractBinaryFunctionParameters(const TExprBase& op, const TExprNode& argument, TExprContext& ctx, TPositionHandle pos)
{
    const auto left = ConvertComparisonNode(TExprBase(op.Ref().HeadPtr()), argument, ctx, pos);
    if (left.size() != 1U) {
        return std::nullopt;
    }

    const auto right = ConvertComparisonNode(TExprBase(op.Ref().TailPtr()), argument, ctx, pos);
    if (right.size() != 1U) {
        return std::nullopt;
    }

    return std::make_pair(left.front(), right.front());
}

std::optional<std::array<TExprBase, 3U>> ExtractTernaryFunctionParameters(const TExprBase& op, const TExprNode& argument, TExprContext& ctx, TPositionHandle pos)
{
    const auto first = ConvertComparisonNode(TExprBase(op.Ref().ChildPtr(0U)), argument, ctx, pos);
    if (first.size() != 1U) {
        return std::nullopt;
    }

    const auto second = ConvertComparisonNode(TExprBase(op.Ref().ChildPtr(1U)), argument, ctx, pos);
    if (second.size() != 1U) {
        return std::nullopt;
    }

    const auto third = ConvertComparisonNode(TExprBase(op.Ref().ChildPtr(2U)), argument, ctx, pos);
    if (third.size() != 1U) {
        return std::nullopt;
    }

    return std::array<TExprBase, 3U>{first.front(), second.front(), third.front()};
}

std::vector<std::pair<TExprBase, TExprBase>> ExtractComparisonParameters(const TCoCompare& predicate, const TExprNode& argument, TExprContext& ctx, TPositionHandle pos)
{
    std::vector<std::pair<TExprBase, TExprBase>> out;
    auto left = ConvertComparisonNode(predicate.Left(), argument, ctx, pos);
    if (left.empty()) {
        return out;
    }

    auto right = ConvertComparisonNode(predicate.Right(), argument, ctx, pos);
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
TMaybeNode<TExprBase> YqlCoalescePushdown(const TCoCoalesce& coalesce, const TExprNode& argument, TExprContext& ctx) {
    if (const auto params = ExtractBinaryFunctionParameters(coalesce, argument, ctx, coalesce.Pos())) {
        return Build<TKqpOlapFilterBinaryOp>(ctx, coalesce.Pos())
                .Operator().Value("??", TNodeFlags::Default).Build()
                .Left(params->first)
                .Right(params->second)
                .Done();
    }

    return NullNode;
}

TMaybeNode<TExprBase> YqlIfPushdown(const TCoIf& ifOp, const TExprNode& argument, TExprContext& ctx) {
    if (const auto params = ExtractTernaryFunctionParameters(ifOp, argument, ctx, ifOp.Pos())) {
        return Build<TKqpOlapFilterTernaryOp>(ctx, ifOp.Pos())
            .Operator().Value("if", TNodeFlags::Default).Build()
            .First(std::get<0U>(*params))
            .Second(std::get<1U>(*params))
            .Third(std::get<2U>(*params))
            .Done();
    }

    return NullNode;
}

TMaybeNode<TExprBase> YqlApplyPushdown(const TExprBase& apply, const TExprNode& argument, TExprContext& ctx) {
    const auto members = FindNodes(apply.Ptr(), [&argument] (const TExprNode::TPtr& node) {
        if (const auto maybeMember = TMaybeNode<TCoMember>(node))
            return maybeMember.Cast().Struct().Raw() == &argument;
        return false;
    });

    TNodeOnNodeOwnedMap replacements(members.size());
    TExprNode::TListType columns, arguments;
    columns.reserve(members.size());
    arguments.reserve(members.size());
    for (const auto& member : members) {
        columns.emplace_back(member->TailPtr());
        arguments.emplace_back(ctx.NewArgument(member->Pos(), columns.back()->Content()));
        replacements.emplace(member.Get(), arguments.back());
    }

    return Build<TKqpOlapApply>(ctx, apply.Pos())
        .Type(ExpandType(argument.Pos(), *argument.GetTypeAnn(), ctx))
        .Columns().Add(std::move(columns)).Build()
        .Lambda(ctx.NewLambda(apply.Pos(), ctx.NewArguments(argument.Pos(), std::move(arguments)), ctx.ReplaceNodes(apply.Ptr(), replacements)))
        .Done();
}

std::vector<TExprBase> ConvertComparisonNode(const TExprBase& nodeIn, const TExprNode& argument, TExprContext& ctx, TPositionHandle pos)
{
    std::vector<TExprBase> out;
    const auto convertNode = [&ctx, &pos, &argument](const TExprBase& node) -> TMaybeNode<TExprBase> {
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

        if (const auto maybeJust = node.Maybe<TCoJust>()) {
            if (const auto params = ConvertComparisonNode(maybeJust.Cast().Input(), argument, ctx, pos); 1U == params.size()) {
                return Build<TKqpOlapFilterUnaryOp>(ctx, node.Pos())
                    .Operator().Value("just", TNodeFlags::Default).Build()
                    .Arg(params.front())
                    .Done();
            }
        }

        if (const auto maybeIf = node.Maybe<TCoIf>()) {
            return YqlIfPushdown(maybeIf.Cast(), argument, ctx);
        }

        if constexpr (NKikimr::NSsa::RuntimeVersion >= 4U) {
            if (const auto maybeArithmetic = node.Maybe<TCoBinaryArithmetic>()) {
                const auto arithmetic = maybeArithmetic.Cast();
                if (const auto params = ExtractBinaryFunctionParameters(arithmetic, argument, ctx, pos)) {
                    return Build<TKqpOlapFilterBinaryOp>(ctx, pos)
                            .Operator().Value(arithmetic.Ref().Content(), TNodeFlags::Default).Build()
                            .Left(params->first)
                            .Right(params->second)
                            .Done();
                }
            }

            if (const auto maybeArithmetic = node.Maybe<TCoUnaryArithmetic>()) {
                const auto arithmetic = maybeArithmetic.Cast();
                if (const auto params = ConvertComparisonNode(arithmetic.Arg(), argument, ctx, pos); 1U == params.size()) {
                    TString oper(arithmetic.Ref().Content());
                    YQL_ENSURE(oper.to_lower());
                    return Build<TKqpOlapFilterUnaryOp>(ctx, pos)
                            .Operator().Value(oper, TNodeFlags::Default).Build()
                            .Arg(params.front())
                            .Done();
                }
            }

            if (const auto maybeCoalesce = node.Maybe<TCoCoalesce>()) {
                return YqlCoalescePushdown(maybeCoalesce.Cast(), argument, ctx);
            }
        }

        if (const auto maybeCompare = node.Maybe<TCoCompare>()) {
            if (const auto params = ExtractComparisonParameters(maybeCompare.Cast(), argument, ctx, pos); !params.empty()) {
                return ComparisonPushdown(params, maybeCompare.Cast(), ctx, pos);
            }
        }


        if constexpr (NKikimr::NSsa::RuntimeVersion >= 5U) {
            return YqlApplyPushdown(node, argument, ctx);
        } else {
            return NullNode;
        }
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
    } else if constexpr (NKikimr::NSsa::RuntimeVersion >= 2U) {
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

TMaybeNode<TExprBase> SimplePredicatePushdown(const TCoCompare& predicate, const TExprNode& argument, TExprContext& ctx, TPositionHandle pos)
{
    const auto parameters = ExtractComparisonParameters(predicate, argument, ctx, pos);
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

TMaybeNode<TExprBase> SafeCastPredicatePushdown(const TCoFlatMap& inputFlatmap, const TExprNode& argument, TExprContext& ctx, TPositionHandle pos)
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
    auto left = ConvertComparisonNode(inputFlatmap.Input(), argument, ctx, pos);
    if (left.empty()) {
        return NullNode;
    }

    auto flatmap = inputFlatmap.Lambda().Body().Cast<TCoFlatMap>();
    auto right = ConvertComparisonNode(flatmap.Input(), argument, ctx, pos);
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

TMaybeNode<TExprBase> CoalescePushdown(const TCoCoalesce& coalesce, const TExprNode& argument, TExprContext& ctx, TPositionHandle pos)
{
    if constexpr (NSsa::RuntimeVersion >= 4U) {
        if (const auto node = YqlCoalescePushdown(coalesce, argument, ctx)) {
            return node;
        }
    }

    auto predicate = coalesce.Predicate();
    if (const auto maybeFlatmap = predicate.Maybe<TCoFlatMap>()) {
        return SafeCastPredicatePushdown(maybeFlatmap.Cast(), argument, ctx, pos);
    } else if (auto maybePredicate = predicate.Maybe<TCoCompare>()) {
        return SimplePredicatePushdown(maybePredicate.Cast(), argument, ctx, pos);
    } else if (auto maybeJsonExists = predicate.Maybe<TCoJsonExists>()) {
        return JsonExistsPushdown(maybeJsonExists.Cast(), ctx, pos);
    }

    return NullNode;
}

TFilterOpsLevels PredicatePushdown(const TExprBase& predicate, const TExprNode& argument, TExprContext& ctx, TPositionHandle pos)
{
    if (const auto maybeCoalesce = predicate.Maybe<TCoCoalesce>()) {
        auto coalescePred = CoalescePushdown(maybeCoalesce.Cast(), argument, ctx, pos);
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
        auto pred = SimplePredicatePushdown(maybePredicate.Cast(), argument, ctx, pos);
        return TFilterOpsLevels(pred);
    }

    if (const auto maybeIf = predicate.Maybe<TCoIf>()) {
        return YqlIfPushdown(maybeIf.Cast(), argument, ctx);
    }

    if (const auto maybeNot = predicate.Maybe<TCoNot>()) {
        const auto notNode = maybeNot.Cast();
        if constexpr (NSsa::RuntimeVersion >= 4U) {
            if (const auto maybeExists = notNode.Value().Maybe<TCoExists>()) {
                return TFilterOpsLevels(ExistsPushdown<true>(maybeExists.Cast(), ctx, pos));
            }
        }
        auto pushedFilters = PredicatePushdown(notNode.Value(), argument, ctx, pos);
        pushedFilters.WrapToNotOp(ctx, pos);
        return pushedFilters;
    }

    if (predicate.Maybe<TCoAnd>() || predicate.Maybe<TCoOr>() || predicate.Maybe<TCoXor>()) {
        TVector<TExprBase> firstLvlOps;
        TVector<TExprBase> secondLvlOps;
        firstLvlOps.reserve(predicate.Ptr()->ChildrenSize());
        secondLvlOps.reserve(predicate.Ptr()->ChildrenSize());

        for (auto& child: predicate.Ptr()->Children()) {
            auto pushedChild = PredicatePushdown(TExprBase(child), argument, ctx, pos);

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

    return YqlApplyPushdown(predicate, argument, ctx);
}

TOLAPPredicateNode WrapPredicates(const std::vector<TOLAPPredicateNode>& predicates, TExprContext& ctx, TPositionHandle pos) {
    if (predicates.empty()) {
        return {};
    }

    if (const auto predicatesSize = predicates.size(); 1U == predicatesSize) {
        return predicates.front();
    } else {
        TOLAPPredicateNode result;
        result.Children = predicates;
        result.CanBePushed = true;

        TVector<NNodes::TExprBase> exprNodes;
        exprNodes.reserve(predicatesSize);
        for (const auto& pred : predicates) {
            exprNodes.emplace_back(pred.ExprNode);
            result.CanBePushed &= pred.CanBePushed;
        }
        result.ExprNode = NNodes::Build<NNodes::TCoAnd>(ctx, pos)
            .Add(exprNodes)
            .Done().Ptr();
        return result;
    }
}

void SplitForPartialPushdown(const TOLAPPredicateNode& predicateTree, TOLAPPredicateNode& predicatesToPush, TOLAPPredicateNode& remainingPredicates,
    TExprContext& ctx, TPositionHandle pos)
{
    if (predicateTree.CanBePushed) {
        predicatesToPush = predicateTree;
        remainingPredicates.ExprNode = MakeBool<true>(pos, ctx);
        return;
    }

    if (!TCoAnd::Match(predicateTree.ExprNode.Get())) {
        // We can partially pushdown predicates from AND operator only.
        // For OR operator we would need to have several read operators which is not acceptable.
        // TODO: Add support for NOT(op1 OR op2), because it expands to (!op1 AND !op2).
        remainingPredicates = predicateTree;
        return;
    }

    bool isFoundNotStrictOp = false;
    std::vector<TOLAPPredicateNode> pushable, remaining;
    for (const auto& predicate : predicateTree.Children) {
        if (predicate.CanBePushed && !isFoundNotStrictOp) {
            pushable.emplace_back(predicate);
        } else {
            if (!IsStrict(predicate.ExprNode)) {
                isFoundNotStrictOp = true;
            }
            remaining.emplace_back(predicate);
        }
    }
    predicatesToPush = WrapPredicates(pushable, ctx, pos);
    remainingPredicates = WrapPredicates(remaining, ctx, pos);
}

} // anonymous namespace end

TExprBase KqpPushOlapFilter(TExprBase node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx,
    TTypeAnnotationContext& typesCtx)
{
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

    YQL_CLOG(TRACE, ProviderKqp) << "Initial OLAP lambda: " << KqpExprToPrettyString(lambda, ctx);

    const auto maybeOptionalIf = lambda.Body().Maybe<TCoOptionalIf>();
    if (!maybeOptionalIf.IsValid()) {
        return node;
    }

    const auto& optionaIf = maybeOptionalIf.Cast();
    auto predicate = optionaIf.Predicate();
    auto value = optionaIf.Value();

    if constexpr (NSsa::RuntimeVersion >= 5U) {
        TExprNode::TPtr afterPeephole;
        bool hasNonDeterministicFunctions;
        if (const auto status = PeepHoleOptimizeNode(optionaIf.Ptr(), afterPeephole, ctx, typesCtx, nullptr, hasNonDeterministicFunctions);
            status != IGraphTransformer::TStatus::Ok) {
            YQL_CLOG(ERROR, ProviderKqp) << "Peephole OLAP failed." << Endl << ctx.IssueManager.GetIssues().ToString();
            return node;
        }

        const TCoIf simplified(std::move(afterPeephole));
        predicate = simplified.Predicate();
        value = simplified.ThenValue().Cast<TCoJust>().Input();
    }

    TOLAPPredicateNode predicateTree;
    predicateTree.ExprNode = predicate.Ptr();
    const auto& lambdaArg = lambda.Args().Arg(0).Ref();
    CollectPredicates(predicate, predicateTree, &lambdaArg, read.Process().Body());
    YQL_ENSURE(predicateTree.IsValid(), "Collected OLAP predicates are invalid");

    TOLAPPredicateNode predicatesToPush, remainingPredicates;
    SplitForPartialPushdown(predicateTree, predicatesToPush, remainingPredicates, ctx, node.Pos());
    if (!predicatesToPush.IsValid()) {
        return node;
    }

    YQL_ENSURE(predicatesToPush.IsValid(), "Predicates to push is invalid");
    YQL_ENSURE(remainingPredicates.IsValid(), "Remaining predicates is invalid");

    const auto pushedFilters = PredicatePushdown(TExprBase(predicatesToPush.ExprNode), lambdaArg, ctx, node.Pos());
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
                    .Apply(TExprBase(remainingPredicates.ExprNode))
                    .With(lambda.Args().Arg(0), "new_arg")
                    .Build()
                .Value<TExprApplier>()
                    .Apply(value)
                    .With(lambda.Args().Arg(0), "new_arg")
                    .Build()
                .Build()
            .Build()
        .Done();

    return newFlatmap;
#endif
}

} // namespace NKikimr::NKqp::NOpt
