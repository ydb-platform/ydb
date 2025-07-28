#include "kqp_opt_phy_rules.h"
#include "predicate_collector.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/core/peephole_opt/yql_opt_peephole_physical.h>

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

static TMaybeNode<TExprBase> CombinePredicatesWithAnd(const TVector<TExprBase>& conjuncts, TExprContext& ctx, TPositionHandle pos, bool useOlapAnd, bool trueForEmpty) {
    if (conjuncts.empty()) {
        return trueForEmpty ? TMaybeNode<TExprBase>{MakeBool<true>(pos, ctx)} : TMaybeNode<TExprBase>{};
    } else if (conjuncts.size() == 1) {
        return conjuncts[0];
    } else {
        if (useOlapAnd) {
            return Build<TKqpOlapAnd>(ctx, pos)
                .Add(conjuncts)
            .Done();
        } else {
            return Build<TCoAnd>(ctx, pos)
                .Add(conjuncts)
            .Done();
        }
    }
}

static TMaybeNode<TExprBase> CombinePredicatesWithAnd(const TVector<TOLAPPredicateNode>& conjuncts, TExprContext& ctx, TPositionHandle pos, bool useOlapAnd, bool trueForEmpty) {
    TVector<TExprBase> exprs;
    for(const auto& c: conjuncts) {
        exprs.emplace_back(c.ExprNode);
    }
    return CombinePredicatesWithAnd(exprs, ctx, pos, useOlapAnd, trueForEmpty);
}

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


    static TFilterOpsLevels Merge(TVector<TFilterOpsLevels> predicates, TExprContext& ctx, TPositionHandle pos) {
        TVector<TExprBase> predicatesFirstLevel;
        TVector<TExprBase> predicatesSecondLevel;
        for (const auto& p: predicates) {
            if (p.FirstLevelOps.IsValid()) {
                predicatesFirstLevel.emplace_back(p.FirstLevelOps.Cast());
            }
            if (p.SecondLevelOps.IsValid()) {
                predicatesSecondLevel.emplace_back(p.SecondLevelOps.Cast());
            }
        }
        return {
            CombinePredicatesWithAnd(predicatesFirstLevel, ctx, pos, true, false),
            CombinePredicatesWithAnd(predicatesSecondLevel, ctx, pos, true, false),
        };
    }

    TMaybeNode<TExprBase> FirstLevelOps;
    TMaybeNode<TExprBase> SecondLevelOps;
};

static TFilterOpsLevels NullFilterOpsLevels = TFilterOpsLevels(NullNode, NullNode);

bool IsFalseLiteral(TExprBase node) {
    return node.Maybe<TCoBool>() && !FromString<bool>(node.Cast<TCoBool>().Literal().Value());
}

std::vector<TExprBase> ConvertComparisonNode(const TExprBase& nodeIn, const TExprNode& argument, TExprContext& ctx, TPositionHandle pos, bool allowApply);

std::optional<std::pair<TExprBase, TExprBase>> ExtractBinaryFunctionParameters(const TExprBase& op, const TExprNode& argument, TExprContext& ctx, TPositionHandle pos, bool allowApply)
{
    const auto left = ConvertComparisonNode(TExprBase(op.Ref().HeadPtr()), argument, ctx, pos, allowApply);
    if (left.size() != 1U) {
        return std::nullopt;
    }

    const auto right = ConvertComparisonNode(TExprBase(op.Ref().TailPtr()), argument, ctx, pos, allowApply);
    if (right.size() != 1U) {
        return std::nullopt;
    }

    return std::make_pair(left.front(), right.front());
}

std::optional<std::array<TExprBase, 3U>> ExtractTernaryFunctionParameters(const TExprBase& op, const TExprNode& argument, TExprContext& ctx, TPositionHandle pos, bool allowApply)
{
    const auto first = ConvertComparisonNode(TExprBase(op.Ref().ChildPtr(0U)), argument, ctx, pos, allowApply);
    if (first.size() != 1U) {
        return std::nullopt;
    }

    const auto second = ConvertComparisonNode(TExprBase(op.Ref().ChildPtr(1U)), argument, ctx, pos, allowApply);
    if (second.size() != 1U) {
        return std::nullopt;
    }

    const auto third = ConvertComparisonNode(TExprBase(op.Ref().ChildPtr(2U)), argument, ctx, pos, allowApply);
    if (third.size() != 1U) {
        return std::nullopt;
    }

    return std::array<TExprBase, 3U>{first.front(), second.front(), third.front()};
}

std::vector<std::pair<TExprBase, TExprBase>> ExtractComparisonParameters(const TCoCompare& predicate, const TExprNode& argument, TExprContext& ctx, TPositionHandle pos, bool allowApply)
{
    std::vector<std::pair<TExprBase, TExprBase>> out;
    auto left = ConvertComparisonNode(predicate.Left(), argument, ctx, pos, allowApply);
    if (left.empty()) {
        return out;
    }

    auto right = ConvertComparisonNode(predicate.Right(), argument, ctx, pos, allowApply);
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
TMaybeNode<TExprBase> CoalescePushdown(const TCoCoalesce& coalesce, const TExprNode& argument, TExprContext& ctx, bool allowApply) {
    if (const auto params = ExtractBinaryFunctionParameters(coalesce, argument, ctx, coalesce.Pos(), allowApply)) {
        return Build<TKqpOlapFilterBinaryOp>(ctx, coalesce.Pos())
                .Operator().Value("??", TNodeFlags::Default).Build()
                .Left(params->first)
                .Right(params->second)
                .Done();
    }

    return NullNode;
}

TMaybeNode<TExprBase> YqlIfPushdown(const TCoIf& ifOp, const TExprNode& argument, TExprContext& ctx, bool allowApply) {
    if (const auto params = ExtractTernaryFunctionParameters(ifOp, argument, ctx, ifOp.Pos(), allowApply)) {
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
    const auto parameters = FindNodes(apply.Ptr(), [] (const TExprNode::TPtr& node) {
        if (const auto maybeParam = TMaybeNode<TCoParameter>(node))
            return true;
        return false;
    });

    const auto members = FindNodes(apply.Ptr(), [&argument] (const TExprNode::TPtr& node) {
        if (const auto maybeMember = TMaybeNode<TCoMember>(node))
            return maybeMember.Cast().Struct().Raw() == &argument;
        return false;
    });

    // Temporary fix for https://st.yandex-team.ru/KIKIMR-22560
    if (!members.size()) {
        return nullptr;
    }

    TNodeOnNodeOwnedMap replacements(members.size());
    TExprNode::TListType realArgs;
    TExprNode::TListType lambdaArgs;

    for (const auto& member : members) {
        const auto& columnName = member->TailPtr();
        auto columnArg = Build<TKqpOlapApplyColumnArg>(ctx, member->Pos())
            .TableRowType(ExpandType(argument.Pos(), *argument.GetTypeAnn(), ctx))
            .ColumnName(columnName)
        .Done();

        realArgs.push_back(columnArg.Ptr());
        TString argumentName = "members_" + TString(columnName->Content());
        lambdaArgs.emplace_back(ctx.NewArgument(member->Pos(), TStringBuf(argumentName)));
        replacements.emplace(member.Get(), lambdaArgs.back());
    }

    for(const auto& pptr : parameters) {
        realArgs.push_back(pptr);
        const auto& parameter = TMaybeNode<TCoParameter>(pptr).Cast();
        TString argumentName = "parameter_" + TString(parameter.Name().StringValue());
        lambdaArgs.emplace_back(ctx.NewArgument(pptr->Pos(), TStringBuf(argumentName)));
        replacements.emplace(pptr.Get(), lambdaArgs.back());
    }


    return Build<TKqpOlapApply>(ctx, apply.Pos())
        .Lambda(ctx.NewLambda(apply.Pos(), ctx.NewArguments(argument.Pos(), std::move(lambdaArgs)), ctx.ReplaceNodes(apply.Ptr(), replacements)))
        .Args().Add(std::move(realArgs)).Build()
        .KernelName(ctx.NewAtom(apply.Pos(), ""))    
        .Done();
}

TMaybeNode<TExprBase> JsonExistsPushdown(const TCoJsonExists& jsonExists, TExprContext& ctx, TPositionHandle pos)
{
    auto columnName = jsonExists.Json().Cast<TCoMember>().Name();
    return Build<TKqpOlapJsonExists>(ctx, pos)
        .Column(columnName)
        .Path(jsonExists.JsonPath().Cast<TCoUtf8>())
        .Done();
}
TMaybeNode<TExprBase> SimplePredicatePushdown(const TCoCompare& predicate, const TExprNode& argument, TExprContext& ctx, TPositionHandle pos, bool allowApply)
{
    const auto parameters = ExtractComparisonParameters(predicate, argument, ctx, pos, allowApply);
    if (parameters.empty()) {
        return NullNode;
    }

    return ComparisonPushdown(parameters, predicate, ctx, pos);
}

TMaybeNode<TExprBase> SafeCastPredicatePushdown(const TCoFlatMap& inputFlatmap, const TExprNode& argument, TExprContext& ctx, TPositionHandle pos, bool allowApply)
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
    auto left = ConvertComparisonNode(inputFlatmap.Input(), argument, ctx, pos, allowApply);
    if (left.empty()) {
        return NullNode;
    }

    auto flatmap = inputFlatmap.Lambda().Body().Cast<TCoFlatMap>();
    auto right = ConvertComparisonNode(flatmap.Input(), argument, ctx, pos, allowApply);
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

namespace {

//Workarownd for #19125
NYql::NNodes::TCoUtf8 RemoveJsonPathUnnecessaryQuote(const NYql::NNodes::TCoUtf8& node, TExprContext& ctx) {
    const std::string_view& path = node.Literal();
    if (UTF8Detect(path) == ASCII && path.starts_with("$.\"") && path.substr(3).ends_with("\"")) {
        const auto& nakedPath = path.substr(3, path.length()-4);
        for (auto c: nakedPath) {
            if (!isalpha(c) && !isdigit(c) && c != '_') {
                return node;
            }
        }
        return Build<TCoUtf8>(ctx, node.Pos()).Literal().Build(TString("$.") + nakedPath).Done();
    }
    return node;
}

TExprBase UnwrapOptionalTKqpOlapApplyColumnArg(const TExprBase& node) {
    if (const auto& maybeColumnArg = node.Maybe<TKqpOlapApplyColumnArg>()) {
        return maybeColumnArg.Cast().ColumnName();
    }
    return node;
}

} //namespace

std::vector<TExprBase> ConvertComparisonNode(const TExprBase& nodeIn, const TExprNode& argument, TExprContext& ctx, TPositionHandle pos, bool allowApply)
{
    const auto convertNode = [&ctx, &pos, &argument, allowApply](const TExprBase& node) -> TMaybeNode<TExprBase> {
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
            return Build<TKqpOlapApplyColumnArg>(ctx, pos)
                .TableRowType(ExpandType(argument.Pos(), *argument.GetTypeAnn(), ctx))
                .ColumnName(maybeMember.Cast().Name())
            .Done();
        }

        if (auto maybeJsonValue = node.Maybe<TCoJsonValue>()) {
            auto maybeColMember = maybeJsonValue.Cast().Json().Maybe<TCoMember>();
            auto maybePathUtf8 = maybeJsonValue.Cast().JsonPath().Maybe<TCoUtf8>();
            auto maybeReturningType = maybeJsonValue.Cast().ReturningType();

            YQL_ENSURE(maybeColMember, "Expected TCoMember in column field of JSON_VALUE function for pushdown");
            YQL_ENSURE(maybePathUtf8, "Expected TCoUtf8 in path of JSON_VALUE function for pushdown");

            auto builder = Build<TKqpOlapJsonValue>(ctx, pos)
                .Column(maybeColMember.Cast().Name())
                .Path(RemoveJsonPathUnnecessaryQuote(maybePathUtf8.Cast(), ctx));
            if (maybeReturningType) {
                builder.ReturningType(maybeReturningType.Cast());
            } else {
                builder.ReturningType<TCoDataType>()
                    .Type().Value("Utf8", TNodeFlags::Default).Build()
                    .Build();
            }
            return builder.Done();
        }

        if (auto maybeJsonExists = node.Maybe<TCoJsonExists>()) {
            return JsonExistsPushdown(maybeJsonExists.Cast(), ctx, pos);
        }

        if (const auto maybeJust = node.Maybe<TCoJust>()) {
            if (const auto params = ConvertComparisonNode(maybeJust.Cast().Input(), argument, ctx, pos, allowApply); 1U == params.size()) {
                return Build<TKqpOlapFilterUnaryOp>(ctx, node.Pos())
                    .Operator().Value("just", TNodeFlags::Default).Build()
                    .Arg(params.front())
                    .Done();
            }
        }

        if (const auto maybeIf = node.Maybe<TCoIf>()) {
            return YqlIfPushdown(maybeIf.Cast(), argument, ctx, allowApply);
        }

        if (const auto maybeArithmetic = node.Maybe<TCoBinaryArithmetic>()) {
            const auto arithmetic = maybeArithmetic.Cast();
            if (const auto params = ExtractBinaryFunctionParameters(arithmetic, argument, ctx, pos, allowApply)) {
                return Build<TKqpOlapFilterBinaryOp>(ctx, pos)
                        .Operator().Value(arithmetic.Ref().Content(), TNodeFlags::Default).Build()
                        .Left(UnwrapOptionalTKqpOlapApplyColumnArg(params->first))
                        .Right(UnwrapOptionalTKqpOlapApplyColumnArg(params->second))
                        .Done();
            }
        }

        if (const auto maybeArithmetic = node.Maybe<TCoUnaryArithmetic>()) {
            const auto arithmetic = maybeArithmetic.Cast();
            if (const auto params = ConvertComparisonNode(arithmetic.Arg(), argument, ctx, pos, allowApply); 1U == params.size()) {
                TString oper(arithmetic.Ref().Content());
                YQL_ENSURE(oper.to_lower());
                return Build<TKqpOlapFilterUnaryOp>(ctx, pos)
                        .Operator().Value(oper, TNodeFlags::Default).Build()
                        .Arg(UnwrapOptionalTKqpOlapApplyColumnArg(params.front()))
                        .Done();
            }
        }

        if (const auto maybeCoalesce = node.Maybe<TCoCoalesce>()) {
            return CoalescePushdown(maybeCoalesce.Cast(), argument, ctx, allowApply);
        }

        if (const auto maybeCompare = node.Maybe<TCoCompare>()) {
            if (const auto params = ExtractComparisonParameters(maybeCompare.Cast(), argument, ctx, pos, allowApply); !params.empty()) {
                return ComparisonPushdown(params, maybeCompare.Cast(), ctx, pos);
            }
        }

        if (const auto maybeFlatmap = node.Maybe<TCoFlatMap>()) {
            return SafeCastPredicatePushdown(maybeFlatmap.Cast(), argument, ctx, pos, allowApply);
        } else if (auto maybePredicate = node.Maybe<TCoCompare>()) {
            return SimplePredicatePushdown(maybePredicate.Cast(), argument, ctx, pos, allowApply);
        }

        if (allowApply) {
            return YqlApplyPushdown(node, argument, ctx);
        } else {
            return NullNode;
        }
    };

    if (const auto& list = nodeIn.Maybe<TExprList>()) {
        const auto& tuple = list.Cast();
        std::vector<TExprBase> out;

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
    } else if (const auto& node = convertNode(nodeIn); node.IsValid()) {
        return {node.Cast()};
    } else {
        return {};
    }
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

    if (const auto* stringUdfFunction = IgnoreCaseSubstringMatchFunctions.FindPtr(predicate.CallableName())) {
        const auto& leftArg = ctx.NewArgument(pos, "left");
        const auto& rightArg = ctx.NewArgument(pos, "right");

        const auto& callUdfLambda = ctx.NewLambda(pos, ctx.NewArguments(pos, {leftArg, rightArg}),
            ctx.Builder(pos)
                .Callable("Apply")
                    .Callable(0, "Udf")
                        .Atom(0, *stringUdfFunction)
                    .Seal()
                    .Add(1, leftArg)
                    .Add(2, rightArg)
                .Seal()
            .Build()
        );

        return Build<TKqpOlapApply>(ctx, pos)
            .Lambda(callUdfLambda)
            .Args()
                .Add(parameter.first)
                .Add(parameter.second)
            .Build()
            .KernelName(ctx.NewAtom(pos, *stringUdfFunction))
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
    } else {
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
        .Left(UnwrapOptionalTKqpOlapApplyColumnArg(parameter.first))
        .Right(UnwrapOptionalTKqpOlapApplyColumnArg(parameter.second))
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
                .Left(UnwrapOptionalTKqpOlapApplyColumnArg(parameters[j].first))
                .Right(UnwrapOptionalTKqpOlapApplyColumnArg(parameters[j].second))
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

template<bool Empty>
TMaybeNode<TExprBase> ExistsPushdown(const TCoExists& exists, TExprContext& ctx, TPositionHandle pos)
{
    const auto columnName = exists.Optional().Cast<TCoMember>().Name();
    return Build<TKqpOlapFilterUnaryOp>(ctx, pos)
            .Operator().Value(Empty ? "empty" : "exists", TNodeFlags::Default).Build()
            .Arg(columnName)
            .Done();
}

TFilterOpsLevels PredicatePushdown(const TExprBase& predicate, const TExprNode& argument, TExprContext& ctx, TPositionHandle pos, bool allowApply)
{
    if (const auto maybeCoalesce = predicate.Maybe<TCoCoalesce>()) {
        auto coalescePred = CoalescePushdown(maybeCoalesce.Cast(), argument, ctx, allowApply);
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
        auto pred = SimplePredicatePushdown(maybePredicate.Cast(), argument, ctx, pos, allowApply);
        return TFilterOpsLevels(pred);
    }

    if (const auto maybeIf = predicate.Maybe<TCoIf>()) {
        return YqlIfPushdown(maybeIf.Cast(), argument, ctx, allowApply);
    }

    if (const auto maybeNot = predicate.Maybe<TCoNot>()) {
        const auto notNode = maybeNot.Cast();
        if (const auto maybeExists = notNode.Value().Maybe<TCoExists>()) {
            return TFilterOpsLevels(ExistsPushdown<true>(maybeExists.Cast(), ctx, pos));
        }
        auto pushedFilters = PredicatePushdown(notNode.Value(), argument, ctx, pos, allowApply);
        pushedFilters.WrapToNotOp(ctx, pos);
        return pushedFilters;
    }

    if (predicate.Maybe<TCoAnd>() || predicate.Maybe<TCoOr>() || predicate.Maybe<TCoXor>()) {
        TVector<TExprBase> firstLvlOps;
        TVector<TExprBase> secondLvlOps;
        firstLvlOps.reserve(predicate.Ptr()->ChildrenSize());
        secondLvlOps.reserve(predicate.Ptr()->ChildrenSize());

        for (auto& child: predicate.Ptr()->Children()) {
            auto pushedChild = PredicatePushdown(TExprBase(child), argument, ctx, pos, allowApply);

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

std::pair<TVector<TOLAPPredicateNode>, TVector<TOLAPPredicateNode>> SplitForPartialPushdown(const TOLAPPredicateNode& predicateTree, bool allowApply)
{
    bool canBePushed =  (predicateTree.CanBePushed || predicateTree.CanBePushedApply && allowApply);

    if (canBePushed) {
        return {{predicateTree}, {}};
    }

    if (!TCoAnd::Match(predicateTree.ExprNode.Get())) {
        // We can partially pushdown predicates from AND operator only.
        // For OR operator we would need to have several read operators which is not acceptable.
        // TODO: Add support for NOT(op1 OR op2), because it expands to (!op1 AND !op2).
        return {{}, {predicateTree}};
    }

    bool isFoundNotStrictOp = false;
    TVector<TOLAPPredicateNode> pushable;
    TVector<TOLAPPredicateNode> remaining;
    for (const auto& predicate : predicateTree.Children) {
        canBePushed =  (predicate.CanBePushed || predicate.CanBePushedApply && allowApply);
        if (canBePushed && !isFoundNotStrictOp) {
            pushable.emplace_back(predicate);
        } else {
            if (!IsStrict(predicate.ExprNode)) {
                isFoundNotStrictOp = true;
            }
            remaining.emplace_back(predicate);
        }
    }
    return {pushable, remaining};
}

bool IsSuitableToCollectProjection(TExprBase node) {
    // Currently support only `JsonDocument`.
    if (auto maybeJsonValue = node.Maybe<TCoJsonValue>()) {
        auto jsonMember = maybeJsonValue.Cast().Json().Maybe<TCoMember>();
        auto jsonPath = maybeJsonValue.Cast().JsonPath().Maybe<TCoUtf8>();
        return jsonMember && jsonPath;
    }
    return false;
}

// Collects all operations for projections and returns a vector of pair - [columName, olap operation].
TVector<std::pair<TString, TExprNode::TPtr>> CollectOlapOperationsForProjections(const TExprNode::TPtr& node, const TExprNode& arg,
                                                                                 TNodeOnNodeOwnedMap& replaces,
                                                                                 const THashSet<TString>& predicateMembers, TExprContext& ctx) {
    auto asStructPred = [](const TExprNode::TPtr& node) -> bool { return !!TMaybeNode<TCoAsStruct>(node); };
    auto memberPred = [](const TExprNode::TPtr& node) { return !!TMaybeNode<TCoMember>(node); };
    THashSet<TString> projectionMembers;
    ui32 nextMemberId = 0;

    TVector<std::pair<TString, TExprNode::TPtr>> olapOperationsForProjections;
    // Expressions for projections are placed in `AsStruct` callable.
    if (auto asStruct = FindNode(node, asStructPred)) {
        // Process each child for `AsStruct` callable.
        for (auto child : TExprBase(asStruct).Cast<TCoAsStruct>()) {
            if (IsSuitableToCollectProjection(child.Item(1))) {
                // Search for the `TCoMember` in expression, we need expression with only one `TCoMember`.
                if (auto originalMembers = FindNodes(child.Item(1).Ptr(), memberPred); originalMembers.size() == 1) {
                    // Convert YQL op to OLAP op.
                    if (auto olapOperations = ConvertComparisonNode(TExprBase(child.Item(1)), arg, ctx, node->Pos(), false);
                        olapOperations.size() == 1) {
                        auto originalMember = TExprBase(originalMembers.front()).Cast<TCoMember>();
                        auto originalMemberName = TString(originalMember.Name());

                        if (!predicateMembers.contains(originalMemberName)) {
                            if (projectionMembers.contains(originalMemberName)) {
                                originalMemberName = "__kqp_olap_projection_" + originalMemberName + ToString(nextMemberId++);
                            } else {
                                projectionMembers.insert(originalMemberName);
                            }

                            auto newMember = Build<TCoMember>(ctx, node->Pos())
                                .Struct(originalMember.Struct())
                                .Name<TCoAtom>()
                                    .Value(originalMemberName)
                                    .Build()
                            .Done();

                            auto olapOperation = olapOperations.front();
                            // Replace full expression with only member.
                            replaces[child.Item(1).Raw()] = newMember.Ptr();
                            olapOperationsForProjections.emplace_back(TString(newMember.Name()), olapOperation.Ptr());

                            YQL_CLOG(TRACE, ProviderKqp)
                                << "[OLAP PROJECTION] Operation in olap dialect: " << KqpExprToPrettyString(olapOperation, ctx);
                        }
                    }
                }
            }
        }
    }

    return olapOperationsForProjections;
}

void CollectPredicateMembers(TExprNode::TPtr predicate, THashSet<TString>& predicateMembers) {
    auto memberPred = [](const TExprNode::TPtr& node) { return !!TMaybeNode<TCoMember>(node); };
    auto members = FindNodes(predicate, memberPred);
    for (const auto& member : members) {
        predicateMembers.insert(TString(TExprBase(member).Cast<TCoMember>().Name()));
    }
}

}  // anonymous namespace end

TExprBase KqpPushOlapProjections(TExprBase node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx,
    TTypeAnnotationContext& typesCtx)
{
    Y_UNUSED(typesCtx);
    if (!(kqpCtx.Config->HasOptEnableOlapPushdown() && kqpCtx.Config->GetEnableOlapPushdownProjections())) {
        return node;
    }

    if (!node.Maybe<TCoFlatMap>().Input().Maybe<TKqpReadOlapTableRanges>()) {
        return node;
    }

    auto flatmap = node.Cast<TCoFlatMap>();
    const auto& lambda = flatmap.Lambda();

    // Collect `TCoMembers` from predicate, we cannot push projection if some predicate for the same column still not pushed.
    THashSet<TString> predicateMembers;
    if (auto maybeOptionalIf = lambda.Body().Maybe<TCoOptionalIf>()) {
        CollectPredicateMembers(maybeOptionalIf.Cast().Predicate().Ptr(), predicateMembers);
    }

    // Combinations of `OlapAgg` and `OlapProjections` are not supported yet.
    auto olapAggPred = [](const TExprNode::TPtr& node) -> bool { return !!TMaybeNode<TKqpOlapAgg>(node); };
    if (auto maybeOlapAgg = FindNode(lambda.Body().Ptr(), olapAggPred)) {
        return node;
    }

    const auto& lambdaArg = lambda.Args().Arg(0).Ref();
    auto read = flatmap.Input().Cast<TKqpReadOlapTableRanges>();

    TNodeOnNodeOwnedMap replaces;
    auto olapOperationsForProjections = CollectOlapOperationsForProjections(flatmap.Ptr(), lambdaArg, replaces, predicateMembers, ctx);
    if (olapOperationsForProjections.empty()) {
        return node;
    }

    TVector<TExprBase> projections;
    for (const auto& [columnName, olapOperation] : olapOperationsForProjections) {
        auto olapProjection = Build<TKqpOlapProjection>(ctx, node.Pos())
            .OlapOperation(olapOperation)
            .ColumnName().Build(columnName)
            .Done();
        projections.push_back(olapProjection);
    }

    auto olapProjections = Build<TKqpOlapProjections>(ctx, node.Pos())
        .Input(read.Process().Body())
        .Projections()
            .Add(projections)
            .Build()
        .Done();

    auto newLambda = Build<TCoLambda>(ctx, node.Pos())
        .Args({"arg"})
        .Body<TExprApplier>()
            .Apply(olapProjections)
            .With(read.Process().Args().Arg(0), "arg")
            .Build()
        .Done();

    auto newRead = Build<TKqpReadOlapTableRanges>(ctx, node.Pos())
        .Table(read.Table())
        .Ranges(read.Ranges())
        .Columns(read.Columns())
        .Settings(read.Settings())
        .ExplainPrompt(read.ExplainPrompt())
        .Process(newLambda)
        .Done();

    replaces[read.Raw()] = newRead.Ptr();
    auto newFlatmap = TExprBase(TExprBase(ctx.ReplaceNodes(flatmap.Ptr(), replaces)).Cast<TCoFlatMap>());

    YQL_CLOG(TRACE, ProviderKqp) << "[OLAP PROJECTION] After rewrite: " << KqpExprToPrettyString(newFlatmap, ctx);
    return newFlatmap;
}

TExprBase KqpPushOlapFilter(TExprBase node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx,
    TTypeAnnotationContext& typesCtx, NYql::IGraphTransformer &typeAnn)
{
    const auto pushdownOptions = TPushdownOptions{
        kqpCtx.Config->EnableOlapScalarApply,
        kqpCtx.Config->EnableOlapSubstringPushdown
    };
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
    const auto& lambdaArg = lambda.Args().Arg(0).Ref();

    YQL_CLOG(TRACE, ProviderKqp) << "[KQP_PUSH_OLAP_FILTER] Initial lambda: " << KqpExprToPrettyString(lambda, ctx);

    const auto maybeOptionalIf = lambda.Body().Maybe<TCoOptionalIf>();
    if (!maybeOptionalIf.IsValid()) {
        return node;
    }

    const auto& optionaIf = maybeOptionalIf.Cast();
    auto predicate = optionaIf.Predicate();
    auto value = optionaIf.Value();
    // Use original value in final flatmap, because we need an original ast for the given value in `KqpPushOlapProjection`.
    auto originalValue = value;

    TOLAPPredicateNode predicateTree;
    predicateTree.ExprNode = predicate.Ptr();
    CollectPredicates(predicate, predicateTree, &lambdaArg, read.Process().Body(), pushdownOptions);
    YQL_ENSURE(predicateTree.IsValid(), "Collected OLAP predicates are invalid");

    auto [pushable, remaining] = SplitForPartialPushdown(predicateTree, false);
    TVector<TFilterOpsLevels> pushedPredicates;
    for (const auto& p: pushable) {
        pushedPredicates.emplace_back(PredicatePushdown(TExprBase(p.ExprNode), lambdaArg, ctx, node.Pos(), pushdownOptions.AllowOlapApply));
    }

    if (pushdownOptions.AllowOlapApply) {
        TVector<TOLAPPredicateNode> remainingAfterApply;
        for (const auto &predicateExprHolder : remaining) {
            // Closure an original predicate, we cannot call `Peephole` for free args.
            TVector<const TTypeAnnotationNode *> argTypes{lambda.Args().Arg(0).Ptr()->GetTypeAnn()};
            auto olapPredicateClosure = Build<TKqpOlapPredicateClosure>(ctx, node.Pos())
                .Lambda<TCoLambda>()
                    .Args({"arg"})
                    .Body<TCoOptionalIf>()
                        .Predicate<TExprApplier>()
                            .Apply(TExprBase(predicateExprHolder.ExprNode))
                            .With(lambda.Args().Arg(0), "arg")
                        .Build()
                        .Value<TExprApplier>()
                            .Apply(value)
                            .With(lambda.Args().Arg(0), "arg")
                        .Build()
                    .Build()
                .Build()
                .ArgsType(ExpandType(node.Pos(), *ctx.MakeType<TTupleExprType>(argTypes), ctx))
            .Done();

            YQL_CLOG(TRACE, ProviderKqp) << "[KQP_PUSH_OLAP_FILTER] Before peephole: " << KqpExprToPrettyString(olapPredicateClosure, ctx);

            TExprNode::TPtr afterPeephole;
            bool hasNonDeterministicFunctions;
            if (const auto status =
                    PeepHoleOptimizeNode(olapPredicateClosure.Ptr(), afterPeephole, ctx, typesCtx, &typeAnn, hasNonDeterministicFunctions);
                status != IGraphTransformer::TStatus::Ok) {
                YQL_CLOG(ERROR, ProviderKqp) << "[KQP_PUSH_OLAP_FILTER] Peephole failed with status: " << status << Endl;
                return node;
            }

            YQL_CLOG(TRACE, ProviderKqp) << "[KQP_PUSH_OLAP_FILTER] After peephole: " << KqpExprToPrettyString(TExprBase(afterPeephole), ctx);

            auto lambda = TExprBase(afterPeephole).Cast<TKqpOlapPredicateClosure>().Lambda();
            auto &lArg = lambda.Args().Arg(0).Ref();

            const auto maybeIf = lambda.Body().Maybe<TCoIf>();
            if (!maybeIf.IsValid()) {
                YQL_CLOG(TRACE, ProviderKqp) << "[KQP_PUSH_OLAP_FILTER] Cannot convert to TCoIf after peephole. " << Endl;
                return node;
            }

            predicate = maybeIf.Cast().Predicate();
            TOLAPPredicateNode predicateTree;
            predicateTree.ExprNode = predicate.Ptr();
            CollectPredicates(predicate, predicateTree, &lArg, lambda.Body(), {true, pushdownOptions.PushdownSubstring});

            YQL_ENSURE(predicateTree.IsValid(), "Collected OLAP predicates are invalid");
            auto [pushable, remaining] = SplitForPartialPushdown(predicateTree, true);
            for (const auto &p : pushable) {
                if (p.CanBePushed) {
                    auto pred = PredicatePushdown(TExprBase(p.ExprNode), lArg, ctx, node.Pos(), pushdownOptions.AllowOlapApply);
                    pushedPredicates.emplace_back(pred);
                } else {
                    auto expr = YqlApplyPushdown(TExprBase(p.ExprNode), lArg, ctx);
                    TFilterOpsLevels pred(expr);
                    pushedPredicates.emplace_back(pred);
                }
            }
            if (remaining.size()) {
                Y_ENSURE(remaining.size() == 1);
                // Use an orignal expr node if we cannot push to cs.
                remainingAfterApply.push_back(predicateExprHolder);
            }
        }
        remaining = std::move(remainingAfterApply);
    }

    if (pushedPredicates.empty()) {
        return node;
    }

    const auto& pushedFilter = TFilterOpsLevels::Merge(pushedPredicates, ctx, node.Pos());
    const auto remainingFilter = CombinePredicatesWithAnd(remaining, ctx, node.Pos(), false, true);

    TMaybeNode<TExprBase> olapFilter;
    if (pushedFilter.FirstLevelOps.IsValid()) {
        olapFilter = Build<TKqpOlapFilter>(ctx, node.Pos())
            .Input(read.Process().Body())
            .Condition(pushedFilter.FirstLevelOps.Cast())
            .Done();
    }

    if (pushedFilter.SecondLevelOps.IsValid()) {
        olapFilter = Build<TKqpOlapFilter>(ctx, node.Pos())
            .Input(olapFilter.IsValid() ? olapFilter.Cast() : read.Process().Body())
            .Condition(pushedFilter.SecondLevelOps.Cast())
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
                    .Apply(remainingFilter.Cast())
                    .With(lambda.Args().Arg(0), "new_arg")
                    .Build()
                .Value<TExprApplier>()
                    .Apply(originalValue)
                    .With(lambda.Args().Arg(0), "new_arg")
                    .Build()
                .Build()
            .Build()
        .Done();

    return newFlatmap;
#endif
}

TExprBase KqpAddColumnForEmptyColumnsOlapRead(TExprBase node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx) {
    if (!node.Maybe<TKqpReadOlapTableRanges>()) {
        return node;
    }

    auto readOlap = node.Cast<TKqpReadOlapTableRanges>();
    if (readOlap.Columns().Size()!=0) {
        return node;
    }

    const auto& tableData = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, readOlap.Table().Path().Value());
    auto keyColumns = tableData.Metadata->KeyColumnNames;

    TVector<TExprNode::TPtr> newColumns;
    newColumns.push_back(ctx.NewAtom(node.Pos(), keyColumns[0]));

    return Build<TKqpReadOlapTableRanges>(ctx, node.Pos())
        .Table(readOlap.Table())
        .Ranges(readOlap.Ranges())
        .Columns()
            .Add(newColumns)
            .Build()
        .Settings(readOlap.Settings())
        .ExplainPrompt(readOlap.ExplainPrompt())
        .Process(readOlap.Process())
        .Done();
}

} // namespace NKikimr::NKqp::NOpt
