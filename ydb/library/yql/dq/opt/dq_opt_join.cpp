#include "dq_opt_phy.h"

#include <ydb/library/yql/core/yql_join.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/dq/type_ann/dq_type_ann.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/core/yql_type_helpers.h>

namespace NYql::NDq {

using namespace NYql::NNodes;

namespace {

struct TJoinInputDesc {
    TJoinInputDesc(TMaybe<TStringBuf> label, const TExprBase& input,
        TSet<std::pair<TStringBuf, TStringBuf>>&& keys)
        : Label(label)
        , Input(input)
        , Keys(std::move(keys)) {}

    bool IsRealTable() const {
        return Label.Defined();
    }

    TMaybe<TStringBuf> Label; // defined for real table input only, empty otherwise
    TExprBase Input;
    TSet<std::pair<TStringBuf, TStringBuf>> Keys; // set of (label, column_name) pairs in this input
};

void CollectJoinColumns(const TExprBase& joinSettings, THashMap<TStringBuf, TVector<TStringBuf>>* columnsToRename,
    THashSet<TStringBuf>* columnsToDrop)
{
    for (const auto& option : joinSettings.Ref().Children()) {
        if (option->Head().IsAtom("rename")) {
            TCoAtom fromName{option->Child(1)};
            YQL_ENSURE(!fromName.Value().Empty());
            TCoAtom toName{option->Child(2)};
            if (!toName.Value().Empty()) {
                (*columnsToRename)[fromName.Value()].emplace_back(toName.Value());
            } else {
                columnsToDrop->emplace(fromName.Value());
            }
        }
    }
}

TExprBase BuildSkipNullKeys(TExprContext& ctx, TPositionHandle pos,
    const TExprBase& input, const TVector<TCoAtom>& keys)
{
    return Build<TCoSkipNullMembers>(ctx, pos)
        .Input(input)
        .Members()
            .Add(keys)
            .Build()
        .Done();
};

TExprBase BuildDqJoinInput(TExprContext& ctx, TPositionHandle pos, const TExprBase& input, const TVector<TCoAtom>& keys, bool any) {
    if (!any) {
        return input;
    }

    auto keyExtractor = ctx.Builder(pos)
        .Lambda()
            .Param("item")
            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                auto listBuilder = parent.List();
                int pos = 0;
                for (const auto& key : keys) {
                    listBuilder
                        .Callable(pos++, "Member")
                            .Arg(0, "item")
                            .Add(1, key.Ptr())
                        .Seal();
                }

                return listBuilder.Seal();
            })
        .Seal()
        .Build();

    auto condense = Build<TCoLambda>(ctx, pos)
        .Args({"list"})
        .Body<TCoCondense1>()
            .Input("list")
            .InitHandler(BuildIdentityLambda(pos, ctx))
            .SwitchHandler()
                .Args({"item", "state"})
                .Body<TCoAggrNotEqual>()
                    .Left<TExprApplier>().Apply(TCoLambda(keyExtractor)).With(0, "item")
                        .Build()
                    .Right<TExprApplier>().Apply(TCoLambda(keyExtractor)).With(0, "state")
                        .Build()
                    .Build()
                .Build()
            .UpdateHandler()
                .Args({"item", "state"})
                .Body("state")
            .Build()
        .Build()
        .Done();

    auto partition = Build<TCoPartitionsByKeys>(ctx, pos)
        .Input(input)
        .KeySelectorLambda(keyExtractor)
        .SortDirections<TCoVoid>()
            .Build()
        .SortKeySelectorLambda<TCoVoid>()
            .Build()
        .ListHandlerLambda(condense)
        .Done();

    return partition;
}

TMaybe<TJoinInputDesc> BuildDqJoin(const TCoEquiJoinTuple& joinTuple,
    const THashMap<TStringBuf, TJoinInputDesc>& inputs, TExprContext& ctx)
{
    auto options = joinTuple.Options();
    auto linkSettings = GetEquiJoinLinkSettings(options.Ref());
    bool leftAny = linkSettings.LeftHints.contains("any");
    bool rightAny = linkSettings.RightHints.contains("any");

    TMaybe<TJoinInputDesc> left;
    if (joinTuple.LeftScope().Maybe<TCoAtom>()) {
        left = inputs.at(joinTuple.LeftScope().Cast<TCoAtom>().Value());
        YQL_ENSURE(left, "unknown scope " << joinTuple.LeftScope().Cast<TCoAtom>().Value());
    } else {
        left = BuildDqJoin(joinTuple.LeftScope().Cast<TCoEquiJoinTuple>(), inputs, ctx);
        if (!left) {
            return {};
        }
    }

    TMaybe<TJoinInputDesc> right;
    if (joinTuple.RightScope().Maybe<TCoAtom>()) {
        right = inputs.at(joinTuple.RightScope().Cast<TCoAtom>().Value());
        YQL_ENSURE(right, "unknown scope " << joinTuple.RightScope().Cast<TCoAtom>().Value());
    } else {
        right = BuildDqJoin(joinTuple.RightScope().Cast<TCoEquiJoinTuple>(), inputs, ctx);
        if (!right) {
            return {};
        }
    }

    TStringBuf joinType = joinTuple.Type().Value();
    TSet<std::pair<TStringBuf, TStringBuf>> resultKeys;
    if (joinType != TStringBuf("RightOnly") && joinType != TStringBuf("RightSemi")) {
        resultKeys.insert(left->Keys.begin(), left->Keys.end());
    }
    if (joinType != TStringBuf("LeftOnly") && joinType != TStringBuf("LeftSemi")) {
        resultKeys.insert(right->Keys.begin(), right->Keys.end());
    }

    auto leftTableLabel = left->IsRealTable()
        ? BuildAtom(*left->Label, left->Input.Pos(), ctx).Ptr()
        : Build<TCoVoid>(ctx, left->Input.Pos()).Done().Ptr();
    auto rightTableLabel = right->IsRealTable()
        ? BuildAtom(*right->Label, right->Input.Pos(), ctx).Ptr()
        : Build<TCoVoid>(ctx, right->Input.Pos()).Done().Ptr();

    size_t joinKeysCount = joinTuple.LeftKeys().Size() / 2;
    TVector<TCoAtom> leftJoinKeys;
    leftJoinKeys.reserve(joinKeysCount);
    TVector<TCoAtom> rightJoinKeys;
    rightJoinKeys.reserve(joinKeysCount);
    auto joinKeysBuilder = Build<TDqJoinKeyTupleList>(ctx, left->Input.Pos());

    for (size_t i = 0; i < joinKeysCount; ++i) {
        size_t keyIndex = i * 2;

        auto leftScopeAtom = joinTuple.LeftKeys().Item(keyIndex);
        auto leftColumnAtom = joinTuple.LeftKeys().Item(keyIndex + 1);
        auto rightScopeAtom = joinTuple.RightKeys().Item(keyIndex);
        auto rightColumnAtom = joinTuple.RightKeys().Item(keyIndex + 1);

        auto leftKey = Build<TCoAtom>(ctx, left->Input.Pos())
            .Value(left->IsRealTable()
                ? ToString(leftColumnAtom.Value())
                : FullColumnName(leftScopeAtom.Value(), leftColumnAtom.Value()))
            .Done();
        auto rightKey = Build<TCoAtom>(ctx, right->Input.Pos())
            .Value(right->IsRealTable()
                ? ToString(rightColumnAtom.Value())
                : FullColumnName(rightScopeAtom.Value(), rightColumnAtom.Value()))
            .Done();

        joinKeysBuilder.Add<TDqJoinKeyTuple>()
            .LeftLabel(leftScopeAtom)
            .LeftColumn(leftColumnAtom)
            .RightLabel(rightScopeAtom)
            .RightColumn(rightColumnAtom)
            .Build();

        leftJoinKeys.emplace_back(leftKey);
        rightJoinKeys.emplace_back(rightKey);
    }

    auto dqJoin = Build<TDqJoin>(ctx, joinTuple.Pos())
        .LeftInput(BuildDqJoinInput(ctx, joinTuple.Pos(), left->Input, leftJoinKeys, leftAny))
        .LeftLabel(leftTableLabel)
        .RightInput(BuildDqJoinInput(ctx, joinTuple.Pos(), right->Input, rightJoinKeys, rightAny))
        .RightLabel(rightTableLabel)
        .JoinType(joinTuple.Type())
        .JoinKeys(joinKeysBuilder.Done())
        .Done();

    return TJoinInputDesc(Nothing(), dqJoin, std::move(resultKeys));
}

TMaybe<TJoinInputDesc> PrepareJoinInput(const TCoEquiJoinInput& input) {
    if (!input.Scope().Maybe<TCoAtom>()) {
        YQL_CLOG(TRACE, CoreDq) << "EquiJoin input scope is not an Atom: " << input.Scope().Ref().Content();
        return {};
    }
    auto scope = input.Scope().Cast<TCoAtom>().Value();

    auto listType = input.List().Ref().GetTypeAnn()->Cast<TListExprType>();
    auto resultStructType = listType->GetItemType()->Cast<TStructExprType>();

    TSet<std::pair<TStringBuf, TStringBuf>> keys;
    for (auto member : resultStructType->GetItems()) {
        keys.emplace(scope, member->GetName());
    }

    return TJoinInputDesc(scope, input.List(), std::move(keys));
}

TStringBuf RotateRightJoinType(TStringBuf joinType) {
    if (joinType == "Right") {
        return "Left";
    }
    if (joinType == "RightOnly") {
        return "LeftOnly";
    }
    if (joinType == "RightSemi") {
        return "LeftSemi";
    }
    YQL_ENSURE(false, "unexpected right join type " << joinType);
}

std::pair<TVector<TCoAtom>, TVector<TCoAtom>> GetJoinKeys(const TDqJoin& join, TExprContext& ctx) {
    TVector<TCoAtom> leftJoinKeys;
    TVector<TCoAtom> rightJoinKeys;

    auto size = join.JoinKeys().Size();
    leftJoinKeys.reserve(size);
    rightJoinKeys.reserve(size);

    for (const auto& keyTuple : join.JoinKeys()) {
        auto leftLabel = keyTuple.LeftLabel().Value();
        auto rightLabel = keyTuple.RightLabel().Value();

        auto leftKey = Build<TCoAtom>(ctx, join.Pos())
            .Value(join.LeftLabel().Maybe<TCoAtom>()
                ? keyTuple.LeftColumn().StringValue()
                : FullColumnName(leftLabel, keyTuple.LeftColumn().Value()))
            .Done();

        auto rightKey = Build<TCoAtom>(ctx, join.Pos())
            .Value(join.RightLabel().Maybe<TCoAtom>()
                ? keyTuple.RightColumn().StringValue()
                : FullColumnName(rightLabel, keyTuple.RightColumn().Value()))
            .Done();

        leftJoinKeys.emplace_back(std::move(leftKey));
        rightJoinKeys.emplace_back(std::move(rightKey));
    }

    return std::make_pair(std::move(leftJoinKeys), std::move(rightJoinKeys));
}


TDqPhyMapJoin DqMakePhyMapJoin(const TDqJoin& join, const TExprBase& leftInput, const TExprBase& rightInput,
    TExprContext& ctx)
{
    static const std::set<std::string_view> supportedTypes = {"Inner"sv, "Left"sv, "LeftOnly"sv, "LeftSemi"sv};
    auto joinType = join.JoinType().Value();
    bool supportedJoin = supportedTypes.contains(joinType);
    YQL_ENSURE(supportedJoin, "" << joinType);

    auto [leftJoinKeys, rightJoinKeys] = GetJoinKeys(join, ctx);

    TVector<TCoAtom> leftFilterKeys;
    TVector<TCoAtom> rightFilterKeys;

    if (joinType == "Inner"sv || joinType == "LeftSemi"sv) {
        for (const auto& key : leftJoinKeys) {
            leftFilterKeys.push_back(key);
        }
    }

    for (const auto& key : rightJoinKeys) {
        rightFilterKeys.push_back(key);
    }

    auto leftFilteredInput = BuildSkipNullKeys(ctx, join.Pos(), leftInput, leftFilterKeys);
    auto rightFilteredInput = BuildSkipNullKeys(ctx, join.Pos(), rightInput, rightFilterKeys);

    return Build<TDqPhyMapJoin>(ctx, join.Pos())
        .LeftInput(leftFilteredInput)
        .LeftLabel(join.LeftLabel())
        .RightInput(rightFilteredInput)
        .RightLabel(join.RightLabel())
        .JoinType(join.JoinType())
        .JoinKeys(join.JoinKeys())
        .Done();
}

} // namespace

// used in yql_dq_recapture.cpp
bool CheckJoinColumns(const TExprBase& node) {
    try {
        auto equiJoin = node.Cast<TCoEquiJoin>();
        THashMap<TStringBuf, TVector<TStringBuf>> columnsToRename;
        THashSet<TStringBuf> columnsToDrop;
        CollectJoinColumns(equiJoin.Arg(equiJoin.ArgCount() - 1), &columnsToRename, &columnsToDrop);
        return true;
    } catch (...) {
        return false;
    }
}

/**
 * Rewrite `EquiJoin` to a number of `DqJoin` callables. This is done to simplify next step of building
 * physical stages with join operators.
 * Potentially this optimizer can also perform joins reorder given cardinality information.
 */
TExprBase DqRewriteEquiJoin(const TExprBase& node, TExprContext& ctx) {
    if (!node.Maybe<TCoEquiJoin>()) {
        return node;
    }
    auto equiJoin = node.Cast<TCoEquiJoin>();
    YQL_ENSURE(equiJoin.ArgCount() >= 4);

    THashMap<TStringBuf, TJoinInputDesc> inputs;
    for (size_t i = 0; i < equiJoin.ArgCount() - 2; ++i) {
        if (auto input = PrepareJoinInput(equiJoin.Arg(i).Cast<TCoEquiJoinInput>())) {
            inputs.emplace(*input->Label, std::move(*input));
        } else {
            return node;
        }
    }

    auto joinTuple = equiJoin.Arg(equiJoin.ArgCount() - 2).Cast<TCoEquiJoinTuple>();
    auto result = BuildDqJoin(joinTuple, inputs, ctx);
    if (!result) {
        return node;
    }

    THashMap<TStringBuf, TVector<TStringBuf>> columnsToRename;
    THashSet<TStringBuf> columnsToDrop;
    CollectJoinColumns(equiJoin.Arg(equiJoin.ArgCount() - 1), &columnsToRename, &columnsToDrop);

    if (columnsToRename.empty() && columnsToDrop.empty()) {
        return result->Input;
    }

    auto row = Build<TCoArgument>(ctx, node.Pos())
            .Name("row")
            .Done();

    TVector<TExprBase> members;
    for (auto key : result->Keys) {
        auto fqColumnName = FullColumnName(key.first, key.second);
        if (columnsToDrop.contains(fqColumnName)) {
            continue;
        }

        auto member = Build<TCoMember>(ctx, node.Pos())
            .Struct(row)
            .Name().Build(fqColumnName)
            .Done();

        auto* renames = columnsToRename.FindPtr(fqColumnName);
        if (renames) {
            for (const auto& name : *renames) {
                members.emplace_back(
                    Build<TCoNameValueTuple>(ctx, node.Pos())
                        .Name().Build(name)
                        .Value(member)
                        .Done());
            }
        } else {
            members.emplace_back(
                Build<TCoNameValueTuple>(ctx, node.Pos())
                    .Name().Build(fqColumnName)
                    .Value(member)
                    .Done());
        }
    }

    auto projection = Build<TCoMap>(ctx, node.Pos())
        .Input(result->Input)
        .Lambda()
            .Args({row})
            .Body<TCoAsStruct>()
                .Add(members)
                .Build()
            .Build()
        .Done();

    return projection;
}

TExprBase DqRewriteRightJoinToLeft(const TExprBase node, TExprContext& ctx) {
    if (!node.Maybe<TDqJoin>()) {
        return node;
    }

    auto dqJoin = node.Cast<TDqJoin>();
    if (!dqJoin.JoinType().Value().StartsWith("Right")) {
        return node;
    }

    auto joinKeysBuilder = Build<TDqJoinKeyTupleList>(ctx, dqJoin.Pos());
    for (const auto& keys : dqJoin.JoinKeys()) {
        joinKeysBuilder.Add<TDqJoinKeyTuple>()
            .LeftLabel(keys.RightLabel())
            .LeftColumn(keys.RightColumn())
            .RightLabel(keys.LeftLabel())
            .RightColumn(keys.LeftColumn())
            .Build();
    }

    return Build<TDqJoin>(ctx, dqJoin.Pos())
        .LeftInput(dqJoin.RightInput())
        .RightInput(dqJoin.LeftInput())
        .LeftLabel(dqJoin.RightLabel())
        .RightLabel(dqJoin.LeftLabel())
        .JoinType()
            .Value(RotateRightJoinType(dqJoin.JoinType().Value()))
            .Build()
        .JoinKeys(joinKeysBuilder.Done())
        .Done();
}

TExprBase DqRewriteLeftPureJoin(const TExprBase node, TExprContext& ctx, const TParentsMap& parentsMap,
    bool allowStageMultiUsage)
{
    if (!node.Maybe<TDqJoin>()) {
        return node;
    }

    auto join = node.Cast<TDqJoin>();

    static const std::set<std::string_view> supportedTypes = {"Left"sv, "LeftOnly"sv, "LeftSemi"sv};
    auto joinType = join.JoinType().Value();
    if (!supportedTypes.contains(joinType)) {
        return node;
    }

    if (!join.RightInput().Maybe<TDqCnUnionAll>()) {
        return node;
    }

    if (!IsDqPureExpr(join.LeftInput())) {
        return node;
    }

    auto rightConnection = join.RightInput().Cast<TDqCnUnionAll>();

    if (!IsSingleConsumerConnection(rightConnection, parentsMap, allowStageMultiUsage)) {
        return node;
    }

    auto leftStage = Build<TDqStage>(ctx, join.Pos())
        .Inputs()
            .Build()
        .Program()
            .Args({})
            .Body<TCoToFlow>()
                .Input(join.LeftInput())
                .Build()
            .Build()
        .Settings(TDqStageSettings().BuildNode(ctx, join.Pos()))
        .Done();

    auto leftConnection = Build<TDqCnUnionAll>(ctx, join.Pos())
        .Output()
            .Stage(leftStage)
            .Index().Build("0")
            .Build()
        .Done();

    // TODO: Right input might be large, there are better possible physical plans.
    // We only need matching key from the right side. Instead of broadcasting
    // all right input data to single task, we can do a "partial" right semi join
    // on in the right stage to extract only necessary rows.
    return Build<TDqJoin>(ctx, join.Pos())
        .LeftInput(leftConnection)
        .LeftLabel(join.LeftLabel())
        .RightInput(join.RightInput())
        .RightLabel(join.RightLabel())
        .JoinType().Build(joinType)
        .JoinKeys(join.JoinKeys())
        .Done();
}

TExprBase DqBuildPhyJoin(const TDqJoin& join, bool pushLeftStage, TExprContext& ctx, IOptimizationContext& optCtx) {
    static const std::set<std::string_view> supportedTypes = {
        "Inner"sv,
        "Left"sv,
        "Cross"sv,
        "LeftOnly"sv,
        "LeftSemi"sv
    };

    auto joinType = join.JoinType().Value();

    if (!supportedTypes.contains(joinType)) {
        return join;
    }

    YQL_ENSURE(join.LeftInput().Maybe<TDqCnUnionAll>());
    TDqCnUnionAll leftCn = join.LeftInput().Cast<TDqCnUnionAll>();

    TMaybeNode<TDqCnUnionAll> rightCn = join.RightInput().Maybe<TDqCnUnionAll>();
    YQL_ENSURE(rightCn || IsDqPureExpr(join.RightInput(), /* isPrecomputePure */ true));

    TMaybeNode<TDqCnBroadcast> rightBroadcast;
    TNodeOnNodeOwnedMap rightPrecomputes;

    if (rightCn) {
        auto collectRightStage = Build<TDqStage>(ctx, join.Pos())
            .Inputs()
                .Add(rightCn.Cast())
                .Build()
            .Program()
                .Args({"stream"})
                .Body("stream")
                .Build()
            .Settings(TDqStageSettings().BuildNode(ctx, join.Pos()))
            .Done();

        rightBroadcast = Build<TDqCnBroadcast>(ctx, join.Pos())
            .Output()
                .Stage(collectRightStage)
                .Index().Build("0")
                .Build()
            .Done();
    } else {
        YQL_CLOG(TRACE, CoreDq) << "-- DqBuildPhyJoin: right input is DqPure expr";

        // right input is DqPure expression (may contain precomputes)
        VisitExpr(join.RightInput().Ptr(), [&rightPrecomputes](const TExprNode::TPtr& node) {
                if (TDqPhyPrecompute::Match(node.Get())) {
                    rightPrecomputes[node.Get()] = node;
                    return false;
                }
                return true;
            },
            [](const TExprNode::TPtr&) { return true; });

        if (rightPrecomputes.empty()) {
            // absolutely pure expression
            YQL_CLOG(TRACE, CoreDq) << "-- DqBuildPhyJoin: right input is absolutely pure expr";
        } else {
            YQL_CLOG(TRACE, CoreDq) << "-- DqBuildPhyJoin: right input is DqPure expr with " << rightPrecomputes.size()
                << " precomputes";

            if (IsDqDependsOnStage(join.RightInput(), leftCn.Output().Stage())) {
                YQL_CLOG(TRACE, CoreDq) << "-- DqBuildPhyJoin: right input is DqPure expr and depends on left side";

                TVector<TCoArgument> args; args.reserve(rightPrecomputes.size());
                TVector<TExprBase> inputs; inputs.reserve(rightPrecomputes.size());
                TNodeOnNodeOwnedMap argsReplaces;
                int i = 0;
                for (auto [raw, ptr] : rightPrecomputes) {
                    args.emplace_back(TCoArgument(ctx.NewArgument(raw->Pos(), TStringBuilder() << "precompute_" << (i++))));
                    inputs.emplace_back(ptr);
                    argsReplaces[raw] = args.back().Ptr();
                }

                auto collectRightStage = Build<TDqStage>(ctx, join.Pos())
                    .Inputs()
                        .Add(inputs)
                        .Build()
                    .Program()
                        .Args(args)
                        .Body<TCoToStream>()
                            .Input(ctx.ReplaceNodes(join.RightInput().Ptr(), argsReplaces))
                            .Build()
                        .Build()
                    .Settings(TDqStageSettings().BuildNode(ctx, join.Pos()))
                    .Done();

                rightBroadcast = Build<TDqCnBroadcast>(ctx, join.Pos())
                    .Output()
                        .Stage(collectRightStage)
                        .Index().Build("0")
                        .Build()
                    .Done();
            } else {
                // do nothing
                YQL_CLOG(TRACE, CoreDq) << "-- right input is DqPure expr and doesn't depend on left side";
            }
        }
    }

    TCoArgument leftInputArg{ctx.NewArgument(join.Pos(), "_dq_join_left")};
    TCoArgument rightInputArg{ctx.NewArgument(join.Pos(), "_dq_join_right")};

    bool buildNewStage = !pushLeftStage;
    if (!rightCn) {
        // right input is DqPure expression, try to push down the join...
        if (rightPrecomputes.empty()) {
            // absolutely pure expression, don't need to create a new stage
            buildNewStage = false;
        } else {
            // right input contains precompute(s), and it may depend on left side (if rightBroadcast is defined)
            buildNewStage = rightBroadcast.IsValid();
        }
    } else if (!buildNewStage) {
        // NOTE: Can't pass data from the stage to itself.
        buildNewStage = IsDqDependsOnStage(join.RightInput(), leftCn.Output().Stage());
        if (!buildNewStage) {
            // NOTE: Do not push join to stage with multiple outputs, reduce memory footprint.
            buildNewStage = GetStageOutputsCount(leftCn.Output().Stage()) > 1;
        }
    }

    TExprBase joinRightInput = buildNewStage
        ? (TExprBase) rightInputArg
        : (rightBroadcast
            ? (TExprBase) rightBroadcast.Cast()
            : (TExprBase) Build<TCoToFlow>(ctx, join.Pos())
                .Input(join.RightInput())
                .Done());

    TMaybeNode<TExprBase> phyJoin;
    if (join.JoinType().Value() != "Cross"sv) {
        phyJoin = DqMakePhyMapJoin(join, leftInputArg, joinRightInput, ctx);
    } else {
        YQL_ENSURE(join.JoinKeys().Empty());

        phyJoin = Build<TDqPhyCrossJoin>(ctx, join.Pos())
            .LeftInput(leftInputArg)
            .LeftLabel(join.LeftLabel())
            .RightInput(joinRightInput)
            .RightLabel(join.RightLabel())
            .JoinType(join.JoinType())
            .JoinKeys(join.JoinKeys())
            .Done();
    }

    TMaybeNode<TDqCnUnionAll> newConnection;
    if (buildNewStage) {
        auto newJoinStage = Build<TDqStage>(ctx, join.Pos())
            .Inputs()
                .Add<TDqCnMap>()
                    .Output(leftCn.Output())
                    .Build()
                .Add(rightBroadcast.Cast())
                .Build()
            .Program()
                .Args({leftInputArg, rightInputArg})
                .Body(phyJoin.Cast())
                .Build()
            .Settings(TDqStageSettings().BuildNode(ctx, join.Pos()))
            .Done();

        newConnection = Build<TDqCnUnionAll>(ctx, join.Pos())
            .Output()
                .Stage(newJoinStage)
                .Index().Build("0")
                .Build()
            .Done();
    } else {
        auto lambda = Build<TCoLambda>(ctx, join.Pos())
            .Args({leftInputArg})
            .Body(phyJoin.Cast())
            .Done();

        TVector<TDqConnection> lambdaConnections;
        if (rightBroadcast) {
            lambdaConnections.emplace_back(rightBroadcast.Cast());
        }

        auto maybeCn = DqPushLambdaToStageUnionAll(leftCn, lambda, lambdaConnections, ctx, optCtx);
        YQL_ENSURE(maybeCn);

        auto newCn = maybeCn.Cast();
        YQL_ENSURE(newCn.Maybe<TDqCnUnionAll>());

        newConnection = newCn.Cast<TDqCnUnionAll>();
    }

    return newConnection.Cast();
}


TExprBase DqBuildJoinDict(const TDqJoin& join, TExprContext& ctx) {
    auto joinType = join.JoinType().Value();

    if (joinType != "Full"sv && joinType != "Exclusion"sv) {
        return join;
    }

    auto buildShuffle = [&ctx, &join](const TExprBase& input, const TVector<TCoAtom>& keys) {
        auto stage = Build<TDqStage>(ctx, join.Pos())
            .Inputs()
                .Add(input)
                .Build()
            .Program()
                .Args({"stream"})
                .Body("stream")
                .Build()
            .Settings(TDqStageSettings().BuildNode(ctx, join.Pos()))
            .Done();

        return Build<TDqCnHashShuffle>(ctx, join.Pos())
            .Output()
                .Stage(stage)
                .Index().Build("0")
                .Build()
            .KeyColumns()
                .Add(keys)
                .Build()
            .Done();
    };

    bool leftIsUnionAll = join.LeftInput().Maybe<TDqCnUnionAll>().IsValid();
    bool rightIsUnionAll = join.RightInput().Maybe<TDqCnUnionAll>().IsValid();

    TMaybeNode<TDqStage> joinStage;

    // join streams
    if (leftIsUnionAll && rightIsUnionAll) {
        auto leftCn = join.LeftInput().Cast<TDqCnUnionAll>();
        auto rightCn = join.RightInput().Cast<TDqCnUnionAll>();

        auto [leftJoinKeys, rightJoinKeys] = GetJoinKeys(join, ctx);

        auto rightShuffle = buildShuffle(rightCn, rightJoinKeys);
        auto leftShuffle = buildShuffle(leftCn, leftJoinKeys);

        TCoArgument leftInputArg{ctx.NewArgument(join.Pos(), "_dq_join_left")};
        TCoArgument rightInputArg{ctx.NewArgument(join.Pos(), "_dq_join_right")};

        auto phyJoin = Build<TDqPhyJoinDict>(ctx, join.Pos())
            .LeftInput(leftInputArg)
            .LeftLabel(join.LeftLabel())
            .RightInput(rightInputArg)
            .RightLabel(join.RightLabel())
            .JoinType(join.JoinType())
            .JoinKeys(join.JoinKeys())
            .Done();

        joinStage = Build<TDqStage>(ctx, join.Pos())
            .Inputs()
                .Add(leftShuffle)
                .Add(rightShuffle)
                .Build()
            .Program()
                .Args({leftInputArg, rightInputArg})
                .Body(phyJoin)
                .Build()
            .Settings(TDqStageSettings().BuildNode(ctx, join.Pos()))
            .Done();
    }

    // join stream with pure expr
    else if (leftIsUnionAll && IsDqPureExpr(join.RightInput(), /* isPrecomputePure */ true)) {
        auto leftCn = join.LeftInput().Cast<TDqCnUnionAll>();

        auto [leftJoinKeys, _] = GetJoinKeys(join, ctx);

        auto leftShuffle = buildShuffle(leftCn, leftJoinKeys);
        TCoArgument leftInputArg{ctx.NewArgument(join.Pos(), "_dq_join_left")};

        auto phyJoin = Build<TDqPhyJoinDict>(ctx, join.Pos())
            .LeftInput(leftInputArg)
            .LeftLabel(join.LeftLabel())
            .RightInput<TCoToStream>()
                .Input(join.RightInput())
                .Build()
            .RightLabel(join.RightLabel())
            .JoinType(join.JoinType())
            .JoinKeys(join.JoinKeys())
            .Done();

        joinStage = Build<TDqStage>(ctx, join.Pos())
            .Inputs()
                .Add(leftShuffle)
                .Build()
            .Program()
                .Args({leftInputArg})
                .Body(phyJoin)
                .Build()
            .Settings(TDqStageSettings().BuildNode(ctx, join.Pos()))
            .Done();
    }

    // join pure expr with stream
    else if (IsDqPureExpr(join.RightInput(), /* isPrecomputePure */ true) && rightIsUnionAll) {
        auto rightCn = join.RightInput().Cast<TDqCnUnionAll>();

        auto [_, rightJoinKeys] = GetJoinKeys(join, ctx);

        auto rightShuffle = buildShuffle(rightCn, rightJoinKeys);
        TCoArgument rightInputArg{ctx.NewArgument(join.Pos(), "_dq_join_right")};

        auto phyJoin = Build<TDqPhyJoinDict>(ctx, join.Pos())
            .LeftInput(join.LeftInput())
            .LeftLabel(join.LeftLabel())
            .RightInput<TCoToStream>()
                .Input(rightInputArg)
                .Build()
            .RightLabel(join.RightLabel())
            .JoinType(join.JoinType())
            .JoinKeys(join.JoinKeys())
            .Done();

        joinStage = Build<TDqStage>(ctx, join.Pos())
            .Inputs()
                .Add(rightShuffle)
                .Build()
            .Program()
                .Args({rightInputArg})
                .Body(phyJoin)
                .Build()
            .Settings(TDqStageSettings().BuildNode(ctx, join.Pos()))
            .Done();
    }

    else {
        // TODO: pure join, do nothing?
    }

    if (joinStage) {
        return Build<TDqCnUnionAll>(ctx, join.Pos())
            .Output()
                .Stage(joinStage.Cast())
                .Index().Build("0")
                .Build()
            .Done();
    }

    return join;
}

TExprBase DqBuildGraceJoin(const TDqJoin& join, TExprContext& ctx) {

    static const std::set<std::string_view> supportedTypes = {
        "Inner"sv,
        "Left"sv,
        "Cross"sv,
        "LeftOnly"sv,
        "LeftSemi"sv,
        "Right"sv,
        "RightOnly"sv,
        "RightSemi"sv,
        "Full"sv,
        "Exclusion"sv
    };

    auto joinType = join.JoinType().Value();

    if (!supportedTypes.contains(joinType)) {
        return join;
    }

    auto buildShuffle = [&ctx, &join](const TExprBase& input, const TVector<TCoAtom>& keys) {
        auto stage = Build<TDqStage>(ctx, join.Pos())
            .Inputs()
                .Add(input)
                .Build()
            .Program()
                .Args({"stream"})
                .Body("stream")
                .Build()
            .Settings(TDqStageSettings().BuildNode(ctx, join.Pos()))
            .Done();

        return Build<TDqCnHashShuffle>(ctx, join.Pos())
            .Output()
                .Stage(stage)
                .Index().Build("0")
                .Build()
            .KeyColumns()
                .Add(keys)
                .Build()
            .Done();
    };

    TMaybeNode<TDqStage> joinStage;

    auto leftCn = join.LeftInput().Cast<TDqCnUnionAll>();
    auto rightCn = join.RightInput().Cast<TDqCnUnionAll>();

    const TStructExprType* leftStructType = nullptr;
    auto leftSeqType = GetSequenceItemType(leftCn, false, ctx);
    leftStructType = leftSeqType->Cast<TStructExprType>();

    const TStructExprType* rightStructType = nullptr;
    auto rightSeqType = GetSequenceItemType(rightCn, false, ctx);
    rightStructType = rightSeqType->Cast<TStructExprType>();


    const TVector<const TItemExprType*> & leftItems = leftStructType->GetItems();
    const TVector<const TItemExprType*> & rightItems = rightStructType->GetItems();

    std::map<TStringBuf, ui32> leftNames;
    for (ui32 i = 0; i < leftItems.size(); i++) {
        auto v = leftItems[i];
        leftNames.emplace(v->GetName(), i);
    }

    std::map<TStringBuf, ui32> rightNames;
    for (ui32 i = 0; i < rightItems.size(); i++) {
        auto v = rightItems[i];
        rightNames.emplace(v->GetName(), i);
    }

    auto [leftJoinKeys, rightJoinKeys] = GetJoinKeys(join, ctx);

    auto leftJoinKeysVar = leftJoinKeys;
    auto rightJoinKeysVar = rightJoinKeys;

    auto rightShuffle = buildShuffle(rightCn, rightJoinKeys);
    auto leftShuffle = buildShuffle(leftCn, leftJoinKeys);

    TCoArgument leftInputArg{ctx.NewArgument(join.Pos(), "_dq_join_left")};
    TCoArgument rightInputArg{ctx.NewArgument(join.Pos(), "_dq_join_right")};

    auto leftWideFlow = ctx.Builder(join.Pos())
            .Callable("ExpandMap")
                .Add(0, leftInputArg.Ptr())
                .Lambda(1)
                    .Param("item")
                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                            for (ui32 i = 0U; i < leftItems.size(); ++i) {
                                parent.Callable(i, "Member")
                                    .Arg(0, "item")
                                    .Atom(1, leftItems[i]->GetName())
                                    .Seal();
                            }
                            return parent;
                        })
                .Seal()
            .Seal()
            .Build();

    auto rightWideFlow = ctx.Builder(join.Pos())
            .Callable("ExpandMap")
                .Add(0, rightInputArg.Ptr())
                .Lambda(1)
                    .Param("item")
                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                            for (ui32 i = 0U; i < rightItems.size(); ++i) {
                                parent.Callable(i, "Member")
                                    .Arg(0, "item")
                                    .Atom(1, rightItems[i]->GetName())
                                    .Seal();
                            }
                            return parent;
                        })
                .Seal()
            .Seal()
            .Build();

    auto graceJoin = ctx.Builder(join.Pos())
            .Callable("GraceJoinCore")
                .Add(0, leftWideFlow)            
                .Add(1, rightWideFlow)
                .Atom(2, joinType)
                .List(3)
                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                        for (ui32 i = 0U; i < leftJoinKeysVar.size(); ++i) {
                            parent.Atom(i, std::to_string(leftNames[leftJoinKeysVar[i]]) );
                        }
                        return parent;
                    })
                .Seal()
                .List(4)
                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                        for (ui32 i = 0U; i < rightJoinKeysVar.size(); ++i) {
                            parent.Atom(i, std::to_string(rightNames[rightJoinKeysVar[i]]) );
                        }
                        return parent;
                    })
                .Seal() 
                .List(5)
                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                        for (ui32 i = 0U; i < leftNames.size(); ++i) {
                            parent.Atom(2*i, std::to_string(i));
                            parent.Atom(2*i + 1, std::to_string(i));
                        }
                        return parent;
                    })
                .Seal()
                .List(6)
                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                        ui32 colIndexStart = leftNames.size();
                        for (ui32 i = 0U; i < rightNames.size(); ++i) {
                            parent.Atom(2*i, std::to_string(i) );
                            parent.Atom(2*i + 1, std::to_string(colIndexStart + i) );
                        }
                        return parent;
                    })
                .Seal() 
            .Seal()
            .Build();

    TStringBuf leftTableName, rightTableName; 

    if ( join.LeftLabel().Ref().IsAtom() ) {
        leftTableName  = join.LeftLabel().Cast<TCoAtom>().Value();
    }

    if ( join.RightLabel().Ref().IsAtom() ) {
        rightTableName  = join.RightLabel().Cast<TCoAtom>().Value();
    }


    TVector<TString> fullColNames;


    for (const auto & v: leftNames ) {
        auto name = FullColumnName(leftTableName, v.first);
        if ( leftTableName.size() > 0) {
            fullColNames.emplace_back(name);
        } else {
            fullColNames.emplace_back(v.first);
        }
    }

    for (const auto & v: rightNames ) {
        auto name = FullColumnName(rightTableName, v.first);
        if ( rightTableName.size() > 0) {
            fullColNames.emplace_back(name);
        } else {
            fullColNames.emplace_back(v.first);  
        }
    }


    auto narrowMapJoin = ctx.Builder(join.Pos())
            .Callable("NarrowMap")
                .Add(0, graceJoin)
                .Lambda(1)
                    .Params("fields", fullColNames.size())
                    .Callable("AsStruct")
                        .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                            ui32 i = 0U;
                            for (const auto& colName : fullColNames) {
                                parent.List(i)
                                    .Atom(0, colName)
                                    .Arg(1, "fields", i)
                                .Seal();
                                ++i;
                            }
                            return parent;
                        })
                    .Seal()
                .Seal()
            .Seal()
            .Build();

        
        joinStage = Build<TDqStage>(ctx, join.Pos())
            .Inputs()
                .Add(leftShuffle)
                .Add(rightShuffle)
                .Build()
            .Program()
                .Args({leftInputArg, rightInputArg})
                .Body(narrowMapJoin)
                .Build()
            .Settings(TDqStageSettings().BuildNode(ctx, join.Pos()))
            .Done();

        

    if (joinStage) {
        return Build<TDqCnUnionAll>(ctx, join.Pos())
            .Output()
                .Stage(joinStage.Cast())
                .Index().Build("0")
                .Build()
            .Done();
    }

    return join;
}


} // namespace NYql::NDq
