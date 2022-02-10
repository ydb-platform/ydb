#include "dq_opt_phy.h"

#include <ydb/library/yql/core/yql_join.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/dq/type_ann/dq_type_ann.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>

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

TMaybe<TJoinInputDesc> BuildDqJoin(const TCoEquiJoinTuple& joinTuple,
    const THashMap<TStringBuf, TJoinInputDesc>& inputs, TExprContext& ctx)
{
    auto options = joinTuple.Options();
    auto linkSettings = GetEquiJoinLinkSettings(options.Ref());
    if (linkSettings.LeftHints.contains("any") || linkSettings.RightHints.contains("any")) {
        return {};
    }

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
        .LeftInput(left->Input)
        .LeftLabel(leftTableLabel)
        .RightInput(right->Input)
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

bool CheckJoinTupleLinkSettings(const TCoEquiJoinTuple& joinTuple) {
    auto options = joinTuple.Options();
    auto linkSettings = GetEquiJoinLinkSettings(options.Ref());
    if (linkSettings.LeftHints.contains("any") || linkSettings.RightHints.contains("any")) {
        return false;
    }

    bool result = true;
    if (!joinTuple.LeftScope().Maybe<TCoAtom>()) {
        result &= CheckJoinTupleLinkSettings(joinTuple.LeftScope().Cast<TCoEquiJoinTuple>());
    }
    if (!result) {
        return result;
    }

    if (!joinTuple.RightScope().Maybe<TCoAtom>()) {
        result &= CheckJoinTupleLinkSettings(joinTuple.RightScope().Cast<TCoEquiJoinTuple>());
    }
    return result;
}

bool CheckJoinLinkSettings(const TExprBase& node) {
    if (!node.Maybe<TCoEquiJoin>()) {
        return true;
    }
    auto equiJoin = node.Cast<TCoEquiJoin>();
    YQL_ENSURE(equiJoin.ArgCount() >= 4);
    auto joinTuple = equiJoin.Arg(equiJoin.ArgCount() - 2).Cast<TCoEquiJoinTuple>();
    return CheckJoinTupleLinkSettings(joinTuple);
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
 
TExprBase DqPushJoinToStage(const TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx,
    const TParentsMap& parentsMap, bool allowStageMultiUsage) 
{
    if (!node.Maybe<TDqJoin>()) {
        return node;
    } 
 
    auto join = node.Cast<TDqJoin>();

    static const std::set<std::string_view> supportedTypes = {"Inner"sv, "Left"sv, "LeftOnly"sv, "LeftSemi"sv}; 
    auto joinType = join.JoinType().Value(); 
    if (!supportedTypes.contains(joinType)) { 
        return node; 
    } 
 
    TMaybeNode<TDqCnUnionAll> connection;
    bool pushLeft = false;
    if (join.LeftInput().Maybe<TDqCnUnionAll>() && IsDqPureExpr(join.RightInput())) {
        connection = join.LeftInput().Cast<TDqCnUnionAll>();
        pushLeft = true;
    }
    if (join.RightInput().Maybe<TDqCnUnionAll>() && IsDqPureExpr(join.LeftInput())) {
        connection = join.RightInput().Cast<TDqCnUnionAll>();
        pushLeft = false; 
    }
 
    if (!connection) {
        return node;
    }
 
    if (!IsSingleConsumerConnection(connection.Cast(), parentsMap, allowStageMultiUsage)) {
        return node;
    }
 
    TCoArgument inputArg{ctx.NewArgument(join.Pos(), "_dq_join_input")};

    auto makeFlow = [&ctx](const TExprBase& list) {
        return Build<TCoToFlow>(ctx, list.Pos())
            .Input(list)
            .Done(); 
    };

    auto phyJoin = DqMakePhyMapJoin(
        join,
        pushLeft ? TExprBase(inputArg) : makeFlow(join.LeftInput()),
        pushLeft ? makeFlow(join.RightInput()) : TExprBase(inputArg),
        ctx);

    auto lambda = Build<TCoLambda>(ctx, join.Pos())
        .Args({inputArg})
        .Body(phyJoin)
        .Done();

    auto result = DqPushLambdaToStageUnionAll(connection.Cast(), lambda, {}, ctx, optCtx); 
    if (!result) { 
        return node; 
    } 
 
    YQL_ENSURE(result.Maybe<TDqCnUnionAll>());
    return result.Cast();
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
            buildNewStage = GetStageOutputsCount(leftCn.Output().Stage(), true) > 1; 
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
 
} // namespace NYql::NDq
