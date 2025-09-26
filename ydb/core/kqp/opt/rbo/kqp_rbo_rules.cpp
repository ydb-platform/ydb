#include "kqp_rbo_rules.h"
#include <ydb/core/kqp/common/kqp_yql.h>
#include <yql/essentials/utils/log/log.h>
#include <typeinfo>



using namespace NYql::NNodes;

namespace {
using namespace NKikimr;
using namespace NKikimr::NKqp; 

TExprNode::TPtr ReplaceArg(TExprNode::TPtr input, TExprNode::TPtr arg, TExprContext& ctx) {
    if (input->IsCallable("Member")) {
        auto member = TCoMember(input);
        // clang-format off
        return Build<TCoMember>(ctx, input->Pos())
            .Struct(arg)
            .Name(member.Name())
        .Done().Ptr();
        // clang-format on
    }
    else if (input->IsCallable()){
        TVector<TExprNode::TPtr> newChildren;
        for (auto c : input->Children()) {
            newChildren.push_back(ReplaceArg(c, arg, ctx));
        }
        // clang-format off
        return ctx.Builder(input->Pos())
            .Callable(input->Content())
            .Add(std::move(newChildren))
            .Seal()
        .Build();
        // clang-format on
    }
    else if(input->IsList()){
        TVector<TExprNode::TPtr> newChildren;
        for (auto c : input->Children()) {
            newChildren.push_back(ReplaceArg(c, arg, ctx));
        }
        // clang-format off
        return ctx.Builder(input->Pos())
            .List()
            .Add(std::move(newChildren))
            .Seal()
        .Build();
        // clang-format on
    }
    else {
        return input;
    }
}

TExprNode::TPtr BuildFilterLambdaFromConjuncts(TPositionHandle pos, TVector<TFilterInfo> conjuncts, TExprContext& ctx) {
    auto arg = Build<TCoArgument>(ctx, pos).Name("lambda_arg").Done();
    TExprNode::TPtr lambda;

    if (conjuncts.size()==1) {
        auto body = TExprBase(ReplaceArg(conjuncts[0].FilterBody, arg.Ptr(), ctx));

        // clang-format off
        return Build<TCoLambda>(ctx, pos)
            .Args(arg)
            .Body(body)
        .Done().Ptr();
        // clang-format on
    }
    else {
        TVector<TExprNode::TPtr> newConjuncts;

        for (auto c : conjuncts ) {
            newConjuncts.push_back( ReplaceArg(c.FilterBody, arg.Ptr(), ctx));
        }

        // clang-format off
        return Build<TCoLambda>(ctx, pos)
            .Args(arg)
            .Body<TCoAnd>()
                .Add(newConjuncts)
            .Build()
        .Done().Ptr();
        // clang-format on
    }
}

THashSet<TString> CmpOperators{"=", "<", ">", "<=", ">="};

TExprNode::TPtr PruneCast(TExprNode::TPtr node) {
    if (node->IsCallable("ToPg") || node->IsCallable("PgCast")) {
        return node->Child(0);
    }
    return node;
}

bool IsNullRejectingPredicate(const TFilterInfo& filter, TExprContext& ctx) {
    Y_UNUSED(ctx);
#ifdef DEBUG_PREDICATE
    YQL_CLOG(TRACE, CoreDq) << "IsNullRejectingPredicate: " << NYql::KqpExprToPrettyString(TExprBase(filter.FilterBody), ctx);
#endif
    auto predicate = filter.FilterBody;
    if (predicate->IsCallable("PgResolvedOp") && CmpOperators.contains(TString(predicate->Child(0)->Content()))) {
        auto left = PruneCast(predicate->Child(2));
        auto right = PruneCast(predicate->Child(3));
        return (left->IsCallable("PgConst") || right->IsCallable("PgConst"));
    }
    return false;
}
}  // namespace

namespace NKikimr {
namespace NKqp {

std::shared_ptr<IOperator> TExtractJoinExpressionsRule::SimpleTestAndApply(const std::shared_ptr<IOperator> & input, TExprContext& ctx, 
    const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx, 
    TTypeAnnotationContext& typeCtx, 
    const TKikimrConfiguration::TPtr& config,
    TPlanProps& props) {

    Y_UNUSED(ctx);
    Y_UNUSED(kqpCtx);
    Y_UNUSED(typeCtx);
    Y_UNUSED(config);
    Y_UNUSED(props);

    if (input->Kind != EOperator::Filter) {
        return input;
    }

    auto filter = CastOperator<TOpFilter>(input);
    auto conjInfo = filter->GetConjunctInfo();

    TExprNode::TPtr matchedConjunct;

    size_t idx = 0;
    for ( ; idx < conjInfo.Filters.size(); idx++) {
        auto f = conjInfo.Filters[idx];

        if (f.FilterIUs.size() == 2) {
            auto predicate = f.FilterBody;
            if (predicate->IsCallable("PgResolvedOp") && predicate->Child(0)->Content() == "=") {
                auto leftSide = predicate->Child(2);
                auto rightSide = predicate->Child(3);
                TVector<TInfoUnit> leftIUs;
                TVector<TInfoUnit> rightIUs;
                GetAllMembers(leftSide, leftIUs);
                GetAllMembers(rightSide, rightIUs);

                if (leftIUs.size()==1 && rightIUs.size()==1) {
                    matchedConjunct = predicate;
                    break;
                }
            }
        }
    }

    return input;
}

std::shared_ptr<IOperator> TPushFilterRule::SimpleTestAndApply(const std::shared_ptr<IOperator> & input, TExprContext& ctx, 
    const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx, 
    TTypeAnnotationContext& typeCtx, 
    const TKikimrConfiguration::TPtr& config,
    TPlanProps& props) {

    Y_UNUSED(kqpCtx);
    Y_UNUSED(typeCtx);
    Y_UNUSED(config);
    Y_UNUSED(props);

    if (input->Kind != EOperator::Filter) {
        return input;
    }

    auto filter = CastOperator<TOpFilter>(input);
    if (filter->GetInput()->Kind != EOperator::Join) {
        return input;
    }

    // Only handle Inner and Cross join at this time
    auto join = CastOperator<TOpJoin>(filter->GetInput());

    // Make sure the join and its inputs are single consumer
    if (!join->IsSingleConsumer() || !join->GetLeftInput()->IsSingleConsumer() || !join->GetRightInput()->IsSingleConsumer()) {
        return input;
    }

    if (join->JoinKind != "Inner" && join->JoinKind != "Cross" && to_lower(join->JoinKind) != "left") {
        YQL_CLOG(TRACE, CoreDq) << "Wrong join type " << join->JoinKind << Endl;
        return input;
    }

    auto leftIUs = join->GetLeftInput()->GetOutputIUs();
    auto rightIUs = join->GetRightInput()->GetOutputIUs();

    // Break the filter into join conditions and other conjuncts
    // Join conditions can be pushed into the join operator and conjucts can either be pushed
    // or left on top of the join
    
    auto conjunctInfo = filter->GetConjunctInfo();

    // Check if we need a top level filter
    TVector<TFilterInfo> topLevelPreds;
    TVector<TFilterInfo> pushLeft;
    TVector<TFilterInfo> pushRight;
    TVector<std::pair<TInfoUnit, TInfoUnit>> joinConditions;

    for (auto f : conjunctInfo.Filters) {
        if (!IUSetDiff(f.FilterIUs, leftIUs).size()) {
            pushLeft.push_back(f);
        }
        else if (!IUSetDiff(f.FilterIUs, rightIUs).size()) {
            pushRight.push_back(f);
        }
        else {
            topLevelPreds.push_back(f);
        }
    }

    for (auto c: conjunctInfo.JoinConditions) {
        if (!IUSetDiff({c.LeftIU}, leftIUs).size() && !IUSetDiff({c.RightIU}, rightIUs).size()) {
            joinConditions.push_back(std::make_pair(c.LeftIU, c.RightIU));
        }
        else if (!IUSetDiff({c.LeftIU}, rightIUs).size() && !IUSetDiff({c.RightIU}, leftIUs).size()) {
            joinConditions.push_back(std::make_pair(c.RightIU, c.LeftIU));
        }
        else {
            TVector<TInfoUnit> vars = {c.LeftIU};
            vars.push_back(c.RightIU);
            if (!IUSetDiff(vars, leftIUs).size()) {
                pushLeft.push_back(TFilterInfo(c.ConjunctExpr, vars));
            }
            else {
                pushRight.push_back(TFilterInfo(c.ConjunctExpr, vars));
            }
        }
    }

    if (!pushLeft.size() && !pushRight.size() && !joinConditions.size()) {
        YQL_CLOG(TRACE, CoreDq) << "Nothing to push";
        return input;
    }

    join->JoinKeys.insert(join->JoinKeys.end(), joinConditions.begin(), joinConditions.end());
    auto leftInput = join->GetLeftInput();
    auto rightInput = join->GetRightInput();

    if (pushLeft.size()) {
        auto leftLambda = BuildFilterLambdaFromConjuncts(leftInput->Node->Pos(), pushLeft, ctx);
        leftInput = std::make_shared<TOpFilter>(leftInput, leftLambda);
    }

    if (pushRight.size()) {
        if (join->JoinKind == "Left") {
            TVector<TFilterInfo> predicatesForRightSide;
            for (const auto& predicate : pushRight) {
                if (IsNullRejectingPredicate(predicate, ctx)) {
                    predicatesForRightSide.push_back(predicate);
                } else {
                    topLevelPreds.push_back(predicate);
                }
            }
            if (predicatesForRightSide.size()) {
                auto rightLambda = BuildFilterLambdaFromConjuncts(rightInput->Node->Pos(), pushRight, ctx);
                rightInput = std::make_shared<TOpFilter>(rightInput, rightLambda);
                join->JoinKind = "Inner";
            } else {
                return input;
            }
        } else {
            auto rightLambda = BuildFilterLambdaFromConjuncts(rightInput->Node->Pos(), pushRight, ctx);
            rightInput = std::make_shared<TOpFilter>(rightInput, rightLambda);
        }
    }

    if (join->JoinKind == "Cross" && joinConditions.size()) {
        join->JoinKind = "Inner";
    }

    join->Children[0] = leftInput;
    join->Children[1] = rightInput;

    if (topLevelPreds.size()) {
        auto topFilterLambda = BuildFilterLambdaFromConjuncts(join->Node->Pos(), topLevelPreds, ctx);
        return std::make_shared<TOpFilter>(join, topFilterLambda);
    } else {
        return join;
    }
}

bool TAssignStagesRule::TestAndApply(std::shared_ptr<IOperator> & input, TExprContext& ctx, 
    const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx, 
    TTypeAnnotationContext& typeCtx, 
    const TKikimrConfiguration::TPtr& config,
    TPlanProps& props) {

    Y_UNUSED(ctx);
    Y_UNUSED(kqpCtx);
    Y_UNUSED(typeCtx);
    Y_UNUSED(config);
    Y_UNUSED(props);

    auto nodeName = input->Node->Content();

    YQL_CLOG(TRACE, CoreDq) << "Assign stages: " << nodeName;

    if (input->Props.StageId.has_value()) {
        YQL_CLOG(TRACE, CoreDq) << "Assign stages: " << nodeName << " stage assigned already";
        return false;
    }

    for (auto & c : input->Children) {
        if (!c->Props.StageId.has_value()) {
            YQL_CLOG(TRACE, CoreDq) << "Assign stages: " << nodeName << " child with unassigned stage";
            return false;
        }
    }

    if (input->Kind == EOperator::EmptySource || input->Kind == EOperator::Source) {

        TString readName;
        if (input->Kind == EOperator::Source) {
            auto opRead = CastOperator<TOpRead>(input);
            auto newStageId = props.StageGraph.AddSourceStage(opRead->GetOutputIUs());
            input->Props.StageId = newStageId;
            readName = opRead->Alias;
        }
        else {
            auto newStageId = props.StageGraph.AddStage();
            input->Props.StageId = newStageId;
        }
        YQL_CLOG(TRACE, CoreDq) << "Assign stages source: " << readName;

    }

    else if (input->Kind == EOperator::Join) {
        auto join = CastOperator<TOpJoin>(input);
        auto leftStage = join->GetLeftInput()->Props.StageId;
        auto rightStage = join->GetRightInput()->Props.StageId;

        auto newStageId = props.StageGraph.AddStage();
        join->Props.StageId = newStageId;

        bool isLeftSourceStage = props.StageGraph.IsSourceStage(*leftStage);
        bool isRightSourceStage = props.StageGraph.IsSourceStage(*rightStage);

        // For cross-join we build a stage with map and broadcast connections
        if (join->JoinKind == "Cross") {
            props.StageGraph.Connect(*leftStage, newStageId, std::make_shared<TMapConnection>(isLeftSourceStage));
            props.StageGraph.Connect(*rightStage, newStageId, std::make_shared<TBroadcastConnection>(isRightSourceStage));
        }
        
        // For inner join (we don't support other joins yet) we build a new stage
        // with GraceJoinCore and connect inputs via Shuffle connections
        else {
            TVector<TInfoUnit> leftShuffleKeys;
            TVector<TInfoUnit> rightShuffleKeys;
            for (auto k : join->JoinKeys) {
                leftShuffleKeys.push_back(k.first);
                rightShuffleKeys.push_back(k.second);
            }

            props.StageGraph.Connect(*leftStage, newStageId, std::make_shared<TShuffleConnection>(leftShuffleKeys, isLeftSourceStage));
            props.StageGraph.Connect(*rightStage, newStageId, std::make_shared<TShuffleConnection>(rightShuffleKeys, isRightSourceStage));
        }
        YQL_CLOG(TRACE, CoreDq) << "Assign stages join";
    }
    else if (input->Kind == EOperator::Filter || input->Kind == EOperator::Map) {
        auto childOp = input->Children[0];
        auto prevStageId = *(childOp->Props.StageId);

        // If the child operator is a source, it requires its own stage
        // So we have build a new stage for current operator
        if (childOp->Kind == EOperator::Source) {
            auto newStageId = props.StageGraph.AddStage();
            input->Props.StageId = newStageId;
            props.StageGraph.Connect(prevStageId, newStageId, std::make_shared<TSourceConnection>());
        }
        else {
            input->Props.StageId = prevStageId;
        }
        YQL_CLOG(TRACE, CoreDq) << "Assign stages rest";
    }
    else if (input->Kind == EOperator::Limit) {
        auto newStageId = props.StageGraph.AddStage();
        input->Props.StageId = newStageId;
        auto prevStageId = *(input->Children[0]->Props.StageId);
        props.StageGraph.Connect(prevStageId, newStageId, std::make_shared<TUnionAllConnection>(props.StageGraph.IsSourceStage(prevStageId)));
    }
    else {
        Y_ENSURE(true, "Unknown operator encountered");
    }

    return true;
}

struct Scope {
    Scope(){}

    TVector<int> ParentScopes;
    bool TopScope = false;
    bool IdentityMap = true;
    THashSet<TInfoUnit, TInfoUnit::THashFunction> Unrenameable;
    TVector<TInfoUnit> OutputIUs;
    TVector<std::shared_ptr<IOperator>> Operators;

    TString ToString() {
        auto res = TStringBuilder() <<  "{parents: [";
        for (int p : ParentScopes) {
            res << p << ",";
        }
        res << "], Identity: " << IdentityMap << ", TopScope: " << TopScope << ", Unrenameable: {";
        for (auto & iu : Unrenameable) {
            res << iu.GetFullName() << ",";
        }
        res << "}, Output: {";
        for (auto & iu : OutputIUs) {
            res << iu.GetFullName() << ",";
        }
        res << "}, Operators: [";
        for (auto & op : Operators) {
            res << op->ToString() << ",";
        }
        res << "]}";
        return res;
    }
};

struct TIOperatorSharedPtrHash
{
    size_t operator()(const std::shared_ptr<IOperator>& p) const
    {
        return p ? THash<int64_t>{}((int64_t)p.get()) : 0; 
    }
};

class Scopes {
    public:
    void ComputeScopesRec(std::shared_ptr<IOperator> & op, int & currScope);
    void ComputeScopes(std::shared_ptr<IOperator> & op);

    THashMap<int, Scope> ScopeMap;
    THashMap<std::shared_ptr<IOperator>, int, TIOperatorSharedPtrHash> RevScopeMap;
};

void Scopes::ComputeScopesRec(std::shared_ptr<IOperator> & op, int & currScope) {
    if (RevScopeMap.contains(op)) {
        return;
    }
    bool makeNewScope = (op->Kind == EOperator::Map && CastOperator<TOpMap>(op)->Project) || (op->Kind == EOperator::Project) || (op->Parents.size() >= 2);
    
    YQL_CLOG(TRACE, CoreDq) << "Op: " << op->ToString() << ", nparents = " << op->Parents.size();

    if (makeNewScope) {
        currScope++;
        auto newScope = Scope();
        //FIXME: The top scope is a scope with id=1
        if (currScope==1) {
            newScope.TopScope=true;
        }

        if (op->Kind == EOperator::Map && CastOperator<TOpMap>(op)->Project) {
            auto map = CastOperator<TOpMap>(op);
            newScope.OutputIUs = map->GetOutputIUs();
            newScope.IdentityMap = false;
        } 
        else if (op->Kind == EOperator::Project) {
            auto project = CastOperator<TOpProject>(op);
            newScope.OutputIUs = project->GetOutputIUs();
            newScope.IdentityMap = false;
        }
        ScopeMap[currScope] = newScope;
    }

    if (op->Kind == EOperator::Source) {
        for (auto iu : op->GetOutputIUs()) {
            ScopeMap.at(currScope).Unrenameable.insert(iu);
        }
    }

    ScopeMap.at(currScope).Operators.push_back(op);
    RevScopeMap[op] = currScope;
    for (auto c : op->Children) {
        ComputeScopesRec(c, currScope);
    }
}

void Scopes::ComputeScopes(std::shared_ptr<IOperator> & op) {
    int currScope = 0;
    ScopeMap[0] = Scope();
    ComputeScopesRec(op, currScope);
    for (auto & [id, sc] : ScopeMap) {
        auto topOp = sc.Operators[0];
        for ( auto & p : topOp->Parents) {
            auto parentScopeId = RevScopeMap.at(p.lock());
            sc.ParentScopes.push_back(parentScopeId);
            if (topOp->Parents.size()>=2) {
                auto & parentScope = ScopeMap.at(parentScopeId);
                for ( auto iu : sc.OutputIUs) {
                    parentScope.Unrenameable.insert(iu);
                }
            }
        }
    }
}

struct TIntTUnitPairHash
{
    size_t operator()(const std::pair<int, TInfoUnit>& p) const
    {
        return THash<int>{}(p.first) ^ TInfoUnit::THashFunction{}(p.second);  
    }
};

void TRenameStage::RunStage(TRuleBasedOptimizer* optimizer, TOpRoot & root, TExprContext& ctx) {
    Y_UNUSED(optimizer);
    Y_UNUSED(ctx);

    YQL_CLOG(TRACE, CoreDq) << "Before compute parents";

    for (auto it : root) {
        YQL_CLOG(TRACE, CoreDq) << "Iterator: " << it.Current->ToString();
        for (auto c : it.Current->Children) {
            YQL_CLOG(TRACE, CoreDq) << "Child: " << c->ToString();
        }
    }

    root.ComputeParents();

    // We need to build scopes for the plan, because same aliases and variable names may be
    // used multiple times in different scopes
    auto scopes = Scopes();
    scopes.ComputeScopes(root.GetInput());

    for (auto & [id, sc] : scopes.ScopeMap) {
        YQL_CLOG(TRACE, CoreDq) << "Scope map: " << id << ": " << sc.ToString();
    }

    // Build a rename map by startingg at maps that rename variables and project
    // Follow the parent scopes as far as possible and pick the top-most mapping
    // If at any point there are multiple parent scopes - stop

    THashMap<std::pair<int, TInfoUnit>, TVector<std::pair<int, TInfoUnit>>, TIntTUnitPairHash> renameMap;

    int newAliasId = 1;

    for (auto iter : root ) {
        if (iter.Current->Kind == EOperator::Map && CastOperator<TOpMap>(iter.Current)->Project) {
            auto map = CastOperator<TOpMap>(iter.Current);

            for (auto [to, body] : map->MapElements) {

                auto scopeId = scopes.RevScopeMap.at(map);
                auto scope = scopes.ScopeMap.at(scopeId);
                auto parentScopes = scope.ParentScopes;

                // If we're not in the final scope that exports variables to the user,
                // generate a unique new alias for the variable to avoid collisions
                auto exportTo = to;
                if (!scope.TopScope) {
                    TString newAlias = "#" + std::to_string(newAliasId++);
                    exportTo = TInfoUnit(newAlias, to.ColumnName);
                }

                // "Export" the result of map output to the upper scope, but only if there is one
                // parent scope only
                auto source = std::make_pair(scopeId, to);
                auto target = std::make_pair(parentScopes[0], exportTo);
                renameMap[source].push_back(target);

                //if (parentScopes.size()==1) {
                //    renameMap[source].push_back(target);
                //}

                // If the map element is a rename, record the rename in the map within the same scope
                // However skip all unrenamable uis
                if (std::holds_alternative<TInfoUnit>(body)) {
                    auto sourceIU = std::get<TInfoUnit>(body);
                    if (!scope.Unrenameable.contains(sourceIU)) {
                        source = std::make_pair(scopeId, sourceIU);
                        target = std::make_pair(scopeId, to);
                        renameMap[source].push_back(target);
                    }
                }
            }
        }
    }

    for (auto & [key, value] : renameMap) {
        if (value.size()==1) {
            YQL_CLOG(TRACE, CoreDq) << "Rename map: " << key.second.GetFullName() << "," << key.first << " -> " << value[0].second.GetFullName() << "," << value[0].first;
        } else {
            YQL_CLOG(TRACE, CoreDq) << "Rename map: " << key.second.GetFullName() << "," << key.first << " -> ";
            for (auto v : value ) {
                YQL_CLOG(TRACE, CoreDq) << v.second.GetFullName() << "," << v.first;
            }
        }
    }

    // Make a transitive closure of rename map
    THashMap<std::pair<int, TInfoUnit>, std::pair<int, TInfoUnit>, TIntTUnitPairHash> closedMap;
    for (auto & [k, v] : renameMap) {
        if (v.size()==1) {
            closedMap[k] = v[0];
        }
    }

    bool fixpointReached = false;
    while(! fixpointReached) {

        fixpointReached = true;
        for (auto & [k,v] : closedMap) {
            if (closedMap.contains(v)) {
                fixpointReached = false;
            }

            while (closedMap.contains(v)) {
                v = closedMap.at(v);
            }
            closedMap[k] = v;
        }
    }

    // Add unique aliases 

    // Iterate through the plan, applying renames to one operator at a time

    for (auto it : root ) {
        // Build a subset of the map for the current scope only
        auto scopeId = scopes.RevScopeMap.at(it.Current);

        // Exclude all IUs from OpReads in this scope
        //THashSet<TInfoUnit, TInfoUnit::THashFunction> exclude;
        //for (auto & op : scopes.ScopeMap.at(scopeId).Operators) {
        //    if (op->Kind == EOperator::Source) {
        //        for (auto iu : op->GetOutputIUs()) {
        //            exclude.insert(iu);
        //        }
        //    }
        //}

        auto scopedRenameMap = THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>();
        for (auto & [k,v] : closedMap) {
            //if (k.first == scopeId && !exclude.contains(k.second)) {
            if (k.first == scopeId) {
                scopedRenameMap.emplace(k.second, v.second);
            }
        }

        YQL_CLOG(TRACE, CoreDq) << "Applying renames to operator: " << scopeId << ":" << it.Current->ToString();
        for (auto & [k,v] : scopedRenameMap ) {
            YQL_CLOG(TRACE, CoreDq) << "From " << k.GetFullName() << ", To " << v.GetFullName();
        }

        it.Current->RenameIUs(scopedRenameMap, ctx);
    }

}

TRuleBasedStage RuleStage1 = TRuleBasedStage({std::make_shared<TPushFilterRule>()}, true);
TRuleBasedStage RuleStage2 = TRuleBasedStage({std::make_shared<TAssignStagesRule>()}, false);

}
}  // namespace NKikimr