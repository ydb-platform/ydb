#include "kqp_expression.h"
#include <yql/essentials/core/yql_expr_optimize.h>

using namespace NYql::NNodes;

namespace {

using namespace NKikimr;
using namespace NKikimr::NKqp;

TExprNode::TPtr ReplaceArg(TExprNode::TPtr input, TExprNode::TPtr arg, TExprContext &ctx) {
    if (input->IsCallable("Member")) {
        auto member = TCoMember(input);
        // clang-format off
        return Build<TCoMember>(ctx, input->Pos())
            .Struct(arg)
            .Name(member.Name())
        .Done().Ptr();
        // clang-format on
    } else if (input->IsCallable()) {
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
    } else if (input->IsList()) {
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
    } else {
        return input;
    }
}

bool TestAndExtractEqualityPredicate(TExprNode::TPtr pred, TExprNode::TPtr& leftArg, TExprNode::TPtr& rightArg) {
    if (pred->IsCallable("PgResolvedOp") && pred->ChildPtr(0)->Content() == "=") {
        leftArg = pred->ChildPtr(2);
        rightArg = pred->ChildPtr(3);
        return true;
    } else if (pred->IsCallable("==")) {
        leftArg = pred->ChildPtr(0);
        rightArg = pred->ChildPtr(1);
        return true;
    }
    return false;
}

TExprNode::TPtr RenameMembers(TExprNode::TPtr input, const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction> &renameMap,
                              TExprContext &ctx) {
    if (input->IsCallable("Member")) {
        auto member = TCoMember(input);
        auto memberName = member.Name();
        if (renameMap.contains(TInfoUnit(memberName.StringValue()))) {
            auto renamed = renameMap.at(TInfoUnit(memberName.StringValue()));
            // clang-format off
             memberName = Build<TCoAtom>(ctx, input->Pos()).Value(renamed.GetFullName()).Done();
            // clang-format on
        }
        // clang-format off
        return Build<TCoMember>(ctx, input->Pos())
            .Struct(member.Struct())
            .Name(memberName)
        .Done().Ptr();
        // clang-format on
    } else if (input->IsCallable()) {
        TVector<TExprNode::TPtr> newChildren;
        for (auto c : input->ChildrenList()) {
            newChildren.push_back(RenameMembers(c, renameMap, ctx));
        }
        // clang-format off
            return ctx.Builder(input->Pos())
                .Callable(input->Content())
                    .Add(std::move(newChildren))
                    .Seal()
                .Build();
        // clang-format on
    } else if (input->IsList()) {
        TVector<TExprNode::TPtr> newChildren;
        for (auto c : input->Children()) {
            newChildren.push_back(RenameMembers(c, renameMap, ctx));
        }
        // clang-format off
            return ctx.Builder(input->Pos())
                .List()
                    .Add(std::move(newChildren))
                    .Seal()
                .Build();
        // clang-format on
    } else if (input->IsLambda()){
        auto lambda = TCoLambda(input);
        return Build<TCoLambda>(ctx, input->Pos())
            .Args(lambda.Args())
            .Body(RenameMembers(lambda.Body().Ptr(), renameMap, ctx))
            .Done().Ptr();
    } else {
        return input;
    }
}

TExprNode::TPtr FindMemberArg(TExprNode::TPtr input) {
    if (input->IsCallable("Member")) {
        auto member = TCoMember(input);
        return member.Struct().Ptr();
    } else if (input->IsCallable()) {
        for (auto c : input->ChildrenList()) {
            if (auto arg = FindMemberArg(c))
                return arg;
        }
    } else if (input->IsList()) {
        for (auto c : input->ChildrenList()) {
            if (auto arg = FindMemberArg(c)) {
                return arg;
            }
        }
    } else if (input->IsLambda()) {
        return FindMemberArg(input->ChildPtr(1));
    }
    return TExprNode::TPtr();
}
}

namespace NKikimr {
namespace NKqp {

TExpression::TExpression(TExprNode::TPtr node, TExprContext* ctx, TPlanProps* props) : Ctx(ctx), PlanProps(props) {
    Y_ENSURE(ctx, "Creating an expression will null context");

    if (node->IsLambda()) {
        Node = node;
    } else {
        auto arg = Build<TCoArgument>(*ctx, node->Pos()).Name("lambda_arg").Done().Ptr();
        Node = Build<TCoLambda>(*ctx, node->Pos())
            .Args({arg})
            .Body(ReplaceArg(node, arg, *ctx))
            .Done().Ptr();
    }
}

TVector<TExpression> TExpression::SplitConjunct() const {
    Y_ENSURE(PlanProps);

    auto body = Node->ChildPtr(1);
    if (body->IsCallable("ToPg")) {
        body = body->ChildPtr(0);
    }

    TVector<TExpression> conjuncts;
    if (body->IsCallable("And")) {
        for (auto conj : body->ChildrenList()) {
            conjuncts.push_back(TExpression(conj, Ctx, PlanProps));
        }
    } else {
        conjuncts.push_back(TExpression(body, Ctx, PlanProps));
    }

    return conjuncts;
}

bool TExpression::IsColumnAccess() const {
    Y_ENSURE(Node->IsLambda(), "Expression node is not a lambda");
    auto body = Node->ChildPtr(1);
    if (body->IsCallable("FromPg")) {
        body = body->ChildPtr(0);
    }

    return body->IsCallable("Member");
}

 bool TExpression::IsSingleCallable(const THashSet<TString>& allowedCallables) const {
    Y_ENSURE(Node->IsLambda(), "Expression node is not a lambda");
     auto body = Node->ChildPtr(1);
    if (body->IsCallable(allowedCallables) && body->ChildrenSize() == 1 && body->Child(0)->IsCallable("Member")) {
        return true;
    } else {
        return false;
    }
 }

 bool TExpression::IsCast() const {
    Y_ENSURE(Node->IsLambda(), "Expression node is not a lambda");
    auto body = Node->ChildPtr(1);
    return (body->IsCallable("ToPg") || body->IsCallable("PgCast"));
 }

bool TExpression::MaybeJoinCondition(bool includeExpressions) const {
    auto body = Node->ChildPtr(1);

    if (body->IsCallable("FromPg")) {
        body = body->ChildPtr(0);
    }

    TExprNode::TPtr leftArg;
    TExprNode::TPtr rightArg;
    if (TestAndExtractEqualityPredicate(body, leftArg, rightArg)) {
        TVector<TInfoUnit> bodyIUs;
        GetAllMembers(body, bodyIUs, *PlanProps, true, false);

        if (bodyIUs.size() < 2) {
            return false;
        }

        if (!includeExpressions && bodyIUs.size()!= 2) {
            return false;
        }

        TVector<TInfoUnit> leftIUs;
        TVector<TInfoUnit> rightIUs;
        GetAllMembers(leftArg, leftIUs, *PlanProps, true, false);
        GetAllMembers(rightArg, rightIUs, *PlanProps, true, false);

        if (!includeExpressions) {
            return leftIUs.size()==1 && rightIUs.size()==1 && leftArg->IsCallable("Member") && rightArg->IsCallable("Member");
        } else {
            return (leftIUs.size() >= 1 && rightIUs.size() >= 1);
        }
    }
    return false;
}

TExprNode::TPtr TExpression::GetLambda() const {
    Y_ENSURE(Node->IsLambda(), "Expression node is not a lambda");
    return Node;
}

TExprNode::TPtr TExpression::GetExpressionBody() const {
    Y_ENSURE(Node->IsLambda(), "Expression node is not a lambda");
    return Node->ChildPtr(1);
}

TVector<TInfoUnit> TExpression::GetInputIUs(bool includeSubplanVars, bool includeCorrelatedDeps) const {
    Y_ENSURE(Node->IsLambda(), "Expression node is not lambda");
    TVector<TInfoUnit> IUs;
    GetAllMembers(Node, IUs);
    if (IUs.empty()) {
        return {};
    }
    else {
        IUs.clear();
        Y_ENSURE(PlanProps, "Plan properties null for an expression with members");
        GetAllMembers(Node, IUs, *PlanProps, includeSubplanVars, includeCorrelatedDeps);
        return IUs;
    }
}

TExpression TExpression::ApplyRenames(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction> &renameMap) const {
    Y_ENSURE(Node->IsLambda(), "Expression node is not lambda");
    return TExpression(RenameMembers(Node, renameMap, *Ctx), Ctx, PlanProps);
}

TExpression TExpression::ApplyReplaceMap(const TNodeOnNodeOwnedMap& map, TRBOContext& ctx) const {
    Y_ENSURE(Node->IsLambda(), "Expression node is not lambda");
    TOptimizeExprSettings settings(&ctx.TypeCtx);
    TExprNode::TPtr output;
    RemapExpr(Node, output, map, ctx.ExprCtx, settings);
    YQL_CLOG(TRACE, CoreDq) << "After replace " << PrintRBOExpression(output, *Ctx);

    return TExpression(output, Ctx, PlanProps);
}

TExpression TExpression::PruneCast() const {
    Y_ENSURE(Node->IsLambda(), "Expression node is not a lambda");
    auto body = Node->ChildPtr(1);
    Y_ENSURE(body->IsCallable("ToPg") || body->IsCallable("PgCast"), "Not a cast in prune cast call");
    return TExpression(body->ChildPtr(0), Ctx, PlanProps);
}

TString PrintRBOExpression(TExprNode::TPtr expr, TExprContext & ctx) {
    if (expr->IsLambda()) {
        expr = expr->Child(1);
    }
    try {
        TConvertToAstSettings settings;
        settings.AllowFreeArgs = true;
 
        auto ast = ConvertToAst(*expr, ctx, settings);
        TStringStream exprStream;
        YQL_ENSURE(ast.Root);
        ast.Root->PrintTo(exprStream);

        TString exprText = exprStream.Str();

        return exprText;
    } catch (const std::exception& e) {
        return TStringBuilder() << "Failed to render expression to pretty string: " << e.what();
    }
}

TString TExpression::ToString() const {
    return PrintRBOExpression(Node, *Ctx);
}

TJoinCondition::TJoinCondition(const TExpression& expr) : Expr(expr)
{
    auto body = Expr.Node->ChildPtr(1);

    if (body->IsCallable("FromPg")) {
        body = body->ChildPtr(0);
    }

    TExprNode::TPtr leftArg;
    TExprNode::TPtr rightArg;
    if (TestAndExtractEqualityPredicate(body, leftArg, rightArg)) {
        EquiJoin = true;

        TVector<TInfoUnit> bodyIUs;
        GetAllMembers(body, bodyIUs, *Expr.PlanProps, false, true);

        GetAllMembers(leftArg, LeftIUs, *Expr.PlanProps, false, true);
        GetAllMembers(rightArg, RightIUs, *Expr.PlanProps, false, true);
    } else {
        EquiJoin = false;

        Y_ENSURE(body->ChildrenSize()==2, "Non-binary callable in join condition");

        GetAllMembers(body->ChildPtr(0), LeftIUs, *Expr.PlanProps, false, true);
        GetAllMembers(body->ChildPtr(1), RightIUs, *Expr.PlanProps, false, true);
    }

    if (body->ChildPtr(0)->IsCallable("Member") && body->ChildPtr(1)->IsCallable("Member")) {
        IncludesExpressions = false;
    }
}

TInfoUnit TJoinCondition::GetLeftIU() const {
    Y_ENSURE(LeftIUs.size()==1);

    return LeftIUs[0];
}

TInfoUnit TJoinCondition::GetRightIU() const {
    Y_ENSURE(RightIUs.size()==1);

    return RightIUs[0];
}

bool TJoinCondition::ExtractExpressions(TNodeOnNodeOwnedMap& renameMap, TVector<std::pair<TInfoUnit, TExprNode::TPtr>>& exprMap) {
    Y_ENSURE(Expr.PlanProps, "Plan properties null when extracting expressions from join condition");

    if (!IncludesExpressions) {
        return false;
    }

    auto body = Expr.Node->ChildPtr(1);

    if (body->IsCallable("FromPg")) {
        body = body->ChildPtr(0);
    }

    TExprNode::TPtr leftArg;
    TExprNode::TPtr rightArg;
    TestAndExtractEqualityPredicate(body, leftArg, rightArg);
    bool expressionExtracted = false;

    if (!leftArg->IsCallable("Member")) {
        auto memberArg = FindMemberArg(leftArg);
        TString newName = "_rbo_arg_" + std::to_string(Expr.PlanProps->InternalVarIdx++);

        // clang-format off
        auto newLeftArg= Build<TCoMember>(*Expr.Ctx, leftArg->Pos())
            .Struct(memberArg)
            .Name().Value(newName).Build()
            .Done().Ptr();
        // clang-format on

        renameMap[leftArg.Get()] = newLeftArg;
        exprMap.emplace_back(TInfoUnit(newName), leftArg);
        expressionExtracted = true;
    }

    if (!rightArg->IsCallable("Member")) {
        auto memberArg = FindMemberArg(rightArg);
        TString newName = "_rbo_arg_" + std::to_string(Expr.PlanProps->InternalVarIdx++);

        // clang-format off
        auto newRightArg= Build<TCoMember>(*Expr.Ctx, rightArg->Pos())
            .Struct(memberArg)
            .Name().Value(newName).Build()
            .Done().Ptr();
        // clang-format on

        renameMap[rightArg.Get()] = newRightArg;
        exprMap.emplace_back(TInfoUnit(newName), rightArg);
        expressionExtracted = true;
    }
    
    return expressionExtracted;
}

TExpression MakeColumnAccess(const TInfoUnit& column, TPositionHandle pos, TExprContext* ctx, TPlanProps* props) {
    auto lambda_arg = Build<TCoArgument>(*ctx, pos).Name("arg").Done().Ptr();

    // clang-format off
    auto lambda = Build<TCoLambda>(*ctx, pos)
        .Args({lambda_arg})
        .Body<TCoMember>()
            .Struct(lambda_arg)
            .Name().Value(column.GetFullName()).Build()
        .Build()
        .Done().Ptr();
        // clang-format on

    return TExpression(lambda, ctx, props);
}

TExpression MakeConstant(const TString& type, const TString& value, TPositionHandle pos, TExprContext* ctx) {
     auto constExpr = ctx->NewCallable(pos, type, {ctx->NewAtom(pos, value)});
     return TExpression(constExpr, ctx);
}

TExpression MakeNothing(TPositionHandle pos, const TTypeAnnotationNode* type, TExprContext* ctx) {
    Y_ENSURE(type);
    auto nullExpr = ctx->NewCallable(pos, "Nothing", {ExpandType(pos, *type, *ctx)});
    return TExpression(nullExpr, ctx);
}

TExpression MakeConjunction(const TVector<TExpression>& vec, bool pgSyntax) {
    Y_ENSURE(vec.size());

    // Fetch context and plan properties from one of the conjuncts
    TExprContext* ctx;
    TPlanProps* props;

    for (auto& expr : vec) {
        if (expr.Ctx) {
            ctx = expr.Ctx;
        }
        if (expr.PlanProps) {
            props = expr.PlanProps;
        }
    }

    Y_ENSURE(ctx);
    Y_ENSURE(props);
    auto pos = vec[0].Node->Pos();

    if (vec.size() == 1) {
        return TExpression(vec[0].Node, ctx, props);
    }

    // clang-format off
    auto lambda_arg = Build<TCoArgument>(*ctx, pos).Name("arg").Done().Ptr();
    TVector<TExprNode::TPtr> conjuncts;

    for (auto & expr : vec) {
        auto exprLambda = expr.GetExpressionBody();
        conjuncts.push_back(ReplaceArg(exprLambda, lambda_arg, *ctx));
    }

    auto conjunction = Build<TCoAnd>(*ctx, pos)
        .Add(conjuncts)
        .Done().Ptr();

    if (pgSyntax) {
        conjunction = ctx->Builder(pos).Callable("ToPg").Add(0, conjunction).Seal().Build();
    }

    auto lambda = Build<TCoLambda>(*ctx, pos)
        .Args({lambda_arg})
        .Body(conjunction)
        .Done().Ptr();
    // clang-format on

    return TExpression(lambda, ctx, props);
}

TExpression MakeBinaryPredicate(const TString& callable, const TExpression& left, const TExpression& right) {
    // Fetch context and plan properties from one of the arguments
    TExprContext* ctx;
    TPlanProps* props;

    if (left.Ctx) {
        ctx = left.Ctx;
    }
    if (left.PlanProps) {
        props = left.PlanProps;
    }
    if (right.Ctx) {
        ctx = right.Ctx;
    }
    if (right.PlanProps) {
        props = right.PlanProps;
    }

    auto pos = left.Node->Pos();

    Y_ENSURE(ctx);
    Y_ENSURE(props);

    auto lambda = ctx->NewCallable(pos, callable, {left.GetExpressionBody(), right.GetExpressionBody()});
    return TExpression(lambda, ctx, props);
}

void GetAllMembers(TExprNode::TPtr node, TVector<TInfoUnit> &IUs) {
    if (node->IsCallable("Member")) {
        auto member = TCoMember(node);
        IUs.push_back(TInfoUnit(member.Name().StringValue()));
        return;
    }

    for (auto c : node->Children()) {
        GetAllMembers(c, IUs);
    }
}

void GetAllMembers(TExprNode::TPtr node, TVector<TInfoUnit> &IUs, const TPlanProps& props, bool withSubplanContext, bool withDependencies) {
    if (node->IsCallable("Member")) {
        auto member = TCoMember(node);
        auto iu = TInfoUnit(member.Name().StringValue());
        if (props.Subplans.PlanMap.contains(iu)){
            if (withSubplanContext) {
                iu.SetSubplanContext(true);
                iu.AddDependencies(props.Subplans.PlanMap.at(iu).Tuple);
                IUs.push_back(iu);
            }
            if (withDependencies) {
                for (auto dep : props.Subplans.PlanMap.at(iu).Tuple) {
                    IUs.push_back(dep);
                }
            }
        }
        else {
            IUs.push_back(iu);
        }
        return;
    }

    for (auto c : node->Children()) {
        GetAllMembers(c, IUs, props, withSubplanContext, withDependencies);
    }
}

}
}