#include "kqp_expression.h"

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
    if (pred->IsCallable("PgResolvedOp") && pred->Child(0)->Content() == "=") {
        leftArg = pred->Child(2);
        rightArg = pred->Child(3);
        return true;
    } else if (pred->IsCallable("==")) {
        leftArg = pred->Child(0);
        rightArg = pred->Child(1);
        return true;
    }
    return false;
}
}

namespace NKikimr {
namespace NKqp {

TExpression::TExpression(TExprNode::TPtr node, TExprContext* ctx, const TPlanProps* props) : Ctx(ctx), PlanProps(props) {
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
        for (auto conj : body->Children()) {
            conjuncts.push_back(TExpression(conj, Ctx, PlanProps));
        }
    } else {
        conjuncts.push_back(TExpression(body, Ctx, PlanProps));
    }

    return conjuncts;
}

bool TExpression::IsColumnAccess() const {
    auto body = Node->ChildPtr(1);
    if (body->IsCallable("FromPg")) {
        body = body->ChildPtr(0);
    }

    return body->IsCallable("Member");
}

 bool TExpression::IsSingleCallable(THashSet<TString> allowedCallables) const {
     auto body = Node->ChildPtr(1);
    if (body->IsCallable(allowedCallables) && body->ChildrenSize() == 1 && body->Child(0)->IsCallable("Member")) {
        return true;
    } else {
        return false;
    }
 }

 bool TExpression::IsCast() const {
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
        GetAllMembers(body, bodyIUs, *PlanProps, false, true);

        if (bodyIUs.size() < 2) {
            return false;
        }

        if (!includeExpressions && bodyIUs.size()!= 2) {
            return false;
        }

        TVector<TInfoUnit> leftIUs;
        TVector<TInfoUnit> rightIUs;
        GetAllMembers(leftArg, leftIUs, *PlanProps, false, true);
        GetAllMembers(rightArg, rightIUs, *PlanProps, false, true);

        if (!includeExpressions) {
            return leftIUs.size()==1 && rightIUs.size()==1 && leftArg->IsCallable("Member") && rightArg->IsCallable("Member");
        } else {
            return (leftIUs.size() >= 1 && rightIUs.size() >= 1);
        }
    }
    return false;
}



}
}