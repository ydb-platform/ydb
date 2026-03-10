#include "kqp_rules_include.h"

namespace {
    
using namespace NKikimr;
using namespace NKikimr::NKqp;

TIntrusivePtr<TOpCBOTree> JoinCBOTrees(TIntrusivePtr<TOpCBOTree> & left, TIntrusivePtr<TOpCBOTree> & right, TIntrusivePtr<TOpJoin> &join) {
    auto newJoin = MakeIntrusive<TOpJoin>(left->TreeRoot, right->TreeRoot, join->Pos, join->JoinKind, join->JoinKeys);

    auto treeNodes = left->TreeNodes;
    treeNodes.insert(treeNodes.end(), right->TreeNodes.begin(), right->TreeNodes.end());
    treeNodes.push_back(newJoin);

    return MakeIntrusive<TOpCBOTree>(newJoin, treeNodes, newJoin->Pos);
}

TIntrusivePtr<TOpCBOTree> AddJoinToCBOTree(TIntrusivePtr<TOpCBOTree> & cboTree, TIntrusivePtr<TOpJoin> &join) {
    TVector<TIntrusivePtr<IOperator>> treeNodes;

    if (join->GetLeftInput() == cboTree) {
        join->SetLeftInput(cboTree->TreeRoot);
        treeNodes.insert(treeNodes.end(), cboTree->TreeNodes.begin(), cboTree->TreeNodes.end());
        treeNodes.push_back(join);
    }
    else {
        join->SetRightInput(cboTree->TreeRoot);
        treeNodes.insert(treeNodes.end(), cboTree->TreeNodes.begin(), cboTree->TreeNodes.end());
        treeNodes.push_back(join);
    }

    return MakeIntrusive<TOpCBOTree>(join, treeNodes, join->Pos);
}

TIntrusivePtr<TOpFilter> FuseFilters(const TIntrusivePtr<TOpFilter>& top, const TIntrusivePtr<TOpFilter>& bottom, bool pgSyntax) {
    TVector<TExpression> conjuncts = top->FilterExpr.SplitConjunct();
    TVector<TExpression> bottomConjuncts = bottom->FilterExpr.SplitConjunct();
    conjuncts.insert(conjuncts.begin(), bottomConjuncts.begin(), bottomConjuncts.end());

    return MakeIntrusive<TOpFilter>(bottom->GetInput(), top->Pos, MakeConjunction(conjuncts, pgSyntax));
}

} // namespace

namespace NKikimr {
namespace NKqp {

/**
 * Expanding CBO tree is more tricky:
 *  - We can have a join that joins a CBOtree with something else, and there could be a filter in between that we
 *    would like to push out
 *  - We need to extend this to support filter and aggregates that will be later supported by DP CBO
 * FIXME: Add maybes to make matching look simpler
 * FIXME: Support other joins for filter push-out, refactor into a lambda to apply to both sides
 */
TIntrusivePtr<IOperator> TExpandCBOTreeRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {
    Y_UNUSED(ctx);
    Y_UNUSED(props);

    // In case there is a join of a CBO tree (maybe with a filter stuck in-between)
    // we push this join into the CBO tree and push the filter out above

    if (input->Kind == EOperator::Join) {
        auto join = CastOperator<TOpJoin>(input);
        auto leftInput = join->GetLeftInput();
        auto rightInput = join->GetRightInput();

        TIntrusivePtr<TOpFilter> maybeFilter;
        TIntrusivePtr<TOpCBOTree> cboTree;

        bool leftSideCBOTree = true;

        auto findCBOTree = [&join](const TIntrusivePtr<IOperator>& op,
                TIntrusivePtr<TOpCBOTree>& cboTree,
                TIntrusivePtr<TOpFilter>& maybeFilter) {

            if (op->Kind == EOperator::CBOTree) {
                cboTree = CastOperator<TOpCBOTree>(op);
                return true;
            }
            if (op->Kind == EOperator::Filter &&
                    CastOperator<TOpFilter>(op)->GetInput()->Kind == EOperator::CBOTree &&
                    join->JoinKind == "Inner") {

                maybeFilter = CastOperator<TOpFilter>(op);
                cboTree = CastOperator<TOpCBOTree>(maybeFilter->GetInput());
                return true;
            }

            return false;
        };

        if (!findCBOTree(leftInput, cboTree, maybeFilter)) {
            if (!findCBOTree(rightInput, cboTree, maybeFilter)) {
                return input;
            } else {
                leftSideCBOTree = false;
            }
        }

        TIntrusivePtr<TOpFilter> maybeAnotherFilter;
        auto otherSide = leftSideCBOTree ? join->GetRightInput() : join->GetLeftInput();
        TIntrusivePtr<TOpCBOTree> otherSideCBOTree;

        if (otherSide->Kind == EOperator::Filter &&
                CastOperator<TOpFilter>(otherSide)->GetInput()->Kind == EOperator::CBOTree &&
                join->JoinKind == "Inner") {

            maybeAnotherFilter = CastOperator<TOpFilter>(otherSide);
            otherSideCBOTree = CastOperator<TOpCBOTree>(maybeAnotherFilter->GetInput());
        }

        if (otherSideCBOTree) {
            if (leftSideCBOTree) {
                cboTree = JoinCBOTrees(cboTree, otherSideCBOTree, join);
            } else {
                cboTree = JoinCBOTrees(otherSideCBOTree, cboTree, join);
            }
        } else {
            cboTree = AddJoinToCBOTree(cboTree, join);
        }

        if (maybeFilter && maybeAnotherFilter) {
            maybeFilter = FuseFilters(maybeFilter, maybeAnotherFilter, props.PgSyntax);
        } else if (maybeAnotherFilter) {
            maybeFilter = maybeAnotherFilter;
        }

        if (maybeFilter) {
            maybeFilter->SetInput(cboTree);
            return maybeFilter;
        } else {
            return cboTree;
        }
    }

    return input;
}
}
}