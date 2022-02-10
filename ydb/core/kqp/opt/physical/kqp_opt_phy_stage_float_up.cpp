#include "kqp_opt_phy_rules.h"
#include "kqp_opt_phy_impl.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/kqp_opt_impl.h>

#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NNodes;
using TStatus = IGraphTransformer::TStatus;

namespace {
TExprNode::TListType FindPrecomputes(const TExprBase& node) {
    auto filter = [](const TExprNode::TPtr& node) {
        return !TMaybeNode<TDqPhyPrecompute>(node).IsValid();
    };

    auto predicate = [](const TExprNode::TPtr& node) {
        auto maybePrecompute = TMaybeNode<TDqPhyPrecompute>(node);

        if (!maybePrecompute.IsValid()) {
            return false;
        }

        return true;
    };

    return FindNodes(node.Ptr(), filter, predicate);
}

bool IsArgumentUsed(const TExprNode::TPtr node, const TExprNode* argument) {
    auto filter = [](const TExprNode::TPtr& node) {
        return !TMaybeNode<TDqStage>(node).IsValid();
    };

    auto predicate = [argument](const TExprNode::TPtr& node) {
        return node.Get() == argument;
    };

    return !!FindNode(node, filter, predicate);
}

} // anonymous namespace end

TExprBase KqpFloatUpStage(TExprBase node, TExprContext& ctx) {
    auto maybeStage = node.Maybe<TDqStage>();

    if (!maybeStage.IsValid()) {
        return node;
    }

    TExprNode::TListType innerPrecomputePtrs = FindPrecomputes(maybeStage.Cast().Program());

    if (innerPrecomputePtrs.empty()) {
        return node;
    }

    /*
     * There is "stage inside stage", i.e. outer stage program refers to inner stage not listed in outer stage inputs,
     * thus need to move inner stage output to outer stage input
     * I.e. you have following code, it is wrong in case stage can't be inside stage
     * Stage1(a1, .., TDqPhyPrecompute(aX), TDqPhyPrecompute(aY), ..., an) (args)
     * - let CN = Connection(Stage2)
     * -- Stage2(b1, .., bn) (args)
     * --- Operation (aX)
     * - Operation (TDqPhyPrecompute(CN))
     * - Operation (aY)
     *
     * Convert to
     *
     * Stage2(b1, .., bn, TDqPhyPrecompute(aX)) (args)
     * - Operation (aX)
     * let CN = Connection(Stage2)
     * Stage1(a1, .., TDqPhyPrecompute(aY), .., an, TDqPhyPrecompute(CN)) (args, X)
     * - Operation (X)
     * - Operation (aY)
     *
     * Also this function additionaly may move precomputes to stage inputs in case it is same logic. I.e.
     * DqStage( () () ... Operation(TDqPhyPrecompute(X)) ... )
     * will be converted to
     * DqStage( (TDqPhyPrecompute(X)) (arg1) ... Operation(arg1) ... )
     *
     * Sometimes there is situation when both stages share part of program. Thus need to deep copy both of them
     * 1. Outer stage copy converts TDqPhyPrecompute node to argument
     * 2. Inner stage should copy whole program
     *
     * Both of copy functions need to receive replacement in deepClones, i.e. which arguments to replace.
     */
    struct TStageData {
        TVector<TExprNode::TPtr> Inputs;
        TVector<TExprNode::TPtr> Args;
        TNodeOnNodeOwnedMap Replaces;

        void Add(TExprNode::TPtr input, const TExprNode* arg, TExprContext& ctx, const TString& argName) {
            auto newArg = ctx.NewArgument(input->Pos(), argName);
            Inputs.emplace_back(input);
            Args.emplace_back(newArg);
            Replaces.emplace(arg, newArg);
        }

        bool Empty() {
            return Inputs.empty();
        }
    };

    struct TStageData outer;
    auto outerStage = maybeStage.Cast();

    for (ui64 i = 0; i < outerStage.Inputs().Size(); ++i) {
        auto input = outerStage.Inputs().Item(i);
        auto arg = outerStage.Program().Args().Ptr()->Child(i);

        if (!IsArgumentUsed(outerStage.Program().Body().Ptr(), arg)) {
            continue;
        }

        outer.Add(input.Ptr(), arg, ctx, TStringBuilder() << "_kqp_outer_stage_arg_" << i);
    }

    std::vector<std::pair<TDqPhyPrecompute, TStageData>> innerPrecomputes;
    innerPrecomputes.reserve(innerPrecomputePtrs.size());

    for (auto& precomputePtr: innerPrecomputePtrs) {
        innerPrecomputes.emplace_back(
            std::make_pair<TDqPhyPrecompute, struct TStageData>(std::move(TDqPhyPrecompute(precomputePtr)), TStageData())
        );
    }

    ui64 precomputeIndex = 0;
    for (auto& item: innerPrecomputes) {
        auto innerStage = item.first.Connection().Output().Stage();

        for (ui64 i = 0; i < outerStage.Inputs().Size(); ++i) {
            auto input = outerStage.Inputs().Item(i);
            auto arg = outerStage.Program().Args().Ptr()->Child(i);

            if (!IsArgumentUsed(innerStage.Program().Body().Ptr(), arg)) {
                continue;
            }

            item.second.Add(input.Ptr(), arg, ctx, TStringBuilder() << "_kqp_inner_stage_arg_" << i);
        }

        TExprNode::TPtr newConnection;

        if (!item.second.Empty()) {
            // Deep copy inner stage
            auto newInnerBody = ctx.DeepCopy(
                innerStage.Program().Body().Ref(), ctx, item.second.Replaces,
                true /* internStrings */, true /* copyTypes */, false, nullptr
            );

            auto newInnerStage = Build<TDqStage>(ctx, innerStage.Pos())
                .Inputs()
                    .Add(item.second.Inputs)
                    .Build()
                .Program()
                    .Args(item.second.Args)
                    .Body(newInnerBody)
                    .Build()
                .Settings()
                    .Build()
                .Done();

            newConnection = ctx.ReplaceNode(
                std::move(item.first.Connection().Ptr()), innerStage.Ref(), newInnerStage.Ptr()
            );
        } else {
            newConnection = item.first.Connection().Ptr();
        }

        outer.Inputs.emplace_back(
            Build<TDqPhyPrecompute>(ctx, outerStage.Pos())
                .Connection(newConnection)
                .Done().Ptr()
        );

        auto argument = ctx.NewArgument(
            outerStage.Pos(),
            TStringBuilder() << "_kqp_inner_to_outer_arg_" << ++precomputeIndex
        );
        outer.Args.emplace_back(argument);
        outer.Replaces.emplace(item.first.Ptr().Get(), argument);
    }

    // Change TDqPhyPrecompute to arguments
    auto newOuterBody = ctx.ReplaceNodes(outerStage.Program().Body().Ptr(), outer.Replaces);

    return Build<TDqStage>(ctx, outerStage.Pos())
        .Inputs()
            .Add(outer.Inputs)
            .Build()
        .Program()
            .Args(outer.Args)
            .Body(newOuterBody)
            .Build()
        .Settings()
            .Build()
        .Done();
}

} // namespace NKikimr::NKqp::NOpt

