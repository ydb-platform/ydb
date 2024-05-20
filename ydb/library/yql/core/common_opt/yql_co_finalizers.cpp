#include "yql_co_extr_members.h"
#include "yql_flatmap_over_join.h"
#include "yql_co.h"

#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/core/yql_opt_utils.h>

#include <ydb/library/yql/utils/log/log.h>

namespace NYql {

namespace {

using namespace NNodes;

IGraphTransformer::TStatus MultiUsageFlatMapOverJoin(const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) {
    auto it = (*optCtx.ParentsMap).find(node.Get());
    if (it == (*optCtx.ParentsMap).cend() || it->second.size() <= 1) {
        return IGraphTransformer::TStatus::Ok;
    }

    TExprNode::TListType newParents;
    for (auto parent : it->second) {
        if (auto maybeFlatMap = TMaybeNode<TCoFlatMapBase>(parent)) {
            auto flatMap = maybeFlatMap.Cast();
            auto newParent = FlatMapOverEquiJoin(flatMap, ctx, *optCtx.ParentsMap, true, optCtx.Types->FilterPushdownOverJoinOptionalSide);
            if (!newParent.Raw()) {
                return IGraphTransformer::TStatus::Error;
            }

            if (newParent.Raw() == flatMap.Raw()) {
                return IGraphTransformer::TStatus::Ok;
            }

            newParents.push_back(newParent.Ptr());
        } else {
            return IGraphTransformer::TStatus::Ok;
        }
    }

    ui32 index = 0;
    for (auto parent : it->second) {
        toOptimize[parent] = newParents[index++];
    }

    return IGraphTransformer::TStatus::Repeat;
}

}

void RegisterCoFinalizers(TFinalizingOptimizerMap& map) {
    map[TCoExtend::CallableName()] = map[TCoOrderedExtend::CallableName()] = map[TCoMerge::CallableName()] = [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) {
        OptimizeSubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx,
            [] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap&, TExprContext& ctx) {
                return ApplyExtractMembersToExtend(input, members, ctx, " with multi-usage");
            }
        );

        return true;
    };

    map[TCoTake::CallableName()] = [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) {
        OptimizeSubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx,
            [] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap&, TExprContext& ctx) {
                return ApplyExtractMembersToTake(input, members, ctx, " with multi-usage");
            }
        );

        return true;
    };

    map[TCoSkip::CallableName()] = [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) {
        OptimizeSubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx,
            [] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap&, TExprContext& ctx) {
                return ApplyExtractMembersToSkip(input, members, ctx, " with multi-usage");
            }
        );

        return true;
    };

    map[TCoSkipNullMembers::CallableName()] = [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) {
        OptimizeSubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx,
            [] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap&, TExprContext& ctx) {
                return ApplyExtractMembersToSkipNullMembers(input, members, ctx, " with multi-usage");
            }
        );

        return true;
    };

    map[TCoFlatMap::CallableName()] = map[TCoOrderedFlatMap::CallableName()] = [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) {
        OptimizeSubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx,
            [] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap&, TExprContext& ctx) {
                return ApplyExtractMembersToFlatMap(input, members, ctx, " with multi-usage");
            }
        );

        return true;
    };

    map[TCoSort::CallableName()] = map[TCoAssumeSorted::CallableName()] = [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) {
        OptimizeSubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx,
            [] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap& parentsMap, TExprContext& ctx) {
                return ApplyExtractMembersToSort(input, members, parentsMap, ctx, " with multi-usage");
            }
        );

        return true;
    };

    map[TCoAssumeUnique::CallableName()] = map[TCoAssumeDistinct::CallableName()] =  [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) {
        OptimizeSubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx,
            [] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap&, TExprContext& ctx) {
                return ApplyExtractMembersToAssumeUnique(input, members, ctx, " with multi-usage");
            }
        );

        return true;
    };

    map[TCoTop::CallableName()] = map[TCoTopSort::CallableName()] = [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) {
        OptimizeSubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx,
            [] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap& parentsMap, TExprContext& ctx) {
                return ApplyExtractMembersToTop(input, members, parentsMap, ctx, " with multi-usage");
            }
        );

        return true;
    };

    map[TCoEquiJoin::CallableName()] = [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) {
        OptimizeSubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx,
            [](const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap&, TExprContext& ctx) {
                return ApplyExtractMembersToEquiJoin(input, members, ctx, " with multi-usage");
            }
        );
        if (!toOptimize.empty()) {
            return true;
        }

        auto status = MultiUsageFlatMapOverJoin(node, toOptimize, ctx, optCtx);
        if (status == IGraphTransformer::TStatus::Error) {
            return false;
        }

        if (status == IGraphTransformer::TStatus::Repeat) {
            return true;
        }

        return true;
    };

    map[TCoPartitionByKey::CallableName()] = [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) {
        OptimizeSubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx,
            [] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap&, TExprContext& ctx) {
                return ApplyExtractMembersToPartitionByKey(input, members, ctx, " with multi-usage");
            }
        );

        return true;
    };

    map[TCoCalcOverWindowGroup::CallableName()] = map[TCoCalcOverWindow::CallableName()] =
    map[TCoCalcOverSessionWindow::CallableName()] =
        [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx)
    {
        OptimizeSubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx,
            [] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap&, TExprContext& ctx) {
                return ApplyExtractMembersToCalcOverWindow(input, members, ctx, " with multi-usage");
            }
        );

        return true;
    };

    map[TCoAggregate::CallableName()] = [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) {
        OptimizeSubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx,
            [] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap& parentsMap, TExprContext& ctx) {
                return ApplyExtractMembersToAggregate(input, members, parentsMap, ctx, " with multi-usage");
            }
        );

        return true;
    };

    map[TCoChopper::CallableName()] = [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) {
        OptimizeSubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx,
            [] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap&, TExprContext& ctx) {
                return ApplyExtractMembersToChopper(input, members, ctx, " with multi-usage");
            }
        );

        return true;
    };

    map[TCoCollect::CallableName()] = [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) {
        OptimizeSubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx,
            [] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap&, TExprContext& ctx) {
                return ApplyExtractMembersToCollect(input, members, ctx, " with multi-usage");
            }
        );

        return true;
    };

    map[TCoMapNext::CallableName()] = [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) {
        OptimizeSubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx,
            [] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap&, TExprContext& ctx) {
                return ApplyExtractMembersToMapNext(input, members, ctx, " with multi-usage");
            }
        );

        return true;
    };

    map[TCoChain1Map::CallableName()] = [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) {
        OptimizeSubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx,
            [] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap& parentsMap, TExprContext& ctx) {
                return ApplyExtractMembersToChain1Map(input, members, parentsMap, ctx, " with multi-usage");
            }
        );

        return true;
    };

    map[TCoCondense1::CallableName()] = [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) {
        OptimizeSubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx,
            [] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap& parentsMap, TExprContext& ctx) {
                return ApplyExtractMembersToCondense1(input, members, parentsMap, ctx, " with multi-usage");
            }
        );

        return true;
    };

    map[TCoCombineCore::CallableName()] = [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) {
        OptimizeSubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx,
            [] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap&, TExprContext& ctx) {
                return ApplyExtractMembersToCombineCore(input, members, ctx, " with multi-usage");
            }
        );

        return true;
    };
}

} // NYql
