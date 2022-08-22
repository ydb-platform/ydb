#include "yql_co_extr_members.h"
#include "yql_flatmap_over_join.h"
#include "yql_co.h"

#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/core/yql_opt_utils.h>

#include <ydb/library/yql/utils/log/log.h>

namespace NYql {

namespace {

using namespace NNodes;

bool SubsetFieldsForNodeWithMultiUsage(const TExprNode::TPtr& node, const TParentsMap& parentsMap,
    TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx,
    std::function<TExprNode::TPtr(const TExprNode::TPtr&, const TExprNode::TPtr&, const TParentsMap&, TExprContext&)> handler)
{
    // Ignore stream input, because it cannot be used multiple times
    if (node->GetTypeAnn()->GetKind() != ETypeAnnotationKind::List) {
        return false;
    }
    auto itemType = node->GetTypeAnn()->Cast<TListExprType>()->GetItemType();
    if (itemType->GetKind() != ETypeAnnotationKind::Struct) {
        return false;
    }
    auto structType = itemType->Cast<TStructExprType>();

    auto it = parentsMap.find(node.Get());
    if (it == parentsMap.cend() || it->second.size() <= 1) {
        return false;
    }

    TSet<TStringBuf> usedFields;
    for (auto parent: it->second) {
        if (auto maybeFlatMap = TMaybeNode<TCoFlatMapBase>(parent)) {
            auto flatMap = maybeFlatMap.Cast();
            TSet<TStringBuf> lambdaSubset;
            if (!HaveFieldsSubset(flatMap.Lambda().Body().Ptr(), flatMap.Lambda().Args().Arg(0).Ref(), lambdaSubset, parentsMap)) {
                return false;
            }
            usedFields.insert(lambdaSubset.cbegin(), lambdaSubset.cend());
        }
        else if (auto maybeExtractMembers = TMaybeNode<TCoExtractMembers>(parent)) {
            auto extractMembers = maybeExtractMembers.Cast();
            for (auto member: extractMembers.Members()) {
                usedFields.insert(member.Value());
            }
        }
        else {
            return false;
        }
        if (usedFields.size() == structType->GetSize()) {
            return false;
        }
    }

    TExprNode::TListType members;
    for (auto column : usedFields) {
        members.push_back(ctx.NewAtom(node->Pos(), column));
    }

    auto newInput = handler(node, ctx.NewList(node->Pos(), std::move(members)), parentsMap, ctx);
    if (!newInput || newInput == node) {
        return false;
    }

    for (auto parent: it->second) {
        if (TCoExtractMembers::Match(parent)) {
            if (parent->GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>()->GetSize() == usedFields.size()) {
                toOptimize[parent] = newInput;
            } else {
                toOptimize[parent] = ctx.ChangeChild(*parent, 0, TExprNode::TPtr(newInput));
            }
        } else {
            toOptimize[parent] = ctx.Builder(parent->Pos())
                .Callable(parent->Content())
                    .Add(0, newInput)
                    .Lambda(1)
                        .Param("item")
                        .Apply(parent->ChildPtr(1)).With(0, "item").Seal()
                    .Seal()
                .Seal()
                .Build();
        }
    }

    return true;
}

IGraphTransformer::TStatus MultiUsageFlatMapOverJoin(const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) {
    auto it = (*optCtx.ParentsMap).find(node.Get());
    if (it == (*optCtx.ParentsMap).cend() || it->second.size() <= 1) {
        return IGraphTransformer::TStatus::Ok;
    }

    TExprNode::TListType newParents;
    for (auto parent : it->second) {
        if (auto maybeFlatMap = TMaybeNode<TCoFlatMapBase>(parent)) {
            auto flatMap = maybeFlatMap.Cast();
            auto newParent = FlatMapOverEquiJoin(flatMap, ctx, *optCtx.ParentsMap, true);
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
        SubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx,
            [] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap&, TExprContext& ctx) {
                return ApplyExtractMembersToExtend(input, members, ctx, " with multi-usage");
            }
        );

        return true;
    };

    map[TCoTake::CallableName()] = [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) {
        SubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx,
            [] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap&, TExprContext& ctx) {
                return ApplyExtractMembersToTake(input, members, ctx, " with multi-usage");
            }
        );

        return true;
    };

    map[TCoSkip::CallableName()] = [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) {
        SubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx,
            [] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap&, TExprContext& ctx) {
                return ApplyExtractMembersToSkip(input, members, ctx, " with multi-usage");
            }
        );

        return true;
    };

    map[TCoSkipNullMembers::CallableName()] = [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) {
        SubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx,
            [] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap&, TExprContext& ctx) {
                return ApplyExtractMembersToSkipNullMembers(input, members, ctx, " with multi-usage");
            }
        );

        return true;
    };

    map[TCoFlatMap::CallableName()] = map[TCoOrderedFlatMap::CallableName()] = [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) {
        SubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx,
            [] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap&, TExprContext& ctx) {
                return ApplyExtractMembersToFlatMap(input, members, ctx, " with multi-usage");
            }
        );

        return true;
    };

    map[TCoSort::CallableName()] = map[TCoAssumeSorted::CallableName()] = [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) {
        SubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx,
            [] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap& parentsMap, TExprContext& ctx) {
                return ApplyExtractMembersToSort(input, members, parentsMap, ctx, " with multi-usage");
            }
        );

        return true;
    };

    map[TCoAssumeUnique::CallableName()] = [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) {
        SubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx,
            [] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap&, TExprContext& ctx) {
                return ApplyExtractMembersToAssumeUnique(input, members, ctx, " with multi-usage");
            }
        );

        return true;
    };

    map[TCoTop::CallableName()] = map[TCoTopSort::CallableName()] = [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) {
        SubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx,
            [] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap& parentsMap, TExprContext& ctx) {
                return ApplyExtractMembersToTop(input, members, parentsMap, ctx, " with multi-usage");
            }
        );

        return true;
    };

    map[TCoEquiJoin::CallableName()] = [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) {
        if (SubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx,
            [](const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap&, TExprContext& ctx) {
                return ApplyExtractMembersToEquiJoin(input, members, ctx, " with multi-usage");
            })) {
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
        SubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx,
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
        SubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx,
            [] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap&, TExprContext& ctx) {
                return ApplyExtractMembersToCalcOverWindow(input, members, ctx, " with multi-usage");
            }
        );

        return true;
    };

    map[TCoAggregate::CallableName()] = [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) {
        SubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx,
            [] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap& parentsMap, TExprContext& ctx) {
                return ApplyExtractMembersToAggregate(input, members, parentsMap, ctx, " with multi-usage");
            }
        );

        return true;
    };

    map[TCoChopper::CallableName()] = [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) {
        SubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx,
            [] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap&, TExprContext& ctx) {
                return ApplyExtractMembersToChopper(input, members, ctx, " with multi-usage");
            }
        );

        return true;
    };

    map[TCoCollect::CallableName()] = [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) {
        SubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx,
            [] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap&, TExprContext& ctx) {
                return ApplyExtractMembersToCollect(input, members, ctx, " with multi-usage");
            }
        );

        return true;
    };

    map[TCoMapNext::CallableName()] = [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) {
        SubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx,
            [] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap&, TExprContext& ctx) {
                return ApplyExtractMembersToMapNext(input, members, ctx, " with multi-usage");
            }
        );

        return true;
    };
}

} // NYql
