#include "yql_co_extr_members.h" 
#include "yql_co.h" 
 
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
 
#include <ydb/library/yql/utils/log/log.h>
 
namespace NYql { 
 
namespace { 
 
using namespace NNodes; 
 
void SubsetFieldsForNodeWithMultiUsage(const TExprNode::TPtr& node, const TParentsMap& parentsMap, 
    TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, 
    std::function<TExprNode::TPtr(const TExprNode::TPtr&, const TExprNode::TPtr&, const TParentsMap&, TExprContext&)> handler) 
{ 
    // Ignore stream input, because it cannot be used multiple times 
    if (node->GetTypeAnn()->GetKind() != ETypeAnnotationKind::List) { 
        return; 
    } 
    auto itemType = node->GetTypeAnn()->Cast<TListExprType>()->GetItemType(); 
    if (itemType->GetKind() != ETypeAnnotationKind::Struct) { 
        return; 
    } 
    auto structType = itemType->Cast<TStructExprType>(); 
 
    auto it = parentsMap.find(node.Get()); 
    if (it == parentsMap.cend() || it->second.size() <= 1) { 
        return; 
    } 
 
    TSet<TStringBuf> usedFields; 
    for (auto parent: it->second) { 
        if (auto maybeFlatMap = TMaybeNode<TCoFlatMapBase>(parent)) { 
            auto flatMap = maybeFlatMap.Cast(); 
            TSet<TStringBuf> lambdaSubset; 
            if (!HaveFieldsSubset(flatMap.Lambda().Body().Ptr(), flatMap.Lambda().Args().Arg(0).Ref(), lambdaSubset, parentsMap)) { 
                return; 
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
            return; 
        } 
        if (usedFields.size() == structType->GetSize()) { 
            return; 
        } 
    } 
 
    TExprNode::TListType members; 
    for (auto column : usedFields) { 
        members.push_back(ctx.NewAtom(node->Pos(), column)); 
    } 
 
    auto newInput = handler(node, ctx.NewList(node->Pos(), std::move(members)), parentsMap, ctx); 
    if (!newInput || newInput == node) { 
        return; 
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
} 
 
} 
 
void RegisterCoFinalizers(TFinalizingOptimizerMap& map) { 
    map[TCoExtend::CallableName()] = map[TCoOrderedExtend::CallableName()] = map[TCoMerge::CallableName()] = [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) { 
        SubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx, 
            [] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap&, TExprContext& ctx) { 
                return ApplyExtractMembersToExtend(input, members, ctx, " with multi-usage"); 
            } 
        ); 
    }; 
 
    map[TCoTake::CallableName()] = [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) { 
        SubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx, 
            [] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap&, TExprContext& ctx) { 
                return ApplyExtractMembersToTake(input, members, ctx, " with multi-usage"); 
            } 
        ); 
    }; 
 
    map[TCoSkip::CallableName()] = [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) { 
        SubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx, 
            [] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap&, TExprContext& ctx) { 
                return ApplyExtractMembersToSkip(input, members, ctx, " with multi-usage"); 
            } 
        ); 
    }; 
 
    map[TCoSkipNullMembers::CallableName()] = [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) { 
        SubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx, 
            [] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap&, TExprContext& ctx) { 
                return ApplyExtractMembersToSkipNullMembers(input, members, ctx, " with multi-usage"); 
            } 
        ); 
    }; 
 
    map[TCoFlatMap::CallableName()] = map[TCoOrderedFlatMap::CallableName()] = [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) { 
        SubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx, 
            [] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap&, TExprContext& ctx) { 
                return ApplyExtractMembersToFlatMap(input, members, ctx, " with multi-usage"); 
            } 
        ); 
    }; 
 
    map[TCoSort::CallableName()] = map[TCoAssumeSorted::CallableName()] = [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) { 
        SubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx, 
            [] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap& parentsMap, TExprContext& ctx) { 
                return ApplyExtractMembersToSort(input, members, parentsMap, ctx, " with multi-usage"); 
            } 
        ); 
    }; 
 
    map[TCoAssumeUnique::CallableName()] = [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) { 
        SubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx, 
            [] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap&, TExprContext& ctx) { 
                return ApplyExtractMembersToAssumeUnique(input, members, ctx, " with multi-usage"); 
            } 
        ); 
    }; 
 
    map[TCoTop::CallableName()] = map[TCoTopSort::CallableName()] = [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) { 
        SubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx, 
            [] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap& parentsMap, TExprContext& ctx) { 
                return ApplyExtractMembersToTop(input, members, parentsMap, ctx, " with multi-usage"); 
            } 
        ); 
    }; 
 
    map[TCoEquiJoin::CallableName()] = [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) { 
        SubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx, 
            [] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap&, TExprContext& ctx) { 
                return ApplyExtractMembersToEquiJoin(input, members, ctx, " with multi-usage"); 
            } 
        ); 
    }; 
 
    map[TCoPartitionByKey::CallableName()] = [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) { 
        SubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx, 
            [] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap&, TExprContext& ctx) { 
                return ApplyExtractMembersToPartitionByKey(input, members, ctx, " with multi-usage"); 
            } 
        ); 
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
    }; 
 
    map[TCoAggregate::CallableName()] = [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) { 
        SubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx, 
            [] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap& parentsMap, TExprContext& ctx) { 
                return ApplyExtractMembersToAggregate(input, members, parentsMap, ctx, " with multi-usage"); 
            } 
        ); 
    }; 

    map[TCoChopper::CallableName()] = [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) {
        SubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx,
            [] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap&, TExprContext& ctx) {
                return ApplyExtractMembersToChopper(input, members, ctx, " with multi-usage");
            }
        );
    };

    map[TCoCollect::CallableName()] = [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) {
        SubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx,
            [] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap&, TExprContext& ctx) {
                return ApplyExtractMembersToCollect(input, members, ctx, " with multi-usage");
            }
        );
    };
} 
 
} // NYql 
