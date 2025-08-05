#include "yql_co_extr_members.h"
#include "yql_flatmap_over_join.h"
#include "yql_co.h"

#include <yql/essentials/core/yql_expr_csee.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_opt_utils.h>

#include <yql/essentials/utils/log/log.h>

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
            auto newParent = FlatMapOverEquiJoin(flatMap, ctx, *optCtx.ParentsMap, true, optCtx.Types);
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

bool IsFilterMultiusageEnabled(const TOptimizeContext& optCtx) {
    YQL_ENSURE(optCtx.Types);
    static const TString multiUsageFlags = to_lower(TString("FilterPushdownEnableMultiusage"));
    static const TString noMultiUsageFlags = to_lower(TString("FilterPushdownDisableMultiusage"));
    return optCtx.Types->OptimizerFlags.contains(multiUsageFlags) && !optCtx.Types->OptimizerFlags.contains(noMultiUsageFlags);
}

void FilterPushdownWithMultiusage(const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) {
    if (!node->IsCallable() || !IsFilterMultiusageEnabled(optCtx) || !optCtx.HasParent(*node) || optCtx.IsSingleUsage(*node)) {
        return;
    }

    if (node->GetTypeAnn()->GetKind() != ETypeAnnotationKind::List ||
        node->GetTypeAnn()->Cast<TListExprType>()->GetItemType()->GetKind() != ETypeAnnotationKind::Struct)
    {
        return;
    }

    static const THashSet<TStringBuf> skipNodes = {"ExtractMembers", "Unordered", "AssumeColumnOrder"};

    TVector<const TExprNode*> immediateParents;
    YQL_ENSURE(optCtx.ParentsMap);
    auto immediate = optCtx.ParentsMap->find(node.Get());
    if (immediate != optCtx.ParentsMap->end()) {
        immediateParents.assign(immediate->second.begin(), immediate->second.end());
        // normalize parent order
        Sort(immediateParents, [](const TExprNode* left, const TExprNode* right) { return CompareNodes(*left, *right) < 0; });
    }

    struct TConsumerInfo {
        const TExprNode* OriginalFlatMap = nullptr;
        const TTypeAnnotationNode* OriginalRowType = nullptr;
        TExprNode::TPtr FilterLambda;
        TExprNode::TPtr ValueLambda;
        TExprNode::TPtr PushdownLambda;
        TString ColumnName;
    };

    TVector<TConsumerInfo> consumers;
    bool hasOrdered = false;
    size_t pushdownCount = 0;
    const auto inputStructType = node->GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    const auto genColumnNames = GenNoClashColumns(*inputStructType, "_yql_filter_pushdown", immediateParents.size());
    for (size_t i = 0; i < immediateParents.size(); ++i) {
        const TExprNode* parent = immediateParents[i];
        while (skipNodes.contains(parent->Content())) {
            auto newParent = optCtx.GetParentIfSingle(*parent);
            if (newParent) {
                parent = newParent;
            } else {
                break;
            }
        }
        if (!TCoFlatMapBase::Match(parent)) {
            return;
        }

        if (TCoOrderedFlatMap::Match(parent)) {
            hasOrdered = true;
        }

        TCoFlatMapBase parentFlatMap(parent);
        if (auto cond = parentFlatMap.Lambda().Body().Maybe<TCoConditionalValueBase>()) {
            const TCoArgument lambdaArg = parentFlatMap.Lambda().Args().Arg(0);
            auto pred = cond.Cast().Predicate();
            if (pred.Maybe<TCoNoPushBase>() ||
                (pred.Maybe<TCoAnd>() && AnyOf(pred.Ref().ChildrenList(), [](const auto& p) { return IsNoPush(*p); })) ||
                !IsStrict(pred.Ptr()) ||
                HasDependsOn(pred.Ptr(), lambdaArg.Ptr()) ||
                IsDepended(parentFlatMap.Lambda().Ref(), *node))
            {
                return;
            }

            TExprNodeList andPredicates;
            if (pred.Maybe<TCoAnd>()) {
                andPredicates = pred.Ref().ChildrenList();
            } else {
                andPredicates.push_back(pred.Ptr());
            }

            TExprNodeList pushdownPreds;
            TExprNodeList restPreds;
            for (auto& p : andPredicates) {
                if (TCoMember::Match(p.Get()) && p->Child(0) == lambdaArg.Raw()) {
                    restPreds.push_back(p);
                } else {
                    pushdownPreds.push_back(p);
                }
            }

            const TPositionHandle pos = pred.Pos();
            consumers.emplace_back();
            TConsumerInfo& consumer = consumers.back();
            consumer.OriginalFlatMap = parent;
            consumer.OriginalRowType = lambdaArg.Ref().GetTypeAnn();
            consumer.ColumnName = genColumnNames[i];
            if (!pushdownPreds.empty()) {
                ++pushdownCount;
                restPreds.push_back(
                    ctx.Builder(pos)
                        .Callable("Member")
                            .Add(0, lambdaArg.Ptr())
                            .Atom(1, consumer.ColumnName)
                        .Seal()
                        .Build());
                auto restPred = ctx.NewCallable(pos, "And", std::move(restPreds));
                auto pushdownPred = ctx.NewCallable(pos, "And", std::move(pushdownPreds));

                consumer.FilterLambda = ctx.NewLambda(pos, ctx.NewArguments(pos, { lambdaArg.Ptr() }), std::move(restPred));
                consumer.PushdownLambda = ctx.NewLambda(pos, ctx.NewArguments(pos, { lambdaArg.Ptr() }), std::move(pushdownPred));
            } else {
                consumer.FilterLambda = ctx.NewLambda(pos, ctx.NewArguments(pos, { lambdaArg.Ptr() }), pred.Ptr());
            }
            consumer.ValueLambda = ctx.NewLambda(pos, ctx.NewArguments(pos, { lambdaArg.Ptr() }), cond.Cast().Value().Ptr());
        } else {
            return;
        }
    }

    if (!pushdownCount) {
        return;
    }

    YQL_CLOG(DEBUG, Core) << "Pushdown predicate from " << pushdownCount << " filters (out of total " << consumers.size() << ") to common parent " << node->Content();

    YQL_ENSURE(consumers.size() > 1);
    YQL_ENSURE(consumers.size() == immediateParents.size());

    TExprNode::TPtr mapArg = ctx.NewArgument(node->Pos(), "row");
    TExprNode::TPtr mapBody = mapArg;
    TExprNode::TPtr filterArg = ctx.NewArgument(node->Pos(), "row");
    TExprNodeList filterPreds;
    for (size_t i = 0; i < consumers.size(); ++i) {
        const TConsumerInfo& consumer = consumers[i];
        if (consumer.PushdownLambda) {
            mapBody = ctx.Builder(mapBody->Pos())
                .Callable("AddMember")
                    .Add(0, mapBody)
                    .Atom(1, consumer.ColumnName)
                    .Apply(2, consumer.PushdownLambda)
                        .With(0)
                            .Callable("CastStruct")
                                .Add(0, mapArg)
                                .Add(1, ExpandType(mapArg->Pos(), *consumer.OriginalRowType, ctx))
                            .Seal()
                        .Done()
                    .Seal()
                .Seal()
                .Build();
        }

        filterPreds.push_back(ctx.Builder(node->Pos())
            .Apply(consumer.FilterLambda)
                // CastStruct is not needed here, since FilterLambda is AND over column references
                .With(0, filterArg)
            .Seal()
            .Build());
    }

    auto newNode = ctx.Builder(node->Pos())
        .Callable(hasOrdered ? "OrderedFilter" : "Filter")
            .Callable(0, hasOrdered ? "OrderedMap" : "Map")
                .Add(0, node)
                .Add(1, ctx.NewLambda(node->Pos(), ctx.NewArguments(node->Pos(), { mapArg }), std::move(mapBody)))
            .Seal()
            .Add(1, ctx.NewLambda(node->Pos(), ctx.NewArguments(node->Pos(), { filterArg }), ctx.NewCallable(node->Pos(), "Or", std::move(filterPreds))))
        .Seal()
        .Build();

    for (size_t i = 0; i < immediateParents.size(); ++i) {
        const TExprNode* curr = immediateParents[i];
        TExprNode::TPtr resultNode = newNode;
        const TConsumerInfo& consumer = consumers[i];
        while (curr != consumer.OriginalFlatMap) {
            if (curr->IsCallable("AssumeColumnOrder")) {
                resultNode = ctx.ChangeChild(*ctx.RenameNode(*curr, "AssumeColumnOrderPartial"), 0, std::move(resultNode));
            } else if (curr->IsCallable("ExtractMembers")) {
                TExprNodeList columns = curr->Child(1)->ChildrenList();
                if (consumer.PushdownLambda) {
                    columns.push_back(ctx.NewAtom(curr->Child(1)->Pos(), consumer.ColumnName));
                }
                resultNode = ctx.ChangeChildren(*curr, { resultNode, ctx.NewList(curr->Child(1)->Pos(), std::move(columns)) });
            } else {
                resultNode = ctx.ChangeChild(*curr, 0, std::move(resultNode));
            }
            curr = optCtx.GetParentIfSingle(*curr);
            YQL_ENSURE(curr);
        }

        TCoFlatMapBase flatMap(curr);
        TCoConditionalValueBase cond = flatMap.Lambda().Body().Cast<TCoConditionalValueBase>();
        TExprNode::TPtr input = flatMap.Input().Ptr();
        toOptimize[consumer.OriginalFlatMap] = ctx.Builder(curr->Pos())
            .Callable(flatMap.CallableName())
                .Add(0, resultNode)
                .Lambda(1)
                    .Param("row")
                    .Callable(cond.CallableName())
                        .Apply(0, consumer.FilterLambda)
                            .With(0, "row")
                        .Seal()
                        .Apply(1, consumer.ValueLambda)
                            .With(0)
                                .Callable("CastStruct")
                                    .Arg(0, "row")
                                    .Add(1, ExpandType(curr->Pos(), *consumer.OriginalRowType, ctx))
                                .Seal()
                            .Done()
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
            .Build();
    }
}

bool AllConsumersAreUnordered(const TExprNode::TPtr& node, const TParentsMap& parents, TNodeSet& unorderedConsumers) {
    static const THashSet<TStringBuf> traverseCallables = {
        TCoExtractMembers::CallableName(),
        TCoAssumeDistinct::CallableName(),
        TCoAssumeUnique::CallableName(),
        TCoAssumeColumnOrder::CallableName(),
    };
    TNodeSet current{node.Get()};
    while (!current.empty()) {
        TNodeSet next;
        for (auto curr : current) {
            auto it = parents.find(curr);
            if (it == parents.cend() || it->second.empty()) {
                return false;
            }
            for (auto parent : it->second) {
                if (TCoUnordered::Match(parent)) {
                    // TODO: should probably add more callables here
                    unorderedConsumers.insert(parent);
                    continue;
                }
                if (!parent->IsCallable(traverseCallables)) {
                    return false;
                }
                YQL_ENSURE(&parent->Head() == curr);
                next.insert(parent);
            }
        }
        current = std::move(next);
    }
    return true;
}

bool AllConsumersAreMembers(const TExprNode::TPtr& node, const TParentsMap& parents, TNodeSet& memberConsumers) {
    auto it = parents.find(node.Get());
    if (it == parents.cend() || it->second.empty()) {
        return false;
    }

    for (auto parent : it->second) {
        if (!TCoMember::Match(parent)) {
            return false;
        }
        memberConsumers.insert(parent);
    }

    return true;
}

bool OptimizeForUnorderedConsumers(const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) {
    static const char optName[] = "UnorderedOverSortImproved";
    YQL_ENSURE(optCtx.Types);
    const bool optEnabled = !IsOptimizerDisabled<optName>(*optCtx.Types);
    if (!optEnabled) {
        return false;
    }

    if (!node->IsCallable({"Sort", "AssumeSorted", "TopSort"})) {
        return false;
    }

    YQL_ENSURE(optCtx.ParentsMap);
    const auto& parentsMap = *optCtx.ParentsMap;
    TNodeSet unorderedConsumers;
    if (!AllConsumersAreUnordered(node, parentsMap, unorderedConsumers)) {
        return false;
    }

    YQL_ENSURE(!unorderedConsumers.empty());
    const TExprNode::TPtr newNode = node->IsCallable("TopSort") ? ctx.RenameNode(*node, "Top") : node->HeadPtr();
    for (auto consumer : unorderedConsumers) {
        toOptimize[consumer] = ctx.ReplaceNode(ctx.ShallowCopy(*consumer), *node, newNode);
    }

    YQL_CLOG(DEBUG, Core) << "Droping sort due to unordered consumers for " << node->Content();
    return true;
}

bool IsFieldSubsetForOptionalsEnabled(const TOptimizeContext& optCtx) {
    YQL_ENSURE(optCtx.Types);
    static const char optName[] = "MemberNthOverFlatMap";
    return !IsOptimizerDisabled<optName>(*optCtx.Types);
}

void OptimizeForMemberConsumers(const TCoFlatMapBase& self, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) {
    YQL_ENSURE(optCtx.Types);
    static const char optName[] = "MemberNthOverFlatMap";
    if (IsOptimizerDisabled<optName>(*optCtx.Types)) {
        return;
    }

    auto maybeAsStruct = self.Lambda().Body().Maybe<TCoJust>().Input().Maybe<TCoAsStruct>();
    if (!maybeAsStruct) {
        return;
    }

    YQL_ENSURE(optCtx.ParentsMap);
    const auto& parentsMap = *optCtx.ParentsMap;
    TNodeSet memberConsumers;
    if (!AllConsumersAreMembers(self.Ptr(), parentsMap, memberConsumers)) {
        return;
    }

    TMap<TStringBuf, TExprNode::TPtr> structItems;
    for (auto item : maybeAsStruct.Cast().Ref().ChildrenList()) {
        TCoNameValueTuple tuple(item);
        structItems[tuple.Name().Value()] = tuple.Value().Cast().Ptr();
    }

    TNodeSet restMembers;
    size_t separableMembersCount = 0;
    for (auto memberNode : memberConsumers) {
        TCoMember member(memberNode);
        auto name = member.Name().Value();
        auto it = structItems.find(name);
        YQL_ENSURE(it != structItems.end());

        TExprNode::TPtr entry = it->second;
        if (IsDepended(*entry, self.Lambda().Args().Arg(0).Ref())) {
            restMembers.insert(memberNode);
        } else {
            ++separableMembersCount;
            toOptimize[memberNode] = ctx.Builder(memberNode->Pos())
                .Callable(self.CallableName())
                    .Add(0, self.Input().Ptr())
                    .Lambda(1)
                        .Param("unused")
                        .Add(0, ctx.WrapByCallableIf(!entry->GetTypeAnn()->IsOptionalOrNull(), "Just", std::move(entry)))
                    .Seal()
                .Seal()
                .Build();
            structItems.erase(it);
        }
    }

    if (separableMembersCount && !restMembers.empty()) {
        auto restBody = ctx.Builder(self.Lambda().Body().Pos())
            .Callable("Just")
                .Callable(0, "AsStruct")
                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                        ui32 i = 0U;
                        for (const auto& [name, value] : structItems) {
                            parent.List(i)
                                .Atom(0, name)
                                .Add(1, value)
                            .Seal();
                            ++i;
                        }
                        return parent;
                    })
                .Seal()
            .Seal()
            .Build();
        auto restFlatMap = ctx.ChangeChild(self.Ref(), TCoFlatMapBase::idx_Lambda,
            ctx.DeepCopyLambda(*ctx.ChangeChild(self.Lambda().Ref(), TCoLambda::idx_Body, std::move(restBody))));
        for (auto restMember : restMembers) {
            toOptimize[restMember] = ctx.ChangeChild(*restMember, TCoMember::idx_Struct, TExprNode::TPtr(restFlatMap));
        }
    }

    if (separableMembersCount) {
        YQL_CLOG(DEBUG, Core) << separableMembersCount << " separable Members and " << restMembers.size() << " non separable over " << self.CallableName();
    }
}

}

void RegisterCoFinalizers(TFinalizingOptimizerMap& map) {
    map[TCoExtend::CallableName()] = map[TCoOrderedExtend::CallableName()] = map[TCoMerge::CallableName()] = [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) {
        OptimizeSubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx,
            [] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap&, TExprContext& ctx) {
                return ApplyExtractMembersToExtend(input, members, ctx, " with multi-usage");
            },
            IsFieldSubsetForOptionalsEnabled(optCtx)
        );

        return true;
    };

    map[TCoTake::CallableName()] = [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) {
        OptimizeSubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx,
            [] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap&, TExprContext& ctx) {
                return ApplyExtractMembersToTake(input, members, ctx, " with multi-usage");
            },
            IsFieldSubsetForOptionalsEnabled(optCtx)
        );

        return true;
    };

    map[TCoSkip::CallableName()] = [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) {
        OptimizeSubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx,
            [] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap&, TExprContext& ctx) {
                return ApplyExtractMembersToSkip(input, members, ctx, " with multi-usage");
            },
            IsFieldSubsetForOptionalsEnabled(optCtx)
        );

        return true;
    };

    map[TCoFilterNullMembers::CallableName()] =
    map[TCoSkipNullMembers::CallableName()] =
    [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) {
        OptimizeSubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx,
            [&] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap&, TExprContext& ctx) {
                return ApplyExtractMembersToFilterSkipNullMembers(input, members, ctx, optCtx, " with multi-usage");
            },
            IsFieldSubsetForOptionalsEnabled(optCtx)
        );

        return true;
    };

    map[TCoFlatMap::CallableName()] = map[TCoOrderedFlatMap::CallableName()] = [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) {
        OptimizeSubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx,
            [] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap&, TExprContext& ctx) {
                return ApplyExtractMembersToFlatMap(input, members, ctx, " with multi-usage");
            },
            IsFieldSubsetForOptionalsEnabled(optCtx)
        );

        if (toOptimize.empty() && TCoFlatMapBase::Match(node.Get())) {
            OptimizeForMemberConsumers(TCoFlatMapBase(node), toOptimize, ctx, optCtx);
        }

        return true;
    };

    map[TCoSort::CallableName()] = map[TCoAssumeSorted::CallableName()] = [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) {
        if (OptimizeForUnorderedConsumers(node, toOptimize, ctx, optCtx)) {
            return true;
        }
        OptimizeSubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx,
            [] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap& parentsMap, TExprContext& ctx) {
                return ApplyExtractMembersToSortOrPruneKeys(input, members, parentsMap, ctx, " with multi-usage");
            },
            IsFieldSubsetForOptionalsEnabled(optCtx)
        );

        return true;
    };

    map[TCoPruneKeys::CallableName()] = map[TCoPruneAdjacentKeys::CallableName()] = [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) {
        OptimizeSubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx,
            [] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap& parentsMap, TExprContext& ctx) {
                return ApplyExtractMembersToSortOrPruneKeys(input, members, parentsMap, ctx, " with multi-usage");
            },
            IsFieldSubsetForOptionalsEnabled(optCtx)
        );

        return true;
    };

    map[TCoAssumeUnique::CallableName()] = map[TCoAssumeDistinct::CallableName()] =  [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) {
        OptimizeSubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx,
            [] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap&, TExprContext& ctx) {
                return ApplyExtractMembersToAssumeUnique(input, members, ctx, " with multi-usage");
            },
            IsFieldSubsetForOptionalsEnabled(optCtx)
        );

        return true;
    };

    map[TCoTop::CallableName()] = map[TCoTopSort::CallableName()] = [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) {
        if (OptimizeForUnorderedConsumers(node, toOptimize, ctx, optCtx)) {
            return true;
        }
        OptimizeSubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx,
            [] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap& parentsMap, TExprContext& ctx) {
                return ApplyExtractMembersToTop(input, members, parentsMap, ctx, " with multi-usage");
            },
            IsFieldSubsetForOptionalsEnabled(optCtx)
        );

        return true;
    };

    map[TCoEquiJoin::CallableName()] = [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) {
        OptimizeSubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx,
            [](const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap&, TExprContext& ctx) {
                return ApplyExtractMembersToEquiJoin(input, members, ctx, " with multi-usage");
            },
            IsFieldSubsetForOptionalsEnabled(optCtx)
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
            },
            IsFieldSubsetForOptionalsEnabled(optCtx)
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
            },
            IsFieldSubsetForOptionalsEnabled(optCtx)
        );

        return true;
    };

    map[TCoAggregate::CallableName()] = [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) {
        OptimizeSubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx,
            [] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap& parentsMap, TExprContext& ctx) {
                return ApplyExtractMembersToAggregate(input, members, parentsMap, ctx, " with multi-usage");
            },
            IsFieldSubsetForOptionalsEnabled(optCtx)
        );

        return true;
    };

    map[TCoChopper::CallableName()] = [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) {
        OptimizeSubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx,
            [] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap&, TExprContext& ctx) {
                return ApplyExtractMembersToChopper(input, members, ctx, " with multi-usage");
            },
            IsFieldSubsetForOptionalsEnabled(optCtx)
        );

        return true;
    };

    map[TCoCollect::CallableName()] = [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) {
        OptimizeSubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx,
            [] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap&, TExprContext& ctx) {
                return ApplyExtractMembersToCollect(input, members, ctx, " with multi-usage");
            },
            IsFieldSubsetForOptionalsEnabled(optCtx)
        );

        return true;
    };

    map[TCoMapNext::CallableName()] = [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) {
        OptimizeSubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx,
            [] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap&, TExprContext& ctx) {
                return ApplyExtractMembersToMapNext(input, members, ctx, " with multi-usage");
            },
            IsFieldSubsetForOptionalsEnabled(optCtx)
        );

        return true;
    };

    map[TCoChain1Map::CallableName()] = [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) {
        OptimizeSubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx,
            [] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap& parentsMap, TExprContext& ctx) {
                return ApplyExtractMembersToChain1Map(input, members, parentsMap, ctx, " with multi-usage");
            },
            IsFieldSubsetForOptionalsEnabled(optCtx)
        );

        return true;
    };

    map[TCoCondense1::CallableName()] = [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) {
        OptimizeSubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx,
            [] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap& parentsMap, TExprContext& ctx) {
                return ApplyExtractMembersToCondense1(input, members, parentsMap, ctx, " with multi-usage");
            },
            IsFieldSubsetForOptionalsEnabled(optCtx)
        );

        return true;
    };

    map[TCoCombineCore::CallableName()] = [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) {
        OptimizeSubsetFieldsForNodeWithMultiUsage(node, *optCtx.ParentsMap, toOptimize, ctx,
            [] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap&, TExprContext& ctx) {
                return ApplyExtractMembersToCombineCore(input, members, ctx, " with multi-usage");
            },
            IsFieldSubsetForOptionalsEnabled(optCtx)
        );

        return true;
    };

    map[""] = [](const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx, TOptimizeContext& optCtx) {
        FilterPushdownWithMultiusage(node, toOptimize, ctx, optCtx);
        return true;
    };
}

} // NYql
