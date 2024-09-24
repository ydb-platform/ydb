#include "yql_co.h"
#include "yql_co_pgselect.h"

#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/core/yql_expr_csee.h>

#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <library/cpp/disjoint_sets/disjoint_sets.h>

namespace NYql {

namespace {

using namespace NNodes;

bool HasTotalOrder(const TTypeAnnotationNode& type) {
    if (type.GetKind() == ETypeAnnotationKind::Optional || type.GetKind() == ETypeAnnotationKind::Pg) {
        return false; // may be null
    }

    if (type.GetKind() == ETypeAnnotationKind::Data) {
        auto dataSlot = type.Cast<TDataExprType>()->GetSlot();
        return dataSlot != EDataSlot::Float && dataSlot != EDataSlot::Double && dataSlot != EDataSlot::Decimal;
    }

    if (type.GetKind() == ETypeAnnotationKind::Struct) {
        for (const auto& x : type.Cast<TStructExprType>()->GetItems()) {
            if (!HasTotalOrder(*x->GetItemType())) {
                return false;
            }
        }

        return true;
    }

    if (type.GetKind() == ETypeAnnotationKind::Tuple) {
        for (const auto& x : type.Cast<TTupleExprType>()->GetItems()) {
            if (!HasTotalOrder(*x)) {
                return false;
            }
        }

        return true;
    }

    if (type.GetKind() == ETypeAnnotationKind::List) {
        auto listType = type.Cast<TListExprType>();
        return HasTotalOrder(*listType->GetItemType());
    }

    if (type.GetKind() == ETypeAnnotationKind::Dict) {
        auto dictType = type.Cast<TDictExprType>();
        return HasTotalOrder(*dictType->GetKeyType())
            && HasTotalOrder(*dictType->GetPayloadType());
    }

    if (type.GetKind() == ETypeAnnotationKind::Void) {
        return true;
    }

    if (type.GetKind() == ETypeAnnotationKind::EmptyList) {
        return true;
    }

    if (type.GetKind() == ETypeAnnotationKind::EmptyDict) {
        return true;
    }

    if (type.GetKind() == ETypeAnnotationKind::Null) {
        return false;
    }

    if (type.GetKind() == ETypeAnnotationKind::Variant) {
        auto variantType = type.Cast<TVariantExprType>();
        return HasTotalOrder(*variantType->GetUnderlyingType());
    }

    if (type.GetKind() == ETypeAnnotationKind::Tagged) {
        auto taggedType = type.Cast<TTaggedExprType>();
        return HasTotalOrder(*taggedType->GetBaseType());
    }

    YQL_ENSURE(false, "Unordered type: " << type);
}

TExprNode::TPtr DeduplicateAggregateSameTraits(const TExprNode::TPtr& node, TExprContext& ctx) {
    const TCoAggregate self(node);
    if (self.Handlers().Size() == 0) {
        return node;
    }

    // keep index of main handler or Max if this handler is the main one
    std::vector<ui32> handlersMapping(self.Handlers().Size(), Max<ui32>());
    TNodeMap<ui32> nonDistinctHandlers; // map trait->handler index
    THashMap<std::pair<TStringBuf, const TExprNode*>, ui32> distinctHandlers; // map column name+trait->handler index
    ui32 duplicatesCount = 0;
    for (ui32 index = 0; index < self.Handlers().Size(); ++index) {
        auto& handler = self.Handlers().Item(index).Ref();
        auto nameNode = handler.Child(0);
        if (nameNode->IsList()) {
            // skip multioutput nodes
            continue;
        }

        const bool isDistinct = (handler.ChildrenSize() == 3);
        if (isDistinct) {
            auto distinctColumn = handler.Child(2)->Content();
            auto x = distinctHandlers.insert({ { distinctColumn, handler.Child(1) }, index });
            if (!x.second) {
                ++duplicatesCount;
                handlersMapping[index] = x.first->second;
            }
        } else {
            auto x = nonDistinctHandlers.insert({ handler.Child(1), index });
            if (!x.second) {
                ++duplicatesCount;
                handlersMapping[index] = x.first->second;
            }
        }
    }

    if (!duplicatesCount) {
        return node;
    }

    TExprNode::TListType filteredHandlers;
    filteredHandlers.reserve(handlersMapping.size() - duplicatesCount);
    for (ui32 index = 0; index < handlersMapping.size(); ++index) {
        if (handlersMapping[index] == Max<ui32>()) {
            filteredHandlers.push_back(self.Handlers().Item(index).Ptr());
        }
    }

    auto dedupedAggregate = Build<TCoAggregate>(ctx, self.Pos())
        .Input(self.Input())
        .Keys(self.Keys())
        .Handlers(ctx.NewList(self.Pos(), std::move(filteredHandlers)))
        .Settings(self.Settings())
        .Build()
        .Value();

    return ctx.Builder(self.Pos())
        .Callable("Map")
            .Add(0, dedupedAggregate.Ptr())
            .Lambda(1)
                .Param("row")
                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                    auto structObj = parent.Callable("AsStruct");
                    ui32 targetIndex = 0;
                    for (ui32 index = 0; index < self.Keys().Size(); ++index) {
                        auto keyAtom = self.Keys().Item(index).Ptr();
                        structObj
                            .List(targetIndex++)
                                .Add(0, keyAtom)
                                .Callable(1, "Member")
                                    .Arg(0, "row")
                                    .Add(1, keyAtom)
                                .Seal()
                            .Seal();
                    }

                    for (ui32 index = 0; index < handlersMapping.size(); ++index) {
                        const auto& columnNode = self.Handlers().Item(index).Ref().Child(0);
                        if (columnNode->IsAtom()) {
                            const auto& myColumn = columnNode->Content();
                            const auto& originalColumn = self.Handlers().Item(handlersMapping[index] == Max<ui32>() ?
                                index : handlersMapping[index]).Ref().Child(0)->Content();
                            structObj
                                .List(targetIndex++)
                                    .Atom(0, myColumn)
                                    .Callable(1, "Member")
                                        .Arg(0, "row")
                                        .Atom(1, originalColumn)
                                    .Seal()
                                .Seal();
                        } else {
                            for (auto childAtom : columnNode->Children()) {
                                structObj
                                    .List(targetIndex++)
                                        .Add(0, childAtom)
                                        .Callable(1, "Member")
                                            .Arg(0, "row")
                                            .Add(1, childAtom)
                                        .Seal()
                                    .Seal();
                            }
                        }
                    }

                    auto settings = self.Settings();
                    auto hoppingSetting = GetSetting(settings.Ref(), "hopping");
                    if (hoppingSetting) {
                        structObj
                            .List(targetIndex++)
                                .Atom(0, "_yql_time", TNodeFlags::Default)
                                .Callable(1, "Member")
                                    .Arg(0, "row")
                                    .Atom(1, "_yql_time", TNodeFlags::Default)
                                .Seal()
                            .Seal();
                    }

                    return structObj.Seal();
                })
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr MergeAggregateTraits(const TExprNode::TPtr& node, TExprContext& ctx) {
    const TCoAggregate self(node);
    using TMergeKey = std::pair<TExprNodeList, TStringBuf>; // all TCoAggregationTraits args (except finish), distinct column name
    struct TCompareMergeKey {
        bool operator()(const TMergeKey& a, const TMergeKey& b) const {
            size_t len = std::min(a.first.size(), b.first.size());
            for (size_t i = 0; i < len; ++i) {
                if (a.first[i] != b.first[i]) {
                    return a.first[i].Get() < b.first[i].Get();
                }
            }
            if (a.first.size() != b.first.size()) {
                return a.first.size() < b.first.size();
            }
            return a.second < b.second;
        }
    };
    TExprNodeList resultAggTuples;
    TMap<TMergeKey, TVector<TCoAggregateTuple>, TCompareMergeKey> tuplesByKey;
    TVector<TMergeKey> mergeKeys;
    for (const auto& aggTuple : self.Handlers()) {
        auto maybeAggTraits = aggTuple.Trait().Maybe<TCoAggregationTraits>();
        if (!maybeAggTraits || !maybeAggTraits.Cast().FinishHandler().Ref().IsComplete() || !aggTuple.ColumnName().Ref().IsAtom()) {
            resultAggTuples.emplace_back(aggTuple.Ptr());
            continue;
        }

        TExprNodeList aggTraits = maybeAggTraits.Cast().Ref().ChildrenList();
        YQL_ENSURE(aggTraits.size() > TCoAggregationTraits::idx_FinishHandler);
        aggTraits.erase(aggTraits.begin() + TCoAggregationTraits::idx_FinishHandler);

        TStringBuf distinctKey;
        if (aggTuple.DistinctName()) {
            distinctKey = aggTuple.DistinctName().Cast().Value();
        }

        TMergeKey key(std::move(aggTraits), distinctKey);
        auto& tuples = tuplesByKey[key];
        if (tuples.empty()) {
            mergeKeys.push_back(key);
        }
        tuples.push_back(aggTuple);
    }

    bool merged = false;
    for (auto& key : mergeKeys) {
        auto it = tuplesByKey.find(key);
        YQL_ENSURE(it != tuplesByKey.end());
        auto& tuples = it->second;
        if (tuples.size() == 1) {
            resultAggTuples.push_back(tuples.front().Ptr());
            continue;
        }
        merged = true;
        YQL_ENSURE(!tuples.empty());
        auto arg = ctx.NewArgument(tuples.front().Pos(), "arg");
        TExprNodeList bodyItems;
        TExprNodeList columnNames;

        for (auto& tuple : tuples) {
            bodyItems.push_back(
                ctx.Builder(tuple.Trait().Cast<TCoAggregationTraits>().FinishHandler().Pos())
                    .Apply(tuple.Trait().Cast<TCoAggregationTraits>().FinishHandler().Ref())
                        .With(0, arg)
                    .Seal()
                    .Build()
            );
            columnNames.push_back(tuple.ColumnName().Cast<TCoAtom>().Ptr());
        }

        auto newHandler = ctx.NewLambda(arg->Pos(), ctx.NewArguments(arg->Pos(), { arg }), ctx.NewList(arg->Pos(), std::move(bodyItems)));
        auto newTraits = Build<TCoAggregationTraits>(ctx, tuples.front().Pos())
            .InitFrom(tuples.front().Trait().Cast<TCoAggregationTraits>())
            .FinishHandler(newHandler)
            .Done().Ptr();
        auto newTuple = ctx.ChangeChild(tuples.front().Ref(), TCoAggregateTuple::idx_Trait, std::move(newTraits));
        newTuple = ctx.ChangeChild(*newTuple, TCoAggregateTuple::idx_ColumnName, ctx.NewList(tuples.front().Pos(), std::move(columnNames)));
        resultAggTuples.push_back(std::move(newTuple));
    }

    if (!merged) {
        return node;
    }

    return Build<TCoAggregate>(ctx, node->Pos())
        .InitFrom(self)
        .Handlers(ctx.NewList(self.Pos(), std::move(resultAggTuples)))
        .Done()
        .Ptr();
}

TExprNode::TPtr SimplifySync(const TExprNode::TPtr& node, TExprContext& ctx) {
    TExprNode::TListType ordered;
    TNodeOnNodeOwnedMap realWorlds;
    bool flatten = false;
    for (auto child : node->Children()) {
        if (child->IsCallable(SyncName)) {
            flatten = true;
            for (auto& subchild : child->Children()) {
                if (subchild->Type() != TExprNode::World) {
                    if (realWorlds.emplace(subchild.Get(), subchild).second) {
                        ordered.push_back(subchild);
                    }
                }
            }
        } else if (child->Type() != TExprNode::World) {
            if (realWorlds.emplace(child.Get(), child).second) {
                ordered.push_back(child);
            }
        }
    }

    if (realWorlds.size() == 1) {
        YQL_CLOG(DEBUG, Core) << "Simplify " << node->Content();
        return realWorlds.cbegin()->second;
    }

    if (flatten || (realWorlds.size() != node->ChildrenSize())) {
        if (realWorlds.empty()) {
            YQL_CLOG(DEBUG, Core) << node->Content() << " to World";
            return ctx.NewWorld(node->Pos());
        }

        YQL_CLOG(DEBUG, Core) << "Simplify " << node->Content();
        return ctx.NewCallable(node->Pos(), SyncName, std::move(ordered));
    }

    return node;
}

void DropDups(TExprNode::TListType& children) {
    TNodeSet set(children.size());
    for (auto it = children.cbegin(); children.cend() != it;) {
        if (set.emplace(it->Get()).second) {
            ++it;
        } else {
            it = children.erase(it);
        }
    }
}

void StripLikely(TExprNodeList& args, TNodeOnNodeOwnedMap& likelyArgs) {
    for (auto& arg : args) {
        if (arg->IsCallable("Likely")) {
            likelyArgs[arg->Child(0)] = arg;
            arg = arg->HeadPtr();
        }
    }
}

void UnstripLikely(TExprNodeList& args, const TNodeOnNodeOwnedMap& likelyArgs) {
    for (auto& arg : args) {
        if (auto it = likelyArgs.find(arg.Get()); it != likelyArgs.end()) {
            arg = it->second;
        }
    }
}

TExprNode::TPtr OptimizeDups(const TExprNode::TPtr& node, TExprContext& ctx) {
    auto children = node->ChildrenList();
    TNodeOnNodeOwnedMap likelyArgs;
    StripLikely(children, likelyArgs);
    DropDups(children);
    if (children.size() < node->ChildrenSize()) {
        UnstripLikely(children, likelyArgs);
        YQL_CLOG(DEBUG, Core) << node->Content() << " with " << node->ChildrenSize() - children.size() << " dups";
        return 1U == children.size() ? children.front() : ctx.ChangeChildren(*node, std::move(children));
    }

    return node;
}

TExprNode::TPtr DropAggrOverSame(const TExprNode::TPtr& node) {
    if (&node->Head() == &node->Tail()) {
        YQL_CLOG(DEBUG, Core) << "Drop " << node->Content() << " with same args";
        return node->TailPtr();
    }

    return node;
}

TExprNode::TPtr OptimizeXor(const TExprNode::TPtr& node, TExprContext& ctx) {
    auto children = node->ChildrenList();
    TNodeSet set(children.size());
    TNodeMap<TExprNode::TListType::const_iterator> map(children.size());
    for (auto it = children.cbegin(); children.cend() != it;) {
        if (set.emplace(it->Get()).second)
            ++it;
        else if (const auto ins = map.emplace(it->Get(), it); ins.second)
            ++it;
        else {
            children.erase(it);
            children.erase(ins.first->second);
            set.clear();
            map.clear();
            it = children.cbegin();
        }
    }

    if (children.size() < node->ChildrenSize()) {
        YQL_CLOG(DEBUG, Core) << node->Content() << " over some dups";
        return ctx.ChangeChildren(*node, std::move(children));
    }

    return node;
}

TExprNode::TPtr OptimizeNot(const TExprNode::TPtr& node, TExprContext& ctx) {
    static const std::unordered_map<std::string_view, std::string_view> InverseComparators = {
        {"==", "!="},
        {"!=", "=="},
        {"<",  ">="},
        {">=", "<"},
        {">",  "<="},
        {"<=", ">"},
        {"AggrEquals", "AggrNotEquals"},
        {"AggrNotEquals", "AggrEquals"},
        {"AggrLess", "AggrGreaterOrEqual"},
        {"AggrGreaterOrEqual", "AggrLess"},
        {"AggrLessOrEqual", "AggrGreater"},
        {"AggrGreater", "AggrLessOrEqual"},
    };

    auto& arg = node->Head();
    const auto it = InverseComparators.find(arg.Content());
    if (InverseComparators.cend() != it && (
        (arg.Content().front() != '<' && arg.Content().front() != '>') ||
        (HasTotalOrder(*arg.Head().GetTypeAnn()) && HasTotalOrder(*arg.Tail().GetTypeAnn()))))
    {
        YQL_CLOG(DEBUG, Core) << node->Content() << " over " << arg.Content();
        return ctx.RenameNode(arg, it->second);
    }

    return node;
}

TExprNode::TPtr OptimizeExistsAndUnwrap(const TExprNode::TPtr& node, TExprContext& ctx) {
    YQL_ENSURE(node->IsCallable("And"));
    TExprNodeList children = node->ChildrenList();
    TNodeMap<size_t> exists;
    TNodeSet toReplace;
    for (size_t i = 0; i < children.size(); ++i) {
        const auto& child = children[i];
        if (child->IsCallable("Exists")) {
            auto pred = child->Child(0);
            YQL_ENSURE(!exists.contains(pred));
            exists[pred] = i;
        } else if (child->IsCallable("Unwrap")) {
            auto pred = child->Child(0);
            if (exists.contains(pred)) {
                toReplace.insert(pred);
            }
        }
    }

    if (toReplace.empty()) {
        return node;
    }

    TExprNodeList newChildren;
    for (size_t i = 0; i < children.size(); ++i) {
        const auto& child = children[i];
        if (child->IsCallable({"Exists", "Unwrap"})) {
            auto pred = child->HeadPtr();
            auto it = exists.find(pred.Get());
            if (it != exists.end() && toReplace.contains(it->first)) {
                if (i == it->second) {
                    newChildren.push_back(ctx.NewCallable(pred->Pos(), "Coalesce", { pred, MakeBool<false>(pred->Pos(), ctx)}));
                    continue;
                }
                if (i > it->second) {
                    continue;
                }
            }
        }
        newChildren.push_back(child);
    }

    YQL_CLOG(DEBUG, Core) << "Exist(pred) AND Unwrap(pred) -> Coalesce(pred, false)";
    return ctx.ChangeChildren(*node, std::move(newChildren));
}

bool IsExtractCommonPredicatesFromLogicalOpsEnabled(const TOptimizeContext& optCtx) {
    YQL_ENSURE(optCtx.Types);
    static const TString enable = to_lower(TString("ExtractCommonPredicatesFromLogicalOps"));
    static const TString disable = to_lower(TString("DisableExtractCommonPredicatesFromLogicalOps"));
    return optCtx.Types->OptimizerFlags.contains(enable) && !optCtx.Types->OptimizerFlags.contains(disable);
}

size_t GetNodeId(const TExprNode* node, const TNodeMap<size_t>& node2id) {
    auto it = node2id.find(node);
    YQL_ENSURE(it != node2id.end());
    return it->second;
}

TExprNodeList GetOrChildren(const TExprNode::TPtr& node) {
    return node->IsCallable("Or") ? node->ChildrenList() : TExprNodeList{ node };
}

bool AllOrNoneOr(const TExprNodeList& children) {
    size_t orCount = 0;
    for (auto& child : children) {
        orCount += child->IsCallable("Or");
    }
    return orCount == children.size() || orCount == 0;
}

TExprNodeList GetOrAndChildren(TExprNode::TPtr node, bool visitOr) {
    if (node->IsCallable("Likely")) {
        node = node->HeadPtr();
    }
    if (visitOr && node->IsCallable("Or") || !visitOr && node->IsCallable("And")) {
        return node->ChildrenList();
    }
    return { node };
}

const TExprNode* GetFirstOrAndChild(TExprNode::TPtr node, bool visitOr) {
    TExprNodeList children = GetOrAndChildren(node, visitOr);
    YQL_ENSURE(!children.empty());
    return children.front().Get();
}

TVector<TVector<size_t>> SplitToNonIntersectingGroups(const TExprNodeList& children, bool visitOr) {
    TNodeMap<size_t> node2id;
    for (const auto& child : children) {
        TExprNodeList components = GetOrAndChildren(child, visitOr);
        for (auto& p : components) {
            size_t currSize = node2id.size();
            node2id.insert({p.Get(), currSize});
        }
    }

    TDisjointSets disjointSets(node2id.size());
    for (const auto& child : children) {
        TExprNodeList components = GetOrAndChildren(child, visitOr);
        size_t first = GetNodeId(components.front().Get(), node2id);
        for (auto& p : components) {
            disjointSets.UnionSets(first, GetNodeId(p.Get(), node2id));
        }
    }

    THashMap<size_t, size_t> canonicalElement2Group;
    TVector<TVector<size_t>> groups;
    for (size_t i = 0; i < children.size(); ++i) {
        size_t first = GetNodeId(GetFirstOrAndChild(children[i], visitOr), node2id);
        size_t canonicalElement = disjointSets.CanonicSetElement(first);
        auto it = canonicalElement2Group.find(canonicalElement);
        if (it != canonicalElement2Group.end()) {
            YQL_ENSURE(groups.size() > it->second);
            groups[it->second].push_back(i);
        } else {
            canonicalElement2Group[canonicalElement] = groups.size();
            groups.emplace_back();
            groups.back().push_back(i);
        }
    }

    return groups;
}

TExprNode::TPtr ApplyAndAbsorption(const TExprNode::TPtr& node, TExprContext& ctx) {
    YQL_ENSURE(node->IsCallable("And"));
    TExprNodeList children = node->ChildrenList();
    TNodeOnNodeOwnedMap likelyPreds;
    StripLikely(children, likelyPreds);
    if (AllOrNoneOr(children)) {
        return node;
    }

    // AND Absorption law
    // (A OR B) AND A -> A
    const TVector<TVector<size_t>> groups = SplitToNonIntersectingGroups(children, true);

    THashSet<size_t> toDrop;
    for (auto& group : groups) {
        TVector<size_t> orIndexes;
        TNodeSet restSet;
        for (auto& index : group) {
            if (children[index]->IsCallable("Or")) {
                orIndexes.push_back(index);
            } else {
                restSet.insert(children[index].Get());
            }
        }
        for (auto& idx : orIndexes) {
            if (AnyOf(children[idx]->ChildrenList(), [&](const auto& n) { return restSet.contains(n.Get()); })) {
                toDrop.insert(idx);
            }
        }
    }

    if (!toDrop.empty()) {
        TExprNodeList newChildren;
        for (size_t i = 0; i < children.size(); ++i) {
            if (!toDrop.contains(i)) {
                newChildren.push_back(children[i]);
            }
        }
        UnstripLikely(newChildren, likelyPreds);
        bool addJust = node->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Optional &&
            AllOf(newChildren, [](const auto& node) {
                YQL_ENSURE(node->GetTypeAnn());
                return node->GetTypeAnn()->GetKind() != ETypeAnnotationKind::Optional;
            });
        YQL_CLOG(DEBUG, Core) << "Absorption law for AND. Original size: " << node->ChildrenSize() << ", result size: " << newChildren.size();
        return ctx.WrapByCallableIf(addJust, "Just", ctx.ChangeChildren(*node, std::move(newChildren)));
    }

    return node;
}

TExprNode::TPtr OptimizeAnd(const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) {
    if (auto opt = OptimizeDups(node, ctx); opt != node) {
        return opt;
    }

    if (auto opt = OptimizeExistsAndUnwrap(node, ctx); opt != node) {
        return opt;
    }

    if (IsExtractCommonPredicatesFromLogicalOpsEnabled(optCtx)) {
        if (auto opt = ApplyAndAbsorption(node, ctx); opt != node) {
            return opt;
        }
    }

    return node;
}

TExprNode::TPtr ApplyOrAbsorption(const TExprNode::TPtr& node, TExprContext& ctx) {
    YQL_ENSURE(node->IsCallable("Or"));
    const TExprNodeList children = node->ChildrenList();
    YQL_ENSURE(!children.empty());
    if (AnyOf(children, [](const auto& child) { return child->IsCallable("And"); })) {
        // OR Absorption law
        // X AND A OR A -> A
        // (X AND (B OR A)) OR A OR B -> A OR B
        TVector<size_t> andIndexes;
        TNodeSet restSet;
        for (size_t i = 0; i < children.size(); ++i) {
            if (children[i]->IsCallable("And")) {
                andIndexes.push_back(i);
            } else {
                restSet.insert(children[i].Get());
            }
        }

        THashSet<size_t> toDrop;
        for (auto& idx : andIndexes) {
            TExprNodeList andChildren = children[idx]->ChildrenList();
            bool haveCommonFactor = AnyOf(andChildren, [&](TExprNode::TPtr child) {
                if (child->IsCallable("Likely")) {
                    child = child->HeadPtr();
                }
                TExprNodeList orList = GetOrChildren(child);
                return AllOf(orList, [&](const auto& n) { return restSet.contains(n.Get()); });
            });
            if (haveCommonFactor) {
                toDrop.insert(idx);
            }
        }

        if (!toDrop.empty()) {
            TExprNodeList newChildren;
            for (size_t i = 0; i < children.size(); ++i) {
                if (!toDrop.contains(i)) {
                    newChildren.push_back(children[i]);
                }
            }
            bool addJust = node->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Optional &&
                AllOf(newChildren, [](const auto& node) {
                    YQL_ENSURE(node->GetTypeAnn());
                    return node->GetTypeAnn()->GetKind() != ETypeAnnotationKind::Optional;
                });
            YQL_CLOG(DEBUG, Core) << "Absorption law for OR. Original size: " << node->ChildrenSize() << ", result size: " << newChildren.size();
            return ctx.WrapByCallableIf(addJust, "Just", ctx.ChangeChildren(*node, std::move(newChildren)));
        }
    }
    return node;
}

TExprNode::TPtr ApplyOrDistributive(const TExprNode::TPtr& node, TExprContext& ctx) {
    YQL_ENSURE(node->IsCallable("Or"));
    const TExprNodeList children = node->ChildrenList();
    YQL_ENSURE(!children.empty());
    if (AllOf(children, [](const auto& child) { return child->IsCallable("And"); })) {
        // OR Distributive law: (a AND b) OR (b AND c) -> (a OR c) AND b
        if (!IsStrict(node)) {
            return node;
        }
        const TVector<TVector<size_t>> groups = SplitToNonIntersectingGroups(children, false);
        auto ptrComparator = [](const TExprNode::TPtr& l, const TExprNode::TPtr& r) {
            return l.Get() < r.Get();
        };
        TExprNodeList newChildren;
        bool changed = false;
        for (auto& group : groups) {
            YQL_ENSURE(!group.empty());
            if (group.size() == 1) {
                newChildren.push_back(children[group.front()]);
                continue;
            }
            TExprNodeList commonPreds = children[group.front()]->ChildrenList();

            TNodeOnNodeOwnedMap likelyPreds;
            StripLikely(commonPreds, likelyPreds);

            Sort(commonPreds, ptrComparator);
            for (size_t i = 1; i < group.size() && !commonPreds.empty(); ++i) {
                TExprNodeList curr = children[group[i]]->ChildrenList();
                StripLikely(curr, likelyPreds);
                Sort(curr, ptrComparator);

                TExprNodeList intersected;
                SetIntersection(commonPreds.begin(), commonPreds.end(), curr.begin(), curr.end(), std::back_inserter(intersected), ptrComparator);
                std::swap(commonPreds, intersected);
            }

            if (!commonPreds.empty()) {
                changed = true;
                TNodeSet commonSet;
                commonSet.reserve(commonPreds.size());
                for (const auto& c : commonPreds) {
                    commonSet.insert(c.Get());
                }
                UnstripLikely(commonPreds, likelyPreds);
                // stabilize common predicate order
                Sort(commonPreds, [](const auto& l, const auto& r) { return CompareNodes(*l, *r) < 0; });

                TExprNodeList newGroup;
                for (auto& idx : group) {
                    auto childAnd = children[idx];
                    TExprNodeList preds = childAnd->ChildrenList();
                    EraseIf(preds, [&](const TExprNode::TPtr& p) { return commonSet.contains(p->IsCallable("Likely") ? p->Child(0) : p.Get()); });
                    if (!preds.empty()) {
                        newGroup.emplace_back(ctx.ChangeChildren(*childAnd, std::move(preds)));
                    }
                }
                auto restPreds = ctx.NewCallable(node->Pos(), "Or", std::move(newGroup));
                commonPreds.push_back(restPreds);
                newChildren.push_back(ctx.NewCallable(node->Pos(), "And", std::move(commonPreds)));
            } else {
                for (auto& idx : group) {
                    newChildren.push_back(children[idx]);
                }
            }
        }

        if (changed) {
            YQL_CLOG(DEBUG, Core) << "Distributive law for OR";
            return ctx.ChangeChildren(*node, std::move(newChildren));
        }
    }

    return node;
}

TExprNode::TPtr OptimizeOr(const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) {
    if (auto opt = OptimizeDups(node, ctx); opt != node) {
        return opt;
    }

    TNodeOnNodeOwnedMap likelyPreds;
    TExprNodeList children = node->ChildrenList();
    StripLikely(children, likelyPreds);
    if (!likelyPreds.empty()) {
        // Likely(A) OR B -> Likely(A OR B)
        YQL_CLOG(DEBUG, Core) << "Or with Likely argument";
        return ctx.NewCallable(node->Pos(), "Likely", { ctx.ChangeChildren(*node, std::move(children)) });
    }

    if (IsExtractCommonPredicatesFromLogicalOpsEnabled(optCtx)) {
        if (auto opt = ApplyOrAbsorption(node, ctx); opt != node) {
            return opt;
        }
        if (auto opt = ApplyOrDistributive(node, ctx); opt != node) {
            return opt;
        }
    }
    return node;
}

TExprNode::TPtr OptimizeMinMax(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (node->GetTypeAnn()->IsOptionalOrNull()) {
        return node;
    }

    return OptimizeDups(node, ctx);
}

TExprNode::TPtr CheckIfWorldWithSame(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (node->Child(3U) == node->Child(2U)) {
        YQL_CLOG(DEBUG, Core) << node->Content() << " with identical branches";
        return ctx.NewCallable(node->Pos(), SyncName, {node->HeadPtr(), node->TailPtr()});
    }

    return node;
}

TExprNode::TPtr CheckIfWithSame(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (node->Child(node->ChildrenSize() - 1U) == node->Child(node->ChildrenSize() - 2U)) {
        YQL_CLOG(DEBUG, Core) << node->Content() << " with identical branches.";
        auto children = node->ChildrenList();
        children[children.size() - 3U] = std::move(children.back());
        children.resize(children.size() -2U);
        return 1U == children.size() ? children.front() : ctx.ChangeChildren(*node, std::move(children));
    }

    if (const auto width = node->ChildrenSize() >> 1U; width > 1U) {
        TNodeSet predicates(width), branches(width);
        for (auto i =0U; i < node->ChildrenSize() - 1U; ++i) {
            predicates.emplace(node->Child(i));
            branches.emplace(node->Child(++i));
        }

        if (predicates.size() < width) {
            YQL_CLOG(DEBUG, Core) << node->Content() << " with identical predicates.";
            auto children = node->ChildrenList();
            for (auto i = 0U; i < children.size() - 1U;) {
                if (predicates.erase(children[i].Get()))
                    i += 2U;
                else
                    children.erase(children.cbegin() + i, children.cbegin() + i + 2U);
            }
            return ctx.ChangeChildren(*node, std::move(children));
        }
        if (branches.size() < width) {
            for (auto i = 1U; i < node->ChildrenSize() - 2U; ++++i) {
                if (node->Child(i) ==  node->Child(i + 2U)) {
                    YQL_CLOG(DEBUG, Core) << node->Content() << " with identical branches.";
                    auto children = node->ChildrenList();
                    auto& prev = children[i - 1U];
                    auto& next = children[i + 1U];
                    if (prev->IsCallable("Or")) {
                        auto many = prev->ChildrenList();
                        many.emplace_back(std::move(next));
                        prev = ctx.ChangeChildren(*prev, std::move(many));
                    } else
                        prev = ctx.NewCallable(node->Pos(), "Or", {std::move(prev), std::move(next)});
                    children.erase(children.cbegin() + i + 1U, children.cbegin() + i + 3U);
                    return ctx.ChangeChildren(*node, std::move(children));
                }
            }
        }
    }

    return node;
}

template <bool Equal, bool Aggr>
TExprNode::TPtr CheckCompareSame(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (&node->Head() == &node->Tail() && (Aggr || HasTotalOrder(*node->Head().GetTypeAnn()))) {
        YQL_CLOG(DEBUG, Core) << (Equal ? "Equal" : "Unequal") << " '" << node->Content() << "' with same args";
        return MakeBool<Equal>(node->Pos(), ctx);
    }

    return node;
}

TExprNode::TPtr IfPresentSubsetFields(const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) {
    if (3U == node->ChildrenSize() && TCoFilterNullMembers::Match(&node->Head())) {
        auto children = node->ChildrenList();
        const auto& lambda = *children[TCoIfPresent::idx_PresentHandler];

        YQL_ENSURE(optCtx.ParentsMap);
        TSet<TStringBuf> usedFields;
        if (HaveFieldsSubset(lambda.TailPtr(), lambda.Head().Head(), usedFields, *optCtx.ParentsMap)) {
            YQL_CLOG(DEBUG, Core) << node->Content() << "SubsetFields";
            children[TCoIfPresent::idx_Optional] = FilterByFields(children[TCoIfPresent::idx_Optional]->Pos(), children[TCoIfPresent::idx_Optional], usedFields, ctx, false);
            children[TCoIfPresent::idx_PresentHandler] = ctx.DeepCopyLambda(*children[TCoIfPresent::idx_PresentHandler]);
            return ctx.ChangeChildren(*node, std::move(children));
        }
    }

    return node;
}

}

void RegisterCoSimpleCallables2(TCallableOptimizerMap& map) {
    using namespace std::placeholders;

    map[SyncName] = std::bind(&SimplifySync, _1, _2);

    map[IfName] = std::bind(&CheckIfWorldWithSame, _1, _2);

    map["If"] = std::bind(&CheckIfWithSame, _1, _2);

    map["Aggregate"] = [](const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext&) {
        if (auto deduplicated = DeduplicateAggregateSameTraits(node, ctx); deduplicated != node) {
            YQL_CLOG(DEBUG, Core) << "Deduplicate " << node->Content() << " traits";
            return deduplicated;

        }
        if (auto merged = MergeAggregateTraits(node, ctx); merged != node) {
            YQL_CLOG(DEBUG, Core) << "Merge aggregation traits in " << node->Content();
            return merged;
        }
        return node;
    };

    map["Xor"] = std::bind(&OptimizeXor, _1, _2);
    map["Not"] = std::bind(&OptimizeNot, _1, _2);

    map["And"] = OptimizeAnd;
    map["Or"] = OptimizeOr;

    map["Min"] = map["Max"] = std::bind(&OptimizeMinMax, _1, _2);

    map["AggrMin"] = map["AggrMax"] = map["Coalesce"] = std::bind(&DropAggrOverSame, _1);

    map["StartsWith"] = map["EndsWith"] = map["StringContains"] = std::bind(&CheckCompareSame<true, false>, _1, _2);

    map["=="] = map["<="] = map[">="] = std::bind(&CheckCompareSame<true, false>, _1, _2);
    map["!="] = map["<"] = map[">"] = std::bind(&CheckCompareSame<false, false>, _1, _2);

    map["AggrEquals"] = map["AggrLessOrEqual"] = map["AggrGreaterOrEqual"] = std::bind(&CheckCompareSame<true, true>, _1, _2);
    map["AggrNotEquals"] = map["AggrLess"] = map["AggrGreater"] = std::bind(&CheckCompareSame<false, true>, _1, _2);

    map["IfPresent"] = std::bind(&IfPresentSubsetFields, _1, _2, _3);

    map["SqlIn"] = [](const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext&) {
        auto collection = node->HeadPtr();
        if (collection->GetTypeAnn()->GetKind() == ETypeAnnotationKind::List && collection->GetConstraint<TSortedConstraintNode>()) {
            YQL_CLOG(DEBUG, Core) << "IN over sorted collection";
            return ctx.ChangeChild(*node, 0, ctx.NewCallable(collection->Pos(), "Unordered", {collection}));
        }

        return node;
    };

    map["PgGrouping"] = ExpandPgGrouping;
}

}
