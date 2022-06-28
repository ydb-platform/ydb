#include "yql_co.h"

#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/core/yql_expr_csee.h>

#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/utils/yql_panic.h>

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

    YQL_CLOG(DEBUG, Core) << "Deduplicate " << node->Content() << " traits";
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

TExprNode::TPtr OptimizeDups(const TExprNode::TPtr& node, TExprContext& ctx) {
    auto children = node->ChildrenList();
    DropDups(children);
    if (children.size() < node->ChildrenSize()) {
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
    DropDups(children);
    if (children.size() < node->ChildrenSize()) {
        const bool inverse = (node->ChildrenSize() - children.size()) % 2U;
        YQL_CLOG(DEBUG, Core) << node->Content() << " over some dups";
        return children.empty() ?
            ctx.WrapByCallableIf(node->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Optional, "Just", MakeBool(node->Pos(), inverse, ctx)):
            ctx.WrapByCallableIf(inverse, "Not", ctx.ChangeChildren(*node, std::move(children)));
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

TExprNode::TPtr OptimizeAnd(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (auto opt = OptimizeDups(node, ctx); opt != node) {
        return opt;
    }

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

    map["Aggregate"] = std::bind(&DeduplicateAggregateSameTraits, _1, _2);

    map["Xor"] = std::bind(&OptimizeXor, _1, _2);
    map["Not"] = std::bind(&OptimizeNot, _1, _2);

    map["And"] = std::bind(&OptimizeAnd, _1, _2);
    map["Or"] = std::bind(OptimizeDups, _1, _2);

    map["Min"] = map["Max"] = std::bind(&OptimizeDups, _1, _2);

    map["AggrMin"] = map["AggrMax"] = map["Coalesce"] = std::bind(&DropAggrOverSame, _1);

    map["StartsWith"] = map["EndsWith"] = std::bind(&CheckCompareSame<true, false>, _1, _2);

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
}

}
