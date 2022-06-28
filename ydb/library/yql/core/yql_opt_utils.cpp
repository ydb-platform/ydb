#include "yql_opt_utils.h"
#include "yql_expr_optimize.h"
#include "yql_expr_type_annotation.h"
#include "yql_type_annotation.h"
#include "yql_type_helpers.h"

#include <ydb/library/yql/ast/yql_constraint.h>
#include <ydb/library/yql/utils/log/log.h>

#include <util/generic/set.h>
#include <util/string/type.h>

namespace NYql {

using namespace NNodes;

TExprNode::TPtr MakeBoolNothing(TPositionHandle position, TExprContext& ctx) {
    return ctx.NewCallable(position, "Nothing", {
        ctx.NewCallable(position, "OptionalType", {
            ctx.NewCallable(position, "DataType", {
                ctx.NewAtom(position, "Bool", TNodeFlags::Default) }) }) });
}

TExprNode::TPtr MakeNull(TPositionHandle position, TExprContext& ctx) {
   return ctx.NewCallable(position, "Null", {});
}

TExprNode::TPtr MakeConstMap(TPositionHandle position, const TExprNode::TPtr& input,
   const TExprNode::TPtr& value, TExprContext& ctx) {
   return ctx.Builder(position)
       .Callable("Map")
           .Add(0, input)
           .Lambda(1)
               .Param("x")
               .Set(value)
           .Seal()
       .Seal()
       .Build();
}

template <bool Bool>
TExprNode::TPtr MakeBool(TPositionHandle position, TExprContext& ctx) {
    return ctx.NewCallable(position, "Bool", { ctx.NewAtom(position, Bool ? "true" : "false", TNodeFlags::Default) });
}

TExprNode::TPtr MakeBool(TPositionHandle position, bool value, TExprContext& ctx) {
    return value ? MakeBool<true>(position, ctx) : MakeBool<false>(position, ctx);
}

TExprNode::TPtr MakeOptionalBool(TPositionHandle position, bool value, TExprContext& ctx) {
    return ctx.NewCallable(position, "Just", { MakeBool(position, value, ctx)});
}

TExprNode::TPtr MakeIdentityLambda(TPositionHandle position, TExprContext& ctx) {
    return ctx.Builder(position)
        .Lambda()
            .Param("arg")
            .Arg("arg")
        .Seal()
        .Build();
}

bool IsJustOrSingleAsList(const TExprNode& node) {
    return node.ChildrenSize() == 1U && node.IsCallable({"Just", "AsList"});
}

bool IsTransparentIfPresent(const TExprNode& node) {
    return (node.IsCallable("FlatMap") || (3U == node.ChildrenSize() && node.IsCallable("IfPresent") && node.Tail().IsCallable("Nothing")))
        && node.Child(1U)->Tail().IsCallable("Just");
}

bool IsPredicateFlatMap(const TExprNode& node) {
    return node.IsCallable({"FlatListIf", "ListIf", "OptionalIf", "FlatOptionalIf"});
}

bool IsFilterFlatMap(const NNodes::TCoLambda& lambda) {
    const auto& arg = lambda.Args().Arg(0);
    const auto& body = lambda.Body();
    if (body.Maybe<TCoOptionalIf>() || body.Maybe<TCoListIf>()) {
        if (body.Ref().Child(1) == arg.Raw()) {
            return true;
        }
    }

    return false;
}

bool IsListReorder(const TExprNode& node) {
    return node.IsCallable({"Sort", "Reverse"});
}

bool IsRenameFlatMap(const NNodes::TCoFlatMapBase& node, TExprNode::TPtr& structNode) {
    auto lambda = node.Lambda();
    if (!IsJustOrSingleAsList(lambda.Body().Ref())) {
        return false;
    }

    structNode = lambda.Body().Ref().Child(0);
    if (!structNode->IsCallable("AsStruct")) {
        return false;
    }

    for (auto& child : structNode->Children()) {
        auto item = child->Child(1);
        if (!item->IsCallable("Member") || item->Child(0) != lambda.Args().Arg(0).Raw()) {
            return false;
        }
    }

    return true;
}

bool IsPassthroughFlatMap(const TCoFlatMapBase& flatmap, TMaybe<THashSet<TStringBuf>>* passthroughFields, bool analyzeJustMember) {
    return IsPassthroughLambda(flatmap.Lambda(), passthroughFields, analyzeJustMember);
}

bool IsPassthroughLambda(const TCoLambda& lambda, TMaybe<THashSet<TStringBuf>>* passthroughFields, bool analyzeJustMember) {
    auto body = lambda.Body();
    auto arg = lambda.Args().Arg(0);

    TMaybeNode<TExprBase> outItem;
    if (IsJustOrSingleAsList(body.Ref())) {
        outItem = body.Ref().Child(0);
    }

    if (body.Maybe<TCoOptionalIf>() || body.Maybe<TCoListIf>()) {
        outItem = body.Cast<TCoConditionalValueBase>().Value();
    }

    if (!outItem) {
        return false;
    }

    if (outItem.Cast().Raw() == arg.Raw()) {
        return true;
    }

    if (auto maybeStruct = outItem.Maybe<TCoAsStruct>()) {
        if (passthroughFields) {
            passthroughFields->ConstructInPlace();
        }

        for (auto child : maybeStruct.Cast()) {
            auto tuple = child.Cast<TCoNameValueTuple>();
            auto value = tuple.Value();
            if (analyzeJustMember && value.Maybe<TCoJust>()) {
                value = value.Cast<TCoJust>().Input();
            }

            if (!value.Maybe<TCoMember>()) {
                if (!passthroughFields) {
                    return false;
                }

                continue;
            }

            auto member = value.Cast<TCoMember>();
            if (member.Struct().Raw() != arg.Raw() || member.Name().Value() != tuple.Name().Value()) {
                if (!passthroughFields) {
                    return false;
                }
            } else {
                if (passthroughFields) {
                    (*passthroughFields)->insert(member.Name().Value());
                }
            }
        }

        return true;
    }

    return false;
}

bool IsTablePropsDependent(const TExprNode& node) {
    bool found = false;
    VisitExpr(node, [&found](const TExprNode& n) {
        found = found || TCoTablePropBase::Match(&n) || (TCoMember::Match(&n) && TCoMember(&n).Name().Value().StartsWith("_yql_sys_"));
        return !found;
    });
    return found;
}

TExprNode::TPtr KeepColumnOrder(const TExprNode::TPtr& node, const TExprNode& src, TExprContext& ctx, const TTypeAnnotationContext& typeCtx) {
    auto columnOrder = typeCtx.LookupColumnOrder(src);
    if (!columnOrder) {
        return node;
    }

    return ctx.Builder(node->Pos())
        .Callable("AssumeColumnOrder")
            .Add(0, node)
            .List(1)
                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                    size_t index = 0;
                    for (auto& col : *columnOrder) {
                        parent
                            .Atom(index++, col);
                    }
                    return parent;
                })
            .Seal()
        .Seal()
        .Build();
}

template<class TFieldsSet>
bool HaveFieldsSubset(const TExprNode::TPtr& start, const TExprNode& arg, TFieldsSet& usedFields, const TParentsMap& parentsMap, bool allowDependsOn) {
    const TTypeAnnotationNode* argType = RemoveOptionalType(arg.GetTypeAnn());
    if (argType->GetKind() != ETypeAnnotationKind::Struct) {
        return false;
    }

    if (&arg == start.Get()) {
        return false;
    }

    const auto inputStructType = argType->Cast<TStructExprType>();
    if (!IsDepended(*start, arg)) {
        return inputStructType->GetSize() > 0;
    }

    TNodeSet nodes;
    VisitExpr(start, [&](const TExprNode::TPtr& node) {
        nodes.insert(node.Get());
        return true;
    });

    const auto parents = parentsMap.find(&arg);
    YQL_ENSURE(parents != parentsMap.cend());
    for (const auto& parent : parents->second) {
        if (nodes.cend() == nodes.find(parent)) {
            continue;
        }

        if (parent->IsCallable("Member")) {
            if constexpr (std::is_same_v<typename TFieldsSet::key_type, typename TFieldsSet::value_type>)
                usedFields.emplace(parent->Tail().Content());
            else
                usedFields.emplace(parent->Tail().Content(), parent->TailPtr());
        } else if (allowDependsOn && parent->IsCallable("DependsOn")) {
            continue;
        } else {
            // unknown node
            usedFields.clear();
            return false;
        }
    }

    return usedFields.size() < inputStructType->GetSize();
}

template bool HaveFieldsSubset(const TExprNode::TPtr& start, const TExprNode& arg, TSet<TStringBuf>& usedFields, const TParentsMap& parentsMap, bool allowDependsOn);
template bool HaveFieldsSubset(const TExprNode::TPtr& start, const TExprNode& arg, TSet<TString>& usedFields, const TParentsMap& parentsMap, bool allowDependsOn);
template bool HaveFieldsSubset(const TExprNode::TPtr& start, const TExprNode& arg, std::map<std::string_view, TExprNode::TPtr>& usedFields, const TParentsMap& parentsMap, bool allowDependsOn);

template<class TFieldsSet>
TExprNode::TPtr FilterByFields(TPositionHandle position, const TExprNode::TPtr& input, const TFieldsSet& subsetFields,
    TExprContext& ctx, bool singleValue) {
    if (singleValue) {
        TExprNode::TListType structItems;
        for (auto field : subsetFields) {
            auto name = ctx.NewAtom(position, field);
            auto member = ctx.NewCallable(position, "Member", { input , name });
            structItems.push_back(ctx.NewList(position, { name, member }));
        }

        return ctx.NewCallable(position, "AsStruct", std::move(structItems));
    }

    TExprNode::TListType fields;
    for (auto& x : subsetFields) {
        fields.emplace_back(ctx.NewAtom(position, x));
    }

    return ctx.NewCallable(position, "ExtractMembers", { input, ctx.NewList(position, std::move(fields)) });
}

template TExprNode::TPtr FilterByFields(TPositionHandle position, const TExprNode::TPtr& input, const TSet<TStringBuf>& subsetFields, TExprContext& ctx, bool singleValue);
template TExprNode::TPtr FilterByFields(TPositionHandle position, const TExprNode::TPtr& input, const TSet<TString>& subsetFields, TExprContext& ctx, bool singleValue);


bool IsDependedImpl(const TExprNode* from, const TExprNode* to, TNodeSet& visited) {
    if (from == to)
        return true;

    if (!visited.emplace(from).second) {
        return false;
    }

    for (const auto& child : from->Children()) {
        if (IsDependedImpl(child.Get(), to, visited))
            return true;
    }

    return false;
}

bool IsDepended(const TExprNode& from, const TExprNode& to) {
    TNodeSet visited;
    return IsDependedImpl(&from, &to, visited);
}

bool IsEmpty(const TExprNode& node, const TTypeAnnotationContext& typeCtx) {
    return typeCtx.IsConstraintCheckEnabled<TEmptyConstraintNode>() && node.Type() != TExprNode::Argument && node.GetConstraint<TEmptyConstraintNode>() != nullptr;
}

bool IsEmptyOptional(const TExprNode& node) {
    return node.IsCallable("Nothing");
}

bool IsEmptyContainer(const TExprNode& node) {
    return node.IsCallable({"EmptyList", "EmptyDict"})
        || (1U == node.ChildrenSize() && node.IsCallable({"List", "Nothing", "EmptyIterator", "Dict"}));
}

const TTypeAnnotationNode* RemoveOptionalType(const TTypeAnnotationNode* type) {
    if (!type || type->GetKind() != ETypeAnnotationKind::Optional) {
        return type;
    }

    return type->Cast<TOptionalExprType>()->GetItemType();
}

const TTypeAnnotationNode* RemoveAllOptionals(const TTypeAnnotationNode* type) {
    while (type && type->GetKind() == ETypeAnnotationKind::Optional) {
        type = type->Cast<TOptionalExprType>()->GetItemType();
    }
    return type;
}

const TTypeAnnotationNode* GetSeqItemType(const TTypeAnnotationNode* type) {
    switch (type->GetKind()) {
        case ETypeAnnotationKind::List: return type->Cast<TListExprType>()->GetItemType();
        case ETypeAnnotationKind::Flow: return type->Cast<TFlowExprType>()->GetItemType();
        case ETypeAnnotationKind::Stream: return type->Cast<TStreamExprType>()->GetItemType();
        case ETypeAnnotationKind::Optional: return type->Cast<TOptionalExprType>()->GetItemType();
        default: break;
    }

    THROW yexception() << "Impossible to get item type from " << *type;
}

TExprNode::TPtr GetSetting(const TExprNode& settings, const TStringBuf& name) {
    for (auto& setting : settings.Children()) {
        if (setting->ChildrenSize() != 0 && setting->Child(0)->Content() == name) {
            return setting;
        }
    }
    return nullptr;
}

bool HasSetting(const TExprNode& settings, const TStringBuf& name) {
    return GetSetting(settings, name) != nullptr;
}

bool HasAnySetting(const TExprNode& settings, const THashSet<TString>& names) {
    for (auto& setting : settings.Children()) {
        if (setting->ChildrenSize() != 0 && names.contains(setting->Head().Content())) {
            return true;
        }
    }
    return false;
}

TExprNode::TPtr RemoveSetting(const TExprNode& settings, const TStringBuf& name, TExprContext& ctx) {
    TExprNode::TListType children;
    for (auto setting : settings.Children()) {
        if (setting->ChildrenSize() != 0 && setting->Head().Content() == name) {
            continue;
        }

        children.push_back(setting);
    }

    return ctx.NewList(settings.Pos(), std::move(children));
}

TExprNode::TPtr ReplaceSetting(const TExprNode& settings, TPositionHandle pos, const TString& name, const TExprNode::TPtr& value, TExprContext& ctx) {
    TExprNode::TListType newChildren;
    for (auto setting : settings.Children()) {
        if (setting->ChildrenSize() != 0 && setting->Head().Content() == name) {
            continue;
        }

        newChildren.push_back(setting);
    }

    newChildren.push_back(value ? ctx.NewList(pos, { ctx.NewAtom(pos, name), value }) : ctx.NewList(pos, { ctx.NewAtom(pos, name) }));

    auto ret = ctx.NewList(settings.Pos(), std::move(newChildren));
    return ret;
}

TExprNode::TPtr AddSetting(const TExprNode& settings, TPositionHandle pos, const TString& name, const TExprNode::TPtr& value, TExprContext& ctx) {
    auto newChildren = settings.ChildrenList();
    newChildren.push_back(value ? ctx.NewList(pos, { ctx.NewAtom(pos, name), value }) : ctx.NewList(pos, { ctx.NewAtom(pos, name) }));

    auto ret = ctx.NewList(settings.Pos(), std::move(newChildren));
    return ret;
}


TExprNode::TPtr MergeSettings(const TExprNode& settings1, const TExprNode& settings2, TExprContext& ctx) {
    auto newChildren = settings1.ChildrenList();
    newChildren.insert(
        newChildren.cend(),
        settings2.Children().begin(),
        settings2.Children().end());
    auto ret = ctx.NewList(settings1.Pos(), std::move(newChildren));
    return ret;
}

TMaybe<TIssue> ParseToDictSettings(const TExprNode& input, TExprContext& ctx, TMaybe<bool>& isMany, TMaybe<bool>& isHashed, TMaybe<ui64>& itemsCount, bool& isCompact) {
    isCompact = false;
    auto settings = input.Child(3);
    if (settings->Type() != TExprNode::List) {
        return TIssue(ctx.GetPosition(settings->Pos()), TStringBuilder() << "Expected tuple, but got: " << input.Type());
    }

    for (auto& child : settings->Children()) {
        if (child->Type() == TExprNode::Atom) {
            if (child->Content() == "One") {
                isMany = false;
            }
            else if (child->Content() == "Many") {
                isMany = true;
            }
            else if (child->Content() == "Sorted") {
                isHashed = false;
            }
            else if (child->Content() == "Hashed") {
                isHashed = true;
            }
            else if (child->Content() == "Compact") {
                isCompact = true;
            }
            else {
                return TIssue(ctx.GetPosition(child->Pos()), TStringBuilder() << "Unsupported option: " << child->Content());
            }
        } else if (child->Type() == TExprNode::List) {
            if (child->ChildrenSize() != 2) {
                return TIssue(ctx.GetPosition(child->Pos()), TStringBuilder() << "Expected list with 2 elemenst");
            }
            if (child->Child(0)->Content() == "ItemsCount") {
                ui64 count = 0;
                if (TryFromString(child->Child(1)->Content(), count)) {
                    itemsCount = count;
                } else {
                    return TIssue(ctx.GetPosition(child->Pos()), TStringBuilder() << "Bad 'ItemsCount' value: " << child->Child(1)->Content());
                }
            }
            else {
                return TIssue(ctx.GetPosition(child->Pos()), TStringBuilder() << "Bad option: " << child->Child(0)->Content());
            }
        } else {
            return TIssue(ctx.GetPosition(child->Pos()), TStringBuilder() << "Expected atom or list, but got: " << input.Type());
        }

    }

    if (!isHashed || !isMany) {
        return TIssue(ctx.GetPosition(input.Pos()), TStringBuilder() << "Both options must be specified: Sorted/Hashed and Many/One");
    }

    return TMaybe<TIssue>();
}

TExprNode::TPtr MakeSingleGroupRow(const TExprNode& aggregateNode, TExprNode::TPtr reduced, TExprContext& ctx) {
    auto pos = aggregateNode.Pos();
    auto aggregatedColumns = aggregateNode.Child(2);
    auto opt = ctx.NewCallable(pos, "ToOptional", { reduced });
    TExprNode::TListType finalRowNodes;
    for (ui32 index = 0; index < aggregatedColumns->ChildrenSize(); ++index) {
        auto column = aggregatedColumns->Child(index);
        auto trait = column->Child(1);
        auto defVal = trait->Child(7);

        if (column->Child(0)->IsAtom()) {
            finalRowNodes.push_back(ctx.Builder(pos)
                .List()
                    .Atom(0, column->Child(0)->Content())
                    .Callable(1, "Coalesce")
                        .Callable(0, "Member")
                            .Add(0, opt)
                            .Atom(1, column->Child(0)->Content())
                        .Seal()
                        .Add(1, defVal)
                    .Seal()
                .Seal()
                .Build());
        } else {
            const auto& multiFields = column->Child(0)->Children();
            for (ui32 field = 0; field < multiFields.size(); ++field) {
                finalRowNodes.push_back(ctx.Builder(pos)
                    .List()
                        .Atom(0, multiFields[field]->Content())
                        .Callable(1, "Member")
                            .Add(0, opt)
                            .Atom(1, multiFields[field]->Content())
                        .Seal()
                    .Seal()
                    .Build());
            }
        }
    }

    return ctx.Builder(pos)
        .Callable("AsList")
            .Add(0, ctx.NewCallable(pos, "AsStruct", std::move(finalRowNodes)))
        .Seal()
        .Build();
}

bool UpdateStructMembers(TExprContext& ctx, const TExprNode::TPtr& node, const TStringBuf& goal, TExprNode::TListType& members, MemberUpdaterFunc updaterFunc, const TTypeAnnotationNode* nodeType) {
    if (!nodeType) {
        nodeType = node->GetTypeAnn();
        Y_VERIFY_DEBUG(nodeType || !"Unset node type for UpdateStructMembers");
    }
    bool filtered = false;
    if (node->IsCallable("AsStruct")) {
        YQL_CLOG(DEBUG, Core) << "Enumerate struct literal for " << goal;
        for (ui32 i = 0; i < node->ChildrenSize(); ++i) {
            const auto memberNode = node->ChildPtr(i);
            const auto& memberName = memberNode->Child(0)->Content();
            TString useName = TString(memberName);
            if (updaterFunc && !updaterFunc(useName, memberNode->GetTypeAnn())) {
                filtered = true;
                continue;
            }
            if (useName == memberName) {
                members.push_back(memberNode);
            } else {
                auto useNameNode = ctx.NewAtom(node->Pos(), useName);
                members.push_back(ctx.NewList(node->Pos(), { useNameNode, memberNode->ChildPtr(1) }));
            }
        }
    } else if (nodeType->GetKind() == ETypeAnnotationKind::Optional) {
        YQL_CLOG(DEBUG, Core) << "Enumerate optional struct for " << goal;
        auto itemType = nodeType->Cast<TOptionalExprType>()->GetItemType();
        return UpdateStructMembers(ctx, node, goal, members, updaterFunc, itemType);
    } else {
        YQL_CLOG(DEBUG, Core) << "Enumerate struct object for " << goal;
        auto structType = nodeType->Cast<TStructExprType>();
        for (auto item : structType->GetItems()) {
            TString useName = TString(item->GetName());
            if (updaterFunc && !updaterFunc(useName, item->GetItemType())) {
                filtered = true;
                continue;
            }
            members.push_back(ctx.Builder(node->Pos())
                .List()
                    .Atom(0, useName)
                    .Callable(1, "Member")
                        .Add(0, node)
                        .Atom(1, item->GetName())
                    .Seal()
                .Seal()
                .Build());
        }
    }
    return filtered;
}

TExprNode::TPtr ExpandRemoveMember(const TExprNode::TPtr& node, TExprContext& ctx) {
    const bool force = node->Content() == "ForceRemoveMember";
    const auto& removeMemberName = node->Child(1)->Content();
    MemberUpdaterFunc removeFunc = [&removeMemberName](TString& memberName, const TTypeAnnotationNode*) {
        return memberName != removeMemberName;
    };
    TExprNode::TListType members;
    if (!UpdateStructMembers(ctx, node->ChildPtr(0), node->Content(), members, removeFunc)) {
        if (!force) {
            YQL_ENSURE(false, "Unexpected member name: " << removeMemberName);
        }
        return node->ChildPtr(0);
    }
    return ctx.NewCallable(node->Pos(), "AsStruct", std::move(members));
}

TExprNode::TPtr ExpandRemovePrefixMembers(const TExprNode::TPtr& node, TExprContext& ctx) {
    YQL_CLOG(DEBUG, Core) << "Expand " << node->Content();

    const TTypeAnnotationNode* targetItemType = GetItemType(*node->GetTypeAnn());
    bool isSequence = true;
    if (!targetItemType) {
        targetItemType = node->GetTypeAnn();
        isSequence = false;
    }

    auto rebuildStruct = [&](const TExprNode::TPtr& srcStruct, const TStructExprType* targetType) -> TExprNode::TPtr {
        TExprNode::TListType nonSystemMembers;
        for (auto item : targetType->GetItems()) {
            nonSystemMembers.push_back(
                Build<TCoNameValueTuple>(ctx, srcStruct->Pos())
                    .Name()
                        .Value(item->GetName())
                    .Build()
                    .Value<TCoMember>()
                        .Struct(srcStruct)
                        .Name()
                            .Value(item->GetName())
                        .Build()
                    .Build()
                .Done().Ptr()
            );
        }
        return Build<TCoAsStruct>(ctx, srcStruct->Pos())
            .Add(std::move(nonSystemMembers))
            .Done()
            .Ptr();
    };

    if (targetItemType->GetKind() == ETypeAnnotationKind::Struct) {
        if (isSequence) {
            TExprNode::TListType nonSystemMembers;
            for (auto item : targetItemType->Cast<TStructExprType>()->GetItems()) {
                nonSystemMembers.push_back(ctx.NewAtom(node->Pos(), item->GetName()));
            }

            return Build<TCoExtractMembers>(ctx, node->Pos())
                .Input(node->HeadPtr())
                .Members()
                    .Add(nonSystemMembers)
                .Build()
                .Done().Ptr();
        }

        return rebuildStruct(node->HeadPtr(), targetItemType->Cast<TStructExprType>());
    }

    YQL_ENSURE(targetItemType->GetKind() == ETypeAnnotationKind::Variant);

    const auto targetUnderType = targetItemType->Cast<TVariantExprType>()->GetUnderlyingType();
    TExprNode::TListType variants;
    TTypeAnnotationNode::TListType types;
    switch (targetUnderType->GetKind()) {
        case ETypeAnnotationKind::Tuple: {
            types = targetUnderType->Cast<TTupleExprType>()->GetItems();
            variants.resize(types.size());
            for (ui32 i = 0U; i < variants.size(); ++i) {
                variants[i] = ctx.NewAtom(node->Pos(), ToString(i), TNodeFlags::Default);
            }
            break;
        }
        case ETypeAnnotationKind::Struct: {
            const auto& items = targetUnderType->Cast<TStructExprType>()->GetItems();
            types.resize(items.size());
            variants.resize(items.size());
            for (ui32 i = 0U; i < items.size(); ++i) {
                types[i] = items[i]->GetItemType();
                variants[i] = ctx.NewAtom(node->Pos(), items[i]->GetName());
            }
            break;
        }
        default: break;
    }
    const auto type = ExpandType(node->Pos(), *targetItemType, ctx);

    if (isSequence) {
        return ctx.Builder(node->Pos())
            .Callable("Map")
                .Add(0, node->HeadPtr())
                .Lambda(1)
                    .Param("varItem")
                    .Callable("Visit")
                        .Arg(0, "varItem")
                        .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                            for (ui32 i = 0U; i < variants.size(); ++i) {
                                parent.Add(1 + 2 * i, variants[i]);
                                parent.Lambda(2 + 2 * i)
                                    .Param("item")
                                    .Callable("Variant")
                                        .Callable(0, "AsStruct")
                                        .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                            auto targetStructType = types[i]->Cast<TStructExprType>();
                                            for (size_t j = 0; j < targetStructType->GetItems().size(); ++j) {
                                                auto name = targetStructType->GetItems()[j]->GetName();
                                                parent.List(j)
                                                    .Atom(0, name)
                                                    .Callable(1, "Member")
                                                        .Arg(0, "item")
                                                        .Atom(1, name)
                                                    .Seal()
                                                .Seal();
                                            }
                                            return parent;
                                        })
                                        .Seal()
                                        .Add(1, std::move(variants[i]))
                                        .Add(2, type)
                                    .Seal()
                                .Seal();
                            }
                            return parent;
                        })
                    .Seal()
                .Seal()
            .Seal()
            .Build();
    }

    return ctx.Builder(node->Pos())
        .Callable("Visit")
            .Add(0, node->HeadPtr())
            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                for (ui32 i = 0U; i < variants.size(); ++i) {
                    parent.Add(1 + 2 * i, variants[i]);
                    parent.Lambda(2 + 2 * i)
                        .Param("item")
                        .Callable("Variant")
                            .Callable(0, "AsStruct")
                            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                auto targetStructType = types[i]->Cast<TStructExprType>();
                                for (size_t j = 0; j < targetStructType->GetItems().size(); ++j) {
                                    auto name = targetStructType->GetItems()[j]->GetName();
                                    parent.List(j)
                                        .Atom(0, name)
                                        .Callable(1, "Member")
                                            .Arg(0, "item")
                                            .Atom(1, name)
                                        .Seal()
                                    .Seal();
                                }
                                return parent;
                            })
                            .Seal()
                            .Add(1, std::move(variants[i]))
                            .Add(2, type)
                        .Seal()
                    .Seal();
                }
                return parent;
            })
        .Seal()
        .Build();
}

TExprNode::TPtr ExpandFlattenMembers(const TExprNode::TPtr& node, TExprContext& ctx) {
    TExprNode::TListType members;
    for (auto& child : node->Children()) {
        auto prefix = child->Child(0)->Content();
        MemberUpdaterFunc addPrefixFunc = [&prefix](TString& memberName, const TTypeAnnotationNode*) {
            memberName = prefix + memberName;
            return true;
        };
        UpdateStructMembers(ctx, child->ChildPtr(1), "FlattenMembers", members, addPrefixFunc);
    }
    return ctx.NewCallable(node->Pos(), "AsStruct", std::move(members));
}

TExprNode::TPtr ExpandFlattenStructs(const TExprNode::TPtr& node, TExprContext& ctx) {
    TExprNode::TListType members;
    auto structObj = node->Child(0);
    for (auto& x : structObj->GetTypeAnn()->Cast<TStructExprType>()->GetItems()) {
        auto subMember = ctx.Builder(node->Pos())
            .Callable("Member")
            .Add(0, structObj)
            .Atom(1, x->GetName())
            .Seal()
            .Build();

        auto itemType = x->GetItemType();
        if (itemType->GetKind() == ETypeAnnotationKind::Optional) {
            itemType = itemType->Cast<TOptionalExprType>()->GetItemType();
        }

        if (itemType->GetKind() != ETypeAnnotationKind::Struct) {
            members.push_back(ctx.Builder(node->Pos())
                .List()
                    .Atom(0, x->GetName())
                    .Add(1, subMember)
                .Seal()
                .Build());
            continue;
        }

        UpdateStructMembers(ctx, subMember, "FlattenStructs", members, {}, x->GetItemType());
    }
    return ctx.NewCallable(node->Pos(), "AsStruct", std::move(members));
}

TExprNode::TPtr ExpandDivePrefixMembers(const TExprNode::TPtr& node, TExprContext& ctx) {
    auto prefixes = node->Child(1)->Children();
    MemberUpdaterFunc filterAndCutByPrefixFunc = [&prefixes](TString& memberName, const TTypeAnnotationNode*) {
        for (const auto& p : prefixes) {
            auto prefix = p->Content();
            if (memberName.StartsWith(prefix)) {
                memberName = memberName.substr(prefix.length());
                return true;
            }
        }
        return false;
    };
    TExprNode::TListType members;
    UpdateStructMembers(ctx, node->ChildPtr(0), "DivePrefixMembers", members, filterAndCutByPrefixFunc);
    auto ret = ctx.NewCallable(node->Pos(), "AsStruct", std::move(members));
    return ret;
}

TExprNode::TPtr ExpandAddMember(const TExprNode::TPtr& node, TExprContext& ctx) {
    TExprNode::TListType members;
    UpdateStructMembers(ctx, node->ChildPtr(0), "AddMember", members);
    members.push_back(ctx.NewList(node->Pos(), { node->ChildPtr(1), node->ChildPtr(2) }));
    return ctx.NewCallable(node->Pos(), "AsStruct", std::move(members));
}

TExprNode::TPtr ExpandReplaceMember(const TExprNode::TPtr& node, TExprContext& ctx) {
    const auto& removeMemberName = node->Child(1)->Content();
    MemberUpdaterFunc cloneFunc = [&removeMemberName](TString& memberName, const TTypeAnnotationNode*) {
        return memberName != removeMemberName;
    };
    TExprNode::TListType members;
    UpdateStructMembers(ctx, node->ChildPtr(0), "ReplaceMember", members, cloneFunc);
    members.push_back(ctx.NewList(node->Pos(), { node->ChildPtr(1), node->ChildPtr(2) }));
    auto ret = ctx.NewCallable(node->Pos(), "AsStruct", std::move(members));
    return ret;
}

TExprNode::TPtr ExpandFlattenByColumns(const TExprNode::TPtr& node, TExprContext& ctx) {
    ui32 shift = 0;
    TStringBuf mode = "auto";
    if (node->Child(0)->IsAtom()) {
        mode = node->Child(0)->Content();
        shift = 1;
    }

    auto structObj = node->ChildPtr(shift);
    TExprNode::TListType members;
    struct TFlattenInfo {
        const TTypeAnnotationNode* Type = nullptr;
        TExprNode::TPtr ArgNode = nullptr;
        TExprNode::TPtr ListMember = nullptr;
        TExprNode::TPtr ColumnNode = nullptr;
        bool ReplaceName = false;
    };
    TVector<TFlattenInfo> flattens;
    TMap<TString, size_t> name2Info;
    size_t flattenIndex = 0;
    for (auto iter = node->Children().begin() + 1 + shift; iter != node->Children().end(); ++iter) {
        auto flattenColumnNode = *iter;
        const bool haveAlias = flattenColumnNode->ChildrenSize() == 2;
        const TString flattenItemName = (haveAlias) ? TString(flattenColumnNode->Child(1)->Content()) : TString(flattenColumnNode->Content());
        TFlattenInfo flattenInfo;
        if (haveAlias) {
            flattenColumnNode = flattenColumnNode->Child(0);
        }
        else {
            flattenInfo.ReplaceName = true;
        }
        const auto useNameNode = ctx.NewAtom(node->Pos(), flattenItemName);
        flattenInfo.ArgNode = ctx.NewArgument(node->Pos(), flattenItemName);
        flattenInfo.ColumnNode = flattenColumnNode;
        members.push_back(ctx.NewList(node->Pos(), { useNameNode, flattenInfo.ArgNode }));
        flattens.emplace_back(std::move(flattenInfo));
        name2Info.insert({ TString(flattenColumnNode->Content()), flattenIndex++ });
    }
    MemberUpdaterFunc removeAndInfosUpdateFunc = [&flattens, &name2Info](TString& memberName, const TTypeAnnotationNode* type) {
        const auto iter = name2Info.find(memberName);
        if (iter == name2Info.end()) {
            return true;
        }
        TFlattenInfo& flattenInfo = flattens[iter->second];
        flattenInfo.Type = type;
        return !flattenInfo.ReplaceName;
    };
    UpdateStructMembers(ctx, structObj, "FlattenByColumns", members, removeAndInfosUpdateFunc);
    auto internalResult = ctx.NewCallable(node->Pos(), "AsStruct", std::move(members));
    TDeque<const TFlattenInfo*> flattenPriority;
    for (auto& flattenInfo : flattens) {
        bool isDict = false;
        bool isList = false;
        flattenInfo.ListMember = ctx.Builder(structObj->Pos())
            .Callable("Member")
            .Add(0, structObj)
            .Add(1, flattenInfo.ColumnNode)
            .Seal().Build();
        if (mode == "list" && flattenInfo.Type->GetKind() == ETypeAnnotationKind::Optional) {
            isList = true;
            flattenInfo.ListMember = ctx.Builder(structObj->Pos())
                .Callable("Coalesce")
                    .Add(0, flattenInfo.ListMember)
                    .Callable(1, "List")
                        .Add(0, ExpandType(structObj->Pos(), *flattenInfo.Type->Cast<TOptionalExprType>()->GetItemType(), ctx))
                    .Seal()
                .Seal()
                .Build();
        } else if (mode == "dict" && flattenInfo.Type->GetKind() == ETypeAnnotationKind::Optional) {
            isDict = true;
            flattenInfo.ListMember = ctx.Builder(structObj->Pos())
                .Callable("Coalesce")
                    .Add(0, flattenInfo.ListMember)
                    .Callable(1, "Dict")
                        .Add(0, ExpandType(structObj->Pos(), *flattenInfo.Type->Cast<TOptionalExprType>()->GetItemType(), ctx))
                    .Seal()
                .Seal()
                .Build();
        } else {
            isList = flattenInfo.Type->GetKind() == ETypeAnnotationKind::List;
            isDict = flattenInfo.Type->GetKind() == ETypeAnnotationKind::Dict;
        }

        if (isDict) {
            flattenInfo.ListMember = ctx.Builder(structObj->Pos())
                .Callable("DictItems")
                .Add(0, flattenInfo.ListMember)
                .Seal().Build();
        }

        if (!isDict && !isList) {
            flattenPriority.push_back(&flattenInfo);
        } else {
            flattenPriority.push_front(&flattenInfo);
        }
    }

    TString operation = "OrderedMap";
    for (const auto& infoPtr : flattenPriority) {
        const TFlattenInfo& flattenInfo = *infoPtr;
        auto mapLambda = ctx.NewLambda(node->Pos(), ctx.NewArguments(node->Pos(), { flattenInfo.ArgNode }), std::move(internalResult));
        internalResult = ctx.Builder(node->Pos())
            .Callable(operation)
            .Add(0, flattenInfo.ListMember)
            .Add(1, mapLambda)
            .Seal().Build();
        operation = "OrderedFlatMap";
    }
    return internalResult;
}

TExprNode::TPtr ExpandCastStruct(const TExprNode::TPtr& node, TExprContext& ctx) {
    YQL_CLOG(DEBUG, Core) << "Expand " << node->Content();
    auto targetType = node->Tail().GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TStructExprType>();
    TExprNode::TListType items;
    for (auto item : targetType->GetItems()) {
        auto nameAtom = ctx.NewAtom(node->Pos(), item->GetName());
        auto tuple = ctx.NewList(node->Pos(), {
            nameAtom, ctx.NewCallable(node->Pos(), "Member", { node->HeadPtr(), nameAtom }) });
        items.push_back(std::move(tuple));
    }

    return ctx.NewCallable(node->Pos(), "AsStruct", std::move(items));
}

void ExtractSimpleKeys(const TExprNode* keySelectorBody, const TExprNode* keySelectorArg, TVector<TStringBuf>& columns) {
    if (keySelectorBody->IsList()) {
        for (auto& child: keySelectorBody->Children()) {
            if (child->IsCallable("Member") && child->Child(0) == keySelectorArg) {
                columns.push_back(child->Child(1)->Content());
            } else {
                break;
            }
        }
    } else if (keySelectorBody->IsCallable("Member") && keySelectorBody->Child(0) == keySelectorArg) {
        columns.push_back(keySelectorBody->Child(1)->Content());
    }
}

void ExtractSimpleSortTraits(const TExprNode& sortDirections, const TExprNode& keySelectorLambda, TVector<bool>& dirs, TVector<TStringBuf>& columns) {
    auto keySelectorBody = keySelectorLambda.Child(1);
    auto keySelectorArg = keySelectorLambda.Child(0)->Child(0);

    const bool singleKey = sortDirections.IsCallable("Bool") || (sortDirections.IsList() && sortDirections.ChildrenSize() == 1);
    if (singleKey) {
        const auto item = keySelectorBody->IsList() && keySelectorBody->ChildrenSize() == 1
            ? keySelectorBody->Child(0)
            : keySelectorBody;
        if (item->IsCallable("Member") && item->Child(0) == keySelectorArg) {
            columns.push_back(item->Child(1)->Content());
        }
    } else {
        if (keySelectorBody->IsList()) {
            for (size_t i = 0; i < keySelectorBody->ChildrenSize(); ++i) {
                auto& key = keySelectorBody->Children()[i];
                if (key->IsCallable("Member") && key->Child(0) == keySelectorArg) {
                    columns.push_back(key->Child(1)->Content());
                } else {
                    break;
                }
            }
        }
    }

    if (sortDirections.IsList()) {
        for (size_t i = 0; i < sortDirections.ChildrenSize(); ++i) {
            auto& dir = sortDirections.Children()[i];
            if (dir->IsCallable("Bool")) {
                dirs.push_back(IsTrue(dir->Child(0)->Content()));
            } else {
                break;
            }
        }
    } else if (sortDirections.IsCallable("Bool")) {
        dirs.push_back(IsTrue(sortDirections.Child(0)->Content()));
    }
    size_t minSize = Min<size_t>(columns.size(), dirs.size());
    if (columns.size() > minSize) {
        columns.erase(columns.begin() + minSize, columns.end());
    }
    if (dirs.size() > minSize) {
        dirs.erase(dirs.begin() + minSize, dirs.end());
    }
}

const TExprNode& SkipCallables(const TExprNode& node, const std::initializer_list<std::string_view>& skipCallables) {
    const TExprNode* p = &node;
    while (p->IsCallable(skipCallables)) {
        p = &p->Head();
    }
    return *p;
}

namespace {
TExprNode::TPtr ApplyWithCastStructForFirstArg(const TExprNode::TPtr& node, const TTypeAnnotationNode& targetType, TExprContext& ctx) {
    YQL_ENSURE(node->IsLambda());
    TCoLambda lambda(node);
    YQL_ENSURE(lambda.Args().Size() >= 1);

    TPositionHandle pos = lambda.Pos();

    TExprNodeList args = lambda.Args().Ref().ChildrenList();
    TExprNode::TPtr body = lambda.Body().Ptr();

    auto newArg = ctx.NewArgument(pos, "row");
    auto cast = ctx.NewCallable(pos, "CastStruct", { newArg, ExpandType(pos, targetType, ctx) });

    body = ctx.ReplaceNodes(std::move(body), {{ args.front().Get(), cast }});
    args.front() = newArg;

    auto result = ctx.NewLambda(pos, ctx.NewArguments(pos, std::move(args)), std::move(body));
    return ctx.DeepCopyLambda(*result);
}

}

void ExtractSortKeyAndOrder(TPositionHandle pos, const TExprNode::TPtr& sortTraitsNode, TExprNode::TPtr& sortKey, TExprNode::TPtr& sortOrder, TExprContext& ctx) {
    if (sortTraitsNode->IsCallable("SortTraits")) {
        TCoSortTraits sortTraits(sortTraitsNode);
        auto lambdaInputType =
            sortTraits.ListType().Ref().GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TListExprType>()->GetItemType();
        sortOrder = sortTraits.SortDirections().Ptr();
        sortKey = ApplyWithCastStructForFirstArg(sortTraits.SortKeySelectorLambda().Ptr(), *lambdaInputType, ctx);
    } else {
        YQL_ENSURE(sortTraitsNode->IsCallable("Void"));
        sortOrder = sortKey = ctx.NewCallable(pos, "Void", {});
    }
}

void ExtractSessionWindowParams(TPositionHandle pos, const TExprNode::TPtr& sessionTraits, TExprNode::TPtr& sessionKey,
    const TTypeAnnotationNode*& sessionKeyType, const TTypeAnnotationNode*& sessionParamsType, TExprNode::TPtr& sessionSortTraits, TExprNode::TPtr& sessionInit,
    TExprNode::TPtr& sessionUpdate, TExprContext& ctx)
{
    sessionKey = sessionSortTraits = sessionInit = sessionUpdate = {};
    sessionKeyType = nullptr;
    sessionParamsType = nullptr;

    if (sessionTraits && sessionTraits->IsCallable("SessionWindowTraits")) {
        TCoSessionWindowTraits swt(sessionTraits);
        auto lambdaInputType =
            swt.ListType().Ref().GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TListExprType>()->GetItemType();

        sessionKeyType = swt.Calculate().Ref().GetTypeAnn();

        TVector<const TItemExprType*> sessionParamItems;
        sessionParamItems.push_back(ctx.MakeType<TItemExprType>("start",  sessionKeyType));
        sessionParamItems.push_back(ctx.MakeType<TItemExprType>("state",  swt.InitState().Ref().GetTypeAnn()));
        sessionParamsType = ctx.MakeType<TStructExprType>(sessionParamItems);

        sessionSortTraits = swt.SortSpec().Ptr();
        sessionKey = ApplyWithCastStructForFirstArg(swt.Calculate().Ptr(), *lambdaInputType, ctx);
        sessionInit = ApplyWithCastStructForFirstArg(swt.InitState().Ptr(), *lambdaInputType, ctx);
        sessionUpdate = ApplyWithCastStructForFirstArg(swt.UpdateState().Ptr(), *lambdaInputType, ctx);
    } else {
        YQL_ENSURE(!sessionTraits || sessionTraits->IsCallable("Void"));
        sessionSortTraits = ctx.NewCallable(pos, "Void", {});
    }
}

TExprNode::TPtr BuildKeySelector(TPositionHandle pos, const TStructExprType& rowType, const TExprNode::TPtr& keyColumns, TExprContext& ctx) {
    auto keyExtractorArg = ctx.NewArgument(pos, "item");
    TExprNode::TListType tupleItems;
    for (auto column : keyColumns->Children()) {
        auto itemType = rowType.GetItems()[*rowType.FindItem(column->Content())]->GetItemType();
        auto keyValue = ctx.NewCallable(pos, "Member", { keyExtractorArg, column });
        if (RemoveOptionalType(itemType)->GetKind() != ETypeAnnotationKind::Data) {
            keyValue = ctx.NewCallable(pos, "StablePickle", { keyValue });
        }

        tupleItems.push_back(keyValue);
    }

    TExprNode::TPtr tuple;
    if (tupleItems.size() == 0) {
        tuple = ctx.Builder(pos).Callable("Uint32").Atom(0, "0").Seal().Build();
    } else if (tupleItems.size() == 1) {
        tuple = tupleItems[0];
    } else {
        tuple = ctx.NewList(pos, std::move(tupleItems));
    }
    return ctx.NewLambda(pos, ctx.NewArguments(pos, {keyExtractorArg}), std::move(tuple));
}

template <bool Cannonize, bool EnableNewOptimizers>
TExprNode::TPtr OptimizeIfPresent(const TExprNode::TPtr& node, TExprContext& ctx) {
    auto optionals = node->ChildrenList();
    optionals.resize(optionals.size() - 2U);
    const auto& lambda = *node->Child(optionals.size());
    if (lambda.Tail().GetDependencyScope()->second != &lambda) {
        YQL_CLOG(DEBUG, Core) << node->Content() << " where then branch isn't depended on optional";
        if (1U == optionals.size()) {
            return ctx.Builder(node->Pos())
                .Callable("If")
                    .Callable(0, "Exists")
                        .Add(0, std::move(optionals.front()))
                    .Seal()
                    .Add(1, lambda.TailPtr())
                    .Add(2, node->TailPtr())
                .Seal().Build();
        }

        std::for_each(optionals.begin(), optionals.end(), [&ctx](TExprNode::TPtr& node) {
            const auto p = node->Pos();
            node = ctx.NewCallable(p, "Exists", {std::move(node)});
        });
        return ctx.Builder(node->Pos())
            .Callable("If")
                .Callable(0, "And")
                    .Add(std::move(optionals))
                .Seal()
                .Add(1, lambda.TailPtr())
                .Add(2, node->TailPtr())
            .Seal().Build();
    }

    if (std::any_of(optionals.cbegin(), optionals.cend(), [](const TExprNode::TPtr& node) { return node->IsCallable("Nothing"); })) {
        YQL_CLOG(DEBUG, Core) << node->Content() << " over Nothing.";
        return node->TailPtr();
    }

    if (std::any_of(optionals.cbegin(), optionals.cend(), [](const TExprNode::TPtr& node) { return node->IsCallable("Just"); })) {
        YQL_CLOG(DEBUG, Core) << node->Content() << " over Just.";
        TExprNode::TListType args;
        args.reserve(optionals.size());
        std::for_each(optionals.begin(), optionals.end(), [&args](TExprNode::TPtr& arg) {
            if (arg->IsCallable("Just"))
                arg = arg->HeadPtr();
            else
                args.emplace_back(std::move(arg));
        });

        if (args.empty()) {
            TNodeOnNodeOwnedMap replaces(optionals.size());
            for (ui32 i = 0U; i < optionals.size(); ++i)
                replaces.emplace(lambda.Head().Child(i), std::move(optionals[i]));
            return ctx.ReplaceNodes(lambda.TailPtr(), replaces);
        }

        auto simplify = ctx.Builder(node->Pos())
            .Lambda()
                .Params("items", optionals.size() - args.size())
                .Apply(lambda)
                    .Do([&](TExprNodeReplaceBuilder& parent) -> TExprNodeReplaceBuilder& {
                        for (auto i = 0U, j = 0U; i < optionals.size(); ++i) {
                            if (auto opt = std::move(optionals[i]))
                                parent.With(i, std::move(opt));
                            else
                                parent.With(i, "items", j++);
                        }
                        return parent;
                    })
                .Seal()
            .Seal().Build();

        args.emplace_back(std::move(simplify));
        args.emplace_back(node->TailPtr());
        return ctx.ChangeChildren(*node, std::move(args));
    }

    if (3U == node->ChildrenSize()) {
        if (&lambda.Tail() == &lambda.Head().Head()) {
            YQL_CLOG(DEBUG, Core) << "Simplify " << node->Content() << " as Coalesce";
            return ctx.NewCallable(node->Pos(), "Coalesce", {node->HeadPtr(), node->TailPtr()});
        }

        if (const auto& input = node->Head(); IsTransparentIfPresent(input)) {
            YQL_CLOG(DEBUG, Core) << node->Content() << " over transparent " << input.Content();
            return ctx.Builder(node->Pos())
                .Callable(node->Content())
                    .Add(0, input.HeadPtr())
                    .Lambda(1)
                        .Param("item")
                        .Apply(*node->Child(1))
                            .With(0)
                                .ApplyPartial(input.Child(1)->HeadPtr(), input.Child(1)->Tail().HeadPtr())
                                    .With(0, "item")
                                .Seal()
                            .Done()
                        .Seal()
                    .Seal()
                    .Add(2, node->TailPtr())
                .Seal().Build();
        }

        if constexpr (Cannonize) {
            if (node->Tail().IsCallable("Nothing") && node->Tail().GetTypeAnn()->GetKind() != ETypeAnnotationKind::Pg) {
                YQL_CLOG(DEBUG, Core) << node->Content() << " with else " << node->Tail().Content();
                return ctx.NewCallable(node->Pos(), "FlatMap", { node->HeadPtr(), node->ChildPtr(1) });
            }
        }
    }

    if constexpr (!Cannonize && EnableNewOptimizers) {
        if (lambda.Tail().IsCallable("IfPresent") && &node->Tail() == &lambda.Tail().Tail() && lambda.Tail().Head().GetDependencyScope()->second != &lambda) {
            auto innerOptionals = lambda.Tail().ChildrenList();
            innerOptionals.resize(innerOptionals.size() - 2U);
            const auto find = std::find_if(innerOptionals.cbegin(), innerOptionals.cend(), [l = &lambda] (const TExprNode::TPtr& child) {
                return child->GetDependencyScope()->second == l;
            });
            const auto count = std::distance(innerOptionals.cbegin(), find);
            YQL_CLOG(DEBUG, Core) << node->Content() << " pull " << count <<  " arg(s) from inner " << lambda.Tail().Content();

            auto& innerLambda = *lambda.Tail().Child(innerOptionals.size());

            auto args = lambda.Head().ChildrenList();
            auto innerArgs = innerLambda.Head().ChildrenList();

            const auto splitter = [count](TExprNode::TListType& src, TExprNode::TListType& dst) {
                dst.reserve(dst.size() + count);
                auto it = src.begin();
                std::advance(it, count);
                std::move(src.begin(), it, std::back_inserter(dst));
                src.erase(src.cbegin(), it);
            };

            splitter(innerArgs, args);
            splitter(innerOptionals, optionals);

            auto body = innerLambda.TailPtr();
            if (!(innerArgs.empty() || innerOptionals.empty())) {
                innerOptionals.emplace_back(ctx.DeepCopyLambda(*ctx.NewLambda(innerLambda.Pos(), ctx.NewArguments(innerLambda.Head().Pos(), std::move(innerArgs)), std::move(body))));
                innerOptionals.emplace_back(lambda.Tail().TailPtr());
                body = ctx.ChangeChildren(lambda.Tail(), std::move(innerOptionals));
            }

            optionals.emplace_back(ctx.DeepCopyLambda(*ctx.NewLambda(lambda.Pos(), ctx.NewArguments(lambda.Head().Pos(), std::move(args)), std::move(body))));
            optionals.emplace_back(node->TailPtr());
            return ctx.ChangeChildren(*node, std::move(optionals));
        }
    }

    return node;
}

template TExprNode::TPtr OptimizeIfPresent<true, true>(const TExprNode::TPtr& node, TExprContext& ctx);
template TExprNode::TPtr OptimizeIfPresent<false, true>(const TExprNode::TPtr& node, TExprContext& ctx);
template TExprNode::TPtr OptimizeIfPresent<false, false>(const TExprNode::TPtr& node, TExprContext& ctx);

TExprNode::TPtr OptimizeExists(const TExprNode::TPtr& node, TExprContext& ctx)  {
    if (HasError(node->Head().GetTypeAnn(), ctx)) {
        return TExprNode::TPtr();
    }

    if (node->Head().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Void) {
        YQL_CLOG(DEBUG, Core) << node->Content() << " over " << node->Head().Content();
        return MakeBool<false>(node->Pos(), ctx);
    }

    if (node->Head().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Null) {
        YQL_CLOG(DEBUG, Core) << node->Content() << " over " << node->Head().Content();
        return MakeBool<false>(node->Pos(), ctx);
    }

    if (node->Head().IsCallable("Just")) {
        YQL_CLOG(DEBUG, Core) << node->Content() << " over " << node->Head().Content();
        return MakeBool<true>(node->Pos(), ctx);
    }

    if (node->Head().IsCallable("Nothing")) {
        YQL_CLOG(DEBUG, Core) << node->Content() << " over " << node->Head().Content();
        return MakeBool<false>(node->Pos(), ctx);
    }

    if (node->Head().GetTypeAnn()->GetKind() != ETypeAnnotationKind::Optional &&
        node->Head().GetTypeAnn()->GetKind() != ETypeAnnotationKind::Pg) {
        YQL_CLOG(DEBUG, Core) << node->Content() << " over non-optional";
        return MakeBool<true>(node->Pos(), ctx);
    }

    if (const auto& input = node->Head(); IsTransparentIfPresent(input)) {
        YQL_CLOG(DEBUG, Core) << node->Content() << " over transparent " << input.Content();
        return ctx.ChangeChildren(*node, {input.HeadPtr()});
    }

    return node;
}

bool WarnUnroderedSubquery(const TExprNode& unourderedSubquery, TExprContext& ctx) {
    YQL_ENSURE(unourderedSubquery.IsCallable("UnorderedSubquery"));

    auto issueCode = EYqlIssueCode::TIssuesIds_EIssueCode_YQL_ORDER_BY_WITHOUT_LIMIT_IN_SUBQUERY;
    auto issue = TIssue(ctx.GetPosition(unourderedSubquery.Head().Pos()), "ORDER BY in subquery will be ignored");
    auto subIssue = TIssue(ctx.GetPosition(unourderedSubquery.Pos()), "When used here");
    SetIssueCode(issueCode, issue);
    SetIssueCode(issueCode, subIssue);
    issue.AddSubIssue(MakeIntrusive<TIssue>(subIssue));

    return ctx.AddWarning(issue);
}

IGraphTransformer::TStatus LocalUnorderedOptimize(TExprNode::TPtr input, TExprNode::TPtr& output, const std::function<bool(const TExprNode*)>& stopTraverse, TExprContext& ctx, TTypeAnnotationContext* typeCtx) {
    output = input;
    TProcessedNodesSet processedNodes;
    bool hasUnordered = false;
    VisitExpr(input, [&stopTraverse, &processedNodes, &hasUnordered](const TExprNode::TPtr& node) {
        if (stopTraverse(node.Get())) {
            processedNodes.insert(node->UniqueId());
            return false;
        }
        else if (TCoUnorderedBase::Match(node.Get())) {
            hasUnordered = true;
        }
        return true;
    });
    if (!hasUnordered) {
        return IGraphTransformer::TStatus::Ok;
    }

    TOptimizeExprSettings settings(typeCtx);
    settings.ProcessedNodes = &processedNodes; // Prevent optimizer to go deeper

    static THashSet<TStringBuf> CALLABLE = {"AssumeUnique",
        "Map", "OrderedMap",
        "Filter", "OrderedFilter",
        "FlatMap", "OrderedFlatMap",
        "FoldMap", "Fold1Map", "Chain1Map",
        "Take", "Skip",
        "TakeWhile", "SkipWhile",
        "TakeWhileInclusive", "SkipWhileInclusive",
        "SkipNullMembers", "FilterNullMembers",
        "SkipNullElements", "FilterNullElements",
        "Condense", "Condense1",
        "MapJoinCore", "CommonJoinCore",
        "CombineCore", "ExtractMembers",
        "PartitionByKey", "PartitionsByKeys",
        "FromFlow", "ToFlow", "Collect", "Iterator"};
    static THashSet<TStringBuf> SORTED = {"AssumeSorted", "Sort"};

    auto status = OptimizeExpr(input, output, [&](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
        if (TCoUnorderedBase::Match(node.Get())) {
            if (node->Head().IsCallable(CALLABLE)) {
                auto res = ctx.SwapWithHead(*node);
                auto name = res->Content();
                if (name.SkipPrefix("Ordered")) {
                    res = ctx.RenameNode(*res, name);
                }
                return res;
            }
            if (node->Head().IsCallable(SORTED)) {
                return node->Head().HeadPtr();
            }
        }

        return node;
    }, ctx, settings);

    if (status.Level == IGraphTransformer::TStatus::Error) {
        return status;
    }

    if (input != output && input->IsLambda()) {
        output = ctx.DeepCopyLambda(*output);
        status = status.Combine(IGraphTransformer::TStatus::Repeat);
    }

    return status;

}

std::pair<TExprNode::TPtr, TExprNode::TPtr> ReplaceDependsOn(TExprNode::TPtr lambda, TExprContext& ctx, TTypeAnnotationContext* typeCtx) {
    auto placeHolder = ctx.NewArgument(lambda->Pos(), "placeholder");

    auto status = OptimizeExpr(lambda, lambda, [&placeHolder, arg = &lambda->Head().Head()](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
        if (TCoDependsOn::Match(node.Get()) && &node->Head() == arg) {
            return ctx.ChangeChild(*node, 0, TExprNode::TPtr(placeHolder));
        }
        return node;
    }, ctx, TOptimizeExprSettings{typeCtx});

    if (status.Level == IGraphTransformer::TStatus::Error) {
        return std::pair<TExprNode::TPtr, TExprNode::TPtr>{};
    }

    return {placeHolder, lambda};
}

TStringBuf GetEmptyCollectionName(ETypeAnnotationKind kind) {
    switch (kind) {
        case ETypeAnnotationKind::Flow:
        case ETypeAnnotationKind::Stream:   return "EmptyIterator";
        case ETypeAnnotationKind::List:     return "List";
        case ETypeAnnotationKind::Optional: return "Nothing";
        case ETypeAnnotationKind::Dict:     return "Dict";
        case ETypeAnnotationKind::Pg:       return "Nothing";
        default: break;
    }
    return {};
}

}
