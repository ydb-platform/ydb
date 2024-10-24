#include "yql_opt_utils.h"
#include "yql_expr_optimize.h"
#include "yql_expr_type_annotation.h"
#include "yql_type_annotation.h"
#include "yql_type_helpers.h"

#include <ydb/library/yql/ast/yql_constraint.h>
#include <ydb/library/yql/parser/pg_catalog/catalog.h>
#include <ydb/library/yql/utils/log/log.h>

#include <util/generic/set.h>
#include <util/string/type.h>

#include <limits>

namespace NYql {

using namespace NNodes;

namespace {

template<bool Distinct>
TExprNode::TPtr KeepUniqueConstraint(TExprNode::TPtr node, const TExprNode& src, TExprContext& ctx) {
    if (const auto constraint = src.GetConstraint<TUniqueConstraintNodeBase<Distinct>>()) {
        const auto pos = node->Pos();
        TExprNode::TListType children(1U, std::move(node));
        for (const auto& sets : constraint->GetContent()) {
            TExprNode::TListType lists;
            lists.reserve(sets.size());
            for (const auto& set : sets) {
                TExprNode::TListType columns;
                columns.reserve(set.size());
                for (const auto& path : set) {
                    if (1U == path.size())
                        columns.emplace_back(ctx.NewAtom(pos, path.front()));
                    else {
                        TExprNode::TListType atoms(path.size());
                        std::transform(path.cbegin(), path.cend(), atoms.begin(), [&](const std::string_view& name) { return ctx.NewAtom(pos, name); });
                        columns.emplace_back(ctx.NewList(pos, std::move(atoms)));
                    }
                }
                lists.emplace_back(ctx.NewList(pos, std::move(columns)));
            }
            children.emplace_back(ctx.NewList(pos, std::move(lists)));
        }
        return ctx.NewCallable(pos, TString("Assume") += TUniqueConstraintNodeBase<Distinct>::Name(), std::move(children));
    }
    return node;
}

TExprNode::TPtr KeepChoppedConstraint(TExprNode::TPtr node, const TExprNode& src, TExprContext& ctx) {
    if (const auto constraint = src.GetConstraint<TChoppedConstraintNode>()) {
        const auto pos = node->Pos();
        TExprNode::TListType children(1U, std::move(node));
        for (const auto& set : constraint->GetContent()) {
            TExprNode::TListType columns;
            columns.reserve(set.size());
            for (const auto& path : set) {
                if (1U == path.size())
                    columns.emplace_back(ctx.NewAtom(pos, path.front()));
                else {
                    TExprNode::TListType atoms(path.size());
                    std::transform(path.cbegin(), path.cend(), atoms.begin(), [&](const std::string_view& name) { return ctx.NewAtom(pos, name); });
                    columns.emplace_back(ctx.NewList(pos, std::move(atoms)));
                }
            }
            children.emplace_back(ctx.NewList(pos, std::move(columns)));
        }
        return ctx.NewCallable(pos, TString("Assume") += TChoppedConstraintNode::Name(), std::move(children));
    }
    return node;
}

TExprNodeBuilder GetterBuilder(TExprNodeBuilder parent, ui32 index, const TTypeAnnotationNode& type, TPartOfConstraintBase::TPathType path) {
    if (path.empty())
        return parent.Arg(index, "item");

    const auto& name = path.back();
    path.pop_back();
    const auto parentType = TPartOfConstraintBase::GetSubTypeByPath(path, type);
    YQL_ENSURE(parentType, "Wrong path '" << path << "' to component of: " << type);
    return GetterBuilder(parent.Callable(index, ETypeAnnotationKind::Struct == parentType->GetKind() ? "Member" : "Nth"), 0U, type, path).Atom(1, name).Seal();
}

const TExprNode& GetLiteralStructMember(const TExprNode& literal, const TExprNode& member) {
    for (const auto& child : literal.Children())
        if (&child->Head() == &member || child->Head().Content() == member.Content())
            return child->Tail();
    ythrow yexception() << "Member '" << member.Content() << "' not found in literal struct.";
}

}

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

TExprNode::TPtr MakePgBool(TPositionHandle position, bool value, TExprContext& ctx) {
    return ctx.NewCallable(position, "PgConst", {
        ctx.NewAtom(position, value ? "t" : "f", TNodeFlags::Default),
        ctx.NewCallable(position, "PgType", { ctx.NewAtom(position, "bool")})
     });
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
    return (node.IsCallable("FlatMap") || (3U == node.ChildrenSize() && node.IsCallable("IfPresent") && node.Tail().IsCallable({"Nothing", "EmptyFrom"})))
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

// Check if the flat map is a simple rename flat map
bool IsRenameFlatMap(const NNodes::TCoFlatMapBase& node, TExprNode::TPtr& structNode) {

    auto lambda = node.Lambda();
    if (!IsJustOrSingleAsList(lambda.Body().Ref())) {
        return false;
    }

    structNode = lambda.Body().Ref().Child(0);

    auto asStruct = TExprBase(structNode);
    if (!asStruct.Maybe<TCoAsStruct>()) {
        return false;
    }

    for (auto child : asStruct.Cast<TCoAsStruct>()) {

        if (!child.Item(1).Maybe<TCoMember>()) {
            return false;
        }

        auto member = child.Item(1).Cast<TCoMember>();
        if(member.Struct().Raw() != lambda.Args().Arg(0).Raw()) {
            return false;
        }
    }

    return true;
}

// Check if the flat map is a simple rename flat map and compute the mapping from new names to original ones
bool IsRenameFlatMapWithMapping(const NNodes::TCoFlatMapBase& node, TExprNode::TPtr& structNode,
    THashMap<TString, TString> & mapping) {

    auto lambda = node.Lambda();
    if (!IsJustOrSingleAsList(lambda.Body().Ref())) {
        return false;
    }

    structNode = lambda.Body().Ref().Child(0);

    auto asStruct = TExprBase(structNode);
    if (!asStruct.Maybe<TCoAsStruct>()) {
        return false;
    }

    for (auto child : asStruct.Cast<TCoAsStruct>()) {

        if (!child.Item(1).Maybe<TCoMember>()) {
            return false;
        }

        auto member = child.Item(1).Cast<TCoMember>();
        if(member.Struct().Raw() != lambda.Args().Arg(0).Raw()) {
            return false;
        }

        auto to = child.Item(0).Cast<TCoAtom>();
        auto from = member.Name();

        if (to != from){
            mapping[to.StringValue()] = from.StringValue();
        }
    }

    return true;
}

// Check if the flat map is a simple rename flat map or a flatmap that also computes some
// values in 1-1 fashion
bool IsRenameOrApplyFlatMapWithMapping(const NNodes::TCoFlatMapBase& node, TExprNode::TPtr& structNode,
    THashMap<TString, TString> & mapping, TSet<TString> & apply) {

    auto lambda = node.Lambda();
    if (!IsJustOrSingleAsList(lambda.Body().Ref())) {
        return false;
    }

    structNode = lambda.Body().Ref().Child(0);

    auto asStruct = TExprBase(structNode);
    if (!asStruct.Maybe<TCoAsStruct>()) {
        return false;
    }

    for (auto child : asStruct.Cast<TCoAsStruct>()) {

        if (!child.Item(1).Maybe<TCoMember>()) {
            apply.insert(child.Item(0).Cast<TCoAtom>().StringValue());
            continue;
        }

        auto member = child.Item(1).Cast<TCoMember>();
        if(member.Struct().Raw() != lambda.Args().Arg(0).Raw()) {
            return false;
        }

        auto to = child.Item(0).Cast<TCoAtom>();
        auto from = member.Name();

        if (to != from){
            mapping[to.StringValue()] = from.StringValue();
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

    return KeepColumnOrder(*columnOrder, node, ctx);
}

TExprNode::TPtr KeepColumnOrder(const TColumnOrder& order, const TExprNode::TPtr& node, TExprContext& ctx) {
    return ctx.Builder(node->Pos())
        .Callable("AssumeColumnOrder")
            .Add(0, node)
            .List(1)
                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                    size_t index = 0;
                    for (auto& [col, gen_col] : order) {
                        parent
                            .Atom(index++, gen_col);
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

template bool HaveFieldsSubset(const TExprNode::TPtr& start, const TExprNode& arg, TSet<TStringBuf>& usedFields, const TParentsMap& parentsMap,
                            bool allowDependsOn);
template bool HaveFieldsSubset(const TExprNode::TPtr& start, const TExprNode& arg, TSet<TString>& usedFields, const TParentsMap& parentsMap,
                            bool allowDependsOn);
template bool HaveFieldsSubset(const TExprNode::TPtr& start, const TExprNode& arg, std::map<std::string_view, TExprNode::TPtr>& usedFields,
                            const TParentsMap& parentsMap, bool allowDependsOn);

TExprNode::TPtr AddMembersUsedInside(const TExprNode::TPtr& start, const TExprNode& arg, TExprNode::TPtr&& members, const TParentsMap& parentsMap, TExprContext& ctx) {
    if (!members || !start || &arg == start.Get()) {
        return {};
    }

    if (RemoveOptionalType(arg.GetTypeAnn())->GetKind() != ETypeAnnotationKind::Struct) {
        return {};
    }

    if (!IsDepended(*start, arg)) {
        return std::move(members);
    }

    TNodeSet nodes;
    VisitExpr(start, [&](const TExprNode::TPtr& node) {
        if (!node->IsCallable("DependsOn"))
            nodes.emplace(node.Get());
        return true;
    });

    std::unordered_set<std::string_view> names(members->ChildrenSize());
    members->ForEachChild([&names](const TExprNode& name){ names.emplace(name.Content()); });


    const auto parents = parentsMap.find(&arg);
    YQL_ENSURE(parents != parentsMap.cend());
    TExprNode::TListType extra;
    for (const auto& parent : parents->second) {
        if (nodes.cend() == nodes.find(parent))
            continue;
        if (!parent->IsCallable("Member"))
            return {};
        if (names.emplace(parent->Tail().Content()).second)
            extra.emplace_back(parent->TailPtr());
    }


    if (!extra.empty()) {
        auto children = members->ChildrenList();
        std::move(extra.begin(), extra.end(), std::back_inserter(children));
        members = ctx.ChangeChildren(*members, std::move(children));
    }

    return std::move(members);
}

template<class TFieldsSet>
TExprNode::TPtr FilterByFields(TPositionHandle position, const TExprNode::TPtr& input, const TFieldsSet& subsetFields,
    TExprContext& ctx, bool singleValue) {
    TExprNode::TListType fields;
    fields.reserve(subsetFields.size());
    for (const auto& x : subsetFields) {
        fields.emplace_back(ctx.NewAtom(position, x));
    }

    return ctx.NewCallable(position, singleValue ? "FilterMembers" : "ExtractMembers", { input, ctx.NewList(position, std::move(fields)) });
}

template TExprNode::TPtr FilterByFields(TPositionHandle position, const TExprNode::TPtr& input, const TSet<TStringBuf>& subsetFields, TExprContext& ctx, bool singleValue);
template TExprNode::TPtr FilterByFields(TPositionHandle position, const TExprNode::TPtr& input, const TSet<TString>& subsetFields, TExprContext& ctx, bool singleValue);


bool IsDependedImpl(const TExprNode* from, const TExprNode* to, TNodeMap<bool>& deps) {
    if (from == to)
        return true;

    auto [it, inserted] = deps.emplace(from, false);
    if (!inserted) {
        return it->second;
    }

    for (const auto& child : from->Children()) {
        if (IsDependedImpl(child.Get(), to, deps)) {
            return it->second = true;
        }
    }

    return false;
}

bool IsDepended(const TExprNode& from, const TExprNode& to) {
    TNodeMap<bool> deps;
    return IsDependedImpl(&from, &to, deps);
}

bool MarkDepended(const TExprNode& from, const TExprNode& to, TNodeMap<bool>& deps) {
    return IsDependedImpl(&from, &to, deps);
}

bool IsEmpty(const TExprNode& node, const TTypeAnnotationContext& typeCtx) {
    return typeCtx.IsConstraintCheckEnabled<TEmptyConstraintNode>() && node.Type() != TExprNode::Argument && node.GetConstraint<TEmptyConstraintNode>() != nullptr;
}

bool IsEmptyContainer(const TExprNode& node) {
    return node.IsCallable({"EmptyList", "EmptyDict"})
        || (1U == node.ChildrenSize() && node.IsCallable({"List", "Nothing", "EmptyIterator", "Dict", "EmptyFrom"}));
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

TExprNode::TPtr GetSetting(const TExprNode& settings, const TStringBuf& name) {
    for (auto& setting : settings.Children()) {
        if (setting->ChildrenSize() != 0 && setting->Child(0)->Content() == name) {
            return setting;
        }
    }
    return nullptr;
}

TExprNode::TPtr FilterSettings(const TExprNode& settings, const THashSet<TStringBuf>& names, TExprContext& ctx) {
    TExprNode::TListType children;
    for (auto setting : settings.Children()) {
        if (setting->ChildrenSize() != 0 && names.contains(setting->Head().Content())) {
            children.push_back(setting);
        }
    }

    return ctx.NewList(settings.Pos(), std::move(children));
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
    auto newSetting = value ? ctx.NewList(pos, { ctx.NewAtom(pos, name), value }) : ctx.NewList(pos, { ctx.NewAtom(pos, name) });
    return ReplaceSetting(settings, newSetting, ctx);
}

TExprNode::TPtr ReplaceSetting(const TExprNode& settings, const TExprNode::TPtr& newSetting, TExprContext& ctx) {
    TExprNode::TListType newChildren;
    const TStringBuf name = newSetting->Head().Content();
    for (auto setting : settings.Children()) {
        if (setting->ChildrenSize() != 0 && setting->Head().Content() == name) {
            continue;
        }

        newChildren.push_back(setting);
    }

    newChildren.push_back(newSetting);

    auto ret = ctx.NewList(settings.Pos(), std::move(newChildren));
    return ret;
}

TExprNode::TPtr AddSetting(const TExprNode& settings, TPositionHandle pos, const TString& name, const TExprNode::TPtr& value, TExprContext& ctx) {
    auto newSetting = value ? ctx.NewList(pos, { ctx.NewAtom(pos, name), value }) : ctx.NewList(pos, { ctx.NewAtom(pos, name) });
    return AddSetting(settings, newSetting, ctx);
}

TExprNode::TPtr AddSetting(const TExprNode& settings, const TExprNode::TPtr& newSetting, TExprContext& ctx) {
    auto newChildren = settings.ChildrenList();
    newChildren.push_back(newSetting);

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

TMaybe<TIssue> ParseToDictSettings(const TExprNode& input, TExprContext& ctx, TMaybe<EDictType>& type, TMaybe<bool>& isMany, TMaybe<ui64>& itemsCount, bool& isCompact) {
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
                type = EDictType::Sorted;
            }
            else if (child->Content() == "Hashed") {
                type = EDictType::Hashed;
            }
            else if (child->Content() == "Auto") {
                type = EDictType::Auto;
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

    if (!type || !isMany) {
        return TIssue(ctx.GetPosition(input.Pos()), TStringBuilder() << "Both options must be specified: Sorted/Hashed/Auto and Many/One");
    }

    return TMaybe<TIssue>();
}

EDictType SelectDictType(EDictType type, const TTypeAnnotationNode* keyType) {
    if (type != EDictType::Auto) {
        return type;
    }

    if (keyType->IsHashable() && keyType->IsEquatable()) {
        return EDictType::Hashed;
    }

    YQL_ENSURE(keyType->IsComparableInternal());
    return EDictType::Sorted;
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
        Y_DEBUG_ABORT_UNLESS(nodeType || !"Unset node type for UpdateStructMembers");
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

TExprNode::TPtr ExpandRemoveMembers(const TExprNode::TPtr& node, TExprContext& ctx) {
    const auto& membersToRemove = node->Child(1);
    MemberUpdaterFunc removeFunc = [&membersToRemove](TString& memberName, const TTypeAnnotationNode*) {
        for (const auto& x : membersToRemove->Children()) {
            if (memberName == x->Content()) {
                return false;
            }
        }

        return true;
    };

    TExprNode::TListType members;
    if (!UpdateStructMembers(ctx, node->ChildPtr(0), node->Content(), members, removeFunc)) {
        return node->ChildPtr(0);
    }
    return ctx.NewCallable(node->Pos(), "AsStruct", std::move(members));
}

TExprNode::TPtr ExpandRemovePrefixMembers(const TExprNode::TPtr& node, TExprContext& ctx) {
    YQL_CLOG(DEBUG, Core) << "Expand " << node->Content();

    const TTypeAnnotationNode* targetItemType = GetSeqItemType(node->GetTypeAnn());
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

TExprNode::TListType GetOptionals(const TPositionHandle& pos, const TStructExprType& type, TExprContext& ctx) {
    TExprNode::TListType result;
    for (const auto& item : type.GetItems())
        if (ETypeAnnotationKind::Optional == item->GetItemType()->GetKind())
            result.emplace_back(ctx.NewAtom(pos, item->GetName()));
    return result;
}

TExprNode::TListType GetOptionals(const TPositionHandle& pos, const TTupleExprType& type, TExprContext& ctx) {
    TExprNode::TListType result;
    if (const auto& items = type.GetItems(); !items.empty())
        for (ui32 i = 0U; i < items.size(); ++i)
            if (ETypeAnnotationKind::Optional == items[i]->GetKind())
                result.emplace_back(ctx.NewAtom(pos, i));
    return result;
}

TExprNode::TPtr ExpandSkipNullFields(const TExprNode::TPtr& node, TExprContext& ctx) {
    YQL_ENSURE(node->IsCallable({"SkipNullMembers", "SkipNullElements"}));
    YQL_CLOG(DEBUG, Core) << "Expand " << node->Content();
    const bool isTuple = node->IsCallable("SkipNullElements");
    TExprNode::TListType fields;
    if (node->ChildrenSize() > 1) {
        fields = node->Child(1)->ChildrenList();
    } else if (isTuple) {
        fields = GetOptionals(node->Pos(), *GetSeqItemType(node->Head().GetTypeAnn())->Cast<TTupleExprType>(), ctx);
    } else {
        fields = GetOptionals(node->Pos(), *GetSeqItemType(node->Head().GetTypeAnn())->Cast<TStructExprType>(), ctx);
    }
    if (fields.empty()) {
        return node->HeadPtr();
    }
    return ctx.Builder(node->Pos())
        .Callable("OrderedFilter")
            .Add(0, node->HeadPtr())
            .Lambda(1)
                .Param("item")
                .Callable("And")
                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                        for (ui32 i = 0U; i < fields.size(); ++i) {
                            parent
                                .Callable(i, "Exists")
                                    .Callable(0, isTuple ? "Nth" : "Member")
                                        .Arg(0, "item")
                                        .Add(1, std::move(fields[i]))
                                    .Seal()
                                .Seal();
                        }
                        return parent;
                    })
                .Seal()
            .Seal()
        .Seal().Build();
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

void ExtractSortKeyAndOrder(TPositionHandle pos, const TExprNode::TPtr& sortTraitsNode, TSortParams& sortParams, TExprContext& ctx)
{
    ExtractSortKeyAndOrder(pos, sortTraitsNode, sortParams.Key, sortParams.Order, ctx);
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

void ExtractSessionWindowParams(TPositionHandle pos, TSessionWindowParams& sessionParams, TExprContext& ctx)
{
    ExtractSessionWindowParams(pos, sessionParams.Traits, sessionParams.Key,
        sessionParams.KeyType, sessionParams.ParamsType, sessionParams.SortTraits, sessionParams.Init,
        sessionParams.Update, ctx);
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
        tuple = ctx.Builder(pos).Callable("Uint32").Atom(0, 0U).Seal().Build();
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

    if (std::any_of(optionals.cbegin(), optionals.cend(), [](const TExprNode::TPtr& node) { return node->IsCallable({"Nothing","EmptyFrom"}); })) {
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
                .Params("items", args.size())
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

        if (lambda.Tail().IsCallable({"SafeCast", "StrictCast"}) && node->Tail().IsCallable({"Nothing","EmptyFrom"}) && &lambda.Tail().Head() == &lambda.Head().Head() &&
            ETypeAnnotationKind::Optional != node->Head().GetTypeAnn()->Cast<TOptionalExprType>()->GetItemType()->GetKind()) {
            YQL_CLOG(DEBUG, Core) << "Drop " << node->Content() << " with " << lambda.Tail().Content() << " and " << node->Tail().Content();
            return ctx.ChangeChild(lambda.Tail(), 0U, node->HeadPtr());
        }

        if constexpr (Cannonize) {
            if (node->Tail().IsCallable({"Nothing","EmptyFrom"}) && node->Tail().GetTypeAnn()->GetKind() != ETypeAnnotationKind::Pg) {
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

    if (node->Head().IsCallable({"Just", "PgConst"})) {
        YQL_CLOG(DEBUG, Core) << node->Content() << " over " << node->Head().Content();
        return MakeBool<true>(node->Pos(), ctx);
    }

    if (node->Head().IsCallable({"Nothing","EmptyFrom"})) {
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

namespace {

constexpr ui64 MaxWeight = std::numeric_limits<ui64>::max();
constexpr ui64 UnknownWeight = std::numeric_limits<ui32>::max();

ui64 GetTypeWeight(const TTypeAnnotationNode& type) {
    switch (type.GetKind()) {
        case ETypeAnnotationKind::Data:
            switch (type.Cast<TDataExprType>()->GetSlot()) {
                case NUdf::EDataSlot::Bool:
                case NUdf::EDataSlot::Int8:
                case NUdf::EDataSlot::Uint8: return 1;

                case NUdf::EDataSlot::Int16:
                case NUdf::EDataSlot::Uint16:
                case NUdf::EDataSlot::Date: return 2;

                case NUdf::EDataSlot::TzDate: return 3;

                case NUdf::EDataSlot::Int32:
                case NUdf::EDataSlot::Uint32:
                case NUdf::EDataSlot::Float:
                case NUdf::EDataSlot::Date32:
                case NUdf::EDataSlot::Datetime: return 4;

                case NUdf::EDataSlot::TzDatetime: return 5;

                case NUdf::EDataSlot::Int64:
                case NUdf::EDataSlot::Uint64:
                case NUdf::EDataSlot::Double:
                case NUdf::EDataSlot::Datetime64:
                case NUdf::EDataSlot::Timestamp64:
                case NUdf::EDataSlot::Interval64:
                case NUdf::EDataSlot::Timestamp:
                case NUdf::EDataSlot::Interval:  return 8;

                case NUdf::EDataSlot::TzTimestamp: return 9;

                case NUdf::EDataSlot::Decimal: return 15;
                case NUdf::EDataSlot::Uuid: return 16;

                default: return 32;
            }
        case ETypeAnnotationKind::Optional:
            return 1 + GetTypeWeight(*type.Cast<TOptionalExprType>()->GetItemType());
        case ETypeAnnotationKind::Pg: {
            const auto& typeDesc = NPg::LookupType(type.Cast<TPgExprType>()->GetId());
            if (typeDesc.PassByValue) {
                return typeDesc.TypeLen;
            }
            return UnknownWeight;
        }
        default:
            return UnknownWeight;
    }
}

} // namespace

const TItemExprType* GetLightColumn(const TStructExprType& type) {
    ui64 weight = MaxWeight;
    const TItemExprType* field = nullptr;
    for (const auto& item : type.GetItems()) {
        if (const auto w = GetTypeWeight(*item->GetItemType()); w < weight) {
            weight = w;
            field = item;
        }
    }
    return field;
}

TVector<TStringBuf> GetCommonKeysFromVariantSelector(const NNodes::TCoLambda& lambda) {
    if (auto maybeVisit = lambda.Body().Maybe<TCoVisit>()) {
        if (maybeVisit.Input().Raw() == lambda.Args().Arg(0).Raw()) {
            TVector<TStringBuf> members;
            for (ui32 index = 1; index < maybeVisit.Raw()->ChildrenSize(); ++index) {
                if (maybeVisit.Raw()->Child(index)->IsAtom()) {
                    ++index;
                    auto visitLambda = maybeVisit.Raw()->Child(index);
                    auto arg = visitLambda->Child(0)->Child(0);

                    TVector<TStringBuf> visitMembers;
                    if (TMaybeNode<TCoMember>(visitLambda->Child(1)).Struct().Raw() == arg) {
                        visitMembers.push_back(TCoMember(visitLambda->Child(1)).Name().Value());
                    }
                    else if (auto maybeList = TMaybeNode<TExprList>(visitLambda->Child(1))) {
                        for (auto item: maybeList.Cast()) {
                            if (item.Maybe<TCoMember>().Struct().Raw() == arg) {
                                visitMembers.push_back(item.Cast<TCoMember>().Name().Value());
                            } else {
                                return {};
                            }
                        }
                    }
                    if (visitMembers.empty()) {
                        return {};
                    } else if (members.empty()) {
                        members = visitMembers;
                    } else if (members != visitMembers) {
                        return {};
                    }
                } else {
                    return {};
                }
            }
            return members;
        }
        return {};
    }
    return {};
}

bool IsIdentityLambda(const TExprNode& lambda) {
    return lambda.IsLambda() && lambda.Head().ChildrenSize() == 1 && &lambda.Head().Head() == &lambda.Tail();
}

TExprNode::TPtr MakeExpandMap(TPositionHandle pos, const TVector<TString>& columns, const TExprNode::TPtr& input, TExprContext& ctx) {
    return ctx.Builder(pos)
        .Callable("ExpandMap")
            .Add(0, input)
            .Lambda(1)
                .Param("item")
                .Do([&](TExprNodeBuilder& lambda) -> TExprNodeBuilder& {
                    ui32 i = 0U;
                    for (const auto& col : columns) {
                        lambda.Callable(i++, "Member")
                            .Arg(0, "item")
                            .Atom(1, col)
                        .Seal();
                    }
                    return lambda;
                })
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr MakeNarrowMap(TPositionHandle pos, const TVector<TString>& columns, const TExprNode::TPtr& input, TExprContext& ctx) {
    return ctx.Builder(pos)
        .Callable("NarrowMap")
            .Add(0, input)
            .Lambda(1)
                .Params("fields", columns.size())
                .Callable("AsStruct")
                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                        ui32 i = 0U;
                        for (const auto& col : columns) {
                            parent.List(i)
                                .Atom(0, col)
                                .Arg(1, "fields", i)
                            .Seal();
                            ++i;
                        }
                        return parent;
                    })
                .Seal()
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr FindNonYieldTransparentNodeImpl(const TExprNode::TPtr& root, const bool udfSupportsYield, const TNodeSet& flowSources) {
    auto depensOnFlow = [&flowSources](const TExprNode::TPtr& node) {
        return !!FindNode(node,
            [](const TExprNode::TPtr& n) {
                return !TCoDependsOn::Match(n.Get());
            },
            [&flowSources](const TExprNode::TPtr& n) {
                return flowSources.contains(n.Get());
            }
        );
    };

    auto candidates = FindNodes(root,
        [&flowSources](const TExprNode::TPtr& node) {
            if (flowSources.contains(node.Get()) || TCoDependsOn::Match(node.Get())) {
                return false;
            }
            if (node->ChildrenSize() > 0 && node->Head().GetTypeAnn()->GetKind() == ETypeAnnotationKind::World) {
                return false;
            }
            return true;
        },
        [](const TExprNode::TPtr& node) {
            return TCoCollect::Match(node.Get())
                || TCoForwardList::Match(node.Get())
                || TCoApply::Match(node.Get())
                || TCoSwitch::Match(node.Get())
                || node->IsCallable("DqReplicate")
                || TCoPartitionsByKeys::Match(node.Get());
        }
    );

    for (auto candidate: candidates) {
        if (TCoCollect::Match(candidate.Get()) || TCoForwardList::Match(candidate.Get())) {
            if (depensOnFlow(candidate->HeadPtr())) {
                return candidate;
            }
        } else if (TCoApply::Match(candidate.Get())) {
            if (AnyOf(candidate->Children().begin() + 1, candidate->Children().end(), depensOnFlow)) {
                if (!IsFlowOrStream(*candidate)) {
                    while (TCoApply::Match(candidate.Get())) {
                        candidate = candidate->HeadPtr();
                    }
                    return candidate;
                }
                if (!udfSupportsYield) {
                    while (TCoApply::Match(candidate.Get())) {
                        candidate = candidate->HeadPtr();
                    }
                    if (TCoScriptUdf::Match(candidate.Get())) {
                        return candidate;
                    }
                }
            }
        } else if (TCoSwitch::Match(candidate.Get())) {
            for (size_t i = 3; i < candidate->ChildrenSize(); i += 2) {
                if (auto node = FindNonYieldTransparentNodeImpl(candidate->Child(i)->TailPtr(), udfSupportsYield, TNodeSet{&candidate->Child(i)->Head().Head()})) {
                    return node;
                }
            }
        } else if (candidate->IsCallable("DqReplicate")) {
            for (size_t i = 1; i < candidate->ChildrenSize(); ++i) {
                if (auto node = FindNonYieldTransparentNodeImpl(candidate->Child(i)->TailPtr(), udfSupportsYield, TNodeSet{&candidate->Child(i)->Head().Head()})) {
                    return node;
                }
            }
        } else if (TCoPartitionsByKeys::Match(candidate.Get())) {
            const auto handlerChild = candidate->Child(TCoPartitionsByKeys::idx_ListHandlerLambda);
            if (auto node = FindNonYieldTransparentNodeImpl(handlerChild->TailPtr(), udfSupportsYield, TNodeSet{&handlerChild->Head().Head()})) {
                return node;
            }
        }
    }
    return {};
}

TExprNode::TPtr FindNonYieldTransparentNode(const TExprNode::TPtr& root, const TTypeAnnotationContext& typeCtx, TNodeSet flowSources) {
    TExprNode::TPtr from = root;
    if (root->IsLambda()) {
        if (IsIdentityLambda(*root)) {
            return {};
        }
        from = root->TailPtr();

        // Add all flow lambda args
        root->Head().ForEachChild([&flowSources](const TExprNode& arg) {
            if (IsFlowOrStream(arg)) {
                flowSources.insert(&arg);
            }
        });
    }

    static const THashSet<TStringBuf> WHITE_LIST = {"EmptyIterator"sv, TCoToStream::CallableName(), TCoIterator::CallableName(),
        TCoToFlow::CallableName(), TCoApply::CallableName(), TCoNth::CallableName(), TCoMux::CallableName()};
    // Find all other flow sources (readers)
    auto sources = FindNodes(from,
        [](const TExprNode::TPtr& node) {
            return !node->IsCallable(WHITE_LIST)
                && node->IsCallable()
                && IsFlowOrStream(*node)
                && (node->ChildrenSize() == 0 || !IsFlowOrStream(node->Head()));
        }
    );
    std::for_each(sources.cbegin(), sources.cend(), [&flowSources](const TExprNode::TPtr& node) { flowSources.insert(node.Get()); });

    if (flowSources.empty()) {
        return {};
    }
    return FindNonYieldTransparentNodeImpl(from, typeCtx.UdfSupportsYield, flowSources);
}

bool IsYieldTransparent(const TExprNode::TPtr& root, const TTypeAnnotationContext& typeCtx) {
    return !FindNonYieldTransparentNode(root, typeCtx);
}

TMaybe<bool> IsStrictNoRecurse(const TExprNode& node) {
    if (node.IsCallable({"Unwrap", "Ensure", "ScriptUdf", "Error", "ErrorType"})) {
        return false;
    }
    if (node.IsCallable("Udf")) {
        return HasSetting(*node.Child(TCoUdf::idx_Settings), "strict");
    }
    return {};
}

bool IsStrict(const TExprNode::TPtr& root) {
    // TODO: add TExprNode::IsStrict() method (with corresponding flag). Fill it as part of type annotation pass
    bool isStrict = true;
    VisitExpr(root, [&](const TExprNode::TPtr& node) {
        if (node->IsCallable("AssumeStrict")) {
            return false;
        }

        if (node->IsCallable("AssumeNonStrict")) {
            isStrict = false;
            return false;
        }

        auto maybeStrict = IsStrictNoRecurse(*node);
        if (maybeStrict.Defined() && !*maybeStrict) {
            isStrict = false;
        }

        return isStrict;
    });

    return isStrict;
}

bool HasDependsOn(const TExprNode::TPtr& root, const TExprNode::TPtr& arg) {
    bool withDependsOn = false;
    size_t insideDependsOn = 0;

    VisitExpr(root, [&](const TExprNode::TPtr& node) {
        if (node->IsCallable("DependsOn")) {
            ++insideDependsOn;
        } else if (insideDependsOn && node == arg) {
            withDependsOn = true;
        }
        return !withDependsOn;
    }, [&](const TExprNode::TPtr& node) {
        if (node->IsCallable("DependsOn")) {
            YQL_ENSURE(insideDependsOn > 0);
            --insideDependsOn;
        }
        return true;
    });

    return withDependsOn;
}

template<bool Assume>
TExprNode::TPtr MakeSortConstraintImpl(TExprNode::TPtr node, const TSortedConstraintNode* sorted, const TTypeAnnotationNode* rowType, TExprContext& ctx) {
    if (!(sorted && rowType))
        return node;

    const auto& constent = sorted->GetContent();
    return ctx.Builder(node->Pos())
        .Callable(Assume ? "AssumeSorted" : "Sort")
            .Add(0, std::move(node))
            .List(1)
                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                    size_t index = 0;
                    for (const auto& c : constent) {
                        parent.Callable(index++, "Bool")
                            .Atom(0, ToString(c.second), TNodeFlags::Default)
                        .Seal();
                    }
                    return parent;
                })
            .Seal()
            .Lambda(2)
                .Param("item")
                .List()
                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                        size_t index = 0;
                        for (const auto& c : constent)
                            GetterBuilder(parent, index++, *rowType, c.first.front());
                        return parent;
                    })
                .Seal()
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr KeepSortedConstraint(TExprNode::TPtr node, const TSortedConstraintNode* sorted, const TTypeAnnotationNode* rowType, TExprContext& ctx) {
    return MakeSortConstraintImpl<true>(std::move(node), sorted, rowType, ctx);
}

TExprNode::TPtr MakeSortByConstraint(TExprNode::TPtr node, const TSortedConstraintNode* sorted, const TTypeAnnotationNode* rowType, TExprContext& ctx) {
    return MakeSortConstraintImpl<false>(std::move(node), sorted, rowType, ctx);
}

TExprNode::TPtr KeepConstraints(TExprNode::TPtr node, const TExprNode& src, TExprContext& ctx) {
    auto res = KeepSortedConstraint(node, src.GetConstraint<TSortedConstraintNode>(), GetSeqItemType(src.GetTypeAnn()), ctx);
    res = KeepChoppedConstraint(std::move(res), src, ctx);
    res = KeepUniqueConstraint<true>(std::move(res), src, ctx);
    res = KeepUniqueConstraint<false>(std::move(res), src, ctx);
    return res;
}

bool HasOnlyOneJoinType(const TExprNode& joinTree, TStringBuf joinType) {
    if (joinTree.IsAtom()) {
        return true;
    }

    YQL_ENSURE(joinTree.Child(0)->IsAtom());
    if (joinTree.Child(0)->Content() != joinType) {
        return false;
    }

    return HasOnlyOneJoinType(*joinTree.Child(1), joinType) && HasOnlyOneJoinType(*joinTree.Child(2), joinType);
}

void OptimizeSubsetFieldsForNodeWithMultiUsage(const TExprNode::TPtr& node, const TParentsMap& parentsMap,
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


template<bool Ordered>
std::optional<std::pair<TPartOfConstraintBase::TPathType, ui32>> GetPathToKey(const TExprNode& body, const TExprNode::TChildrenType& args) {
    if (body.IsArgument()) {
        for (auto i = 0U; i < args.size(); ++i)
            if (&body == args[i].Get())
                return std::make_pair(TPartOfConstraintBase::TPathType(), i);
    } else if (body.IsCallable({"Member","Nth"})) {
        if (auto path = GetPathToKey<Ordered>(body.Head(), args)) {
            path->first.emplace_back(body.Tail().Content());
            return path;
        } else if (const auto& head = SkipCallables(body.Head(), {"CastStruct","FilterMembers"}); head.IsCallable("AsStruct") && body.IsCallable("Member")) {
            return GetPathToKey<Ordered>(GetLiteralStructMember(head, body.Tail()), args);
        } else if (body.IsCallable("Nth") && body.Head().IsList()) {
            return GetPathToKey<Ordered>(*body.Head().Child(FromString<ui32>(body.Tail().Content())), args);
        } else if (body.IsCallable({"CastStruct","FilterMembers"}))  {
            return GetPathToKey<Ordered>(body.Head(), args);
        }
    } else if constexpr (!Ordered) {
        if (body.IsCallable("StablePickle")) {
            return GetPathToKey<Ordered>(body.Head(), args);
        }
    }

    return std::nullopt;
}

template std::optional<std::pair<TPartOfConstraintBase::TPathType, ui32>> GetPathToKey<true>(const TExprNode& body, const TExprNode::TChildrenType& args);
template std::optional<std::pair<TPartOfConstraintBase::TPathType, ui32>> GetPathToKey<false>(const TExprNode& body, const TExprNode::TChildrenType& args);

template<bool Ordered>
std::optional<TPartOfConstraintBase::TPathType> GetPathToKey(const TExprNode& body, const TExprNode& arg) {
    if (&body == &arg)
        return TPartOfConstraintBase::TPathType();

    if (body.IsCallable({"Member","Nth"})) {
        if (auto path = GetPathToKey(body.Head(), arg)) {
            path->emplace_back(body.Tail().Content());
            return path;
        }
    }

    if (body.IsCallable({"CastStruct","FilterMembers","Just","Unwrap"}))
        return GetPathToKey<Ordered>(body.Head(), arg);
    if (body.IsCallable("Member") && body.Head().IsCallable("AsStruct"))
        return GetPathToKey<Ordered>(GetLiteralStructMember(body.Head(), body.Tail()), arg);
    if (body.IsCallable("Nth") && body.Head().IsList())
        return GetPathToKey<Ordered>(*body.Head().Child(FromString<ui32>(body.Tail().Content())), arg);
    if (body.IsList() && 1U == body.ChildrenSize() && body.Head().IsCallable("Nth") && body.Head().Tail().IsAtom("0") &&
        1U == RemoveOptionality(*body.Head().Head().GetTypeAnn()).Cast<TTupleExprType>()->GetSize())
        // Especialy for "Extract single item tuple from Condense1" optimizer.
        return GetPathToKey(body.Head().Head(), arg);
    if (body.IsCallable("AsStruct") && 1U == body.ChildrenSize() && body.Head().Tail().IsCallable("Member") &&
        body.Head().Head().Content() == body.Head().Tail().Tail().Content() &&
        1U == RemoveOptionality(*body.Head().Tail().Head().GetTypeAnn()).Cast<TStructExprType>()->GetSize())
        // Especialy for "Extract single item struct from Condense1" optimizer.
        return GetPathToKey<Ordered>(body.Head().Tail().Head(), arg);
    if (IsTransparentIfPresent(body) && &body.Head() == &arg)
        return GetPathToKey<Ordered>(body.Child(1)->Tail().Head(), body.Child(1)->Head().Head());
    if constexpr (!Ordered)
        if (body.IsCallable("StablePickle"))
            return GetPathToKey<Ordered>(body.Head(), arg);

    return std::nullopt;
}

template<bool Ordered>
TPartOfConstraintBase::TSetType GetPathsToKeys(const TExprNode& body, const TExprNode& arg) {
    TPartOfConstraintBase::TSetType keys;
    if (body.IsList()) {
        if (const auto size = body.ChildrenSize()) {
            keys.reserve(size);
            for (auto i = 0U; i < size; ++i)
                if (auto path = GetPathToKey<Ordered>(*body.Child(i), arg))
                    keys.insert_unique(std::move(*path));
        }
    } else if constexpr (!Ordered) {
        if (body.IsCallable("StablePickle")) {
            return GetPathsToKeys<Ordered>(body.Head(), arg);
        }
    }
    if (auto path = GetPathToKey<Ordered>(body, arg)) {
        keys.insert_unique(std::move(*path));
    }

    return keys;
}

template TPartOfConstraintBase::TSetType GetPathsToKeys<true>(const TExprNode& body, const TExprNode& arg);
template TPartOfConstraintBase::TSetType GetPathsToKeys<false>(const TExprNode& body, const TExprNode& arg);

TVector<TString> GenNoClashColumns(const TStructExprType& source, TStringBuf prefix, size_t count) {
    YQL_ENSURE(prefix.StartsWith("_yql"));
    TSet<size_t> existing;
    for (auto& item : source.GetItems()) {
        TStringBuf column = item->GetName();
        if (column.SkipPrefix(prefix)) {
            size_t idx;
            if (TryFromString(column, idx)) {
                existing.insert(idx);
            }
        }
    }

    size_t current = 0;
    TVector<TString> result;
    auto it = existing.cbegin();
    while (count) {
        if (it == existing.cend() || current < *it) {
            result.push_back(TStringBuilder() << prefix << current);
            --count;
        } else {
            ++it;
        }
        YQL_ENSURE(!count || (current + 1 > current));
        ++current;
    }
    return result;
}


}
