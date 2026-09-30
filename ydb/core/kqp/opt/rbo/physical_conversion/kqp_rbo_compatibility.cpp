#include "kqp_rbo_compatibility.h"
#include "kqp_rbo_physical_strict_cast.h"

#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_opt_range.h>
#include <yql/essentials/core/yql_opt_utils.h>

namespace NKikimr::NKqp {
namespace {

using namespace NYql;

TExprNode::TPtr ExpandExtractMembers(const TExprNode::TPtr& node, TExprContext& ctx) {
    auto arg = ctx.NewArgument(node->Pos(), "extract_members_arg");
    TExprNode::TListType fields;
    fields.reserve(node->Tail().ChildrenSize());
    for (const auto& member : node->Tail().Children()) {
        fields.emplace_back(ctx.NewList(node->Pos(), {
            member,
            ctx.NewCallable(node->Pos(), "Member", {arg, member})
        }));
    }

    auto body = ctx.NewCallable(node->Pos(), "AsStruct", std::move(fields));
    auto lambda = ctx.NewLambda(node->Pos(), ctx.NewArguments(node->Pos(), {std::move(arg)}), std::move(body));
    // Preserve ordering when constraints have not been computed for the newly built input yet.
    return ctx.NewCallable(node->Pos(), "OrderedMap", {node->HeadPtr(), std::move(lambda)});
}

TExprNode::TPtr ExpandOptionalIf(const TExprNode::TPtr& node, TExprContext& ctx) {
    auto item = ctx.NewCallable(node->Pos(), "Just", {node->TailPtr()});
    auto empty = ctx.NewCallable(node->Pos(), "EmptyFrom", {item});
    return ctx.NewCallable(node->Pos(), "If", {node->HeadPtr(), std::move(item), std::move(empty)});
}

bool IsSqlScalar(const TTypeAnnotationNode* type) {
    return type && (IsDataOrOptionalOfData(type) || type->GetKind() == ETypeAnnotationKind::Null);
}

TExprNode::TPtr TupleItem(const TExprNode::TPtr& tuple, size_t index, TExprContext& ctx) {
    return ctx.NewCallable(
        tuple->Pos(), "Nth",
        {tuple, ctx.NewAtom(tuple->Pos(), ToString(index), TNodeFlags::Default)});
}

bool CanExpandTupleComparison(const TExprNode::TPtr& node) {
    const auto leftType = node->Head().GetTypeAnn();
    const auto rightType = node->Tail().GetTypeAnn();
    if (!leftType || !rightType ||
        leftType->GetKind() != ETypeAnnotationKind::Tuple ||
        rightType->GetKind() != ETypeAnnotationKind::Tuple) {
        return false;
    }

    const auto leftItems = leftType->Cast<TTupleExprType>()->GetItems();
    const auto rightItems = rightType->Cast<TTupleExprType>()->GetItems();
    if (leftItems.size() != rightItems.size()) {
        return false;
    }
    for (size_t index = 0; index < leftItems.size(); ++index) {
        if (!IsSqlScalar(leftItems[index]) || !IsSqlScalar(rightItems[index])) {
            return false;
        }
    }
    return true;
}

TExprNode::TPtr BuildTupleItemComparison(
    const TExprNode::TPtr& node,
    size_t index,
    TStringBuf callable,
    TExprContext& ctx) {
    return ctx.NewCallable(
        node->Pos(), callable,
        {TupleItem(node->HeadPtr(), index, ctx), TupleItem(node->TailPtr(), index, ctx)});
}

TExprNode::TPtr ExpandTupleComparison(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (!CanExpandTupleComparison(node)) {
        return node;
    }

    const size_t size = node->Head().GetTypeAnn()->Cast<TTupleExprType>()->GetSize();
    const auto callable = node->Content();
    if (!size) {
        return MakeBool(node->Pos(), callable == "==" || callable == "<=" || callable == ">=", ctx);
    }

    if (callable == "==" || callable == "!=") {
        TExprNode::TListType items;
        items.reserve(size);
        for (size_t index = 0; index < size; ++index) {
            items.push_back(BuildTupleItemComparison(node, index, callable, ctx));
        }
        return ctx.NewCallable(node->Pos(), callable == "==" ? "And" : "Or", std::move(items));
    }

    const TStringBuf strictCallable = callable.StartsWith('<') ? "<" : ">";
    auto result = BuildTupleItemComparison(node, size - 1, callable, ctx);
    for (size_t index = size - 1; index-- > 0;) {
        result = ctx.Builder(node->Pos())
            .Callable("If")
                .Callable(0, "Coalesce")
                    .Add(0, BuildTupleItemComparison(node, index, "==", ctx))
                    .Add(1, MakeBool(node->Pos(), false, ctx))
                .Seal()
                .Add(1, std::move(result))
                .Add(2, BuildTupleItemComparison(node, index, strictCallable, ctx))
            .Seal()
            .Build();
    }
    return result;
}

TExprNode::TPtr BuildSqlInComparisons(const TExprNode::TPtr& node, bool ansi, TExprContext& ctx) {
    const auto collection = node->HeadPtr();
    const auto lookup = node->ChildPtr(1);
    const bool legacyNullable = !ansi && IsSqlInCollectionItemsNullable(NNodes::TCoSqlIn(node));
    const bool explicitItems = collection->IsList() || collection->IsCallable("AsList");
    const size_t size = explicitItems
        ? collection->ChildrenSize()
        : collection->GetTypeAnn()->Cast<TTupleExprType>()->GetSize();

    TExprNode::TListType equals;
    equals.reserve(size);
    for (size_t index = 0; index < size; ++index) {
        auto item = explicitItems ? collection->ChildPtr(index) : TupleItem(collection, index, ctx);
        auto equal = ctx.NewCallable(node->Pos(), "==", {lookup, std::move(item)});
        if (legacyNullable) {
            equal = ctx.Builder(node->Pos())
                .Callable("Coalesce")
                    .Add(0, std::move(equal))
                    .Add(1, MakeBool(node->Pos(), false, ctx))
                .Seal().Build();
        }
        equals.push_back(std::move(equal));
    }

    auto result = ctx.NewCallable(node->Pos(), "Or", std::move(equals));
    if (legacyNullable && lookup->GetTypeAnn()->HasOptionalOrNull()) {
        result = ctx.Builder(node->Pos())
            .Callable("If")
                .Callable(0, "HasNull")
                    .Add(0, lookup)
                .Seal()
                .Add(1, MakeNull(node->Pos(), ctx))
                .Add(2, std::move(result))
            .Seal().Build();
    }
    return result;
}

TExprNode::TPtr BuildSqlInSet(const TExprNode::TPtr& list, TExprContext& ctx) {
    return ctx.Builder(list->Pos())
        .Callable("ToDict")
            .Add(0, list)
            .Lambda(1).Param("item").Arg("item").Seal()
            .Lambda(2).Param("item").Callable("Void").Seal().Seal()
            .List(3)
                .Atom(0, "Auto", TNodeFlags::Default)
                .Atom(1, "One", TNodeFlags::Default)
                .Atom(2, "Compact", TNodeFlags::Default)
            .Seal()
        .Seal().Build();
}

TExprNode::TPtr BuildSqlInContains(
    TPositionHandle pos,
    const TExprNode::TPtr& dict,
    const TExprNode::TPtr& lookup,
    const TTypeAnnotationNode* keyType,
    TExprContext& ctx) {
    const auto lookupType = lookup->GetTypeAnn();
    if (IsSameAnnotation(*lookupType, *keyType)) {
        return ctx.NewCallable(pos, "Contains", {dict, lookup});
    }
    const auto castOptions = CastResult<true>(lookupType, keyType);
    if (castOptions & NUdf::ECastOptions::Impossible) {
        return MakeBool(pos, false, ctx);
    }
    if (!(castOptions & NUdf::ECastOptions::MayFail)) {
        const auto casted = ctx.NewCallable(pos, "StrictCast", {lookup, ExpandType(pos, *keyType, ctx)});
        return ctx.NewCallable(pos, "Contains", {dict, casted});
    }

    // MiniKQL Contains requires an exact key type. Convert the lookup here,
    // treating failed or lossy conversions as non-matches.
    const auto castType = ctx.MakeType<TOptionalExprType>(keyType);
    const auto casted = ctx.NewCallable(pos, "StrictCast", {lookup, ExpandType(pos, *castType, ctx)});
    return ctx.Builder(pos)
        .Callable("IfPresent")
            .Add(0, casted)
            .Lambda(1)
                .Param("key")
                .Callable("Contains")
                    .Add(0, dict)
                    .Arg(1, "key")
                .Seal()
            .Seal()
            .Add(2, MakeBool(pos, false, ctx))
        .Seal().Build();
}

TExprNode::TPtr ExpandScalarSqlIn(const TExprNode::TPtr& node, TExprContext& ctx) {
    const auto collection = node->HeadPtr();
    const auto lookup = node->ChildPtr(1);
    const auto collectionType = collection->GetTypeAnn();
    const auto lookupType = lookup->GetTypeAnn();
    if (HasSetting(*node->Child(2), "tableSource") || !collectionType || !IsSqlScalar(lookupType)) {
        return node;
    }

    const bool ansi = HasSetting(*node->Child(2), "ansi");
    const TTypeAnnotationNode* itemType = nullptr;
    // Preserve comparison expansion for fixed-size tuples and explicit lists.
    // Other scalar lists, including parameters, use runtime dictionary lookup.
    if (collectionType->GetKind() == ETypeAnnotationKind::Tuple) {
        const auto tupleType = collectionType->Cast<TTupleExprType>();
        if (!AllOf(tupleType->GetItems(), IsSqlScalar)) {
            return node;
        }
        if (tupleType->GetSize()) {
            return BuildSqlInComparisons(node, ansi, ctx);
        }
    } else if (collectionType->GetKind() == ETypeAnnotationKind::List) {
        itemType = collectionType->Cast<TListExprType>()->GetItemType();
        if (!IsSqlScalar(itemType)) {
            return node;
        }
        if ((collection->IsList() || collection->IsCallable("AsList")) && collection->ChildrenSize()) {
            return BuildSqlInComparisons(node, ansi, ctx);
        }
    } else if (collectionType->GetKind() != ETypeAnnotationKind::EmptyList) {
        return node;
    }

    const auto pos = node->Pos();
    const bool nullableLookup = lookupType->HasOptionalOrNull();
    const auto falseNode = MakeBool(pos, false, ctx);
    const auto justFalse = ctx.NewCallable(pos, "Just", {falseNode});
    const auto nothing = MakeBoolNothing(pos, ctx);
    const auto legacyFalse = nullableLookup
        ? ctx.NewCallable(pos, "If", {ctx.NewCallable(pos, "HasNull", {lookup}), nothing, justFalse})
        : falseNode;

    if (!itemType) { // EmptyList or an empty tuple.
        return ansi && nullableLookup ? justFalse : legacyFalse;
    }

    // A list of Null has no possible match. ANSI IN still distinguishes an
    // empty collection (false) from a nonempty collection (unknown).
    const auto hasItems = ctx.NewCallable(pos, "HasItems", {collection});
    const auto emptyResult = ctx.NewCallable(pos, "If", {hasItems, nothing, justFalse});
    if (itemType->GetKind() == ETypeAnnotationKind::Null) {
        return ansi ? emptyResult : legacyFalse;
    }

    const auto dict = BuildSqlInSet(collection, ctx);
    const auto contains = BuildSqlInContains(pos, dict, lookup, itemType, ctx);
    auto result = contains;
    if (ansi && itemType->GetKind() == ETypeAnnotationKind::Optional) {
        // Preserve null keys: checking membership of Nothing detects nulls
        // without scanning the collection a second time.
        const auto nullKey = ctx.NewCallable(pos, "Nothing", {ExpandType(pos, *itemType, ctx)});
        const auto hasNull = ctx.NewCallable(pos, "Contains", {dict, nullKey});
        const auto justTrue = ctx.NewCallable(pos, "Just", {MakeBool(pos, true, ctx)});
        result = ctx.NewCallable(pos, "If", {
            contains, justTrue, ctx.NewCallable(pos, "If", {hasNull, nothing, justFalse})});
    } else if (nullableLookup) {
        result = ctx.NewCallable(pos, "Just", {contains});
    }

    if (nullableLookup) {
        // Legacy IN is unknown for a null lookup even when the list is empty.
        result = ctx.NewCallable(pos, "If", {
            ctx.NewCallable(pos, "HasNull", {lookup}), ansi ? emptyResult : nothing, result});
    }
    return result;
}

bool IsSupportedHasNullType(const TTypeAnnotationNode* type) {
    type = RemoveAllOptionals(type);
    return type && (type->GetKind() == ETypeAnnotationKind::Data ||
                    type->GetKind() == ETypeAnnotationKind::Null);
}

bool IsComplexComparison(const TExprNode::TPtr& node) {
    return node->IsCallable({"==", ">", "<", ">=", "<=", "!="}) &&
        (!IsDataOrOptionalOfData(node->Head().GetTypeAnn()) ||
         !IsDataOrOptionalOfData(node->Tail().GetTypeAnn()));
}

TExprNode::TPtr ExpandScalarHasNull(
    const TExprNode::TPtr& node,
    TExprContext& ctx,
    const TTypeAnnotationContext& types) {
    const auto type = node->Head().GetTypeAnn();
    if (!IsSupportedHasNullType(type)) {
        return node;
    }

    TExprNode::TPtr result;
    switch (type->GetKind()) {
        case ETypeAnnotationKind::Data:
            result = MakeBool(node->Pos(), false, ctx);
            break;
        case ETypeAnnotationKind::Null:
            result = MakeBool(node->Pos(), true, ctx);
            break;
        case ETypeAnnotationKind::Optional:
            result = ctx.Builder(node->Pos())
                .Callable("IfPresent")
                    .Add(0, node->HeadPtr())
                    .Lambda(1)
                        .Param("item")
                        .Callable("HasNull")
                            .Arg(0, "item")
                        .Seal()
                    .Seal()
                    .Add(2, MakeBool(node->Pos(), true, ctx))
                .Seal()
                .Build();
            break;
        default:
            return node;
    }

    result = KeepWorld(std::move(result), *node, ctx, types);
    return KeepSideEffects(std::move(result), node->HeadPtr(), ctx);
}

TExprNode::TPtr FindCompatibilityNode(const TExprNode::TPtr& root) {
    return FindNode(root, [](const TExprNode::TPtr& node) {
        return node->IsCallable({"ExtractMembers", "OptionalIf", "StrictCast", "HasNull", "SqlIn", "RangeEmpty", "AsRange", "RangeFor"}) ||
            IsComplexComparison(node);
    });
}

} // namespace

bool NeedsRboCompatibilityLowering(const NYql::TExprNode::TPtr& root) {
    return !!FindCompatibilityNode(root);
}

NYql::TExprNode::TPtr RewriteRboCompatibilityNode(
    const NYql::TExprNode::TPtr& node,
    NYql::TExprContext& ctx,
    const NYql::TTypeAnnotationContext& types) {
    if (node->IsCallable("ExtractMembers")) {
        return ExpandExtractMembers(node, ctx);
    }
    if (node->IsCallable("OptionalIf")) {
        return ExpandOptionalIf(node, ctx);
    }
    if (node->IsCallable("StrictCast")) {
        return NPhysicalConvertionUtils::ExpandScalarStrictCast(node, ctx);
    }
    if (node->IsCallable("HasNull")) {
        return ExpandScalarHasNull(node, ctx, types);
    }
    if (node->IsCallable("SqlIn")) {
        return ExpandScalarSqlIn(node, ctx);
    }
    if (IsComplexComparison(node)) {
        if (node->Head().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Null ||
            node->Tail().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Null) {
            auto result = KeepWorld(MakeBoolNothing(node->Pos(), ctx), *node, ctx, types);
            result = KeepSideEffects(std::move(result), node->TailPtr(), ctx);
            return KeepSideEffects(std::move(result), node->HeadPtr(), ctx);
        }
        return ExpandTupleComparison(node, ctx);
    }
    if (node->IsCallable("RangeEmpty")) {
        return ExpandRangeEmpty(node, ctx);
    }
    if (node->IsCallable("AsRange")) {
        return ExpandAsRange(node, ctx);
    }
    if (node->IsCallable("RangeFor")) {
        return ExpandRangeFor(node, ctx);
    }
    return node;
}

void EnsureRboCompatibilityLowered(const NYql::TExprNode::TPtr& root) {
    if (const auto unsupported = FindCompatibilityNode(root)) {
        YQL_ENSURE(false, "Focused RBO compatibility lowering failed on "
                              << unsupported->Content());
    }
}

} // namespace NKikimr::NKqp
