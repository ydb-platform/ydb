#include "kqp_rbo_compatibility.h"
#include "kqp_rbo_physical_strict_cast.h"

#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_opt_utils.h>

namespace NKikimr::NKqp {
namespace {

using namespace NYql;

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

bool CanExpandFiniteSqlIn(const TExprNode::TPtr& node) {
    if (HasSetting(*node->Child(2), "tableSource")) {
        return false;
    }

    const auto lookupType = node->Child(1)->GetTypeAnn();
    const auto collectionType = node->Head().GetTypeAnn();
    if (!IsSqlScalar(lookupType) || !collectionType) {
        return false;
    }
    if (collectionType->GetKind() == ETypeAnnotationKind::Tuple) {
        const auto tupleType = collectionType->Cast<TTupleExprType>();
        return tupleType->GetSize() && AllOf(tupleType->GetItems(), IsSqlScalar);
    }

    const auto collection = node->HeadPtr();
    return collectionType->GetKind() == ETypeAnnotationKind::List &&
        (collection->IsList() || collection->IsCallable("AsList")) &&
        collection->ChildrenSize() &&
        IsSqlScalar(collectionType->Cast<TListExprType>()->GetItemType());
}

TExprNode::TPtr ExpandFiniteSqlIn(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (!CanExpandFiniteSqlIn(node)) {
        return node;
    }

    const auto collection = node->HeadPtr();
    const auto lookup = node->ChildPtr(1);
    const bool ansi = HasSetting(*node->Child(2), "ansi");
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
        return node->IsCallable({"StrictCast", "HasNull", "SqlIn"}) || IsComplexComparison(node);
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
    if (node->IsCallable("StrictCast")) {
        return NPhysicalConvertionUtils::ExpandScalarStrictCast(node, ctx);
    }
    if (node->IsCallable("HasNull")) {
        return ExpandScalarHasNull(node, ctx, types);
    }
    if (node->IsCallable("SqlIn")) {
        return ExpandFiniteSqlIn(node, ctx);
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
    return node;
}

void EnsureRboCompatibilityLowered(const NYql::TExprNode::TPtr& root) {
    if (const auto unsupported = FindCompatibilityNode(root)) {
        YQL_ENSURE(false, "Focused RBO compatibility lowering failed on "
                              << unsupported->Content());
    }
}

} // namespace NKikimr::NKqp
