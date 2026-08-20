#include "kqp_opt_peephole_rules.h"

#include <ydb/core/kqp/opt/physical/kqp_opt_phy_impl.h>

#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NNodes;

namespace {

//! A string-like data slot whose bytes we can pin into WASM linear memory.
bool IsResidentStringSlot(EDataSlot slot) {
    switch (slot) {
        case EDataSlot::String:
        case EDataSlot::Utf8:
        case EDataSlot::Yson:
        case EDataSlot::Json:
        case EDataSlot::JsonDocument:
            return true;
        default:
            return false;
    }
}

bool IsResidentStringType(const TTypeAnnotationNode* type) {
    if (!type) {
        return false;
    }
    if (type->GetKind() == ETypeAnnotationKind::Optional) {
        type = type->Cast<TOptionalExprType>()->GetItemType();
    }
    if (type->GetKind() != ETypeAnnotationKind::Data) {
        return false;
    }
    return IsResidentStringSlot(type->Cast<TDataExprType>()->GetSlot());
}

//! A subtree that references no lambda argument is loop-invariant inside its
//! stage: it depends only on parameters (e.g. a precompute scalar) and literals,
//! so it is evaluated once and can be safely pinned and cached.
bool DependsOnArgs(const TExprNode& node) {
    bool found = false;
    VisitExpr(node, [&found](const TExprNode& n) {
        if (found) {
            return false;
        }
        if (n.IsArgument()) {
            found = true;
            return false;
        }
        return true;
    });
    return found;
}

} // namespace

TExprBase KqpRewriteWasmResidentConstArgs(const TExprBase& node, TExprContext& ctx) {
    auto apply = node.Cast<TCoApply>();

    // Only direct UDF calls: the compiler cannot tell a wasm UDF from a native
    // one here, so this rule is gated behind an explicit pragma.
    if (!apply.Callable().Maybe<TCoUdf>()) {
        return node;
    }

    TExprNode::TListType children = node.Ref().ChildrenList();
    bool changed = false;
    // Child 0 is the callable; free args start at 1.
    for (size_t i = 1; i < children.size(); ++i) {
        const auto& arg = children[i];

        if (TCoArgument::Match(arg.Get())) {
            continue;
        }
        if (TKqpWasmResidentString::Match(arg.Get())) {
            continue;
        }
        if (!IsResidentStringType(arg->GetTypeAnn())) {
            continue;
        }
        if (DependsOnArgs(*arg)) {
            continue;
        }

        children[i] = Build<TKqpWasmResidentString>(ctx, arg->Pos())
            .Value(TExprBase(arg))
            .Done()
            .Ptr();
        changed = true;
    }

    if (!changed) {
        return node;
    }

    return TExprBase(ctx.ChangeChildren(node.Ref(), std::move(children)));
}

} // namespace NKikimr::NKqp::NOpt
