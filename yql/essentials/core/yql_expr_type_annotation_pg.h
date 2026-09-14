#pragma once

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/core/yql_node_transform.h>
#include <yql/essentials/parser/pg_catalog/catalog.h>

#include <util/generic/fwd.h>
#include <util/generic/maybe.h>
#include <util/generic/vector.h>

#include <functional>
#include <variant>

namespace NYql {

struct TPgCallResolutionResult {
    struct TProc {
        const NPg::TProcDesc* Proc = nullptr;
        TVector<TMaybe<TNodeTransform>> InputTransforms;
        TVector<TExprNode::TPtr> DefaultArgs;

        TVector<TExprNode::TPtr> BuildArgs(const TVector<TExprNode::TPtr>& inputArgNodes, TExprContext& ctx) const;
    };

    std::variant<TProc, const NPg::TTypeDesc*> Value;

    const TProc* AsProc() const {
        return std::get_if<TProc>(&Value);
    }

    const NPg::TTypeDesc* AsType() const {
        const auto* p = std::get_if<const NPg::TTypeDesc*>(&Value);
        return p ? *p : nullptr;
    }
};

bool IsCastRequired(ui32 fromTypeId, ui32 toTypeId);

TExprNodePtr WrapWithPgCast(TExprNodePtr node, ui32 targetTypeId, TExprContext& ctx);

TPgCallResolutionResult ResolvePgCall(
    const TString& name,
    const TVector<ui32>& argTypes,
    TPositionHandle pos,
    TExprContext& ctx);

// Fill |ctx| with error if returned Nothing().
TMaybe<TPgCallResolutionResult> ResolvePgCall(
    const TString& name,
    const TVector<TExprNode::TPtr>& inputArgNodes,
    TPositionHandle pos,
    TExprContext& ctx);

} // namespace NYql
