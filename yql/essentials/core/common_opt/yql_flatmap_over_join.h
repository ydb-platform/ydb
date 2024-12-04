#pragma once

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/core/yql_type_annotation.h>

#include <yql/essentials/ast/yql_expr.h>

#include <util/generic/strbuf.h>


namespace NYql {

NNodes::TExprBase FlatMapOverEquiJoin(
    const NNodes::TCoFlatMapBase& node,
    TExprContext& ctx,
    const TParentsMap& parentsMap,
    bool multiUsage,
    const TTypeAnnotationContext* types);

} // NYql
