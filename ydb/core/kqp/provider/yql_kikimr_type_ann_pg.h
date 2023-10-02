#pragma once
#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/core/kqp/provider/yql_kikimr_expr_nodes.h>

namespace NYql {
namespace NPgTypeAnn {
    
bool NeedsValuesRename(
    const NNodes::TKiWriteTable& node, 
    TYdbOperation op);

bool RewriteValuesColumnNames(
    const NNodes::TKiWriteTable& node, 
    const TKikimrTableDescription* table, 
    TExprContext& ctx, 
    TTypeAnnotationContext& types);

bool ValidatePgUpdateKeys(
    const NNodes::TKiWriteTable& node, 
    const TKikimrTableDescription* table, 
    TExprContext& ctx);


} // namespace NPgTypeAnn
} // namespace NYql
