#pragma once
#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <ydb/core/kqp/provider/yql_kikimr_expr_nodes.h>

namespace NYql {
namespace NPgTypeAnn {
    
bool IsPgInsert(
    const NNodes::TKiWriteTable& node, 
    TYdbOperation op);

bool ValidatePgUpdateKeys(
    const NNodes::TKiWriteTable& node, 
    const TKikimrTableDescription* table, 
    TExprContext& ctx);


} // namespace NPgTypeAnn
} // namespace NYql
