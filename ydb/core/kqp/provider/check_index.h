#pragma once

#include "yql_kikimr_provider.h"

namespace NYql {

TExprNode::TPtr BuildCheckIndex(const TExprNode& read, const TKikimrTableDescription& base,
    TStringBuf indexName, TKikimrSessionContext& session, TTypeAnnotationContext& types, TExprContext& ctx);

} // namespace NYql
