#pragma once

#include <yql/essentials/ast/yql_expr.h>

#include <util/generic/hash.h>

namespace NKikimr::NKqp {

// Make UDF secret dependencies visible to the normal KQP secret resolution and
// permission checks. Values are structured token references, never plaintext.
void FillRemoteEmbedderSecureParams(const NYql::TExprNode::TPtr& node, THashMap<TString, TString>& secureParams);

} // namespace NKikimr::NKqp
