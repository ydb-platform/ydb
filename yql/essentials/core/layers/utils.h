#pragma once

#include "layers_fwd.h"

#include <yql/essentials/ast/yql_expr.h>

namespace NYql::NLayers {
TMaybe<TVector<TLocations>> RemoveDuplicates(const TVector<std::pair<TKey, const TLayerInfo*>>& layers, TStringBuf system, const TString& cluster, TExprContext& ctx);
} // namespace NYql::NLayers
