#pragma once

#include "layers_fwd.h"

#include <yql/essentials/ast/yql_expr.h>

namespace NYql::NLayers {

class ILayersIntegration: public TThrRefBase {
public:
    virtual TMaybe<TLocations> ResolveLayers(const TVector<TKey>& order, const TString& cluster, NYql::TExprContext& ctx) const = 0;
    virtual bool UpdateLayerCache(const TKey& key, const TLayerInfo& layer, NYql::TExprContext& ctx) = 0;
    virtual bool UpdateLayerLocations(const TKey& key, TLocations&& locs, NYql::TExprContext& ctx) = 0;
    virtual void RemoveLayerByName(const TString& name) = 0;
    virtual ~ILayersIntegration() = default;
};

using ILayersIntegrationPtr = TIntrusivePtr<ILayersIntegration>;
} // namespace NYql::NLayers
