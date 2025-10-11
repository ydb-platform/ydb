#pragma once

#include "layers_fwd.h"
#include "remote_layer_provider.h"
#include "layers_integration.h"

namespace NYql::NLayers {

using TLayerOrder = TVector<TString>;

class ILayersRegistry: public TThrRefBase {
public:
    virtual TMaybe<TVector<TKey>> ResolveLogicalLayers(const TVector<TLayerOrder>& orders, TExprContext& ctx) const = 0;
    virtual TMaybe<TLocations> ResolveLayers(const TVector<TKey>& order, const TString& system, const TString& cluster, TExprContext& ctx) const = 0;
    virtual bool HasLayer(const TKey& key) const = 0;
    virtual bool AddLayer(const TString& name, const TMaybe<TString>& parent, const TMaybe<TString>& url, TExprContext& ctx) = 0;
    virtual bool AddLayerFromJson(TStringBuf json, TExprContext& ctx) = 0;
    virtual void ClearLayers() = 0;
    virtual ~ILayersRegistry() = default;
};
using ILayersRegistryPtr = TIntrusivePtr<ILayersRegistry>;

// remove duplicates when cluster and system is fixed
TMaybe<TVector<TLocations>> RemoveDuplicates(const TVector<std::pair<TKey, const TLayerInfo*>>& layers, TStringBuf system, const TString& cluster, TExprContext& ctx);

ILayersRegistryPtr MakeLayersRegistry(const THashMap<TString, IRemoteLayerProviderPtr>& remoteProviders, const THashMap<TString, ILayersIntegrationPtr>& integrations);
} // namespace NYql::NLayers
