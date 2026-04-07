#include "yql_yt_layers_integration.h"

#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/core/layers/utils.h>

namespace {
using namespace NYql::NLayers;
class TYtLayersIntegration : public ILayersIntegration {
public:
    TMaybe<TLocations> ResolveLayers(const TVector<TKey>& order, const TString& cluster, NYql::TExprContext& ctx) const override {
        TVector<std::pair<TKey, const TLayerInfo*>> infos;
        infos.reserve(order.size());
        for (auto &e: order) {
            infos.emplace_back(e, Data_.FindPtr(e));
            if (infos.back().second->Locations.empty()) {
                ctx.AddError(NYql::TIssue(
                    TStringBuilder() << "No locations for layer " << e.ToString() << ", consider adding it using yt.LayerCaches"
                ));
                return {};
            }
        }
        auto layers = RemoveDuplicates(infos, NYql::YtProviderName, cluster, ctx);
        if (!layers) {
            return {};
        }
        TVector<TLocation> result;
        result.reserve(layers->size());
        for (auto &e: *layers) {
            // TODO(mrlolthe1st): now we're choosing any suitable location.
            // but later we can prioritize something, maybe it will be SQUASHFS
            result.emplace_back(e.front());
        }
        return result;
    }

    bool UpdateLayerCache(const TKey& key, const TLayerInfo& layer, NYql::TExprContext&) override {
        Data_.emplace(key, layer);
        return true;
    }

    bool UpdateLayerLocations(const TKey& key, TVector<TLocation>&& locs, NYql::TExprContext& ctx) override {
        auto ptr = Data_.FindPtr(key);
        if (!ptr) {
            ctx.AddError(NYql::TIssue(
                TStringBuilder() << "Layer with key=(Name=" << key.Name.GetOrElse("nullopt").Quote()
                << ", Url=" << key.Url.GetOrElse("nullopt").Quote() << ") not found"
            ));
            return false;
        }
        ptr->Locations = std::move(locs);
        return true;
    }

    void RemoveLayerByName(const TString& name) override {
        Data_.erase(TKey(name));
    }

private:
    THashMap<TKey, TLayerInfo> Data_;
};
}

namespace NYql {
NLayers::ILayersIntegrationPtr CreateYtLayersIntegration() {
    return MakeIntrusive<TYtLayersIntegration>();
}
}
