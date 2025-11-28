#include "layers.h"

#include <yql/essentials/utils/fetch/fetch.h>

#include <util/generic/hash_set.h>
#include <library/cpp/json/json_reader.h>

#include <functional>
#include <numeric>

namespace {

using namespace NYql::NLayers;

struct TLogicalInfo {
    TString Name;
    TMaybe<TString> Parent;
    TMaybe<TString> Url;
};

class TLayersRegistry: public ILayersRegistry {
public:
    TLayersRegistry(const THashMap<TString, IRemoteLayerProviderPtr>& remoteProviders, const THashMap<TString, ILayersIntegrationPtr>& integrations)
        : RemoteProviders_(remoteProviders)
        , Integrations_(integrations)
    {
    }

    TMaybe<TVector<TKey>> ResolveLogicalLayers(const TVector<TLayerOrder>& orders, NYql::TExprContext& ctx) const override {
        THashMap<TKey, THashSet<TKey>> graph;
        // first of all let's prepare graph of restrictions
        for (auto& order : orders) {
            for (size_t i = 0; i < order.size(); ++i) {
                auto& layer = order[i];
                auto logicalInfoPtr = LogicalInfoByName_.FindPtr(layer);
                if (!logicalInfoPtr) {
                    ctx.AddError(NYql::TIssue(TStringBuilder() << "Layer " << layer.Quote() << " not found"));
                    return {};
                }
                auto currLayer = logicalInfoPtr;
                while (currLayer) {
                    // current comes after parent in topsort
                    TKey currKey(currLayer->Name);
                    if (graph.contains(currKey)) {
                        break;
                    }
                    auto& vertex = graph[currKey];
                    if (currLayer->Parent) {
                        vertex.emplace(TKey(*currLayer->Parent));
                        currLayer = LogicalInfoByName_.FindPtr(*currLayer->Parent);
                        YQL_ENSURE(currLayer);
                    } else {
                        currLayer = nullptr;
                    }
                }
                auto key = TKey(layer);
                if (i) {
                    // current comes after previous in topsort
                    graph[key].emplace(TKey(order[i - 1]));
                }
                auto curr = logicalInfoPtr->Url;
                if (curr) {
                    // fake edge from current to url (it cause duplicates, that will be filtered after)
                    // keys, which location we're know (it means that location associated with url),
                    // will not present in the resulting order. It means, that
                    // only integration-specific keys will be returned
                    graph[key].emplace(TKey({}, *curr));
                }
                while (curr) {
                    TKey loc({}, *curr);
                    if (graph.contains(loc)) { // already present
                        break;
                    }
                    auto ptr = InfoByUrl_.FindPtr(*curr);
                    if (!ptr) {
                        ctx.AddError(NYql::TIssue(TStringBuilder() << "Layer with url " << curr->Quote() << " not found"));
                        return {};
                    }
                    auto& currVertex = graph[loc];
                    if (ptr->Parent) {
                        currVertex.emplace(*ptr->Parent);
                        curr = ptr->Parent->Url;
                    } else {
                        curr = {};
                    }
                }
            }
        }

        THashSet<TKey> visited;
        THashSet<TKey> currentVisited;
        TVector<TKey> order;
        bool hasErrors = false;
        std::function<void(const TKey&)> topSort = [&](const TKey& v) {
            currentVisited.emplace(v);
            for (auto& e : graph[v]) {
                if (currentVisited.contains(e)) {
                    ctx.AddError(NYql::TIssue(TStringBuilder() << "Layers graph contain cycles"));
                    hasErrors = true;
                    return;
                }
                if (visited.emplace(e).second) {
                    topSort(e);
                }
            }
            currentVisited.erase(v);
            order.emplace_back(v);
        };
        for (auto& [v, _] : graph) {
            if (!visited.emplace(v).second) {
                continue;
            }
            topSort(v);
        }
        if (hasErrors) {
            return {};
        }
        size_t sz = 0;
        for (auto& k : order) {
            if (k.Url) {
                order[sz++] = std::move(k);
                continue;
            }
            YQL_ENSURE(k.Name);
            auto ptr = LogicalInfoByName_.FindPtr(*k.Name);
            YQL_ENSURE(ptr);
            if (!ptr->Url) {
                order[sz++] = std::move(k);
            }
        }
        order.crop(sz);
        return order;
    }

    TMaybe<TLocations> ResolveLayers(const TVector<TKey>& order, const TString& system, const TString& cluster, NYql::TExprContext& ctx) const override {
        auto integrationPtr = Integrations_.FindPtr(system);
        if (!integrationPtr) {
            throw yexception() << "Unkown system: " << system.Quote();
        }
        return (*integrationPtr)->ResolveLayers(order, cluster, ctx);
    }

    bool HasLayer(const TKey& key) const override {
        if (key.Name) {
            return LogicalInfoByName_.contains(*key.Name);
        }
        YQL_ENSURE(key.Url);
        return InfoByUrl_.contains(*key.Url);
    }

    bool AddLayer(const TString& name, const TMaybe<TString>& parent, const TMaybe<TString>& url, NYql::TExprContext& ctx) override {
        if (parent && *parent == name) {
            ctx.AddError(NYql::TIssue(
                TStringBuilder() << "Layer's parent can't point to itself"));
            return false;
        }

        if (parent && !LogicalInfoByName_.contains(*parent)) {
            ctx.AddError(NYql::TIssue(
                TStringBuilder() << "Layer " << parent->Quote() << " not found"));
            return false;
        }

        auto [_, emplaced] = LogicalInfoByName_.emplace(name, TLogicalInfo{
                                                                  .Name = name,
                                                                  .Parent = parent,
                                                                  .Url = url});

        if (!emplaced) {
            ctx.AddError(NYql::TIssue(
                TStringBuilder() << "Layer " << name.Quote() << " already exists"));
            return false;
        }

        for (const auto& [_, integration] : Integrations_) {
            if (!integration->UpdateLayerCache(TKey(name),
                                               TLayerInfo{.Parent = parent ? TMaybe<TKey>(TKey(*parent)) : TMaybe<TKey>{}}, ctx))
            {
                return false;
            }
        }

        if (!url || InfoByUrl_.contains(*url)) {
            return true;
        }

        TString schema;
        try {
            auto parsedUrl = NYql::ParseURL(*url);
            schema = parsedUrl.Get(NUri::TField::FieldScheme);
        } catch (...) {
            ctx.AddError(NYql::TIssue(CurrentExceptionMessage()));
            return false;
        }

        auto providerPtr = RemoteProviders_.FindPtr(schema);
        if (!providerPtr) {
            ctx.AddError(NYql::TIssue(
                TStringBuilder() << "Unkown layer schema: " << schema.Quote()));
            return false;
        }

        auto& provider = *providerPtr;

        TMaybe<TKey> currentUrl = TKey({}, *url);
        while (currentUrl && !InfoByUrl_.contains(*currentUrl->Url)) {
            try {
                auto info = provider->GetLayerInfo(parent, *currentUrl->Url).GetValueSync();
                for (const auto& [_, integration] : Integrations_) {
                    if (!integration->UpdateLayerCache(TKey({}, *currentUrl->Url), info, ctx)) {
                        return false;
                    }
                }
                auto next = info.Parent;
                InfoByUrl_.emplace(*currentUrl->Url, std::move(info));
                currentUrl = next;
            } catch (...) {
                ctx.AddError(NYql::TIssue(CurrentExceptionMessage()));
                return false;
            }
        }
        return true;
    }

    void ClearLayers() override {
        for (const auto& [name, _] : LogicalInfoByName_) {
            for (const auto& [_, integration] : Integrations_) {
                integration->RemoveLayerByName(name);
            }
        }
        LogicalInfoByName_.clear();
    }

    bool AddLayerFromJson(TStringBuf json, NYql::TExprContext& ctx) override {
        NJson::TJsonValue val;
        if (!NJson::ReadJsonTree(json, &val)) {
            ctx.AddError(NYql::TIssue(
                TStringBuilder() << "Layer description must be a valid JSON string"));
            return false;
        }
        if (!val.Has("name")) {
            ctx.AddError(NYql::TIssue(
                TStringBuilder() << "Layer description must contain name field"));
            return false;
        }
        for (const auto& [name, _] : val.GetMap()) {
            if (name != "name" && name != "parent" && name != "url") {
                ctx.AddError(NYql::TIssue(
                    TStringBuilder() << "Layer unsupported attribute: " << name.Quote()));
                return false;
            }
        }

        return AddLayer(
            val["name"].GetString(),
            val.Has("parent") ? val["parent"].GetString() : TMaybe<TString>(),
            val.Has("url") ? val["url"].GetString() : TMaybe<TString>(),
            ctx);
    }

private:
    const THashMap<TString, IRemoteLayerProviderPtr> RemoteProviders_;
    const THashMap<TString, ILayersIntegrationPtr> Integrations_;
    THashMap<TString, TLayerInfo> InfoByUrl_;
    THashMap<TString, TLogicalInfo> LogicalInfoByName_;
};

class TDummyRemoteLayerProvider: public IRemoteLayerProvider {
public:
    TDummyRemoteLayerProvider(TString errorMessage)
        : ErrorMessage_(std::move(errorMessage))
    {
    }

    NThreading::TFuture<TLayerInfo> GetLayerInfo(const TMaybe<TString>&, const TString&) const override {
        return NThreading::MakeErrorFuture<TLayerInfo>(std::make_exception_ptr(yexception() << ErrorMessage_));
    }

private:
    TString ErrorMessage_;
};
} // namespace

namespace NYql::NLayers {

TMaybe<TVector<TLocations>> RemoveDuplicates(const TVector<std::pair<TKey, const TLayerInfo*>>& layers, TStringBuf system, const TString& cluster, TExprContext& ctx) {
    if (!layers.size()) {
        ctx.AddError(NYql::TIssue("No layers passed to RemoveDuplicates"));
        return {};
    }
    // transitive closure
    // [[1, 2], [3, 4], [2, 3]] => [[1, 2, 2, 3, 3, 4]] (in one layer info locations can contain duplicates,
    // but all locations inside one layer info are equivalent)
    TVector<size_t> DSU(layers.size());
    std::iota(DSU.begin(), DSU.end(), 0);
    std::function<size_t(size_t)> getParent = [&](size_t v) {
        return DSU[v] = (DSU[v] == v ? v : getParent(DSU[v]));
    };
    // N*Log(N), can make O(N*ackermann_function^{-1}(N)) when tracking component sizes
    auto unite = [&](size_t a, size_t b) {
        DSU[getParent(b)] = a;
    };

    THashMap<TString, size_t> firstAppearAt;
    THashSet<TString> baseLayerPaths;
    for (size_t i = 0; i < layers.size(); ++i) {
        auto& layer = layers[i];
        bool exists = false;
        for (auto& loc : layer.second->Locations) {
            if (loc.System != system || loc.Cluster != cluster) {
                continue;
            }
            exists = true;
            if (!layers[i].second->Parent) {
                baseLayerPaths.emplace(loc.Path);
            }
            auto [it, emplaced] = firstAppearAt.emplace(loc.Path, i);
            if (getParent(i) != getParent(it->second)) {
                unite(it->second, i);
            }
        }
        if (!exists) {
            ctx.AddError(NYql::TIssue(TStringBuilder() << "No location found for layer=" << layers[i].first.ToString() << ", system=" << system << ", cluster=" << cluster));
            return {};
        }
    }
    THashMap<size_t, size_t> present;
    TVector<TLocations> result;
    for (size_t i = 0; i < layers.size(); ++i) {
        auto [it, emplaced] = present.emplace(getParent(i), result.size());
        if (emplaced) {
            if (i && !layers[i].second->Parent) {
                TStringBuilder err;
                err << "Found base layer, that differs from another base layer (there are no common paths). Choises [";
                for (auto& loc : layers[i].second->Locations) {
                    if (loc.System != system || loc.Cluster != cluster) {
                        continue;
                    }
                    err << loc.Path.Quote();
                    break;
                }
                err << ", " << result.front().front().Path.Quote() << "]";
                ctx.AddError(NYql::TIssue(std::move(err)));
                return {};
            }
            result.emplace_back();
        }
        for (auto& loc : layers[i].second->Locations) {
            if (loc.System != system || loc.Cluster != cluster) {
                continue;
            }
            result[it->second].emplace_back(loc);
        }
    }
    return result;
}

ILayersRegistryPtr MakeLayersRegistry(const THashMap<TString, IRemoteLayerProviderPtr>& remoteProviders, const THashMap<TString, ILayersIntegrationPtr>& integrations) {
    return MakeIntrusive<TLayersRegistry>(remoteProviders, integrations);
}

IRemoteLayerProviderPtr MakeDummyRemoteLayerProvider(TString errorMessage) {
    return MakeIntrusive<TDummyRemoteLayerProvider>(std::move(errorMessage));
}

} // namespace NYql::NLayers
