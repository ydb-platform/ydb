#include <library/cpp/testing/unittest/registar.h>

#include "layers.h"

using namespace NYql::NLayers;

class TDummyRemote: public IRemoteLayerProvider {
public:
    NThreading::TFuture<TLayerInfo> GetLayerInfo(const TMaybe<TString>& parent, const TString& url) const override {
        if (parent) {
            throw yexception() << "Can't use parent here";
        }
        if (url == "dummy://1") {
            return NThreading::MakeFuture<TLayerInfo>(TLayerInfo{
                .Locations = {
                    TLocation{
                        .System = "yt",
                        .Cluster = "hahn",
                        .Path = "2"},
                    TLocation{
                        .System = "yt",
                        .Cluster = "hahn",
                        .Path = "3"}},
                .Parent = TKey({}, "dummy://2")});
        }

        if (url == "dummy://2") {
            return NThreading::MakeFuture<TLayerInfo>(TLayerInfo{
                .Locations = {
                    TLocation{
                        .System = "yt",
                        .Cluster = "hahn",
                        .Path = "3"},
                    TLocation{
                        .System = "yt",
                        .Cluster = "hahn",
                        .Path = "4"}},
                .Parent = TKey({}, "dummy://3")});
        }

        if (url == "dummy://3") {
            return NThreading::MakeFuture<TLayerInfo>(TLayerInfo{
                .Locations = {
                    TLocation{
                        .System = "yt",
                        .Cluster = "hahn",
                        .Path = "1"},
                    TLocation{
                        .System = "yt",
                        .Cluster = "hahn",
                        .Path = "2"}}});
        }
        throw yexception() << "???";
    }
};

class TDummyIntegration: public ILayersIntegration {
public:
    TMaybe<TVector<TLocation>> ResolveLayers(const TVector<TKey>& order, const TString& cluster, NYql::TExprContext& ctx) const override {
        TVector<std::pair<TKey, const TLayerInfo*>> infos;
        infos.reserve(order.size());
        for (auto& e : order) {
            infos.emplace_back(e, Data_.FindPtr(e));
        }
        auto layers = RemoveDuplicates(infos, "yt", cluster, ctx);
        if (!layers) {
            return {};
        }
        TVector<TLocation> result;
        result.reserve(layers->size());
        for (auto& e : *layers) {
            result.emplace_back(e.front());
        }
        return result;
    }

    bool UpdateLayerCache(const TKey& key, const TLayerInfo& layer, NYql::TExprContext&) override {
        Data_.emplace(key, layer);
        return true;
    }

    bool UpdateLayerLocations(const TKey&, TVector<TLocation>&&, NYql::TExprContext&) override {
        return true;
    }

    void RemoveLayerByName(const TString&) override {
    }

private:
    THashMap<TKey, TLayerInfo> Data_;
};

ILayersRegistryPtr MakeRegistry() {
    return MakeLayersRegistry({{"dummy", MakeIntrusive<TDummyRemote>()}},
                              {{"yt", MakeIntrusive<TDummyIntegration>()}});
};

Y_UNIT_TEST_SUITE(TLayers) {
Y_UNIT_TEST(InvariantsTest) {
    auto reg = MakeRegistry();
    auto ctx = NYql::TExprContext();
    UNIT_ASSERT(reg->AddLayer("11", {}, {}, ctx));
    UNIT_ASSERT(!reg->AddLayer("11", {}, {}, ctx));
    UNIT_ASSERT(reg->AddLayer("1", {}, {}, ctx));

    UNIT_ASSERT(reg->AddLayer("Pen", {}, {}, ctx));
    UNIT_ASSERT(reg->AddLayer("ApplePen", "Pen", {}, ctx));
    UNIT_ASSERT(reg->AddLayer("PineappleApplePen", "ApplePen", {}, ctx));
    auto layers = reg->ResolveLogicalLayers({{"Pen", "PineappleApplePen"}}, ctx);
    UNIT_ASSERT(layers);
    TVector<TString> exp = {"Pen", "ApplePen", "PineappleApplePen"};
    UNIT_ASSERT_EQUAL(layers->size(), exp.size());
    for (size_t i = 0; i < layers->size(); ++i) {
        UNIT_ASSERT((*layers)[i].Name);
        UNIT_ASSERT_STRINGS_EQUAL(exp[i], *((*layers)[i].Name));
    }
    layers = reg->ResolveLogicalLayers({{"PineappleApplePen"}}, ctx);
    UNIT_ASSERT(layers);
    UNIT_ASSERT_EQUAL(layers->size(), exp.size());
    for (size_t i = 0; i < layers->size(); ++i) {
        UNIT_ASSERT((*layers)[i].Name);
        UNIT_ASSERT_STRINGS_EQUAL(exp[i], *((*layers)[i].Name));
    }
    UNIT_ASSERT(reg->AddLayer("foo", {}, "dummy://1", ctx));

    layers = reg->ResolveLogicalLayers({{"foo"}}, ctx);
    UNIT_ASSERT(layers);
    auto resolved = reg->ResolveLayers(*layers, "yt", "hahn", ctx);
    UNIT_ASSERT(resolved);
    UNIT_ASSERT_EQUAL(resolved->size(), 1);

    UNIT_ASSERT(reg->AddLayer("bar", {}, "dummy://2", ctx));

    layers = reg->ResolveLogicalLayers({{"bar"}}, ctx);
    UNIT_ASSERT(layers);
    resolved = reg->ResolveLayers(*layers, "yt", "hahn", ctx);
    UNIT_ASSERT(resolved);
    UNIT_ASSERT_EQUAL(resolved->size(), 2);

    layers = reg->ResolveLogicalLayers({{"bar", "foo"}}, ctx);
    UNIT_ASSERT(layers);
    resolved = reg->ResolveLayers(*layers, "yt", "hahn", ctx);
    UNIT_ASSERT(resolved);
    UNIT_ASSERT_EQUAL(resolved->size(), 1);
}

} // Y_UNIT_TEST_SUITE(TLayers)
