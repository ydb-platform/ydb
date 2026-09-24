#include "parsing.h"

namespace NYql {

using namespace NNodes;

TObjectSettingsImpl::TObjectSettingsImpl(const TString& typeId, const TString& objectId, const THashMap<TString, TString>& features, const TResetFeatures& resetFeatures)
    : TypeId(typeId)
    , ObjectId(objectId)
    , Features(features.begin(), features.end())
    , ResetFeatures(resetFeatures)
    , FeaturesExtractor(std::make_shared<TFeaturesExtractor>(Features, ResetFeatures))
{}

TFeaturesExtractor& TObjectSettingsImpl::GetFeaturesExtractor() const {
    Y_ABORT_UNLESS(!!FeaturesExtractor);
    return *FeaturesExtractor;
}

void TObjectSettingsImpl::DeserializeFeatures(const TCoNameValueTupleList& features, TFeaturesExtractor::TFeatures& result) {
    for (const auto& feature : features) {
        if (const auto& maybeAtom = feature.Maybe<TCoAtom>()) {
            result.emplace(maybeAtom.Cast(), ""sv);
        } else if (const auto& maybeTuple = feature.Maybe<TCoNameValueTuple>()) {
            const auto& tuple = maybeTuple.Cast();
            const TString name(tuple.Name());
            auto value = tuple.Value();
            if (const auto literal = value.Maybe<TCoJust>().Input().Maybe<TCoDataCtor>()) {
                value = literal.Cast();
            }
            if (const auto& maybeAtom = value.Maybe<TCoAtom>()) {
                result.emplace(name, maybeAtom.Cast());
            } else if (value.Maybe<TCoIntegralCtor>() || value.Maybe<TCoBool>() || value.Maybe<TCoString>() || value.Maybe<TCoUtf8>() || value.Maybe<TCoTimestamp>()) {
                result.emplace(name, value.Cast<TCoDataCtor>().Literal().Cast<TCoAtom>());
            } else if (const auto& maybeList = value.Maybe<TCoNameValueTupleList>()) {
                TFeaturesExtractor::TFeatures features;
                DeserializeFeatures(maybeList.Cast(), features);
                result.emplace(name, std::move(features));
            }
        }
    }
}

} // namespace NYql
