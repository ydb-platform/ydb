#include "request_features.h"

#include <util/string/builder.h>
#include <util/string/join.h>

namespace NYql {

TFeaturesExtractor::TFeature::TFeature(TString&& value)
    : Value(std::move(value))
{}

TFeaturesExtractor::TFeature::TFeature(const TString& value)
    : Value(value)
{}

TFeaturesExtractor::TFeature::TFeature(TStringBuf value)
    : Value(TString(value))
{}

TFeaturesExtractor::TFeature::TFeature(THashMap<TString, TFeature>&& value)
    : Value(std::move(value))
{}

TFeaturesExtractor::TFeaturesExtractor(const THashMap<TString, TString>& features, const std::unordered_set<TString>& resetFeatures)
    : Features(features.begin(), features.end())
    , ResetFeatures(resetFeatures)
{}

TFeaturesExtractor::TFeaturesExtractor(THashMap<TString, TString>&& features, std::unordered_set<TString>&& resetFeatures)
    : Features(std::make_move_iterator(features.begin()), std::make_move_iterator(features.end()))
    , ResetFeatures(std::move(resetFeatures))
{}

TFeaturesExtractor::TFeaturesExtractor(TFeatures&& features, std::unordered_set<TString>&& resetFeatures)
    : Features(std::move(features))
    , ResetFeatures(std::move(resetFeatures))
{}

TFeaturesExtractor::TFeaturesExtractor(const TFeatures& features, const std::unordered_set<TString>& resetFeatures)
    : Features(features)
    , ResetFeatures(resetFeatures)
{}

bool TFeaturesExtractor::IsFinished() const {
    return Features.empty() && ResetFeatures.empty();
}

TString TFeaturesExtractor::GetRemainedParamsString() const {
    std::set<TString> features;

    GetRemainedFeatures(Features, "", features);

    for (const auto& feature : ResetFeatures) {
        features.emplace(feature);
    }

    return JoinSeq(", ", features);
}

std::optional<TString> TFeaturesExtractor::ValidateResetFeatures() const {
    std::set<TString> duplicateFeatures;
    for (const auto& feature : ResetFeatures) {
        if (Features.contains(feature)) {
            duplicateFeatures.emplace(feature);
        }
    }

    if (!duplicateFeatures.empty()) {
        return TStringBuilder() << "Duplicate reset feature: " << JoinSeq(", ", duplicateFeatures);
    }

    return std::nullopt;
}

std::optional<TString> TFeaturesExtractor::Extract(const TString& featureId) {
    const auto it = Features.find(featureId);
    if (it == Features.end() || !std::holds_alternative<TString>(it->second.Value)) {
        return {};
    }

    TString result = std::get<TString>(std::move(it->second.Value));
    Features.erase(it);
    return std::move(result);
}

std::optional<std::variant<TString, TFeaturesExtractor>> TFeaturesExtractor::ExtractNested(const TString& featureId) {
    const auto it = Features.find(featureId);
    if (it == Features.end()) {
        return {};
    }

    auto result = std::move(it->second.Value);
    Features.erase(it);

    if (std::holds_alternative<TString>(result)) {
        return std::get<TString>(std::move(result));
    }

    return TFeaturesExtractor(std::get<TFeatures>(std::move(result)), {});
}

bool TFeaturesExtractor::ExtractResetFeature(const TString& featureId) {
    const auto it = ResetFeatures.find(featureId);
    if (it == ResetFeatures.end()) {
        return false;
    }

    ResetFeatures.erase(it);
    return true;
}

void TFeaturesExtractor::GetRemainedFeatures(const TFeatures& features, const TString& prefix, std::set<TString>& result) {
    for (const auto& [key, value] : features) {
        auto name = TStringBuilder() << prefix << key;
        if (std::holds_alternative<TString>(value.Value)) {
            result.emplace(name);
        } else {
            GetRemainedFeatures(std::get<TFeatures>(value.Value), name << ".", result);
        }
    }
}

} // namespace NYql
