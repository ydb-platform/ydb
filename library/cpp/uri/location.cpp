#include "location.h"
#include "uri.h"

namespace NUri {
    static const ui64 URI_PARSE_FLAGS =
        (TFeature::FeaturesRecommended | TFeature::FeatureConvertHostIDN | TFeature::FeatureEncodeExtendedDelim | TFeature::FeatureEncodePercent) & ~TFeature::FeatureHashBangToEscapedFragment;

    TString ResolveRedirectLocation(const TStringBuf& baseUrl,
                                    const TStringBuf& location) {
        TUri baseUri;
        TUri locationUri;

        // Parse base URL.
        if (baseUri.Parse(baseUrl, URI_PARSE_FLAGS) != NUri::TState::ParsedOK) {
            return "";
        }
        // Parse location with respect to the base URL.
        if (locationUri.Parse(location, baseUri, URI_PARSE_FLAGS) != NUri::TState::ParsedOK) {
            return "";
        }
        // Inherit fragment.
        if (!locationUri.GetField(NUri::TField::FieldFragment)) {
            NUri::TUriUpdate update(locationUri);
            update.Set(NUri::TField::FieldFragment, baseUri.GetField(NUri::TField::FieldFragment));
        }
        TString res;
        locationUri.Print(res, NUri::TField::FlagAllFields);
        return res;
    }

}
