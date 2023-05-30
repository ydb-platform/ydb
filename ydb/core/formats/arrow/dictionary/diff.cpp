#include "diff.h"
#include "object.h"
#include <util/string/cast.h>

namespace NKikimr::NArrow::NDictionary {

NKikimrSchemeOp::TDictionaryEncodingSettings TEncodingDiff::SerializeToProto() const {
    NKikimrSchemeOp::TDictionaryEncodingSettings result;
    if (Enabled) {
        result.SetEnabled(*Enabled);
    }
    return result;
}

TConclusionStatus TEncodingDiff::DeserializeFromRequestFeatures(NYql::TFeaturesExtractor& features) {
    {
        auto fValue = features.Extract("ENCODING.DICTIONARY.ENABLED");
        if (fValue) {
            bool enabled = false;
            if (!TryFromString<bool>(*fValue, enabled)) {
                return TConclusionStatus::Fail("cannot parse COMPRESSTION.DICTIONARY.ENABLED as boolean");
            }
            Enabled = enabled;
        }
    }
    return TConclusionStatus::Success();
}

bool TEncodingDiff::DeserializeFromProto(const NKikimrSchemeOp::TDictionaryEncodingSettings& proto) {
    if (proto.HasEnabled()) {
        Enabled = proto.GetEnabled();
    }
    return true;
}

TConclusionStatus TEncodingDiff::Apply(std::optional<TEncodingSettings>& settings) const {
    if (IsEmpty()) {
        return TConclusionStatus::Success();
    }
    if (!settings) {
        settings = TEncodingSettings();
    }
    if (Enabled) {
        settings->Enabled = *Enabled;
    }
    return TConclusionStatus::Success();
}

}
