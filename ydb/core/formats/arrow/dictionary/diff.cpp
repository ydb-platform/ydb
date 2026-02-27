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
