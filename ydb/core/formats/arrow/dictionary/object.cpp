#include "object.h"
#include <ydb/core/formats/arrow/transformer/dictionary.h>
#include <ydb/library/formats/arrow/common/validation.h>
#include <util/string/builder.h>

namespace NKikimr::NArrow::NDictionary {

TConclusionStatus TEncodingSettings::DeserializeFromProto(const NKikimrSchemeOp::TDictionaryEncodingSettings& proto) {
    if (proto.HasEnabled()) {
        Enabled = proto.GetEnabled();
    }
    return TConclusionStatus::Success();
}

NKikimrSchemeOp::TDictionaryEncodingSettings TEncodingSettings::SerializeToProto() const {
    NKikimrSchemeOp::TDictionaryEncodingSettings result;
    result.SetEnabled(Enabled);
    return result;
}

TString TEncodingSettings::DebugString() const {
    TStringBuilder sb;
    sb << "enabled=" << Enabled << ";";
    return sb;
}

NTransformation::ITransformer::TPtr TEncodingSettings::BuildEncoder() const {
    if (Enabled) {
        return std::make_shared<NArrow::NTransformation::TDictionaryPackTransformer>();
    } else {
        return nullptr;
    }
}

NTransformation::ITransformer::TPtr TEncodingSettings::BuildDecoder() const {
    if (Enabled) {
        return std::make_shared<NArrow::NTransformation::TDictionaryUnpackTransformer>();
    } else {
        return nullptr;
    }
}

}
