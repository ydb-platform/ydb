#include "constructor.h"
#include "request.h"

namespace NKikimr::NArrow::NAccessor::NCompactKV {

NKikimrArrowAccessorProto::TRequestedConstructor TRequestedConstuctor::DoSerializeToProto() const {
    NKikimrArrowAccessorProto::TRequestedConstructor result;
    *result.MutableCompactKV() = Settings.SerializeToRequestedProto();
    return result;
}

bool TRequestedConstuctor::DoDeserializeFromProto(const NKikimrArrowAccessorProto::TRequestedConstructor& proto) {
    return Settings.DeserializeFromRequestedProto(proto.GetCompactKV());
}

TConclusionStatus TRequestedConstuctor::DoDeserializeFromRequest(NYql::TFeaturesExtractor& features) {
    if (auto parseNested = features.Extract<bool>("PARSE_NESTED")) {
        Settings.SetParseNested(*parseNested);
    }
    return TConclusionStatus::Success();
}

TConclusion<TConstructorContainer> TRequestedConstuctor::DoBuildConstructor() const {
    return std::make_shared<TConstructor>(Settings);
}

}   // namespace NKikimr::NArrow::NAccessor::NCompactKV
