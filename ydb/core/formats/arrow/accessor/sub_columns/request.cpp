#include "request.h"
#include "constructor.h"

namespace NKikimr::NArrow::NAccessor::NSubColumns {

NKikimrArrowAccessorProto::TRequestedConstructor TRequestedConstuctor::DoSerializeToProto() const {
    NKikimrArrowAccessorProto::TRequestedConstructor result;
    *result.MutableSubColumns()->MutableSettings() = Settings.SerializeToRequestedProto();
    return result;
}

bool TRequestedConstuctor::DoDeserializeFromProto(const NKikimrArrowAccessorProto::TRequestedConstructor& proto) {
    return Settings.DeserializeFromRequestedProto(proto.GetSubColumns().GetSettings());
}

NKikimr::TConclusionStatus TRequestedConstuctor::DoDeserializeFromRequest(NYql::TFeaturesExtractor& features) {
    if (auto columnsLimit = features.Extract<ui32>("COLUMNS_LIMIT")) {
        Settings.SetColumnsLimit(*columnsLimit);
    }
    if (auto kff = features.Extract<ui32>("SPARSED_DETECTOR_KFF")) {
        Settings.SetSparsedDetectorKff(*kff);
    }
    if (auto memLimit = features.Extract<ui32>("MEM_LIMIT_CHUNK")) {
        Settings.SetChunkMemoryLimit(*memLimit);
    }
    return TConclusionStatus::Success();
}

NKikimr::TConclusion<TConstructorContainer> TRequestedConstuctor::DoBuildConstructor() const {
    return std::make_shared<TConstructor>(Settings);
}

}
