#include "abstract.h"
#include "arrow.h"
namespace NKikimr::NArrow::NSerialization {

NKikimr::TConclusionStatus TSerializerContainer::DeserializeFromProto(const NKikimrSchemeOp::TCompressionOptions& proto) {
    NKikimrSchemeOp::TOlapColumn::TSerializer serializerProto;
    serializerProto.SetClassName(NArrow::NSerialization::TArrowSerializer::GetClassNameStatic());
    *serializerProto.MutableArrowCompression() = proto;
    AFL_VERIFY(Initialize(NArrow::NSerialization::TArrowSerializer::GetClassNameStatic()));
    return GetObjectPtr()->DeserializeFromProto(serializerProto);
}

NKikimr::TConclusionStatus TSerializerContainer::DeserializeFromRequest(NYql::TFeaturesExtractor& features) {
    const std::optional<TString> className = features.Extract("SERIALIZER.CLASS_NAME");
    if (!className) {
        return TConclusionStatus::Success();
    }
    if (!TBase::Initialize(*className)) {
        return TConclusionStatus::Fail("dont know anything about class_name=" + *className);
    }
    return TBase::GetObjectPtr()->DeserializeFromRequest(features);
}

}
