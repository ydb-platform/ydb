#include "request.h"
#include "constructor.h"

namespace NKikimr::NArrow::NAccessor::NSubColumns {

NKikimrArrowAccessorProto::TRequestedConstructor TRequestedConstuctor::DoSerializeToProto() const {
    return NKikimrArrowAccessorProto::TRequestedConstructor();
}

bool TRequestedConstuctor::DoDeserializeFromProto(const NKikimrArrowAccessorProto::TRequestedConstructor& /*proto*/) {
    return true;
}

NKikimr::TConclusionStatus TRequestedConstuctor::DoDeserializeFromRequest(NYql::TFeaturesExtractor& /*features*/) {
    return TConclusionStatus::Success();
}

NKikimr::TConclusion<TConstructorContainer> TRequestedConstuctor::DoBuildConstructor() const {
    return std::make_shared<TConstructor>();
}

}
