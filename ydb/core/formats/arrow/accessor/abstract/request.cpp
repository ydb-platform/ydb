#include "request.h"

namespace NKikimr::NArrow::NAccessor {

TConclusionStatus TRequestedConstructorContainer::DeserializeFromRequest(NYql::TFeaturesExtractor& features) {
    const std::optional<TString> className = features.Extract("DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME");
    if (!className) {
        return TConclusionStatus::Success();
    }
    if (!TBase::Initialize(*className)) {
        return TConclusionStatus::Fail("don't know anything about class_name=" + *className);
    }
    return TBase::GetObjectPtr()->DeserializeFromRequest(features);
}

}
