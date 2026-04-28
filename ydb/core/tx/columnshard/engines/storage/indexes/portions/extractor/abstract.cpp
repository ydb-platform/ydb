#include "abstract.h"
#include "default.h"
#include "sub_column.h"

namespace NKikimr::NOlap::NIndexes {

TConclusionStatus TReadDataExtractorContainer::DeserializeFromJson(const NJson::TJsonValue& jsonValue) {
    if (!jsonValue.IsDefined() || jsonValue.IsNull()) {
        if (!Initialize(TDefaultDataExtractor::GetClassNameStatic())) {
            return TConclusionStatus::Fail(
                "cannot build default data extractor ('" + TDefaultDataExtractor::GetClassNameStatic() + "')");
        }
        return TConclusionStatus::Success();
    }
    const TString className = jsonValue["class_name"].GetStringRobust();
    auto dataExtractor = IReadDataExtractor::TFactory::MakeHolder(className);
    if (!dataExtractor) {
        return TConclusionStatus::Fail("cannot build data extractor (unexpected class name: '" + className + "')");
    }
    auto parseConclusion = dataExtractor->DeserializeFromJson(jsonValue);
    if (parseConclusion.IsFail()) {
        return parseConclusion;
    }
    Object = std::shared_ptr<IReadDataExtractor>(dataExtractor.Release());
    return TConclusionStatus::Success();
}

bool TReadDataExtractorContainer::DeserializeFromProto(const IReadDataExtractor::TProto& data) {
    if (!data.GetClassName()) {
        AFL_VERIFY(Initialize(TDefaultDataExtractor::GetClassNameStatic()));
        return true;
    } else {
        return TBase::DeserializeFromProto(data);
    }
}

bool TReadDataExtractorContainer::HasSubColumn() const {
    return Object && Object->GetClassName() == TSubColumnDataExtractor::GetClassNameStatic();
}

}   // namespace NKikimr::NOlap::NIndexes
