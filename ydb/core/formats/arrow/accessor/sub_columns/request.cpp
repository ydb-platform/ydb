#include "constructor.h"
#include "data_extractor.h"
#include "request.h"

namespace NKikimr::NArrow::NAccessor::NSubColumns {

NKikimrArrowAccessorProto::TRequestedConstructor TRequestedConstuctor::DoSerializeToProto() const {
    NKikimrArrowAccessorProto::TRequestedConstructor result;
    *result.MutableSubColumns()->MutableSettings() = Settings.SerializeToRequestedProto();
    return result;
}

bool TRequestedConstuctor::DoDeserializeFromProto(const NKikimrArrowAccessorProto::TRequestedConstructor& proto) {
    return Settings.DeserializeFromRequestedProto(proto.GetSubColumns().GetSettings());
}

TConclusionStatus TRequestedConstuctor::DoDeserializeFromRequest(NYql::TFeaturesExtractor& features) {
    if (auto columnsLimit = features.Extract<ui32>("COLUMNS_LIMIT")) {
        Settings.SetColumnsLimit(*columnsLimit);
    }
    if (auto kff = features.Extract<ui32>("SPARSED_DETECTOR_KFF")) {
        Settings.SetSparsedDetectorKff(*kff);
    }
    if (auto dataExtractorClassName = features.Extract<TString>("DATA_EXTRACTOR_CLASS_NAME")) {
        auto extractor = IDataAdapter::TFactory::MakeHolder(*dataExtractorClassName);
        if (!extractor) {
            return TConclusionStatus::Fail("incorrect data extractor class name");
        }
        auto parseConclusion = extractor->DeserializeFromRequest(features);
        if (parseConclusion.IsFail()) {
            return parseConclusion;
        }
        Settings.SetDataExtractor(std::shared_ptr<IDataAdapter>(extractor.Release()));
    } else {
        Settings.SetDataExtractor(std::make_shared<TJsonScanExtractor>(false));
    }
    if (auto memLimit = features.Extract<ui32>("MEM_LIMIT_CHUNK")) {
        Settings.SetChunkMemoryLimit(*memLimit);
    }
    if (auto othersFraction = features.Extract<double>("OTHERS_ALLOWED_FRACTION")) {
        if (*othersFraction < 0 || 1 < *othersFraction) {
            return TConclusionStatus::Fail("others fraction have to be in [0, 1] interval");
        }
        Settings.SetOthersAllowedFraction(*othersFraction);
    }
    return TConclusionStatus::Success();
}

NKikimr::TConclusion<TConstructorContainer> TRequestedConstuctor::DoBuildConstructor() const {
    return std::make_shared<TConstructor>(Settings);
}

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
