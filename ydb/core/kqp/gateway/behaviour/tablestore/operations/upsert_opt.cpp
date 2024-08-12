#include "upsert_opt.h"
#include <util/string/type.h>
#include <library/cpp/json/json_reader.h>

namespace NKikimr::NKqp {

TConclusionStatus TUpsertOptionsOperation::DoDeserialize(NYql::TObjectSettingsImpl::TFeaturesExtractor& features) {
    auto value = features.Extract<bool>("SCHEME_NEED_ACTUALIZATION", false);
    if (!value) {
        return TConclusionStatus::Fail("Incorrect value for SCHEME_NEED_ACTUALIZATION: cannot parse as boolean");
    }
    SchemeNeedActualization = *value;
    ExternalGuaranteeExclusivePK = features.Extract<bool>("EXTERNAL_GUARANTEE_EXCLUSIVE_PK");
    if (const auto className = features.Extract<TString>("COMPACTION_PLANNER.CLASS_NAME")) {
        if (!CompactionPlannerConstructor.Initialize(*className)) {
            return TConclusionStatus::Fail("incorrect class name for compaction planner:" + *className);
        }

        NJson::TJsonValue jsonData = NJson::JSON_MAP;
        auto fValue = features.Extract("COMPACTION_PLANNER.FEATURES");
        if (fValue) {
            if (!NJson::ReadJsonFastTree(*fValue, &jsonData)) {
                return TConclusionStatus::Fail("incorrect json in request COMPACTION_PLANNER.FEATURES parameter");
            }
        }
        auto result = CompactionPlannerConstructor->DeserializeFromJson(jsonData);
        if (result.IsFail()) {
            return result;
        }
    }

    return TConclusionStatus::Success();
}

void TUpsertOptionsOperation::DoSerializeScheme(NKikimrSchemeOp::TAlterColumnTableSchema& schemaData) const {
    schemaData.MutableOptions()->SetSchemeNeedActualization(SchemeNeedActualization);
    if (ExternalGuaranteeExclusivePK) {
        schemaData.MutableOptions()->SetExternalGuaranteeExclusivePK(*ExternalGuaranteeExclusivePK);
    }
    if (CompactionPlannerConstructor.HasObject()) {
        CompactionPlannerConstructor.SerializeToProto(*schemaData.MutableOptions()->MutableCompactionPlannerConstructor());
    }
}

}
