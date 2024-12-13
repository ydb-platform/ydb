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
    ScanReaderPolicyName = features.Extract<TString>("SCAN_READER_POLICY_NAME");
    if (ScanReaderPolicyName) {
        if (*ScanReaderPolicyName != "PLAIN" && *ScanReaderPolicyName != "SIMPLE") {
            return TConclusionStatus::Fail("SCAN_READER_POLICY_NAME have to be in ['PLAIN', 'SIMPLE']");
        }
    }
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

    if (const auto className = features.Extract<TString>("METADATA_MEMORY_MANAGER.CLASS_NAME")) {
        if (!MetadataManagerConstructor.Initialize(*className)) {
            return TConclusionStatus::Fail("incorrect class name for metadata manager:" + *className);
        }

        NJson::TJsonValue jsonData = NJson::JSON_MAP;
        auto fValue = features.Extract("METADATA_MEMORY_MANAGER.FEATURES");
        if (fValue) {
            if (!NJson::ReadJsonFastTree(*fValue, &jsonData)) {
                return TConclusionStatus::Fail("incorrect json in request METADATA_MEMORY_MANAGER.FEATURES parameter");
            }
        }
        auto result = MetadataManagerConstructor->DeserializeFromJson(jsonData);
        if (result.IsFail()) {
            return result;
        }
    }

    return TConclusionStatus::Success();
}

void TUpsertOptionsOperation::DoSerializeScheme(NKikimrSchemeOp::TAlterColumnTableSchema& schemaData) const {
    schemaData.MutableOptions()->SetSchemeNeedActualization(SchemeNeedActualization);
    if (ScanReaderPolicyName) {
        schemaData.MutableOptions()->SetScanReaderPolicyName(*ScanReaderPolicyName);
    }
    if (CompactionPlannerConstructor.HasObject()) {
        CompactionPlannerConstructor.SerializeToProto(*schemaData.MutableOptions()->MutableCompactionPlannerConstructor());
    }
    if (MetadataManagerConstructor.HasObject()) {
        MetadataManagerConstructor.SerializeToProto(*schemaData.MutableOptions()->MutableMetadataManagerConstructor());
    }
}

}
