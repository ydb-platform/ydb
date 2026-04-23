#include "reset_upsert_opt.h"

#include <util/string/builder.h>
#include <util/string/strip.h>

namespace NKikimr::NKqp {

namespace {

static constexpr TStringBuf KnownResetTargets =
    "COMPACTION_PLANNER, METADATA_MEMORY_MANAGER, SCHEME_NEED_ACTUALIZATION, SCAN_READER_POLICY_NAME";

}

TConclusionStatus TResetUpsertOptionsOperation::DoDeserialize(NYql::TObjectSettingsImpl::TFeaturesExtractor& features) {
    auto resetTarget = features.Extract("RESET_TARGET");
    if (!resetTarget) {
        return TConclusionStatus::Fail(TStringBuilder() << "RESET_TARGET is required; known values: " << KnownResetTargets);
    }
    const TString target = StripString(*resetTarget);
    if (target == "COMPACTION_PLANNER") {
        Target = EResetTarget::CompactionPlanner;
        CompactionPlannerConstructor = NOlap::NStorageOptimizer::TOptimizerPlannerConstructorContainer(
            NOlap::NStorageOptimizer::IOptimizerPlannerConstructor::BuildDefault());
    } else if (target == "METADATA_MEMORY_MANAGER") {
        Target = EResetTarget::MetadataMemoryManager;
        MetadataManagerConstructor = NOlap::NDataAccessorControl::TMetadataManagerConstructorContainer(
            NOlap::NDataAccessorControl::IManagerConstructor::BuildDefault());
    } else if (target == "SCHEME_NEED_ACTUALIZATION") {
        Target = EResetTarget::SchemeNeedActualization;
    } else if (target == "SCAN_READER_POLICY_NAME") {
        Target = EResetTarget::ScanReaderPolicyName;
    } else {
        return TConclusionStatus::Fail(TStringBuilder() << "unknown RESET_TARGET: " << target << "; known values: " << KnownResetTargets);
    }
    return TConclusionStatus::Success();
}

void TResetUpsertOptionsOperation::DoSerializeScheme(NKikimrSchemeOp::TAlterColumnTableSchema& schemaData) const {
    switch (Target) {
        case EResetTarget::CompactionPlanner:
            if (CompactionPlannerConstructor.HasObject()) {
                CompactionPlannerConstructor.SerializeToProto(*schemaData.MutableOptions()->MutableCompactionPlannerConstructor());
            }
            break;
        case EResetTarget::MetadataMemoryManager:
            if (MetadataManagerConstructor.HasObject()) {
                MetadataManagerConstructor.SerializeToProto(*schemaData.MutableOptions()->MutableMetadataManagerConstructor());
            }
            break;
        case EResetTarget::SchemeNeedActualization:
            schemaData.MutableOptions()->SetSchemeNeedActualization(false);
            break;
        case EResetTarget::ScanReaderPolicyName:
            schemaData.MutableOptions()->SetScanReaderPolicyName(TString{});
            break;
        case EResetTarget::None:
            break;
    }
}

}
