#include "reset_upsert_opt.h"

namespace NKikimr::NKqp {

TConclusionStatus TResetUpsertOptionsOperation::DoDeserialize(NYql::TObjectSettingsImpl::TFeaturesExtractor& /*features*/) {
    CompactionPlannerConstructor = NOlap::NStorageOptimizer::TOptimizerPlannerConstructorContainer(
        NOlap::NStorageOptimizer::IOptimizerPlannerConstructor::BuildDefault());
    return TConclusionStatus::Success();
}

void TResetUpsertOptionsOperation::DoSerializeScheme(NKikimrSchemeOp::TAlterColumnTableSchema& schemaData) const {
    if (CompactionPlannerConstructor.HasObject()) {
        CompactionPlannerConstructor.SerializeToProto(*schemaData.MutableOptions()->MutableCompactionPlannerConstructor());
    }
}

}
