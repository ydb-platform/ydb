#include "update.h"

#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/tx/tiering/rule/behaviour.h>

namespace NKikimr::NSchemeShard::NOperations {

TTieringRuleUpdateBehaviour::TFactoryByPropertiesImpl::TRegistrator<TTieringRuleUpdateBehaviour> TTieringRuleUpdateBehaviour::RegistratorByPropertiesImpl(
    TTieringRuleUpdateBehaviour::PropertiesImplCase);
TTieringRuleUpdateBehaviour::TFactoryByPath::TRegistrator<TTieringRuleUpdateBehaviour> TTieringRuleUpdateBehaviour::RegistratorByPath(
    TTieringRuleUpdateBehaviour::PathType);

TTieringRuleValidator::TSchemeConclusionStatus TTieringRuleValidator::ValidatePath() const {
    const TString storagePath = NColumnShard::NTiers::TTieringRuleBehaviour().GetStorageTablePath();
    if (!IsEqualPaths(Parent.PathString(), storagePath)) {
        return TSchemeConclusionStatus::Fail("Tiering rules must be placed at " + storagePath + ", got " + Parent.PathString());
    }
    return TSchemeConclusionStatus::Success();
}

TTieringRuleValidator::TSchemeConclusionStatus TTieringRuleValidator::ValidateObject(const TMetadataObjectInfo::TPtr object) const {
    const auto& properties = object->GetPropertiesVerified<TTieringRuleInfo>();
    if (properties.DefaultColumn.Empty()) {
        return TSchemeConclusionStatus::Fail("Empty default column");
    }
    if (properties.Intervals.empty()) {
        return TSchemeConclusionStatus::Fail("Tiering rule must contain at least 1 tier");
    }
    return TSchemeConclusionStatus ::Success();
}

TTieringRuleValidator::TSchemeConclusionStatus TTieringRuleValidator::ValidateAlter(
    const TMetadataObjectInfo::TPtr, const NKikimrSchemeOp::TMetadataObjectProperties&) const {
    return TSchemeConclusionStatus::Success();
}

TTieringRuleValidator::TSchemeConclusionStatus TTieringRuleValidator::ValidateDrop(const TMetadataObjectInfo::TPtr) const {
    if (auto tables = Ctx.SS->ColumnTables.GetTablesWithTiering(Name); !tables.empty()) {
        const TString tableString = TPath::Init(*tables.begin(), Ctx.SS).PathString();
        return TSchemeConclusionStatus::Fail("Tiering is in use by column table: " + tableString);
    }

    return TSchemeConclusionStatus::Success();
}
}   // namespace NKikimr::NSchemeShard::NOperations
