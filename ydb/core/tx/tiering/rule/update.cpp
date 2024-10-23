#include "update.h"

#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/tx/tiering/rule/behaviour.h>

namespace NKikimr::NColumnShard::NTiers {

TTieringRuleUpdateBehaviour::TFactoryByPropertiesImpl::TRegistrator<TTieringRuleUpdateBehaviour>
    TTieringRuleUpdateBehaviour::RegistratorByPropertiesImpl(TTieringRuleUpdateBehaviour::PropertiesImplCase);
TTieringRuleUpdateBehaviour::TFactoryByPath::TRegistrator<TTieringRuleUpdateBehaviour> TTieringRuleUpdateBehaviour::RegistratorByPath(
    TTieringRuleUpdateBehaviour::PathType);

TTieringRuleValidator::TSchemeConclusionStatus TTieringRuleValidator::ValidatePath() const {
    const TString storagePath = NColumnShard::NTiers::TTieringRuleBehaviour().GetStorageTablePath();
    if (!IsEqualPaths(Parent.PathString(), storagePath)) {
        return TSchemeConclusionStatus::Fail("Tiering rules must be placed at " + storagePath + ", got " + Parent.PathString());
    }
    return TSchemeConclusionStatus::Success();
}

TTieringRuleValidator::TSchemeConclusionStatus TTieringRuleValidator::ValidateProperties(
    const NSchemeShard::IMetadataObjectProperties::TPtr object) const {
    const auto tieringRule = std::dynamic_pointer_cast<TTieringRule>(object);
    AFL_VERIFY(tieringRule);
    if (tieringRule->GetDefaultColumn().Empty()) {
        return TSchemeConclusionStatus::Fail("Empty default column");
    }
    if (tieringRule->GetIntervals().empty()) {
        return TSchemeConclusionStatus::Fail("Tiering rule must contain at least 1 tier");
    }
    return TSchemeConclusionStatus::Success();
}

TTieringRuleValidator::TSchemeConclusionStatus TTieringRuleValidator::ValidateAlter(
    const NSchemeShard::IMetadataObjectProperties::TPtr, const NKikimrSchemeOp::TMetadataObjectProperties&) const {
    return TSchemeConclusionStatus::Success();
}

TTieringRuleValidator::TSchemeConclusionStatus TTieringRuleValidator::ValidateDrop(const NSchemeShard::IMetadataObjectProperties::TPtr) const {
    if (auto tables = Ctx.SS->ColumnTables.GetTablesWithTiering(Name); !tables.empty()) {
        const TString tableString = NSchemeShard::TPath::Init(*tables.begin(), Ctx.SS).PathString();
        return TSchemeConclusionStatus::Fail("Tiering is in use by column table: " + tableString);
    }

    return TSchemeConclusionStatus::Success();
}
}   // namespace NKikimr::NColumnShard::NTiers
