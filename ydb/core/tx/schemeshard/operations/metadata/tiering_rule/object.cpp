#include "object.h"
#include "update.h"

#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

namespace NKikimr::NSchemeShard::NOperations {

TTieringRuleEntity::TFactory::TRegistrator<TTieringRuleEntity> TTieringRuleEntity::Registrator(NKikimrSchemeOp::EPathType::EPathTypeTieringRule);

[[nodiscard]] TConclusionStatus TTieringRuleEntity::DoInitialize(const TEntityInitializationContext& context) {
    auto* tieringRule = context.GetSSOperationContext()->SS->TieringRules.FindPtr(GetPathId());
    if (!tieringRule) {
        return TConclusionStatus::Fail("Tiering rule not found");
    }
    TieringRuleInfo = *tieringRule;
    return TConclusionStatus::Success();
}

    std::shared_ptr<TMetadataUpdateDrop> TTieringRuleEntity::GetDropUpdate() const {
        return std::make_shared<TDropTieringRule>();
    }

}   // namespace NKikimr::NSchemeShard::NOperations
