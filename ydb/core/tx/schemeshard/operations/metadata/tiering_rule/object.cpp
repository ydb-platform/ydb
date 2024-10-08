#include "object.h"
#include "update.h"

#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

namespace NKikimr::NSchemeShard::NOperations {

[[nodiscard]] TConclusionStatus TTieringRuleEntity::DoInitialize(const TEntityInitializationContext& context) {
    {
        auto* tieringRule = context.GetSSOperationContext()->SS->TieringRules.FindPtr(GetPathId());
        AFL_VERIFY(tieringRule);
        TieringRuleInfo = *tieringRule;
    }
    return TConclusionStatus::Success();
}

}   // namespace NKikimr::NSchemeShard::NOperations
