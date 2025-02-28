#include "collection.h"
#include "projection.h"

namespace NKikimr::NArrow::NSSA {

TConclusionStatus TProjectionProcessor::DoExecute(const std::shared_ptr<TAccessorsCollection>& resources) const {
    resources->RemainOnly(TColumnChainInfo::ExtractColumnIds(GetInput()), true);
    return TConclusionStatus::Success();
}

}   // namespace NKikimr::NArrow::NSSA
