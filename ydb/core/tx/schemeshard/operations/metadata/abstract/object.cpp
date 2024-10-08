#include "object.h"
#include "update.h"

namespace NKikimr::NSchemeShard::NOperations {

TConclusion<std::shared_ptr<ISSEntityUpdate>> TMetadataEntity::DoCreateUpdate(const TUpdateInitializationContext& context) const {
    auto update = TMetadataUpdate::MakeUpdate(*context.GetModification());
    if (auto status = update->Initialize(context); status.IsFail()) {
        return status;
    }
    return update;
}

TConclusion<std::shared_ptr<ISSEntityUpdate>> TMetadataEntity::DoRestoreUpdate(const TUpdateRestoreContext& /*context*/) const {
    return TConclusionStatus::Fail("Restoring updates is not supported for metadata objects");
}

}   // namespace NKikimr::NSchemeShard::NOperations
