#include "schema.h"

namespace NKikimr::NSchemeShard::NOlap::NAlter {

TConclusionStatus TOlapTTL::Update(const TOlapTTLUpdate& update) {
    const ui64 currentTtlVersion = Proto.GetVersion();
    const auto& ttlUpdate = update.GetPatch();
    if (ttlUpdate.HasEnabled()) {
        *Proto.MutableEnabled() = ttlUpdate.GetEnabled();
    }
    if (ttlUpdate.HasDisabled()) {
        *Proto.MutableDisabled() = ttlUpdate.GetDisabled();
    }
    Proto.SetVersion(currentTtlVersion + 1);
    return TConclusionStatus::Success();
}

}