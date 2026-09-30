#include "schema.h"

#include <ydb/core/tx/tiering/tier/identifier.h>

#include <vector>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

namespace {

std::vector<NColumnShard::NTiers::TExternalStorageId> GetTreeStorageIds(const NKikimrSchemeOp::TColumnDataLifeCycle& ttl) {
    std::vector<NColumnShard::NTiers::TExternalStorageId> result;
    for (auto&& tier : ttl.GetEnabled().GetTiers()) {
        if (tier.HasEvictToExternalStorage() && tier.GetEvictToExternalStorage().HasObjectKeyPrefix()) {
            const auto& settings = tier.GetEvictToExternalStorage();
            result.emplace_back(settings.GetStorage(), settings.GetObjectKeyPrefix());
        }
    }

    return result;
}

}

TConclusionStatus TOlapTTL::Update(const TOlapTTLUpdate& update) {
    const ui64 currentTtlVersion = Proto.GetVersion();
    const auto& ttlUpdate = update.GetPatch();
    if ((ttlUpdate.HasEnabled() || ttlUpdate.HasDisabled()) && GetTreeStorageIds(Proto) != GetTreeStorageIds(ttlUpdate)) {
        return TConclusionStatus::Fail("Changing tiering object key prefixes with ALTER is not supported yet; set them when creating the table");
    }

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
