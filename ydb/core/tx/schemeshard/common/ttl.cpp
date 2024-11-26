#include "ttl.h"

namespace NKikimr::NSchemeShard {

TConclusion<TDuration> GetExpireAfter(const NKikimrSchemeOp::TTTLSettings::TEnabled& settings) {
    if (settings.TiersSize()) {
        for (const auto& tier : settings.GetTiers()) {
            if (tier.HasDelete()) {
                return TDuration::Seconds(tier.GetApplyAfterSeconds());
            }
        }
        return TConclusionStatus::Fail("TTL settings does not contain DELETE action");
    } else {
        // legacy format
        return TDuration::Seconds(settings.GetExpireAfterSeconds());
    }
}

}   // namespace NKikimr
