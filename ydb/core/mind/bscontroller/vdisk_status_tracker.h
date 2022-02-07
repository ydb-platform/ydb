#pragma once

#include "defs.h"

namespace NKikimr::NBsController {

    class TVDiskStatusTracker {
        NKikimrBlobStorage::EVDiskStatus Status;
        TInstant SwitchTime = TInstant::Max();
        const TDuration Threshold;

    public:
        TVDiskStatusTracker(TDuration threshold = TDuration::Seconds(60))
            : Threshold(threshold)
        {}

        void Update(NKikimrBlobStorage::EVDiskStatus status, TInstant now) {
            if (SwitchTime == TInstant::Max() || status != Status) {
                Status = status;
                SwitchTime = now;
            }
        }

        std::optional<NKikimrBlobStorage::EVDiskStatus> GetStatus(TInstant now) const {
            return SwitchTime <= now - Threshold ? std::make_optional(Status) : std::nullopt;
        }
    };

} // NKikimr::NBsController
