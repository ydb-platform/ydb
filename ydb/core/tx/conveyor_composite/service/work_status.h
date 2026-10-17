#pragma once

namespace NKikimr::NConveyorComposite {

    enum class ESchedulableWorkStatus {
        IDLE,
        THROTTLED,
        STARTED,
    };

} // namespace NKikimr::NConveyorComposite
