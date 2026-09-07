#pragma once

#include "billing.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>

#include <util/generic/string.h>
#include <util/system/types.h>

namespace NKikimr::NSqsTopic::V1 {

    struct TRequestUnitsQuoterSettings {
        TString Database;
        ui64 Ru = 0;
        TString Token;
    };

    struct TEvChargeRequestUnitsResponse
        : public NActors::TEventLocal<TEvChargeRequestUnitsResponse, EventSpaceBegin(NActors::TEvents::ES_PRIVATE) + 8123>
    {
        enum class EStatus {
            Ok,
            Throttled,
            Error,
        };

        EStatus Status = EStatus::Ok;
        TString Message;
    };

    NActors::IActor* CreateRequestUnitsQuoter(
        const NActors::TActorId& parent,
        TRequestUnitsQuoterSettings settings);

} // namespace NKikimr::NSqsTopic::V1
