#pragma once

#include <util/datetime/base.h>
#include <ydb/core/security/token_manager/token_manager.h>
#include <ydb/library/actors/core/events.h>

namespace NKikimr::NTokenManager {

namespace TEvPrivate {
    enum EEv {
        EvUpdateToken = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvErrorUpdateToken,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

    struct TEvUpdateToken : NActors::TEventLocal<TEvUpdateToken, EvUpdateToken> {
        TString Id;
        TEvTokenManager::TStatus Status;
        TString Token;
        TDuration RefreshPeriod;

        TEvUpdateToken(const TString& id, const TEvTokenManager::TStatus& status, const TString& token, const TDuration& refreshPeriod)
            : Id(id)
            , Status(status)
            , Token(token)
            , RefreshPeriod(refreshPeriod)
        {}
    };
}

} // NKikimr::NTokenManager
