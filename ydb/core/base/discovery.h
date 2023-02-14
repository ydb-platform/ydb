#pragma once

#include "defs.h"
#include "events.h"

namespace NKikimr {

struct TEvDiscovery {
    enum EEv {
        EvError = EventSpaceBegin(TKikimrEvents::ES_DISCOVERY),
        EvEnd
    };

    struct TEvError: public TEventLocal<TEvError, EvError> {
        enum EStatus {
            KEY_PARSE_ERROR,
            RESOLVE_ERROR,
            DATABASE_NOT_EXIST,
            ACCESS_DENIED,
        };

        EStatus Status;
        TString Error;

        explicit TEvError(EStatus status, const TString& error)
            : Status(status)
            , Error(error)
        {
        }
    };
};

// Reply with:
// - in case of success: TEvStateStorage::TEvBoardInfo
// - otherwise: TEvDiscovery::TEvError
IActor* CreateDiscoverer(const TString& database, const TActorId& replyTo, const TActorId& cacheId);

// Used to reduce number of requests to Board
IActor* CreateDiscoveryCache();

}
