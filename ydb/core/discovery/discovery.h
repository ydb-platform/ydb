#pragma once

#include <ydb/core/base/events.h>
#include <ydb/core/base/statestorage.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/interconnect/interconnect.h>

namespace NKikimr {

namespace NDiscovery {
    struct TCachedMessageData {
        TString CachedMessage;
        TString CachedMessageSsl;
        TMap<TActorId, TEvStateStorage::TBoardInfoEntry> InfoEntries; // OwnerId -> Payload
        TEvStateStorage::TEvBoardInfo::EStatus Status = TEvStateStorage::TEvBoardInfo::EStatus::Ok;
    };
}

struct TEvDiscovery {

    enum EEv {
        EvError = EventSpaceBegin(TKikimrEvents::ES_DISCOVERY),
        EvDiscoveryData,
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

    struct TEvDiscoveryData : public TEventLocal<TEvDiscoveryData, EvDiscoveryData> {
        std::shared_ptr<const NDiscovery::TCachedMessageData> CachedMessageData;

        TEvDiscoveryData(
                std::shared_ptr<const NDiscovery::TCachedMessageData> cachedMessageData)
            : CachedMessageData(std::move(cachedMessageData))
        {}
    };
};

namespace NDiscovery {
    NDiscovery::TCachedMessageData CreateCachedMessage(
        const TMap<TActorId, TEvStateStorage::TBoardInfoEntry>&,
        TMap<TActorId, TEvStateStorage::TBoardInfoEntry>,
        TSet<TString>,
        TString,
        const THolder<TEvInterconnect::TEvNodeInfo>&);
}

using TLookupPathFunc = std::function<TString(const TString&)>;

// Reply with:
// - in case of success: TEvStateStorage::TEvBoardInfo
// - otherwise: TEvDiscovery::TEvError
IActor* CreateDiscoverer(
    TLookupPathFunc f,
    const TString& database,
    const TActorId& replyTo,
    const TActorId& cacheId);

// Used to reduce number of requests to Board
IActor* CreateDiscoveryCache(const TString& endpointId = {});

}
