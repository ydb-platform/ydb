#pragma once

#include <ydb/core/base/events.h>
#include <ydb/core/base/statestorage.h>
#include <ydb/core/base/bridge.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/interconnect/interconnect.h>

namespace NKikimr {

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
        TString CachedMessage;
        TString CachedMessageSsl;

        TMap<TActorId, TEvStateStorage::TBoardInfoEntry> InfoEntries; // OwnerId -> Payload
        TBridgeInfo::TPtr BridgeInfo;
        TEvStateStorage::TEvBoardInfo::EStatus Status;
    };
};

namespace NDiscovery {

class TCachedMessageData {
public:
    TString CachedMessage;
    TString CachedMessageSsl;

    TMap<TActorId, TEvStateStorage::TBoardInfoEntry> InfoEntries; // OwnerId -> Payload
    TBridgeInfo::TPtr BridgeInfo;
    TEvStateStorage::TEvBoardInfo::EStatus Status;

public:
    TCachedMessageData(const TString& cachedMessage, const TString& cachedMessageSsl,
                        const TMap<TActorId, TEvStateStorage::TBoardInfoEntry>& infoEntries,
                        const TBridgeInfo::TPtr& bridgeInfo = nullptr,
                        TEvStateStorage::TEvBoardInfo::EStatus status = TEvStateStorage::TEvBoardInfo::EStatus::Ok)
        : CachedMessage(cachedMessage)
        , CachedMessageSsl(cachedMessageSsl)
        , InfoEntries(infoEntries)
        , BridgeInfo(bridgeInfo)
        , Status(status)
    {}

    TCachedMessageData(TMap<TActorId, TEvStateStorage::TBoardInfoEntry> infoEntries,
                        const THolder<TEvInterconnect::TEvNodeInfo>& nameserviceResponse,
                        const TBridgeInfo::TPtr& bridgeInfo,
                        const TString& endpointId = {},
                        const TSet<TString>& services = {},
                        TEvStateStorage::TEvBoardInfo::EStatus status = TEvStateStorage::TEvBoardInfo::EStatus::Ok)
        : InfoEntries(std::move(infoEntries))
        , BridgeInfo(bridgeInfo)
        , Status(status)
    {
        UpdateCache(nameserviceResponse, bridgeInfo, endpointId, services);
    }

    void UpdateEntries(TMap<TActorId, TEvStateStorage::TBoardInfoEntry>&& newInfoEntries);

    void UpdateCache(const THolder<TEvInterconnect::TEvNodeInfo>& nameserviceResponse,
                        const TBridgeInfo::TPtr& bridgeInfo,
                        const TString& endpointId = {},
                        const TSet<TString>& services = {});

    TEvDiscovery::TEvDiscoveryData* ToEvent(bool returnSerializedMessage) const;
};

}

using TLookupPathFunc = std::function<TString(const TString&)>;

// Reply with:
// - in case of success: TEvStateStorage::TEvBoardInfo
// - otherwise: TEvDiscovery::TEvError
IActor* CreateDiscoverer(
    TLookupPathFunc f,
    const TString& database,
    bool returnSerializedMessage,
    const TActorId& replyTo,
    const TActorId& cacheId);

// Used to reduce number of requests to Board
IActor* CreateDiscoveryCache(const TString& endpointId = {}, const TMaybe<TActorId>& nameserviceActorId = {});

}
