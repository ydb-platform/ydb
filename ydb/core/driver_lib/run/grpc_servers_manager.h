#pragma once

#include <ydb/core/base/defs.h>
#include <ydb/core/base/appdata.h>
#include <ydb/library/actors/core/event_local.h>

namespace NKikimr {

struct TEvGRpcServersManager {
    enum EEv {
        EvDisconnectRequestStarted = EventSpaceBegin(TEvents::ES_PRIVATE) + 10000,
        EvDisconnectRequestFinished,
    };

    struct TEvDisconnectRequestStarted : public TEventLocal<TEvDisconnectRequestStarted, EvDisconnectRequestStarted> {
    };

    struct TEvDisconnectRequestFinished : public TEventLocal<TEvDisconnectRequestFinished, EvDisconnectRequestFinished> {
    };
};

inline TActorId MakeGRpcServersManagerId(ui32 nodeId) {
    char x[12] = {'g','r','p','c','s','r','v','r','m','n','g','r'};
    return TActorId(nodeId, TStringBuf(x, 12));
}

} // namespace NKikimr

