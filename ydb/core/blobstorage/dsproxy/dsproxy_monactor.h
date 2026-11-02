#pragma once

#include "defs.h"
#include "dsproxy_mon.h"

namespace NKikimr {

struct TEvThroughputAddRequest : public TEventLocal<TEvThroughputAddRequest, TEvBlobStorage::EvThroughputAddRequest> {
    NKikimrBlobStorage::EPutHandleClass PutHandleClass;
    ui64 Bytes;

    TEvThroughputAddRequest(NKikimrBlobStorage::EPutHandleClass putHandleClass, ui64 bytes)
        : PutHandleClass(putHandleClass)
        , Bytes(bytes)
    {}
};

struct TEvSetProxyDormant : public TEventLocal<TEvSetProxyDormant, TEvBlobStorage::EvSetProxyDormant> {
    bool IsDormant;

    explicit TEvSetProxyDormant(bool isDormant)
        : IsDormant(isDormant)
    {}
};

IActor* CreateBlobStorageGroupProxyMon(TIntrusivePtr<TBlobStorageGroupProxyMon> mon, ui32 groupId,
        TIntrusivePtr<TBlobStorageGroupInfo> info, TActorId proxyId, bool isDormant = false);

} // NKikimr
