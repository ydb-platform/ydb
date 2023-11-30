#pragma once

////////////////////////////////////////////
#include <ydb/library/actors/core/defs.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event.h>

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/protos/grpc_status_proxy.pb.h>

////////////////////////////////////////////
namespace NKikimr {

////////////////////////////////////////////
TActorId MakeGRpcProxyStatusID(ui32 node);

////////////////////////////////////////////
struct TEvGRpcProxyStatus {
    //
    enum EEv {
        EvGetStatusRequest = EventSpaceBegin(TKikimrEvents::ES_GRPC_PROXY_STATUS),
        EvGetStatusResponse,
        EvUpdateStatus, //local event
        EvSetup, //local event
        EvRequest, //local event
        EvResponse, //local event
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_GRPC_PROXY_STATUS), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_GRPC_PROXY_STATUS)");

    struct TEvUpdateStatus : public TEventLocal<TEvUpdateStatus, EvUpdateStatus> {
        //
        ui64 ReadBytes; //diff
        ui64 WriteBytes; //diff
        i32 WriteSessions; //diff
        i32 ReadSessions; //diff

        TEvUpdateStatus(const ui64 readBytes, const ui64 writeBytes, const i32 writeSessions, const i32 readSessions)
            : ReadBytes(readBytes)
            , WriteBytes(writeBytes)
            , WriteSessions(writeSessions)
            , ReadSessions(readSessions)
        {}
    };

    struct TEvSetup : public TEventLocal<TEvSetup, EvSetup> {
        //
        bool Allowed;
        ui32 MaxWriteSessions;
        ui32 MaxReadSessions;

        TEvSetup(bool allowed, ui32 maxWriteSessions, ui32 maxReadSessions)
            : Allowed(allowed)
            , MaxWriteSessions(maxWriteSessions)
            , MaxReadSessions(maxReadSessions)
        {}
    };


    struct TEvRequest : public TEventLocal<TEvRequest, EvRequest> {
    };


    struct TEvGetStatusRequest : public TEventPB<TEvGetStatusRequest, NKikimrGRpcProxy::TEvGetStatusRequest, EvGetStatusRequest> {
    };

    struct TEvGetStatusResponse : public TEventPB<TEvGetStatusResponse, NKikimrGRpcProxy::TEvGetStatusResponse, EvGetStatusResponse> {
    };


    struct TEvResponse : public TEventLocal<TEvResponse, EvResponse> {
        THashMap<ui32, std::shared_ptr<TEvGRpcProxyStatus::TEvGetStatusResponse>> PerNodeResponse;
        THashMap<ui32, TString> NodeNames;
        THashMap<ui32, TString> NodeDataCenter;
    };

};

IActor* CreateGRpcProxyStatus();


} // end of the NKikimr namespace

