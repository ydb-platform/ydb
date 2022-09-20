#pragma once

#include "defs.h"
#include "events.h"

namespace NKikimr::NTestShard {

    inline TActorId MakeStateServerInterfaceActorId() {
        return TActorId(0, TStringBuf("StateServerI", 12));
    }

    struct TTestShardContext : TSimpleRefCount<TTestShardContext> {
        class TData;
        std::unique_ptr<TData> Data;

        TTestShardContext();
        ~TTestShardContext();

        using TPtr = TIntrusivePtr<TTestShardContext>;
        static TPtr Create();
    };

    IActor *CreateStateServerInterfaceActor(TTestShardContext::TPtr context);

    struct TEvStateServerConnect : TEventLocal<TEvStateServerConnect, TEvTestShard::EvStateServerConnect> {
        const TString Host;
        const i32 Port;

        TEvStateServerConnect(TString host, i32 port)
            : Host(std::move(host))
            , Port(port)
        {}
    };

    struct TEvStateServerDisconnect : TEventLocal<TEvStateServerDisconnect, TEvTestShard::EvStateServerDisconnect>
    {};

    struct TEvStateServerStatus : TEventLocal<TEvStateServerStatus, TEvTestShard::EvStateServerStatus> {
        const bool Connected; // true if successfully connected, false on error
        TEvStateServerStatus(bool connected) : Connected(connected) {}
    };

    struct TEvStateServerRequest : TEventPB<TEvStateServerRequest, ::NTestShard::TStateServer::TRequest, TEvTestShard::EvStateServerRequest>
    {};

    struct TEvStateServerWriteResult : TEventPB<TEvStateServerWriteResult, ::NTestShard::TStateServer::TWriteResult, TEvTestShard::EvStateServerWriteResult>
    {};

    struct TEvStateServerReadResult : TEventPB<TEvStateServerReadResult, ::NTestShard::TStateServer::TReadResult, TEvTestShard::EvStateServerReadResult>
    {};

} // NKikimr::NTestShard
