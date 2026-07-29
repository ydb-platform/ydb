#pragma once

#include <ydb/library/actors/interconnect/events/events.h>
#include <ydb/library/actors/interconnect/uring_context.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/util/rc_buf.h>

#include "poller_actor.h"

namespace NActors {

    struct TEvUringRegister : TEventLocal<TEvUringRegister, ui32(ENetwork::EvUringRegister)> {
        TIntrusivePtr<TSharedDescriptor> Socket;
        TIntrusivePtr<TSharedDescriptor> XdcSocket;
        TActorId ReadActorId;
        TActorId WriteActorId;

        TEvUringRegister(TIntrusivePtr<TSharedDescriptor> socket,
                         TIntrusivePtr<TSharedDescriptor> xdcSocket,
                         TActorId readActorId,
                         TActorId writeActorId)
            : Socket(std::move(socket))
            , XdcSocket(std::move(xdcSocket))
            , ReadActorId(readActorId)
            , WriteActorId(writeActorId)
        {}
    };

    struct TEvUringRegisterResult : TEventLocal<TEvUringRegisterResult, ui32(ENetwork::EvUringRegisterResult)> {
        TUringContext::TPtr Context;
        ui16 MainRecvBufGroupId;
        ui16 XdcRecvBufGroupId;

        TEvUringRegisterResult(TUringContext::TPtr context, ui16 mainBufGid, ui16 xdcBufGid)
            : Context(std::move(context))
            , MainRecvBufGroupId(mainBufGid)
            , XdcRecvBufGroupId(xdcBufGid)
        {}
    };

    struct TEvUringWriteComplete : TEventLocal<TEvUringWriteComplete, ui32(ENetwork::EvUringWriteComplete)> {
        i32 Result;
        ui64 UserData;

        TEvUringWriteComplete(i32 result, ui64 userData)
            : Result(result)
            , UserData(userData)
        {}
    };

    struct TEvUringRecvComplete : TEventLocal<TEvUringRecvComplete, ui32(ENetwork::EvUringRecvComplete)> {
        i32 Result;
        ui32 Flags;
        ui64 UserData;
        TRcBuf Data;

        TEvUringRecvComplete(i32 result, ui32 flags, ui64 userData)
            : Result(result)
            , Flags(flags)
            , UserData(userData)
        {}

        TEvUringRecvComplete(i32 result, ui32 flags, ui64 userData, TRcBuf data)
            : Result(result)
            , Flags(flags)
            , UserData(userData)
            , Data(std::move(data))
        {}
    };

    struct TEvUringSendZcNotif : TEventLocal<TEvUringSendZcNotif, ui32(ENetwork::EvUringSendZcNotif)> {
        ui64 UserData;

        explicit TEvUringSendZcNotif(ui64 userData)
            : UserData(userData)
        {}
    };

    // Sent by the output session on teardown so the reaper can release the session ring
    // (exit the ring, release the registered socket files, free the buffer ring, close the
    // eventfd). EventFd identifies the session ring in the reaper's maps.
    struct TEvUringUnregister : TEventLocal<TEvUringUnregister, ui32(ENetwork::EvUringUnregister)> {
        int EventFd;

        explicit TEvUringUnregister(int eventFd)
            : EventFd(eventFd)
        {}
    };

    // Sent by the poller to the output session when io_uring setup failed: the session must
    // fall back to registering its sockets with the epoll TPollerActor.
    struct TEvUringRegisterFailed : TEventLocal<TEvUringRegisterFailed, ui32(ENetwork::EvUringRegisterFailed)> {
    };

#ifdef __linux__
    IActor* CreateUringPollerActor(bool enableSqpoll);
#else
    inline IActor* CreateUringPollerActor(bool /*enableSqpoll*/) { return nullptr; }
#endif

    inline TActorId MakeUringPollerActorId() {
        char x[12] = {'I', 'C', 'U', 'r', 'i', 'n', 'g', 'P', '\xDE', '\xAD', '\xBE', '\xEF'};
        return TActorId(0, TStringBuf(std::begin(x), std::end(x)));
    }

} // namespace NActors
