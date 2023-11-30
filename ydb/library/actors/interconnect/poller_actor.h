#pragma once

#include "events_local.h"
#include "poller.h"
#include <ydb/library/actors/core/actor.h>

namespace NActors {
    struct TEvPollerRegister : TEventLocal<TEvPollerRegister, ui32(ENetwork::EvPollerRegister)> {
        const TIntrusivePtr<TSharedDescriptor> Socket; // socket to watch for
        const TActorId ReadActorId;                    // actor id to notify about read availability
        const TActorId WriteActorId;                   // actor id to notify about write availability; may be the same as the ReadActorId

        TEvPollerRegister(TIntrusivePtr<TSharedDescriptor> socket, const TActorId& readActorId, const TActorId& writeActorId)
            : Socket(std::move(socket))
            , ReadActorId(readActorId)
            , WriteActorId(writeActorId)
        {}
    };

    // poller token is sent in response to TEvPollerRegister; it allows requesting poll when read/write returns EAGAIN
    class TPollerToken : public TThrRefBase {
        class TImpl;
        std::unique_ptr<TImpl> Impl;

        friend class TPollerActor;
        TPollerToken(std::unique_ptr<TImpl> impl);

    public:
        ~TPollerToken();
        void Request(bool read, bool write);
        bool RequestReadNotificationAfterWouldBlock();
        bool RequestWriteNotificationAfterWouldBlock();

        bool RequestNotificationAfterWouldBlock(bool read, bool write) {
            bool status = false;
            status |= read && RequestReadNotificationAfterWouldBlock();
            status |= write && RequestWriteNotificationAfterWouldBlock();
            return status;
        }

        using TPtr = TIntrusivePtr<TPollerToken>;
    };

    struct TEvPollerRegisterResult : TEventLocal<TEvPollerRegisterResult, ui32(ENetwork::EvPollerRegisterResult)> {
        TIntrusivePtr<TSharedDescriptor> Socket;
        TPollerToken::TPtr PollerToken;

        TEvPollerRegisterResult(TIntrusivePtr<TSharedDescriptor> socket, TPollerToken::TPtr pollerToken)
            : Socket(std::move(socket))
            , PollerToken(std::move(pollerToken))
        {}
    };

    struct TEvPollerReady : TEventLocal<TEvPollerReady, ui32(ENetwork::EvPollerReady)> {
        TIntrusivePtr<TSharedDescriptor> Socket;
        const bool Read, Write;

        TEvPollerReady(TIntrusivePtr<TSharedDescriptor> socket, bool read, bool write)
            : Socket(std::move(socket))
            , Read(read)
            , Write(write)
        {}
    };

    IActor* CreatePollerActor();

    inline TActorId MakePollerActorId() {
        char x[12] = {'I', 'C', 'P', 'o', 'l', 'l', 'e', 'r', '\xDE', '\xAD', '\xBE', '\xEF'};
        return TActorId(0, TStringBuf(std::begin(x), std::end(x)));
    }

}
