#pragma once

#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/event_pb.h>
#include <ydb/library/actors/core/events.h>

#include "interconnect_common.h"
#include "poller_actor.h"
#include "events_local.h"

namespace NActors {
    class TInterconnectListenerTCP: public TActor<TInterconnectListenerTCP>, public TInterconnectLoggingBase {
    public:
        static constexpr EActivityType ActorActivityType() {
            return EActivityType::INTERCONNECT_COMMON;
        }

        TInterconnectListenerTCP(const TString& address, ui16 port, TInterconnectProxyCommon::TPtr common, const TMaybe<SOCKET>& socket = Nothing());
        int Bind();

    private:
        STFUNC(Initial) {
            switch (ev->GetTypeRewrite()) {
                CFunc(TEvents::TEvBootstrap::EventType, Bootstrap);
                CFunc(TEvents::TEvPoisonPill::EventType, Die);
            }
        }

        STFUNC(Listen) {
            switch (ev->GetTypeRewrite()) {
                CFunc(TEvents::TEvPoisonPill::EventType, Die);
                HFunc(TEvPollerRegisterResult, Handle);
                CFunc(TEvPollerReady::EventType, Process);
            }
        }

        TAutoPtr<IEventHandle> AfterRegister(const TActorId& self, const TActorId& parentId) override;

        void Die(const TActorContext& ctx) override;

        void Bootstrap(const TActorContext& ctx);
        void Handle(TEvPollerRegisterResult::TPtr ev, const TActorContext& ctx);

        void Process(const TActorContext& ctx);

        const TString Address;
        const ui16 Port;
        TIntrusivePtr<NInterconnect::TStreamSocket> Listener;
        const bool ExternalSocket;
        TPollerToken::TPtr PollerToken;
        TInterconnectProxyCommon::TPtr const ProxyCommonCtx;
    };

    static inline TActorId MakeInterconnectListenerActorId(bool dynamic) {
        char x[12] = {'I', 'C', 'L', 'i', 's', 't', 'e', 'n', 'e', 'r', '/', dynamic ? 'D' : 'S'};
        return TActorId(0, TStringBuf(x, 12));
    }
}
