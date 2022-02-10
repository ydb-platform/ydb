#pragma once

#include "acceptor_status.h"
#include "defs.h"
#include "event_loop.h"
#include "netaddr.h"
#include "session_impl.h"
#include "shutdown_state.h"

#include <library/cpp/messagebus/actor/actor.h>

#include <util/system/event.h>

namespace NBus {
    namespace NPrivate {
        class TAcceptor
           : public NEventLoop::IEventHandler,
              private ::NActor::TActor<TAcceptor> {
            friend struct TBusSessionImpl;
            friend class ::NActor::TActor<TAcceptor>;

        public:
            TAcceptor(TBusSessionImpl* session, ui64 acceptorId, SOCKET socket, const TNetAddr& addr);

            void HandleEvent(SOCKET socket, void* cookie) override;

            void Shutdown();

            inline ::NActor::TActor<TAcceptor>* GetActor() {
                return this;
            }

        private:
            void SendStatus(TInstant now);
            void Act(::NActor::TDefaultTag);

        private:
            const ui64 AcceptorId;

            TBusSessionImpl* const Session;
            NEventLoop::TChannelPtr Channel;

            TAcceptorStatus Stats;

            TAtomicShutdownState ShutdownState;

            struct TGranStatus {
                TGranStatus(TDuration gran)
                    : Listen(gran)
                {
                }

                TGranUp<TAcceptorStatus> Listen;
            };

            TGranStatus GranStatus;
        };

    }
}
