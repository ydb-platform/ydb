#pragma once

#include <library/cpp/actors/core/actor.h>

#include <deque>

namespace NActors {
    class THandshakeBroker : public TActor<THandshakeBroker> {
    private:
        std::deque<TActorId> Waiting;
        ui32 Capacity;

        void Handle(TEvHandshakeBrokerTake::TPtr &ev) {
            if (Capacity > 0) {
                Capacity -= 1;
                Send(ev->Sender, new TEvHandshakeBrokerPermit());
            } else {
                Waiting.push_back(ev->Sender);
            }
        }

        void Handle(TEvHandshakeBrokerFree::TPtr& ev) {
            Y_UNUSED(ev);
            if (Capacity == 0 && !Waiting.empty()) {
                Send(Waiting.back(), new TEvHandshakeBrokerPermit());
                Waiting.pop_back();
            } else {
                Capacity += 1;
            }
        }

        void PassAway() override {
            while (!Waiting.empty()) {
                Send(Waiting.back(), new TEvHandshakeBrokerPermit());
                Waiting.pop_back();
            }
            TActor::PassAway();
        }

    public:
        THandshakeBroker(ui32 inflightLimit)
            : TActor(&TThis::StateFunc)
            , Capacity(inflightLimit)
        {
        }

        static constexpr char ActorName[] = "HANDSHAKE_BROKER_ACTOR";

        STFUNC(StateFunc) {
            Y_UNUSED(ctx);
            switch(ev->GetTypeRewrite()) {
                hFunc(TEvHandshakeBrokerTake, Handle);
                hFunc(TEvHandshakeBrokerFree, Handle);
                cFunc(TEvents::TSystem::Poison, PassAway);
            }
        }

        void Bootstrap() {
            Become(&TThis::StateFunc);
        };
    };

    inline IActor* CreateHandshakeBroker(ui32 maxCapacity) {
        return new THandshakeBroker(maxCapacity);
    }

    inline TActorId MakeHandshakeBrokerOutId() {
        char x[12] = {'I', 'C', 'H', 's', 'h', 'k', 'B', 'r', 'k', 'O', 'u', 't'};
        return TActorId(0, TStringBuf(std::begin(x), std::end(x)));
    }
};
