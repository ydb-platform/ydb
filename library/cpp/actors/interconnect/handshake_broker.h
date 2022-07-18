#pragma once

#include <library/cpp/actors/core/actor.h>

#include <deque>

namespace NActors {
    static constexpr ui32 DEFAULT_INFLIGHT = 100;

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
                Send(Waiting.front(), new TEvHandshakeBrokerPermit());
                Waiting.pop_front();
            } else {
                Capacity += 1;
            }
        }

        void PassAway() override {
            while (!Waiting.empty()) {
                Send(Waiting.front(), new TEvHandshakeBrokerPermit());
                Waiting.pop_front();
            }
            TActor::PassAway();
        }

    public:
        THandshakeBroker(ui32 inflightLimit = DEFAULT_INFLIGHT)
            : TActor(&TThis::StateFunc)
            , Capacity(inflightLimit) 
        {
        }

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

    inline IActor* CreateHandshakeBroker() {
        return new THandshakeBroker();
    }

    inline TActorId MakeHandshakeBrokerOutId() {
        char x[12] = {'I', 'C', 'H', 's', 'h', 'k', 'B', 'r', 'k', 'O', 'u', 't'};
        return TActorId(0, TStringBuf(std::begin(x), std::end(x)));
    }

    inline TActorId MakeHandshakeBrokerInId() {
        char x[12] = {'I', 'C', 'H', 's', 'h', 'k', 'B', 'r', 'k', 'r', 'I', 'n'};
        return TActorId(0, TStringBuf(std::begin(x), std::end(x)));
    }
};
