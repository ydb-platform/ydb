#pragma once

#include <library/cpp/actors/core/actor.h>

#include <deque>

namespace NActors {
    class TBrokerLeaseHolder {
    public:
        TBrokerLeaseHolder(TActorId waiterId, TActorId brokerId)
            : WaiterId(waiterId)
            , BrokerId(brokerId) {
            if (TActivationContext::Send(new IEventHandleFat(BrokerId, WaiterId, new TEvHandshakeBrokerTake()))) {
                LeaseRequested = true;
            }
        }

        ~TBrokerLeaseHolder() {
            if (LeaseRequested) {
                TActivationContext::Send(new IEventHandleFat(BrokerId, WaiterId, new TEvHandshakeBrokerFree()));
            }
        }

        bool IsLeaseRequested() {
            return LeaseRequested;
        }

        void ForgetLease() {
            // only call when TDtorException was caught
            LeaseRequested = false;
        }

    private:
        TActorId WaiterId;
        TActorId BrokerId;
        bool LeaseRequested = false;
    };

    class THandshakeBroker : public TActor<THandshakeBroker> {
    private:
        void PermitNext() {
            if (Capacity == 0 && !Waiters.empty()) {
                const TActorId waiter = Waiters.front();
                Waiters.pop_front();
                WaiterLookup.erase(waiter);

                Send(waiter, new TEvHandshakeBrokerPermit());
                PermittedLeases.insert(waiter);
            } else {
                Capacity += 1;
            }
        }

    private:
        using TWaiters = std::list<TActorId>;
        TWaiters Waiters;
        std::unordered_map<TActorId, TWaiters::iterator> WaiterLookup;
        std::unordered_set<TActorId> PermittedLeases;

        ui32 Capacity;

        void Handle(TEvHandshakeBrokerTake::TPtr &ev) {
            const TActorId sender = ev->Sender;
            if (Capacity > 0) {
                Capacity -= 1;
                PermittedLeases.insert(sender);
                Send(sender, new TEvHandshakeBrokerPermit());
            } else {
                const auto [it, inserted] = WaiterLookup.try_emplace(sender,
                        Waiters.insert(Waiters.end(), sender));
                Y_VERIFY(inserted);
            }
        }

        void Handle(TEvHandshakeBrokerFree::TPtr& ev) {
            const TActorId sender = ev->Sender;
            if (!PermittedLeases.erase(sender)) {
                // Lease was not permitted yet, remove sender from Waiters queue
                const auto it = WaiterLookup.find(sender);
                Y_VERIFY(it != WaiterLookup.end());
                Waiters.erase(it->second);
                WaiterLookup.erase(it);
            }
            PermitNext();
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
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvHandshakeBrokerTake, Handle);
                hFunc(TEvHandshakeBrokerFree, Handle);

            default:
                Y_FAIL("unexpected event 0x%08" PRIx32, ev->GetTypeRewrite());
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
