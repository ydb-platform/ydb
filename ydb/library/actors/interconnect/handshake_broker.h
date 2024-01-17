#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/interconnect/events_local.h>

#include <deque>

namespace NActors {
    class TBrokerLeaseHolder {
    public:
        TBrokerLeaseHolder(TActorId waiterId, TActorId brokerId)
            : WaiterId(waiterId)
            , BrokerId(brokerId) {
            if (TActivationContext::Send(new IEventHandle(BrokerId, WaiterId, new TEvHandshakeBrokerTake()))) {
                LeaseRequested = true;
            }
        }

        ~TBrokerLeaseHolder() {
            if (LeaseRequested) {
                TActivationContext::Send(new IEventHandle(BrokerId, WaiterId, new TEvHandshakeBrokerFree()));
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
        enum class ESelectionStrategy {
            FIFO = 0,
            LIFO,
            Random,
        };

    private:
        void PermitNext() {
            if (Capacity == 0 && !Waiters.empty()) {
                TActorId waiter;

                switch (SelectionStrategy) {
                case ESelectionStrategy::FIFO:
                    waiter = Waiters.front();
                    Waiters.pop_front();
                    SelectionStrategy = ESelectionStrategy::LIFO;
                    break;

                case ESelectionStrategy::LIFO:
                    waiter = Waiters.back();
                    Waiters.pop_back();
                    SelectionStrategy = ESelectionStrategy::Random;
                    break;

                case ESelectionStrategy::Random: {
                    const auto it = WaiterLookup.begin();
                    waiter = it->first;
                    Waiters.erase(it->second);
                    SelectionStrategy = ESelectionStrategy::FIFO;
                    break;
                }

                default:
                    Y_ABORT("Unimplimented selection strategy");
                }

                const size_t n = WaiterLookup.erase(waiter);
                Y_ABORT_UNLESS(n == 1);

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

        ESelectionStrategy SelectionStrategy = ESelectionStrategy::FIFO;

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
                Y_ABORT_UNLESS(inserted);
            }
        }

        void Handle(TEvHandshakeBrokerFree::TPtr& ev) {
            const TActorId sender = ev->Sender;
            if (!PermittedLeases.erase(sender)) {
                // Lease was not permitted yet, remove sender from Waiters queue
                const auto it = WaiterLookup.find(sender);
                Y_ABORT_UNLESS(it != WaiterLookup.end());
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
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvHandshakeBrokerTake, Handle);
                hFunc(TEvHandshakeBrokerFree, Handle);

            default:
                Y_ABORT("unexpected event 0x%08" PRIx32, ev->GetTypeRewrite());
            }
        }

        void Bootstrap() {
            Become(&TThis::StateFunc);
        }
    };

    inline IActor* CreateHandshakeBroker(ui32 maxCapacity) {
        return new THandshakeBroker(maxCapacity);
    }

    inline TActorId MakeHandshakeBrokerOutId() {
        char x[12] = {'I', 'C', 'H', 's', 'h', 'k', 'B', 'r', 'k', 'O', 'u', 't'};
        return TActorId(0, TStringBuf(std::begin(x), std::end(x)));
    }
}
