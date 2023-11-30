#include "ask.h"

#include "actor_bootstrapped.h"
#include "actorid.h"
#include "event.h"
#include "hfunc.h"

namespace NActors {
    namespace {
        class TAskActor: public TActorBootstrapped<TAskActor> {
            enum {
                Timeout = EventSpaceBegin(TEvents::ES_PRIVATE),
            };

            // We can't use the standard timeout event because recipient may send us one.
            struct TTimeout: public TEventLocal<TTimeout, Timeout> {
            };

        public:
            TAskActor(
                    TMaybe<ui32> expectedEventType,
                    TActorId recipient,
                    THolder<IEventBase> event,
                    TDuration timeout,
                    const NThreading::TPromise<THolder<IEventBase>>& promise)
                : ExpectedEventType_(expectedEventType)
                , Recipient_(recipient)
                , Event_(std::move(event))
                , Timeout_(timeout)
                , Promise_(promise)
            {
            }

            static constexpr char ActorName[] = "ASK_ACTOR";

        public:
            void Bootstrap() {
                Send(Recipient_, std::move(Event_));
                Become(&TAskActor::Waiting);

                if (Timeout_ != TDuration::Max()) {
                    Schedule(Timeout_, new TTimeout);
                }
            }

            STATEFN(Waiting) {
                if (ev->GetTypeRewrite() == TTimeout::EventType) {
                    Promise_.SetException(std::make_exception_ptr(yexception() << "ask timeout"));
                } else if (!ExpectedEventType_ || ev->GetTypeRewrite() == ExpectedEventType_) {
                    Promise_.SetValue(ev.Get()->ReleaseBase());
                } else {
                    Promise_.SetException(std::make_exception_ptr(yexception() << "received unexpected response " << ev.Get()->GetBase()->ToString()));
                }

                PassAway();
            }

        public:
            TMaybe<ui32> ExpectedEventType_;
            TActorId Recipient_;
            THolder<IEventBase> Event_;
            TDuration Timeout_;
            NThreading::TPromise<THolder<IEventBase>> Promise_;
        };
    }

    THolder<IActor> MakeAskActor(
            TMaybe<ui32> expectedEventType,
            TActorId recipient,
            THolder<IEventBase> event,
            TDuration timeout,
            const NThreading::TPromise<THolder<IEventBase>>& promise)
    {
        return MakeHolder<TAskActor>(expectedEventType, std::move(recipient), std::move(event), timeout, promise);
    }
}
