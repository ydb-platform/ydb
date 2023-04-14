#pragma once

#include <util/generic/vector.h>
#include <ydb/core/actorlib_impl/mailbox.h>

namespace NActors {

    ////////////////////////////////////////////////////////////////////////////////
    // ROUND ROBIN ROUTER
    // Routes events via a set of target actors in a round robin fashion.
    // It creates Num actors of a given type TTargetActor. Actors can inherit
    // thread pool and mailbox type, or they can be redefined
    ////////////////////////////////////////////////////////////////////////////////
    template <class TTargetActor, int Num, int ThreadPoolId = -1,
            TMailboxType::EType MailBoxType = TMailboxType::Inherited>
    class TRoundRobinRouter : public NActors::TActor<TRoundRobinRouter<TTargetActor, Num, ThreadPoolId, MailBoxType> > {
        typedef TRoundRobinRouter<TTargetActor, Num, ThreadPoolId, MailBoxType> TSelf;
        typedef NActors::TActor<TSelf> TBase;
    public:
        TRoundRobinRouter()
            : TBase(&TSelf::StateFunc)
            , Pos(0)
        {
        }

        STFUNC(StateFunc) {
            switch (ev->GetTypeRewrite()) {
                    HFunc(TEvents::TEvBootstrap, Handle);
                default: HandleDefault(ev);
            }
        }

        void HandleDefault(TAutoPtr<IEventHandle> &ev) {
            TAutoPtr<IEventHandle> f = ev->Forward(Actors[Pos++]);
            if (Pos == Num)
                Pos = 0;
            Send(f);
        }

        void Handle(TEvents::TEvBootstrap::TPtr &, const TActorContext &ctx) {
            int threadPoolId = (ThreadPoolId == -1) ? ctx.SelfID.PoolID() : ThreadPoolId;
            TMailboxType::EType mbType = (MailBoxType == TMailboxType::Inherited) ?
            TMailboxType::EType(ctx.Mailbox.Type) : MailBoxType;

            Actors.reserve(Num);
            for (int i = 0; i < Num; i++) {
                // create an actor
                NActors::IActor *actor = new TTargetActor();

                // register the actor
                NActors::TActorId actorId = ctx.ExecutorThread.RegisterActor(actor, mbType, threadPoolId);
                Actors.push_back(actorId);
            }
        }

        virtual TAutoPtr<NActors::IEventHandle> AfterRegister(const NActors::TActorId &self, const TActorId& parentId) override {
            Y_UNUSED(parentId);
            return new NActors::IEventHandle(self, self, new TEvents::TEvBootstrap(), 0);
        }

    protected:
        TVector<NActors::TActorId> Actors;
        int Pos;
    };

} // NActors
