#pragma once

namespace NActors {
    class TSenderBaseActor: public TActorBootstrapped<TSenderBaseActor> {
    protected:
        const TActorId RecipientActorId;
        const ui32 Preload;
        ui64 SequenceNumber = 0;
        ui32 InFlySize = 0;

    public:
        TSenderBaseActor(const TActorId& recipientActorId, ui32 preload = 1)
            : RecipientActorId(recipientActorId)
            , Preload(preload)
        {
        }

        virtual ~TSenderBaseActor() {
        }

        virtual void Bootstrap(const TActorContext& ctx) {
            Become(&TSenderBaseActor::StateFunc);
            ctx.Send(ctx.ExecutorThread.ActorSystem->InterconnectProxy(RecipientActorId.NodeId()), new TEvInterconnect::TEvConnectNode);
        }

        virtual void SendMessagesIfPossible(const TActorContext& ctx) {
            while (InFlySize < Preload) {
                SendMessage(ctx);
            }
        }

        virtual void SendMessage(const TActorContext& /*ctx*/) {
            ++SequenceNumber;
        }

        virtual void Handle(TEvents::TEvUndelivered::TPtr& /*ev*/, const TActorContext& ctx) {
            SendMessage(ctx);
        }

        virtual void Handle(TEvTestResponse::TPtr& /*ev*/, const TActorContext& ctx) {
            SendMessagesIfPossible(ctx);
        }

        void Handle(TEvInterconnect::TEvNodeConnected::TPtr& /*ev*/, const TActorContext& ctx) {
            SendMessagesIfPossible(ctx);
        }

        void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr& /*ev*/, const TActorContext& /*ctx*/) {
        }

        virtual void Handle(TEvents::TEvPoisonPill::TPtr& /*ev*/, const TActorContext& ctx) {
            Die(ctx);
        }

        virtual STRICT_STFUNC(StateFunc,
            HFunc(TEvTestResponse, Handle)
            HFunc(TEvents::TEvUndelivered, Handle)
            HFunc(TEvents::TEvPoisonPill, Handle)
            HFunc(TEvInterconnect::TEvNodeConnected, Handle)
            HFunc(TEvInterconnect::TEvNodeDisconnected, Handle)
        )
    };

    class TReceiverBaseActor: public TActor<TReceiverBaseActor> {
    protected:
        ui64 ReceivedCount = 0;

    public:
        TReceiverBaseActor()
            : TActor(&TReceiverBaseActor::StateFunc)
        {
        }

        virtual ~TReceiverBaseActor() {
        }

        virtual STRICT_STFUNC(StateFunc,
            HFunc(TEvTest, Handle)
        )

        virtual void Handle(TEvTest::TPtr& /*ev*/, const TActorContext& /*ctx*/) {}
    };
}
