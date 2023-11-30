#pragma once

#include "defs.h"
#include <ydb/core/util/activeactors.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/blobstorage.h>

namespace NKikimr {

    inline TString ToStringLocalTimeUpToSeconds(const TInstant &time) {
        return time.GetValue() ? time.ToStringLocalUpToSeconds() : "0";
    }

    ////////////////////////////////////////////////////////////////////////////
    // RunInBatchPool
    ////////////////////////////////////////////////////////////////////////////
    inline TActorId RunInBatchPool(const TActorContext &ctx, IActor *actor) {
        ui32 poolId = AppData(ctx)->BatchPoolId;
        TActorId actorID = ctx.Register(actor, TMailboxType::HTSwap, poolId);
        return actorID;
    }

    template<typename T, typename A>
    inline void AppendToVector(TVector<T, A>& to, TVector<T, A>&& from) {
        if (to) {
            to.insert(to.end(), from.begin(), from.end());
        } else {
            to.swap(from);
        }
    }

    template<typename T, typename A>
    inline void AppendToVector(TVector<T, A>& to, const TVector<T, A>& from) {
        to.insert(to.end(), from.begin(), from.end());
    }


    template<typename TContainer>
    inline TString FormatList(const TContainer& cont) {
        TStringStream str;
        str << "[";
        bool first = true;
        for (const auto& item : cont) {
            if (first) {
                first = false;
            } else {
                str << " ";
            }
            str << item;
        }
        str << "]";
        return str.Str();
    }

    template<typename TContainer>
    inline void FormatList(IOutputStream &str, const TContainer& cont) {
        str << "[";
        bool first = true;
        for (const auto& item : cont) {
            if (first) {
                first = false;
            } else {
                str << " ";
            }
            item.Output(str);
        }
        str << "]";
    }

    ////////////////////////////////////////////////////////////////////////////
    // TChildrenSweeper
    // TChildrenSweeper helps to manage child actors until their death.
    // SetupUndertakerCounter setups how many actors we wait, for each death
    // call DecrementUndertakerCounter.
    // When UndertakerCounter becomes zero, notify the caller of TEvPoisonPill
    // and die itself.
    ////////////////////////////////////////////////////////////////////////////
    template <typename TDerivedActor>
    class TChildrenSweeper {
    protected:
        unsigned UndertakerCounter;
        TActorId UndertakerNotifyId;

        void SetupUndertakerCounter(const TActorContext &ctx, const TActorId notifyId, unsigned eventsToWait) {
            UndertakerCounter = eventsToWait;
            UndertakerNotifyId = notifyId;
            UndertakerCheckToDie(ctx);
        }

        void DecrementUndertakerCounter(const TActorContext &ctx) {
            Y_DEBUG_ABORT_UNLESS(UndertakerCounter > 0);
            --UndertakerCounter;
            UndertakerCheckToDie(ctx);
        }

        void UndertakerCheckToDie(const TActorContext &ctx) {
            if (UndertakerCounter == 0) {
                ctx.Send(UndertakerNotifyId, new TEvents::TEvPoisonTaken());
                static_cast<TDerivedActor *>(this)->Die(ctx);
            }
        }

        TChildrenSweeper()
            : UndertakerCounter(0)
            , UndertakerNotifyId()
        {}
    };

    ////////////////////////////////////////////////////////////////////////////
    // IActorNotify -- interface for notifying about something from datastructure
    // internals. We use it to send messages in case of death of some object
    ////////////////////////////////////////////////////////////////////////////
    class IActorNotify {
    public:
        virtual void Notify(std::function<IEventBase *()> &&message) = 0;
        virtual ~IActorNotify() = default;
    };

    ////////////////////////////////////////////////////////////////////////////
    // TFakeNotify -- fake implementation of IActorNotify
    ////////////////////////////////////////////////////////////////////////////
    class TFakeNotify : public IActorNotify {
    public:
        void Notify(std::function<IEventBase *()> &&) override {
            // nothing to do
        }
    };

    ////////////////////////////////////////////////////////////////////////////
    // TActorNotify -- implementation of IActorNotify for Actor System
    ////////////////////////////////////////////////////////////////////////////
    class TActorNotify : public IActorNotify {
    public:
        TActorNotify(TActorSystem *as, TActorId notifyId)
            : ActorSystem(as)
            , NotifyId(notifyId)
        {}

        void Notify(std::function<IEventBase *()> &&message) override {
            if (ActorSystem) {
                ActorSystem->Send(NotifyId, message());
            }
        }

    private:
        TActorSystem *ActorSystem = nullptr;
        TActorId NotifyId;
    };

} // NKikimr

