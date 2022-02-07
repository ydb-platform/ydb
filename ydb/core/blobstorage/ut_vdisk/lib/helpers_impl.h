#pragma once

#include "defs.h"
#include "helpers.h"


///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
namespace NKikimr {

    template <class TRequestFunc, class TCheckResponseFunc>
    class TOneGetActor : public TActorBootstrapped<TOneGetActor<TRequestFunc, TCheckResponseFunc> > {
    protected:
        typedef TOneGetActor<TRequestFunc, TCheckResponseFunc> TThis;
        const TActorId NotifyID;
        TRequestFunc RequestFunc;
        TCheckResponseFunc CheckResponseFunc;

    private:
        friend class TActorBootstrapped<TThis>;

        void Bootstrap(const TActorContext &ctx) {
            TThis::Become(&TThis::StateFuncGet);
            RequestFunc(ctx);
        }

        void Handle(TEvBlobStorage::TEvVGetResult::TPtr &ev, const TActorContext &ctx) {
            CheckResponseFunc(ev, ctx);
            ctx.Send(NotifyID, new TEvents::TEvCompleted());
            TThis::Die(ctx);
        }

        STRICT_STFUNC(StateFuncGet,
            HFunc(TEvBlobStorage::TEvVGetResult, Handle);
            IgnoreFunc(TEvBlobStorage::TEvVWindowChange);
        )

    public:
        TOneGetActor(const TActorId &notifyID, TRequestFunc requestFunc, TCheckResponseFunc checkRespFunc)
            : TActorBootstrapped<TOneGetActor>()
            , NotifyID(notifyID)
            , RequestFunc(requestFunc)
            , CheckResponseFunc(checkRespFunc)
        {}
    };

} // NKikimr

template <class TRequestFunc, class TCheckResponseFunc>
NActors::IActor *CreateOneGet(const NActors::TActorId &notifyID, TRequestFunc req, TCheckResponseFunc check) {
    return new NKikimr::TOneGetActor<TRequestFunc, TCheckResponseFunc>(notifyID, req, check);
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
