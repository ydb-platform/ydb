#pragma once

#include "world.h"
#include "events.h"
#include <util/system/type_name.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/tablet.h>
#include <ydb/core/mind/local.h>
#include <ydb/core/tablet/tablet_setup.h>
#include <ydb/core/tablet_flat/util_fmt_logger.h>
#include <ydb/core/tablet_flat/util_fmt_abort.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NFake {

    class TOwner final : public ::NActors::IActorCallback {
    public:
        using TEventHandlePtr = TAutoPtr<::NActors::IEventHandle>;
        using ELnLev = NUtil::ELnLev;
        using TInfo = TTabletStorageInfo;
        using TSetup = TTabletSetupInfo;

        TOwner(TActorId user, ui32 limit, TIntrusivePtr<TInfo> info, TIntrusivePtr<TSetup> setup, ui32 followerId)
            : ::NActors::IActorCallback(static_cast<TReceiveFunc>(&TOwner::Inbox), NKikimrServices::TActivity::FAKE_ENV_A)
            , Info(std::move(info))
            , Setup(std::move(setup))
            , User(user)
            , Limit(Max(ui32(1), limit))
            , FollowerId(followerId)
        {
            Y_ABORT_UNLESS(TTabletTypes::TypeInvalid != Info->TabletType);
        }

    private:
        void Registered(TActorSystem *sys, const TActorId &owner) override
        {
            Owner = owner;
            Logger = new NUtil::TLogger(sys, NKikimrServices::FAKE_ENV);

            sys->Send(SelfId(), new TEvents::TEvBootstrap);
        }

        void Inbox(TEventHandlePtr &eh)
        {
            if (auto *ev = eh->CastAsLocal<TEvTablet::TEvTabletDead>()) {

                if (ev->Reason != TEvTablet::TEvTabletDead::ReasonPill) {
                    if (auto logl = Logger->Log(ELnLev::Emerg)) {
                        logl
                            << "Tablet " << Info->TabletID << " suddenly died"
                            << ", " << (Alive ? "alive" : "dead")
                            << ", borns " << Borns << ", reason " << ev->Reason;
                    }

                    Send(TWorld::Where(EPath::Root), new TEvents::TEvPoison);

                    DoSuicide();

                } else if (Borns >= Limit) {
                    DoSuicide();
                } else {
                    Alive = false, Agent = { }, Start(this->ActorContext());
                }

            } else if (eh->CastAsLocal<TEvTablet::TEvRestored>()) {
                Alive = true;
            } else if (eh->CastAsLocal<TEvLocal::TEvTabletMetrics>()) {

            } else if (eh->CastAsLocal<TEvents::TEvBootstrap>()) {
                Start(this->ActorContext());
            } else if (eh->CastAsLocal<TEvents::TEvPoison>()) {
                if (auto logl = Logger->Log(ELnLev::Debug))
                    logl << "Got kill req for Tablet " << Info->TabletID;

                Send(Agent, new TEvents::TEvPoison);
            } else if (eh->CastAsLocal<TEvTablet::TEvReady>()) {

            } else {
                Y_Fail("Unexpected event " << eh->GetTypeName());
            }
        }

        void Start(const TActorContext &ctx) noexcept
        {
            Y_ABORT_UNLESS(!Agent, "Tablet actor is already started");

            if (auto logl = Logger->Log(ELnLev::Debug)) {
                logl
                    << "Starting tablet " << Info->TabletID
                    << ", hope " << Borns << " of " << Limit;
            }

            auto &profile = AppData(ctx)->ResourceProfiles;

            if (FollowerId == 0) {
                Agent = Setup->Tablet(Info.Get(), SelfId(), ctx, 0, profile);
            } else {
                Agent = Setup->Follower(Info.Get(), SelfId(), ctx, FollowerId, profile);
            }

            Y_ABORT_UNLESS(Agent, "Failed to start new tablet actor");

            Borns += 1;
        }

        void DoSuicide() noexcept
        {
            Send(std::exchange(Owner, { }), new TEvents::TEvGone);
            Send(std::exchange(User, { }), new TEvents::TEvGone);

            PassAway();
        }

    private:
        TIntrusivePtr<TInfo> Info;
        TIntrusivePtr<TSetup> Setup;
        TAutoPtr<NUtil::ILogger> Logger;

        TActorId Owner;
        TActorId Agent;
        TActorId User;
        ui32 Borns = 0;
        ui32 Limit = 1;
        const ui32 FollowerId;
        bool Alive = false;
    };

}
}
