#pragma once

#include "world.h"
#include "storage.h"
#include <ydb/library/actors/core/actor.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/tablet_flat/util_fmt_logger.h>
#include <util/system/type_name.h>

#include <array>

namespace NKikimr {
namespace NFake {

    class TWarden : public ::NActors::IActorCallback {

        enum class EState : ui8 {
            Forbid  = 0,
            Allow   = 1,
            Fired   = 2,
            Shut    = 3,
            Gone    = 8,
        };

    public:
        using TEventHandlePtr = TAutoPtr<::NActors::IEventHandle>;
        using ELnLev = NUtil::ELnLev;

        TWarden(ui32 groups)
            : ::NActors::IActorCallback(static_cast<TReceiveFunc>(&TWarden::Inbox), NKikimrServices::TActivity::FAKE_ENV_A)
        {
             Y_ABORT_UNLESS(groups < State.size(), "Too many groups requested");

             for (auto group: xrange(groups))
                State[group] = EState::Allow;
        }

    private:
        void Registered(TActorSystem *sys, const TActorId &owner) override
        {
            Sys = sys, Owner = owner;

            Logger = new NUtil::TLogger(sys, NKikimrServices::FAKE_ENV);
        }

        void Inbox(TEventHandlePtr &eh)
        {
            if (ShouldForward(eh->GetTypeRewrite())) {
                auto proxy = eh.Get()->GetForwardOnNondeliveryRecipient();
                auto group = GroupIDFromBlobStorageProxyID(proxy);

                if (group >= State.size() || State[group] == EState::Forbid) {
                    if (auto logl = Logger->Log(ELnLev::Abort)) {
                        logl
                            << "BS group " << group << " is not configured"
                            << ", ev " << eh->GetTypeName();
                    }

                    return; /* cannot process unknown groups */

                } else if (State[group] == EState::Allow) {
                    State[group] = EState::Fired;

                    Y_ABORT_UNLESS(++Alive <= State.size(), "Out of group states");

                    StartGroup(group);

                } else if (State[group] >= EState::Shut) {
                    if (auto logl = Logger->Log(ELnLev::Crit)) {
                        logl
                            << "BS group " << group << " is unavailable"
                            << ", ev " << eh->GetTypeName();
                    }

                    auto why = TEvents::TEvUndelivered::ReasonActorUnknown;

                    TlsActivationContext->Send(eh->ForwardOnNondelivery(eh, why));

                    return;
                }

                TlsActivationContext->Forward(eh, proxy);

            } else if (eh->CastAsLocal<TEvents::TEvPoison>()) {
                if (std::exchange(Shutting, true)) {
                    Y_ABORT("Got double BS storage shut order");
                } else if (auto logl = Logger->Log(ELnLev::Info))
                    logl << "Shut order, stopping " << Alive << " BS groups";

                for (auto group: xrange(State.size())) {
                    if (State[group] == EState::Fired) {
                        auto to = MakeBlobStorageProxyID(group);

                        Send(to, new TEvents::TEvPoison);

                        State[group] = EState::Shut;
                    } else {
                        State[group] = EState::Gone;
                    }
                }

                TryToDie();

            } else if (eh->CastAsLocal<TEvents::TEvGone>()) {
                const auto group = eh->Cookie;

                if (group >= State.size() || State[group] < EState::Fired) {
                    Y_ABORT("Got an TEvGone event form unknown BS group");
                } else if (!Shutting || State[group] != EState::Shut) {
                    Y_ABORT("Got unexpected TEvGone from BS group mock");
                }

                --Alive, State[group] = EState::Gone;

                TryToDie();

            } else if (eh->CastAsLocal<NFake::TEvTerm>()) {

            } else {
                Y_ABORT("Got unexpected message");
            }
        }

        void StartGroup(ui32 group) noexcept
        {
            if (auto logl = Logger->Log(ELnLev::Info))
                logl << "Starting storage for BS group " << group;

            auto actor = Register(new NFake::TStorage(group));

            Sys->RegisterLocalService(MakeBlobStorageProxyID(group), actor);
        }

        void TryToDie() noexcept
        {
            if (Shutting && Alive == 0) {
                if (auto logl = Logger->Log(ELnLev::Info))
                    logl << "All BS storage groups are stopped";

                Send(Owner, new TEvents::TEvGone);
                PassAway();
            }
        }

        static constexpr bool ShouldForward(ui32 ev) noexcept
        {
            return
                ev == TEvBlobStorage::EvPut
                || ev == TEvBlobStorage::EvGet
                || ev == TEvBlobStorage::EvBlock
                || ev == TEvBlobStorage::EvDiscover
                || ev == TEvBlobStorage::EvRange
                || ev == TEvBlobStorage::EvCollectGarbage
                || ev == TEvBlobStorage::EvStatus;
        }

    public:
        TActorSystem * Sys = nullptr;
        TAutoPtr<NUtil::ILogger> Logger;
        TActorId Owner;         /* ActorID of the leader proceess   */
        ui32 Alive = 0;         /* Groups in EState::{Fired,Shut}   */
        bool Shutting = false;  /* Storages is being in shutted     */
        std::array<EState, 32> State{ };
    };

}
}
