#pragma once

#include "world.h"
#include "events.h"
#include <ydb/core/tablet_flat/util_fmt_logger.h>
#include <ydb/core/tablet_flat/util_fmt_abort.h>
#include <ydb/core/tablet_flat/util_fmt_desc.h>
#include <ydb/core/tablet_flat/util_fmt_basic.h>
#include <ydb/library/actors/core/actor.h>

namespace NKikimr {
namespace NFake {

    class TLeader: public ::NActors::IActorCallback {
        enum class EState {
            Lock    = 0,    /* Do not start shutdown process    */
            Free    = 1,    /* Start to shut as Head will gone  */
            Shut    = 2,    /* In progress, waiting for EvGone  */
            Gone    = 8,
        };

    public:
        using ELnLev = NUtil::ELnLev;

        TLeader(ui32 head, TAtomic &stopped)
            : ::NActors::IActorCallback(static_cast<TReceiveFunc>(&TLeader::Inbox), NKikimrServices::TActivity::FAKE_ENV_A)
            , Time(TAppData::TimeProvider.Get())
            , Edge(head) /* Live until this runlevel exists */
            , Stopped(stopped)
        {
             Y_ABORT_UNLESS(Edge < Levels.size(), "Out of runlevels slots");
        }

    private:
        void Registered(TActorSystem *sys, const TActorId&) override
        {
            Sys = sys, Start = Time->Now();

            Logger = new NUtil::TLogger(sys, NKikimrServices::FAKE_ENV);

            /* This actor takes functions of NFake root actor that should
                perform correct system shutdown on test scene completion. */

            Sys->RegisterLocalService(TWorld::Where(EPath::Root), SelfId());
        }

        void Inbox(TAutoPtr<::NActors::IEventHandle> &eh)
        {
            if (auto *fire = eh->CastAsLocal<NFake::TEvFire>()) {
                DoFire(fire->Level, fire->Alias, std::move(fire->Cmd));
            } else if (eh->CastAsLocal<TEvents::TEvGone>()) {
                HandleGone(eh->Sender);
            } else if (eh->CastAsLocal<TEvents::TEvPoison>()) {
                State = Max(State, EState::Free);

                DoShutDown(true);
            } else if (eh->CastAsLocal<TEvents::TEvWakeup>()) {

                if (Last == TInstant::Max()) {
                    Wait = false; /* Do not expect more TEvGone */
                } else if (Time->Now() - Last < Delay) {
                    Schedule(Gran, new TEvents::TEvWakeup);
                } else if (auto logl = Logger->Log(ELnLev::Abort)) {
                    logl
                        << "Model shutdown stucked at level " << Head
                        << ", left " << Levels.at(Head).Left << " actors"
                        << ", spent " << NFmt::TDelay(Time->Now() - Last);
                }

            } else if (eh->CastAsLocal<NFake::TEvTerm>()) {

            } else {
                Y_Fail("Unexpected event " << eh->GetTypeName());
            }
        }

        void HandleGone(const TActorId &actor) noexcept
        {
            auto it = Childs.find(actor);

            if (it != Childs.end()) {
                const auto level = it->second;

                NUtil::SubSafe(Levels.at(level).Left, ui32(1));
                Childs.erase(it);

                if (level == Head && State >= EState::Free)
                    DoShutDown(false);

            } else if (auto logl = Logger->Log(ELnLev::Abort)) {
                logl
                    << "Leader got gone event from actor " << actor
                    << ", but it hasn't been registered in runlevels";
            }
        }

        void DoFire(ui32 level, const TActorId &alias, TActorSetupCmd cmd)
        {
            if (level <= Edge && Levels[level].Alive) {
                auto actor = Register(cmd.Actor.release(), cmd.MailboxType, 0);
                auto result = Childs.emplace(actor, level);

                Y_ABORT_UNLESS(result.second, "Cannot register same actor twice");

                Levels[level].Left += 1, Total += 1, Head = Max(Head, level);

                if (alias)
                    Sys->RegisterLocalService(alias, actor);

                if (level == Edge && State == EState::Lock)
                    State = EState::Free;

            } else if (auto logl = Logger->Log(ELnLev::Abort)) {
                logl
                    << "Cannot register " << TypeName(*cmd.Actor) << " on "
                    << " level " << level << ", head " << Head << ".." << Edge;
            }
        }

        void DoShutDown(bool force) noexcept
        {
            force = force && (State < EState::Shut);

            if (std::exchange(State, Max(State, EState::Shut)) < EState::Shut) {
                if (auto logl = Logger->Log(ELnLev::Info)) {
                    logl
                        << "Model starts " << (force ? "hard" : "soft")
                        << " shutdown on level " << Head << " of " << Edge
                        << ", left " << Childs.size() << " actors";
                }
            }

            while (State != EState::Gone) {
                if (Levels.at(Head).Left && !std::exchange(force, false)) {
                    break;
                } else if (Levels.at(Head).Left == 0 && Head == 0) {
                    if (auto logl = Logger->Log(ELnLev::Info)) {
                        logl
                            << "Model stopped, hosted " << Total << " actors"
                            << ", spent " << NFmt::TDelay(Time->Now() - Start);
                    }

                    State = EState::Gone, Last = TInstant::Max(), Stopped = 1;

                    Send(SelfId(), new NFake::TEvTerm); /* stops the world */

                } else if (std::exchange(Levels[Head].Alive, false)) {
                    Last = Time->Now();

                    for (auto &it : Childs) {
                        if (it.second == Head) {
                            Send(it.first, new TEvents::TEvPoison);

                            if (!std::exchange(Wait, true))
                                Schedule(Gran, new TEvents::TEvWakeup);
                        }
                    }
                } else if (Levels[Head].Left == 0 && Head > 0) {
                    Head -= 1, force = true; /* Try to shutdown next level */
                }
            }
        }

    private:
        struct TLevel {
            ui32 Left = 0;
            bool Alive = true;
        };

        ITimeProvider * const Time = nullptr;
        TActorSystem * Sys = nullptr;
        const TDuration Gran{ TDuration::Seconds(1) };
        const TDuration Delay{ TDuration::Seconds(32) };
        TAutoPtr<NUtil::ILogger> Logger;
        EState State = EState::Lock;
        bool Wait = false;
        TInstant Start = TInstant::Max();
        TInstant Last = TInstant::Max();
        const ui32 Edge = Max<ui32>();
        ui32 Head = 0;
        ui64 Total = 0;
        TAtomic &Stopped;
        std::map<TActorId, ui32> Childs;
        std::array<TLevel, 9> Levels;
     };
}
}
