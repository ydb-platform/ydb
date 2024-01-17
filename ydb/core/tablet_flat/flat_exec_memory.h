#pragma once

#include "flat_exec_seat.h"
#include "flat_exec_broker.h"
#include "util_fmt_line.h"

#include <ydb/library/actors/core/actor.h>

namespace NKikimr {
namespace NTabletFlatExecutor {

    class TMemory {
        static constexpr ui32 MyPrio = 5;

    public:
        using ELnLev = NUtil::ELnLev;
        using TEvs = NResourceBroker::TEvResourceBroker;
        using ETablet = NKikimrTabletBase::TTabletTypes::EType;
        using IOps = NActors::IActorOps;

        struct TUsed {
            void Describe(IOutputStream &out) const noexcept
            {
                out << "Memory{" << Static << " dyn " << Dynamic << "}";
            }

            ui64 Static = 0;
            ui64 Dynamic = 0;
        };

        struct TCookie : public TResource {
            TCookie(TSeat *seat)
                : TResource(ESource::Seat)
                , Seat(seat)
            {

            }

            TSeat * const Seat = nullptr;
        };

        TMemory(
                NUtil::ILogger *logger,
                IOps *ops,
                TIntrusivePtr<TIdEmitter> emitter,
                TString taskNameSuffix = { })
            : Logger(logger)
            , Ops(ops)
            , Emitter(emitter)
            , Profiles(new TResourceProfiles)
            , TaskNameSuffix(std::move(taskNameSuffix))
        {

        }

        const TUsed& Stats() const noexcept { return Used; }

        ui64 RemainedStatic(const TSeat &seat) noexcept
        {
            ui64 txLimit = Profile->GetStaticTxMemoryLimit()
                ? Profile->GetStaticTxMemoryLimit() : Max<ui64>();
            if (txLimit <= seat.CurrentMemoryLimit)
                return 0;
            const ui64 remain = txLimit - seat.CurrentMemoryLimit;

            ui64 tabletLimit = Profile->GetStaticTabletTxMemoryLimit()
                ? Profile->GetStaticTabletTxMemoryLimit() : Max<ui64>();
            if (tabletLimit <= Used.Static)
                return 0;

            return Min(remain, tabletLimit - Used.Static);
        }

        void RequestLimit(TSeat &seat, ui64 desired) noexcept
        {
            seat.CurrentMemoryLimit = desired;

            const auto type = GetTaskType(seat.CurrentMemoryLimit);

            ui64 taskId = seat.TaskId ? seat.TaskId : Emitter->Do();

            if (auto logl = Logger->Log(ELnLev::Debug)) {
                logl
                    << NFmt::Do(*Ops) << " " << NFmt::Do(seat) << " "
                    << (seat.TaskId ? "update" : "request") << " Res{"
                    << taskId << " " << seat.CurrentMemoryLimit << "b}"
                    << " type " << type;
            }

            if (seat.TaskId) {
                Send(new TEvs::TEvUpdateTask(
                         seat.TaskId,
                         {{ 0, seat.CurrentMemoryLimit }},
                         type, MyPrio, true));
            } else {
                Send(new TEvs::TEvSubmitTask(
                         taskId,
                         NFmt::Ln(seat) + TaskNameSuffix,
                         {{ 0, seat.CurrentMemoryLimit }},
                         type, MyPrio, new TCookie(&seat)));
            }
        }

        void AcquiredMemory(TSeat &seat, ui32 task) noexcept
        {
            seat.TaskId = task;

            Used.Dynamic += seat.CurrentMemoryLimit;

            if (auto logl = Logger->Log(ELnLev::Debug)) {
                logl
                    << NFmt::Do(*Ops) << " " << NFmt::Do(seat)
                    << " acquired dyn mem Res{" << seat.TaskId << " "
                    << seat.CurrentMemoryLimit << "b}, " << NFmt::Do(Used);
            }
        }

        void ReleaseMemory(TSeat &seat) noexcept
        {
            if (!seat.TaskId) {
                FreeStatic(seat, seat.CapturedMemory ? seat.CapturedMemory->Size : 0);
            } else if (seat.CapturedMemory) {
                CaptureDynamic(seat);
            } else {
                Used.Dynamic -= seat.CurrentMemoryLimit;

                if (auto logl = Logger->Log(ELnLev::Debug)) {
                    logl
                        << NFmt::Do(*Ops) << " " << NFmt::Do(seat)
                        << " release Res{"<< seat.TaskId
                        << " " << seat.CurrentMemoryLimit << "b}"
                        << ", " << NFmt::Do(Used);
                }

                Send(new TEvs::TEvFinishTask(seat.TaskId));

                seat.TaskId = 0;
            }

            if (seat.CapturedMemory) {
                if (auto logl = Logger->Log(ELnLev::Debug)) {
                    logl
                        << NFmt::Do(*Ops) << " " << NFmt::Do(seat) << " "
                        << "captured " << NFmt::Do(*seat.CapturedMemory);
                }

                Tokens.insert(std::move(seat.CapturedMemory));
            }

            if (seat.AttachedMemory) {
                if (seat.AttachedMemory->GCToken->TaskId) {
                    Send(new TEvs::TEvFinishTask(seat.AttachedMemory->GCToken->TaskId));

                    Used.Dynamic -= seat.AttachedMemory->GCToken->Size;
                } else {
                    Used.Static -= seat.AttachedMemory->GCToken->Size;
                }

                if (auto logl = Logger->Log(ELnLev::Debug)) {
                    logl
                        << NFmt::Do(*Ops) << " release attached "
                        << NFmt::Do(*seat.AttachedMemory->GCToken)
                        << ", " << NFmt::Do(Used);
                }

                Tokens.erase(seat.AttachedMemory->GCToken);
                seat.AttachedMemory = nullptr;
            }
        }

        void ReleaseTxData(TSeat &seat) noexcept
        {
            if (seat.CapturedMemory) {

                if (auto logl = Logger->Log(ELnLev::Debug)) {
                    logl
                        << NFmt::Do(*Ops) << " " << NFmt::Do(seat) << " "
                        << "captured " << NFmt::Do(*seat.CapturedMemory);
                }

                if (seat.CapturedMemory->TaskId) {
                    CaptureDynamic(seat);
                } else {
                    seat.CurrentMemoryLimit -= seat.CapturedMemory->Size;
                    seat.CurrentTxDataLimit -= seat.CapturedMemory->Size;
                }

                Tokens.insert(std::move(seat.CapturedMemory));

                ScheduleGC();
            }

            if (seat.TaskId) {
                Used.Dynamic -= seat.CurrentMemoryLimit;

                if (auto logl = Logger->Log(ELnLev::Debug)) {
                    logl
                        << NFmt::Do(*Ops) << " released on update Res{"
                        << seat.TaskId << " " << seat.CurrentMemoryLimit << "b}"
                        << ", " << NFmt::Do(Used);
                }
            }
        }

        void AttachMemory(TSeat &seat) noexcept
        {
            const ui64 taskId = seat.AttachedMemory->GCToken->TaskId;
            const ui64 bytes = seat.AttachedMemory->GCToken->Size;

            if (auto logl = Logger->Log(ELnLev::Debug)) {
                logl
                    << NFmt::Do(*Ops) << " found attached "
                    << NFmt::Do(*seat.AttachedMemory->GCToken);
            }

            if (!taskId) {
                /* Attached static memory is just released here. It will
                    be allocated back if needed and possible. */

                Used.Static -= bytes;
                seat.CurrentTxDataLimit = bytes;

                if (auto logl = Logger->Log(ELnLev::Debug)) {
                    logl
                        << NFmt::Do(*Ops) << " release captured by tx "
                        << NFmt::Do(*seat.AttachedMemory->GCToken)
                        << ", " << NFmt::Do(Used);
                }

            } else if (seat.TaskId) {
                // If we have two tasks then merge resources into a single one.
                seat.CurrentTxDataLimit = bytes;
                seat.CurrentMemoryLimit += bytes;

                auto limit = Profile->GetTxMemoryLimit();

                if (limit && seat.CurrentMemoryLimit > limit)
                    seat.CurrentMemoryLimit = limit;

                const auto type = GetTaskType(seat.CurrentMemoryLimit);

                if (auto logl = Logger->Log(ELnLev::Debug)) {
                    logl
                        << NFmt::Do(*Ops) << " moving tx data from attached "
                        << NFmt::Do(*seat.AttachedMemory->GCToken)
                        << " to Res{" << seat.TaskId << " ...}";
                }

                Send(new TEvs::TEvUpdateTask(
                         seat.TaskId,
                         {{ 0, seat.CurrentMemoryLimit }},
                         type, MyPrio, false));

                Send(new TEvs::TEvFinishTask(taskId));

            } else {
                // Don't support mix of static and dynamic memory.
                // Release static memory then.
                seat.TaskId = taskId;
                Used.Static -= seat.CurrentMemoryLimit;

                if (auto logl = Logger->Log(ELnLev::Debug)) {
                    logl
                        << "release " << seat.CurrentMemoryLimit << "b of"
                        << " static tx data due to attached res " << taskId
                        << ", " << NFmt::Do(Used);
                }

                Send(new TEvs::TEvUpdateTaskCookie(taskId, new TCookie(&seat)));

                seat.CurrentTxDataLimit = bytes;
                seat.CurrentMemoryLimit = bytes;
            }

            Tokens.erase(seat.AttachedMemory->GCToken);
            seat.AttachedMemory = nullptr;
        }

        void AllocStatic(TSeat &seat, ui64 newLimit) noexcept
        {
            Y_ABORT_UNLESS(newLimit >= seat.CurrentTxDataLimit + seat.MemoryTouched);

            Used.Static -= seat.CurrentMemoryLimit;
            seat.CurrentMemoryLimit = newLimit;
            Used.Static += seat.CurrentMemoryLimit;

            if (auto logl = Logger->Log(ELnLev::Debug)) {
                logl
                    << NFmt::Do(*Ops) << " " << NFmt::Do(seat) << " took "
                    << newLimit << "b of static mem, " << NFmt::Do(Used);
            }
        }

        void FreeStatic(TSeat &seat, ui64 hold) noexcept
        {
            if (seat.TaskId)
                return;

            Y_ABORT_UNLESS(seat.CurrentMemoryLimit >= hold);
            ui64 release = seat.CurrentMemoryLimit - hold;
            Used.Static -= release;

            if (auto logl = Logger->Log(ELnLev::Debug)) {
                logl
                    << NFmt::Do(*Ops) << " " << NFmt::Do(seat) << " release"
                    << " " << release << "b of static, " << NFmt::Do(Used);
            }
        }

        void RunMemoryGC() noexcept
        {
            GCScheduled = false;

            TVector<TIntrusivePtr<TMemoryGCToken>> dropped;

            for (auto it = Tokens.begin(); it != Tokens.end(); ++it) {
                if ((*it)->IsDropped()) {
                    if ((*it)->TaskId) {
                        Send(new TEvs::TEvFinishTask((*it)->TaskId));

                        Used.Dynamic -= (*it)->Size;
                    } else {
                        Used.Static -= (*it)->Size;
                    }

                    if (auto logl = Logger->Log(ELnLev::Crit)) {
                        logl
                            << NFmt::Do(*Ops) << " released leaked "
                            << NFmt::Do(**it) << ", " << NFmt::Do(Used);
                    }

                    dropped.push_back(*it);
                }
            }

            for (auto &token : dropped)
                Tokens.erase(token);

            ScheduleGC();
        }

        void ScheduleGC() noexcept
        {
            if (Tokens && !std::exchange(GCScheduled, true))
                Ops->Schedule(TDuration::Minutes(1),
                    new TEvents::TEvWakeup(ui64(EWakeTag::Memory)));
        }

        void DumpStateToHTML(IOutputStream &out) noexcept
        {
            HTML(out) {
                DIV_CLASS("row") {out << "profile: " << Profile->ShortDebugString(); }
                DIV_CLASS("row") {out << "static memory: " << Used.Static; }
                DIV_CLASS("row") {out << "dynamic memory: " << Used.Dynamic; }

                for (auto &token : Tokens) {
                    DIV_CLASS("row") {
                        out << "captured memory: " << NFmt::Do(*token);
                    }
                }
            }
        }

        void SetProfiles(TResourceProfilesPtr profiles) noexcept
        {
            Profiles = profiles ? profiles : new TResourceProfiles;
        }

        void UseProfile(ETablet type, const TString &name) noexcept
        {
            Profile = Profiles->GetProfile(type, name);
        }

    private:
        void CaptureDynamic(TSeat &seat) noexcept
        {
            const auto type =  GetTaskType(seat.CapturedMemory->Size);

            Send(new TEvs::TEvUpdateTask(
                     seat.TaskId,
                     {{ 0, seat.CapturedMemory->Size }},
                     type, MyPrio, false));

            auto released = seat.CurrentMemoryLimit - seat.CapturedMemory->Size;
            Used.Dynamic -= released;

            if (auto logl = Logger->Log(ELnLev::Debug)) {
                logl
                    << NFmt::Do(*Ops) << " " << NFmt::Do(seat)
                    << " update resource task " << seat.CapturedMemory->TaskId
                    << " releasing " << released << "b, " << NFmt::Do(Used);
            }

            seat.TaskId = 0;
            seat.CurrentMemoryLimit = 0;
            seat.CurrentTxDataLimit -= seat.CapturedMemory->Size;
        }

        void Send(TAutoPtr<IEventBase> event)
        {
            using namespace NResourceBroker;

            Ops->Send(MakeResourceBrokerID(), event.Release(), 0);
        }

        TString GetTaskType(ui64 limit) const noexcept
        {
            if (limit <= Profile->GetSmallTxMemoryLimit())
                return Profile->GetSmallTxTaskType();
            if (limit <= Profile->GetMediumTxMemoryLimit())
                return Profile->GetMediumTxTaskType();
            return Profile->GetLargeTxTaskType();
        }

    private:
        NUtil::ILogger * const Logger = nullptr;
        IOps * const Ops = nullptr;
        const TIntrusivePtr<TIdEmitter> Emitter;

        bool GCScheduled = false;
        TUsed Used;
        THashSet<TIntrusivePtr<TMemoryGCToken>, TPtrHash> Tokens;
        TIntrusivePtr<TResourceProfiles> Profiles;
        TString TaskNameSuffix;

    public:
        TResourceProfiles::TPtr Profile;
    };

}
}
