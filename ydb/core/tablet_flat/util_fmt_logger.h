#pragma once

#include "util_fmt_line.h"

#include <ydb/core/base/appdata.h>
#include <library/cpp/time_provider/time_provider.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/log_iface.h>

namespace NKikimr {
namespace NUtil {

    class TLogger : private ILogged, public ILogger {
    public:
        using TSys = ::NActors::TActorSystem;
        using EComp = ::NActors::NLog::EComponent;
        using EPrio = ::NActors::NLog::EPrio;
        using TPath = ::NActors::TActorId;

        TLogger(TSys *sys, EComp comp)
            : Time(TAppData::TimeProvider.Get())
            , Sys(sys)
            , Path(Sys->LoggerSettings()->LoggerActorId)
            , Comp(comp)
        {

        }

        TLogLn Log(ELnLev level_) const noexcept override
        {
            const NActors::NLog::TLevel level{ ui32(level_) };

            const auto force = level.IsUrgentAbortion();
            const auto pass = ShouldLog(level.ToPrio(), level.Raw & 0xff);

            return { force || pass ? this : nullptr, level_ };
        }

    private:
        bool ShouldLog(EPrio prio_, ui8 minor) const noexcept
        {
            /* Minor filtering isn't implemented in levle settings */

            const auto raw = ui32(prio_) + (minor ? 1 : 0);
            const auto prio = NActors::NLog::EPriority(raw);
            const auto *conf = Sys ? Sys->LoggerSettings() : nullptr;

            return conf && conf->Satisfies(prio, Comp, 0ull);
        }

        void LogLn(ELnLev level, const TString &line) const noexcept override
        {
            /* Usage of time provider will be possible on complete migration
                to this logger. Legacy macros based logging takes just Now().
             */

            auto stamp = false ? Time->Now() : TInstant::Now();

            auto *ev = new NLog::TEvLog(stamp, ui32(level), Comp, line);

            if (!Sys->Send(Path, ev))
                Y_ABORT("Cannot send NLog::TEvLog to logger actor");
        }

    private:
        ITimeProvider * const Time = nullptr;
        TSys * const Sys = nullptr;
        const TPath Path;
        const EComp Comp;
    };
}
}
