#pragma once

#include <util/stream/str.h>
#include <util/string/printf.h>

namespace NKikimr {
namespace NUtil {

    enum class ELnLev { /* named values for NActors::NLog::TLevel */

        /* value = (((major = (1 + prio)) << 8) | minor */

        Abort   = ((0 + 0) << 8) | 0,   /* Urgently abort execution */

        Emerg   = ((1 + 0) << 8) | 0,

        Crit    = ((1 + 2) << 8) | 1,
        Error   = ((1 + 3) << 8) | 0,
        Warn    = ((1 + 4) << 8) | 0,

        Info    = ((1 + 6) << 8) | 0,
        Inf1    = ((1 + 6) << 8) | 1,
        Inf2    = ((1 + 6) << 8) | 2,
        Inf3    = ((1 + 6) << 8) | 3,

        Debug   = ((1 + 7) << 8) | 0,
        Dbg01   = ((1 + 7) << 8) | 1,
        Dbg02   = ((1 + 7) << 8) | 2,
        Dbg03   = ((1 + 7) << 8) | 3,
    };

    class ILogged {
    public:
        virtual void LogLn(ELnLev, const TString&) const noexcept = 0;
    };

    class TLogLn : public TStringOutput {
    public:
        TLogLn(TLogLn &&line)
            : TStringOutput(line.Raw)
            , Logged(line.Logged)
            , Level(line.Level)
            , Raw(std::move(line.Raw))
        {
            if (Logged) Raw.reserve(256);
        }

        TLogLn(const ILogged *logged, ELnLev level)
            : TStringOutput(Raw)
            , Logged(logged)
            , Level(level)
        {

        }

        ~TLogLn()
        {
            if (Logged && Raw)
                Logged->LogLn(Level, Raw);
        }

        explicit operator bool() const noexcept
        {
            return Logged != nullptr;
        }

    private:
        const ILogged * Logged  = nullptr;
        const ELnLev Level = ELnLev::Debug;
        TString Raw;
    };

    class ILogger {
    public:
        virtual ~ILogger() = default;
        virtual TLogLn Log(ELnLev) const noexcept = 0;
    };

}
}
