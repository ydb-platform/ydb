#pragma once

#include <util/stream/str.h>
#include <util/string/printf.h>

namespace NKikiSched {

    enum class ELnLev {
        None    = 0,

        CRIT    = 1,
        ERROR   = 2,
        WARN    = 3,

        INFO    = 4,
        INF1    = 5,
        INF2    = 6,
        INF3    = 7,

        DEBUG   = 8,
        DBG01   = 9,
    };

    class ILogged {
    public:
        virtual void LogLn(ELnLev, const TString&) const = 0;
    };

    class TLogLn : public TStringStream {
    public:
        TLogLn(const ILogged *logged, ELnLev level)
            : Logged(logged)
            , Level(level)
        {

        }

        ~TLogLn()
        {
            if (!Logged) {
                /* Just a dummy line    */

            } else if (this->empty()) {
                /* Nothing was logged   */

            } else {
                Logged->LogLn(Level, Str());
            }
        }

        explicit operator bool() const noexcept
        {
            return Logged != nullptr;
        }

    private:
        const ILogged * Logged  = nullptr;
        const ELnLev    Level   = ELnLev::None;
    };

    class ILogger {
    public:
        virtual ~ILogger()
        {

        }

        virtual TLogLn Log(ELnLev) const = 0;
    };
}
