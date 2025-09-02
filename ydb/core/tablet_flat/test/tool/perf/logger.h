#pragma once

#include "iface_logger.h"
#include "format.h"

#include <util/datetime/base.h>

namespace NKikimr {

namespace NFmt {
    using namespace NKikiSched::NFmt;
}

namespace NTable {
namespace NPerf {

    using TLogLn    = NKikiSched::TLogLn;
    using ELnLev    = NKikiSched::ELnLev;

    class TLogger
            : public NKikiSched::ILogged
            , public NKikiSched::ILogger {

    public:
        TLogger(int level = 0, int relev = -1, IOutputStream *redir = nullptr)
            : Start(Now())
            , Level(level)
            , Relev(redir ? relev : -1)
            , Redir(redir)
        {

        }

        TLogLn operator ()(ELnLev lev) const
        {
            return Log(lev);
        }

        TLogLn Log(ELnLev lev) const override
        {
            return { ShouldLog(lev) ? this : nullptr, lev };
        }

    protected:
        bool ShouldLog(ELnLev level) const
        {
            return (int)level <= Max(Level, Relev);
        }

        void LogLn(ELnLev level, const TString &line) const override
        {
            auto left = (Now() - Start).SecondsFloat();

            auto *label = Lev2Label(level);

            auto out = Sprintf("%08.3f %2s| %s\n", left, label, line.c_str());

            if ((int)level <= Level) Cerr << out;
            if ((int)level <= Relev) *Redir << out;
        }

        static const char* Lev2Label(ELnLev level)
        {
            static const char *line[] = { "??", "**", "EE", "WW", "I0",
                "I1", "I2", "I3", "D0", "D1", "D2", "D3" };

            return int(level) < 12 ? line[int(level)] : "DG";
        }

    private:
        const TInstant Start;
        const int Level = 0;
        const int Relev = 0;
        IOutputStream *Redir = nullptr;
    };
}
}
}
