#pragma once

#include "defs.h"
#include "util_fmt_desc.h"

namespace NKikimr {

    namespace NFmt {

        struct TStamp {
            TStamp(ui64 stamp) : Stamp(stamp) { }

            TOut& Do(TOut &out) const noexcept
            {
                return out << Stamp.Gen() << ":" << Stamp.Step();
            }

        private:
            const NTable::TTxStamp Stamp;
        };

        inline TOut& operator<<(TOut &out, const NFmt::TStamp &print)
        {
            return print.Do(out);
        }
    }
}
