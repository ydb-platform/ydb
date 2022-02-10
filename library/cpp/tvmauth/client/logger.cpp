#include "logger.h"

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NTvmAuth {
    void TCerrLogger::Log(int lvl, const TString& msg) {
        if (lvl > Level_)
            return;
        Cerr << TInstant::Now().ToStringLocal() << " lvl=" << lvl << " msg: " << msg << "\n";
    }
}
