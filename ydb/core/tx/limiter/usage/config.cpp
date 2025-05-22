#include "config.h"
#include <util/string/builder.h>

namespace NKikimr::NLimiter {

TString TConfig::DebugString() const {
    TStringBuilder sb;
    sb << "Period=" << Period << ";Limit=" << Limit << ";Enabled=" << EnabledFlag << ";";
    return sb;
}

}
