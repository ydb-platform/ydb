#include "proxy.h"

#include <util/system/hostname.h>

namespace NFq {

TString MakeInternalError(const TString& text) {
    return TStringBuilder() << "Internal error (" << text << ", host: " << HostName() << ")";
}

} // namespace NFq
