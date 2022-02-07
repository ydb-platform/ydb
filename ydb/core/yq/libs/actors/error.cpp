#include "proxy.h"

#include <util/system/hostname.h>

namespace NYq {

TString MakeInternalError(const TString& text) {
    return TStringBuilder() << "Internal error (" << text << ", host: " << HostName() << ")";
}

} // namespace NYq
