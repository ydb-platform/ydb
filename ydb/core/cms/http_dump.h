#pragma once

#include <util/generic/string.h>

namespace NMonitoring {
    struct IMonHttpRequest;
}

namespace NKikimr::NCms {

bool IsHiddenHeader(const TString& headerName);

TString DumpRequest(const NMonitoring::IMonHttpRequest& request);

} // namespace NKikimr::NCms
