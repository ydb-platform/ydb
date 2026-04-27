#pragma once

#include <library/cpp/monlib/service/mon_service_http_request.h>

#include <util/generic/string.h>

namespace NKikimr::NCms {

bool IsHiddenHeader(const TString& headerName);

TString DumpRequest(const NMonitoring::IMonHttpRequest& request);

} // namespace NKikimr::NCms
