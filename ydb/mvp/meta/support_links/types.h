#pragma once

#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NMVP::NSupportLinks {

inline constexpr const char* SOURCE_GRAFANA_DASHBOARD = "grafana/dashboard";
inline constexpr const char* SOURCE_GRAFANA_DASHBOARD_SEARCH = "grafana/dashboard/search";
inline constexpr const char* SOURCE_META = "meta";
inline constexpr const char* INVALID_IDENTITY_PARAMS_MESSAGE =
    "Invalid identity parameters. Supported entities: cluster requires 'cluster'; database requires 'cluster' and 'database'.";

struct TResolvedLink {
    TString Title;
    TString Url;
};

struct TSupportError {
    TString Source;
    TMaybe<ui32> Status;
    TString Reason;
    TString Message;
};

} // namespace NMVP::NSupportLinks
