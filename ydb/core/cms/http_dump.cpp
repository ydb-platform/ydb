#include "http_dump.h"

#include <util/string/builder.h>

namespace NKikimr::NCms {

bool IsHiddenHeader(const TString& headerName) {
    return stricmp(headerName.data(), "Authorization") == 0
        || stricmp(headerName.data(), "X-Ya-Service-Ticket") == 0
        || stricmp(headerName.data(), "Session_id") == 0;
}

TString DumpRequest(const NMonitoring::IMonHttpRequest& request) {
    TStringBuilder result;
    result << "{";

    result << " Method: " << request.GetMethod()
            << " Uri: " << request.GetUri();

    result << " Headers {";
    for (const auto& header : request.GetHeaders()) {
        if (IsHiddenHeader(header.Name())) {
            continue;
        }

        result << " " << header.ToString();
    }
    result << " }";

    result << " Body: " << request.GetPostContent().Head(1000);

    result << " }";
    return result;
}

} // namespace NKikimr::NCms
