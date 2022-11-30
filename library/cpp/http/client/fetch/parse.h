#pragma once

#include "fetch_result.h"
#include <library/cpp/uri/http_url.h>

namespace NHttpFetcher {
    void ParseUrl(const TStringBuf url, THttpURL::EKind& kind, TString& host, ui16& port);

    void ParseHttpResponse(TResult& result, IInputStream& stream, THttpURL::EKind kind,
                           TStringBuf host, ui16 port);

    void ParseHttpResponse(TResult& result, IInputStream& stream, const TString& url);

}
