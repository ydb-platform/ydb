#pragma once

#include <util/generic/strbuf.h>
#include <util/generic/string.h>

namespace NKikimr::NHttpProxy {

struct THttpRequestContext;

// host is the HTTP Host / :authority value. headers is the raw header blob.
// tlsSecure is true when the connection to http_proxy itself is TLS.
// Prefers the first X-Forwarded-Host token when it is a host[:port] without '/', '#' or '?';
// otherwise uses host. Unknown X-Forwarded-Proto values fall back to tlsSecure.
TString MakeSqsRequestEndpoint(TStringBuf host, TStringBuf headers, bool tlsSecure);
TString MakeSqsRequestEndpoint(const THttpRequestContext& httpContext);

} // namespace NKikimr::NHttpProxy
