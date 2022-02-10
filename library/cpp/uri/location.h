#pragma once

#include <util/generic/string.h>

namespace NUri {
    /**
 * Resolve Location header according to https://tools.ietf.org/html/rfc7231#section-7.1.2
 *
 * @return  Resolved location's url or empty string in case of any error.
 */
    TString ResolveRedirectLocation(const TStringBuf& baseUrl, const TStringBuf& location);

}
