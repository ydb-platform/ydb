#include "api_utils.h"

#include <ydb/core/base/validation.h>

#include <util/generic/string.h>
#include <util/generic/yexception.h>
#include <util/string/builder.h>

#include <library/cpp/string_utils/url/url.h>

namespace NYdb::NConsoleClient::NAi {

TString CreateApiUrl(const TString& baseUrl, const TString& uri) {
    Y_DEBUG_VERIFY(uri, "Internal error. Uri should not be empty for model API");

    TStringBuf sanitizedUrl;
    TStringBuf query;
    TStringBuf fragment;
    SeparateUrlFromQueryAndFragment(baseUrl, sanitizedUrl, query, fragment);

    if (query || fragment) {
        auto error = yexception() << "Invalid model API base url: '" << baseUrl << "'";
        if (query) {
            error << ". Query part should be empty, but got: '" << query << "'";
        }
        if (fragment) {
            error << ". Fragment part should be empty, but got: '" << fragment << "'";
        }
        throw error;
    }

    return TStringBuilder() << RemoveFinalSlash(sanitizedUrl) << uri;
}

} // namespace NYdb::NConsoleClient::NAi
