#pragma once

#include <library/cpp/http/simple/http_client.h>

namespace NTvmAuth::NUtils {
    struct TFetchResult {
        TKeepAliveHttpClient::THttpCode Code;
        THttpHeaders Headers;
        TStringBuf Path;
        TString Response;
        TString RetrySettings;
    };
}
