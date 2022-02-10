#include "settings.h"

#include <library/cpp/string_utils/url/url.h>

#include <util/system/env.h>

namespace NTvmAuth::NTvmTool {
    TClientSettings::TClientSettings(const TAlias& selfAias)
        : SelfAias_(selfAias)
        , Hostname_("localhost")
        , Port_(1)
        , SocketTimeout_(TDuration::Seconds(5))
        , ConnectTimeout_(TDuration::Seconds(30))
    {
        AuthToken_ = GetEnv("TVMTOOL_LOCAL_AUTHTOKEN");
        if (!AuthToken_) {
            AuthToken_ = GetEnv("QLOUD_TVM_TOKEN");
        }
        TStringBuf auth(AuthToken_);
        FixSpaces(auth);
        AuthToken_ = auth;

        const TString url = GetEnv("DEPLOY_TVM_TOOL_URL");
        if (url) {
            TStringBuf scheme, host;
            TryGetSchemeHostAndPort(url, scheme, host, Port_);
        }

        Y_ENSURE_EX(SelfAias_, TBrokenTvmClientSettings() << "Alias for your TVM client cannot be empty");
    }

    void TClientSettings::FixSpaces(TStringBuf& str) {
        while (str && isspace(str.back())) {
            str.Chop(1);
        }
    }
}
