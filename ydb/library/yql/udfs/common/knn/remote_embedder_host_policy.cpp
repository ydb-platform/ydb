#include "remote_embedder.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/query_service_config.h>
#include <ydb/core/protos/config.pb.h>

#include <library/cpp/regex/pcre/regexp.h>

namespace NKnn {

void ValidateRemoteEmbedderHost(TStringBuf host) {
    if (!NKikimr::HasAppData()) {
        return;
    }
    const auto* appData = NKikimr::AppData();
    const auto config = NKikimr::LoadQueryServiceConfig(*appData);
    const auto& patterns = (config ? *config : appData->QueryServiceConfig).GetHostnamePatterns();
    if (patterns.empty()) {
        return;
    }
    if (host.StartsWith('[') && host.EndsWith(']')) {
        host = host.SubStr(1, host.size() - 2);
    }
    const TString hostname(host);
    for (const auto& pattern : patterns) {
        if (TRegExMatch(pattern).Match(hostname.c_str())) {
            return;
        }
    }
    ythrow yexception() << "RemoteEmbedder: It is not allowed to access hostname '" << hostname << "'";
}

} // namespace NKnn
