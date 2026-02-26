#include "yql_yt_client.h"

#include <yql/essentials/utils/log/log.h>

namespace NYql::NFmr {

namespace {
const TString TMP_YQL_PREFIX = "tmp/yql/";
}

void NormalizeRichPath(NYT::TRichYPath& richPath) {
    TString prefix = NYT::TConfig::Get()->Prefix;
    if (prefix.empty()) {
        prefix = "//";
    }
    richPath.Path(NYT::AddPathPrefix(richPath.Path_, prefix));
}

NYT::IClientPtr CreateClient(const TClusterConnection& clusterConnection) {
    NYT::TCreateClientOptions createOpts;
    auto token = clusterConnection.Token;
    if (token) {
        createOpts.Token(*token);
    }
    return NYT::CreateClient(clusterConnection.YtServerName, createOpts);
}

} // namespace NYql::NFmr
