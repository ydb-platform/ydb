#include "yql_yt_client.h"

namespace NYql::NFmr {

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
