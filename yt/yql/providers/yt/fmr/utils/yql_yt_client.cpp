#include "yql_yt_client.h"

namespace NYql::NFmr {

NYT::IClientPtr CreateClient(const TClusterConnection& clusterConnection) {
    NYT::TCreateClientOptions createOpts;
    auto token = clusterConnection.Token;
    if (token) {
        createOpts.Token(*token);
    }
    return NYT::CreateClient(clusterConnection.YtServerName, createOpts);
}

} // namespace NYql::NFmr
