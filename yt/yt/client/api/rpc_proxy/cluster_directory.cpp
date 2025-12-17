#include "cluster_directory.h"
#include "config.h"
#include "connection.h"

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

IConnectionPtr TClusterDirectory::CreateConnection(const std::string& name, const NYTree::INodePtr& connectionConfig)
{
    auto config = ConvertTo<TConnectionConfigPtr>(connectionConfig);
    if (!config->ClusterName) {
        config->ClusterName = name;
    }
    return NRpcProxy::CreateConnection(config, ConnectionOptions_);
}

////////////////////////////////////////////////////////////////////////////////

TClientDirectory::TClientDirectory(
    TClusterDirectoryPtr clusterDirectory,
    TClientOptions clientOptions)
    : ClusterDirectory_(std::move(clusterDirectory))
    , ClientOptions_(std::move(clientOptions))
{ }

NApi::IClientPtr TClientDirectory::FindClient(const std::string& clusterName) const
{
    const auto& connection = ClusterDirectory_->FindConnection(clusterName);
    return connection->CreateClient(ClientOptions_);
}

NApi::IClientPtr TClientDirectory::GetClientOrThrow(const std::string& clusterName) const
{
    const auto& connection = ClusterDirectory_->GetConnectionOrThrow(clusterName);
    return connection->CreateClient(ClientOptions_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
