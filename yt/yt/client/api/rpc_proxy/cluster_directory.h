#pragma once

#include "public.h"

#include <yt/yt/client/hive/cluster_directory.h>

#include <yt/yt/client/api/connection.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TClusterDirectory
    : public NHiveClient::TClusterDirectoryBase<NApi::IConnection>
{
protected:
    IConnectionPtr CreateConnection(const std::string& name, const NYTree::INodePtr& connectionConfig) override;
};

DEFINE_REFCOUNTED_TYPE(TClusterDirectory);

////////////////////////////////////////////////////////////////////////////////

class TClientDirectory
    : public TRefCounted
{
public:
    TClientDirectory(
        TClusterDirectoryPtr clusterDirectory,
        NApi::TClientOptions clientOptions);

    //! Returns the client to the cluster with a given #clusterName.
    //! Returns |nullptr| if no connection is found in the underlying cluster
    //! directory.
    NApi::IClientPtr FindClient(const std::string& clusterName) const;
    //! Same as #FindClient but throws if no connection is found in the
    //! underlying cluster directory.
    NApi::IClientPtr GetClientOrThrow(const std::string& clusterName) const;

private:
    const TClusterDirectoryPtr ClusterDirectory_;
    const NApi::TClientOptions ClientOptions_;
};

DEFINE_REFCOUNTED_TYPE(TClientDirectory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
