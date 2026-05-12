#pragma once

#include "public.h"

#include <yt/yt/library/tvm/service/public.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/ytree/public.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NHiveClient {

////////////////////////////////////////////////////////////////////////////////

struct TClusterDirectoryUpdateResult
{
    //! \note Errors already contain cluster name in description, so you can use them directly instead of wrapping in an error with the cluster name.
    THashMap<std::string, TError> ClusterToErrorMapping;

    TError GetCumulativeError() const;
};

/////////////////////////////////////////////////////////////////////////////////

//! Maintains a map for a bunch of cluster connections.
/*!
 *  Thread affinity: any
 */
template <std::derived_from<NApi::IConnection> TConnection>
class TClusterDirectoryBase
    : public virtual TRefCounted
{
    using TConnectionPtr = TIntrusivePtr<TConnection>;

public:
    explicit TClusterDirectoryBase(TConnection::TConnectionOptions connectionOptions);

    //! Returns the connection to cluster with a given #cellTag.
    //! Only applies to native connections. Returns |nullptr| if no connection is found.
    TConnectionPtr FindConnection(NApi::TClusterTag cellTag) const;
    //! Same as #FindConnection but throws if no connection is found.
    TConnectionPtr GetConnectionOrThrow(NApi::TClusterTag cellTag) const;
    //! Same as #FindConnection but crashes if no connection is found.
    TConnectionPtr GetConnection(NApi::TClusterTag cellTag) const;

    //! Returns the connection to cluster with a given #clusterName.
    //! Returns |nullptr| if no connection is found.
    TConnectionPtr FindConnection(const std::string& clusterName) const;
    //! Same as #FindConnection but throws if no connection is found.
    TConnectionPtr GetConnectionOrThrow(const std::string& clusterName) const;
    //! Same as #FindConnection but crashes if no connection is found.
    TConnectionPtr GetConnection(const std::string& clusterName) const;

    //! Returns the list of names of all registered clusters.
    std::vector<std::string> GetClusterNames() const;

    //! Removes the cluster of a given #name.
    //! Does nothing if no such cluster is registered.
    void RemoveCluster(const std::string& name);

    //! Drops all directory entries.
    void Clear();

    //! Tries to update configuration of all clusters given in #protoDirectory independently, i.e. failure to update one cluster does not affect the update of other clusters.
    //! Removes all clusters that are currently known but are missing in #protoDirectory.
    //! Returns the result of the update for each cluster.
    TClusterDirectoryUpdateResult TryUpdateDirectory(const NProto::TClusterDirectory& protoDirectory);

    //! Tries to update configuration of all clusters given in #config independently, i.e. failure to update one cluster does not affect the update of other clusters.
    //! Removes all clusters that are currently known but are missing in #config.
    //! Returns the result of the update for each cluster.
    TClusterDirectoryUpdateResult TryUpdateDirectory(const TClusterDirectoryConfigPtr& config);

    //! Tries to update the configuration of a cluster with a given #name,
    //! recreates the connection if configuration changes.
    //! Returns an error if the update fails.
    TError TryUpdateCluster(const std::string& name, const NYTree::INodePtr& connectionConfig);

    //! Updates configuration of all clusters given in #protoDirectory independently, i.e. failure to update one cluster does not affect the update of other clusters.
    //! Removes all clusters that are currently known but are missing in #protoDirectory.
    //! Throws before return if any cluster update fails.
    void UpdateDirectory(const NProto::TClusterDirectory& protoDirectory);

    //! Updates configuration of all clusters given in #config independently, i.e. failure to update one cluster does not affect the update of other clusters.
    //! Removes all clusters that are currently known but are missing in #config.
    //! Throws before return if any cluster update fails.
    void UpdateDirectory(const TClusterDirectoryConfigPtr& config);

    //! Returns true if there is a cluster with corresponding TVM id in the directory.
    bool HasTvmId(NAuth::TTvmId tvmId) const;

    DEFINE_SIGNAL(void(const std::string&, NYTree::INodePtr), OnClusterUpdated);

protected:
    struct TCluster
    {
        std::string Name;
        NYTree::INodePtr ConnectionConfig;
        TConnectionPtr Connection;
    };

    const TConnection::TConnectionOptions ConnectionOptions_;

    virtual TConnectionPtr CreateConnection(const std::string& name, const NYTree::INodePtr& connectionConfig) = 0;
    virtual NObjectClient::TCellTagList GetCellTags(const TCluster& cluster);

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    THashMap<NApi::TClusterTag, TCluster> CellTagToCluster_;
    THashMap<std::string, TCluster> NameToCluster_;
    THashMultiSet<NAuth::TTvmId> ClusterTvmIds_;

    TClusterDirectoryUpdateResult TryUpdateDirectory(const THashMap<std::string, NYTree::INodePtr>& nameToConfig);
    TCluster CreateCluster(const std::string& name, const NYTree::INodePtr& connectionConfig);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveClient

#define CLUSTER_DIRECTORY_INL_H_
#include "cluster_directory-inl.h"
#undef CLUSTER_DIRECTORY_INL_H_
