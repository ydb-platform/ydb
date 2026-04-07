#ifndef CLUSTER_DIRECTORY_INL_H_
#error "Direct inclusion of this file is not allowed, include cluster_directory.h"
// For the sake of sane code completion.
#include "cluster_directory.h"
#endif
#undef CLUSTER_DIRECTORY_INL_H_

#include "private.h"
#include "config.h"

#include <yt/yt_proto/yt/client/hive/proto/cluster_directory.pb.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <yt/yt/core/ytree/ypath_client.h>
#include <yt/yt/core/ytree/convert.h>

namespace NYT::NHiveClient {

////////////////////////////////////////////////////////////////////////////////

template <std::derived_from<NApi::IConnection> TConnection>
TClusterDirectoryBase<TConnection>::TClusterDirectoryBase(TConnection::TConnectionOptions connectionOptions)
    : ConnectionOptions_(std::move(connectionOptions))
{ }

template <std::derived_from<NApi::IConnection> TConnection>
TIntrusivePtr<TConnection> TClusterDirectoryBase<TConnection>::FindConnection(NApi::TClusterTag cellTag) const
{
    auto guard = Guard(Lock_);
    auto it = CellTagToCluster_.find(cellTag);
    return it == CellTagToCluster_.end() ? nullptr : it->second.Connection;
}

template <std::derived_from<NApi::IConnection> TConnection>
TIntrusivePtr<TConnection> TClusterDirectoryBase<TConnection>::GetConnectionOrThrow(NApi::TClusterTag cellTag) const
{
    auto connection = FindConnection(cellTag);
    if (!connection) {
        THROW_ERROR_EXCEPTION("Cannot find cluster with tag %Qv", cellTag);
    }
    return connection;
}

template <std::derived_from<NApi::IConnection> TConnection>
TIntrusivePtr<TConnection> TClusterDirectoryBase<TConnection>::GetConnection(NApi::TClusterTag cellTag) const
{
    auto connection = FindConnection(cellTag);
    YT_VERIFY(connection);
    return connection;
}

template <std::derived_from<NApi::IConnection> TConnection>
TIntrusivePtr<TConnection> TClusterDirectoryBase<TConnection>::FindConnection(const std::string& clusterName) const
{
    auto guard = Guard(Lock_);
    auto it = NameToCluster_.find(clusterName);
    return it == NameToCluster_.end() ? nullptr : it->second.Connection;
}

template <std::derived_from<NApi::IConnection> TConnection>
TIntrusivePtr<TConnection> TClusterDirectoryBase<TConnection>::GetConnectionOrThrow(const std::string& clusterName) const
{
    auto connection = FindConnection(clusterName);
    if (!connection) {
        THROW_ERROR_EXCEPTION("Cannot find cluster with name %Qv", clusterName);
    }
    return connection;
}

template <std::derived_from<NApi::IConnection> TConnection>
TIntrusivePtr<TConnection> TClusterDirectoryBase<TConnection>::GetConnection(const std::string& clusterName) const
{
    auto connection = FindConnection(clusterName);
    YT_VERIFY(connection);
    return connection;
}

template <std::derived_from<NApi::IConnection> TConnection>
std::vector<std::string> TClusterDirectoryBase<TConnection>::GetClusterNames() const
{
    auto guard = Guard(Lock_);
    return GetKeys(NameToCluster_);
}

template <std::derived_from<NApi::IConnection> TConnection>
void TClusterDirectoryBase<TConnection>::RemoveCluster(const std::string& name)
{
    auto guard = Guard(Lock_);
    auto nameIt = NameToCluster_.find(name);
    if (nameIt == NameToCluster_.end()) {
        return;
    }
    const auto& cluster = nameIt->second;
    auto cellTags = GetCellTags(cluster);
    cluster.Connection->Terminate();
    if (auto tvmId = cluster.Connection->GetTvmId()) {
        auto tvmIdsIt = ClusterTvmIds_.find(*tvmId);
        YT_VERIFY(tvmIdsIt != ClusterTvmIds_.end());
        ClusterTvmIds_.erase(tvmIdsIt);
    }
    NameToCluster_.erase(nameIt);
    for (auto cellTag : cellTags) {
        YT_VERIFY(CellTagToCluster_.erase(cellTag) == 1);
    }
    auto Logger = HiveClientLogger;
    YT_LOG_DEBUG("Remote cluster unregistered (Name: %v, CellTags: %v)",
        name,
        cellTags);
}

template <std::derived_from<NApi::IConnection> TConnection>
void TClusterDirectoryBase<TConnection>::Clear()
{
    auto guard = Guard(Lock_);
    CellTagToCluster_.clear();
    NameToCluster_.clear();
    ClusterTvmIds_.clear();

    auto Logger = HiveClientLogger;
    YT_LOG_DEBUG("Cluster directory cleared");
}

template <std::derived_from<NApi::IConnection> TConnection>
void TClusterDirectoryBase<TConnection>::UpdateCluster(const std::string& name, const NYTree::INodePtr& connectionConfig)
{
    auto Logger = HiveClientLogger;

    bool fire = false;
    auto addNewCluster = [&] (const TCluster& cluster) {
        for (auto cellTag : GetCellTags(cluster)) {
            if (CellTagToCluster_.contains(cellTag)) {
                THROW_ERROR_EXCEPTION("Duplicate cell tag %Qv", cellTag)
                    << TErrorAttribute("first_cluster_name", CellTagToCluster_[cellTag].Name)
                    << TErrorAttribute("second_cluster_name", name);
            }
            CellTagToCluster_[cellTag] = cluster;
        }
        NameToCluster_[name] = cluster;
        if (auto tvmId = cluster.Connection->GetTvmId()) {
            ClusterTvmIds_.insert(*tvmId);
        }

        fire = true;
    };

    {
        auto guard = Guard(Lock_);
        auto nameIt = NameToCluster_.find(name);
        if (nameIt == NameToCluster_.end()) {
            auto cluster = CreateCluster(name, connectionConfig);
            addNewCluster(cluster);
            auto cellTags = GetCellTags(cluster);
            YT_LOG_DEBUG("Remote cluster registered (Name: %v, CellTags: %v)",
                name,
                cellTags);
        } else if (!AreNodesEqual(nameIt->second.ConnectionConfig, connectionConfig)) {
            auto cluster = CreateCluster(name, connectionConfig);
            auto oldTvmId = nameIt->second.Connection->GetTvmId();
            auto oldCellTags = GetCellTags(nameIt->second);
            nameIt->second.Connection->Terminate();
            for (auto cellTag : oldCellTags) {
                CellTagToCluster_.erase(cellTag);
            }
            NameToCluster_.erase(nameIt);
            if (oldTvmId) {
                auto tvmIdsIt = ClusterTvmIds_.find(*oldTvmId);
                YT_VERIFY(tvmIdsIt != ClusterTvmIds_.end());
                ClusterTvmIds_.erase(tvmIdsIt);
            }
            addNewCluster(cluster);
            auto cellTags = GetCellTags(cluster);
            YT_LOG_DEBUG("Remote cluster updated (Name: %v, CellTags: %v)",
                name,
                cellTags);
        }
    }

    if (fire) {
        OnClusterUpdated_.Fire(name, connectionConfig);
    }
}

template <std::derived_from<NApi::IConnection> TConnection>
void TClusterDirectoryBase<TConnection>::UpdateDirectory(const NProto::TClusterDirectory& protoDirectory)
{
    THashMap<std::string, NYTree::INodePtr> nameToConfig;
    for (const auto& item : protoDirectory.items()) {
        YT_VERIFY(nameToConfig.emplace(
            item.name(),
            NYTree::ConvertToNode(NYson::TYsonString(item.config()))).second);
    }

    UpdateDirectory(nameToConfig);
}

template <std::derived_from<NApi::IConnection> TConnection>
void TClusterDirectoryBase<TConnection>::UpdateDirectory(const TClusterDirectoryConfigPtr& config)
{
    UpdateDirectory(config->PerClusterConnectionConfig);
}

template <std::derived_from<NApi::IConnection> TConnection>
void TClusterDirectoryBase<TConnection>::UpdateDirectory(
    const THashMap<std::string, NYTree::INodePtr>& nameToConfig)
{
    for (const auto& name : GetClusterNames()) {
        if (nameToConfig.find(name) == nameToConfig.end()) {
            RemoveCluster(name);
        }
    }

    for (const auto& [name, config] : nameToConfig) {
        UpdateCluster(name, config);
    }
}

template <std::derived_from<NApi::IConnection> TConnection>
bool TClusterDirectoryBase<TConnection>::HasTvmId(NAuth::TTvmId tvmId) const
{
    auto guard = Guard(Lock_);
    return ClusterTvmIds_.find(tvmId) != ClusterTvmIds_.end();
}

template <std::derived_from<NApi::IConnection> TConnection>
TClusterDirectoryBase<TConnection>::TCluster TClusterDirectoryBase<TConnection>::CreateCluster(
    const std::string& name,
    const NYTree::INodePtr& config)
{
    TCluster cluster{
        .Name = name,
        .ConnectionConfig = config,
    };

    try {
        cluster.Connection = CreateConnection(name, config);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error creating connection to cluster %Qv",
            name)
            << ex;
    }
    return cluster;
}

template <std::derived_from<NApi::IConnection> TConnection>
NObjectClient::TCellTagList TClusterDirectoryBase<TConnection>::GetCellTags(
    const TClusterDirectoryBase<TConnection>::TCluster& cluster)
{
    return {cluster.Connection->GetClusterTag()};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveClient
