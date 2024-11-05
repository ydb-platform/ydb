#include "config_clusters.h"

#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>

#include <util/generic/vector.h>
#include <util/generic/yexception.h>

namespace NYql {

TConfigClusters::TConfigClusters(const TYtGatewayConfig& config) {
    for (auto& cluster: config.GetClusterMapping()) {
        AddCluster(cluster, true);
    }
}

void TConfigClusters::AddCluster(const TYtClusterConfig& cluster, bool checkDuplicate) {
    if (!cluster.GetName()) {
        ythrow yexception() << "TYtGatewayConfig: Cluster name must be specified";
    }
    if (checkDuplicate && Clusters_.contains(cluster.GetName())) {
        ythrow yexception() << "TYtGatewayConfig: Duplicate cluster name: " << cluster.GetName();
    }

    TClusterInfo& info = Clusters_[cluster.GetName()];
    if (cluster.GetCluster()) {
        info.RealName = cluster.GetCluster();
        info.YtName = cluster.HasYTName() ? cluster.GetYTName() : cluster.GetName();
        TString ytName = info.YtName;
        ytName.to_lower();
        YtName2Name_.emplace(ytName, cluster.GetName());
    } else {
        ythrow yexception() << "TYtGatewayConfig: Cluster address must be specified";
    }

    if (cluster.HasYTToken()) {
        info.Token = cluster.GetYTToken();
    }

    if (cluster.GetDefault()) {
        if (DefaultClusterName_) {
            ythrow yexception() << "TYtGatewayConfig: More than one default cluster (current: "
                << cluster.GetName() << ", previous: " << DefaultClusterName_ << ")";
        }
        DefaultClusterName_ = cluster.GetName();
    }
}

const TString& TConfigClusters::GetServer(const TString& name) const {
    if (const TClusterInfo* info = Clusters_.FindPtr(name)) {
        return info->RealName;
    } else {
        ythrow yexception() << "Unknown cluster name: " << name;
    }
}

TString TConfigClusters::TryGetServer(const TString& name) const {
    if (const TClusterInfo* info = Clusters_.FindPtr(name)) {
        return info->RealName;
    } else {
        return {};
    }
}

const TString& TConfigClusters::GetYtName(const TString& name) const {
    if (const TClusterInfo* info = Clusters_.FindPtr(name)) {
        return info->YtName;
    } else {
        ythrow yexception() << "Unknown cluster name: " << name;
    }
}

TString TConfigClusters::GetNameByYtName(const TString& ytName) const {
    TString ytNameCopy = ytName;
    ytNameCopy.to_lower();

    if (const TString* name = YtName2Name_.FindPtr(ytNameCopy)) {
        return *name;
    }

    // no exception
    return ytName;
}

TMaybe<TString> TConfigClusters::GetAuth(const TString& name) const {
    if (const TClusterInfo* info = Clusters_.FindPtr(name)) {
        return info->Token;
    }
    return Nothing();
}

void TConfigClusters::GetAllClusters(TVector<TString>& names) const {
    names.clear();
    for (const auto& c: Clusters_) {
        names.push_back(c.first);
    }
}

const TString& TConfigClusters::GetDefaultClusterName() const {
    if (!DefaultClusterName_) {
        ythrow yexception() << "TYtGatewayConfig: No default cluster";
    }
    return DefaultClusterName_;
}

TString TConfigClusters::GetDefaultYtServer(const TYtGatewayConfig& config) {
    for (auto& cluster: config.GetClusterMapping()) {
        if (cluster.GetDefault()) {
            return cluster.GetCluster();
        }
    }
    return {};
}

} // NYql
