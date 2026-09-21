#include "cluster_select.h"

#include <util/string/builder.h>
#include <util/string/join.h>
#include <util/string/printf.h>
#include <util/string/split.h>
#include <util/string/strip.h>

#include <algorithm>
#include <ranges>
#include <tuple>

namespace NKikimr::NPQ::NClusterTracker {

bool TClustersList::TCluster::operator==(const TCluster& rhs) const {
    return std::tie(Name, Datacenter, Balancer, IsLocal, IsEnabled, IsFnx, Weight) ==
           std::tie(rhs.Name, rhs.Datacenter, rhs.Balancer, rhs.IsLocal, rhs.IsEnabled, rhs.IsFnx, rhs.Weight);
}

bool TClustersList::operator==(const TClustersList& rhs) const {
    return Clusters == rhs.Clusters && Balancers == rhs.Balancers && Version == rhs.Version;
}

TString TClustersList::DebugString() const {
    auto names = Clusters | std::views::transform([](const auto& cluster) { return cluster.Name; });
    return TStringBuilder() << "[" << JoinSeq(", ", names) << "]";
}

TString TClustersList::TCluster::DebugString() const {
    TStringBuilder builder;
    builder << "(" << Name << ", " << Datacenter << ", " << Balancer << ", ";
    builder << (IsEnabled ? "enabled" : "disabled")  << ", ";
    builder << (IsLocal ? "local" : "remote") << ", ";
    builder << (IsFnx ? "fnx" : "ordinary") << ", ";
    builder << Weight << ")";

    return TString(builder);
}

TString NormalizeDiscoveryHost(TStringBuf authority) {
    TString host(authority);
    host.to_lower();

    if (const auto at = host.find('@'); at != TString::npos) {
        host = host.substr(at + 1);
    }

    if (host.StartsWith('[')) {
        const auto end = host.find(']');
        if (end != TString::npos) {
            return host.substr(1, end - 1);
        }
    }

    const auto firstColon = host.find(':');
    if (firstColon != TString::npos && host.find(':', firstColon + 1) == TString::npos) {
        host.resize(firstColon);
    }

    return host;
}

TVector<TString> ParseFnxClusterCsv(TStringBuf csv) {
    TVector<TString> names;
    StringSplitter(csv).SplitBySet(",;").SkipEmpty().Collect(&names);
    for (auto& name : names) {
        name = StripString(name);
    }
    names.erase(
        std::remove_if(names.begin(), names.end(), [](const TString& name) { return name.empty(); }),
        names.end());
    return names;
}

const TVector<TClustersList::TCluster>& TClustersList::GetClusters(TStringBuf authority) const {
    const TString host = NormalizeDiscoveryHost(authority);
    if (const auto it = ClustersByBalancer.find(host); it != ClustersByBalancer.end()) {
        return it->second;
    }
    return DefaultVisibleClusters;
}

TString BalancerTablePathFromClusterTable(TStringBuf clusterTablePath) {
    static constexpr TStringBuf suffix = "/Cluster";
    if (clusterTablePath.EndsWith(suffix)) {
        return TString(clusterTablePath.substr(0, clusterTablePath.size() - suffix.size())) + "/Balancer";
    }
    return TString(clusterTablePath) + "Balancer";
}

TString MakeListClustersQuery(TStringBuf clusterTablePath, TStringBuf versionTablePath) {
    return Sprintf(
        R"(
               --!syntax_v1
               SELECT C.name, C.balancer, C.local, C.enabled, C.weight, V.version FROM `%s` AS C
               CROSS JOIN
               (SELECT version FROM `%s` WHERE name == 'Cluster') AS V;
            )", TString(clusterTablePath).c_str(), TString(versionTablePath).c_str());
}

TString MakeListBalancersQuery(TStringBuf balancerTablePath, TStringBuf versionTablePath) {
    return Sprintf(
        R"(
               --!syntax_v1
               SELECT B.name, B.clusters, V.version FROM `%s` AS B
               CROSS JOIN
               (SELECT MAX(version) AS version FROM `%s` WHERE name == 'Balancer') AS V;
            )", TString(balancerTablePath).c_str(), TString(versionTablePath).c_str());
}

TString MakeCreateClusterQuery(TStringBuf clusterTablePath) {
    // Match federation_recipe / CM Cluster schema, including kikimrHost.
    // Do not add extra columns (fnx): DestPrepare CREATE TABLE then fails with
    // "Table name conflict". FNX is taken from Balancer.clusters.
    return Sprintf(
        R"(
               --!syntax_v1
               CREATE TABLE IF NOT EXISTS `%s` (
                   name Utf8,
                   enabled Bool,
                   local Bool,
                   balancer Utf8,
                   weight Uint64,
                   advisable Bool,
                   kikimrHost Utf8,
                   kikimrMessageBusMaxInFlight Int32,
                   kikimrMessageBusMaxMessageSize Int64,
                   kikimrPort Int32,
                   zookeeperAddress Utf8,
                   PRIMARY KEY (name)
               );
            )", TString(clusterTablePath).c_str());
}

TString MakeAlterAddFnxQuery(TStringBuf clusterTablePath) {
    return Sprintf(
        R"(
               --!syntax_v1
               ALTER TABLE `%s` ADD COLUMN fnx Bool;
            )", TString(clusterTablePath).c_str());
}

TString MakeCreateBalancerQuery(TStringBuf balancerTablePath) {
    return Sprintf(
        R"(
               --!syntax_v1
               CREATE TABLE IF NOT EXISTS `%s` (
                   name Utf8,
                   clusters Utf8,
                   PRIMARY KEY (name)
               );
            )", TString(balancerTablePath).c_str());
}

TString MakeCreateVersionsQuery(TStringBuf versionTablePath) {
    return Sprintf(
        R"(
               --!syntax_v1
               CREATE TABLE IF NOT EXISTS `%s` (
                   name Utf8,
                   version Int64,
                   PRIMARY KEY (name)
               );
            )", TString(versionTablePath).c_str());
}

TString MakeBackfillFnxQuery(TStringBuf clusterTablePath) {
    return Sprintf(
        R"(
               --!syntax_v1
               UPDATE `%s` SET fnx = false WHERE fnx IS NULL;
            )", TString(clusterTablePath).c_str());
}

bool IssuesLookLikeAlreadyExists(TStringBuf issues) {
    return issues.Contains("already exists")
        || issues.Contains("Already exists")
        || issues.Contains("already exist")
        || issues.Contains("duplicate")
        || issues.Contains("Duplicate");
}

bool IssuesLookLikeMissingTable(TStringBuf issues) {
    return issues.Contains("Cannot find table")
        || issues.Contains("cannot find table")
        || issues.Contains("Path does not exist")
        || issues.Contains("path does not exist")
        || issues.Contains("Unable to find table")
        || issues.Contains("does not exist");
}

bool IssuesLookLikeMissingColumn(TStringBuf issues) {
    return issues.Contains("Member not found")
        || (issues.Contains("column") && issues.Contains("fnx") && (
            issues.Contains("not found") || issues.Contains("Unknown") || issues.Contains("unknown")));
}

bool IssuesLookLikeMissingVersionsTable(TStringBuf issues) {
    return IssuesLookLikeMissingTable(issues) && issues.Contains("Versions");
}

bool IssuesLookLikeClusterSchemaGone(TStringBuf issues) {
    return IssuesLookLikeMissingColumn(issues) || IssuesLookLikeMissingTable(issues);
}

} // namespace NKikimr::NPQ::NClusterTracker
