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
    return std::tie(Name, Datacenter, Balancer, IsLocal, IsEnabled, IsAvailable, IsFnx, Weight) ==
        std::tie(rhs.Name, rhs.Datacenter, rhs.Balancer, rhs.IsLocal, rhs.IsEnabled, rhs.IsAvailable, rhs.IsFnx, rhs.Weight);
}

bool TClustersList::operator==(const TClustersList& rhs) const {
    return Clusters == rhs.Clusters && Balancers == rhs.Balancers && Accounts == rhs.Accounts && Version == rhs.Version;
}

TString TClustersList::DebugString() const {
    auto names = Clusters | std::views::transform([](const auto& cluster) { return cluster.Name; });
    return TStringBuilder() << "[" << JoinSeq(", ", names) << "]";
}

TString TClustersList::TCluster::DebugString() const {
    TStringBuilder builder;
    builder << "(" << Name << ", " << Datacenter << ", " << Balancer << ", ";
    builder << (IsEnabled ? "enabled" : "disabled")  << ", ";
    builder << (IsAvailable ? "available" : "unavailable") << ", ";
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

TString AccountNameFromDatabase(TStringBuf database) {
    while (database.ChopSuffix("/")) {
    }
    if (database.empty() || database == "/Root" || database == "Root") {
        return {};
    }
    const auto slash = database.rfind('/');
    const TStringBuf account = slash == TStringBuf::npos ? database : database.substr(slash + 1);
    if (account.empty()) {
        return {};
    }
    TString name(account);
    name.to_lower();
    if (name == "root") {
        return {};
    }
    return name;
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

TString SiblingTablePath(TStringBuf clusterTablePath, TStringBuf tableName) {
    static constexpr TStringBuf suffix = "/Cluster";
    if (clusterTablePath.EndsWith(suffix)) {
        return TString(clusterTablePath.substr(0, clusterTablePath.size() - suffix.size() + 1)) + tableName;
    }
    return TString(clusterTablePath) + tableName;
}

TString MakeListClustersQuery(TStringBuf clusterTablePath, TStringBuf versionTablePath) {
    // By-name parsing: an absent column (e.g. `available`, LOGBROKER-10701) reads as default
    return Sprintf(
        R"(
               --!syntax_v1
               SELECT C.*, V.version AS version FROM `%s` AS C
               CROSS JOIN
               (SELECT version FROM `%s` WHERE name == 'Cluster') AS V;
            )", TString(clusterTablePath).c_str(), TString(versionTablePath).c_str());
}

TString MakeListAccountsQuery(TStringBuf accountTablePath) {
    // Version is per account row. Pages are ordered by name so a truncated result can continue
    // from $from instead of being published as the full table.
    return Sprintf(
        R"(
               --!syntax_v1
               DECLARE $from AS Utf8;
               SELECT A.name, A.readClusters, A.writeClusters, A.version FROM `%s` AS A
               WHERE A.name > $from
               ORDER BY A.name;
            )", TString(accountTablePath).c_str());
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
    // "Table name conflict". FNX comes from the CM fnx column. Balancer.clusters does not set it.
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
        || issues.Contains("does not exist")
        || ((issues.Contains("Table") || issues.Contains("table"))
            && (issues.Contains("not exists") || issues.Contains("Not exists")));
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
