#include "cluster_tracker.h"
#include "cluster_select.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/persqueue/public/pq_database.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash_set.h>

#include <string>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::PERSQUEUE_CLUSTER_TRACKER

namespace NKikimr::NPQ::NClusterTracker {

inline auto& Ctx() {
    return TActivationContext::AsActorContext();
}

class TClusterTracker: public TActorBootstrapped<TClusterTracker> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::PERSQUEUE_CLUSTER_TRACKER;
    }

    const auto& Cfg() const {
        return AppData(Ctx())->PQConfig;
    }

    void Bootstrap() {
        Become(&TThis::WaitingForSubscribers);
    }

private:
    enum class EQueryKind {
        None,
        MigrateCreateCluster,
        MigrateCreateBalancer,
        MigrateCreateVersions,
        ListClusters,
        ListBalancers,
        ListAccounts,
    };

    void AddSubscriber(const TActorId subscriberId) {
        YDB_LOG_DEBUG_CTX(Ctx(), "AddSubscriber",
            {"subscribersSize", Subscribers.size()});

        Subscribers.insert(subscriberId);
    }

    STATEFN(WaitingForSubscribers) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvClusterTracker::TEvSubscribe, HandleWhileWaiting);
            hFunc(TEvClusterTracker::TEvGetClustersList, HandleWhileWaiting);
        }
    }

    void HandleWhileWaiting(TEvClusterTracker::TEvSubscribe::TPtr& ev) {
        YDB_LOG_DEBUG_CTX(Ctx(), "AddSubscriber TEvSubscriber");

        Become(&TThis::Working);

        AddSubscriber(ev->Sender);
        Send(Ctx().SelfID, new TEvents::TEvWakeup);
    }

    void HandleWhileWaiting(TEvClusterTracker::TEvGetClustersList::TPtr& ev) {
        Become(&TThis::Working);

        GetClustersListRequests.push_back(ev->Sender);
        Send(Ctx().SelfID, new TEvents::TEvWakeup);
    }

    const TString& GetDatabase() {
        if (Database.empty()) {
            Database = GetDatabaseFromConfig(Cfg());
        }

        return Database;
    }

    TString GetBalancerTablePath() const {
        return SiblingTablePath(Cfg().GetClusterTablePath(), "Balancer");
    }

    TString GetAccountTablePath() const {
        return SiblingTablePath(Cfg().GetClusterTablePath(), "Account");
    }

    void StartListAccounts() {
        QueryInFlight = true;
        CurrentQuery = EQueryKind::ListAccounts;
        SendDml(MakeListAccountsQuery(GetAccountTablePath()), AccountPageFrom);
    }

    void FinishWithCachedAccounts() {
        PendingClustersList->Accounts = LastAccounts;
        FinishClustersList(std::move(PendingClustersList));
    }

    STATEFN(Working) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvClusterTracker::TEvSubscribe, HandleWhileWorking);
            hFunc(TEvClusterTracker::TEvGetClustersList, HandleWhileWorking);
            hFunc(TEvents::TEvWakeup, HandleWhileWorking);
            hFunc(NKqp::TEvKqp::TEvQueryResponse, HandleWhileWorking);
        }
    }

    void SendClustersList(const TActorId& subscriberId) {
        YDB_LOG_DEBUG_CTX(Ctx(), "SendClustersList");

        auto ev = MakeHolder<TEvClusterTracker::TEvClustersUpdate>();

        ev->ClustersList = ClustersList;
        ev->ClustersListUpdateTimestamp = ClustersListUpdateTimestamp;

        Send(subscriberId, ev.Release());
    }

    void HandleWhileWorking(TEvClusterTracker::TEvSubscribe::TPtr& ev) {
        YDB_LOG_DEBUG_CTX(Ctx(), "HandleWhileWorking TEvSubscribe",
            {"subscribersSize", Subscribers.size()},
            {"clustersList", (ClustersList == nullptr ? "null" : std::to_string(ClustersList->Clusters.size()))});

        AddSubscriber(ev->Sender);

        // List may be null due to reinit
        if (ClustersList) {
            SendClustersList(ev->Sender);
        }
    }

    void HandleWhileWorking(TEvClusterTracker::TEvGetClustersList::TPtr& ev) {
        YDB_LOG_DEBUG_CTX(Ctx(), "HandleWhileWorking TEvGetClustersList");

        if (ClustersList) {
            SendGetClustersListResponse(ev->Sender);
        } else {
            GetClustersListRequests.push_back(ev->Sender);
        }
    }

    void SendGetClustersListResponse(const TActorId& senderId, bool success = true) {
        YDB_LOG_DEBUG_CTX(Ctx(), "SendGetClustersListResponse",
            {"senderId", senderId});

        auto ev = MakeHolder<TEvClusterTracker::TEvGetClustersListResponse>();
        ev->Success = success;
        ev->ClustersList = ClustersList;

        Send(senderId, ev.Release());
    }

    void BroadcastClustersUpdate() {
        YDB_LOG_DEBUG_CTX(Ctx(), "BroadcastClustersUpdate",
            {"subscribersSize", Subscribers.size()});

        for (const auto& subscriberId : Subscribers) {
            YDB_LOG_DEBUG_CTX(Ctx(), "BroadcastClustersUpdate",
                {"subscriberId", subscriberId});
            SendClustersList(subscriberId);
        }
    }

    void ReplyAllGetClustersListRequests(bool success = true) {
        YDB_LOG_DEBUG_CTX(Ctx(), "ReplyAllGetClustersListRequests",
            {"clustersListRequestsSize", GetClustersListRequests.size()});

        for (const auto& requestId : GetClustersListRequests) {
            SendGetClustersListResponse(requestId, success);
        }
        GetClustersListRequests.clear();
    }

    void SendDdl(const TString& query) {
        auto req = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
        req->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        req->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_DDL);
        req->Record.MutableRequest()->SetKeepSession(false);
        req->Record.MutableRequest()->SetQuery(query);
        req->Record.MutableRequest()->SetDatabase(GetDatabase());
        Send(NKqp::MakeKqpProxyID(Ctx().SelfID.NodeId()), req.Release());
    }

    void SendDml(const TString& query, TMaybe<TString> fromAccount = {}) {
        auto req = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
        req->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        req->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_DML);
        req->Record.MutableRequest()->SetKeepSession(false);
        req->Record.MutableRequest()->SetQuery(query);
        req->Record.MutableRequest()->SetDatabase(GetDatabase());
        req->Record.MutableRequest()->SetUsePublicResponseDataFormat(true);
        req->Record.MutableRequest()->MutableTxControl()->mutable_begin_tx()->mutable_serializable_read_write();
        req->Record.MutableRequest()->MutableTxControl()->set_commit_tx(true);
        if (fromAccount.Defined()) {
            auto& param = (*req->Record.MutableRequest()->MutableYdbParameters())["$from"];
            param.mutable_type()->set_type_id(Ydb::Type::UTF8);
            param.mutable_value()->set_text_value(std::string(*fromAccount));
        }
        Send(NKqp::MakeKqpProxyID(Ctx().SelfID.NodeId()), req.Release());
    }

    void StartListClusters() {
        QueryInFlight = true;
        CurrentQuery = EQueryKind::ListClusters;
        SendDml(MakeListClustersQuery(Cfg().GetClusterTablePath(), Cfg().GetVersionTablePath()));
    }

    void BeginDdl(EQueryKind kind, const TString& query) {
        QueryInFlight = true;
        CurrentQuery = kind;
        SendDdl(query);
    }

    void StartMigrateOrList() {
        if (QueryInFlight) {
            return;
        }
        // List first. DDL runs only after ListClusters/ListBalancers see a missing table.
        StartListClusters();
    }

    void HandleWhileWorking(TEvents::TEvWakeup::TPtr&) {
        StartMigrateOrList();
    }

    void FailAndRetry() {
        QueryInFlight = false;
        CurrentQuery = EQueryKind::None;
        FullSchemaChain = false;
        AccountPageFrom.clear();
        PendingClustersList = nullptr;
        ClustersList = nullptr;
        Schedule(TDuration::Seconds(Cfg().GetClustersUpdateTimeoutOnErrorSec()), new TEvents::TEvWakeup);
        ReplyAllGetClustersListRequests(false);
    }

    bool SchemaChangeOk(bool success, TStringBuf issues) const {
        return success || IssuesLookLikeAlreadyExists(issues);
    }

    void HandleWhileWorking(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev) {
        YDB_LOG_DEBUG_CTX(Ctx(), "HandleWhileWorking TEvQueryResponse");

        const auto& record = ev->Get()->Record;
        const TString issues = record.ShortDebugString();
        const bool success = record.GetYdbStatus() == Ydb::StatusIds::SUCCESS;
        const EQueryKind kind = CurrentQuery;
        QueryInFlight = false;

        switch (kind) {
            case EQueryKind::MigrateCreateCluster:
                if (!SchemaChangeOk(success, issues)) {
                    YDB_LOG_ERROR_CTX(Ctx(), "Failed to CREATE TABLE Cluster",
                        {"record", record});
                    FailAndRetry();
                    return;
                }
                YDB_LOG_DEBUG_CTX(Ctx(), "Start schema migrate: CREATE TABLE Balancer");
                BeginDdl(EQueryKind::MigrateCreateBalancer, MakeCreateBalancerQuery(GetBalancerTablePath()));
                return;

            case EQueryKind::MigrateCreateBalancer:
                if (!SchemaChangeOk(success, issues)) {
                    YDB_LOG_ERROR_CTX(Ctx(), "Failed to CREATE TABLE Balancer",
                        {"record", record});
                    FailAndRetry();
                    return;
                }
                if (FullSchemaChain) {
                    BeginDdl(EQueryKind::MigrateCreateVersions, MakeCreateVersionsQuery(Cfg().GetVersionTablePath()));
                    return;
                }
                StartListClusters();
                return;

            case EQueryKind::MigrateCreateVersions:
                if (!SchemaChangeOk(success, issues)) {
                    YDB_LOG_ERROR_CTX(Ctx(), "Failed to CREATE TABLE Versions",
                        {"record", record});
                    FailAndRetry();
                    return;
                }
                FullSchemaChain = false;
                StartListClusters();
                return;

            case EQueryKind::ListClusters:
                if (success && record.GetResponse().YdbResultsSize() > 0) {
                    NYdb::TResultSetParser parser(record.GetResponse().GetYdbResults(0));
                    if (parser.RowsCount()) {
                        UpdateClustersList(parser);
                        QueryInFlight = true;
                        CurrentQuery = EQueryKind::ListBalancers;
                        SendDml(MakeListBalancersQuery(GetBalancerTablePath(), Cfg().GetVersionTablePath()));
                        return;
                    }
                }
                if (!success && IssuesLookLikeMissingVersionsTable(issues)) {
                    YDB_LOG_ERROR_CTX(Ctx(), "Failed to list clusters, CREATE TABLE Versions",
                        {"record", record});
                    ClustersList = nullptr;
                    ReplyAllGetClustersListRequests(false);
                    BeginDdl(EQueryKind::MigrateCreateVersions, MakeCreateVersionsQuery(Cfg().GetVersionTablePath()));
                    return;
                }
                if (!success && IssuesLookLikeClusterSchemaGone(issues)) {
                    YDB_LOG_ERROR_CTX(Ctx(), "Failed to list clusters, CREATE TABLE Cluster",
                        {"record", record});
                    ClustersList = nullptr;
                    ReplyAllGetClustersListRequests(false);
                    FullSchemaChain = true;
                    BeginDdl(EQueryKind::MigrateCreateCluster, MakeCreateClusterQuery(Cfg().GetClusterTablePath()));
                    return;
                }
                YDB_LOG_ERROR_CTX(Ctx(), "Failed to list",
                    {"clusters", record});
                FailAndRetry();
                return;

            case EQueryKind::ListBalancers:
                if (success) {
                    if (record.GetResponse().YdbResultsSize() > 0) {
                        NYdb::TResultSetParser parser(record.GetResponse().GetYdbResults(0));
                        UpdateBalancersList(parser);
                    } else if (PendingClustersList) {
                        RememberBalancers(*PendingClustersList);
                        StartListAccounts();
                    }
                    return;
                }
                if (IssuesLookLikeMissingVersionsTable(issues)) {
                    // ListBalancers joins Versions, so a dropped Versions table fails this query.
                    YDB_LOG_ERROR_CTX(Ctx(), "Failed to list balancers, CREATE TABLE Versions",
                        {"record", record});
                    ClustersList = nullptr;
                    PendingClustersList = nullptr;
                    ReplyAllGetClustersListRequests(false);
                    BeginDdl(EQueryKind::MigrateCreateVersions, MakeCreateVersionsQuery(Cfg().GetVersionTablePath()));
                    return;
                }
                {
                    const bool missing = IssuesLookLikeClusterSchemaGone(issues);
                    YDB_LOG_ERROR_CTX(Ctx(), "Failed to list balancers, publish clusters with last balancer cache",
                        {"record", record});
                    if (PendingClustersList) {
                        PendingClustersList->BalancerVersion = LastBalancerVersion;
                        PendingClustersList->Balancers = LastBalancers;
                        FinishWithCachedAccounts();
                    } else {
                        FailAndRetry();
                        return;
                    }
                    if (missing) {
                        BeginDdl(EQueryKind::MigrateCreateBalancer, MakeCreateBalancerQuery(GetBalancerTablePath()));
                    }
                }
                return;

            case EQueryKind::ListAccounts:
                if (!PendingClustersList) {
                    FailAndRetry();
                    return;
                }
                if (success && record.GetResponse().YdbResultsSize() > 0) {
                    UpdateAccountsList(record.GetResponse().GetYdbResults(0));
                    return;
                }
                if (record.GetYdbStatus() == Ydb::StatusIds::SCHEME_ERROR) {
                    YDB_LOG_DEBUG_CTX(Ctx(), "Account list scheme error, keep the previous account lists",
                        {"record", record});
                } else {
                    YDB_LOG_ERROR_CTX(Ctx(), "Failed to list accounts, publish clusters with last account cache",
                        {"record", record});
                }
                FinishWithCachedAccounts();
                return;

            case EQueryKind::None:
                YDB_LOG_ERROR_CTX(Ctx(), "Unexpected query response",
                    {"record", record});
                return;
        }
    }

    void FinishClustersList(TIntrusivePtr<TClustersList> clustersList) {
        AFL_ENSURE(clustersList);
        AFL_ENSURE(clustersList->Clusters.size());
        AccountPageFrom.clear();
        clustersList->Version = clustersList->ClusterVersion + clustersList->BalancerVersion + clustersList->SumAccountVersions();
        clustersList->BuildVisibleClusters();
        ClustersList = std::move(clustersList);
        ClustersListUpdateTimestamp = Ctx().Now();
        AFL_ENSURE(ClustersListUpdateTimestamp && *ClustersListUpdateTimestamp);

        BroadcastClustersUpdate();
        ReplyAllGetClustersListRequests();
        CurrentQuery = EQueryKind::None;
        Schedule(TDuration::Seconds(Cfg().GetClustersUpdateTimeoutSec()), new TEvents::TEvWakeup);
    }

    template<typename TProtoRecord>
    void UpdateClustersList(TProtoRecord& parser) {
        auto clustersList = MakeIntrusive<TClustersList>();
        clustersList->Clusters.resize(parser.RowsCount());

        bool firstRow = parser.TryNextRow();
        YQL_ENSURE(firstRow);
        clustersList->ClusterVersion = parser.ColumnParser("version").GetOptionalInt64().value_or(0);
        size_t i = 0;

        do {
            auto& cluster = clustersList->Clusters[i];

            cluster.Name = TString(parser.ColumnParser("name").GetOptionalUtf8().value_or(""));
            cluster.Datacenter = cluster.Name;
            cluster.Balancer = TString(parser.ColumnParser("balancer").GetOptionalUtf8().value_or(""));

            cluster.IsLocal = parser.ColumnParser("local").GetOptionalBool().value_or(false);
            cluster.IsEnabled = parser.ColumnParser("enabled").GetOptionalBool().value_or(false);
            cluster.Weight = parser.ColumnParser("weight").GetOptionalUint64().value_or(1000);
            // Absent on a Cluster table without the CM fnx column: the cluster is not FNX.
            if (parser.ColumnIndex("fnx") >= 0) {
                cluster.IsFnx = parser.ColumnParser("fnx").GetOptionalBool().value_or(false);
            }
            // Absent on not-yet-migrated schema; NULL -> available (LOGBROKER-10701)
            if (parser.ColumnIndex("available") >= 0) {
                cluster.IsAvailable = parser.ColumnParser("available").GetOptionalBool().value_or(true);
            }

            ++i;
        } while (parser.TryNextRow());

        clustersList->LocalCluster = FindIfPtr(clustersList->Clusters, [](const auto& cluster) { return cluster.IsLocal; });
        PendingClustersList = std::move(clustersList);
    }

    template<typename TProtoRecord>
    void UpdateBalancersList(TProtoRecord& parser) {
        auto clustersList = PendingClustersList;
        PendingClustersList = nullptr;
        if (!clustersList) {
            FailAndRetry();
            return;
        }

        while (parser.TryNextRow()) {
            TString name = TString(parser.ColumnParser(0).GetOptionalUtf8().value_or(""));
            const TString csv = TString(parser.ColumnParser(1).GetOptionalUtf8().value_or(""));
            if (!name.empty()) {
                clustersList->Balancers[name] = ParseFnxClusterCsv(csv);
            }
            clustersList->BalancerVersion = std::max<i64>(
                clustersList->BalancerVersion,
                parser.ColumnParser(2).GetOptionalInt64().value_or(0));
        }

        RememberBalancers(*clustersList);
        PendingClustersList = std::move(clustersList);
        StartListAccounts();
    }

    void UpdateAccountsList(const Ydb::ResultSet& resultSet) {
        auto parseNames = [](TStringBuf csv) {
            const auto names = ParseFnxClusterCsv(csv);
            return absl::flat_hash_set<TString>(names.begin(), names.end());
        };

        NYdb::TResultSetParser parser(resultSet);
        TString pageLastName;
        while (parser.TryNextRow()) {
            TString name(parser.ColumnParser(0).GetOptionalUtf8().value_or(""));
            if (name.empty()) {
                continue;
            }
            pageLastName = name;
            name.to_lower();
            auto& account = PendingClustersList->Accounts[name];
            account.Read = parseNames(parser.ColumnParser(1).GetOptionalUtf8().value_or(""));
            account.Write = parseNames(parser.ColumnParser(2).GetOptionalUtf8().value_or(""));
            account.Version = parser.ColumnParser(3).GetOptionalInt64().value_or(0);
        }

        if (resultSet.truncated()) {
            // A truncated page is not the account table. Keep the previous complete cache.
            if (pageLastName <= AccountPageFrom) {
                YDB_LOG_ERROR_CTX(Ctx(), "Account list page did not advance, keep the previous account cache",
                    {"from", AccountPageFrom},
                    {"pageLastName", pageLastName});
                FinishWithCachedAccounts();
                return;
            }
            AccountPageFrom = pageLastName;
            StartListAccounts();
            return;
        }

        LastAccounts = PendingClustersList->Accounts;
        FinishClustersList(std::move(PendingClustersList));
    }

    void RememberBalancers(const TClustersList& clustersList) {
        LastBalancerVersion = clustersList.BalancerVersion;
        LastBalancers = clustersList.Balancers;
    }

private:
    TClustersList::TConstPtr ClustersList = nullptr;
    TIntrusivePtr<TClustersList> PendingClustersList;

    TMaybe<TInstant> ClustersListUpdateTimestamp;
    THashSet<TActorId> Subscribers;
    TVector<TActorId> GetClustersListRequests;
    TString Database;

    bool FullSchemaChain = false;
    bool QueryInFlight = false;
    EQueryKind CurrentQuery = EQueryKind::None;
    i64 LastBalancerVersion = 0;
    absl::flat_hash_map<TString, TVector<TString>> LastBalancers;
    absl::flat_hash_map<TString, TClustersList::TAccountClusters> LastAccounts;
    // Exclusive lower bound of the account page being read. Empty starts from the first name.
    TString AccountPageFrom;
};

NActors::IActor* CreateClusterTracker() {
    return new TClusterTracker();
}

} // namespace NKikimr::NPQ::NClusterTracker
