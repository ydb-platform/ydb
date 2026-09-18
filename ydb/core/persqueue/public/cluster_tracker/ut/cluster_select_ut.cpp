#include <ydb/core/persqueue/public/cluster_tracker/cluster_select.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr::NPQ::NClusterTracker;

Y_UNIT_TEST_SUITE(TClusterSelectTest) {
    Y_UNIT_TEST(NormalizeDiscoveryHost) {
        UNIT_ASSERT_VALUES_EQUAL(NormalizeDiscoveryHost(""), "");
        UNIT_ASSERT_VALUES_EQUAL(NormalizeDiscoveryHost("logbroker-fnx.yandex.net"), "logbroker-fnx.yandex.net");
        UNIT_ASSERT_VALUES_EQUAL(NormalizeDiscoveryHost("LOGBROKER-FNX.YANDEX.NET:2135"), "logbroker-fnx.yandex.net");
        UNIT_ASSERT_VALUES_EQUAL(NormalizeDiscoveryHost("LogBroker-FNX.Yandex.Net"), "logbroker-fnx.yandex.net");
        UNIT_ASSERT_VALUES_EQUAL(NormalizeDiscoveryHost("[::1]:2135"), "::1");
        UNIT_ASSERT_VALUES_EQUAL(NormalizeDiscoveryHost("[::1]"), "::1");
        UNIT_ASSERT_VALUES_EQUAL(NormalizeDiscoveryHost("::1"), "::1");
        UNIT_ASSERT_VALUES_EQUAL(NormalizeDiscoveryHost("[fe80::1"), "[fe80::1");
        UNIT_ASSERT_VALUES_EQUAL(NormalizeDiscoveryHost("user@logbroker-fnx.yandex.net:2135"), "logbroker-fnx.yandex.net");
        UNIT_ASSERT_VALUES_EQUAL(NormalizeDiscoveryHost("127.0.0.1:2135"), "127.0.0.1");
    }

    Y_UNIT_TEST(ParseFnxClusterCsv) {
        UNIT_ASSERT(ParseFnxClusterCsv("").empty());
        UNIT_ASSERT_VALUES_EQUAL(ParseFnxClusterCsv("myt"), TVector<TString>{"myt"});
        UNIT_ASSERT_VALUES_EQUAL(ParseFnxClusterCsv("myt,  klg ,"), (TVector<TString>{"myt", "klg"}));
        UNIT_ASSERT_VALUES_EQUAL(ParseFnxClusterCsv("myt;vla"), (TVector<TString>{"myt", "vla"}));
        UNIT_ASSERT_VALUES_EQUAL(ParseFnxClusterCsv(",,;"), TVector<TString>{});
    }

    static TClustersList::TCluster MakeCluster(const TString& name, bool fnx) {
        TClustersList::TCluster cluster;
        cluster.Name = name;
        cluster.Datacenter = name;
        cluster.Balancer = name + ".logbroker.yandex.net";
        cluster.IsEnabled = true;
        cluster.IsFnx = fnx;
        cluster.Weight = 1000;
        return cluster;
    }

    Y_UNIT_TEST(GetClustersFiltersFnx) {
        TClustersList list;
        list.Clusters.push_back(MakeCluster("sas", false));
        list.Clusters.push_back(MakeCluster("vla", false));
        list.Clusters.push_back(MakeCluster("myt", true));
        list.Clusters.push_back(MakeCluster("klg-fnx", true));

        auto names = [](const auto& clusters) {
            TVector<TString> result;
            for (const auto& cluster : clusters) {
                result.push_back(cluster.Name);
            }
            return result;
        };

        list.BuildVisibleClusters();
        UNIT_ASSERT_VALUES_EQUAL(names(list.GetClusters("")), (TVector<TString>{"sas", "vla"}));
        UNIT_ASSERT_VALUES_EQUAL(names(list.GetClusters("sas.logbroker.yandex.net")), (TVector<TString>{"sas", "vla"}));
        UNIT_ASSERT_VALUES_EQUAL(names(list.GetClusters("unknown.example")), (TVector<TString>{"sas", "vla"}));

        list.Balancers["logbroker-fnx.yandex.net"] = TVector<TString>{"myt", "missing"};
        list.BuildVisibleClusters();
        UNIT_ASSERT_VALUES_EQUAL(names(list.GetClusters("")), (TVector<TString>{"sas", "vla"}));
        UNIT_ASSERT_VALUES_EQUAL(
            names(list.GetClusters("LOGBROKER-FNX.YANDEX.NET:2135")),
            (TVector<TString>{"sas", "vla", "myt"}));

        list.Balancers["logbroker-fnx.yandex.net"] = TVector<TString>{};
        list.BuildVisibleClusters();
        UNIT_ASSERT_VALUES_EQUAL(
            names(list.GetClusters("logbroker-fnx.yandex.net")),
            (TVector<TString>{"sas", "vla"}));

        TClustersList onlyFnx;
        onlyFnx.Clusters.push_back(MakeCluster("myt", true));
        onlyFnx.BuildVisibleClusters();
        UNIT_ASSERT(onlyFnx.GetClusters("logbroker.yandex.net").empty());
        onlyFnx.Balancers["logbroker-fnx.yandex.net"] = TVector<TString>{"myt"};
        onlyFnx.BuildVisibleClusters();
        UNIT_ASSERT_VALUES_EQUAL(
            names(onlyFnx.GetClusters("logbroker-fnx.yandex.net")),
            (TVector<TString>{"myt"}));
    }

    Y_UNIT_TEST(MarkFnxFromBalancers) {
        TClustersList list;
        list.Clusters.push_back(MakeCluster("sas", false));
        list.Clusters.push_back(MakeCluster("myt", false));
        list.Balancers["logbroker-fnx.yandex.net"] = TVector<TString>{"myt", "missing"};
        list.MarkFnxFromBalancers();
        UNIT_ASSERT(!list.Clusters[0].IsFnx);
        UNIT_ASSERT(list.Clusters[1].IsFnx);

        list.Balancers.clear();
        list.MarkFnxFromBalancers();
        UNIT_ASSERT(!list.Clusters[0].IsFnx);
        UNIT_ASSERT(!list.Clusters[1].IsFnx);
    }

    Y_UNIT_TEST(ClustersListEqualityAndDebugString) {
        TClustersList left;
        left.Clusters.push_back(MakeCluster("sas", false));
        left.Clusters.back().IsEnabled = true;
        left.Clusters.back().IsLocal = true;
        left.Version = 2;
        left.ClusterVersion = 2;
        left.Balancers["logbroker-fnx.yandex.net"] = TVector<TString>{"myt"};

        TClustersList right;
        right.Clusters.push_back(MakeCluster("sas", false));
        right.Clusters.back().IsEnabled = true;
        right.Clusters.back().IsLocal = true;
        right.Version = 2;
        right.ClusterVersion = 2;
        right.Balancers["logbroker-fnx.yandex.net"] = TVector<TString>{"myt"};

        UNIT_ASSERT(left == right);
        UNIT_ASSERT(left.Clusters.front() == right.Clusters.front());
        UNIT_ASSERT(left.DebugString().Contains("sas"));
        UNIT_ASSERT(left.Clusters.front().DebugString().Contains("ordinary"));
        UNIT_ASSERT(left.Clusters.front().DebugString().Contains("enabled"));
        UNIT_ASSERT(left.Clusters.front().DebugString().Contains("local"));

        right.Balancers["logbroker-fnx.yandex.net"] = TVector<TString>{"klg"};
        UNIT_ASSERT(!(left == right));

        TClustersList::TCluster fnx = MakeCluster("myt", true);
        fnx.IsEnabled = false;
        UNIT_ASSERT(fnx.DebugString().Contains("fnx"));
        UNIT_ASSERT(fnx.DebugString().Contains("disabled"));
        UNIT_ASSERT(fnx.DebugString().Contains("remote"));
    }

    Y_UNIT_TEST(BalancerTablePathFromClusterTable) {
        UNIT_ASSERT_VALUES_EQUAL(
            BalancerTablePathFromClusterTable("/Root/PQ/Config/V2/Cluster"),
            "/Root/PQ/Config/V2/Balancer");
        UNIT_ASSERT_VALUES_EQUAL(
            BalancerTablePathFromClusterTable("/Root/PQ/Config/V2/Other"),
            "/Root/PQ/Config/V2/OtherBalancer");
    }

    Y_UNIT_TEST(SchemaQueryBuilders) {
        const TString cluster = "/Root/PQ/Config/V2/Cluster";
        const TString balancer = "/Root/PQ/Config/V2/Balancer";
        const TString versions = "/Root/PQ/Config/V2/Versions";

        UNIT_ASSERT(MakeListClustersQuery(cluster, versions).Contains(cluster));
        UNIT_ASSERT(!MakeListClustersQuery(cluster, versions).Contains("C.fnx"));
        UNIT_ASSERT(MakeListBalancersQuery(balancer, versions).Contains(balancer));
        UNIT_ASSERT(MakeListBalancersQuery(balancer, versions).Contains("B.clusters"));
        UNIT_ASSERT(MakeCreateClusterQuery(cluster).Contains("CREATE TABLE IF NOT EXISTS"));
        UNIT_ASSERT(MakeCreateClusterQuery(cluster).Contains(cluster));
        UNIT_ASSERT(MakeCreateClusterQuery(cluster).Contains("kikimrHost Utf8"));
        UNIT_ASSERT(!MakeCreateClusterQuery(cluster).Contains("fnx"));
        UNIT_ASSERT(MakeAlterAddFnxQuery(cluster).Contains("ADD COLUMN fnx"));
        UNIT_ASSERT(MakeCreateBalancerQuery(balancer).Contains("CREATE TABLE IF NOT EXISTS"));
        UNIT_ASSERT(MakeCreateVersionsQuery(versions).Contains("CREATE TABLE IF NOT EXISTS"));
        UNIT_ASSERT(MakeCreateVersionsQuery(versions).Contains(versions));
        UNIT_ASSERT(MakeCreateVersionsQuery(versions).Contains("version Int64"));
        UNIT_ASSERT(MakeBackfillFnxQuery(cluster).Contains("fnx IS NULL"));
    }

    Y_UNIT_TEST(IssuesMatchers) {
        UNIT_ASSERT(IssuesLookLikeAlreadyExists("column already exists"));
        UNIT_ASSERT(IssuesLookLikeAlreadyExists("Already exists: fnx"));
        UNIT_ASSERT(IssuesLookLikeAlreadyExists("Column: \"fnx\" already exists"));
        UNIT_ASSERT(IssuesLookLikeAlreadyExists("duplicate column"));
        UNIT_ASSERT(IssuesLookLikeAlreadyExists("Duplicate table"));
        UNIT_ASSERT(!IssuesLookLikeAlreadyExists("unrelated"));

        UNIT_ASSERT(IssuesLookLikeMissingTable("Cannot find table `/Root/PQ/Config/V2/Cluster`"));
        UNIT_ASSERT(IssuesLookLikeMissingTable("cannot find table"));
        UNIT_ASSERT(IssuesLookLikeMissingTable("Path does not exist"));
        UNIT_ASSERT(IssuesLookLikeMissingTable("path does not exist"));
        UNIT_ASSERT(IssuesLookLikeMissingTable("Unable to find table"));
        UNIT_ASSERT(IssuesLookLikeMissingTable("Table `/x` does not exist"));
        UNIT_ASSERT(!IssuesLookLikeMissingTable("column already exists"));

        UNIT_ASSERT(IssuesLookLikeMissingColumn("Member not found: fnx"));
        UNIT_ASSERT(IssuesLookLikeMissingColumn("column fnx not found"));
        UNIT_ASSERT(IssuesLookLikeMissingColumn("Unknown column fnx"));
        UNIT_ASSERT(IssuesLookLikeMissingColumn("unknown column fnx"));
        UNIT_ASSERT(!IssuesLookLikeMissingColumn("column weight not found"));
        UNIT_ASSERT(!IssuesLookLikeMissingColumn("fnx is fine"));

        UNIT_ASSERT(IssuesLookLikeMissingVersionsTable("Cannot find table `/Root/PQ/Config/V2/Versions`"));
        UNIT_ASSERT(!IssuesLookLikeMissingVersionsTable("Cannot find table `/Root/PQ/Config/V2/Cluster`"));
        UNIT_ASSERT(IssuesLookLikeClusterSchemaGone("Cannot find table `/Root/PQ/Config/V2/Versions`"));
        UNIT_ASSERT(IssuesLookLikeClusterSchemaGone("Cannot find table `/Root/PQ/Config/V2/Cluster`"));
        UNIT_ASSERT(IssuesLookLikeClusterSchemaGone("Member not found: fnx"));
    }
}
