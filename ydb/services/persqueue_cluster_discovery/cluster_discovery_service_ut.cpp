#include <ydb/services/persqueue_cluster_discovery/counters.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>

#include <ydb/core/mind/address_classification/net_classifier.h>
#include <ydb/core/persqueue/cluster_tracker.h>
#include <ydb/core/testlib/test_pq_client.h>

#include <ydb/public/api/grpc/draft/ydb_persqueue_v1.grpc.pb.h>

#include <ydb/library/actors/http/http_proxy.h>

#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/testing/unittest/registar.h>
#include <google/protobuf/text_format.h>

#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>

#include <util/stream/file.h>
#include <util/system/tempfile.h>

namespace NKikimr::NPQCDTests {

using namespace Tests;
using namespace NKikimrClient;
using namespace Ydb::PersQueue::ClusterDiscovery;

struct TFancyGRpcWrapper {
    TFancyGRpcWrapper(const ui16 grpcPort) {
        Channel_ = grpc::CreateChannel("localhost:" + ToString(grpcPort), grpc::InsecureChannelCredentials());
        Stub_ = Ydb::PersQueue::V1::ClusterDiscoveryService::NewStub(Channel_);
    }

    auto PerformRpc(const DiscoverClustersRequest& request, DiscoverClustersResult& result) {
        grpc::ClientContext context;
        DiscoverClustersResponse response;

        const auto status = Stub_->DiscoverClusters(&context, request, &response);

        UNIT_ASSERT(status.ok());
        UNIT_ASSERT(response.operation().ready());

        const auto opStatus = response.operation().status();

        if (opStatus == Ydb::StatusIds::SUCCESS) {
            response.operation().result().UnpackTo(&result);
        }

        return opStatus;
    }

    void Wait() const {
        Sleep(TDuration::MilliSeconds(100));
    }

    void EnsureServiceUnavailability() {
        DiscoverClustersRequest request;
        DiscoverClustersResult result;

        UNIT_ASSERT_VALUES_EQUAL(PerformRpc(request, result), Ydb::StatusIds::UNAVAILABLE);
    }

    void WaitForExactClustersDataVersion(const i64 requiredVersion) {
        while (true) {
            DiscoverClustersRequest request;
            DiscoverClustersResult result;

            if (PerformRpc(request, result) == Ydb::StatusIds::SUCCESS && result.version() == requiredVersion) {
                break;
            }

            UNIT_ASSERT(result.version() < requiredVersion);

            Wait();
        }
    }

private:
    std::shared_ptr<grpc::Channel> Channel_;
    std::unique_ptr<Ydb::PersQueue::V1::ClusterDiscoveryService::Stub> Stub_;
};

static void VerifyClusterInfo(const ClusterInfo& clusterInfo, const TString& name, const TString& endpoint, bool isAvailable) {
    UNIT_ASSERT_STRINGS_EQUAL(clusterInfo.name(), name);
    UNIT_ASSERT_STRINGS_EQUAL(clusterInfo.endpoint(), endpoint);
    UNIT_ASSERT_VALUES_EQUAL(clusterInfo.available(), isAvailable);
}

static void VerifyEqualClusterInfo(const ClusterInfo& c1, const ClusterInfo& c2) {
    VerifyClusterInfo(c1, c2.name(), c2.endpoint(), c2.available());
}

static void CallPQCDAndAssert(TFancyGRpcWrapper& wrapper,
                              const DiscoverClustersRequest& request,
                              DiscoverClustersResult& result,
                              const i64 expectedVersion,
                              const size_t expectedWriteSessionsClusters,
                              const size_t expectedReadSessionsClusters)
{
    UNIT_ASSERT_VALUES_EQUAL(wrapper.PerformRpc(request, result), Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(result.version(), expectedVersion);

    UNIT_ASSERT_VALUES_EQUAL(result.write_sessions_clusters_size(), expectedWriteSessionsClusters);
    UNIT_ASSERT_VALUES_EQUAL(result.read_sessions_clusters_size(), expectedReadSessionsClusters);
}

static void CheckReadDiscoveryHandlers(TFancyGRpcWrapper& wrapper,
                                       const ClusterInfo& c1Info,
                                       const ClusterInfo& c2Info,
                                       i64 expectedVersion)
{
    {
        // EMPTY
        DiscoverClustersRequest request;
        DiscoverClustersResult result;

        CallPQCDAndAssert(wrapper, request, result, expectedVersion, 0, 0);
    }
    {
       // READ ALL ORIGINAL
       DiscoverClustersRequest request;
       auto* readSessionParams = request.add_read_sessions();
       readSessionParams->mutable_all_original();

       DiscoverClustersResult result;

       CallPQCDAndAssert(wrapper, request, result, expectedVersion, 0, 1);

       const auto& readSessionClusters = result.read_sessions_clusters(0);

       UNIT_ASSERT_VALUES_EQUAL(readSessionClusters.clusters_size(), 2);

       VerifyEqualClusterInfo(readSessionClusters.clusters(0), c1Info);
       VerifyEqualClusterInfo(readSessionClusters.clusters(1), c2Info);
   }
   {
       // READ MIRRORED
       DiscoverClustersRequest request;
       auto* readSessionParams = request.add_read_sessions();
       readSessionParams->set_mirror_to_cluster("dc2");
       readSessionParams->set_topic("megatopic");

       DiscoverClustersResult result;

       CallPQCDAndAssert(wrapper, request, result, expectedVersion, 0, 1);

       const auto& readSessionClusters = result.read_sessions_clusters(0);

       UNIT_ASSERT_VALUES_EQUAL(readSessionClusters.clusters_size(), 1);

       VerifyEqualClusterInfo(readSessionClusters.clusters(0), c2Info);
   }
   {
       // READ MIRRORED INEXISTING
       DiscoverClustersRequest request;
       auto* readSessionParams = request.add_read_sessions();
       readSessionParams->set_mirror_to_cluster("trololo");

       DiscoverClustersResult result;

       UNIT_ASSERT_VALUES_EQUAL(wrapper.PerformRpc(request, result), Ydb::StatusIds::BAD_REQUEST);
   }
}

static void CheckWriteDiscoveryHandlers(TFancyGRpcWrapper& wrapper,
                                        const ClusterInfo& c1Info,
                                        const ClusterInfo& c2Info,
                                        i64 expectedVersion)
{
   {
       // WRITE SIMPLE
       DiscoverClustersRequest request;
       {
           // causes the clusters to be arranged in the following order: #1, #2
           auto* writeSessionParams = request.add_write_sessions();
           writeSessionParams->set_topic("topic12");
           writeSessionParams->set_source_id("topic1.log");
           writeSessionParams->set_partition_group(42);
       }
       {
           // causes the clusters to be arranged in the following order: #2, #1
           auto* writeSessionParams = request.add_write_sessions();
           writeSessionParams->set_topic("topic1");
           writeSessionParams->set_source_id("topic1.log");
           writeSessionParams->set_partition_group(42);
       }
       {
           // preferred cluster option causes the clusters to be arranged in the following order: #2, #1
           auto* writeSessionParams = request.add_write_sessions();
           writeSessionParams->set_topic("topic12");
           writeSessionParams->set_source_id("topic1.log");
           writeSessionParams->set_partition_group(42);
           writeSessionParams->set_preferred_cluster_name("dc2");
       }

       DiscoverClustersResult result;

       CallPQCDAndAssert(wrapper, request, result, expectedVersion, 3, 0);

       {
           const auto& writeSessionClusters = result.write_sessions_clusters(0);
           UNIT_ASSERT_VALUES_EQUAL(writeSessionClusters.clusters_size(), 2);

           VerifyEqualClusterInfo(writeSessionClusters.clusters(0), c1Info);
           VerifyEqualClusterInfo(writeSessionClusters.clusters(1), c2Info);

           UNIT_ASSERT_VALUES_EQUAL(writeSessionClusters.primary_cluster_selection_reason(), WriteSessionClusters::CONSISTENT_DISTRIBUTION);
       }
       {
           const auto& writeSessionClusters = result.write_sessions_clusters(1);
           UNIT_ASSERT_VALUES_EQUAL(writeSessionClusters.clusters_size(), 2);

           VerifyEqualClusterInfo(writeSessionClusters.clusters(0), c2Info);
           VerifyEqualClusterInfo(writeSessionClusters.clusters(1), c1Info);

           UNIT_ASSERT_VALUES_EQUAL(writeSessionClusters.primary_cluster_selection_reason(), WriteSessionClusters::CONSISTENT_DISTRIBUTION);
       }
       {
           const auto& writeSessionClusters = result.write_sessions_clusters(2);
           UNIT_ASSERT_VALUES_EQUAL(writeSessionClusters.clusters_size(), 2);

           VerifyEqualClusterInfo(writeSessionClusters.clusters(0), c2Info);
           VerifyEqualClusterInfo(writeSessionClusters.clusters(1), c1Info);

           UNIT_ASSERT_VALUES_EQUAL(writeSessionClusters.primary_cluster_selection_reason(), WriteSessionClusters::CLIENT_PREFERENCE);
       }
   }
   {
       // WRITE INEXISTENT PREFERRED CLUSTER
       DiscoverClustersRequest request;
       {
           auto* writeSessionParams = request.add_write_sessions();
           writeSessionParams->set_topic("topic");
           writeSessionParams->set_source_id("topic1.log");
           writeSessionParams->set_preferred_cluster_name("dc777");
       }

       DiscoverClustersResult result;

       UNIT_ASSERT_VALUES_EQUAL(wrapper.PerformRpc(request, result), Ydb::StatusIds::BAD_REQUEST);
   }
}

class TPQCDServer {
public:
    TPQCDServer(bool enablePQCD = true)
        : PortManager_()
        , BusPort_(PortManager_.GetPort(2134))
        , GrpcPort_(PortManager_.GetPort(2135))
        , Settings_(NPersQueueTests::PQSettings(BusPort_, 1))
        , Sensors_(std::make_shared<NMonitoring::TMetricRegistry>())
    {
        Settings_.PQConfig.SetClustersUpdateTimeoutSec(1);

        Settings_.PQClusterDiscoveryConfig.SetEnabled(enablePQCD);
        Settings_.PQClusterDiscoveryConfig.SetTimedCountersUpdateIntervalSeconds(1);
    }

    ui16 BusPort() const {
        return BusPort_;
    }

    ui16 MonPort() const {
        return Server_->GetRuntime()->GetMonPort();
    }

    ui16 GrpcPort() const {
        return GrpcPort_;
    }

    TServerSettings& MutableSettings() {
        return Settings_;
    }

    void SetNetDataViaFile(const TString& netDataTsv) {
        UNIT_ASSERT(!Server_);

        NetDataFile = MakeHolder<TTempFileHandle>();
        NetDataFile->Write(netDataTsv.Data(), netDataTsv.Size());
        NetDataFile->FlushData();

        Settings_.NetClassifierConfig.SetNetDataFilePath(NetDataFile->Name());
    }

    void Run() {
        Server_ = MakeHolder<TServer>(Settings_);
        Server_->EnableGRpc(NYdbGrpc::TServerOptions().SetHost("localhost").SetPort(GrpcPort_));
    }

    NPersQueueTests::TFlatMsgBusPQClient& PQClient() {
        UNIT_ASSERT(Server_);

        if (!PQClient_) {
            PQClient_ = MakeHolder<NPersQueueTests::TFlatMsgBusPQClient>(Settings_, GrpcPort_);
        }

        return *PQClient_;
    }

    auto& ActorSystem() {
        UNIT_ASSERT(Server_);
        UNIT_ASSERT(Server_->GetRuntime());

        return *Server_->GetRuntime();
    }

    NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr HttpRequest(const TString& path) {
        UNIT_ASSERT(Server_);

        if (!HttpProxyId_) {
            IActor* proxy = NHttp::CreateHttpProxy(Sensors_);
            HttpProxyId_ = ActorSystem().Register(proxy);
        }

        TActorId clientId = ActorSystem().AllocateEdgeActor();
        NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateRequestGet("http://[::1]:" + ToString(MonPort()) + path);
        ActorSystem().Send(new NActors::IEventHandle(HttpProxyId_, clientId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest)), 0, true);

        return ActorSystem().GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingResponse>(clientId);
    }

    void Wait() {
        Sleep(TDuration::MilliSeconds(100));
    }

    auto ServiceCounters() const {
        UNIT_ASSERT(Server_);
        UNIT_ASSERT(Server_->GetGRpcServerRootCounters());

        return GetServiceCounters(Server_->GetGRpcServerRootCounters(), "persqueue")->GetSubgroup("subsystem", "cluster_discovery");
    }

    auto GetTimesResolvedByClassifier(const TString& datacenterLabel) const {
        return ServiceCounters()->GetSubgroup("resolved_dc", datacenterLabel)->GetCounter("TimesResolvedByClassifier", true)->GetAtomic();
    }

    bool CheckServiceHealth() {
        UNIT_ASSERT(Server_);

        while (true) {
            auto responsePtr = HttpRequest("/actors/pqcd/health");
            const auto& response = *responsePtr->Get();

            if (!response.Error && (response.Response->Status == "200") || (response.Response->Status == "418")) {
                UNIT_ASSERT(response.Response->Body);
                return response.Response->Status == "200";
            }

            Wait();
        }

        return false;
    }

    void WaitUntilHealthy() {
        while (true) {
            if (CheckServiceHealth()) {
                return;
            }

            Wait();
        }
    }

    void EnsureServiceUnavailability() {
        // check the response for 3 seconds
        for (size_t i = 0; i < 30; ++i) {
            UNIT_ASSERT(!CheckServiceHealth());
            Wait();
        }

        TFancyGRpcWrapper(GrpcPort_).EnsureServiceUnavailability();
    }

private:
    TPortManager PortManager_;
    ui16 BusPort_;
    ui16 GrpcPort_;
    TServerSettings Settings_;

    std::shared_ptr<NMonitoring::TMetricRegistry> Sensors_;
    THolder<TServer> Server_;
    THolder<NPersQueueTests::TFlatMsgBusPQClient> PQClient_;

    TActorId HttpProxyId_;

    THolder<TTempFileHandle> NetDataFile;
};

Y_UNIT_TEST_SUITE(TPQCDTest) {
    Y_UNIT_TEST(TestRelatedServicesAreRunning) {
        TPQCDServer server(false);

        const TString datacenterLabel = "fancy_datacenter";

        server.SetNetDataViaFile("2a0d:d6c0::/29\t" + datacenterLabel);

        server.Run();

        server.PQClient().InitRoot();
        server.PQClient().InitDCs();

        auto& actorSystem = server.ActorSystem();
        // check that NetClassifier is up and running
        {
            const TActorId sender = actorSystem.AllocateEdgeActor();

            actorSystem.Send(
                new IEventHandle(NNetClassifier::MakeNetClassifierID(), sender,
                    new NNetClassifier::TEvNetClassifier::TEvSubscribe()
            ));

            TAutoPtr<IEventHandle> handle;
            const auto event = actorSystem.GrabEdgeEvent<NNetClassifier::TEvNetClassifier::TEvClassifierUpdate>(handle);

            UNIT_ASSERT(event->NetDataUpdateTimestamp);
            UNIT_ASSERT(*event->NetDataUpdateTimestamp > TInstant::Zero());

            UNIT_ASSERT(event->Classifier);
            {
                const auto classificationResult = event->Classifier->ClassifyAddress("2a0d:d6c0:bbbb:bbbb:bbbb:bbbb:bbbb:bbbb");

                UNIT_ASSERT(classificationResult);
                UNIT_ASSERT_STRINGS_EQUAL(*classificationResult, datacenterLabel);
            }

            {
                const auto classificationResult = event->Classifier->ClassifyAddress("2b0d:d6c0:bbbb:bbbb:bbbb:bbbb:bbbb:bbbb");
                UNIT_ASSERT(!classificationResult);
            }
        }

        // check that ClusterTracker is up and running
        {
            const TActorId sender = actorSystem.AllocateEdgeActor();

            actorSystem.Send(
                new IEventHandle(NPQ::NClusterTracker::MakeClusterTrackerID(), sender,
                    new NPQ::NClusterTracker::TEvClusterTracker::TEvSubscribe()
            ));

            TAutoPtr<IEventHandle> handle;
            const auto event = actorSystem.GrabEdgeEvent<NPQ::NClusterTracker::TEvClusterTracker::TEvClustersUpdate>(handle);

            UNIT_ASSERT(event->ClustersList);
            UNIT_ASSERT(event->ClustersList->Clusters.size());

            UNIT_ASSERT(event->ClustersListUpdateTimestamp);
            UNIT_ASSERT(*event->ClustersListUpdateTimestamp > TInstant::Zero());

            UNIT_ASSERT_STRINGS_EQUAL(event->ClustersList->Clusters.front().Name, "dc1");
        }
    }

    Y_UNIT_TEST(TestDiscoverClusters) {
        TPQCDServer server;

        const TString datacenterLabel = "fancy_datacenter";

        server.SetNetDataViaFile("::1/128\t" + datacenterLabel);

        server.Run();

        server.PQClient().InitRoot();
        server.PQClient().InitDCs();

        server.WaitUntilHealthy();

        TFancyGRpcWrapper wrapper(server.GrpcPort());

        constexpr i64 dcTestIterations = 4; // it's signed to avoid warnings
        for (size_t i = 1; i <= dcTestIterations; ++i) {
            wrapper.WaitForExactClustersDataVersion(i);

            ClusterInfo c1Info;
            c1Info.set_name("dc1");
            c1Info.set_endpoint("localhost");
            c1Info.set_available(true);

            ClusterInfo c2Info;
            c2Info.set_name("dc2");
            c2Info.set_endpoint("dc2.logbroker.yandex.net");
            c2Info.set_available(true);

            // atm it's impossible to disable reading
            CheckReadDiscoveryHandlers(wrapper, c1Info, c2Info, i);

            c1Info.set_available(i % 2);

            // only write discovery is affected
            CheckWriteDiscoveryHandlers(wrapper, c1Info, c2Info, i);

            server.PQClient().UpdateDC("dc1", true, !(i % 2));
        }
        {
            // test handle viewer
            auto responsePtr = server.HttpRequest("/actors/pqcd");
            const auto& response = *responsePtr->Get();

            UNIT_ASSERT(!response.Error);
            UNIT_ASSERT_STRINGS_EQUAL(response.Response->Status, "200");
            UNIT_ASSERT(response.Response->Body.Contains("dc1"));
            UNIT_ASSERT(response.Response->Body.Contains("dc2"));
            UNIT_ASSERT(response.Response->Body.Contains("NetClassifier"));
            UNIT_ASSERT(response.Response->Body.Contains("Clusters list"));
            UNIT_ASSERT(response.Response->Body.Contains("GOOD"));
        }

        UNIT_ASSERT_VALUES_EQUAL(server.ActorSystem().GetNodeCount(), 1);

        // counters values might be somehow higher since WaitForExactClustersDataVersion calls number is unpredictable
        // read check performs 4 calls, write check performs 2 calls. Also for each iteration WaitForExactClustersDataVersion is called at least once
        // therefore the final minimal expected calls number is (4 + 2 + 1) * dcTestIterations
        NPQ::NClusterDiscovery::NCounters::TClusterDiscoveryCounters counters(server.ServiceCounters(), nullptr, nullptr);
        UNIT_ASSERT_GE(counters.TotalRequestsCount->GetAtomic(), (4 + 2 + 1) * dcTestIterations);
        UNIT_ASSERT_GE(counters.SuccessfulRequestsCount->GetAtomic(), (3 + 1 + 1) * dcTestIterations);

        UNIT_ASSERT_VALUES_EQUAL(counters.DroppedRequestsCount->GetAtomic(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counters.HealthProbe->GetAtomic(), 1);
        UNIT_ASSERT_VALUES_EQUAL(counters.FailedRequestsCount->GetAtomic(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counters.BadRequestsCount->GetAtomic(), 2 * dcTestIterations);

        UNIT_ASSERT_VALUES_EQUAL(counters.WriteDiscoveriesCount->GetAtomic(), 4 * dcTestIterations);
        UNIT_ASSERT_VALUES_EQUAL(counters.ReadDiscoveriesCount->GetAtomic(), 3 * dcTestIterations);

        auto getTimesPrioritized = [&server](const TString& clusterName) {
            return server.ServiceCounters()->GetSubgroup("prioritized_cluster", clusterName)->GetCounter("TimesPrioritizedForWrite", true)->GetAtomic();
        };

        UNIT_ASSERT_VALUES_EQUAL(getTimesPrioritized("dc1"), 1 * dcTestIterations);
        UNIT_ASSERT_VALUES_EQUAL(getTimesPrioritized("dc2"), 2 * dcTestIterations);

        // all requests are expected to come from a certain dc since the locality of the test
        UNIT_ASSERT_VALUES_EQUAL(server.GetTimesResolvedByClassifier(datacenterLabel), counters.TotalRequestsCount->GetAtomic());

        auto snapshot = counters.DiscoveryWorkingDurationMs->Snapshot();

        size_t total = 0;
        for (size_t i = 0; i < snapshot->Count(); ++i) {
            total += snapshot->Value(i);
        }
        UNIT_ASSERT_VALUES_EQUAL(total, counters.TotalRequestsCount->GetAtomic() - counters.DroppedRequestsCount->GetAtomic());

        UNIT_ASSERT_VALUES_EQUAL(counters.InfracloudRequestsCount->GetAtomic(), 0);

        UNIT_ASSERT_VALUES_EQUAL(counters.PrimaryClusterSelectionReasons[WriteSessionClusters::CLIENT_PREFERENCE]->GetAtomic(), 1 * dcTestIterations);
        UNIT_ASSERT_VALUES_EQUAL(counters.PrimaryClusterSelectionReasons[WriteSessionClusters::CLIENT_LOCATION]->GetAtomic(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counters.PrimaryClusterSelectionReasons[WriteSessionClusters::CONSISTENT_DISTRIBUTION]->GetAtomic(), 2 * dcTestIterations);
    }

    Y_UNIT_TEST(TestPrioritizeLocalDatacenter) {
        TPQCDServer server;

        const TString datacenterLabel = "dc2";

        server.SetNetDataViaFile("::1/128\t" + datacenterLabel);

        server.Run();

        server.PQClient().InitRoot();
        server.PQClient().InitDCs();

        server.WaitUntilHealthy();

        TFancyGRpcWrapper wrapper(server.GrpcPort());

        DiscoverClustersRequest request;
        {
           auto* writeSessionParams = request.add_write_sessions();
           writeSessionParams->set_topic("topic");
           writeSessionParams->set_source_id("topic1.log");
           writeSessionParams->set_partition_group(42);
        }

        DiscoverClustersResult result;

        CallPQCDAndAssert(wrapper, request, result, 1, 1, 0);

        const auto& writeSessionClusters = result.write_sessions_clusters(0);
        UNIT_ASSERT_VALUES_EQUAL(writeSessionClusters.clusters_size(), 2);

        UNIT_ASSERT_STRINGS_EQUAL(writeSessionClusters.clusters(0).name(), datacenterLabel);

        UNIT_ASSERT_VALUES_EQUAL(writeSessionClusters.primary_cluster_selection_reason(), WriteSessionClusters::CLIENT_LOCATION);

        NPQ::NClusterDiscovery::NCounters::TClusterDiscoveryCounters counters(server.ServiceCounters(), nullptr, nullptr);

        UNIT_ASSERT_VALUES_EQUAL(counters.PrimaryClusterSelectionReasons[WriteSessionClusters::CLIENT_LOCATION]->GetAtomic(), 1);
        UNIT_ASSERT_VALUES_EQUAL(counters.PrimaryClusterSelectionReasons[WriteSessionClusters::CONSISTENT_DISTRIBUTION]->GetAtomic(), 0);
    }

    Y_UNIT_TEST(TestUnavailableWithoutNetClassifier) {
        TPQCDServer server;

        server.Run();

        server.PQClient().InitRoot();
        server.PQClient().InitDCs();

        auto& actorSystem = server.ActorSystem();
        // check that NetClassifier instance is initialized with null
        {
            const TActorId sender = actorSystem.AllocateEdgeActor();

            actorSystem.Send(
                new IEventHandle(NNetClassifier::MakeNetClassifierID(), sender,
                    new NNetClassifier::TEvNetClassifier::TEvSubscribe()
            ));

            TAutoPtr<IEventHandle> handle;
            const auto event = actorSystem.GrabEdgeEvent<NNetClassifier::TEvNetClassifier::TEvClassifierUpdate>(sender);
            UNIT_ASSERT(!event->Get()->Classifier);
        }

        server.EnsureServiceUnavailability();
    }

    Y_UNIT_TEST(TestUnavailableWithoutClustersList) {
        TPQCDServer server;

        server.SetNetDataViaFile("::1/128\tdc1");

        server.Run();

        server.EnsureServiceUnavailability();
    }

    Y_UNIT_TEST(TestUnavailableWithoutBoth) {
        TPQCDServer server;

        server.Run();

        server.EnsureServiceUnavailability();
    }

    Y_UNIT_TEST(TestCloudClientsAreConsistentlyDistributed) {
        TPQCDServer server;

        auto& cloudNets = *server.MutableSettings().PQClusterDiscoveryConfig.MutableCloudNetData();
        auto& subnet = *cloudNets.AddSubnets();
        subnet.SetMask("::1/128");
        subnet.SetLabel("cloudnets");

        server.SetNetDataViaFile("::1/128\tdc2");

        server.Run();

        server.PQClient().InitRoot();
        server.PQClient().InitDCs();

        server.WaitUntilHealthy();

        TFancyGRpcWrapper wrapper(server.GrpcPort());

        DiscoverClustersRequest request;
        {
           auto* writeSessionParams = request.add_write_sessions();
           writeSessionParams->set_topic("topic12");
           writeSessionParams->set_source_id("topic1.log");
        }

        DiscoverClustersResult result;

        CallPQCDAndAssert(wrapper, request, result, 1, 1, 0);

        const auto& writeSessionClusters = result.write_sessions_clusters(0);
        UNIT_ASSERT_VALUES_EQUAL(writeSessionClusters.clusters_size(), 2);

        UNIT_ASSERT_STRINGS_EQUAL(writeSessionClusters.clusters(0).name(), "dc1");

        UNIT_ASSERT_VALUES_EQUAL(writeSessionClusters.primary_cluster_selection_reason(), WriteSessionClusters::CONSISTENT_DISTRIBUTION);

        NPQ::NClusterDiscovery::NCounters::TClusterDiscoveryCounters counters(server.ServiceCounters(), nullptr, nullptr);

        UNIT_ASSERT_VALUES_EQUAL(counters.InfracloudRequestsCount->GetAtomic(), 1);
        UNIT_ASSERT_VALUES_EQUAL(server.GetTimesResolvedByClassifier("dc2"), 1);

        UNIT_ASSERT_VALUES_EQUAL(counters.PrimaryClusterSelectionReasons[WriteSessionClusters::CLIENT_LOCATION]->GetAtomic(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counters.PrimaryClusterSelectionReasons[WriteSessionClusters::CONSISTENT_DISTRIBUTION]->GetAtomic(), 1);
    }
}

} // namespace NKikimr::NPQCDTests
