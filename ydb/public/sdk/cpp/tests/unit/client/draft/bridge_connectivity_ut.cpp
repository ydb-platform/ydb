#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_bridge.h>

#include <ydb/public/api/grpc/draft/ydb_bridge_v1.grpc.pb.h>
#include <ydb/public/api/protos/draft/ydb_bridge.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>

#include <atomic>

namespace NYdb::inline Dev::NBridge {
namespace {

class TBridgeService final : public Ydb::Bridge::V1::BridgeService::Service {
public:
    bool IncludeReport = true;
    Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::SUCCESS;
    std::atomic<bool> RequestedConnectivity = false;

    grpc::Status GetClusterState(
        grpc::ServerContext*,
        const Ydb::Bridge::GetClusterStateRequest* request,
        Ydb::Bridge::GetClusterStateResponse* response) override
    {
        RequestedConnectivity = request->include_pile_connectivity();
        auto* operation = response->mutable_operation();
        operation->set_ready(true);
        operation->set_status(Status);
        if (Status != Ydb::StatusIds::SUCCESS) {
            return grpc::Status::OK;
        }
        Ydb::Bridge::GetClusterStateResult result;
        result.set_generation(42);
        result.set_conversion_in_progress(true);
        auto* state = result.add_pile_states();
        state->set_pile_name("a");
        state->set_state(Ydb::Bridge::PileState::PRIMARY);
        auto* pending = result.mutable_pending_pile_operation();
        pending->set_kind(Ydb::Bridge::PendingPileOperation::REMOVE);
        pending->set_pile_name("retired");
        pending->set_error_reason("retry");
        if (IncludeReport && request->include_pile_connectivity()) {
            auto* report = result.mutable_pile_connectivity();
            report->set_reporter_node_id(17);
            report->set_reporter_is_root(true);
            report->set_binding_in_progress(true);
            report->set_generation(41); // Independent from the public state snapshot.
            auto* pile = report->add_piles();
            pile->set_pile_name("a");
            pile->set_static_nodes(5);
            pile->set_connected_static_nodes(3);
            pile->set_has_node_quorum(true);
            pile->set_in_config_quorum(true);
            pile->set_in_data_quorum(true);
            report->add_piles()->set_pile_name(""); // Preserve empty names and false/zero fields.
        }
        operation->mutable_result()->PackFrom(result);
        return grpc::Status::OK;
    }
};

template <class TCallback>
void WithClient(TBridgeService& service, TCallback callback) {
    int port = 0;
    grpc::ServerBuilder builder;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
    builder.RegisterService(&service);
    auto server = builder.BuildAndStart();
    UNIT_ASSERT(server);
    TDriver driver(TDriverConfig().SetEndpoint("127.0.0.1:" + std::to_string(port))
        .SetDiscoveryMode(EDiscoveryMode::Off));
    TBridgeClient client(driver);
    callback(client);
    driver.Stop(true);
}

TGetClusterStateResult Read(TBridgeClient& client, const TGetClusterStateSettings& settings = {}) {
    auto future = client.GetClusterState(settings);
    UNIT_ASSERT(future.Wait(TDuration::Seconds(10)));
    return future.ExtractValueSync();
}

} // namespace

Y_UNIT_TEST_SUITE(BridgeConnectivity) {
    Y_UNIT_TEST(FlagDefaultsOffAndAbsentReportIsNullopt) {
        TBridgeService service;
        WithClient(service, [&](TBridgeClient& client) {
            const auto result = Read(client);
            UNIT_ASSERT(result.IsSuccess());
            UNIT_ASSERT(!service.RequestedConnectivity);
            UNIT_ASSERT(!result.GetPileConnectivity());
        });
    }

    Y_UNIT_TEST(FlagIsSentAndEveryReportFieldIsParsed) {
        TBridgeService service;
        WithClient(service, [&](TBridgeClient& client) {
            const auto settings = TGetClusterStateSettings().IncludePileConnectivity(true);
            const auto copiedSettings = settings;
            const auto result = Read(client, copiedSettings);
            UNIT_ASSERT(result.IsSuccess());
            UNIT_ASSERT(service.RequestedConnectivity);
            UNIT_ASSERT_VALUES_EQUAL(result.GetGeneration(), 42);
            UNIT_ASSERT(result.IsConversionInProgress());
            UNIT_ASSERT_VALUES_EQUAL(result.GetState().at(0).PileName, "a");
            UNIT_ASSERT(result.GetState().at(0).State == EPileState::PRIMARY);
            UNIT_ASSERT_VALUES_EQUAL(result.GetPendingPileOperation()->ErrorReason, "retry");
            UNIT_ASSERT(result.GetPileConnectivity());
            const auto& report = *result.GetPileConnectivity();
            UNIT_ASSERT_VALUES_EQUAL(report.ReporterNodeId, 17);
            UNIT_ASSERT(report.ReporterIsRoot);
            UNIT_ASSERT(report.BindingInProgress);
            UNIT_ASSERT_VALUES_EQUAL(report.Generation, 41);
            UNIT_ASSERT_VALUES_EQUAL(report.Piles.size(), 2);
            const auto& pile = report.Piles[0];
            UNIT_ASSERT_VALUES_EQUAL(pile.PileName, "a");
            UNIT_ASSERT_VALUES_EQUAL(pile.StaticNodes, 5);
            UNIT_ASSERT_VALUES_EQUAL(pile.ConnectedStaticNodes, 3);
            UNIT_ASSERT(pile.HasNodeQuorum);
            UNIT_ASSERT(pile.InConfigQuorum);
            UNIT_ASSERT(pile.InDataQuorum);
            const auto& empty = report.Piles[1];
            UNIT_ASSERT(empty.PileName.empty());
            UNIT_ASSERT_VALUES_EQUAL(empty.StaticNodes, 0);
            UNIT_ASSERT_VALUES_EQUAL(empty.ConnectedStaticNodes, 0);
            UNIT_ASSERT(!empty.HasNodeQuorum);
            UNIT_ASSERT(!empty.InConfigQuorum);
            UNIT_ASSERT(!empty.InDataQuorum);
        });
    }

    Y_UNIT_TEST(OlderServerMayOmitRequestedReport) {
        TBridgeService service;
        service.IncludeReport = false;
        WithClient(service, [&](TBridgeClient& client) {
            const auto result = Read(client, TGetClusterStateSettings().IncludePileConnectivity(true));
            UNIT_ASSERT(result.IsSuccess());
            UNIT_ASSERT(service.RequestedConnectivity);
            UNIT_ASSERT(!result.GetPileConnectivity());
        });
    }

    Y_UNIT_TEST(ServerFailurePreservesStatusAndHasNoReport) {
        TBridgeService service;
        service.Status = Ydb::StatusIds::UNAVAILABLE;
        WithClient(service, [&](TBridgeClient& client) {
            const auto result = Read(client, TGetClusterStateSettings().IncludePileConnectivity(true));
            UNIT_ASSERT(result.GetStatus() == EStatus::UNAVAILABLE);
            UNIT_ASSERT(!result.GetPileConnectivity());
        });
    }
}

} // namespace NYdb::NBridge
