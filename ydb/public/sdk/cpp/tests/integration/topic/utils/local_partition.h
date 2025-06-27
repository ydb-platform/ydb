#pragma once

#include <ydb/public/sdk/cpp/tests/integration/topic/utils/setup.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <ydb/public/api/grpc/ydb_discovery_v1.grpc.pb.h>

#include <grpc++/grpc++.h>

#include <thread>


namespace NYdb::inline Dev::NTopic::NTests {

class TMockDiscoveryService: public Ydb::Discovery::V1::DiscoveryService::Service {
public:
    TMockDiscoveryService() {
        int discoveryPort = 0;

        grpc::ServerBuilder builder;
        builder.AddListeningPort("0.0.0.0:0", grpc::InsecureServerCredentials(), &discoveryPort);
        builder.RegisterService(this);
        Server = builder.BuildAndStart();

        DiscoveryAddr = "0.0.0.0:" + std::to_string(discoveryPort);
        std::cerr << "==== TMockDiscovery server started on port " << discoveryPort << std::endl;
    }

    void SetGoodEndpoints(ITopicTestSetup& fixture) {
        std::lock_guard lock(Lock);
        std::cerr << "==== TMockDiscovery set good endpoint nodes " << std::endl;

        auto nodeIds = fixture.GetNodeIds();
        SetEndpointsLocked(nodeIds[0], nodeIds.size(), fixture.GetPort());
    }

    // Call this method only after locking the Lock.
    void SetEndpointsLocked(std::uint32_t firstNodeId, std::uint32_t nodeCount, std::uint16_t port) {
        std::cerr << "==== TMockDiscovery add endpoints, firstNodeId " << firstNodeId << ", nodeCount " << nodeCount << ", port " << port << std::endl;

        MockResults.clear_endpoints();
        if (nodeCount > 0) {
            Ydb::Discovery::EndpointInfo* endpoint = MockResults.add_endpoints();
            endpoint->set_address(TStringBuilder() << "localhost");
            endpoint->set_port(port);
            endpoint->set_node_id(firstNodeId);
        }
        if (nodeCount > 1) {
            Ydb::Discovery::EndpointInfo* endpoint = MockResults.add_endpoints();
            endpoint->set_address(TStringBuilder() << "ip6-localhost"); // name should be different
            endpoint->set_port(port);
            endpoint->set_node_id(firstNodeId + 1);
        }
        if (nodeCount > 2) {
            Y_ENSURE(false, "Unsupported count of nodes");
        }
    }

    void SetEndpoints(std::uint32_t firstNodeId, std::uint32_t nodeCount, std::uint16_t port) {
        std::lock_guard lock(Lock);
        SetEndpointsLocked(firstNodeId, nodeCount, port);
    }

    grpc::Status ListEndpoints(grpc::ServerContext* context, const Ydb::Discovery::ListEndpointsRequest* request, Ydb::Discovery::ListEndpointsResponse* response) override {
        std::lock_guard lock(Lock);
        Y_ENSURE(context);

        if (Delay != std::chrono::milliseconds::zero()) {
            std::cerr << "==== Delay " << Delay << " before ListEndpoints request" << std::endl;
            auto start = std::chrono::steady_clock::now();
            while (start + Delay < std::chrono::steady_clock::now()) {
                if (context->IsCancelled()) {
                    return grpc::Status::CANCELLED;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        }

        std::cerr << "==== ListEndpoints request: " << request->ShortDebugString() << std::endl;

        auto* op = response->mutable_operation();
        op->set_ready(true);
        op->set_status(Ydb::StatusIds::SUCCESS);
        op->mutable_result()->PackFrom(MockResults);

        std::cerr << "==== ListEndpoints response: " << response->ShortDebugString() << std::endl;

        return grpc::Status::OK;
    }

    std::string GetDiscoveryAddr() const {
        return DiscoveryAddr;
    }

    void SetDelay(std::chrono::milliseconds delay) {
        Delay = delay;
    }

private:
    Ydb::Discovery::ListEndpointsResult MockResults;
    std::string DiscoveryAddr;
    std::unique_ptr<grpc::Server> Server;
    std::mutex Lock;

    std::chrono::milliseconds Delay = {};
};

TDriverConfig CreateConfig(ITopicTestSetup& setup, const std::string& discoveryAddr);

TWriteSessionSettings CreateWriteSessionSettings(ITopicTestSetup& setup);

TReadSessionSettings CreateReadSessionSettings(ITopicTestSetup& setup);

void WriteMessage(ITopicTestSetup& setup, TTopicClient& client);

void ReadMessage(ITopicTestSetup& setup, TTopicClient& client, std::uint64_t expectedCommitedOffset = 1);

}
