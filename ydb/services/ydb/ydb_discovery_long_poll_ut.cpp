#include "ydb_common_ut.h"

#include <ydb/core/base/statestorage.h>
#include <ydb/public/api/grpc/ydb_discovery_v1.grpc.pb.h>

#include <algorithm>
#include <chrono>
#include <optional>

namespace NYdb {

Y_UNIT_TEST_SUITE(YdbDiscoveryLongPoll) {

Y_UNIT_TEST(DirectPollWakesOnBoardChange) {
    using namespace std::chrono_literals;
    NKikimrConfig::TAppConfig config;
    config.MutableFeatureFlags()->SetEnableSubscriptionsInDiscovery(true);
    TKikimrWithGrpcAndRootSchema server(config);
    auto channel = grpc::CreateChannel(TStringBuilder() << "localhost:" << server.GetPort(),
        grpc::InsecureChannelCredentials());
    auto stub = Ydb::Discovery::V1::DiscoveryService::NewStub(channel);
    Ydb::Discovery::ListEndpointsRequest request;
    request.set_database("/Root");

    auto version = [](const grpc::ClientContext& context) -> std::optional<TString> {
        for (const auto* metadata : {&context.GetServerInitialMetadata(), &context.GetServerTrailingMetadata()}) {
            if (auto it = metadata->find("x-ydb-endpoint-version"); it != metadata->end()) {
                return TString(it->second.data(), it->second.size());
            }
        }
        return {};
    };
    auto list = [&](grpc::ClientContext& context) {
        context.AddMetadata("x-ydb-database", "/Root");
        context.set_deadline(std::chrono::system_clock::now() + 10s);
        Ydb::Discovery::ListEndpointsResponse response;
        const auto status = stub->ListEndpoints(&context, request, &response);
        UNIT_ASSERT_C(status.ok(), status.error_message());
        UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
        return version(context);
    };

    grpc::ClientContext ordinary;
    UNIT_ASSERT(!list(ordinary));

    grpc::ClientContext initial;
    initial.AddMetadata("x-ydb-discovery-long-poll", "1");
    auto before = list(initial);
    UNIT_ASSERT(before && !before->empty());

    // An older managed worker already waited before forwarding the route condition.
    grpc::ClientContext forwarded;
    forwarded.AddMetadata("x-ydb-discovery-long-poll", "1");
    forwarded.AddMetadata("x-ydb-if-route-generation", "7");
    forwarded.AddMetadata("x-ydb-if-endpoint-version", *before);
    UNIT_ASSERT(list(forwarded) == before);

    grpc::CompletionQueue queue;
    grpc::ClientContext poll;
    poll.set_deadline(std::chrono::system_clock::now() + 15s);
    poll.AddMetadata("x-ydb-database", "/Root");
    poll.AddMetadata("x-ydb-discovery-long-poll", "1");
    poll.AddMetadata("x-ydb-if-endpoint-version", *before);
    auto call = stub->AsyncListEndpoints(&poll, request, &queue);
    Ydb::Discovery::ListEndpointsResponse response;
    grpc::Status status;
    call->Finish(&response, &status, &poll);
    bool ok = false;
    void* tag = nullptr;
    auto wait = queue.AsyncNext(&tag, &ok, std::chrono::system_clock::now() + 250ms);
    if (wait == grpc::CompletionQueue::TIMEOUT) {
        NKikimrStateStorage::TEndpointBoardEntry member;
        member.SetAddress("new-node.example");
        member.SetPort(2135);
        auto* runtime = server.GetRuntime();
        runtime->Register(CreateBoardPublishActor(MakeEndpointsBoardPath("/Root"),
            member.SerializeAsString(), runtime->AllocateEdgeActor(), 0, true));
        wait = queue.AsyncNext(&tag, &ok, std::chrono::system_clock::now() + 15s);
    }
    queue.Shutdown();
    bool completed = wait == grpc::CompletionQueue::GOT_EVENT && tag == &poll && ok;
    while (queue.Next(&tag, &ok)) {}
    UNIT_ASSERT(completed);
    UNIT_ASSERT_C(status.ok(), status.error_message());
    UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
    Ydb::Discovery::ListEndpointsResult result;
    UNIT_ASSERT(response.operation().result().UnpackTo(&result));
    UNIT_ASSERT(std::any_of(result.endpoints().begin(), result.endpoints().end(), [](const auto& ep) {
        return ep.address() == "new-node.example" && ep.port() == 2135;
    }));
    const auto after = version(poll);
    UNIT_ASSERT(after && after != before);
}

} // YdbDiscoveryLongPoll
} // namespace NYdb
