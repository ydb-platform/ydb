#include <ydb/apps/bridge_skipper/runtime/ydb_clients.h>

#include <ydb/public/api/grpc/draft/ydb_bridge_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_monitoring_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_config_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_discovery_v1.grpc.pb.h>
#include <ydb/public/api/protos/draft/ydb_bridge.pb.h>
#include <ydb/public/api/protos/ydb_monitoring.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/file.h>
#include <util/string/cast.h>
#include <util/system/tempfile.h>

#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>

#include <atomic>
#include <chrono>
#include <mutex>
#include <thread>
#include <utility>

#include <unistd.h>

namespace NBridgeSkipper {
namespace {

NYdb::TStatus Status(NYdb::EStatus status = NYdb::EStatus::SUCCESS) {
    return NYdb::TStatus(status, NYdb::NIssue::TIssues{});
}

TRpcResult Rpc(EYdbMethod method = EYdbMethod::PlainState) {
    TRpcResult result;
    result.Request = {"grpcs://node:2135", "a", 17};
    result.Method = method;
    result.SentAt = TMonotonic::Seconds(123);
    return result;
}

NYdb::NBridge::TGetClusterStateResult State(
    std::vector<NYdb::NBridge::TPileStateUpdate> piles = {{"a", NYdb::NBridge::EPileState::PRIMARY}, {"b", NYdb::NBridge::EPileState::SYNCHRONIZED}},
    std::optional<NYdb::NBridge::TPileConnectivityReport> connectivity = std::nullopt)
{
    return NYdb::NBridge::TGetClusterStateResult(Status(), std::move(piles), 42, true,
        NYdb::NBridge::TPendingPileOperation{"", NYdb::NBridge::TPendingPileOperation::EKind::REMOVE, "retry"},
        std::move(connectivity));
}

template<class TResult, class TInvoke>
TResult Await(TInvoke invoke) {
    auto promise = NThreading::NewPromise<TResult>();
    auto future = promise.GetFuture();
    invoke([promise](TResult result) mutable {
        promise.SetValue(std::move(result));
    });
    UNIT_ASSERT(future.Wait(TDuration::Seconds(10)));
    return future.ExtractValueSync();
}

struct TWireCapture {
    Ydb::Bridge::GetClusterStateRequest State;
    Ydb::Monitoring::SelfCheckRequest SelfCheck;
    Ydb::Bridge::UpdateClusterStateRequest Update;
    Ydb::Config::FetchConfigRequest Config;
    Ydb::Discovery::ListEndpointsRequest Discovery;
    std::chrono::system_clock::time_point Deadline;
    bool DatabaseHeader = false;
    TString AuthTicket;
    size_t Updates = 0;
};

class TWireService final
    : public Ydb::Bridge::V1::BridgeService::Service
    , public Ydb::Monitoring::V1::MonitoringService::Service
    , public Ydb::Config::V1::ConfigService::Service
    , public Ydb::Discovery::V1::DiscoveryService::Service
{
public:
    // Answers the next call with this operation status, then SUCCESS again.
    Ydb::StatusIds::StatusCode NextStatus = Ydb::StatusIds::SUCCESS;
    std::atomic<bool> HangSelfCheck{false};
    std::atomic<bool> HangDiscovery{false};
    bool DedicatedStorage = false;
    std::optional<ui16> IpEndpointPort;

    grpc::Status FetchConfig(grpc::ServerContext* context, const Ydb::Config::FetchConfigRequest* request,
        Ydb::Config::FetchConfigResponse* response) override
    {
        std::lock_guard lock(Mutex);
        Capture.Config = *request;
        CaptureContext(*context);
        Ydb::Config::FetchConfigResult result;
        auto* entry = result.add_config();
        entry->mutable_identity()->mutable_main();
        entry->set_config(DedicatedStorage ? "config: {}\n"
            : "config: {domains_config: {domain: [{name: CustomRoot}]}}\n");
        if (DedicatedStorage) {
            auto* storage = result.add_config();
            storage->mutable_identity()->mutable_storage();
            storage->set_config("config: {domain_name: DedicatedRoot, domains_config: {explicit_state_storage_config: {}}}\n");
        }
        auto* operation = response->mutable_operation();
        operation->set_ready(true);
        operation->set_status(DedicatedStorage && request->all().has_add_blob_storage_and_domains_config()
            ? Ydb::StatusIds::BAD_REQUEST : std::exchange(NextStatus, Ydb::StatusIds::SUCCESS));
        operation->mutable_result()->PackFrom(result);
        return grpc::Status::OK;
    }

    grpc::Status ListEndpoints(grpc::ServerContext* context, const Ydb::Discovery::ListEndpointsRequest* request,
        Ydb::Discovery::ListEndpointsResponse* response) override
    {
        {
            std::lock_guard lock(Mutex);
            Capture.Discovery = *request;
            CaptureContext(*context);
        }
        while (HangDiscovery && !context->IsCancelled()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        if (context->IsCancelled()) {
            return grpc::Status::CANCELLED;
        }
        Ydb::Discovery::ListEndpointsResult result;
        auto* endpoint = result.add_endpoints();
        endpoint->set_address("new-node");
        endpoint->set_port(2135);
        endpoint->set_node_id(12);
        endpoint->set_bridge_pile_name("new-pile");
        if (IpEndpointPort) {
            endpoint->set_address("unresolvable.invalid");
            endpoint->set_port(*IpEndpointPort);
            endpoint->set_node_id(77);
            endpoint->set_bridge_pile_name("a");
            endpoint->add_ip_v4("127.0.0.1");
        }
        auto* operation = response->mutable_operation();
        operation->set_ready(true);
        operation->set_status(Ydb::StatusIds::SUCCESS);
        operation->mutable_result()->PackFrom(result);
        return grpc::Status::OK;
    }

    TWireCapture ReadCapture() {
        std::lock_guard lock(Mutex);
        return Capture;
    }

    grpc::Status GetClusterState(
        grpc::ServerContext* context,
        const Ydb::Bridge::GetClusterStateRequest* request,
        Ydb::Bridge::GetClusterStateResponse* response) override
    {
        Ydb::StatusIds::StatusCode status;
        {
            std::lock_guard lock(Mutex);
            Capture.State = *request;
            CaptureContext(*context);
            status = std::exchange(NextStatus, Ydb::StatusIds::SUCCESS);
        }
        Ydb::Bridge::GetClusterStateResult result;
        result.set_generation(42);
        auto* a = result.add_pile_states();
        a->set_pile_name("a");
        a->set_state(Ydb::Bridge::PileState::PRIMARY);
        auto* b = result.add_pile_states();
        b->set_pile_name("b");
        b->set_state(Ydb::Bridge::PileState::SYNCHRONIZED);
        if (request->include_pile_connectivity()) {
            auto* report = result.mutable_pile_connectivity();
            report->set_generation(42);
            report->set_reporter_node_id(99);
            report->add_piles()->set_pile_name("a");
        }
        auto* operation = response->mutable_operation();
        operation->set_ready(true);
        operation->set_status(status);
        operation->mutable_result()->PackFrom(result);
        return grpc::Status::OK;
    }

    grpc::Status SelfCheck(
        grpc::ServerContext* context,
        const Ydb::Monitoring::SelfCheckRequest* request,
        Ydb::Monitoring::SelfCheckResponse* response) override
    {
        Ydb::StatusIds::StatusCode status;
        {
            std::lock_guard lock(Mutex);
            Capture.SelfCheck = *request;
            CaptureContext(*context);
            status = std::exchange(NextStatus, Ydb::StatusIds::SUCCESS);
        }
        while (HangSelfCheck && !context->IsCancelled()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        if (context->IsCancelled()) {
            return grpc::Status::CANCELLED;
        }
        Ydb::Monitoring::SelfCheckResult result;
        result.mutable_location()->set_id(77);
        result.mutable_location()->mutable_pile()->set_name("a");
        result.add_issue_log()->set_message("owned issue");
        auto* operation = response->mutable_operation();
        operation->set_ready(true);
        operation->set_status(status);
        operation->mutable_result()->PackFrom(result);
        return grpc::Status::OK;
    }

    grpc::Status UpdateClusterState(
        grpc::ServerContext* context,
        const Ydb::Bridge::UpdateClusterStateRequest* request,
        Ydb::Bridge::UpdateClusterStateResponse* response) override
    {
        Ydb::StatusIds::StatusCode status;
        {
            std::lock_guard lock(Mutex);
            Capture.Update = *request;
            ++Capture.Updates;
            CaptureContext(*context);
            status = std::exchange(NextStatus, Ydb::StatusIds::SUCCESS);
        }
        auto* operation = response->mutable_operation();
        operation->set_ready(true);
        operation->set_status(status);
        return grpc::Status::OK;
    }

private:
    void CaptureContext(const grpc::ServerContext& context) {
        Capture.Deadline = context.deadline();
        const auto& metadata = context.client_metadata();
        Capture.DatabaseHeader = metadata.find("x-ydb-database") != metadata.end();
        if (const auto it = metadata.find("x-ydb-auth-ticket"); it != metadata.end()) {
            Capture.AuthTicket = TString(it->second.data(), it->second.size());
        }
    }

    std::mutex Mutex;
    TWireCapture Capture;
};

void AssertDeadline(const TWireCapture& capture, std::chrono::system_clock::time_point startedAt, TDuration timeout) {
    UNIT_ASSERT(capture.Deadline >= startedAt + std::chrono::microseconds(timeout.MicroSeconds()) - std::chrono::milliseconds(100));
    UNIT_ASSERT(capture.Deadline <= startedAt + std::chrono::microseconds(timeout.MicroSeconds()) + std::chrono::seconds(1));
    UNIT_ASSERT(!capture.DatabaseHeader);
}

std::unique_ptr<grpc::Server> StartWireServer(TWireService& service, TString& endpoint) {
    int port = 0;
    grpc::ServerBuilder builder;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
    builder.RegisterService(static_cast<Ydb::Bridge::V1::BridgeService::Service*>(&service));
    builder.RegisterService(static_cast<Ydb::Monitoring::V1::MonitoringService::Service*>(&service));
    builder.RegisterService(static_cast<Ydb::Config::V1::ConfigService::Service*>(&service));
    builder.RegisterService(static_cast<Ydb::Discovery::V1::DiscoveryService::Service*>(&service));
    auto server = builder.BuildAndStart();
    endpoint = "grpc://127.0.0.1:" + ::ToString(port);
    return server;
}

void VerifyTokenPreparationExpiry(ui64 preparationSeconds) {
    TWireService service;
    TString endpoint;
    auto server = StartWireServer(service, endpoint);
    UNIT_ASSERT(server);
    TTempFileHandle token;
    TFileOutput(token.Name()).Write("token\n");
    const auto config = ParseConfig("cluster_id: test\nendpoints: {a: ['" + endpoint + "']}\n"
        "auth: {token_file: '" + token.Name() + "'}\n");
    std::atomic<ui64> now{100};
    std::atomic<ui64> preparation{preparationSeconds};
    auto clients = CreateYdbClients(config, LoadCredentials(config), [&] {
        return TMonotonic::Seconds(now.load());
    }, [&](const TMaybe<TString>& file) {
        auto loaded = LoadToken(file);
        now.fetch_add(preparation.load());
        return loaded;
    });
    const TPlan plan{42, {{"b", EPileState::Disconnected}}, endpoint};
    auto expiredPromise = NThreading::NewPromise<TUpdateResult>();
    auto expired = expiredPromise.GetFuture();
    const bool launched = clients->Update({endpoint, "a", 1}, plan, {TMonotonic::Seconds(130)},
        [expiredPromise](TUpdateResult result) mutable {
            expiredPromise.SetValue(std::move(result));
        });
    if (launched) {
        UNIT_ASSERT(expired.Wait(TDuration::Seconds(10)));
    }
    const auto preparedAt = now.load();
    const auto firstCount = service.ReadCapture().Updates;
    // Recovery with a new authorization and bounded preparation must launch.
    preparation = 1;
    const auto good = Await<TUpdateResult>([&](auto completed) {
        UNIT_ASSERT(clients->Update({endpoint, "a", 2}, plan, {TMonotonic::Seconds(160)}, std::move(completed)));
    });
    const auto totalCount = service.ReadCapture().Updates;
    clients->Stop();
    UNIT_ASSERT_VALUES_EQUAL(preparedAt, 100 + preparationSeconds);
    UNIT_ASSERT(!launched);
    UNIT_ASSERT(!expired.HasValue());
    UNIT_ASSERT_VALUES_EQUAL(firstCount, 0);
    UNIT_ASSERT(good.Rpc.Success);
    UNIT_ASSERT_VALUES_EQUAL(totalCount, 1);
}

} // namespace

Y_UNIT_TEST_SUITE(TSkipperYdbClients) {
    Y_UNIT_TEST(DiscoverySupportsDedicatedStorageWithoutDowngrade) {
        TWireService service;
        service.DedicatedStorage = true;
        TString endpoint;
        auto server = StartWireServer(service, endpoint);
        const auto config = ParseConfig("cluster_id: test\nendpoints: {a: ['" + endpoint + "']}\n");
        auto clients = CreateYdbClients(config, TLoadedCredentials{});
        const auto result = Await<TInventoryResult>([&](auto completed) {
            clients->Discover({endpoint, "a", 1}, std::move(completed));
        });
        UNIT_ASSERT_C(result.Rpc.Success, result.Rpc.Status);
        UNIT_ASSERT_VALUES_EQUAL(result.Endpoints.size(), 1);
        const auto capture = service.ReadCapture();
        UNIT_ASSERT(!capture.Config.all().has_add_blob_storage_and_domains_config());
        UNIT_ASSERT_VALUES_EQUAL(capture.Discovery.database(), "/DedicatedRoot");
        clients->Stop();
    }

    Y_UNIT_TEST(DiscoveryUsesConfiguredDomainAndAuthenticatedPinnedClients) {
        TWireService service;
        TString endpoint;
        auto server = StartWireServer(service, endpoint);
        const auto config = ParseConfig("cluster_id: test\nendpoints: {a: ['" + endpoint + "']}\n");
        TLoadedCredentials credentials;
        credentials.Token = "discovery-token";
        auto clients = CreateYdbClients(config, credentials);
        const auto result = Await<TInventoryResult>([&](auto completed) {
            clients->Discover({endpoint, "a", 7}, std::move(completed));
        });
        UNIT_ASSERT_C(result.Rpc.Success, result.Error);
        UNIT_ASSERT_VALUES_EQUAL(result.Endpoints.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(result.Endpoints.front().Pile, "new-pile");
        UNIT_ASSERT_VALUES_EQUAL(result.Endpoints.front().NodeId, 12);
        UNIT_ASSERT_VALUES_EQUAL(result.Endpoints.front().Endpoint.Uri, "grpc://new-node:2135");
        const auto capture = service.ReadCapture();
        UNIT_ASSERT(!capture.Config.all().has_add_blob_storage_and_domains_config());
        UNIT_ASSERT_VALUES_EQUAL(capture.Config.operation_params().operation_timeout().seconds(), 1);
        UNIT_ASSERT_VALUES_EQUAL(capture.Discovery.database(), "/CustomRoot");
        UNIT_ASSERT_VALUES_EQUAL(capture.AuthTicket, "discovery-token");
        // A newly registered pinned client can be used and is rejected after removal.
        TWireService second;
        TString learned;
        auto other = StartWireServer(second, learned);
        clients->AddEndpoint(ParseEndpoint(learned), "b");
        const auto state = Await<TStateResult>([&](auto completed) {
            clients->GetState({learned, "b", 8}, EYdbMethod::PlainState, std::move(completed));
        });
        UNIT_ASSERT(state.Rpc.Success);
        clients->RemoveEndpoint(learned);
        const auto removed = Await<TStateResult>([&](auto completed) {
            clients->GetState({learned, "b", 9}, EYdbMethod::PlainState, std::move(completed));
        });
        UNIT_ASSERT_VALUES_EQUAL(removed.Error, "INVALID_RPC_REQUEST");
        clients->Stop();
    }

    Y_UNIT_TEST(DiscoveryDeadlineAndConfigFailureSettleTheInventorySlot) {
        TWireService service;
        TString endpoint;
        auto server = StartWireServer(service, endpoint);
        const auto config = ParseConfig("cluster_id: test\nendpoints: {a: ['" + endpoint + "']}\n");
        auto clients = CreateYdbClients(config, TLoadedCredentials{});
        service.NextStatus = Ydb::StatusIds::UNAUTHORIZED;
        const auto rejected = Await<TInventoryResult>([&](auto completed) {
            clients->Discover({endpoint, "a", 1}, std::move(completed));
        });
        UNIT_ASSERT(!rejected.Rpc.Success);
        UNIT_ASSERT(rejected.Rpc.SdkStatus == NYdb::EStatus::UNAUTHORIZED);
        UNIT_ASSERT(rejected.Endpoints.empty());
        service.HangDiscovery = true;
        const auto started = std::chrono::steady_clock::now();
        const auto hung = Await<TInventoryResult>([&](auto completed) {
            clients->Discover({endpoint, "a", 2}, std::move(completed));
        });
        UNIT_ASSERT(!hung.Rpc.Success);
        UNIT_ASSERT(hung.Rpc.SdkStatus == NYdb::EStatus::CLIENT_DEADLINE_EXCEEDED);
        UNIT_ASSERT(std::chrono::steady_clock::now() - started < std::chrono::seconds(8));
        clients->Stop();
    }

    Y_UNIT_TEST(DiscoveryIpAliasCanReachMonitoringWithoutDns) {
        TWireService service;
        TWireService other;
        TString target;
        auto targetServer = StartWireServer(other, target);
        service.IpEndpointPort = ParseEndpoint(target).Port;
        TString seed;
        auto server = StartWireServer(service, seed);
        auto clients = CreateYdbClients(ParseConfig("cluster_id: test\nendpoints: {a: ['" + seed + "']}\n"),
            TLoadedCredentials{});
        const auto inventory = Await<TInventoryResult>([&](auto completed) {
            clients->Discover({seed, "a", 1}, std::move(completed));
        });
        UNIT_ASSERT_C(inventory.Rpc.Success, inventory.Rpc.Status);
        UNIT_ASSERT_VALUES_EQUAL(inventory.Error, "");
        const auto ip = std::ranges::find_if(inventory.Endpoints, [&](const auto& item) {
            return item.Endpoint.Uri == target;
        });
        UNIT_ASSERT(ip != inventory.Endpoints.end());
        clients->AddEndpoint(ip->Endpoint, ip->Pile);
        const auto health = Await<TSelfCheckResult>([&](auto completed) {
            clients->SelfCheck({ip->Endpoint.Uri, ip->Pile, 2}, std::move(completed));
        });
        UNIT_ASSERT_C(health.Rpc.Success, health.Rpc.Status);
        UNIT_ASSERT(health.ResponderNodeId == ip->NodeId);
        UNIT_ASSERT(health.Reply && health.Reply->ResponderPile == ip->Pile);
        clients->Stop();
    }

    Y_UNIT_TEST(DiscoveryUnsupportedOptionalAliasesKeepOtherPiles) {
        const TRedactor redactor{TLoadedCredentials{}};
        for (const TString unsupported : {"fe80::1%eth0", "fe80::1%25eth0", "bad/address"}) {
            Ydb::Discovery::ListEndpointsResult proto;
            auto* endpoint = proto.add_endpoints();
            endpoint->set_address("node-a");
            endpoint->set_port(2135);
            endpoint->set_node_id(1);
            endpoint->set_bridge_pile_name("a");
            endpoint->add_ip_v6(unsupported);
            endpoint->add_ip_v6("2001:db8::1");
            endpoint->add_ip_v4("127.0.0.1");
            auto* other = proto.add_endpoints();
            other->set_address("node-b");
            other->set_port(2135);
            other->set_node_id(2);
            other->set_bridge_pile_name("b");
            const auto decode = [&] {
                return DecodeInventoryResult(NYdb::NDiscovery::TListEndpointsResult(Status(), proto),
                    Rpc(EYdbMethod::Inventory), redactor, "grpc");
            };
            const auto result = decode();
            UNIT_ASSERT_VALUES_EQUAL_C(result.Error, "", unsupported);
            std::map<TString, std::pair<TString, ui32>> addresses;
            for (const auto& item : result.Endpoints) {
                addresses[item.Endpoint.Uri] = {item.Pile, item.NodeId};
            }
            UNIT_ASSERT((addresses == std::map<TString, std::pair<TString, ui32>>{
                {"grpc://node-a:2135", {"a", 1}}, {"grpc://[2001:db8::1]:2135", {"a", 1}},
                {"grpc://127.0.0.1:2135", {"a", 1}}, {"grpc://node-b:2135", {"b", 2}}}));

            // Only parsing an optional locator is best effort. Conflicting
            // advertised identities still reject the complete refresh.
            proto.mutable_endpoints(1)->add_ip_v4("127.0.0.1");
            UNIT_ASSERT_VALUES_EQUAL(decode().Error, "INVALID_DISCOVERY_ENDPOINT");
            UNIT_ASSERT(decode().Endpoints.empty());
            proto.mutable_endpoints(1)->clear_ip_v4();
            proto.mutable_endpoints(0)->set_address(unsupported);
            UNIT_ASSERT_VALUES_EQUAL(decode().Error, "INVALID_DISCOVERY_ENDPOINT");
            UNIT_ASSERT(decode().Endpoints.empty());
        }
    }

    Y_UNIT_TEST(DiscoveryIpExpansionKeepsInventoryBounded) {
        const TRedactor redactor{TLoadedCredentials{}};
        Ydb::Discovery::ListEndpointsResult proto;
        auto* endpoint = proto.add_endpoints();
        endpoint->set_address("node");
        endpoint->set_port(2135);
        endpoint->set_node_id(1);
        endpoint->set_bridge_pile_name("a");
        for (size_t i = 0; i < 16384; ++i) {
            endpoint->add_ip_v6("2001:db8::" + Sprintf("%zx", i));
        }
        const auto result = DecodeInventoryResult(NYdb::NDiscovery::TListEndpointsResult(Status(), proto),
            Rpc(EYdbMethod::Inventory), redactor, "grpc");
        UNIT_ASSERT_VALUES_EQUAL(result.Error, "DISCOVERY_INVENTORY_TOO_LARGE");
        UNIT_ASSERT(result.Endpoints.empty());
    }

    Y_UNIT_TEST(DiscoveryDomainNameFollowsStoragePoolTransformation) {
        const TString names = "domain_name: Modern, domains_config: {domain: [{name: Old}]}";
        // Automatic-config defaults create the domain when its list is empty.
        for (const TString pools : {"", "storage_pool_types: [], ", "storage_pool_types: [{kind: hdd}], "}) {
            UNIT_ASSERT_VALUES_EQUAL(DiscoveryDatabase("config: {hosts: [{host: localhost}], " + pools
                + "domain_name: Modern, domains_config: {domain: []}}"), "/Modern");
            UNIT_ASSERT_VALUES_EQUAL(DiscoveryDatabase("config: {hosts: [{host: localhost}], " + pools
                + "domains_config: {domain: []}}"), "/Root");
        }
        UNIT_ASSERT_VALUES_EQUAL(DiscoveryDatabase("config: {" + names + "}"), "/Old");
        UNIT_ASSERT_VALUES_EQUAL(DiscoveryDatabase("config: {storage_pool_types: [], " + names + "}"), "/Old");
        UNIT_ASSERT_VALUES_EQUAL(DiscoveryDatabase(
            "config: {storage_pool_types: [{kind: hdd}], " + names + "}"), "/Modern");
        UNIT_ASSERT_VALUES_EQUAL(DiscoveryDatabase(
            "config: {storage_pool_types: [{kind: hdd}], domains_config: {domain: [{name: Old}]}}"), "/Old");
        UNIT_ASSERT_VALUES_EQUAL(DiscoveryDatabase("config: {" + names + "}",
            "config: {storage_pool_types: [{kind: hdd}]}"), "/Modern");
        UNIT_ASSERT_VALUES_EQUAL(DiscoveryDatabase("config: {storage_pool_types: [], " + names + "}",
            "config: {storage_pool_types: [{kind: hdd}]}"), "/Old");
    }

    Y_UNIT_TEST(DiscoveryIncludesPlaintextIpAliasesAndChecksTheirIdentity) {
        const TRedactor redactor{TLoadedCredentials{}};
        Ydb::Discovery::ListEndpointsResult proto;
        auto* endpoint = proto.add_endpoints();
        endpoint->set_address("unresolvable.invalid");
        endpoint->set_port(2135);
        endpoint->set_node_id(12);
        endpoint->set_bridge_pile_name("new-pile");
        endpoint->add_ip_v4("127.0.0.1");
        endpoint->add_ip_v4("127.0.0.1");
        endpoint->add_ip_v6("::1");
        auto decode = [&](TStringBuf scheme = "grpc") {
            return DecodeInventoryResult(NYdb::NDiscovery::TListEndpointsResult(Status(), proto),
                Rpc(EYdbMethod::Inventory), redactor, scheme);
        };
        const auto result = decode();
        UNIT_ASSERT_VALUES_EQUAL(result.Error, "");
        std::set<TString> addresses;
        for (const auto& item : result.Endpoints) {
            addresses.insert(item.Endpoint.Uri);
            UNIT_ASSERT_VALUES_EQUAL(item.NodeId, 12);
            UNIT_ASSERT_VALUES_EQUAL(item.Pile, "new-pile");
        }
        UNIT_ASSERT((addresses == std::set<TString>{"grpc://unresolvable.invalid:2135",
            "grpc://127.0.0.1:2135", "grpc://[::1]:2135"}));
        *proto.add_endpoints() = *proto.mutable_endpoints(0);
        proto.mutable_endpoints(1)->set_address("127.0.0.1");
        proto.mutable_endpoints(1)->set_node_id(13);
        UNIT_ASSERT_VALUES_EQUAL(decode().Error, "INVALID_DISCOVERY_ENDPOINT");
        proto.mutable_endpoints()->RemoveLast();
        proto.mutable_endpoints(0)->set_ssl(true);
        const auto tls = decode("grpcs");
        UNIT_ASSERT_VALUES_EQUAL(tls.Endpoints.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(tls.Endpoints.front().Endpoint.Uri, "grpcs://unresolvable.invalid:2135");
    }

    Y_UNIT_TEST(DiscoveryConfigAndInventoryValidationAreAtomic) {
        UNIT_ASSERT_VALUES_EQUAL(DiscoveryDatabase("config: {domains_config: {domain: [{name: Other}]}}"), "/Other");
        UNIT_ASSERT_VALUES_EQUAL(DiscoveryDatabase("config: {}"), "/Root");
        UNIT_ASSERT_VALUES_EQUAL(DiscoveryDatabase("config: {domain_name: Modern}"), "/Modern");
        UNIT_ASSERT_VALUES_EQUAL(DiscoveryDatabase("config: {}", "config: {domain_name: StorageRoot}"), "/StorageRoot");
        UNIT_ASSERT_VALUES_EQUAL(DiscoveryDatabase("config: {domain_name: MainRoot}", "config: {domain_name: StorageRoot}"), "/MainRoot");
        UNIT_ASSERT_VALUES_EQUAL(DiscoveryDatabase("config: {domains_config: {security_config: {}}}",
            "config: {domains_config: {domain: [{name: StorageRoot}]}}"), "/Root");
        for (const TString yaml : {"config: []", "config: {domains_config: {domain: [{name: a}, {name: b}]}}",
                "config: {domains_config: {domain: [{name: 'bad/path'}]}}", "!!broken secret"})
        {
            UNIT_ASSERT_EXCEPTION_CONTAINS(DiscoveryDatabase(yaml), yexception, "INVALID_DISCOVERY_CONFIG");
        }
        const TRedactor redactor{TLoadedCredentials{}};
        Ydb::Discovery::ListEndpointsResult proto;
        auto* endpoint = proto.add_endpoints();
        endpoint->set_address("::1");
        endpoint->set_port(2135);
        endpoint->set_node_id(1);
        endpoint->set_bridge_pile_name("a");
        auto decode = [&] {
            return DecodeInventoryResult(NYdb::NDiscovery::TListEndpointsResult(Status(), proto), Rpc(EYdbMethod::Inventory), redactor, "grpc");
        };
        UNIT_ASSERT_VALUES_EQUAL(decode().Endpoints.front().Endpoint.Uri, "grpc://[::1]:2135");
        *proto.add_endpoints() = *endpoint;
        UNIT_ASSERT_VALUES_EQUAL(decode().Endpoints.size(), 1);
        proto.mutable_endpoints(1)->set_bridge_pile_name("other");
        UNIT_ASSERT_VALUES_EQUAL(decode().Error, "INVALID_DISCOVERY_ENDPOINT");
        UNIT_ASSERT(decode().Endpoints.empty());
        proto.mutable_endpoints(1)->set_address("other-host");
        UNIT_ASSERT_VALUES_EQUAL(decode().Error, "INVALID_DISCOVERY_ENDPOINT");
        proto.mutable_endpoints(1)->set_ssl(true);
        UNIT_ASSERT_VALUES_EQUAL(decode().Endpoints.size(), 1);
        proto.mutable_endpoints(0)->set_ssl_target_name_override("balancer");
        UNIT_ASSERT(decode().Endpoints.empty());
    }

    Y_UNIT_TEST(TokenPreparationMustFinishWithinLaunchAuthorization) {
        VerifyTokenPreparationExpiry(31);
    }

    Y_UNIT_TEST(TokenPreparationAtLaunchAuthorizationBoundaryDoesNotLaunch) {
        VerifyTokenPreparationExpiry(30);
    }

    Y_UNIT_TEST(PinnedSdkClientsUseExactAuthorityAndInheritDriverTlsCredentials) {
        for (const auto& [uri, authority] : std::vector<std::pair<TString, std::string>>{
                {"grpc://127.0.0.1:21001", "127.0.0.1:21001"},
                {"grpcs://node.example:2136", "node.example:2136"},
                {"grpcs://[::1]:32135", "[::1]:32135"}})
        {
            const auto settings = MakePinnedClientSettings(ParseEndpoint(uri));
            UNIT_ASSERT(settings.DiscoveryEndpoint_);
            UNIT_ASSERT_VALUES_EQUAL(*settings.DiscoveryEndpoint_, authority);
            UNIT_ASSERT(settings.Database_ && settings.Database_->empty());
            UNIT_ASSERT(settings.DiscoveryMode_ == NYdb::EDiscoveryMode::Off);
            UNIT_ASSERT(!settings.SslCredentials_);
            UNIT_ASSERT(!settings.CredentialsProviderFactory_);
        }
    }

    Y_UNIT_TEST(DecodeStatePreservesMetadataTimesAndAbsentConnectivity) {
        const auto result = DecodeStateResult(State(), Rpc(), TRedactor(TLoadedCredentials{}));
        UNIT_ASSERT(result.Rpc.Success);
        UNIT_ASSERT(result.Rpc.SdkStatus == NYdb::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(result.Rpc.Status, "SUCCESS");
        UNIT_ASSERT_VALUES_EQUAL(result.Rpc.Request.Sequence, 17);
        UNIT_ASSERT(result.Error.empty());
        UNIT_ASSERT(result.View);
        UNIT_ASSERT_VALUES_EQUAL(result.View->Generation, 42);
        UNIT_ASSERT_VALUES_EQUAL(result.View->SentAt, TMonotonic::Seconds(123));
        UNIT_ASSERT_VALUES_EQUAL(result.View->Endpoint, "grpcs://node:2135");
        UNIT_ASSERT(result.View->ConversionInProgress);
        UNIT_ASSERT(result.View->Pending->Pile.has_value());
        UNIT_ASSERT_VALUES_EQUAL(*result.View->Pending->Pile, "");
        UNIT_ASSERT_VALUES_EQUAL(result.View->Pending->Kind, "REMOVE");
        UNIT_ASSERT_VALUES_EQUAL(result.View->Pending->Error, "retry");
        UNIT_ASSERT(!result.Connectivity);
    }

    Y_UNIT_TEST(DecodeRejectsDuplicatesUnknownEnumsAndInvalidViewsBeforeMaps) {
        for (const auto& piles : std::vector<std::vector<NYdb::NBridge::TPileStateUpdate>>{
                {{"a", NYdb::NBridge::EPileState::PRIMARY}, {"a", NYdb::NBridge::EPileState::PRIMARY}},
                {{"a", NYdb::NBridge::EPileState::PRIMARY}, {"b", static_cast<NYdb::NBridge::EPileState>(99)}},
                {{"a", NYdb::NBridge::EPileState::PRIMARY}, {"b", NYdb::NBridge::EPileState::PRIMARY}},
                {{"a", NYdb::NBridge::EPileState::UNSPECIFIED}}}) {
            const auto result = DecodeStateResult(State(piles), Rpc(), TRedactor(TLoadedCredentials{}));
            UNIT_ASSERT(!result.Error.empty());
            UNIT_ASSERT(!result.View);
            UNIT_ASSERT(!result.Connectivity);
            UNIT_ASSERT_VALUES_EQUAL(result.Rpc.Status, "SUCCESS");
        }
    }

    Y_UNIT_TEST(ConnectivityPreservesEveryFieldAndRejectsDuplicateNames) {
        NYdb::NBridge::TPileConnectivityReport report;
        report.ReporterNodeId = 99;
        report.ReporterIsRoot = true;
        report.BindingInProgress = true;
        report.Generation = 41;
        report.Piles = {{"a", 5, 3, true, true, true}, {"", 0, 0, false, false, false}};
        auto decoded = DecodeStateResult(State({{"a", NYdb::NBridge::EPileState::PRIMARY}}, report), Rpc(EYdbMethod::ConnectivityState), TRedactor(TLoadedCredentials{}));
        UNIT_ASSERT(decoded.Connectivity);
        const auto& connectivity = *decoded.Connectivity;
        UNIT_ASSERT_VALUES_EQUAL(connectivity.VantagePile, "a");
        UNIT_ASSERT_VALUES_EQUAL(connectivity.ReporterNodeId, 99);
        UNIT_ASSERT(connectivity.ReporterIsRoot);
        UNIT_ASSERT(connectivity.BindingInProgress);
        UNIT_ASSERT_VALUES_EQUAL(connectivity.Generation, 41);
        UNIT_ASSERT_VALUES_EQUAL(connectivity.SentAt, TMonotonic::Seconds(123));
        UNIT_ASSERT_VALUES_EQUAL(connectivity.Piles.at("a").StaticNodes, 5);
        UNIT_ASSERT_VALUES_EQUAL(connectivity.Piles.at("a").ConnectedStaticNodes, 3);
        UNIT_ASSERT(connectivity.Piles.at("a").HasNodeQuorum);
        UNIT_ASSERT(connectivity.Piles.at("a").InConfigQuorum);
        UNIT_ASSERT(connectivity.Piles.at("a").InDataQuorum);
        UNIT_ASSERT(connectivity.Piles.contains(""));
        report.Piles.push_back(report.Piles.front());
        decoded = DecodeStateResult(State({{"a", NYdb::NBridge::EPileState::PRIMARY}}, report), Rpc(EYdbMethod::ConnectivityState), TRedactor(TLoadedCredentials{}));
        UNIT_ASSERT(!decoded.Connectivity);
        UNIT_ASSERT(!decoded.View);
        UNIT_ASSERT(!decoded.Error.empty());
    }

    Y_UNIT_TEST(FailedRepliesRetainTheirExactStatusAndNoPayload) {
        for (const auto status : {NYdb::EStatus::UNAVAILABLE, NYdb::EStatus::TRANSPORT_UNAVAILABLE}) {
            const NYdb::NBridge::TGetClusterStateResult sdkState(Status(status), {}, 0);
            const auto state = DecodeStateResult(sdkState, Rpc(EYdbMethod::ConnectivityState), TRedactor(TLoadedCredentials{}));
            UNIT_ASSERT(!state.Rpc.Success);
            UNIT_ASSERT(state.Rpc.SdkStatus == status);
            UNIT_ASSERT(!state.Error.empty());
            UNIT_ASSERT(!state.View);
            UNIT_ASSERT(!state.Connectivity);
            const auto health = DecodeSelfCheckResult(
                NYdb::NMonitoring::TSelfCheckResult(Status(status), {}), Rpc(EYdbMethod::SelfCheck), TRedactor(TLoadedCredentials{}));
            UNIT_ASSERT(!health.Rpc.Success);
            UNIT_ASSERT(health.Rpc.SdkStatus == status);
            UNIT_ASSERT(!health.Reply);
            UNIT_ASSERT(!health.ResponderNodeId);
        }
    }

    Y_UNIT_TEST(SelfCheckDecodeClassifiesRedactsAndDistinguishesMissingAndEmptyPile) {
        TLoadedCredentials credentials;
        credentials.Token = "selfcheck-secret";
        const TRedactor redactor(credentials);
        TSelfCheckResult saved;
        {
            Ydb::Monitoring::SelfCheckResult proto;
            proto.mutable_location()->set_id(77);
            const NYdb::NMonitoring::TSelfCheckResult absent(Status(), Ydb::Monitoring::SelfCheckResult(proto));
            const auto missing = DecodeSelfCheckResult(absent, Rpc(EYdbMethod::SelfCheck), redactor);
            UNIT_ASSERT(missing.Reply);
            UNIT_ASSERT(!missing.Reply->ResponderPile);
            UNIT_ASSERT(missing.ResponderNodeId == 77);
            proto.mutable_location()->mutable_pile()->set_name("");
            auto* issue = proto.add_issue_log();
            issue->set_type("PILE_STATE_STORAGE");
            issue->set_status(Ydb::Monitoring::StatusFlag::RED);
            issue->set_message("owned selfcheck-secret");
            issue->mutable_location()->mutable_compute()->mutable_state_storage()->mutable_pile()->set_name("b");
            const NYdb::NMonitoring::TSelfCheckResult empty(Status(), std::move(proto));
            saved = DecodeSelfCheckResult(empty, Rpc(EYdbMethod::SelfCheck), redactor);
        }
        // The decoded reply owns its classification; the SDK result is gone.
        UNIT_ASSERT(saved.Reply);
        UNIT_ASSERT_VALUES_EQUAL(saved.Reply->Endpoint, "grpcs://node:2135");
        UNIT_ASSERT_VALUES_EQUAL(saved.Reply->SentAt, TMonotonic::Seconds(123));
        UNIT_ASSERT(saved.Reply->ResponderPile.has_value());
        UNIT_ASSERT_VALUES_EQUAL(*saved.Reply->ResponderPile, "");
        UNIT_ASSERT_VALUES_EQUAL(*saved.ResponderNodeId, 77);
        const auto& notes = saved.Reply->Classification.Notes;
        UNIT_ASSERT_VALUES_EQUAL(notes.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(notes[0], "PILE_STATE_STORAGE RED pile=b: owned [REDACTED]");
        const auto missingLocation = DecodeSelfCheckResult(
            NYdb::NMonitoring::TSelfCheckResult(Status(), {}), Rpc(EYdbMethod::SelfCheck), redactor);
        UNIT_ASSERT(missingLocation.Reply);
        UNIT_ASSERT(!missingLocation.Reply->ResponderPile);
        UNIT_ASSERT(!missingLocation.ResponderNodeId);
    }

    Y_UNIT_TEST(EverySdkStatusClassifiesToItsOutcome) {
        for (const auto status : {NYdb::EStatus::STATUS_UNDEFINED, NYdb::EStatus::SUCCESS, NYdb::EStatus::BAD_REQUEST,
                NYdb::EStatus::UNAUTHORIZED, NYdb::EStatus::INTERNAL_ERROR, NYdb::EStatus::ABORTED, NYdb::EStatus::UNAVAILABLE,
                NYdb::EStatus::OVERLOADED, NYdb::EStatus::SCHEME_ERROR, NYdb::EStatus::GENERIC_ERROR, NYdb::EStatus::TIMEOUT,
                NYdb::EStatus::BAD_SESSION, NYdb::EStatus::PRECONDITION_FAILED, NYdb::EStatus::ALREADY_EXISTS,
                NYdb::EStatus::NOT_FOUND, NYdb::EStatus::SESSION_EXPIRED, NYdb::EStatus::CANCELLED, NYdb::EStatus::UNDETERMINED,
                NYdb::EStatus::UNSUPPORTED, NYdb::EStatus::SESSION_BUSY, NYdb::EStatus::EXTERNAL_ERROR,
                NYdb::EStatus::TRANSPORT_UNAVAILABLE, NYdb::EStatus::CLIENT_RESOURCE_EXHAUSTED, NYdb::EStatus::CLIENT_DEADLINE_EXCEEDED,
                NYdb::EStatus::CLIENT_INTERNAL_ERROR, NYdb::EStatus::CLIENT_CANCELLED, NYdb::EStatus::CLIENT_UNAUTHENTICATED,
                NYdb::EStatus::CLIENT_CALL_UNIMPLEMENTED, NYdb::EStatus::CLIENT_OUT_OF_RANGE, NYdb::EStatus::CLIENT_DISCOVERY_FAILED,
                NYdb::EStatus::CLIENT_LIMITS_REACHED, static_cast<NYdb::EStatus>(999999)}) {
            EMutationOutcome expected = EMutationOutcome::Ambiguous;
            if (status == NYdb::EStatus::SUCCESS) {
                expected = EMutationOutcome::Success;
            } else if (status == NYdb::EStatus::ABORTED) {
                expected = EMutationOutcome::GenerationMoved;
            } else if (status == NYdb::EStatus::UNAVAILABLE || status == NYdb::EStatus::PRECONDITION_FAILED) {
                expected = EMutationOutcome::NotAppliedHere;
            } else if (status == NYdb::EStatus::BAD_REQUEST || status == NYdb::EStatus::UNAUTHORIZED) {
                expected = EMutationOutcome::Rejected;
            }
            const auto result = DecodeUpdateResult(Status(status), Rpc(EYdbMethod::Update), {}, TRedactor(TLoadedCredentials{}));
            UNIT_ASSERT(result.Rpc.SdkStatus == status);
            UNIT_ASSERT(ClassifyUpdateOutcome(status) == expected);
            UNIT_ASSERT(result.Outcome == expected);
        }
        UNIT_ASSERT_VALUES_EQUAL(SdkStatusName(NYdb::EStatus::CLIENT_DEADLINE_EXCEEDED), "CLIENT_DEADLINE_EXCEEDED");
        UNIT_ASSERT_VALUES_EQUAL(SdkStatusName(static_cast<NYdb::EStatus>(999999)), "OTHER");
    }

    Y_UNIT_TEST(RedactsTheTokenBeforeTruncationAndKeepsRemoteStatus) {
        TLoadedCredentials credentials;
        credentials.Token = "secret-token";
        const TRedactor redactor(std::make_shared<const TTokenSource>(Nothing(), credentials.Token));
        UNIT_ASSERT_VALUES_EQUAL(redactor.Redact("a secret-token b secret-token"), "a [REDACTED] b [REDACTED]");
        const TString message = TString(4090, 'x') + "secret-token";
        const auto truncated = redactor.Redact(message);
        UNIT_ASSERT_VALUES_EQUAL(truncated.size(), 4096);
        UNIT_ASSERT(!truncated.Contains("secret"));
        NYdb::NIssue::TIssues issues;
        issues.AddIssue(NYdb::NIssue::TIssue("echo secret-token"));
        const auto result = DecodeUpdateResult(NYdb::TStatus(NYdb::EStatus::INTERNAL_ERROR, std::move(issues)), Rpc(EYdbMethod::Update), {}, redactor);
        UNIT_ASSERT_VALUES_EQUAL(result.Rpc.Status, "INTERNAL_ERROR");
        UNIT_ASSERT(!result.Rpc.Issues.Contains("secret-token"));
        UNIT_ASSERT(result.Rpc.Issues.Contains("[REDACTED]"));
    }

    Y_UNIT_TEST(WireSettingsAndOwnedRepliesUsePinnedClients) {
        TWireService service;
        TString endpoint;
        auto server = StartWireServer(service, endpoint);
        UNIT_ASSERT(server);
        // Each client deadline is derived as its operation timeout + 2 s.
        const auto config = ParseConfig("cluster_id: test\nendpoints: {a: ['" + endpoint + "']}\n"
            "timing: {read_timeout_seconds: 3, selfcheck_operation_timeout_seconds: 11, evidence_ttl_seconds: 30, mutation_operation_timeout_seconds: 5}\n");
        TLoadedCredentials credentials;
        credentials.Token = "wire-token";
        std::atomic<ui64> clock{1000000};
        auto clients = CreateYdbClients(config, credentials, [&clock] {
            return TMonotonic::MicroSeconds(clock.fetch_add(1));
        });
        const auto invalid = Await<TStateResult>([&](auto completed) {
            clients->GetState({endpoint, "other-pile", 99}, EYdbMethod::PlainState, std::move(completed));
        });
        UNIT_ASSERT(!invalid.Rpc.Success);
        UNIT_ASSERT(!invalid.Rpc.SentAt);
        UNIT_ASSERT(!invalid.Rpc.SdkStatus);
        UNIT_ASSERT_VALUES_EQUAL(invalid.Error, "INVALID_RPC_REQUEST");
        for (const auto method : {EYdbMethod::PlainState, EYdbMethod::ConnectivityState}) {
            const auto started = std::chrono::system_clock::now();
            const auto state = Await<TStateResult>([&](auto completed) {
                clients->GetState({endpoint, "a", 1}, method, std::move(completed));
            });
            UNIT_ASSERT(state.Rpc.Success);
            UNIT_ASSERT(state.Rpc.Method == method);
            UNIT_ASSERT(state.Rpc.SentAt < state.Rpc.FinishedAt);
            UNIT_ASSERT_VALUES_EQUAL(state.View->SentAt, state.Rpc.SentAt);
            const auto capture = service.ReadCapture();
            UNIT_ASSERT_VALUES_EQUAL(capture.State.include_pile_connectivity(), method == EYdbMethod::ConnectivityState);
            UNIT_ASSERT_VALUES_EQUAL(capture.State.operation_params().operation_timeout().seconds(), 3);
            UNIT_ASSERT_VALUES_EQUAL(capture.AuthTicket, "wire-token");
            AssertDeadline(capture, started, config.Timing.ReadTimeout);
            UNIT_ASSERT_VALUES_EQUAL(state.Connectivity.has_value(), method == EYdbMethod::ConnectivityState);
        }
        const auto healthStarted = std::chrono::system_clock::now();
        const auto health = Await<TSelfCheckResult>([&](auto completed) {
            clients->SelfCheck({endpoint, "a", 2}, std::move(completed));
        });
        UNIT_ASSERT(health.Rpc.Success);
        const auto healthCapture = service.ReadCapture();
        UNIT_ASSERT(!healthCapture.SelfCheck.return_verbose_status());
        UNIT_ASSERT(!healthCapture.SelfCheck.merge_records());
        UNIT_ASSERT(healthCapture.SelfCheck.do_not_cache());
        UNIT_ASSERT(healthCapture.SelfCheck.minimum_status() == Ydb::Monitoring::StatusFlag::UNSPECIFIED);
        UNIT_ASSERT_VALUES_EQUAL(healthCapture.SelfCheck.maximum_level(), 0);
        UNIT_ASSERT_VALUES_EQUAL(healthCapture.SelfCheck.operation_params().operation_timeout().seconds(), 11);
        AssertDeadline(healthCapture, healthStarted, TDuration::Seconds(13));
        const TPlan plan{42, {{"", EPileState::Primary}, {"b", EPileState::Disconnected}}, endpoint};
        const auto updateStarted = std::chrono::system_clock::now();
        const auto update = Await<TUpdateResult>([&](auto completed) {
            clients->Update({endpoint, "a", 3}, plan, {TMonotonic::Max()}, std::move(completed));
        });
        UNIT_ASSERT(update.Rpc.Success);
        UNIT_ASSERT(update.Plan == plan);
        const auto updateCapture = service.ReadCapture();
        UNIT_ASSERT(updateCapture.Update.has_expected_generation());
        UNIT_ASSERT_VALUES_EQUAL(updateCapture.Update.expected_generation(), 42);
        UNIT_ASSERT_VALUES_EQUAL(updateCapture.Update.quorum_piles_size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(updateCapture.Update.updates_size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(updateCapture.Update.updates(0).pile_name(), "");
        UNIT_ASSERT(updateCapture.Update.updates(0).state() == Ydb::Bridge::PileState::PRIMARY);
        UNIT_ASSERT_VALUES_EQUAL(updateCapture.Update.updates(1).pile_name(), "b");
        UNIT_ASSERT(updateCapture.Update.updates(1).state() == Ydb::Bridge::PileState::DISCONNECTED);
        UNIT_ASSERT_VALUES_EQUAL(updateCapture.Update.operation_params().operation_timeout().seconds(), 5);
        AssertDeadline(updateCapture, updateStarted, TDuration::Seconds(7));
        clients->Stop();
        clients.reset();
        UNIT_ASSERT(health.ResponderNodeId == 77);
        UNIT_ASSERT(health.Reply->ResponderPile == "a");
    }

    Y_UNIT_TEST(SelfCheckSlotSettlesAtTheClientDeadlineWhenTheServerNeverAnswers) {
        TWireService service;
        TString endpoint;
        auto server = StartWireServer(service, endpoint);
        UNIT_ASSERT(server);
        // The client deadline is the operation timeout + 2 s; the handler never answers.
        const auto config = ParseConfig("cluster_id: test\nendpoints: {a: ['" + endpoint + "']}\n"
            "timing: {selfcheck_operation_timeout_seconds: 1}\n");
        auto clients = CreateYdbClients(config, TLoadedCredentials{});
        service.HangSelfCheck = true;
        const auto started = std::chrono::system_clock::now();
        const auto hung = Await<TSelfCheckResult>([&](auto completed) {
            clients->SelfCheck({endpoint, "a", 1}, std::move(completed));
        });
        const auto waited = std::chrono::system_clock::now() - started;
        UNIT_ASSERT(!hung.Rpc.Success);
        UNIT_ASSERT(hung.Rpc.SdkStatus == NYdb::EStatus::CLIENT_DEADLINE_EXCEEDED);
        UNIT_ASSERT_VALUES_EQUAL(hung.Rpc.Status, "CLIENT_DEADLINE_EXCEEDED");
        UNIT_ASSERT(!hung.Reply);
        UNIT_ASSERT(!hung.ResponderNodeId);
        UNIT_ASSERT(waited >= std::chrono::milliseconds(2500));
        UNIT_ASSERT(waited <= std::chrono::seconds(8));
        UNIT_ASSERT(hung.Rpc.FinishedAt - hung.Rpc.SentAt >= TDuration::MilliSeconds(2500));
        AssertDeadline(service.ReadCapture(), started, TDuration::Seconds(3));
        // The slot is free again: the next call on the same client succeeds.
        service.HangSelfCheck = false;
        const auto next = Await<TSelfCheckResult>([&](auto completed) {
            clients->SelfCheck({endpoint, "a", 2}, std::move(completed));
        });
        UNIT_ASSERT(next.Rpc.Success);
        UNIT_ASSERT(next.Reply);
        clients->Stop();
        clients.reset();
    }

    Y_UNIT_TEST(TokenFileIsReReadBeforeUpdateAndAfterUnauthorized) {
        TWireService service;
        TString endpoint;
        auto server = StartWireServer(service, endpoint);
        UNIT_ASSERT(server);
        TTempFileHandle token;
        TFileOutput(token.Name()).Write("first\n");
        const auto config = ParseConfig("cluster_id: test\nendpoints: {a: ['" + endpoint + "']}\n"
            "auth: {token_file: '" + token.Name() + "'}\n");
        const auto credentials = LoadCredentials(config);
        std::atomic<ui64> clock{1000000};
        auto clients = CreateYdbClients(config, credentials, [&clock] {
            return TMonotonic::MicroSeconds(clock.fetch_add(1));
        });
        const auto state = [&] {
            return Await<TStateResult>([&](auto completed) {
                clients->GetState({endpoint, "a", 1}, EYdbMethod::PlainState, std::move(completed));
            });
        };
        const TPlan plan{42, {{"a", EPileState::Primary}, {"b", EPileState::Disconnected}}, endpoint};
        const auto update = [&] {
            return Await<TUpdateResult>([&](auto completed) {
                clients->Update({endpoint, "a", 2}, plan, {TMonotonic::Max()}, std::move(completed));
            });
        };
        UNIT_ASSERT(state().Rpc.Success);
        UNIT_ASSERT_VALUES_EQUAL(service.ReadCapture().AuthTicket, "first");
        // Reads do not re-read the file.
        TFileOutput(token.Name()).Write("second\n");
        UNIT_ASSERT(state().Rpc.Success);
        UNIT_ASSERT_VALUES_EQUAL(service.ReadCapture().AuthTicket, "first");
        UNIT_ASSERT(update().Rpc.Success);
        UNIT_ASSERT_VALUES_EQUAL(service.ReadCapture().AuthTicket, "second");
        // The rejected call carried the old token; the next one carries the new.
        TFileOutput(token.Name()).Write("third\n");
        service.NextStatus = Ydb::StatusIds::UNAUTHORIZED;
        const auto unauthorized = state();
        UNIT_ASSERT(unauthorized.Rpc.SdkStatus == NYdb::EStatus::UNAUTHORIZED);
        UNIT_ASSERT_VALUES_EQUAL(service.ReadCapture().AuthTicket, "second");
        UNIT_ASSERT(state().Rpc.Success);
        UNIT_ASSERT_VALUES_EQUAL(service.ReadCapture().AuthTicket, "third");
        // A failed re-read keeps the current token.
        UNIT_ASSERT_VALUES_EQUAL(unlink(token.Name().c_str()), 0);
        UNIT_ASSERT(update().Rpc.Success);
        UNIT_ASSERT_VALUES_EQUAL(service.ReadCapture().AuthTicket, "third");
        clients->Stop();
        clients.reset();
    }
}

} // namespace NBridgeSkipper
