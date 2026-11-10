#include "rpc_reassign_tablets.h"

#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/public/api/protos/ydb_cms.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NGRpcService {

using namespace Tests;

Y_UNIT_TEST_SUITE(TabletService_ReassignTablets) {
    struct TTestEnv {
        TPortManager Ports;
        TServer::TPtr Server;

        TTestEnv() {
            TServerSettings settings(Ports.GetPort());
            settings.SetDomainName("Root").SetUseRealThreads(false);
            Server = new TServer(settings);
            Server->GetRuntime()->GetAppData().AdministrationAllowedSIDs = {"admins"};
        }

        template <typename TEvRequest>
        auto Run(typename TEvRequest::TRequest request,
                 TString token = NACLib::TUserToken("alice", {"admins"}).SerializeAsString()) {
            auto& runtime = *Server->GetRuntime();
            return runtime.WaitFuture(NRpcService::DoLocalRpc<TEvRequest>(
                std::move(request), "", token, runtime.GetActorSystem(0)));
        }
    };

    Y_UNIT_TEST(ResolvesRealDatabaseHives) {
        TPortManager ports;
        TServerSettings settings(ports.GetPort());
        settings.SetDomainName("Root")
            .SetNodeCount(1)
            .SetDynamicNodeCount(1)
            .SetUseRealThreads(true)
            .AddStoragePoolType("ssd");
        TServer::TPtr server = new TServer(settings);
        TTenants tenants(server);
        Ydb::Cms::CreateDatabaseRequest create;
        create.set_path("/Root/reassign");
        auto* storage = create.mutable_resources()->add_storage_units();
        storage->set_unit_kind("ssd");
        storage->set_count(1);
        tenants.CreateTenant(std::move(create), 1, TDuration::Minutes(1));

        auto& runtime = *server->GetRuntime();
        const auto sender = runtime.AllocateEdgeActor();
        auto navigate = std::make_unique<NSchemeCache::TSchemeCacheNavigate>();
        auto& entry = navigate->ResultSet.emplace_back();
        entry.Path = {"Root", "reassign"};
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
        runtime.Send(new IEventHandle(MakeSchemeCacheID(), sender,
            new TEvTxProxySchemeCache::TEvNavigateKeySet(navigate.release())));
        TAutoPtr<IEventHandle> handle;
        const auto* reply = runtime.GrabEdgeEventRethrow<TEvTxProxySchemeCache::TEvNavigateKeySetResult>(handle);
        const auto& domain = reply->Request->ResultSet.front();
        UNIT_ASSERT(domain.Status == NSchemeCache::TSchemeCacheNavigate::EStatus::Ok);
        UNIT_ASSERT(domain.DomainInfo && domain.DomainInfo->ExtractHive());
        const ui64 tenantHive = domain.DomainInfo->ExtractHive();
        UNIT_ASSERT_VALUES_UNEQUAL(tenantHive, runtime.GetAppData().DomainsInfo->GetHive());
        UNIT_ASSERT(domain.DomainDescription);
        UNIT_ASSERT_VALUES_EQUAL(domain.DomainDescription->Description.StoragePoolsSize(), 1);

        Ydb::Tablet::ReassignTabletsRequest request;
        request.set_database("/Root/reassign");
        request.set_storage_pool(domain.DomainDescription->Description.GetStoragePools(0).GetName());
        request.set_percent(0);
        const auto response = NRpcService::DoLocalRpc<TEvReassignTabletsRequest>(
            std::move(request), "", "", runtime.GetActorSystem(0)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(response.status(), Ydb::StatusIds::SUCCESS, response.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(response.selected_tablets(), 0);
        UNIT_ASSERT_VALUES_EQUAL(response.id().hive_id(), tenantHive);
        UNIT_ASSERT(response.id().hive_generation());
        UNIT_ASSERT(response.id().reassign_id());
        Ydb::Tablet::StopReassignTabletsRequest stop;
        *stop.mutable_id() = response.id();
        const auto stopped = NRpcService::DoLocalRpc<TEvStopReassignTabletsRequest>(
            std::move(stop), "", "", runtime.GetActorSystem(0)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(stopped.status(), Ydb::StatusIds::NOT_FOUND); // Empty job has finished.

        // Root database metadata has no explicit Hive; it uses the configured root Hive.
        Ydb::Tablet::ReassignTabletsRequest rootRequest;
        rootRequest.set_database("/Root");
        rootRequest.set_storage_pool(domain.DomainDescription->Description.GetStoragePools(0).GetName());
        rootRequest.set_percent(0);
        const auto rootResponse = NRpcService::DoLocalRpc<TEvReassignTabletsRequest>(
            std::move(rootRequest), "", "", runtime.GetActorSystem(0)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(rootResponse.status(), Ydb::StatusIds::SUCCESS, rootResponse.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(rootResponse.id().hive_id(), runtime.GetAppData().DomainsInfo->GetHive());
    }

    Y_UNIT_TEST(RequiresAdministrator) {
        TTestEnv env;
        Ydb::Tablet::ReassignTabletsRequest start;
        start.set_database("/Root/db");
        start.set_storage_pool("test-pool");
        Ydb::Tablet::StopReassignTabletsRequest stop;
        stop.mutable_id()->set_hive_id(1);
        stop.mutable_id()->set_hive_generation(2);
        stop.mutable_id()->set_reassign_id(3);
        for (const TString& token : {TString(), NACLib::TUserToken("reader", {"monitoring"}).SerializeAsString()}) {
            UNIT_ASSERT_VALUES_EQUAL(env.Run<TEvReassignTabletsRequest>(start, token).status(), Ydb::StatusIds::UNAUTHORIZED);
            UNIT_ASSERT_VALUES_EQUAL(env.Run<TEvStopReassignTabletsRequest>(stop, token).status(), Ydb::StatusIds::UNAUTHORIZED);
        }
    }

    Y_UNIT_TEST(InvalidRequests) {
        TTestEnv env;
        Ydb::Tablet::ReassignTabletsRequest start;
        start.set_storage_pool("test-pool");
        UNIT_ASSERT_VALUES_EQUAL(env.Run<TEvReassignTabletsRequest>(start).status(), Ydb::StatusIds::BAD_REQUEST);
        start.set_database("/Root/db");
        start.clear_storage_pool();
        UNIT_ASSERT_VALUES_EQUAL(env.Run<TEvReassignTabletsRequest>(start).status(), Ydb::StatusIds::BAD_REQUEST);
        start.set_storage_pool("test-pool");
        start.set_max_in_flight(0);
        UNIT_ASSERT_VALUES_EQUAL(env.Run<TEvReassignTabletsRequest>(start).status(), Ydb::StatusIds::BAD_REQUEST);
        start.clear_max_in_flight();
        start.set_percent(101);
        UNIT_ASSERT_VALUES_EQUAL(env.Run<TEvReassignTabletsRequest>(start).status(), Ydb::StatusIds::BAD_REQUEST);
        Ydb::Tablet::StopReassignTabletsRequest stop;
        UNIT_ASSERT_VALUES_EQUAL(env.Run<TEvStopReassignTabletsRequest>(stop).status(), Ydb::StatusIds::BAD_REQUEST);
        stop.mutable_id()->set_hive_id(1);
        UNIT_ASSERT_VALUES_EQUAL(env.Run<TEvStopReassignTabletsRequest>(stop).status(), Ydb::StatusIds::BAD_REQUEST);
        stop.mutable_id()->set_hive_generation(2);
        UNIT_ASSERT_VALUES_EQUAL(env.Run<TEvStopReassignTabletsRequest>(stop).status(), Ydb::StatusIds::BAD_REQUEST);
    }
}

} // namespace NKikimr::NGRpcService
