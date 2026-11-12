#include "rpc_reassign_tablets.h"

#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/public/api/protos/ydb_cms.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/operation_id/operation_id.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NGRpcService {

using namespace Tests;
using NOperationId::TOperationId;
using TEvGetOperationRequest = TGrpcRequestOperationCall<Ydb::Operations::GetOperationRequest, Ydb::Operations::GetOperationResponse>;
using TEvCancelOperationRequest = TGrpcRequestNoOperationCall<Ydb::Operations::CancelOperationRequest, Ydb::Operations::CancelOperationResponse>;

TString MakeReassignOperationId() {
    TOperationId id;
    id.SetKind(TOperationId::REASSIGN_TABLETS);
    id.AddOptionalValue("hive_id", "1");
    id.AddOptionalValue("hive_generation", "2");
    id.AddOptionalValue("reassign_id", "3");
    return id.ToString();
}

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
                 const TString& database = "/Root/db",
                 TString token = NACLib::TUserToken("alice", {"admins"}).SerializeAsString()) {
            auto& runtime = *Server->GetRuntime();
            return runtime.WaitFuture(NRpcService::DoLocalRpc<TEvRequest>(
                std::move(request), database, token, runtime.GetActorSystem(0)));
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
        request.set_storage_pool(domain.DomainDescription->Description.GetStoragePools(0).GetName());
        request.set_percent(0);
        const auto response = NRpcService::DoLocalRpc<TEvReassignTabletsRequest>(
            std::move(request), "/Root/reassign", "", runtime.GetActorSystem(0)).ExtractValueSync();
        UNIT_ASSERT(!response.operation().ready());
        Ydb::Tablet::ReassignTabletsMetadata metadata;
        UNIT_ASSERT(response.operation().metadata().UnpackTo(&metadata));
        UNIT_ASSERT_VALUES_EQUAL(metadata.selected_tablets(), 0);
        const TOperationId id(response.operation().id());
        UNIT_ASSERT(id.GetKind() == TOperationId::REASSIGN_TABLETS);
        UNIT_ASSERT_VALUES_EQUAL(*id.GetValue("hive_id").at(0), ToString(tenantHive));

        Ydb::Operations::GetOperationRequest get;
        get.set_id(response.operation().id());
        const auto finished = NRpcService::DoLocalRpc<TEvGetOperationRequest>(
            Ydb::Operations::GetOperationRequest(get), "", "", runtime.GetActorSystem(0)).ExtractValueSync();
        UNIT_ASSERT(finished.operation().ready());
        UNIT_ASSERT_VALUES_EQUAL(finished.operation().status(), Ydb::StatusIds::SUCCESS);
        Ydb::Operations::CancelOperationRequest cancel;
        cancel.set_id(get.id());
        const auto cancelled = NRpcService::DoLocalRpc<TEvCancelOperationRequest>(
            std::move(cancel), "", "", runtime.GetActorSystem(0)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(cancelled.status(), Ydb::StatusIds::SUCCESS); // Already finished: no change.
        const auto afterCancel = NRpcService::DoLocalRpc<TEvGetOperationRequest>(
            std::move(get), "", "", runtime.GetActorSystem(0)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(afterCancel.operation().status(), Ydb::StatusIds::SUCCESS);

        // Root database metadata has no explicit Hive; it uses the configured root Hive.
        Ydb::Tablet::ReassignTabletsRequest rootRequest;
        rootRequest.set_storage_pool(domain.DomainDescription->Description.GetStoragePools(0).GetName());
        rootRequest.set_percent(0);
        const auto rootResponse = NRpcService::DoLocalRpc<TEvReassignTabletsRequest>(
            std::move(rootRequest), "/Root", "", runtime.GetActorSystem(0)).ExtractValueSync();
        UNIT_ASSERT(!rootResponse.operation().ready());
        UNIT_ASSERT_VALUES_EQUAL(*TOperationId(rootResponse.operation().id()).GetValue("hive_id").at(0),
            ToString(runtime.GetAppData().DomainsInfo->GetHive()));
    }

    Y_UNIT_TEST(RequiresAdministrator) {
        TTestEnv env;
        Ydb::Tablet::ReassignTabletsRequest start;
        start.set_storage_pool("test-pool");
        Ydb::Operations::GetOperationRequest get;
        get.set_id(MakeReassignOperationId());
        Ydb::Operations::CancelOperationRequest cancel;
        cancel.set_id(get.id());
        for (const TString& token : {TString(), NACLib::TUserToken("reader", {"monitoring"}).SerializeAsString()}) {
            UNIT_ASSERT_VALUES_EQUAL(env.Run<TEvReassignTabletsRequest>(start, "/Root/db", token).operation().status(), Ydb::StatusIds::UNAUTHORIZED);
            UNIT_ASSERT_VALUES_EQUAL(env.Run<TEvGetOperationRequest>(get, "/Root/db", token).operation().status(), Ydb::StatusIds::UNAUTHORIZED);
            UNIT_ASSERT_VALUES_EQUAL(env.Run<TEvCancelOperationRequest>(cancel, "/Root/db", token).status(), Ydb::StatusIds::UNAUTHORIZED);
        }
    }

    Y_UNIT_TEST(InvalidRequests) {
        TTestEnv env;
        Ydb::Tablet::ReassignTabletsRequest start;
        start.set_storage_pool("test-pool");
        UNIT_ASSERT_VALUES_EQUAL(env.Run<TEvReassignTabletsRequest>(start, "").operation().status(), Ydb::StatusIds::BAD_REQUEST);
        start.clear_storage_pool();
        UNIT_ASSERT_VALUES_EQUAL(env.Run<TEvReassignTabletsRequest>(start).operation().status(), Ydb::StatusIds::BAD_REQUEST);
        start.set_storage_pool("test-pool");
        start.set_max_in_flight(0);
        UNIT_ASSERT_VALUES_EQUAL(env.Run<TEvReassignTabletsRequest>(start).operation().status(), Ydb::StatusIds::BAD_REQUEST);
        start.clear_max_in_flight();
        start.set_percent(101);
        UNIT_ASSERT_VALUES_EQUAL(env.Run<TEvReassignTabletsRequest>(start).operation().status(), Ydb::StatusIds::BAD_REQUEST);
        const TString prefix = "ydb://reassigntablets/17?";
        for (const TString& id : {TString(), TString("invalid"), prefix,
                prefix + "hive_id=0&hive_generation=2&reassign_id=3",
                prefix + "hive_id=1&hive_id=2&hive_generation=2&reassign_id=3",
                prefix + "hive_id=1&hive_generation=bad&reassign_id=3"}) {
            Ydb::Operations::GetOperationRequest get;
            get.set_id(id);
            Ydb::Operations::CancelOperationRequest cancel;
            cancel.set_id(id);
            UNIT_ASSERT_VALUES_EQUAL(env.Run<TEvGetOperationRequest>(get).operation().status(), Ydb::StatusIds::BAD_REQUEST);
            UNIT_ASSERT_VALUES_EQUAL(env.Run<TEvCancelOperationRequest>(cancel).status(), Ydb::StatusIds::BAD_REQUEST);
        }
    }
}

} // namespace NKikimr::NGRpcService
