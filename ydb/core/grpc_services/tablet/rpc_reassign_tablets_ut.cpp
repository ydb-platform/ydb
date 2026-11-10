#include "rpc_reassign_tablets.h"

#include <ydb/core/base/hive.h>
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
        bool HasHive = true;
        bool IsDatabase = true;
        bool SharedHive = false;
        bool LookupFails = false;
        bool LoseAcknowledgement = false;
        NKikimrProto::EReplyStatus StopStatus = NKikimrProto::OK;
        ui32 DomainReads = 0;
        ui32 HiveRequests = 0;
        ui32 StopRequests = 0;
        NKikimrHive::TEvReassignTablets LastRequest;
        NKikimrHive::TEvStopReassignTablets LastStopRequest;

        TTestEnv() {
            TServerSettings settings(Ports.GetPort());
            settings.SetDomainName("Root").SetUseRealThreads(false);
            Server = new TServer(settings);
            Server->GetRuntime()->GetAppData().AdministrationAllowedSIDs = {"admins"};
        }

        static TString AdminToken() {
            return NACLib::TUserToken("alice", {"admins"}).SerializeAsString();
        }

        static Ydb::Tablet::ReassignTabletsRequest Request() {
            Ydb::Tablet::ReassignTabletsRequest request;
            request.set_database("/Root/db");
            request.set_storage_pool("test-pool");
            return request;
        }

        TString Id() const {
            return TStringBuilder() << Server->GetRuntime()->GetAppData().DomainsInfo->GetHive() << ":7:42";
        }

        template <typename TEvRequest>
        auto RunRpc(typename TEvRequest::TRequest request, TString token) {
            auto& runtime = *Server->GetRuntime();
            const TPathId domain(Tests::SchemeRoot, 42);
            auto navigate = runtime.AddObserver<TEvTxProxySchemeCache::TEvNavigateKeySet>([&](auto& ev) {
                auto& navigation = ev->Get()->Request;
                if (navigation->ResultSet.size() != 1 || navigation->ResultSet[0].Path != TVector<TString>{"Root", "db"}) {
                    return;
                }
                ++DomainReads;
                auto& entry = navigation->ResultSet[0];
                UNIT_ASSERT(entry.RequestType == NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByPath);
                entry.Status = LookupFails ? NSchemeCache::TSchemeCacheNavigate::EStatus::PathErrorUnknown
                    : NSchemeCache::TSchemeCacheNavigate::EStatus::Ok;
                entry.DomainInfo = new NSchemeCache::TDomainInfo(domain, SharedHive ? TPathId(domain.OwnerId, 43) : domain);
                if (IsDatabase) {
                    entry.DomainDescription = new NSchemeCache::TSchemeCacheNavigate::TDomainDescription();
                }
                if (HasHive) {
                    const ui64 hiveId = runtime.GetAppData().DomainsInfo->GetHive();
                    if (SharedHive) {
                        entry.DomainInfo->SharedHiveId = hiveId;
                    } else {
                        entry.DomainInfo->Params.SetHive(hiveId);
                    }
                }
                runtime.Send(new IEventHandle(ev->Sender, ev->Recipient,
                    new TEvTxProxySchemeCache::TEvNavigateKeySetResult(navigation), 0, ev->Cookie));
                ev.Reset();
            });
            auto reassign = runtime.AddObserver<TEvHive::TEvReassignTablets>([&](auto& ev) {
                ++HiveRequests;
                LastRequest = ev->Get()->Record;
                if (LoseAcknowledgement) {
                    runtime.Send(new IEventHandle(ev->Sender, ev->Recipient, new TEvents::TEvWakeup));
                } else {
                    auto reply = std::make_unique<TEvHive::TEvReassignTabletsReply>();
                    reply->Record.SetStatus(NKikimrProto::OK);
                    reply->Record.SetSelectedTablets(1234);
                    reply->Record.SetHiveGeneration(7);
                    reply->Record.SetReassignId(42);
                    runtime.Send(new IEventHandle(ev->Sender, ev->Recipient, reply.release(), 0, ev->Cookie));
                }
                ev.Reset();
            });
            auto stop = runtime.AddObserver<TEvHive::TEvStopReassignTablets>([&](auto& ev) {
                ++StopRequests;
                LastStopRequest = ev->Get()->Record;
                auto reply = std::make_unique<TEvHive::TEvStopReassignTabletsReply>();
                reply->Record.SetStatus(StopStatus);
                runtime.Send(new IEventHandle(ev->Sender, ev->Recipient, reply.release(), 0, ev->Cookie));
                ev.Reset();
            });
            auto future = NRpcService::DoLocalRpc<TEvRequest>(
                std::move(request), "", token, runtime.GetActorSystem(0));
            return runtime.WaitFuture(std::move(future));
        }

        auto Run(Ydb::Tablet::ReassignTabletsRequest request = Request(), TString token = AdminToken()) {
            return RunRpc<TEvReassignTabletsRequest>(std::move(request), std::move(token));
        }

        auto Stop(const TString& id, TString token = AdminToken()) {
            Ydb::Tablet::StopReassignTabletsRequest request;
            request.set_id(id);
            return RunRpc<TEvStopReassignTabletsRequest>(std::move(request), std::move(token));
        }
    };

    Y_UNIT_TEST(ResolvesDatabaseAndReturnsStoppableId) {
        TTestEnv env;
        auto request = TTestEnv::Request();
        request.set_group(42);
        request.set_max_in_flight(7);
        request.set_percent(40);
        request.set_async(true);
        const auto response = env.Run(request);
        UNIT_ASSERT_VALUES_EQUAL_C(response.status(), Ydb::StatusIds::SUCCESS, response.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(response.selected_tablets(), 1234);
        UNIT_ASSERT_VALUES_EQUAL(response.id(), env.Id());
        UNIT_ASSERT_VALUES_EQUAL(env.DomainReads, 1);
        UNIT_ASSERT_VALUES_EQUAL(env.HiveRequests, 1);
        UNIT_ASSERT_VALUES_EQUAL(env.LastRequest.GetStoragePool(), "test-pool");
        UNIT_ASSERT_VALUES_EQUAL(env.LastRequest.GetGroupId(), 42);
        UNIT_ASSERT_VALUES_EQUAL(env.LastRequest.GetMaxInFlight(), 7);
        UNIT_ASSERT_VALUES_EQUAL(env.LastRequest.GetPercent(), 40);
        UNIT_ASSERT(env.LastRequest.GetAsync());
        UNIT_ASSERT_VALUES_EQUAL(env.LastRequest.GetUserSID(), "alice");

        UNIT_ASSERT_VALUES_EQUAL(env.Stop(response.id()).status(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(env.DomainReads, 1); // Stop routes directly using the returned id.
        UNIT_ASSERT_VALUES_EQUAL(env.StopRequests, 1);
        UNIT_ASSERT_VALUES_EQUAL(env.LastStopRequest.GetReassignId(), 42);
        UNIT_ASSERT_VALUES_EQUAL(env.LastStopRequest.GetHiveGeneration(), 7);
        UNIT_ASSERT_VALUES_EQUAL(env.LastStopRequest.GetUserSID(), "alice");
    }

    Y_UNIT_TEST(SharedHiveAndDefaults) {
        TTestEnv env;
        env.SharedHive = true;
        const auto response = env.Run();
        UNIT_ASSERT_VALUES_EQUAL_C(response.status(), Ydb::StatusIds::SUCCESS, response.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(env.LastRequest.GetMaxInFlight(), 1);
        UNIT_ASSERT_VALUES_EQUAL(env.LastRequest.GetPercent(), 100);
        UNIT_ASSERT_VALUES_EQUAL(env.LastRequest.GetGroupId(), 0);
        UNIT_ASSERT(!env.LastRequest.GetAsync());
    }

    Y_UNIT_TEST(GroupWithoutStoragePool) {
        TTestEnv env;
        auto request = TTestEnv::Request();
        request.clear_storage_pool();
        request.set_group(42);
        const auto response = env.Run(request);
        UNIT_ASSERT_VALUES_EQUAL_C(response.status(), Ydb::StatusIds::SUCCESS, response.DebugString());
        UNIT_ASSERT(env.LastRequest.GetStoragePool().empty());
        UNIT_ASSERT_VALUES_EQUAL(env.LastRequest.GetGroupId(), 42);
        UNIT_ASSERT_VALUES_EQUAL(env.DomainReads, 1);
    }

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
        UNIT_ASSERT_C(TStringBuf(response.id()).StartsWith(ToString(tenantHive) + ":"), response.id());
        Ydb::Tablet::StopReassignTabletsRequest stop;
        stop.set_id(response.id());
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
        const TString rootHivePrefix = ToString(runtime.GetAppData().DomainsInfo->GetHive()) + ":";
        UNIT_ASSERT_C(TStringBuf(rootResponse.id()).StartsWith(rootHivePrefix), rootResponse.id());
    }

    Y_UNIT_TEST(RequiresAdministratorBeforeAnyLookup) {
        TTestEnv env;
        for (const TString& token : {TString(), NACLib::TUserToken("reader", {"monitoring"}).SerializeAsString()}) {
            UNIT_ASSERT_VALUES_EQUAL(env.Run(TTestEnv::Request(), token).status(), Ydb::StatusIds::UNAUTHORIZED);
            UNIT_ASSERT_VALUES_EQUAL(env.Stop(env.Id(), token).status(), Ydb::StatusIds::UNAUTHORIZED);
        }
        UNIT_ASSERT_VALUES_EQUAL(env.DomainReads, 0);
        UNIT_ASSERT_VALUES_EQUAL(env.HiveRequests, 0);
        UNIT_ASSERT_VALUES_EQUAL(env.StopRequests, 0);
    }

    Y_UNIT_TEST(InvalidParametersDoNotStartLookup) {
        TTestEnv env;
        auto request = TTestEnv::Request();
        request.clear_storage_pool();
        UNIT_ASSERT_VALUES_EQUAL(env.Run(request).status(), Ydb::StatusIds::BAD_REQUEST);
        request = TTestEnv::Request();
        request.clear_database();
        UNIT_ASSERT_VALUES_EQUAL(env.Run(request).status(), Ydb::StatusIds::BAD_REQUEST);
        request = TTestEnv::Request();
        request.set_max_in_flight(0);
        UNIT_ASSERT_VALUES_EQUAL(env.Run(request).status(), Ydb::StatusIds::BAD_REQUEST);
        request = TTestEnv::Request();
        request.set_percent(101);
        UNIT_ASSERT_VALUES_EQUAL(env.Run(request).status(), Ydb::StatusIds::BAD_REQUEST);
        for (const TString id : {"", "1:2", "1:2:0", "1:2:3:4", "invalid:2:3"}) {
            UNIT_ASSERT_VALUES_EQUAL(env.Stop(id).status(), Ydb::StatusIds::BAD_REQUEST);
        }
        UNIT_ASSERT_VALUES_EQUAL(env.DomainReads, 0);
        UNIT_ASSERT_VALUES_EQUAL(env.HiveRequests, 0);
        UNIT_ASSERT_VALUES_EQUAL(env.StopRequests, 0);
    }

    Y_UNIT_TEST(UnresolvableDatabasesDoNotReachHive) {
        TTestEnv env;
        env.LookupFails = true;
        UNIT_ASSERT_VALUES_EQUAL(env.Run().status(), Ydb::StatusIds::SCHEME_ERROR);
        env.LookupFails = false;
        env.HasHive = false;
        env.SharedHive = true;
        UNIT_ASSERT_VALUES_EQUAL(env.Run().status(), Ydb::StatusIds::PRECONDITION_FAILED);
        env.IsDatabase = false;
        UNIT_ASSERT_VALUES_EQUAL(env.Run().status(), Ydb::StatusIds::BAD_REQUEST);
        UNIT_ASSERT_VALUES_EQUAL(env.HiveRequests, 0);
    }

    Y_UNIT_TEST(StopFinishedJob) {
        TTestEnv env;
        env.StopStatus = NKikimrProto::NODATA;
        UNIT_ASSERT_VALUES_EQUAL(env.Stop(env.Id()).status(), Ydb::StatusIds::NOT_FOUND);
        UNIT_ASSERT_VALUES_EQUAL(env.StopRequests, 1);
        UNIT_ASSERT_VALUES_EQUAL(env.DomainReads, 0);
    }

    Y_UNIT_TEST(LostAcknowledgementDoesNotRetry) {
        TTestEnv env;
        env.LoseAcknowledgement = true;
        UNIT_ASSERT_VALUES_EQUAL(env.Run().status(), Ydb::StatusIds::UNDETERMINED);
        UNIT_ASSERT_VALUES_EQUAL(env.HiveRequests, 1);
    }
}

} // namespace NKikimr::NGRpcService
