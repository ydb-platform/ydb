#include "../etcd_base_init.h"
#include "../etcd_shared.h"
#include "../etcd_grpc.h"

#include <ydb/public/api/grpc/ydb_query_v1.grpc.pb.h>

#include <ydb/core/protos/config.pb.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/test_client.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/resources/ydb_resources.h>

#include <ydb/library/grpc/client/grpc_client_low.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/logger/backend.h>

namespace NEtcd {

using namespace NActors;
using namespace NKikimr;

namespace {

class TYdbWithGrpcAndRootSchema {
public:
    TYdbWithGrpcAndRootSchema(NKikimrConfig::TAppConfig appConfig = {}, TAutoPtr<TLogBackend> logBackend = {})
    {
        const auto port = PortManager.GetPort(2134);
        const auto grpc = PortManager.GetPort(2135);
        ServerSettings = new Tests::TServerSettings(port);
        ServerSettings->SetGrpcPort(grpc);
        ServerSettings->SetLogBackend(logBackend);
        ServerSettings->SetDomainName("Root");

        ServerSettings->FeatureFlags = appConfig.GetFeatureFlags();
        ServerSettings->RegisterGrpcService<TEtcdKVService>("kv");
        ServerSettings->RegisterGrpcService<TEtcdLeaseService>("lease");
        ServerSettings->Verbose = true;

        Server_.Reset(new Tests::TServer(*ServerSettings));
        Tenants_.Reset(new Tests::TTenants(Server_));

        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::TX_PROXY_SCHEME_CACHE, NActors::NLog::PRI_DEBUG);
        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::SCHEME_BOARD_REPLICA, NActors::NLog::PRI_DEBUG);
        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::SCHEME_BOARD_SUBSCRIBER, NActors::NLog::PRI_TRACE);
        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::SCHEME_BOARD_POPULATOR, NActors::NLog::PRI_DEBUG);
        Server_->GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::TX_PROXY, NActors::NLog::PRI_DEBUG);
        Server_->GetRuntime()->SetLogPriority(NKikimrServices::GRPC_SERVER, NActors::NLog::PRI_DEBUG);
        Server_->GetRuntime()->SetLogPriority(NKikimrServices::GRPC_PROXY, NActors::NLog::PRI_DEBUG);
        Server_->GetRuntime()->SetLogPriority(NKikimrServices::KEYVALUE, NActors::NLog::PRI_DEBUG);
        Server_->GetRuntime()->SetLogPriority(NKikimrServices::BOOTSTRAPPER, NActors::NLog::PRI_DEBUG);
        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::STATESTORAGE, NActors::NLog::PRI_DEBUG);
        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_DEBUG);
        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::SAUSAGE_BIO, NActors::NLog::PRI_DEBUG);
        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::TABLET_FLATBOOT, NActors::NLog::PRI_DEBUG);
        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::TABLET_OPS_HOST, NActors::NLog::PRI_DEBUG);
        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::TABLET_SAUSAGECACHE, NActors::NLog::PRI_DEBUG);
        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::TX_OLAPSHARD, NActors::NLog::PRI_DEBUG);
        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::TX_COLUMNSHARD, NActors::NLog::PRI_DEBUG);

        NYdbGrpc::TServerOptions grpcOption;
        grpcOption.SetPort(grpc);
        Server_->EnableGRpc(grpcOption);

        Tests::TClient(*ServerSettings).InitRootScheme("Root");

        GRpcPort_ = grpc;

        NYdb::TDriverConfig config;
        config.SetEndpoint(TString("localhost:") += ToString(grpc));
        config.SetDatabase("/Root");
        const auto driver = NYdb::TDriver(config);
        NEtcd::TSharedStuff::Get()->Client = std::make_unique<NYdb::NQuery::TQueryClient>(driver);
        NEtcd::TSharedStuff::Get()->Revision.store(1LL);
        NEtcd::TSharedStuff::Get()->Lease.store(1LL);
    }

    ui16 GetPort() {
        return GRpcPort_;
    }

    TPortManager& GetPortManager() {
        return PortManager;
    }

    TTestActorRuntime* GetRuntime() {
        return Server_->GetRuntime();
    }

    Tests::TServer& GetServer() {
        return *Server_;
    }

    Tests::TServerSettings::TPtr ServerSettings;
    Tests::TServer::TPtr Server_;
    THolder<Tests::TTenants> Tenants_;
private:
    TPortManager PortManager;
    ui16 GRpcPort_;
};

void MakeTables(auto &channel) {
    const auto stub = Ydb::Query::V1::QueryService::NewStub(channel);
    Ydb::Query::ExecuteQueryRequest request;
    request.set_exec_mode(Ydb::Query::EXEC_MODE_EXECUTE);
    request.mutable_query_content()->set_text(std::string("PRAGMA TablePathPrefix='/Root';\n") + NEtcd::GetCreateTablesSQL());

    grpc::ClientContext executeCtx;
    Ydb::Query::ExecuteQueryResponsePart response;
    auto reader = stub->ExecuteQuery(&executeCtx, request);
    while (reader->Read(&response)) {
        UNIT_ASSERT_VALUES_EQUAL(response.status(), Ydb::StatusIds::SUCCESS);
    }
}

void MakeSimpleTest(std::function<void(const std::unique_ptr<etcdserverpb::KV::Stub>&)> etcd)
{
    TYdbWithGrpcAndRootSchema server;
    const auto grpc = server.GetPort();
    const auto channel = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());

    MakeTables(channel);

    const std::unique_ptr<etcdserverpb::KV::Stub> stub = etcdserverpb::KV::NewStub(channel);
    etcd(stub);
}

void MakeSimpleTest(std::function<void(const std::unique_ptr<etcdserverpb::KV::Stub>&, const std::unique_ptr<etcdserverpb::Lease::Stub>&)> etcd)
{
    TYdbWithGrpcAndRootSchema server;
    const auto grpc = server.GetPort();
    const auto channel = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());

    MakeTables(channel);

    const std::unique_ptr<etcdserverpb::KV::Stub> kv = etcdserverpb::KV::NewStub(channel);
    const std::unique_ptr<etcdserverpb::Lease::Stub> lease = etcdserverpb::Lease::NewStub(channel);
    etcd(kv, lease);
}

void Put(const std::string_view &key, const std::string_view &value, const std::unique_ptr<etcdserverpb::KV::Stub> &stub, const i64 lease = 0LL)
{
    grpc::ClientContext writeCtx;
    etcdserverpb::PutRequest putRequest;
    putRequest.set_key(key);
    putRequest.set_value(value);
    if (lease)
        putRequest.set_lease(lease);

    etcdserverpb::PutResponse putResponse;
    stub->Put(&writeCtx, putRequest, &putResponse);
    UNIT_ASSERT(!putResponse.has_prev_kv());
}

i64 Delete(const std::string_view &key, const std::unique_ptr<etcdserverpb::KV::Stub> &stub, const std::string_view &rangeEnd = {})
{
    grpc::ClientContext delCtx;
    etcdserverpb::DeleteRangeRequest deleteRangeRequest;
    deleteRangeRequest.set_key(key);
    if (!rangeEnd.empty())
        deleteRangeRequest.set_range_end(rangeEnd);

    etcdserverpb::DeleteRangeResponse deleteRangeResponse;
    stub->DeleteRange(&delCtx, deleteRangeRequest, &deleteRangeResponse);
    UNIT_ASSERT(deleteRangeResponse.prev_kvs().empty());
    return deleteRangeResponse.deleted();
}

i64 Grant(const i64 ttl, const std::unique_ptr<etcdserverpb::Lease::Stub> &stub)
{
    grpc::ClientContext leaseGrantCtx;
    etcdserverpb::LeaseGrantRequest leaseGrantRequest;
    leaseGrantRequest.set_ttl(ttl);

    etcdserverpb::LeaseGrantResponse leaseGrantResponse;
    stub->LeaseGrant(&leaseGrantCtx, leaseGrantRequest, &leaseGrantResponse);

    const auto id = leaseGrantResponse.id();
    UNIT_ASSERT(id != 0LL);
    return id;
}

void Revoke(const i64 id, const std::unique_ptr<etcdserverpb::Lease::Stub> &stub)
{
    grpc::ClientContext leaseRevokeCtx;
    etcdserverpb::LeaseRevokeRequest leaseRevokeRequest;
    leaseRevokeRequest.set_id(id);

    etcdserverpb::LeaseRevokeResponse leaseRevokeResponse;
    stub->LeaseRevoke(&leaseRevokeCtx, leaseRevokeRequest, &leaseRevokeResponse);
}

std::unordered_multiset<i64> Leases(const std::unique_ptr<etcdserverpb::Lease::Stub> &stub)
{
    grpc::ClientContext leasesCtx;
    etcdserverpb::LeaseLeasesRequest leasesRequest;
    etcdserverpb::LeaseLeasesResponse leasesResponse;
    stub->LeaseLeases(&leasesCtx, leasesRequest, &leasesResponse);
    std::unordered_multiset<i64> result(leasesResponse.leases().size());
    for (const auto& l : leasesResponse.leases())
        result.emplace(l.id());
    return result;
}

}

Y_UNIT_TEST_SUITE(Etcd_KV) {
    Y_UNIT_TEST(SimpleWriteReadDelete) {
        MakeSimpleTest([](const std::unique_ptr<etcdserverpb::KV::Stub> &etcd) {
            Put("key0", "value0", etcd);
            Put("key1", "value1", etcd);
            Put("key2", "value2", etcd);
            Put("key3", "value3", etcd);
            Put("key4", "value4", etcd);
            Put("key5", "value5", etcd);
            Put("key6", "value6", etcd);
            Put("key7", "value7", etcd);

            {
                grpc::ClientContext readRangeCtx;
                etcdserverpb::RangeRequest rangeRequest;
                rangeRequest.set_key("key1");
                rangeRequest.set_range_end("key5");

                etcdserverpb::RangeResponse rangeResponse;
                etcd->Range(&readRangeCtx, rangeRequest, &rangeResponse);

                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs().size(), 4U);
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(0).key(), "key1");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(1).key(), "key2");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(2).key(), "key3");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(3).key(), "key4");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(0).value(), "value1");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(1).value(), "value2");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(2).value(), "value3");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(3).value(), "value4");
            }

            Delete("key2", etcd, "key4");

            {
                grpc::ClientContext readRangeCtx;
                etcdserverpb::RangeRequest rangeRequest;
                rangeRequest.set_key("key1");
                rangeRequest.set_range_end("key5");
                rangeRequest.set_keys_only(true);

                etcdserverpb::RangeResponse rangeResponse;
                etcd->Range(&readRangeCtx, rangeRequest, &rangeResponse);

                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs().size(), 2U);
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(0).key(), "key1");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(1).key(), "key4");
            }
        });
    }

    Y_UNIT_TEST(UpdateWithGetPrevious) {
        MakeSimpleTest([](const std::unique_ptr<etcdserverpb::KV::Stub> &etcd) {
            {
                grpc::ClientContext writeCtx;
                etcdserverpb::PutRequest putRequest;
                putRequest.set_key("key");
                putRequest.set_value("value0");
                putRequest.set_prev_kv(true);

                etcdserverpb::PutResponse putResponse;
                etcd->Put(&writeCtx, putRequest, &putResponse);
                UNIT_ASSERT(!putResponse.has_prev_kv());
            }
            {
                grpc::ClientContext writeCtx;
                etcdserverpb::PutRequest putRequest;
                putRequest.set_key("key");
                putRequest.set_value("value1");
                putRequest.set_prev_kv(true);

                etcdserverpb::PutResponse putResponse;
                etcd->Put(&writeCtx, putRequest, &putResponse);
                UNIT_ASSERT(putResponse.has_prev_kv());
                UNIT_ASSERT_VALUES_EQUAL(putResponse.prev_kv().key(), "key");
                UNIT_ASSERT_VALUES_EQUAL(putResponse.prev_kv().value(), "value0");
                UNIT_ASSERT_VALUES_EQUAL(putResponse.prev_kv().version(), 1LL);
            }
            {
                grpc::ClientContext writeCtx;
                etcdserverpb::PutRequest putRequest;
                putRequest.set_key("key");
                putRequest.set_value("value2");
                putRequest.set_prev_kv(true);

                etcdserverpb::PutResponse putResponse;
                etcd->Put(&writeCtx, putRequest, &putResponse);
                UNIT_ASSERT(putResponse.has_prev_kv());
                UNIT_ASSERT_VALUES_EQUAL(putResponse.prev_kv().key(), "key");
                UNIT_ASSERT_VALUES_EQUAL(putResponse.prev_kv().value(), "value1");
                UNIT_ASSERT_VALUES_EQUAL(putResponse.prev_kv().version(), 2LL);
            }

            UNIT_ASSERT_VALUES_EQUAL(Delete("key", etcd), 1LL);

            {
                grpc::ClientContext writeCtx;
                etcdserverpb::PutRequest putRequest;
                putRequest.set_key("key");
                putRequest.set_value("value3");
                putRequest.set_prev_kv(true);

                etcdserverpb::PutResponse putResponse;
                etcd->Put(&writeCtx, putRequest, &putResponse);
                UNIT_ASSERT(!putResponse.has_prev_kv());
            }
            {
                grpc::ClientContext writeCtx;
                etcdserverpb::PutRequest putRequest;
                putRequest.set_key("key");
                putRequest.set_value("value4");
                putRequest.set_prev_kv(true);

                etcdserverpb::PutResponse putResponse;
                etcd->Put(&writeCtx, putRequest, &putResponse);
                UNIT_ASSERT(putResponse.has_prev_kv());
                UNIT_ASSERT_VALUES_EQUAL(putResponse.prev_kv().key(), "key");
                UNIT_ASSERT_VALUES_EQUAL(putResponse.prev_kv().value(), "value3");
                UNIT_ASSERT_VALUES_EQUAL(putResponse.prev_kv().version(), 1LL);
            }
        });
    }

    Y_UNIT_TEST(DeleteWithGetPrevious) {
        MakeSimpleTest([](const std::unique_ptr<etcdserverpb::KV::Stub> &etcd) {
            Put("key0", "value0", etcd);
            Put("key1", "value1", etcd);
            Put("key2", "value2", etcd);
            Put("key3", "value3", etcd);
            Put("key4", "value4", etcd);
            Put("key5", "value5", etcd);
            Put("key6", "value6", etcd);
            Put("key7", "value7", etcd);

            {
                grpc::ClientContext delCtx;
                etcdserverpb::DeleteRangeRequest deleteRangeRequest;
                deleteRangeRequest.set_key("key2");
                deleteRangeRequest.set_prev_kv(true);
                etcdserverpb::DeleteRangeResponse deleteRangeResponse;
                etcd->DeleteRange(&delCtx, deleteRangeRequest, &deleteRangeResponse);
                UNIT_ASSERT_VALUES_EQUAL(deleteRangeResponse.deleted(), 1LL);
                UNIT_ASSERT_VALUES_EQUAL(deleteRangeResponse.prev_kvs().size(), 1U);
                UNIT_ASSERT_VALUES_EQUAL(deleteRangeResponse.prev_kvs(0).key(), "key2");
                UNIT_ASSERT_VALUES_EQUAL(deleteRangeResponse.prev_kvs(0).value(), "value2");
                UNIT_ASSERT_VALUES_EQUAL(deleteRangeResponse.prev_kvs(0).version(), 1LL);
            }

            {
                grpc::ClientContext delCtx;
                etcdserverpb::DeleteRangeRequest deleteRangeRequest;
                deleteRangeRequest.set_key("key4");
                deleteRangeRequest.set_range_end("key7");
                deleteRangeRequest.set_prev_kv(true);
                etcdserverpb::DeleteRangeResponse deleteRangeResponse;
                etcd->DeleteRange(&delCtx, deleteRangeRequest, &deleteRangeResponse);
                UNIT_ASSERT_VALUES_EQUAL(deleteRangeResponse.deleted(), 3LL);
                UNIT_ASSERT_VALUES_EQUAL(deleteRangeResponse.prev_kvs().size(), 3U);

                std::unordered_map<std::string, std::string> map(deleteRangeResponse.prev_kvs().size());
                for (const auto& kvs : deleteRangeResponse.prev_kvs())
                    map.emplace(kvs.key(), kvs.value());

                UNIT_ASSERT_VALUES_EQUAL(map["key4"], "value4");
                UNIT_ASSERT_VALUES_EQUAL(map["key5"], "value5");
                UNIT_ASSERT_VALUES_EQUAL(map["key6"], "value6");
            }

            {
                grpc::ClientContext delCtx;
                etcdserverpb::DeleteRangeRequest deleteRangeRequest;
                deleteRangeRequest.set_key("key");
                deleteRangeRequest.set_range_end("kez");
                deleteRangeRequest.set_prev_kv(true);
                etcdserverpb::DeleteRangeResponse deleteRangeResponse;
                etcd->DeleteRange(&delCtx, deleteRangeRequest, &deleteRangeResponse);
                UNIT_ASSERT_VALUES_EQUAL(deleteRangeResponse.deleted(), 4LL);
                UNIT_ASSERT_VALUES_EQUAL(deleteRangeResponse.prev_kvs().size(), 4U);

                std::unordered_map<std::string, std::string> map(deleteRangeResponse.prev_kvs().size());
                for (const auto& kvs : deleteRangeResponse.prev_kvs())
                    map.emplace(kvs.key(), kvs.value());

                UNIT_ASSERT_VALUES_EQUAL(map["key0"], "value0");
                UNIT_ASSERT_VALUES_EQUAL(map["key1"], "value1");
                UNIT_ASSERT_VALUES_EQUAL(map["key3"], "value3");
                UNIT_ASSERT_VALUES_EQUAL(map["key7"], "value7");
            }
        });
    }
} // Y_UNIT_TEST_SUITE(Etcd_KV)

Y_UNIT_TEST_SUITE(Etcd_Lease) {
    Y_UNIT_TEST(SimpleGrantAndRevoke) {
        MakeSimpleTest([](const std::unique_ptr<etcdserverpb::KV::Stub> &etcd, const std::unique_ptr<etcdserverpb::Lease::Stub> &lease) {
            const auto leaseId = Grant(101LL, lease);

            Put("key0", "value0", etcd);
            Put("key1", "value1", etcd, leaseId);
            Put("key2", "value2", etcd, leaseId);
            Put("key3", "value3", etcd);
            Put("key4", "value4", etcd);
            Put("key5", "value5", etcd, leaseId);
            Put("key6", "value6", etcd, leaseId);
            Put("key7", "value7", etcd);

            {
                grpc::ClientContext readRangeCtx;
                etcdserverpb::RangeRequest rangeRequest;
                rangeRequest.set_key("key");
                rangeRequest.set_range_end("kez");

                etcdserverpb::RangeResponse rangeResponse;
                etcd->Range(&readRangeCtx, rangeRequest, &rangeResponse);

                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs().size(), 8U);
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(0).key(), "key0");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(1).key(), "key1");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(2).key(), "key2");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(3).key(), "key3");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(4).key(), "key4");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(5).key(), "key5");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(6).key(), "key6");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(7).key(), "key7");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(0).value(), "value0");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(1).value(), "value1");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(2).value(), "value2");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(3).value(), "value3");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(4).value(), "value4");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(5).value(), "value5");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(6).value(), "value6");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(7).value(), "value7");
            }

            Revoke(leaseId, lease);

            {
                grpc::ClientContext readRangeCtx;
                etcdserverpb::RangeRequest rangeRequest;
                rangeRequest.set_key("key");
                rangeRequest.set_range_end("kez");
                rangeRequest.set_keys_only(true);

                etcdserverpb::RangeResponse rangeResponse;
                etcd->Range(&readRangeCtx, rangeRequest, &rangeResponse);

                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs().size(), 4U);
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(0).key(), "key0");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(1).key(), "key3");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(2).key(), "key4");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(3).key(), "key7");
            }
        });
    }

    Y_UNIT_TEST(TimeToLive) {
        MakeSimpleTest([](const std::unique_ptr<etcdserverpb::KV::Stub> &etcd, const std::unique_ptr<etcdserverpb::Lease::Stub> &lease) {
            {
                grpc::ClientContext timeToLiveCtx;
                etcdserverpb::LeaseTimeToLiveRequest timeToLiveRequest;
                timeToLiveRequest.set_id(42LL);

                etcdserverpb::LeaseTimeToLiveResponse timeToLiveResponse;
                lease->LeaseTimeToLive(&timeToLiveCtx, timeToLiveRequest, &timeToLiveResponse);

                UNIT_ASSERT_VALUES_EQUAL(timeToLiveResponse.id(), 42LL);
                UNIT_ASSERT_VALUES_EQUAL(timeToLiveResponse.ttl(), -1LL);
                UNIT_ASSERT_VALUES_EQUAL(timeToLiveResponse.grantedttl(), 0LL);
                UNIT_ASSERT(timeToLiveResponse.keys().empty());
            }

            const i64 one = Grant(97LL, lease), two = Grant(17LL, lease);

            Put("key0", "value0", etcd);
            Put("key1", "value1", etcd, two);
            Put("key2", "value2", etcd, one);
            Put("key3", "value3", etcd, one);
            Put("key4", "value4", etcd);
            Put("key5", "value5", etcd, one);
            Put("key6", "value6", etcd, two);
            Put("key7", "value7", etcd);

            {
                grpc::ClientContext timeToLiveCtx;
                etcdserverpb::LeaseTimeToLiveRequest timeToLiveRequest;
                timeToLiveRequest.set_id(one);

                etcdserverpb::LeaseTimeToLiveResponse timeToLiveResponse;
                lease->LeaseTimeToLive(&timeToLiveCtx, timeToLiveRequest, &timeToLiveResponse);

                UNIT_ASSERT_VALUES_EQUAL(timeToLiveResponse.id(), one);
                UNIT_ASSERT_VALUES_EQUAL(timeToLiveResponse.ttl(), 97LL);
                UNIT_ASSERT(timeToLiveResponse.grantedttl() > 0LL && timeToLiveResponse.grantedttl() <= 97LL);
                UNIT_ASSERT(timeToLiveResponse.keys().empty());
            }
            {
                grpc::ClientContext timeToLiveCtx;
                etcdserverpb::LeaseTimeToLiveRequest timeToLiveRequest;
                timeToLiveRequest.set_id(two);
                timeToLiveRequest.set_keys(true);

                etcdserverpb::LeaseTimeToLiveResponse timeToLiveResponse;
                lease->LeaseTimeToLive(&timeToLiveCtx, timeToLiveRequest, &timeToLiveResponse);

                UNIT_ASSERT_VALUES_EQUAL(timeToLiveResponse.id(), two);
                UNIT_ASSERT_VALUES_EQUAL(timeToLiveResponse.ttl(), 17LL);
                UNIT_ASSERT(timeToLiveResponse.grantedttl() > 0LL && timeToLiveResponse.grantedttl() <= 17LL);
                UNIT_ASSERT_VALUES_EQUAL(timeToLiveResponse.keys().size(), 2U);

                const std::unordered_multiset<std::string> set(timeToLiveResponse.keys().cbegin(), timeToLiveResponse.keys().cend());
                UNIT_ASSERT_VALUES_EQUAL(set.count("key1"), 1U);
                UNIT_ASSERT_VALUES_EQUAL(set.count("key6"), 1U);
            }

            Revoke(one, lease);

            {
                grpc::ClientContext timeToLiveCtx;
                etcdserverpb::LeaseTimeToLiveRequest timeToLiveRequest;
                timeToLiveRequest.set_id(one);
                timeToLiveRequest.set_keys(true);

                etcdserverpb::LeaseTimeToLiveResponse timeToLiveResponse;
                lease->LeaseTimeToLive(&timeToLiveCtx, timeToLiveRequest, &timeToLiveResponse);

                UNIT_ASSERT_VALUES_EQUAL(timeToLiveResponse.id(), one);
                UNIT_ASSERT_VALUES_EQUAL(timeToLiveResponse.ttl(), -1LL);
                UNIT_ASSERT_VALUES_EQUAL(timeToLiveResponse.grantedttl(), 0LL);
                UNIT_ASSERT(timeToLiveResponse.keys().empty());
            }
        });
    }

    Y_UNIT_TEST(Leases) {
        MakeSimpleTest([](const std::unique_ptr<etcdserverpb::KV::Stub>&, const std::unique_ptr<etcdserverpb::Lease::Stub> &lease) {
            UNIT_ASSERT(Leases(lease).empty());

            const i64 one = Grant(97LL, lease), two = Grant(17LL, lease);

            {
                const auto& leases = Leases(lease);
                UNIT_ASSERT_VALUES_EQUAL(leases.size(), 2U);
                UNIT_ASSERT_VALUES_EQUAL(leases.count(one), 1U);
                UNIT_ASSERT_VALUES_EQUAL(leases.count(two), 1U);
            }

            Revoke(one, lease);

            {
                const auto& leases = Leases(lease);
                UNIT_ASSERT_VALUES_EQUAL(leases.size(), 1U);
                UNIT_ASSERT_VALUES_EQUAL(leases.count(one), 0U);
                UNIT_ASSERT_VALUES_EQUAL(leases.count(two), 1U);
            }
        });
    }
} // Y_UNIT_TEST_SUITE(Etcd_Lease)

} // NEtcd

