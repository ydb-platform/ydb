#include "../etcd_base_init.h"
#include "../etcd_shared.h"
#include "../etcd_gate.h"
#include "../etcd_grpc.h"
#include "../etcd_lease.h"

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

        const auto stuff = std::make_shared<TSharedStuff>();
        ServerSettings->RegisterGrpcService<TEtcdKVService>("kv", TActorId(), stuff);
        ServerSettings->RegisterGrpcService<TEtcdLeaseService>("lease", TActorId(), stuff);
        ServerSettings->Verbose = true;

        Server_.Reset(new Tests::TServer(*ServerSettings));

        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::TX_PROXY_SCHEME_CACHE, NActors::NLog::PRI_DEBUG);
        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::SCHEME_BOARD_REPLICA, NActors::NLog::PRI_DEBUG);
        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::SCHEME_BOARD_SUBSCRIBER, NActors::NLog::PRI_TRACE);
        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::SCHEME_BOARD_POPULATOR, NActors::NLog::PRI_DEBUG);
        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::TX_PROXY, NActors::NLog::PRI_DEBUG);
        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::GRPC_SERVER, NActors::NLog::PRI_DEBUG);
        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::GRPC_PROXY, NActors::NLog::PRI_DEBUG);
        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::KEYVALUE, NActors::NLog::PRI_DEBUG);
        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::BOOTSTRAPPER, NActors::NLog::PRI_DEBUG);
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
        stuff->Client = std::make_unique<NYdb::NQuery::TQueryClient>(driver);

        GetRuntime()->Register(BuildMainGate({}, stuff));
        GetRuntime()->Register(BuildHolderHouse({}, stuff));
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
private:
    TPortManager PortManager;
    ui16 GRpcPort_;
};

void Execute(const std::string &query, const std::unique_ptr<Ydb::Query::V1::QueryService::Stub> &service) {
    grpc::ClientContext executeCtx;
    Ydb::Query::ExecuteQueryResponsePart response;

    Ydb::Query::ExecuteQueryRequest request;
    request.set_exec_mode(Ydb::Query::EXEC_MODE_EXECUTE);
    request.mutable_query_content()->set_text(query);
    for (const auto reader = service->ExecuteQuery(&executeCtx, request); reader->Read(&response);) {
        UNIT_ASSERT_VALUES_EQUAL(response.status(), Ydb::StatusIds::SUCCESS);
    }
}

void MakeTables(auto &channel) {
    const auto stub = Ydb::Query::V1::QueryService::NewStub(channel);
    const std::string prefix("PRAGMA TablePathPrefix='/Root';\n");
    Execute(NEtcd::GetCreateTablesSQL(prefix), stub);
    Execute(NEtcd::GetInitializeTablesSQL(prefix), stub);
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

i64 Put(std::string_view key, std::optional<std::string_view> value, const std::unique_ptr<etcdserverpb::KV::Stub> &stub, const std::optional<i64> lease = 0LL)
{
    grpc::ClientContext writeCtx;
    etcdserverpb::PutRequest putRequest;
    putRequest.set_key(key);

    if (!value)
        putRequest.set_ignore_value(true);
    else if (!value->empty())
        putRequest.set_value(*value);

    if (!lease)
        putRequest.set_ignore_lease(true);
    else if (const auto l = *lease)
        putRequest.set_lease(l);

    etcdserverpb::PutResponse putResponse;
    UNIT_ASSERT(stub->Put(&writeCtx, putRequest, &putResponse).ok());
    UNIT_ASSERT(!putResponse.has_prev_kv());
    return putResponse.header().revision();
}

std::string Get(std::string_view key, const std::unique_ptr<etcdserverpb::KV::Stub> &stub)
{
    grpc::ClientContext readRangeCtx;
    etcdserverpb::RangeRequest rangeRequest;
    rangeRequest.set_key(key);

    etcdserverpb::RangeResponse rangeResponse;
    UNIT_ASSERT(stub->Range(&readRangeCtx, rangeRequest, &rangeResponse).ok());

    UNIT_ASSERT_VALUES_EQUAL(rangeResponse.count(), 1LL);
    UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs().size(), 1U);
    UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(0).key(), key);
    return rangeResponse.kvs(0).value();
}

i64 Delete(const std::string_view &key, const std::unique_ptr<etcdserverpb::KV::Stub> &stub, const std::string_view &rangeEnd = {})
{
    grpc::ClientContext delCtx;
    etcdserverpb::DeleteRangeRequest deleteRangeRequest;
    deleteRangeRequest.set_key(key);
    if (!rangeEnd.empty())
        deleteRangeRequest.set_range_end(rangeEnd);

    etcdserverpb::DeleteRangeResponse deleteRangeResponse;
    UNIT_ASSERT(stub->DeleteRange(&delCtx, deleteRangeRequest, &deleteRangeResponse).ok());
    UNIT_ASSERT(deleteRangeResponse.prev_kvs().empty());
    return deleteRangeResponse.deleted();
}

i64 Grant(const i64 ttl, const std::unique_ptr<etcdserverpb::Lease::Stub> &stub)
{
    grpc::ClientContext leaseGrantCtx;
    etcdserverpb::LeaseGrantRequest leaseGrantRequest;
    leaseGrantRequest.set_ttl(ttl);

    etcdserverpb::LeaseGrantResponse leaseGrantResponse;
    UNIT_ASSERT(stub->LeaseGrant(&leaseGrantCtx, leaseGrantRequest, &leaseGrantResponse).ok());

    const auto id = leaseGrantResponse.id();
    UNIT_ASSERT(id != 0LL);
    return id;
}

bool Revoke(const i64 id, const std::unique_ptr<etcdserverpb::Lease::Stub> &stub)
{
    grpc::ClientContext leaseRevokeCtx;
    etcdserverpb::LeaseRevokeRequest leaseRevokeRequest;
    leaseRevokeRequest.set_id(id);

    etcdserverpb::LeaseRevokeResponse leaseRevokeResponse;
    return stub->LeaseRevoke(&leaseRevokeCtx, leaseRevokeRequest, &leaseRevokeResponse).ok();
}

std::unordered_multiset<i64> Leases(const std::unique_ptr<etcdserverpb::Lease::Stub> &stub)
{
    grpc::ClientContext leasesCtx;
    etcdserverpb::LeaseLeasesRequest leasesRequest;
    etcdserverpb::LeaseLeasesResponse leasesResponse;
    UNIT_ASSERT(stub->LeaseLeases(&leasesCtx, leasesRequest, &leasesResponse).ok());
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
                UNIT_ASSERT(etcd->Range(&readRangeCtx, rangeRequest, &rangeResponse).ok());

                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.count(), 4LL);
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs().size(), 4U);
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(0).key(), "key1");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(1).key(), "key2");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(2).key(), "key3");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(3).key(), "key4");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(0).value(), "value1");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(1).value(), "value2");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(2).value(), "value3");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(3).value(), "value4");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(0).version(), 1L);
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(1).version(), 1L);
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(2).version(), 1L);
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(3).version(), 1L);
            }

            Delete("key2", etcd, "key4");

            {
                grpc::ClientContext readRangeCtx;
                etcdserverpb::RangeRequest rangeRequest;
                rangeRequest.set_key("key1");
                rangeRequest.set_range_end("key5");
                rangeRequest.set_keys_only(true);

                etcdserverpb::RangeResponse rangeResponse;
                UNIT_ASSERT(etcd->Range(&readRangeCtx, rangeRequest, &rangeResponse).ok());

                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.count(), 2LL);
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs().size(), 2U);
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(0).key(), "key1");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(1).key(), "key4");
            }
        });
    }

    Y_UNIT_TEST(ReadRangeWithLimit) {
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
                rangeRequest.set_range_end("\0"sv);
                rangeRequest.set_limit(4LL);

                etcdserverpb::RangeResponse rangeResponse;
                UNIT_ASSERT(etcd->Range(&readRangeCtx, rangeRequest, &rangeResponse).ok());

                UNIT_ASSERT(rangeResponse.more());
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.count(), 7LL);
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
        });
    }

    Y_UNIT_TEST(ReadRangeWithLimitAndSort) {
        MakeSimpleTest([](const std::unique_ptr<etcdserverpb::KV::Stub> &etcd) {
            Put("key0", "value7", etcd);
            Put("key1", "value6", etcd);
            Put("key2", "value5", etcd);
            Put("key3", "value4", etcd);
            Put("key4", "value3", etcd);
            Put("key5", "value2", etcd);
            Put("key6", "value1", etcd);
            Put("key7", "value0", etcd);

            {
                grpc::ClientContext readRangeCtx;
                etcdserverpb::RangeRequest rangeRequest;
                rangeRequest.set_key("key1");
                rangeRequest.set_range_end("key6");
                rangeRequest.set_limit(3LL);
                rangeRequest.set_sort_target(etcdserverpb::RangeRequest_SortTarget_VALUE);
                rangeRequest.set_sort_order(etcdserverpb::RangeRequest_SortOrder_ASCEND);
                etcdserverpb::RangeResponse rangeResponse;
                UNIT_ASSERT(etcd->Range(&readRangeCtx, rangeRequest, &rangeResponse).ok());

                UNIT_ASSERT(rangeResponse.more());
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.count(), 5LL);
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs().size(), 3U);
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(0).key(), "key5");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(1).key(), "key4");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(2).key(), "key3");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(0).value(), "value2");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(1).value(), "value3");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(2).value(), "value4");
            }

            {
                grpc::ClientContext readRangeCtx;
                etcdserverpb::RangeRequest rangeRequest;
                rangeRequest.set_key("key");
                rangeRequest.set_range_end("kez");
                rangeRequest.set_limit(3LL);
                rangeRequest.set_sort_target(etcdserverpb::RangeRequest_SortTarget_KEY);
                rangeRequest.set_sort_order(etcdserverpb::RangeRequest_SortOrder_DESCEND);
                rangeRequest.set_keys_only(true);
                etcdserverpb::RangeResponse rangeResponse;
                UNIT_ASSERT(etcd->Range(&readRangeCtx, rangeRequest, &rangeResponse).ok());

                UNIT_ASSERT(rangeResponse.more());
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.count(), 8LL);
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs().size(), 3U);
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(0).key(), "key7");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(1).key(), "key6");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(2).key(), "key5");
                UNIT_ASSERT(rangeResponse.kvs(0).value().empty());
                UNIT_ASSERT(rangeResponse.kvs(1).value().empty());
                UNIT_ASSERT(rangeResponse.kvs(2).value().empty());
            }
        });
    }

    Y_UNIT_TEST(ReadRangeFromRevision) {
        MakeSimpleTest([](const std::unique_ptr<etcdserverpb::KV::Stub> &etcd) {
            Put("kek0", "value7", etcd);
            Put("kek1", "value6", etcd);
            Put("kek2", "value5", etcd);
            UNIT_ASSERT_VALUES_EQUAL(Delete("kek0", etcd), 1LL);
            UNIT_ASSERT_VALUES_EQUAL(Delete("kek1", etcd), 1LL);
            Put("kek0", "value4", etcd);
            Put("kek3", "value3", etcd);
            const auto first = Put("abc", "first", etcd);
            Put("kek1", "value9", etcd);
            UNIT_ASSERT_VALUES_EQUAL(Delete("kek0", etcd), 1LL);
            UNIT_ASSERT_VALUES_EQUAL(Delete("kek2", etcd), 1LL);
            Put("kek6", "value2", etcd);
            Put("kek7", "value1", etcd);
            const auto second = Put("xyz", "second", etcd);
            Put("kek8", "value0", etcd);
            Put("kek7", "unused", etcd);

            {
                grpc::ClientContext readRangeCtx;
                etcdserverpb::RangeRequest rangeRequest;
                rangeRequest.set_key("kek");
                rangeRequest.set_range_end("kek9");
                rangeRequest.set_revision(first);
                rangeRequest.set_sort_target(etcdserverpb::RangeRequest_SortTarget_VALUE);
                rangeRequest.set_sort_order(etcdserverpb::RangeRequest_SortOrder_DESCEND);
                etcdserverpb::RangeResponse rangeResponse;
                UNIT_ASSERT(etcd->Range(&readRangeCtx, rangeRequest, &rangeResponse).ok());

                UNIT_ASSERT(!rangeResponse.more());
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.count(), 3LL);
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs().size(), 3U);
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(0).key(), "kek2");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(1).key(), "kek0");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(2).key(), "kek3");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(0).value(), "value5");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(1).value(), "value4");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(2).value(), "value3");
            }

            {
                grpc::ClientContext readRangeCtx;
                etcdserverpb::RangeRequest rangeRequest;
                rangeRequest.set_key("kek");
                rangeRequest.set_range_end("kek9");
                rangeRequest.set_revision(second);
                rangeRequest.set_sort_target(etcdserverpb::RangeRequest_SortTarget_CREATE);
                rangeRequest.set_sort_order(etcdserverpb::RangeRequest_SortOrder_DESCEND);
                etcdserverpb::RangeResponse rangeResponse;
                UNIT_ASSERT(etcd->Range(&readRangeCtx, rangeRequest, &rangeResponse).ok());

                UNIT_ASSERT(!rangeResponse.more());
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.count(), 4LL);
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs().size(), 4U);
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(0).key(), "kek7");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(1).key(), "kek6");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(2).key(), "kek1");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(3).key(), "kek3");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(0).value(), "value1");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(1).value(), "value2");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(2).value(), "value9");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(3).value(), "value3");
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
                UNIT_ASSERT(etcd->Put(&writeCtx, putRequest, &putResponse).ok());
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
                UNIT_ASSERT(etcd->Put(&writeCtx, putRequest, &putResponse).ok());
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
                UNIT_ASSERT(etcd->Put(&writeCtx, putRequest, &putResponse).ok());
                UNIT_ASSERT(!putResponse.has_prev_kv());
            }
            {
                grpc::ClientContext writeCtx;
                etcdserverpb::PutRequest putRequest;
                putRequest.set_key("key");
                putRequest.set_value("value4");
                putRequest.set_prev_kv(true);

                etcdserverpb::PutResponse putResponse;
                UNIT_ASSERT(etcd->Put(&writeCtx, putRequest, &putResponse).ok());
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
                UNIT_ASSERT(etcd->DeleteRange(&delCtx, deleteRangeRequest, &deleteRangeResponse).ok());
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
                UNIT_ASSERT(etcd->DeleteRange(&delCtx, deleteRangeRequest, &deleteRangeResponse).ok());
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
                UNIT_ASSERT(etcd->DeleteRange(&delCtx, deleteRangeRequest, &deleteRangeResponse).ok());
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

    Y_UNIT_TEST(UpdateWithIgnoreValue) {
        MakeSimpleTest([](const std::unique_ptr<etcdserverpb::KV::Stub> &etcd) {
            {
                grpc::ClientContext writeCtx;
                etcdserverpb::PutRequest putRequest;
                putRequest.set_key("my_key");
                putRequest.set_ignore_value(true);

                etcdserverpb::PutResponse putResponse;
                UNIT_ASSERT(!etcd->Put(&writeCtx, putRequest, &putResponse).ok());
            }

            Put("my_key", "my_val", etcd);

            {
                grpc::ClientContext readRangeCtx;
                etcdserverpb::RangeRequest rangeRequest;
                rangeRequest.set_key("my_key");

                etcdserverpb::RangeResponse rangeResponse;
                UNIT_ASSERT(etcd->Range(&readRangeCtx, rangeRequest, &rangeResponse).ok());

                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs().size(), 1U);
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(0).key(), "my_key");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(0).value(), "my_val");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(0).version(), 1LL);
            }

            Put("my_key", std::nullopt, etcd);

            {
                grpc::ClientContext readRangeCtx;
                etcdserverpb::RangeRequest rangeRequest;
                rangeRequest.set_key("my_key");

                etcdserverpb::RangeResponse rangeResponse;
                UNIT_ASSERT(etcd->Range(&readRangeCtx, rangeRequest, &rangeResponse).ok());

                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.count(), 1LL);
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs().size(), 1U);
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(0).key(), "my_key");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(0).value(), "my_val");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(0).version(), 2LL);
            }
         });
    }

    Y_UNIT_TEST(TxnWithoutCompares) {
        MakeSimpleTest([](const std::unique_ptr<etcdserverpb::KV::Stub> &etcd) {
            Put("kk0", "vv0", etcd);
            Put("kk1", "vv1", etcd);
            Put("kk2", "vv2", etcd);
            Put("kk3", "vv3", etcd);

            {
                grpc::ClientContext txnCtx;
                etcdserverpb::TxnRequest txnRequest;

                const auto put1 = txnRequest.add_success()->mutable_request_put();
                put1->set_key("kk5");
                put1->set_value("vv5");

                const auto del = txnRequest.add_success()->mutable_request_delete_range();
                del->set_key("kk1");
                del->set_range_end("kk3");

                const auto put2 = txnRequest.add_success()->mutable_request_put();
                put2->set_key("kk4");
                put2->set_value("vv4");

                etcdserverpb::TxnResponse txnResponse;
                UNIT_ASSERT(etcd->Txn(&txnCtx, txnRequest, &txnResponse).ok());

                UNIT_ASSERT(txnResponse.succeeded());
                UNIT_ASSERT_VALUES_EQUAL(txnResponse.responses(1).response_delete_range().deleted(), 2U);
            }

            {
                grpc::ClientContext readRangeCtx;
                etcdserverpb::RangeRequest rangeRequest;
                rangeRequest.set_key("kk");
                rangeRequest.set_range_end("kl");
                rangeRequest.set_sort_target(etcdserverpb::RangeRequest_SortTarget_KEY);
                rangeRequest.set_sort_order(etcdserverpb::RangeRequest_SortOrder_ASCEND);
                rangeRequest.set_keys_only(true);
                etcdserverpb::RangeResponse rangeResponse;
                UNIT_ASSERT(etcd->Range(&readRangeCtx, rangeRequest, &rangeResponse).ok());

                UNIT_ASSERT(!rangeResponse.more());
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.count(), 4LL);
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs().size(), 4U);
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(0).key(), "kk0");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(1).key(), "kk3");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(2).key(), "kk4");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(3).key(), "kk5");
            }
         });
    }

    Y_UNIT_TEST(OptimisticCreate) {
        MakeSimpleTest([](const std::unique_ptr<etcdserverpb::KV::Stub> &etcd) {
            const auto key = "new_key"sv;
            const auto val = "new_value"sv;
            {
                grpc::ClientContext txnCtx;
                etcdserverpb::TxnRequest txnRequest;

                const auto compare = txnRequest.add_compare();
                compare->set_result(etcdserverpb::Compare_CompareResult_EQUAL);
                compare->set_target(etcdserverpb::Compare_CompareTarget_MOD);
                compare->set_key(key);
                compare->set_mod_revision(0LL);

                const auto put = txnRequest.add_success()->mutable_request_put();
                put->set_key(key);
                put->set_value(val);

                etcdserverpb::TxnResponse txnResponse;
                UNIT_ASSERT(etcd->Txn(&txnCtx, txnRequest, &txnResponse).ok());

                UNIT_ASSERT(txnResponse.succeeded());
            }

            UNIT_ASSERT_VALUES_EQUAL(Get(key, etcd), val);

            {
                grpc::ClientContext txnCtx;
                etcdserverpb::TxnRequest txnRequest;

                const auto compare = txnRequest.add_compare();
                compare->set_result(etcdserverpb::Compare_CompareResult_EQUAL);
                compare->set_target(etcdserverpb::Compare_CompareTarget_MOD);
                compare->set_key(key);
                compare->set_mod_revision(0LL);

                const auto put = txnRequest.add_success()->mutable_request_put();
                put->set_key(key);
                put->set_value("-----");

                etcdserverpb::TxnResponse txnResponse;
                UNIT_ASSERT(etcd->Txn(&txnCtx, txnRequest, &txnResponse).ok());

                UNIT_ASSERT(!txnResponse.succeeded());
            }

            UNIT_ASSERT_VALUES_EQUAL(Get(key, etcd), val);
            UNIT_ASSERT_VALUES_EQUAL(Delete(key, etcd), 1LL);

            {
                grpc::ClientContext txnCtx;
                etcdserverpb::TxnRequest txnRequest;

                const auto compare = txnRequest.add_compare();
                compare->set_result(etcdserverpb::Compare_CompareResult_EQUAL);
                compare->set_target(etcdserverpb::Compare_CompareTarget_MOD);
                compare->set_key(key);
                compare->set_mod_revision(0LL);

                const auto put = txnRequest.add_success()->mutable_request_put();
                put->set_key(key);
                put->set_value(val);

                etcdserverpb::TxnResponse txnResponse;
                UNIT_ASSERT(etcd->Txn(&txnCtx, txnRequest, &txnResponse).ok());

                UNIT_ASSERT(txnResponse.succeeded());
            }

            UNIT_ASSERT_VALUES_EQUAL(Get(key, etcd), val);
         });
    }

    Y_UNIT_TEST(OptimisticUpdate) {
        MakeSimpleTest([](const std::unique_ptr<etcdserverpb::KV::Stub> &etcd) {
            const auto key = "other_key"sv;
            const auto first = Put(key, "eerste", etcd);
            const auto second = Put(key, "twede", etcd);

            {
                grpc::ClientContext txnCtx;
                etcdserverpb::TxnRequest txnRequest;

                const auto compare = txnRequest.add_compare();
                compare->set_result(etcdserverpb::Compare_CompareResult_EQUAL);
                compare->set_target(etcdserverpb::Compare_CompareTarget_MOD);
                compare->set_key(key);
                compare->set_mod_revision(first);

                const auto put = txnRequest.add_success()->mutable_request_put();
                put->set_key(key);
                put->set_value("changed_value");

                const auto range = txnRequest.add_failure()->mutable_request_range();
                range->set_key(key);

                etcdserverpb::TxnResponse txnResponse;
                UNIT_ASSERT(etcd->Txn(&txnCtx, txnRequest, &txnResponse).ok());

                UNIT_ASSERT(!txnResponse.succeeded());
                const auto& resp = txnResponse.responses(0).response_range();
                UNIT_ASSERT_VALUES_EQUAL(resp.kvs().size(), 1U);
                UNIT_ASSERT_VALUES_EQUAL(resp.kvs(0).key(), key);
                UNIT_ASSERT_VALUES_EQUAL(resp.kvs(0).value(), "twede");
                UNIT_ASSERT_VALUES_EQUAL(resp.kvs(0).version(), 2LL);
                UNIT_ASSERT_VALUES_EQUAL(resp.kvs(0).create_revision(), first);
                UNIT_ASSERT_VALUES_EQUAL(resp.kvs(0).mod_revision(), second);
            }

            {
                grpc::ClientContext txnCtx;
                etcdserverpb::TxnRequest txnRequest;

                const auto compare = txnRequest.add_compare();
                compare->set_result(etcdserverpb::Compare_CompareResult_EQUAL);
                compare->set_target(etcdserverpb::Compare_CompareTarget_MOD);
                compare->set_key(key);
                compare->set_mod_revision(second);

                const auto put = txnRequest.add_success()->mutable_request_put();
                put->set_key(key);
                put->set_value("changed_value");

                const auto range = txnRequest.add_failure()->mutable_request_range();
                range->set_key(key);

                etcdserverpb::TxnResponse txnResponse;
                UNIT_ASSERT(etcd->Txn(&txnCtx, txnRequest, &txnResponse).ok());

                UNIT_ASSERT(txnResponse.succeeded());
            }

            UNIT_ASSERT_VALUES_EQUAL(Get(key, etcd), "changed_value");
         });
    }

    Y_UNIT_TEST(OptimisticDelete) {
        MakeSimpleTest([](const std::unique_ptr<etcdserverpb::KV::Stub> &etcd) {
            const auto key = "useless_key"sv;
            const auto first = Put(key, "eerste", etcd);
            const auto second = Put(key, "twede", etcd);

            {
                grpc::ClientContext txnCtx;
                etcdserverpb::TxnRequest txnRequest;

                const auto compare = txnRequest.add_compare();
                compare->set_result(etcdserverpb::Compare_CompareResult_EQUAL);
                compare->set_target(etcdserverpb::Compare_CompareTarget_MOD);
                compare->set_key(key);
                compare->set_mod_revision(first);

                const auto put = txnRequest.add_success()->mutable_request_delete_range();
                put->set_key(key);

                const auto range = txnRequest.add_failure()->mutable_request_range();
                range->set_key(key);

                etcdserverpb::TxnResponse txnResponse;
                UNIT_ASSERT(etcd->Txn(&txnCtx, txnRequest, &txnResponse).ok());

                UNIT_ASSERT(!txnResponse.succeeded());
                const auto& resp = txnResponse.responses(0).response_range();
                UNIT_ASSERT_VALUES_EQUAL(resp.kvs().size(), 1U);
                UNIT_ASSERT_VALUES_EQUAL(resp.kvs(0).key(), key);
                UNIT_ASSERT_VALUES_EQUAL(resp.kvs(0).value(), "twede");
                UNIT_ASSERT_VALUES_EQUAL(resp.kvs(0).version(), 2LL);
                UNIT_ASSERT_VALUES_EQUAL(resp.kvs(0).create_revision(), first);
                UNIT_ASSERT_VALUES_EQUAL(resp.kvs(0).mod_revision(), second);
            }

            {
                grpc::ClientContext txnCtx;
                etcdserverpb::TxnRequest txnRequest;

                const auto compare = txnRequest.add_compare();
                compare->set_result(etcdserverpb::Compare_CompareResult_EQUAL);
                compare->set_target(etcdserverpb::Compare_CompareTarget_MOD);
                compare->set_key(key);
                compare->set_mod_revision(second);

                const auto put = txnRequest.add_success()->mutable_request_delete_range();
                put->set_key(key);

                const auto range = txnRequest.add_failure()->mutable_request_range();
                range->set_key(key);

                etcdserverpb::TxnResponse txnResponse;
                UNIT_ASSERT(etcd->Txn(&txnCtx, txnRequest, &txnResponse).ok());

                UNIT_ASSERT(txnResponse.succeeded());
                UNIT_ASSERT_VALUES_EQUAL(txnResponse.responses(0).response_delete_range().deleted(), 1LL);
            }
         });
    }

    Y_UNIT_TEST(TxnReadOnly) {
        MakeSimpleTest([](const std::unique_ptr<etcdserverpb::KV::Stub> &etcd) {
            Put("one", "first", etcd);
            Put("two", "second", etcd);
            Put("three", "third", etcd);

            {
                grpc::ClientContext txnCtx;
                etcdserverpb::TxnRequest txnRequest;

                {
                    const auto compare = txnRequest.add_compare();
                    compare->set_result(etcdserverpb::Compare_CompareResult_EQUAL);
                    compare->set_target(etcdserverpb::Compare_CompareTarget_VALUE);
                    compare->set_key("three");
                    compare->set_value("3");
                }

                txnRequest.add_success()->mutable_request_range()->set_key("one");
                txnRequest.add_failure()->mutable_request_range()->set_key("two");

                etcdserverpb::TxnResponse txnResponse;
                UNIT_ASSERT(etcd->Txn(&txnCtx, txnRequest, &txnResponse).ok());

                UNIT_ASSERT(!txnResponse.succeeded());

                const auto& resp = txnResponse.responses(0).response_range();
                UNIT_ASSERT_VALUES_EQUAL(resp.kvs().size(), 1U);
                UNIT_ASSERT_VALUES_EQUAL(resp.kvs(0).key(), "two");
                UNIT_ASSERT_VALUES_EQUAL(resp.kvs(0).value(), "second");
            }

            Put("three", "3", etcd);

            {
                grpc::ClientContext txnCtx;
                etcdserverpb::TxnRequest txnRequest;

                {
                    const auto compare = txnRequest.add_compare();
                    compare->set_result(etcdserverpb::Compare_CompareResult_EQUAL);
                    compare->set_target(etcdserverpb::Compare_CompareTarget_VALUE);
                    compare->set_key("three");
                    compare->set_value("3");
                }

                txnRequest.add_success()->mutable_request_range()->set_key("one");
                txnRequest.add_failure()->mutable_request_range()->set_key("two");

                etcdserverpb::TxnResponse txnResponse;
                UNIT_ASSERT(etcd->Txn(&txnCtx, txnRequest, &txnResponse).ok());

                UNIT_ASSERT(txnResponse.succeeded());

                const auto& resp = txnResponse.responses(0).response_range();
                UNIT_ASSERT_VALUES_EQUAL(resp.kvs().size(), 1U);
                UNIT_ASSERT_VALUES_EQUAL(resp.kvs(0).key(), "one");
                UNIT_ASSERT_VALUES_EQUAL(resp.kvs(0).value(), "first");
            }

            Delete("three", etcd);

            {
                grpc::ClientContext txnCtx;
                etcdserverpb::TxnRequest txnRequest;

                {
                    const auto compare = txnRequest.add_compare();
                    compare->set_result(etcdserverpb::Compare_CompareResult_EQUAL);
                    compare->set_target(etcdserverpb::Compare_CompareTarget_VALUE);
                    compare->set_key("three");
                    compare->set_value("3");
                }

                txnRequest.add_success()->mutable_request_range()->set_key("one");
                txnRequest.add_failure()->mutable_request_range()->set_key("two");

                etcdserverpb::TxnResponse txnResponse;
                UNIT_ASSERT(etcd->Txn(&txnCtx, txnRequest, &txnResponse).ok());

                UNIT_ASSERT(!txnResponse.succeeded());

                const auto& resp = txnResponse.responses(0).response_range();
                UNIT_ASSERT_VALUES_EQUAL(resp.kvs().size(), 1U);
                UNIT_ASSERT_VALUES_EQUAL(resp.kvs(0).key(), "two");
                UNIT_ASSERT_VALUES_EQUAL(resp.kvs(0).value(), "second");
            }
        });
    }

    Y_UNIT_TEST(TxnMultiKeysAndOperations) {
        MakeSimpleTest([](const std::unique_ptr<etcdserverpb::KV::Stub> &etcd) {
            Put("one", "first", etcd);
            Put("two", "second", etcd);
            Put("three", "third", etcd);

            {
                grpc::ClientContext txnCtx;
                etcdserverpb::TxnRequest txnRequest;

                {
                    const auto compare = txnRequest.add_compare();
                    compare->set_result(etcdserverpb::Compare_CompareResult_LESS);
                    compare->set_target(etcdserverpb::Compare_CompareTarget_VALUE);
                    compare->set_key("one");
                    compare->set_value("ff");
                }

                {
                    const auto compare = txnRequest.add_compare();
                    compare->set_result(etcdserverpb::Compare_CompareResult_GREATER);
                    compare->set_target(etcdserverpb::Compare_CompareTarget_VALUE);
                    compare->set_key("two");
                    compare->set_value("tt");
                }

                {
                    const auto compare = txnRequest.add_compare();
                    compare->set_result(etcdserverpb::Compare_CompareResult_NOT_EQUAL);
                    compare->set_target(etcdserverpb::Compare_CompareTarget_VALUE);
                    compare->set_key("three");
                    compare->set_value("xx");
                }

                {
                    const auto put = txnRequest.add_success()->mutable_request_put();
                    put->set_key("one");
                    put->set_value("1");
                }

                {
                    const auto put = txnRequest.add_success()->mutable_request_range();
                    put->set_key("two");
                }

                {
                    const auto put = txnRequest.add_success()->mutable_request_delete_range();
                    put->set_key("three");
                }

                {
                    const auto put = txnRequest.add_failure()->mutable_request_put();
                    put->set_key("one");
                    put->set_value("eerste");
                    put->set_prev_kv(true);
                }

                {
                    const auto put = txnRequest.add_failure()->mutable_request_put();
                    put->set_key("two");
                    put->set_value("twede");
                    put->set_prev_kv(true);
                }

                {
                    const auto put = txnRequest.add_failure()->mutable_request_put();
                    put->set_key("three");
                    put->set_value("driede");
                    put->set_prev_kv(true);
                }

                etcdserverpb::TxnResponse txnResponse;
                UNIT_ASSERT(etcd->Txn(&txnCtx, txnRequest, &txnResponse).ok());

                UNIT_ASSERT(!txnResponse.succeeded());

                UNIT_ASSERT_VALUES_EQUAL(txnResponse.responses(0).response_put().prev_kv().value(), "first");
                UNIT_ASSERT_VALUES_EQUAL(txnResponse.responses(1).response_put().prev_kv().value(), "second");
                UNIT_ASSERT_VALUES_EQUAL(txnResponse.responses(2).response_put().prev_kv().value(), "third");
            }

            {
                grpc::ClientContext txnCtx;
                etcdserverpb::TxnRequest txnRequest;

                {
                    const auto compare = txnRequest.add_compare();
                    compare->set_result(etcdserverpb::Compare_CompareResult_LESS);
                    compare->set_target(etcdserverpb::Compare_CompareTarget_VALUE);
                    compare->set_key("one");
                    compare->set_value("ff");
                }

                {
                    const auto compare = txnRequest.add_compare();
                    compare->set_result(etcdserverpb::Compare_CompareResult_GREATER);
                    compare->set_target(etcdserverpb::Compare_CompareTarget_VALUE);
                    compare->set_key("two");
                    compare->set_value("tt");
                }

                {
                    const auto compare = txnRequest.add_compare();
                    compare->set_result(etcdserverpb::Compare_CompareResult_NOT_EQUAL);
                    compare->set_target(etcdserverpb::Compare_CompareTarget_VALUE);
                    compare->set_key("three");
                    compare->set_value("xx");
                }

                {
                    const auto put = txnRequest.add_success()->mutable_request_put();
                    put->set_key("one");
                    put->set_value("1");
                }

                {
                    const auto put = txnRequest.add_success()->mutable_request_range();
                    put->set_key("two");
                }

                {
                    const auto put = txnRequest.add_success()->mutable_request_delete_range();
                    put->set_key("three");
                }

                {
                    const auto range = txnRequest.add_failure()->mutable_request_range();
                    range->set_key("one");
                    range->set_range_end("txn");
                }

                etcdserverpb::TxnResponse txnResponse;
                UNIT_ASSERT(etcd->Txn(&txnCtx, txnRequest, &txnResponse).ok());

                UNIT_ASSERT(txnResponse.succeeded());
            }
        });
    }

    Y_UNIT_TEST(Compact) {
        MakeSimpleTest([](const std::unique_ptr<etcdserverpb::KV::Stub> &etcd) {
            Put("key0", "value0", etcd);
            Put("key3", "value1", etcd);
            Put("key2", "value2", etcd);
            Put("key0", "value3", etcd);
            Put("key1", "value4", etcd);
            Delete("key2", etcd);
            const auto revForCompact = Put("key3", "value5", etcd);
            Delete("key1", etcd);
            const auto revForRequest = Put("key3", "value6", etcd);
            Delete("key0", etcd);
            Delete("key3", etcd);

            {
                grpc::ClientContext readRangeCtx;
                etcdserverpb::RangeRequest rangeRequest;
                rangeRequest.set_key("key");
                rangeRequest.set_range_end("kez");
                rangeRequest.set_keys_only(true);
                etcdserverpb::RangeResponse rangeResponse;
                UNIT_ASSERT(etcd->Range(&readRangeCtx, rangeRequest, &rangeResponse).ok());
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.count(), 0LL);
            }

            {
                grpc::ClientContext compactCtx;
                etcdserverpb::CompactionRequest compactionRequest;
                compactionRequest.set_revision(revForCompact);
                compactionRequest.set_physical(true);
                etcdserverpb::CompactionResponse compactionResponse;
                UNIT_ASSERT(etcd->Compact(&compactCtx, compactionRequest, &compactionResponse).ok());
            }

            {
                grpc::ClientContext readRangeCtx;
                etcdserverpb::RangeRequest rangeRequest;
                rangeRequest.set_key("key");
                rangeRequest.set_range_end("kez");
                rangeRequest.set_revision(revForRequest);
                rangeRequest.set_sort_target(etcdserverpb::RangeRequest_SortTarget_VALUE);
                rangeRequest.set_sort_order(etcdserverpb::RangeRequest_SortOrder_ASCEND);
                etcdserverpb::RangeResponse rangeResponse;
                UNIT_ASSERT(etcd->Range(&readRangeCtx, rangeRequest, &rangeResponse).ok());
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.count(), 2LL);
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs().size(), 2U);
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(0).key(), "key0");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(1).key(), "key3");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(0).value(), "value3");
                UNIT_ASSERT_VALUES_EQUAL(rangeResponse.kvs(1).value(), "value6");
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
                UNIT_ASSERT(etcd->Range(&readRangeCtx, rangeRequest, &rangeResponse).ok());

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

            UNIT_ASSERT(Revoke(leaseId, lease));

            {
                grpc::ClientContext readRangeCtx;
                etcdserverpb::RangeRequest rangeRequest;
                rangeRequest.set_key("key");
                rangeRequest.set_range_end("kez");
                rangeRequest.set_keys_only(true);

                etcdserverpb::RangeResponse rangeResponse;
                UNIT_ASSERT(etcd->Range(&readRangeCtx, rangeRequest, &rangeResponse).ok());

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
                UNIT_ASSERT(lease->LeaseTimeToLive(&timeToLiveCtx, timeToLiveRequest, &timeToLiveResponse).ok());

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
                UNIT_ASSERT(lease->LeaseTimeToLive(&timeToLiveCtx, timeToLiveRequest, &timeToLiveResponse).ok());

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
                UNIT_ASSERT(lease->LeaseTimeToLive(&timeToLiveCtx, timeToLiveRequest, &timeToLiveResponse).ok());

                UNIT_ASSERT_VALUES_EQUAL(timeToLiveResponse.id(), two);
                UNIT_ASSERT_VALUES_EQUAL(timeToLiveResponse.ttl(), 17LL);
                UNIT_ASSERT(timeToLiveResponse.grantedttl() > 0LL && timeToLiveResponse.grantedttl() <= 17LL);
                UNIT_ASSERT_VALUES_EQUAL(timeToLiveResponse.keys().size(), 2U);

                const std::unordered_multiset<std::string> set(timeToLiveResponse.keys().cbegin(), timeToLiveResponse.keys().cend());
                UNIT_ASSERT_VALUES_EQUAL(set.count("key1"), 1U);
                UNIT_ASSERT_VALUES_EQUAL(set.count("key6"), 1U);
            }

            UNIT_ASSERT(Revoke(one, lease));

            {
                grpc::ClientContext timeToLiveCtx;
                etcdserverpb::LeaseTimeToLiveRequest timeToLiveRequest;
                timeToLiveRequest.set_id(one);
                timeToLiveRequest.set_keys(true);

                etcdserverpb::LeaseTimeToLiveResponse timeToLiveResponse;
                UNIT_ASSERT(lease->LeaseTimeToLive(&timeToLiveCtx, timeToLiveRequest, &timeToLiveResponse).ok());

                UNIT_ASSERT_VALUES_EQUAL(timeToLiveResponse.id(), one);
                UNIT_ASSERT_VALUES_EQUAL(timeToLiveResponse.ttl(), -1LL);
                UNIT_ASSERT_VALUES_EQUAL(timeToLiveResponse.grantedttl(), 0LL);
                UNIT_ASSERT(timeToLiveResponse.keys().empty());
            }

            UNIT_ASSERT(!Revoke(one, lease));
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

            UNIT_ASSERT(Revoke(one, lease));

            {
                const auto& leases = Leases(lease);
                UNIT_ASSERT_VALUES_EQUAL(leases.size(), 1U);
                UNIT_ASSERT_VALUES_EQUAL(leases.count(one), 0U);
                UNIT_ASSERT_VALUES_EQUAL(leases.count(two), 1U);
            }

            UNIT_ASSERT(!Revoke(one, lease));
        });
    }
} // Y_UNIT_TEST_SUITE(Etcd_Lease)

} // NEtcd

