#include "../etcd_base_init.h"
#include "../etcd_shared.h"
#include "../etcd_watch.h"
#include "../grpc_service.h"

#include <ydb/public/api/grpc/ydb_query_v1.grpc.pb.h>

#include <ydb/core/protos/config.pb.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/resources/ydb_resources.h>

#include <ydb/library/grpc/client/grpc_client_low.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/logger/backend.h>

#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>

#include <util/string/builder.h>

namespace NKikimr::NGRpcService {

struct TKikimrTestSettings {
    static constexpr bool SSL = false;
    static constexpr bool AUTH = false;
    static constexpr bool PrecreatePools = true;
    static constexpr bool EnableSystemViews = true;
};

struct TKikimrTestWithAuth : TKikimrTestSettings {
    static constexpr bool AUTH = true;
};

struct TKikimrTestWithAuthAndSsl : TKikimrTestWithAuth {
    static constexpr bool SSL = true;
};

struct TKikimrTestNoSystemViews : TKikimrTestSettings {
    static constexpr bool EnableSystemViews = false;
};

template <typename TestSettings = TKikimrTestSettings>
class TBasicKikimrWithGrpcAndRootSchema {
public:
    TBasicKikimrWithGrpcAndRootSchema(
            NKikimrConfig::TAppConfig appConfig = {},
            TAutoPtr<TLogBackend> logBackend = {})
    {
        const auto port = PortManager.GetPort(2134);
        const auto grpc = PortManager.GetPort(2135);
        ServerSettings = new Tests::TServerSettings(port);
        ServerSettings->SetGrpcPort(grpc);
        ServerSettings->SetLogBackend(logBackend);
        ServerSettings->SetDomainName("Root");

        ServerSettings->FeatureFlags = appConfig.GetFeatureFlags();
        ServerSettings->RegisterGrpcService<NKikimr::NGRpcService::TEtcdKVService>("kv");
        ServerSettings->RegisterGrpcService<NKikimr::NGRpcService::TEtcdWatchService>("watch");
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
        if (TestSettings::AUTH) {
            grpcOption.SetUseAuth(true);
        }
        grpcOption.SetPort(grpc);
        Server_->EnableGRpc(grpcOption);

        Tests::TClient annoyingClient(*ServerSettings);

        annoyingClient.InitRootScheme("Root");

        GRpcPort_ = grpc;

        NYdb::TDriverConfig config;
        config.SetEndpoint(TString("localhost:") += ToString(grpc));
        config.SetDatabase("/Root");
        const auto driver = NYdb::TDriver(config);
        NEtcd::TSharedStuff::Get()->Client = std::make_unique<NYdb::NQuery::TQueryClient>(driver);
        NEtcd::TSharedStuff::Get()->Revision.store(1LL);
    }

    ui16 GetPort() {
        return GRpcPort_;
    }

    TPortManager& GetPortManager() {
        return PortManager;
    }

    void ResetSchemeCache(TString path, ui32 nodeIndex = 0) {
        TTestActorRuntime* runtime = Server_->GetRuntime();
        Tests::TClient annoyingClient(*ServerSettings);
        annoyingClient.RefreshPathCache(runtime, path, nodeIndex);
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

using TKikimrWithGrpcAndRootSchema = TBasicKikimrWithGrpcAndRootSchema<TKikimrTestSettings>;

Y_UNIT_TEST_SUITE(EtcdKV) {
    void MakeTables(auto &channel) {
        const auto stub = Ydb::Query::V1::QueryService::NewStub(channel);
        Ydb::Query::ExecuteQueryRequest request;
        request.set_exec_mode(Ydb::Query::EXEC_MODE_EXECUTE);
        TStringBuilder sql;
        sql << "PRAGMA TablePathPrefix='/Root';" << Endl << NEtcd::GetCreateTablesSQL();
        request.mutable_query_content()->set_text(sql);

        grpc::ClientContext executeCtx;
        Ydb::Query::ExecuteQueryResponsePart response;
        auto reader = stub->ExecuteQuery(&executeCtx, request);
        while (reader->Read(&response)) {
            UNIT_ASSERT_VALUES_EQUAL(response.status(), Ydb::StatusIds::SUCCESS);
        }
    }

    void MakeSimpleTest(std::function<void(const std::unique_ptr<etcdserverpb::KV::Stub>&)> etcd)
    {
        TKikimrWithGrpcAndRootSchema server;
        const auto grpc = server.GetPort();
        const auto channel = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());

        MakeTables(channel);

        const std::unique_ptr<etcdserverpb::KV::Stub> stub = etcdserverpb::KV::NewStub(channel);
        etcd(stub);
    }

    void Write(const TString &key, const TString &value, const std::unique_ptr<etcdserverpb::KV::Stub> &stub)
    {
        grpc::ClientContext writeCtx;
        etcdserverpb::PutRequest putRequest;
        putRequest.set_key(key);
        putRequest.set_value(value);

        etcdserverpb::PutResponse putResponse;
        stub->Put(&writeCtx, putRequest, &putResponse);
    }

    Y_UNIT_TEST(SimpleWriteReadDelete) {
        MakeSimpleTest([](const std::unique_ptr<etcdserverpb::KV::Stub> &etcd) {
            Write("key0", "value0", etcd);
            Write("key1", "value1", etcd);
            Write("key2", "value2", etcd);
            Write("key3", "value3", etcd);
            Write("key4", "value4", etcd);
            Write("key5", "value5", etcd);
            Write("key6", "value6", etcd);
            Write("key7", "value7", etcd);

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
            {
                grpc::ClientContext delCtx;
                etcdserverpb::DeleteRangeRequest deleteRangeRequest;
                deleteRangeRequest.set_key("key2");
                deleteRangeRequest.set_range_end("key4");
                etcdserverpb::DeleteRangeResponse deleteRangeResponse;
                etcd->DeleteRange(&delCtx, deleteRangeRequest, &deleteRangeResponse);
            }
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

} // Y_UNIT_TEST_SUITE(EtcdKV)

} // NKikimr::NGRpcService

