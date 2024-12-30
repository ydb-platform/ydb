#include "../grpc_service.h"
#include <ydb/services/keyvalue/grpc_service.h>

#include <ydb/public/api/grpc/ydb_keyvalue_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_etcd_v1.grpc.pb.h>

#include <ydb/core/keyvalue/keyvalue.h>
#include <ydb/core/keyvalue/keyvalue_events.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <ydb/public/api/grpc/ydb_scheme_v1.grpc.pb.h>

#include <ydb/public/sdk/cpp/client/resources/ydb_resources.h>

#include <ydb/library/grpc/client/grpc_client_low.h>


#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/logger/backend.h>

#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>

#include <util/string/builder.h>


#define UNIT_ASSERT_CHECK_STATUS(got, exp) \
    UNIT_ASSERT_C(got.status() == exp, "exp# " << Ydb::StatusIds::StatusCode_Name(exp) \
            << " got# " << Ydb::StatusIds::StatusCode_Name(got.status()) << " issues# "  << got.issues()) \
// UNIT_ASSERT_CHECK_STATUS

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
        ServerSettings->SetDynamicNodeCount(1);
        if (TestSettings::PrecreatePools) {
            ServerSettings->AddStoragePool("ssd");
            ServerSettings->AddStoragePool("hdd");
            ServerSettings->AddStoragePool("hdd1");
            ServerSettings->AddStoragePool("hdd2");
        } else {
            ServerSettings->AddStoragePoolType("ssd");
            ServerSettings->AddStoragePoolType("hdd");
            ServerSettings->AddStoragePoolType("hdd1");
            ServerSettings->AddStoragePoolType("hdd2");
        }
        ServerSettings->Formats = new TFormatFactory;
        ServerSettings->FeatureFlags = appConfig.GetFeatureFlags();
        ServerSettings->RegisterGrpcService<NKikimr::NGRpcService::TEtcdGRpcService>("etcd");
        ServerSettings->RegisterGrpcService<NKikimr::NGRpcService::TKeyValueGRpcService>("keyvalue");
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
        if (ServerSettings->AppConfig->GetDomainsConfig().GetSecurityConfig().GetEnforceUserTokenRequirement()) {
            annoyingClient.SetSecurityToken("root@builtin");
        }
        annoyingClient.InitRootScheme("Root");
        GRpcPort_ = grpc;
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

Y_UNIT_TEST_SUITE(EtcdGRPCService) {

    void InitTablet(TKikimrWithGrpcAndRootSchema &server, ui64 tabletId) {
        server.GetRuntime()->SetScheduledLimit(100);
        CreateTestBootstrapper(*server.GetRuntime(),
            CreateTestTabletInfo(tabletId, TTabletTypes::KeyValue),
            &CreateKeyValueFlat);
        NanoSleep(3'000'000'000);
    }

    void CmdWrite(ui64 tabletId, const TDeque<TString> &keys, const TDeque<TString> &values, TKikimrWithGrpcAndRootSchema &server)
    {
        Y_ABORT_UNLESS(keys.size() == values.size());
        TAutoPtr<IEventHandle> handle;
        TEvKeyValue::TEvResponse *result;
        THolder<TEvKeyValue::TEvRequest> request;
        TActorId edgeActor = server.GetRuntime()->AllocateEdgeActor();
        for (i32 retriesLeft = 2; retriesLeft > 0; --retriesLeft) {
            try {
                server.GetRuntime()->ResetScheduledCount();
                request.Reset(new TEvKeyValue::TEvRequest);
                for (ui64 idx = 0; idx < keys.size(); ++idx) {
                    auto write = request->Record.AddCmdWrite();
                    write->SetKey(keys[idx]);
                    write->SetValue(values[idx]);
                    write->SetStorageChannel(NKikimrClient::TKeyValueRequest::MAIN);
                    write->SetPriority(NKikimrClient::TKeyValueRequest::REALTIME);
                }
                server.GetRuntime()->SendToPipe(tabletId, edgeActor, request.Release(), 0, GetPipeConfigWithRetries());
                result = server.GetRuntime()->GrabEdgeEvent<TEvKeyValue::TEvResponse>(handle);
                UNIT_ASSERT(result);
                UNIT_ASSERT(result->Record.HasStatus());
                UNIT_ASSERT_EQUAL(result->Record.GetStatus(), NMsgBusProxy::MSTATUS_OK);
                UNIT_ASSERT_VALUES_EQUAL(result->Record.WriteResultSize(), values.size());
                for (ui64 idx = 0; idx < values.size(); ++idx) {
                    const auto &writeResult = result->Record.GetWriteResult(idx);
                    UNIT_ASSERT(writeResult.HasStatus());
                    UNIT_ASSERT_EQUAL(writeResult.GetStatus(), NKikimrProto::OK);
                    UNIT_ASSERT(writeResult.HasStatusFlags());
                    if (values[idx].size()) {
                        UNIT_ASSERT(writeResult.GetStatusFlags() & ui32(NKikimrBlobStorage::StatusIsValid));
                    }
                }
                retriesLeft = 0;
            } catch (NActors::TSchedulingLimitReachedException) {
                UNIT_ASSERT(retriesLeft == 2);
            }
        }
    }

    template <typename TCtx>
    void AdjustCtxForDB(TCtx &ctx) {
        ctx.AddMetadata(NYdb::YDB_AUTH_TICKET_HEADER, "root@builtin");
    }

    void MakeDirectory(auto &channel, const TString &path) {
        const auto stub = Ydb::Scheme::V1::SchemeService::NewStub(channel);

        Ydb::Scheme::MakeDirectoryRequest makeDirectoryRequest;
        makeDirectoryRequest.set_path(path);
        Ydb::Scheme::MakeDirectoryResponse makeDirectoryResponse;
        grpc::ClientContext makeDirectoryCtx;
        AdjustCtxForDB(makeDirectoryCtx);
        stub->MakeDirectory(&makeDirectoryCtx, makeDirectoryRequest, &makeDirectoryResponse);
        UNIT_ASSERT_CHECK_STATUS(makeDirectoryResponse.operation(), Ydb::StatusIds::SUCCESS);
    }

    void MakeTable(auto &channel, const TString &path) {
        const auto stub = Ydb::KeyValue::V1::KeyValueService::NewStub(channel);

        Ydb::KeyValue::CreateVolumeRequest createVolumeRequest;
        createVolumeRequest.set_path(path);
        createVolumeRequest.set_partition_count(1);
        auto *storage_config = createVolumeRequest.mutable_storage_config();
        storage_config->add_channel()->set_media("ssd");
        storage_config->add_channel()->set_media("ssd");
        storage_config->add_channel()->set_media("ssd");

        Ydb::KeyValue::CreateVolumeResponse createVolumeResponse;
        Ydb::KeyValue::CreateVolumeResult createVolumeResult;

        grpc::ClientContext createVolumeCtx;
        AdjustCtxForDB(createVolumeCtx);
        stub->CreateVolume(&createVolumeCtx, createVolumeRequest, &createVolumeResponse);
        UNIT_ASSERT_CHECK_STATUS(createVolumeResponse.operation(), Ydb::StatusIds::SUCCESS);
        createVolumeResponse.operation().result().UnpackTo(&createVolumeResult);
    }

    Ydb::Scheme::ListDirectoryResult ListDirectory(auto &channel, const TString &path) {
        const auto stub = Ydb::Scheme::V1::SchemeService::NewStub(channel);
        Ydb::Scheme::ListDirectoryRequest listDirectoryRequest;
        listDirectoryRequest.set_path(path);

        Ydb::Scheme::ListDirectoryResult listDirectoryResult;
        Ydb::Scheme::ListDirectoryResponse listDirectoryResponse;

        grpc::ClientContext listDirectoryCtx;
        AdjustCtxForDB(listDirectoryCtx);
        stub->ListDirectory(&listDirectoryCtx, listDirectoryRequest, &listDirectoryResponse);

        UNIT_ASSERT_CHECK_STATUS(listDirectoryResponse.operation(), Ydb::StatusIds::SUCCESS);
        listDirectoryResponse.operation().result().UnpackTo(&listDirectoryResult);
        return listDirectoryResult;
    }

    void WaitTableCreation(TKikimrWithGrpcAndRootSchema &server, const TString &path) {
        bool again = true;
        for (ui32 i = 0; i < 10 && again; ++i) {
            Cerr << "Wait iteration# " << i << Endl;
            auto req = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
            auto& entry = req->ResultSet.emplace_back();
            entry.Path = SplitPath(path);
            entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByPath;
            entry.ShowPrivatePath = true;
            entry.SyncVersion = false;
            req->UserToken = new NACLib::TUserToken("root@builtin", {});
            UNIT_ASSERT(req->UserToken);
            TActorId edgeActor = server.GetRuntime()->AllocateEdgeActor();
            auto ev = new TEvTxProxySchemeCache::TEvNavigateKeySet(req.Release());
            UNIT_ASSERT(ev->Request->UserToken);
            auto schemeCache = MakeSchemeCacheID();
            server.GetRuntime()->Send(new IEventHandle(schemeCache, edgeActor, ev));

            TAutoPtr<IEventHandle> handle;
            auto *result = server.GetRuntime()->GrabEdgeEvent<TEvTxProxySchemeCache::TEvNavigateKeySetResult>(handle);
            UNIT_ASSERT_VALUES_EQUAL(result->Request->ResultSet.size(), 1);
            again = result->Request->ResultSet[0].Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok;
        }
    }

    void MakeSimpleTest(const TString &tablePath, std::function<void(const std::unique_ptr<etcdserverpb::KV::Stub>&)> etcd)
    {
        TKikimrWithGrpcAndRootSchema server;
        const auto grpc = server.GetPort();
        const auto channel = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        MakeDirectory(channel, "/Root/mydb");
        MakeTable(channel, tablePath);
        const auto pr = SplitPath(tablePath);
        Ydb::Scheme::ListDirectoryResult listDirectoryResult = ListDirectory(channel, "/Root/mydb");
        UNIT_ASSERT_VALUES_EQUAL(listDirectoryResult.self().name(), "mydb");
        UNIT_ASSERT_VALUES_EQUAL(listDirectoryResult.children(0).name(), pr.back());

        WaitTableCreation(server, tablePath);
        const std::unique_ptr<etcdserverpb::KV::Stub> stub = etcdserverpb::KV::NewStub(channel);
        etcd(stub);
    }

    void Write(const TString &key, const TString &value, const std::unique_ptr<etcdserverpb::KV::Stub> &stub)
    {
        grpc::ClientContext writeCtx;
        AdjustCtxForDB(writeCtx);

        etcdserverpb::PutRequest putRequest;
        putRequest.set_key(key);
        putRequest.set_value(value);

        etcdserverpb::PutResponse putResponse;
        stub->Put(&writeCtx, putRequest, &putResponse);
    }

    Y_UNIT_TEST(SimpleWriteReadDelete) {
        TString tablePath = "/Root/mydb/kvtable";
        MakeSimpleTest(tablePath, [](const std::unique_ptr<etcdserverpb::KV::Stub> &etcd) {
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
                AdjustCtxForDB(readRangeCtx);

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
                AdjustCtxForDB(delCtx);

                etcdserverpb::DeleteRangeRequest deleteRangeRequest;
                deleteRangeRequest.set_key("key2");
                deleteRangeRequest.set_range_end("key4");
                etcdserverpb::DeleteRangeResponse deleteRangeResponse;
                etcd->DeleteRange(&delCtx, deleteRangeRequest, &deleteRangeResponse);
            }
            {
                grpc::ClientContext readRangeCtx;
                AdjustCtxForDB(readRangeCtx);

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

} // Y_UNIT_TEST_SUITE(KeyValueGRPCService)

} // NKikimr::NGRpcService

