#include "grpc_service.h"

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
        ui16 port = PortManager.GetPort(2134);
        ui16 grpc = PortManager.GetPort(2135);
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
        ServerSettings->RegisterGrpcService<NKikimr::NGRpcService::TKeyValueGRpcService>("keyvalue");

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

Y_UNIT_TEST_SUITE(KeyValueGRPCService) {

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
        std::unique_ptr<Ydb::Scheme::V1::SchemeService::Stub> stub;
        stub = Ydb::Scheme::V1::SchemeService::NewStub(channel);

        Ydb::Scheme::MakeDirectoryRequest makeDirectoryRequest;
        makeDirectoryRequest.set_path(path);
        Ydb::Scheme::MakeDirectoryResponse makeDirectoryResponse;
        grpc::ClientContext makeDirectoryCtx;
        AdjustCtxForDB(makeDirectoryCtx);
        stub->MakeDirectory(&makeDirectoryCtx, makeDirectoryRequest, &makeDirectoryResponse);
        UNIT_ASSERT_CHECK_STATUS(makeDirectoryResponse.operation(), Ydb::StatusIds::SUCCESS);
    }

    void MakeTable(auto &channel, const TString &path) {
        std::unique_ptr<Ydb::KeyValue::V1::KeyValueService::Stub> stub;
        stub = Ydb::KeyValue::V1::KeyValueService::NewStub(channel);

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

    void AlterVolume(auto &channel, const TString &path, ui32 partition_count = 1) {
        std::unique_ptr<Ydb::KeyValue::V1::KeyValueService::Stub> stub;
        stub = Ydb::KeyValue::V1::KeyValueService::NewStub(channel);

        Ydb::KeyValue::AlterVolumeRequest alterVolumeRequest;
        alterVolumeRequest.set_path(path);
        alterVolumeRequest.set_alter_partition_count(partition_count);

        Ydb::KeyValue::AlterVolumeResponse alterVolumeResponse;
        Ydb::KeyValue::AlterVolumeResult alterVolumeResult;

        grpc::ClientContext alterVolumeCtx;
        AdjustCtxForDB(alterVolumeCtx);
        stub->AlterVolume(&alterVolumeCtx, alterVolumeRequest, &alterVolumeResponse);
        UNIT_ASSERT_CHECK_STATUS(alterVolumeResponse.operation(), Ydb::StatusIds::SUCCESS);
        alterVolumeResponse.operation().result().UnpackTo(&alterVolumeResult);
    }

    void DropVolume(auto &channel, const TString &path) {
        std::unique_ptr<Ydb::KeyValue::V1::KeyValueService::Stub> stub;
        stub = Ydb::KeyValue::V1::KeyValueService::NewStub(channel);

        Ydb::KeyValue::DropVolumeRequest dropVolumeRequest;
        dropVolumeRequest.set_path(path);

        Ydb::KeyValue::DropVolumeResponse dropVolumeResponse;
        Ydb::KeyValue::DropVolumeResult dropVolumeResult;

        grpc::ClientContext dropVolumeCtx;
        AdjustCtxForDB(dropVolumeCtx);
        stub->DropVolume(&dropVolumeCtx, dropVolumeRequest, &dropVolumeResponse);
        UNIT_ASSERT_CHECK_STATUS(dropVolumeResponse.operation(), Ydb::StatusIds::SUCCESS);
        dropVolumeResponse.operation().result().UnpackTo(&dropVolumeResult);
    }

    Ydb::KeyValue::DescribeVolumeResult DescribeVolume(auto &channel, const TString &path) {
        std::unique_ptr<Ydb::KeyValue::V1::KeyValueService::Stub> stub;
        stub = Ydb::KeyValue::V1::KeyValueService::NewStub(channel);

        Ydb::KeyValue::DescribeVolumeRequest describeVolumeRequest;
        describeVolumeRequest.set_path(path);

        Ydb::KeyValue::DescribeVolumeResponse describeVolumeResponse;
        Ydb::KeyValue::DescribeVolumeResult describeVolumeResult;

        grpc::ClientContext describeVolumeCtx;
        AdjustCtxForDB(describeVolumeCtx);
        stub->DescribeVolume(&describeVolumeCtx, describeVolumeRequest, &describeVolumeResponse);
        UNIT_ASSERT_CHECK_STATUS(describeVolumeResponse.operation(), Ydb::StatusIds::SUCCESS);
        describeVolumeResponse.operation().result().UnpackTo(&describeVolumeResult);
        return describeVolumeResult;
    }


    Ydb::Scheme::ListDirectoryResult ListDirectory(auto &channel, const TString &path) {
        std::unique_ptr<Ydb::Scheme::V1::SchemeService::Stub> stub;
        stub = Ydb::Scheme::V1::SchemeService::NewStub(channel);
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

    ui64 AcquireLock( const TString &path, ui64 partitionId, auto &stub) {
        Ydb::KeyValue::AcquireLockRequest request;
        request.set_path(path);
        request.set_partition_id(partitionId);

        Ydb::KeyValue::AcquireLockResponse response;
        Ydb::KeyValue::AcquireLockResult result;

        grpc::ClientContext ctx;
        AdjustCtxForDB(ctx);
        stub->AcquireLock(&ctx, request, &response);
        UNIT_ASSERT_CHECK_STATUS(response.operation(), Ydb::StatusIds::SUCCESS);
        response.operation().result().UnpackTo(&result);
        return result.lock_generation();
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

    void MakeSimpleTest(const TString &tablePath,
            std::function<void(const std::unique_ptr<Ydb::KeyValue::V1::KeyValueService::Stub>&)> func)
    {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        ////////////////////////////////////////////////////////////////////////

        std::shared_ptr<grpc::Channel> channel;
        std::unique_ptr<Ydb::KeyValue::V1::KeyValueService::Stub> stub;
        channel = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        MakeDirectory(channel, "/Root/mydb");
        MakeTable(channel, tablePath);
        auto pr = SplitPath(tablePath);
        Ydb::Scheme::ListDirectoryResult listDirectoryResult = ListDirectory(channel, "/Root/mydb");
        UNIT_ASSERT_VALUES_EQUAL(listDirectoryResult.self().name(), "mydb");
        UNIT_ASSERT_VALUES_EQUAL(listDirectoryResult.children(0).name(), pr.back());

        WaitTableCreation(server, tablePath);
        stub = Ydb::KeyValue::V1::KeyValueService::NewStub(channel);
        func(stub);
    }

    Y_UNIT_TEST(SimpleAcquireLock) {
        TString tablePath = "/Root/mydb/kvtable";
        MakeSimpleTest(tablePath, [tablePath](const std::unique_ptr<Ydb::KeyValue::V1::KeyValueService::Stub> &stub){
            Ydb::KeyValue::AcquireLockRequest request;
            request.set_path(tablePath);
            request.set_partition_id(0);
            Ydb::KeyValue::AcquireLockResponse response;
            Ydb::KeyValue::AcquireLockResult result;

            grpc::ClientContext ctx1;
            AdjustCtxForDB(ctx1);
            stub->AcquireLock(&ctx1, request, &response);
            UNIT_ASSERT_CHECK_STATUS(response.operation(), Ydb::StatusIds::SUCCESS);
            response.operation().result().UnpackTo(&result);
            UNIT_ASSERT(result.lock_generation() == 1);

            grpc::ClientContext ctx2;
            AdjustCtxForDB(ctx2);
            stub->AcquireLock(&ctx2, request, &response);
            UNIT_ASSERT_CHECK_STATUS(response.operation(), Ydb::StatusIds::SUCCESS);
            response.operation().result().UnpackTo(&result);
            UNIT_ASSERT(result.lock_generation() == 2);
        });
    }

    Y_UNIT_TEST(SimpleExecuteTransaction) {
        TString tablePath = "/Root/mydb/kvtable";
        MakeSimpleTest(tablePath, [tablePath](const std::unique_ptr<Ydb::KeyValue::V1::KeyValueService::Stub> &stub){
            Ydb::KeyValue::ExecuteTransactionRequest request;
            request.set_path(tablePath);
            request.set_partition_id(0);
            Ydb::KeyValue::ExecuteTransactionResponse response;

            grpc::ClientContext ctx;
            AdjustCtxForDB(ctx);
            stub->ExecuteTransaction(&ctx, request, &response);
            UNIT_ASSERT_CHECK_STATUS(response.operation(), Ydb::StatusIds::SUCCESS);
        });
    }

    Y_UNIT_TEST(SimpleExecuteTransactionWithWrongGeneration) {
        TString tablePath = "/Root/mydb/kvtable";
        MakeSimpleTest(tablePath, [tablePath](const std::unique_ptr<Ydb::KeyValue::V1::KeyValueService::Stub> &stub){
            Ydb::KeyValue::ExecuteTransactionRequest request;
            request.set_path(tablePath);
            request.set_partition_id(0);
            request.set_lock_generation(42);
            Ydb::KeyValue::ExecuteTransactionResponse response;

            grpc::ClientContext ctx;
            AdjustCtxForDB(ctx);
            stub->ExecuteTransaction(&ctx, request, &response);
            UNIT_ASSERT_CHECK_STATUS(response.operation(), Ydb::StatusIds::PRECONDITION_FAILED);
        });
    }

    Ydb::KeyValue::ExecuteTransactionResult Write(const TString &path, ui64 partitionId, const TString &key, const TString &value, ui64 storageChannel,
            const std::unique_ptr<Ydb::KeyValue::V1::KeyValueService::Stub> &stub)
    {
        Ydb::KeyValue::ExecuteTransactionRequest writeRequest;
        writeRequest.set_path(path);
        writeRequest.set_partition_id(partitionId);
        auto *cmd = writeRequest.add_commands();
        auto *write = cmd->mutable_write();
        write->set_key(key);
        write->set_value(value);
        write->set_storage_channel(storageChannel);
        Ydb::KeyValue::ExecuteTransactionResponse writeResponse;

        grpc::ClientContext writeCtx;
        AdjustCtxForDB(writeCtx);
        stub->ExecuteTransaction(&writeCtx, writeRequest, &writeResponse);
        UNIT_ASSERT_CHECK_STATUS(writeResponse.operation(), Ydb::StatusIds::SUCCESS);
        Ydb::KeyValue::ExecuteTransactionResult writeResult;
        writeResponse.operation().result().UnpackTo(&writeResult);
        return writeResult;
    }

    void Rename(const TString &path, ui64 partitionId, const TString &oldKey, const TString &newKey,
            const std::unique_ptr<Ydb::KeyValue::V1::KeyValueService::Stub> &stub)
    {
        Ydb::KeyValue::ExecuteTransactionRequest request;
        request.set_path(path);
        request.set_partition_id(partitionId);
        auto *cmd = request.add_commands();
        auto *rename = cmd->mutable_rename();
        rename->set_old_key(oldKey);
        rename->set_new_key(newKey);
        Ydb::KeyValue::ExecuteTransactionResponse response;

        grpc::ClientContext ctx;
        AdjustCtxForDB(ctx);
        stub->ExecuteTransaction(&ctx, request, &response);
        UNIT_ASSERT_CHECK_STATUS(response.operation(), Ydb::StatusIds::SUCCESS);
    }


    Y_UNIT_TEST(SimpleRenameUnexistedKey) {
        TString tablePath = "/Root/mydb/kvtable";
        MakeSimpleTest(tablePath, [tablePath](const std::unique_ptr<Ydb::KeyValue::V1::KeyValueService::Stub> &stub){
            Ydb::KeyValue::ExecuteTransactionRequest request;
            request.set_path(tablePath);
            request.set_partition_id(0);
            auto *cmd = request.add_commands();
            auto *rename = cmd->mutable_rename();
            rename->set_old_key("key1");
            rename->set_new_key("key2");
            Ydb::KeyValue::ExecuteTransactionResponse response;

            grpc::ClientContext ctx;
            AdjustCtxForDB(ctx);
            stub->ExecuteTransaction(&ctx, request, &response);
            UNIT_ASSERT_CHECK_STATUS(response.operation(), Ydb::StatusIds::NOT_FOUND);
        });
    }

    Y_UNIT_TEST(SimpleConcatUnexistedKey) {
        TString tablePath = "/Root/mydb/kvtable";
        MakeSimpleTest(tablePath, [tablePath](const std::unique_ptr<Ydb::KeyValue::V1::KeyValueService::Stub> &stub){
            Ydb::KeyValue::ExecuteTransactionRequest request;
            request.set_path(tablePath);
            request.set_partition_id(0);
            auto *cmd = request.add_commands();
            auto *concat = cmd->mutable_concat();
            concat->add_input_keys("key1");
            concat->add_input_keys("key2");
            concat->set_output_key("key3");
            Ydb::KeyValue::ExecuteTransactionResponse response;

            grpc::ClientContext ctx;
            AdjustCtxForDB(ctx);
            stub->ExecuteTransaction(&ctx, request, &response);
            UNIT_ASSERT_CHECK_STATUS(response.operation(), Ydb::StatusIds::NOT_FOUND);
        });
    }

    Y_UNIT_TEST(SimpleCopyUnexistedKey) {
        TString tablePath = "/Root/mydb/kvtable";
        MakeSimpleTest(tablePath, [tablePath](const std::unique_ptr<Ydb::KeyValue::V1::KeyValueService::Stub> &stub){
            Ydb::KeyValue::ExecuteTransactionRequest request;
            request.set_path(tablePath);
            request.set_partition_id(0);
            auto *cmd = request.add_commands();
            auto *rename = cmd->mutable_copy_range();
            auto *range = rename->mutable_range();
            range->set_from_key_inclusive("key1");
            range->set_to_key_inclusive("key2");
            rename->set_prefix_to_add("A");
            Ydb::KeyValue::ExecuteTransactionResponse response;

            grpc::ClientContext ctx;
            AdjustCtxForDB(ctx);
            stub->ExecuteTransaction(&ctx, request, &response);
            UNIT_ASSERT_CHECK_STATUS(response.operation(), Ydb::StatusIds::SUCCESS);
        });
    }

    Y_UNIT_TEST(SimpleWriteRead) {
        TString tablePath = "/Root/mydb/kvtable";
        MakeSimpleTest(tablePath, [tablePath](const std::unique_ptr<Ydb::KeyValue::V1::KeyValueService::Stub> &stub){
            Write(tablePath, 0, "key", "value", 0, stub);

            Ydb::KeyValue::ReadRequest readRequest;
            readRequest.set_path(tablePath);
            readRequest.set_partition_id(0);
            readRequest.set_key("key");
            Ydb::KeyValue::ReadResponse readResponse;
            Ydb::KeyValue::ReadResult readResult;

            grpc::ClientContext readCtx;
            AdjustCtxForDB(readCtx);
            stub->Read(&readCtx, readRequest, &readResponse);
            UNIT_ASSERT_CHECK_STATUS(readResponse.operation(), Ydb::StatusIds::SUCCESS);
            readResponse.operation().result().UnpackTo(&readResult);
            UNIT_ASSERT(!readResult.is_overrun());
            UNIT_ASSERT_VALUES_EQUAL(readResult.requested_key(), "key");
            UNIT_ASSERT_VALUES_EQUAL(readResult.value(), "value");
            UNIT_ASSERT_VALUES_EQUAL(readResult.requested_offset(), 0);
            UNIT_ASSERT_VALUES_EQUAL(readResult.requested_size(), 5);
        });
    }

    Y_UNIT_TEST(SimpleWriteReadWithIncorreectPath) {
        TString tablePath = "/Root/mydb/kvtable";
        MakeSimpleTest(tablePath, [tablePath](const std::unique_ptr<Ydb::KeyValue::V1::KeyValueService::Stub> &stub){
            Write(tablePath, 0, "key", "value", 0, stub);

            Ydb::KeyValue::ReadRequest readRequest;
            readRequest.set_path("/Root/mydb/table");
            readRequest.set_partition_id(0);
            readRequest.set_key("key");
            Ydb::KeyValue::ReadResponse readResponse;
            Ydb::KeyValue::ReadResult readResult;

            grpc::ClientContext readCtx;
            AdjustCtxForDB(readCtx);
            stub->Read(&readCtx, readRequest, &readResponse);
            UNIT_ASSERT_CHECK_STATUS(readResponse.operation(), Ydb::StatusIds::SCHEME_ERROR);
        });
    }

    Y_UNIT_TEST(SimpleWriteReadWithoutToken) {
        TString tablePath = "/Root/mydb/kvtable";
        MakeSimpleTest(tablePath, [tablePath](const std::unique_ptr<Ydb::KeyValue::V1::KeyValueService::Stub> &stub){
            return;
            Write(tablePath, 0, "key", "value", 0, stub);

            Ydb::KeyValue::ReadRequest readRequest;
            readRequest.set_path("/Root/mydb/kvtable");
            readRequest.set_partition_id(0);
            readRequest.set_key("key");
            Ydb::KeyValue::ReadResponse readResponse;
            Ydb::KeyValue::ReadResult readResult;

            grpc::ClientContext readCtx;
            //AdjustCtxForDB(readCtx);
            stub->Read(&readCtx, readRequest, &readResponse);
            UNIT_ASSERT_CHECK_STATUS(readResponse.operation(), Ydb::StatusIds::SCHEME_ERROR);
        });
    }

    Y_UNIT_TEST(SimpleWriteReadWithoutLockGeneration1) {
        TString tablePath = "/Root/mydb/kvtable";
        MakeSimpleTest(tablePath, [tablePath](const std::unique_ptr<Ydb::KeyValue::V1::KeyValueService::Stub> &stub){
            AcquireLock(tablePath, 0, stub);
            Write(tablePath, 0, "key", "value", 0, stub);
            Ydb::KeyValue::ReadRequest readRequest;
            readRequest.set_path(tablePath);
            readRequest.set_partition_id(0);
            readRequest.set_key("key");
            Ydb::KeyValue::ReadResponse readResponse;
            Ydb::KeyValue::ReadResult readResult;

            grpc::ClientContext readCtx;
            AdjustCtxForDB(readCtx);
            stub->Read(&readCtx, readRequest, &readResponse);
            UNIT_ASSERT_CHECK_STATUS(readResponse.operation(), Ydb::StatusIds::SUCCESS);
        });
    }

    Y_UNIT_TEST(SimpleWriteReadWithoutLockGeneration2) {
        TString tablePath = "/Root/mydb/kvtable";
        MakeSimpleTest(tablePath, [tablePath](const std::unique_ptr<Ydb::KeyValue::V1::KeyValueService::Stub> &stub){
            Write(tablePath, 0, "key", "value", 0, stub);
            AcquireLock(tablePath, 0, stub);
            Ydb::KeyValue::ReadRequest readRequest;
            readRequest.set_path(tablePath);
            readRequest.set_partition_id(0);
            readRequest.set_key("key");
            Ydb::KeyValue::ReadResponse readResponse;
            Ydb::KeyValue::ReadResult readResult;

            grpc::ClientContext readCtx;
            AdjustCtxForDB(readCtx);
            stub->Read(&readCtx, readRequest, &readResponse);
            UNIT_ASSERT_CHECK_STATUS(readResponse.operation(), Ydb::StatusIds::SUCCESS);
        });
    }

    Y_UNIT_TEST(SimpleWriteReadOverrun) {
        TString tablePath = "/Root/mydb/kvtable";
        MakeSimpleTest(tablePath, [tablePath](const std::unique_ptr<Ydb::KeyValue::V1::KeyValueService::Stub> &stub){
            Write(tablePath, 0, "key", "value", 0, stub);

            Ydb::KeyValue::ReadRequest readRequest;
            readRequest.set_path(tablePath);
            readRequest.set_partition_id(0);
            readRequest.set_key("key");
            ui64 limitBytes = 1 + 5 + 3 // Key id, length
                    + 1 + 5 + 1 // Value id, length, value
                    + 1 + 8 // Offset id, value
                    + 1 + 8 // Size id, value
                    + 1 + 1 // Status id, value
                    ;
            readRequest.set_limit_bytes(limitBytes);
            Ydb::KeyValue::ReadResponse readResponse;
            Ydb::KeyValue::ReadResult readResult;

            grpc::ClientContext readCtx;
            AdjustCtxForDB(readCtx);
            stub->Read(&readCtx, readRequest, &readResponse);
            UNIT_ASSERT_CHECK_STATUS(readResponse.operation(), Ydb::StatusIds::SUCCESS);
            readResponse.operation().result().UnpackTo(&readResult);
            UNIT_ASSERT(readResult.is_overrun());
            UNIT_ASSERT_VALUES_EQUAL(readResult.requested_key(), "key");
            UNIT_ASSERT_VALUES_EQUAL(readResult.value(), "v");
            UNIT_ASSERT_VALUES_EQUAL(readResult.requested_offset(), 0);
            UNIT_ASSERT_VALUES_EQUAL(readResult.requested_size(), 5);
        });
    }

    Y_UNIT_TEST(SimpleWriteReadRange) {
        TString tablePath = "/Root/mydb/kvtable";
        MakeSimpleTest(tablePath, [tablePath](const std::unique_ptr<Ydb::KeyValue::V1::KeyValueService::Stub> &stub){
            Write(tablePath, 0, "key1", "value1", 1, stub);
            Write(tablePath, 0, "key2", "value12", 2, stub);

            Ydb::KeyValue::ReadRangeRequest readRangeRequest;
            readRangeRequest.set_path(tablePath);
            readRangeRequest.set_partition_id(0);
            auto *r = readRangeRequest.mutable_range();
            r->set_from_key_inclusive("key1");
            r->set_to_key_inclusive("key3");
            Ydb::KeyValue::ReadRangeResponse readRangeResponse;
            Ydb::KeyValue::ReadRangeResult readRangeResult;

            grpc::ClientContext readRangeCtx;
            AdjustCtxForDB(readRangeCtx);
            stub->ReadRange(&readRangeCtx, readRangeRequest, &readRangeResponse);
            UNIT_ASSERT_CHECK_STATUS(readRangeResponse.operation(), Ydb::StatusIds::SUCCESS);
            readRangeResponse.operation().result().UnpackTo(&readRangeResult);

            UNIT_ASSERT_VALUES_EQUAL(readRangeResult.pair(0).key(), "key1");
            UNIT_ASSERT_VALUES_EQUAL(readRangeResult.pair(1).key(), "key2");

            UNIT_ASSERT_VALUES_EQUAL(readRangeResult.pair(0).value(), "value1");
            UNIT_ASSERT_VALUES_EQUAL(readRangeResult.pair(1).value(), "value12");

            UNIT_ASSERT_VALUES_EQUAL(readRangeResult.pair(0).storage_channel(), 1);
            UNIT_ASSERT_VALUES_EQUAL(readRangeResult.pair(1).storage_channel(), 2);
        });
    }


    Y_UNIT_TEST(SimpleWriteListRange) {
        TString tablePath = "/Root/mydb/kvtable";
        MakeSimpleTest(tablePath, [tablePath](const std::unique_ptr<Ydb::KeyValue::V1::KeyValueService::Stub> &stub){
            Write(tablePath, 0, "key1", "value1", 1, stub);
            Write(tablePath, 0, "key2", "value12", 2, stub);

            Ydb::KeyValue::ListRangeRequest listRangeRequest;
            listRangeRequest.set_path(tablePath);
            listRangeRequest.set_partition_id(0);
            auto *r = listRangeRequest.mutable_range();
            r->set_from_key_inclusive("key1");
            r->set_to_key_inclusive("key3");
            Ydb::KeyValue::ListRangeResponse listRangeResponse;
            Ydb::KeyValue::ListRangeResult listRangeResult;

            grpc::ClientContext listRangeCtx;
            AdjustCtxForDB(listRangeCtx);
            stub->ListRange(&listRangeCtx, listRangeRequest, &listRangeResponse);
            UNIT_ASSERT_CHECK_STATUS(listRangeResponse.operation(), Ydb::StatusIds::SUCCESS);
            listRangeResponse.operation().result().UnpackTo(&listRangeResult);

            UNIT_ASSERT_VALUES_EQUAL(listRangeResult.key(0).key(), "key1");
            UNIT_ASSERT_VALUES_EQUAL(listRangeResult.key(1).key(), "key2");

            UNIT_ASSERT_VALUES_EQUAL(listRangeResult.key(0).value_size(), 6);
            UNIT_ASSERT_VALUES_EQUAL(listRangeResult.key(1).value_size(), 7);

            UNIT_ASSERT_VALUES_EQUAL(listRangeResult.key(0).storage_channel(), 1);
            UNIT_ASSERT_VALUES_EQUAL(listRangeResult.key(1).storage_channel(), 2);
        });
    }


    Y_UNIT_TEST(SimpleGetStorageChannelStatus) {
        TString tablePath = "/Root/mydb/kvtable";
        MakeSimpleTest(tablePath, [tablePath](const std::unique_ptr<Ydb::KeyValue::V1::KeyValueService::Stub> &stub){
            Ydb::KeyValue::GetStorageChannelStatusRequest getStatusRequest;
            getStatusRequest.set_path(tablePath);
            getStatusRequest.set_partition_id(0);
            getStatusRequest.add_storage_channel(1);
            getStatusRequest.add_storage_channel(2);
            getStatusRequest.add_storage_channel(3);
            Ydb::KeyValue::GetStorageChannelStatusResponse getStatusResponse;
            Ydb::KeyValue::GetStorageChannelStatusResult getStatusResult;

            grpc::ClientContext getStatusCtx;
            AdjustCtxForDB(getStatusCtx);
            stub->GetStorageChannelStatus(&getStatusCtx, getStatusRequest, &getStatusResponse);
            UNIT_ASSERT_CHECK_STATUS(getStatusResponse.operation(), Ydb::StatusIds::SUCCESS);
            getStatusResponse.operation().result().UnpackTo(&getStatusResult);
            UNIT_ASSERT_VALUES_EQUAL(getStatusResult.storage_channel_info_size(), 3);
        });
    }

    Y_UNIT_TEST(SimpleCreateAlterDropVolume) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        std::shared_ptr<grpc::Channel> channel;
        channel = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());

        TString path = "/Root/mydb/";
        TString tablePath = "/Root/mydb/mytable";
        MakeDirectory(channel, path);
        MakeTable(channel, tablePath);

        Ydb::Scheme::ListDirectoryResult listDirectoryResult = ListDirectory(channel, path);
        UNIT_ASSERT_VALUES_EQUAL(listDirectoryResult.self().name(), "mydb");
        UNIT_ASSERT_VALUES_EQUAL(listDirectoryResult.children(0).name(), "mytable");

        UNIT_ASSERT_VALUES_EQUAL(1, DescribeVolume(channel, tablePath).partition_count());

        AlterVolume(channel, tablePath, 2);
        listDirectoryResult = ListDirectory(channel, path);
        UNIT_ASSERT_VALUES_EQUAL(listDirectoryResult.self().name(), "mydb");
        UNIT_ASSERT_VALUES_EQUAL(listDirectoryResult.children(0).name(), "mytable");


        UNIT_ASSERT_VALUES_EQUAL(2, DescribeVolume(channel, tablePath).partition_count());

        DropVolume(channel, tablePath);
        listDirectoryResult = ListDirectory(channel, path);
        UNIT_ASSERT_VALUES_EQUAL(listDirectoryResult.self().name(), "mydb");
        UNIT_ASSERT_VALUES_EQUAL(listDirectoryResult.children_size(), 0);
    }

    Y_UNIT_TEST(SimpleListPartitions) {
        return; // delete it after adding ydb_token to requests in tests
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        std::shared_ptr<grpc::Channel> channel;
        channel = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());

        TString path = "/Root/mydb/";
        TString tablePath = "/Root/mydb/mytable";
        MakeDirectory(channel, path);
        MakeTable(channel, tablePath);

        std::unique_ptr<Ydb::KeyValue::V1::KeyValueService::Stub> stub;
        stub = Ydb::KeyValue::V1::KeyValueService::NewStub(channel);

        Write(tablePath, 0, "key1", "value1", 1, stub);

        Ydb::KeyValue::ListLocalPartitionsRequest enumerateRequest;
        enumerateRequest.set_path(tablePath);
        enumerateRequest.set_node_id(2);

        Ydb::KeyValue::ListLocalPartitionsResult enumerateResult;
        Ydb::KeyValue::ListLocalPartitionsResponse eumerateResponse;

        grpc::ClientContext enumerateCtx;
        AdjustCtxForDB(enumerateCtx);
        stub->ListLocalPartitions(&enumerateCtx, enumerateRequest, &eumerateResponse);

        UNIT_ASSERT_CHECK_STATUS(eumerateResponse.operation(), Ydb::StatusIds::SUCCESS);
        eumerateResponse.operation().result().UnpackTo(&enumerateResult);
        UNIT_ASSERT_VALUES_EQUAL(enumerateResult.partition_ids_size(), 1);

        auto writeRes = Write(tablePath, enumerateResult.partition_ids(0), "key2", "value2", 1, stub);
        UNIT_ASSERT_VALUES_EQUAL(writeRes.node_id(), 2);
    }

} // Y_UNIT_TEST_SUITE(KeyValueGRPCService)

} // NKikimr::NGRpcService
