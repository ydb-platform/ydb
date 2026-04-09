#include <ydb/services/ydb/ut_common/ydb_ut_test_includes.h>
#include <ydb/services/ydb/ydb_common_ut.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

using namespace Tests;
using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYdb::NScheme;

Y_UNIT_TEST_SUITE(TGRpcClientLowTest) {
    Y_UNIT_TEST(SimpleRequest) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        auto clientConfig = NGRpcProxy::TGRpcClientConfig(location);
        bool allDoneOk = false;

        {
            NYdbGrpc::TGRpcClientLow clientLow;
            auto connection = clientLow.CreateGRpcServiceConnection<Ydb::Table::V1::TableService>(clientConfig);

            Ydb::Table::CreateSessionRequest request;

            NYdbGrpc::TResponseCallback<Ydb::Table::CreateSessionResponse> responseCb =
                [&allDoneOk](NYdbGrpc::TGrpcStatus&& grpcStatus, Ydb::Table::CreateSessionResponse&& response) -> void {
                    UNIT_ASSERT(!grpcStatus.InternalError);
                    UNIT_ASSERT(grpcStatus.GRpcStatusCode == 0);
                    auto deferred = response.operation();
                    UNIT_ASSERT(deferred.ready() == true);
                    allDoneOk = true;
            };

            connection->DoRequest(request, std::move(responseCb), &Ydb::Table::V1::TableService::Stub::AsyncCreateSession);
        }
        UNIT_ASSERT(allDoneOk);
    }

    Y_UNIT_TEST(SimpleRequestDummyService) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        auto clientConfig = NGRpcProxy::TGRpcClientConfig(location);
        bool allDoneOk = false;

        {
            NYdbGrpc::TGRpcClientLow clientLow;
            auto connection = clientLow.CreateGRpcServiceConnection<Draft::Dummy::DummyService>(clientConfig);

            Draft::Dummy::PingRequest request;
            request.set_copy(true);
            request.set_payload("abc");

            NYdbGrpc::TResponseCallback<Draft::Dummy::PingResponse> responseCb =
                [&allDoneOk](NYdbGrpc::TGrpcStatus&& grpcStatus, Draft::Dummy::PingResponse&& response) -> void {
                    UNIT_ASSERT(!grpcStatus.InternalError);
                    UNIT_ASSERT(grpcStatus.GRpcStatusCode == 0);
                    UNIT_ASSERT(response.payload() == "abc");
                    allDoneOk = true;
            };

            connection->DoRequest(request, std::move(responseCb), &Draft::Dummy::DummyService::Stub::AsyncPing);
        }
        UNIT_ASSERT(allDoneOk);
    }

    std::pair<Ydb::StatusIds::StatusCode, grpc::StatusCode> MakeTestRequest(NGRpcProxy::TGRpcClientConfig& clientConfig, const TString& database, const TString& token) {
        NYdbGrpc::TCallMeta meta;
        if (token) { // empty token => no token
            meta.Aux.push_back({YDB_AUTH_TICKET_HEADER, token});
        }
        meta.Aux.push_back({YDB_DATABASE_HEADER, database});

        NYdbGrpc::TGRpcClientLow clientLow;
        auto connection = clientLow.CreateGRpcServiceConnection<Ydb::Table::V1::TableService>(clientConfig);

        Ydb::StatusIds::StatusCode status;
        grpc::StatusCode gStatus;

        do {
            auto promise = NThreading::NewPromise<void>();
            Ydb::Table::CreateSessionRequest request;
            NYdbGrpc::TResponseCallback<Ydb::Table::CreateSessionResponse> responseCb =
                [&status, &gStatus, promise](NYdbGrpc::TGrpcStatus&& grpcStatus, Ydb::Table::CreateSessionResponse&& response)  mutable {
                    UNIT_ASSERT(!grpcStatus.InternalError);
                    gStatus = grpc::StatusCode(grpcStatus.GRpcStatusCode);
                    auto deferred = response.operation();
                    status = deferred.status();
                    promise.SetValue();
                };

            connection->DoRequest(request, std::move(responseCb), &Ydb::Table::V1::TableService::Stub::AsyncCreateSession, meta);
            promise.GetFuture().Wait();
        } while (status == Ydb::StatusIds::UNAVAILABLE);
        Cerr << "TestRequest(database=\"" << database << "\", token=\"" << token << "\") => {" << Ydb::StatusIds::StatusCode_Name(status) << ", " << int(gStatus) << "}" << Endl;
        return std::make_pair(status, gStatus);
    }

    Y_UNIT_TEST(GrpcRequestProxy) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableDomainsConfig()->MutableSecurityConfig()->SetEnforceUserTokenRequirement(true);
        TKikimrWithGrpcAndRootSchemaWithAuth server(appConfig);

        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        auto clientConfig = NGRpcProxy::TGRpcClientConfig(location);

        UNIT_ASSERT_EQUAL(MakeTestRequest(clientConfig, "/Root", "root@builtin"), std::make_pair(Ydb::StatusIds::SUCCESS, grpc::StatusCode::OK));
        UNIT_ASSERT_EQUAL(MakeTestRequest(clientConfig, "/blabla", "root@builtin"), std::make_pair(Ydb::StatusIds::STATUS_CODE_UNSPECIFIED, grpc::StatusCode::UNAUTHENTICATED));
        UNIT_ASSERT_EQUAL(MakeTestRequest(clientConfig, "blabla", "root@builtin"), std::make_pair(Ydb::StatusIds::STATUS_CODE_UNSPECIFIED, grpc::StatusCode::UNAUTHENTICATED));
    }

    Y_UNIT_TEST(GrpcRequestProxyWithoutToken) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableDomainsConfig()->MutableSecurityConfig()->SetEnforceUserTokenRequirement(true);
        TKikimrWithGrpcAndRootSchemaWithAuth server(appConfig);

        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        auto clientConfig = NGRpcProxy::TGRpcClientConfig(location);

        UNIT_ASSERT_EQUAL(MakeTestRequest(clientConfig, "/Root", ""), std::make_pair(Ydb::StatusIds::STATUS_CODE_UNSPECIFIED, grpc::StatusCode::UNAUTHENTICATED));
        UNIT_ASSERT_EQUAL(MakeTestRequest(clientConfig, "/blabla", ""), std::make_pair(Ydb::StatusIds::STATUS_CODE_UNSPECIFIED, grpc::StatusCode::UNAUTHENTICATED));
        UNIT_ASSERT_EQUAL(MakeTestRequest(clientConfig, "blabla", ""), std::make_pair(Ydb::StatusIds::STATUS_CODE_UNSPECIFIED, grpc::StatusCode::UNAUTHENTICATED));
    }

    void GrpcRequestProxyCheckTokenWhenItIsSpecified(bool enforceUserTokenCheckRequirement) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableDomainsConfig()->MutableSecurityConfig()->SetEnforceUserTokenRequirement(false);
        appConfig.MutableDomainsConfig()->MutableSecurityConfig()->SetEnforceUserTokenCheckRequirement(enforceUserTokenCheckRequirement);
        TKikimrWithGrpcAndRootSchemaWithAuth server(appConfig);

        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        auto clientConfig = NGRpcProxy::TGRpcClientConfig(location);

        UNIT_ASSERT_EQUAL(MakeTestRequest(clientConfig, "/Root", ""), std::make_pair(Ydb::StatusIds::SUCCESS, grpc::StatusCode::OK));
        UNIT_ASSERT_EQUAL(MakeTestRequest(clientConfig, "/blabla", ""), std::make_pair(Ydb::StatusIds::SUCCESS, grpc::StatusCode::OK));
        UNIT_ASSERT_EQUAL(MakeTestRequest(clientConfig, "blabla", ""), std::make_pair(Ydb::StatusIds::SUCCESS, grpc::StatusCode::OK));

        UNIT_ASSERT_EQUAL(MakeTestRequest(clientConfig, "/Root", "root@builtin"), std::make_pair(Ydb::StatusIds::SUCCESS, grpc::StatusCode::OK));
        UNIT_ASSERT_EQUAL(MakeTestRequest(clientConfig, "/blabla", "root@builtin"), std::make_pair(Ydb::StatusIds::STATUS_CODE_UNSPECIFIED, grpc::StatusCode::UNAUTHENTICATED));
        UNIT_ASSERT_EQUAL(MakeTestRequest(clientConfig, "blabla", "root@builtin"), std::make_pair(Ydb::StatusIds::STATUS_CODE_UNSPECIFIED, grpc::StatusCode::UNAUTHENTICATED));

        const auto reqResultWithInvalidToken = MakeTestRequest(clientConfig, "/Root", "invalid token");
        if (enforceUserTokenCheckRequirement) {
            UNIT_ASSERT_EQUAL(reqResultWithInvalidToken, std::make_pair(Ydb::StatusIds::STATUS_CODE_UNSPECIFIED, grpc::StatusCode::UNAUTHENTICATED));
        } else {
            UNIT_ASSERT_EQUAL(reqResultWithInvalidToken, std::make_pair(Ydb::StatusIds::UNAUTHORIZED, grpc::StatusCode::OK));
        }

        UNIT_ASSERT_EQUAL(MakeTestRequest(clientConfig, "/blabla", "invalid token"), std::make_pair(Ydb::StatusIds::STATUS_CODE_UNSPECIFIED, grpc::StatusCode::UNAUTHENTICATED));
        UNIT_ASSERT_EQUAL(MakeTestRequest(clientConfig, "blabla", "invalid token"), std::make_pair(Ydb::StatusIds::STATUS_CODE_UNSPECIFIED, grpc::StatusCode::UNAUTHENTICATED));
    }

    Y_UNIT_TEST(GrpcRequestProxyCheckTokenWhenItIsSpecified_Ignore) {
        GrpcRequestProxyCheckTokenWhenItIsSpecified(false);
    }

    Y_UNIT_TEST(GrpcRequestProxyCheckTokenWhenItIsSpecified_Check) {
        GrpcRequestProxyCheckTokenWhenItIsSpecified(true);
    }

    Y_UNIT_TEST(BiStreamPing) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableDomainsConfig()->MutableSecurityConfig()->SetEnforceUserTokenRequirement(true);
        TKikimrWithGrpcAndRootSchemaWithAuth server(appConfig);

        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        auto clientConfig = NGRpcProxy::TGRpcClientConfig(location);
        NYdbGrpc::TCallMeta meta;
        meta.Aux.push_back({YDB_AUTH_TICKET_HEADER, "root@builtin"});
        {
            using TRequest = Draft::Dummy::PingRequest;
            using TResponse = Draft::Dummy::PingResponse;
            using TProcessor = typename NYdbGrpc::IStreamRequestReadWriteProcessor<TRequest, TResponse>::TPtr;
            NYdbGrpc::TGRpcClientLow clientLow;
            auto connection = clientLow.CreateGRpcServiceConnection<Draft::Dummy::DummyService>(clientConfig);

            auto promise = NThreading::NewPromise<void>();
            auto finishCb = [promise](NYdbGrpc::TGrpcStatus&& status) mutable {
                UNIT_ASSERT_EQUAL(status.GRpcStatusCode, grpc::StatusCode::UNAUTHENTICATED);
                promise.SetValue();
            };

            TResponse response;
            auto getReadCb = [finishCb](TProcessor processor) {
                auto readCb = [finishCb, processor](NYdbGrpc::TGrpcStatus&& status) {
                    UNIT_ASSERT(status.Ok());
                    processor->Finish(finishCb);
                };
                return readCb;
            };

            auto lowCallback = [getReadCb, &response]
                (NYdbGrpc::TGrpcStatus grpcStatus, TProcessor processor) mutable {
                    UNIT_ASSERT(grpcStatus.Ok());
                    Draft::Dummy::PingRequest request;
                    request.set_copy(true);
                    request.set_payload("abc");

                    processor->Write(std::move(request));

                    processor->Read(&response, getReadCb(processor));
                };

            connection->DoStreamRequest<TRequest, TResponse>(
                std::move(lowCallback),
                &Draft::Dummy::DummyService::Stub::AsyncBiStreamPing,
                std::move(meta));

            promise.GetFuture().Wait();
        }
    }

    Y_UNIT_TEST(BiStreamCancelled) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableDomainsConfig()->MutableSecurityConfig()->SetEnforceUserTokenRequirement(true);
        TKikimrWithGrpcAndRootSchemaWithAuth server(appConfig);

        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        auto clientConfig = NGRpcProxy::TGRpcClientConfig(location);
        NYdbGrpc::TCallMeta meta;
        meta.Aux.push_back({YDB_AUTH_TICKET_HEADER, "root@builtin"});
        {
            using TRequest = Draft::Dummy::PingRequest;
            using TResponse = Draft::Dummy::PingResponse;
            using TProcessor = typename NYdbGrpc::IStreamRequestReadWriteProcessor<TRequest, TResponse>::TPtr;
            NYdbGrpc::TGRpcClientLow clientLow(/* numWorkerThreads = */ 0);
            auto connection = clientLow.CreateGRpcServiceConnection<Draft::Dummy::DummyService>(clientConfig);

            auto promise = NThreading::NewPromise<NYdbGrpc::TGrpcStatus>();
            auto lowCallback = [promise]
                (NYdbGrpc::TGrpcStatus grpcStatus, TProcessor /*processor*/) mutable {
                    promise.SetValue(grpcStatus);
                };

            // Use context that is already cancelled
            auto context = clientLow.CreateContext();
            context->Cancel();

            // Start a new call using the cancelled context as a provider
            connection->DoStreamRequest<TRequest, TResponse>(
                std::move(lowCallback),
                &Draft::Dummy::DummyService::Stub::AsyncBiStreamPing,
                std::move(meta),
                context.get());

            // Add worker thread after the call was started and cancelled
            clientLow.AddWorkerThreadForTest();

            // Note: it's unlikely but possible the call succeeds
            auto status = promise.GetFuture().ExtractValueSync();
            if (status.Ok()) {
                Cerr << "WARNING: unable to cause start/cancel race" << Endl;
            }
        }
    }

    Y_UNIT_TEST(MultipleSimpleRequests) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        auto clientConfig = NGRpcProxy::TGRpcClientConfig(location);
        TAtomic okResponseCounter = 0;
        const size_t numRequests = 1000;

        {
            NYdbGrpc::TGRpcClientLow clientLow;
            auto connection = clientLow.CreateGRpcServiceConnection<Ydb::Table::V1::TableService>(clientConfig);

            Ydb::Table::CreateSessionRequest request;
            for (size_t i = 0; i < numRequests; i++) {
                NYdbGrpc::TResponseCallback<Ydb::Table::CreateSessionResponse> responseCb =
                    [&okResponseCounter](NYdbGrpc::TGrpcStatus&& grpcStatus, Ydb::Table::CreateSessionResponse&& response) -> void {
                        UNIT_ASSERT(!grpcStatus.InternalError);
                        UNIT_ASSERT(grpcStatus.GRpcStatusCode == 0);
                        auto deferred = response.operation();
                        UNIT_ASSERT(deferred.ready() == true);
                        AtomicIncrement(okResponseCounter);
                };

                connection->DoRequest(request, std::move(responseCb), &Ydb::Table::V1::TableService::Stub::AsyncCreateSession);
            }
        }
        UNIT_ASSERT(numRequests == AtomicGet(okResponseCounter));
    }

    Y_UNIT_TEST(ChangeAcl) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        auto clientConfig = NGRpcProxy::TGRpcClientConfig(location);

        {
            TClient client(*server.ServerSettings);
            client.CreateUser("/Root", "qqq", "password");
        }
        {
            NYdbGrpc::TGRpcClientLow clientLow;
            auto connection = clientLow.CreateGRpcServiceConnection<Ydb::Scheme::V1::SchemeService>(clientConfig);

            Ydb::Scheme::MakeDirectoryRequest request;
            TString scheme(
                "path: \"/Root/TheDirectory\"");
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);

            NYdbGrpc::TResponseCallback<Ydb::Scheme::MakeDirectoryResponse> responseCb =
                [](NYdbGrpc::TGrpcStatus&& grpcStatus, Ydb::Scheme::MakeDirectoryResponse&& response) -> void {
                    UNIT_ASSERT(!grpcStatus.InternalError);
                    UNIT_ASSERT(grpcStatus.GRpcStatusCode == 0);
                    auto deferred = response.operation();
                    UNIT_ASSERT(deferred.ready() == true);
            };

            connection->DoRequest(request, std::move(responseCb), &Ydb::Scheme::V1::SchemeService::Stub::AsyncMakeDirectory);
        }
        {
            NYdbGrpc::TGRpcClientLow clientLow;
            auto connection = clientLow.CreateGRpcServiceConnection<Ydb::Scheme::V1::SchemeService>(clientConfig);

            Ydb::Scheme::ModifyPermissionsRequest request;
            request.set_path("/Root/TheDirectory");
            request.Addactions()->Setchange_owner("qqq");

            auto perm = request.Addactions()->mutable_set();
            perm->set_subject("qqq");
            perm->add_permission_names("ydb.generic.read");


            NYdbGrpc::TResponseCallback<Ydb::Scheme::ModifyPermissionsResponse> responseCb =
                [](NYdbGrpc::TGrpcStatus&& grpcStatus, Ydb::Scheme::ModifyPermissionsResponse&& response) -> void {
                    UNIT_ASSERT(!grpcStatus.InternalError);
                    UNIT_ASSERT(grpcStatus.GRpcStatusCode == 0);
                    auto deferred = response.operation();
                    UNIT_ASSERT(deferred.ready() == true);
                    UNIT_ASSERT(deferred.status() == Ydb::StatusIds::SUCCESS);
            };

            connection->DoRequest(request, std::move(responseCb), &Ydb::Scheme::V1::SchemeService::Stub::AsyncModifyPermissions);
        }
        {
            NYdbGrpc::TGRpcClientLow clientLow;
            auto connection = clientLow.CreateGRpcServiceConnection<Ydb::Scheme::V1::SchemeService>(clientConfig);

            Ydb::Scheme::DescribePathRequest request;
            TString scheme(
                "path: \"/Root/TheDirectory\"");
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);

            NYdbGrpc::TResponseCallback<Ydb::Scheme::DescribePathResponse> responseCb =
            [](NYdbGrpc::TGrpcStatus&& grpcStatus, Ydb::Scheme::DescribePathResponse&& response) -> void {
                    UNIT_ASSERT(!grpcStatus.InternalError);
                    UNIT_ASSERT(grpcStatus.GRpcStatusCode == 0);
                    auto deferred = response.operation();
                    UNIT_ASSERT(deferred.ready() == true);
                    Ydb::Scheme::DescribePathResult result;
                    deferred.result().UnpackTo(&result);
                    result.mutable_self()->clear_created_at(); // variadic part
                    TString tmp;
                    google::protobuf::TextFormat::PrintToString(result, &tmp);
                    const TString expected = R"___(self {
  name: "TheDirectory"
  owner: "qqq"
  type: DIRECTORY
  effective_permissions {
    subject: "qqq"
    permission_names: "ydb.generic.read"
  }
  permissions {
    subject: "qqq"
    permission_names: "ydb.generic.read"
  }
}
)___";
                    UNIT_ASSERT_NO_DIFF(expected, tmp);

            };

            connection->DoRequest(request, std::move(responseCb), &Ydb::Scheme::V1::SchemeService::Stub::AsyncDescribePath);
        }
    }
}

} // namespace NKikimr

