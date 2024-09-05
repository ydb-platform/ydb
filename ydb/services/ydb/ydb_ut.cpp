#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/testing/unittest/registar.h>

#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>

#include <ydb/core/base/storage_pools.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>

#include <ydb/public/api/grpc/ydb_scheme_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_operation_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_table_v1.grpc.pb.h>
#include <ydb/public/api/grpc/draft/dummy.grpc.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/core/protos/follower_group.pb.h>
#include <ydb/core/protos/console_config.pb.h>
#include <ydb/core/protos/console_base.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <ydb/library/grpc/client/grpc_client_low.h>

#include <google/protobuf/any.h>

#include <ydb/library/yql/core/issue/yql_issue.h>
#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>

#include <ydb/public/sdk/cpp/client/ydb_params/params.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>
#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/resources/ydb_resources.h>

#include <ydb/public/lib/yson_value/ydb_yson_value.h>
#include <ydb/public/lib/json_value/ydb_json_value.h>

#include "ydb_common_ut.h"

#include <util/generic/ymath.h>

namespace NYdb {

Ydb::StatusIds::StatusCode WaitForStatus(
    std::shared_ptr<grpc::Channel> channel, const TString& opId, TString* error, int retries, TDuration sleepDuration
) {
    std::unique_ptr<Ydb::Operation::V1::OperationService::Stub> stub;
    stub = Ydb::Operation::V1::OperationService::NewStub(channel);
    Ydb::Operations::GetOperationRequest request;
    request.set_id(opId);
    Ydb::Operations::GetOperationResponse response;
    for (int retry = 0; retry <= retries; ++retry) {
        grpc::ClientContext context;
        auto grpcStatus = stub->GetOperation(&context, request, &response);
        UNIT_ASSERT_C(grpcStatus.ok(), grpcStatus.error_message());
        if (response.operation().ready()) {
            break;
        }
        Sleep(sleepDuration *= 2);
    }
    if (error && response.operation().issues_size() > 0) {
        NYql::TIssues issues;
        NYql::IssuesFromMessage(response.operation().issues(), issues);
        *error = issues.ToString();
    }
    return response.operation().status();
}

}

namespace NKikimr {

using namespace Tests;
using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYdb::NScheme;

static bool HasIssue(const NYql::TIssues& issues, ui32 code,
    std::function<bool(const NYql::TIssue& issue)> predicate = {})
{
    bool hasIssue = false;

    for (auto& issue : issues) {
        WalkThroughIssues(issue, false, [code, predicate, &hasIssue] (const NYql::TIssue& issue, int level) {
            Y_UNUSED(level);
            if (issue.GetCode() == code) {
                hasIssue = predicate
                    ? predicate(issue)
                    : true;
            }
        });
    }

    return hasIssue;
}

Ydb::Table::DescribeTableResult DescribeTable(std::shared_ptr<grpc::Channel> channel, const TString& sessionId, const TString& path)
{
    Ydb::Table::DescribeTableRequest request;
    request.set_session_id(sessionId);
    request.set_path(path);
    request.set_include_shard_key_bounds(true);

    Ydb::Table::DescribeTableResponse response;

    std::unique_ptr<Ydb::Table::V1::TableService::Stub> stub;
    stub = Ydb::Table::V1::TableService::NewStub(channel);
    grpc::ClientContext context;
    auto status = stub->DescribeTable(&context, request, &response);
    UNIT_ASSERT(status.ok());
    auto deferred = response.operation();
    UNIT_ASSERT(deferred.ready() == true);
    NYql::TIssues issues;
    NYql::IssuesFromMessage(deferred.issues(), issues);
    issues.PrintTo(Cerr);

    UNIT_ASSERT(deferred.status() == Ydb::StatusIds::SUCCESS);

    Ydb::Table::DescribeTableResult result;
    Y_ABORT_UNLESS(deferred.result().UnpackTo(&result));
    return result;
}


Ydb::Table::ExecuteQueryResult ExecYql(std::shared_ptr<grpc::Channel> channel, const TString &sessionId, const TString &yql, bool withStat = false);

struct TKikimrTestSettings {
    static constexpr bool SSL = false;
    static constexpr bool AUTH = false;
    static constexpr bool PrecreatePools = true;
};

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
            UNIT_ASSERT_EQUAL(reqResultWithInvalidToken, std::make_pair(Ydb::StatusIds::SUCCESS, grpc::StatusCode::OK));
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

Y_UNIT_TEST_SUITE(TGRpcNewClient) {
    Y_UNIT_TEST(SimpleYqlQuery) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        //TDriver
        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(location));

        auto client = NYdb::NTable::TTableClient(connection);

        std::function<void(const TAsyncCreateSessionResult& future)> createSessionHandler =
            [client] (const TAsyncCreateSessionResult& future) mutable {
                const auto& sessionValue = future.GetValue();
                UNIT_ASSERT(!sessionValue.IsTransportError());
                auto session = sessionValue.GetSession();

                auto createTableHandler =
                    [session, client] (NThreading::TFuture<TStatus> future) mutable {
                        const auto& createTableResult = future.GetValue();
                        UNIT_ASSERT(!createTableResult.IsTransportError());
                        auto sqlResultHandler =
                            [](TAsyncDataQueryResult future) mutable {
                                auto sqlResultSets = future.ExtractValue();
                                UNIT_ASSERT(!sqlResultSets.IsTransportError());

                                auto resultSets = sqlResultSets.GetResultSets();
                                UNIT_ASSERT_EQUAL(resultSets.size(), 1);
                                auto& resultSet = resultSets[0];
                                UNIT_ASSERT_EQUAL(resultSet.ColumnsCount(), 1);
                                auto meta = resultSet.GetColumnsMeta();
                                UNIT_ASSERT_EQUAL(meta.size(), 1);
                                UNIT_ASSERT_EQUAL(meta[0].Name, "colName");
                                TTypeParser parser(meta[0].Type);
                                UNIT_ASSERT(parser.GetKind() == TTypeParser::ETypeKind::Primitive);
                                UNIT_ASSERT(parser.GetPrimitive() == EPrimitiveType::Int32);

                                TResultSetParser rsParser(resultSet);
                                while (rsParser.TryNextRow()) {
                                    UNIT_ASSERT_EQUAL(rsParser.ColumnParser(0).GetInt32(), 42);
                                }
                            };

                        session.ExecuteDataQuery(
                            "SELECT 42 as colName;", TTxControl::BeginTx(
                                TTxSettings::SerializableRW()).CommitTx()
                        ).Apply(std::move(sqlResultHandler)).Wait();
                    };

                auto tableBuilder = client.GetTableBuilder();
                tableBuilder
                    .AddNullableColumn("Key", EPrimitiveType::Int32)
                    .AddNullableColumn("Value", EPrimitiveType::String);
                tableBuilder.SetPrimaryKeyColumn("Key");
                session.CreateTable("/Root/FooTable", tableBuilder.Build()).Apply(createTableHandler).Wait();
            };

        client.CreateSession().Apply(createSessionHandler).Wait();
    }

    Y_UNIT_TEST(TestAuth) {
        TKikimrWithGrpcAndRootSchemaWithAuthAndSsl server;
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetAuthToken("test_user@builtin")
                .UseSecureConnection(NYdbSslTestData::CaCrt)
                .SetEndpoint(location));

        auto client = NYdb::NTable::TTableClient(connection);
        std::function<void(const TAsyncCreateSessionResult& future)> createSessionHandler =
            [client] (const TAsyncCreateSessionResult& future) mutable {
                const auto& sessionValue = future.GetValue();
                UNIT_ASSERT(!sessionValue.IsTransportError());
                UNIT_ASSERT_EQUAL(sessionValue.GetStatus(), EStatus::SUCCESS);
            };

        client.CreateSession().Apply(createSessionHandler).Wait();
        connection.Stop(true);
    }

    Y_UNIT_TEST(YqlQueryWithParams) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        //TDriver
        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(location));

        auto client = NYdb::NTable::TTableClient(connection);

        std::function<void(const TAsyncCreateSessionResult& future)> createSessionHandler =
            [client] (const TAsyncCreateSessionResult& future) mutable {
                const auto& sessionValue = future.GetValue();
                UNIT_ASSERT(!sessionValue.IsTransportError());
                auto session = sessionValue.GetSession();

                auto prepareQueryHandler =
                    [session, client] (TAsyncPrepareQueryResult future) mutable {
                        const auto& prepareQueryResult = future.GetValue();
                        UNIT_ASSERT(!prepareQueryResult.IsTransportError());
                        UNIT_ASSERT_EQUAL(prepareQueryResult.GetStatus(), EStatus::SUCCESS);
                        auto query = prepareQueryResult.GetQuery();
                        auto paramsBuilder = client.GetParamsBuilder();
                        auto& param = paramsBuilder.AddParam("$paramName");
                        param.String("someString").Build();

                        auto sqlResultHandler =
                            [](TAsyncDataQueryResult future) mutable {
                                auto sqlResultSets = future.ExtractValue();
                                UNIT_ASSERT(!sqlResultSets.IsTransportError());
                                UNIT_ASSERT(sqlResultSets.IsSuccess());

                                auto resultSets = sqlResultSets.GetResultSets();
                                UNIT_ASSERT_EQUAL(resultSets.size(), 1);
                                auto& resultSet = resultSets[0];
                                UNIT_ASSERT_EQUAL(resultSet.ColumnsCount(), 1);
                                auto meta = resultSet.GetColumnsMeta();
                                UNIT_ASSERT_EQUAL(meta.size(), 1);
                                TTypeParser parser(meta[0].Type);
                                UNIT_ASSERT(parser.GetKind() == TTypeParser::ETypeKind::Primitive);
                                UNIT_ASSERT(parser.GetPrimitive() == EPrimitiveType::String);

                                TResultSetParser rsParser(resultSet);
                                while (rsParser.TryNextRow()) {
                                    UNIT_ASSERT_EQUAL(rsParser.ColumnParser(0).GetString(), "someString");
                                }
                            };

                        query.Execute(TTxControl::BeginTx(TTxSettings::OnlineRO()).CommitTx(),
                            paramsBuilder.Build()).Apply(sqlResultHandler).Wait();
                    };

                session.PrepareDataQuery("DECLARE $paramName AS String; SELECT $paramName;").Apply(prepareQueryHandler).Wait();
            };

        client.CreateSession().Apply(createSessionHandler).Wait();
    }

    Y_UNIT_TEST(YqlExplainDataQuery) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(location));

        auto client = NYdb::NTable::TTableClient(connection);
        bool done = false;

        std::function<void(const TAsyncCreateSessionResult& future)> createSessionHandler =
            [client, &done] (const TAsyncCreateSessionResult& future) mutable {
                const auto& sessionValue = future.GetValue();
                UNIT_ASSERT(!sessionValue.IsTransportError());
                UNIT_ASSERT_EQUAL(sessionValue.GetStatus(), EStatus::SUCCESS);
                auto session = sessionValue.GetSession();

                auto schemeQueryHandler =
                    [session, &done] (const NYdb::TAsyncStatus& future) mutable {
                        const auto& schemeQueryResult = future.GetValue();
                        UNIT_ASSERT(!schemeQueryResult.IsTransportError());
                        UNIT_ASSERT_EQUAL(schemeQueryResult.GetStatus(), EStatus::SUCCESS);

                        auto sqlResultHandler =
                            [&done] (const TAsyncExplainDataQueryResult& future) mutable {
                                const auto& explainResult = future.GetValue();
                                UNIT_ASSERT(!explainResult.IsTransportError());
                                Cerr << explainResult.GetIssues().ToString() << Endl;
                                UNIT_ASSERT_EQUAL(explainResult.GetStatus(), EStatus::SUCCESS);
                                done = true;
                            };
                        const TString query = "UPSERT INTO `Root/TheTable` (Key, Value) VALUES (1, \"One\");";
                        session.ExplainDataQuery(query).Apply(std::move(sqlResultHandler)).Wait();
                    };

                const TString query = "CREATE TABLE `Root/TheTable` (Key Uint64, Value Utf8, PRIMARY KEY (Key));";
                session.ExecuteSchemeQuery(query).Apply(schemeQueryHandler).Wait();
            };

        client.CreateSession().Apply(createSessionHandler).Wait();
        UNIT_ASSERT(done);
    }

    Y_UNIT_TEST(CreateAlterUpsertDrop) {
        TKikimrWithGrpcAndRootSchemaNoSystemViews server;
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(location));
        {

            auto asyncStatus = NYdb::NScheme::TSchemeClient(connection).MakeDirectory("/Root/TheDir");
            asyncStatus.Wait();
            UNIT_ASSERT_EQUAL(asyncStatus.GetValue().GetStatus(), EStatus::SUCCESS);
        }
        {
            auto asyncDescPath = NYdb::NScheme::TSchemeClient(connection).DescribePath("/Root/TheDir");
            asyncDescPath.Wait();
            auto entry = asyncDescPath.GetValue().GetEntry();
            UNIT_ASSERT_EQUAL(entry.Name, "TheDir");
            UNIT_ASSERT_EQUAL(entry.Type, ESchemeEntryType::Directory);
        }
        {
            auto asyncDescDir = NYdb::NScheme::TSchemeClient(connection).ListDirectory("/Root");
            asyncDescDir.Wait();
            const auto& val = asyncDescDir.GetValue();
            auto entry = val.GetEntry();
            UNIT_ASSERT_EQUAL(entry.Name, "Root");
            UNIT_ASSERT_EQUAL(entry.Type, ESchemeEntryType::Directory);
            auto children = val.GetChildren();
            UNIT_ASSERT_EQUAL(children.size(), 1);
            UNIT_ASSERT_EQUAL(children[0].Name, "TheDir");
            UNIT_ASSERT_EQUAL(children[0].Type, ESchemeEntryType::Directory);

        }

        auto client = NYdb::NTable::TTableClient(connection);
        bool done = false;

        std::function<void(const TAsyncCreateSessionResult& future)> createSessionHandler =
            [client, &done] (const TAsyncCreateSessionResult& future) mutable {
                const auto& sessionValue = future.GetValue();
                UNIT_ASSERT(!sessionValue.IsTransportError());
                auto session = sessionValue.GetSession();

                auto createTableHandler =
                    [session, &done, client] (const NThreading::TFuture<TStatus>& future) mutable {
                        const auto& createTableResult = future.GetValue();
                        UNIT_ASSERT(!createTableResult.IsTransportError());
                        auto alterResultHandler =
                            [session, &done] (const NThreading::TFuture<TStatus>& future) mutable {
                                const auto& alterStatus = future.GetValue();
                                UNIT_ASSERT(!alterStatus.IsTransportError());
                                UNIT_ASSERT_EQUAL(alterStatus.GetStatus(), EStatus::SUCCESS);
                                auto upsertHandler =
                                    [session, &done] (const TAsyncDataQueryResult& future) mutable {
                                        const auto& sqlResultSets = future.GetValue();
                                        UNIT_ASSERT(!sqlResultSets.IsTransportError());
                                        UNIT_ASSERT_EQUAL(sqlResultSets.GetStatus(), EStatus::SUCCESS);
                                        auto describeHandler =
                                            [session, &done] (const TAsyncDescribeTableResult& future) mutable {
                                                const auto& value = future.GetValue();
                                                UNIT_ASSERT(!value.IsTransportError());
                                                UNIT_ASSERT_EQUAL(value.GetStatus(), EStatus::SUCCESS);
                                                auto desc = value.GetTableDescription();
                                                UNIT_ASSERT_EQUAL(desc.GetPrimaryKeyColumns().size(), 1);
                                                UNIT_ASSERT_EQUAL(desc.GetPrimaryKeyColumns()[0], "Key");
                                                auto columns = desc.GetColumns();
                                                UNIT_ASSERT_EQUAL(columns[0].Name, "Key");
                                                UNIT_ASSERT_EQUAL(columns[1].Name, "Value");
                                                UNIT_ASSERT_EQUAL(columns[2].Name, "NewColumn");
                                                TTypeParser column0(columns[0].Type);
                                                TTypeParser column1(columns[1].Type);
                                                TTypeParser column2(columns[2].Type);
                                                UNIT_ASSERT_EQUAL(column0.GetKind(), TTypeParser::ETypeKind::Optional);
                                                UNIT_ASSERT_EQUAL(column1.GetKind(), TTypeParser::ETypeKind::Optional);
                                                UNIT_ASSERT_EQUAL(column2.GetKind(), TTypeParser::ETypeKind::Optional);
                                                column0.OpenOptional();
                                                column1.OpenOptional();
                                                column2.OpenOptional();
                                                UNIT_ASSERT_EQUAL(column0.GetPrimitive(), EPrimitiveType::Int32);
                                                UNIT_ASSERT_EQUAL(column1.GetPrimitive(), EPrimitiveType::String);
                                                UNIT_ASSERT_EQUAL(column2.GetPrimitive(), EPrimitiveType::Utf8);
                                                UNIT_ASSERT_EQUAL( desc.GetOwner(), "root@builtin");
                                                auto dropHandler =
                                                    [&done] (const NThreading::TFuture<TStatus>& future) mutable {
                                                        const auto& dropStatus = future.GetValue();
                                                        UNIT_ASSERT(!dropStatus.IsTransportError());
                                                        UNIT_ASSERT_EQUAL(dropStatus.GetStatus(), EStatus::SUCCESS);
                                                        done = true;
                                                };
                                                session.DropTable("/Root/TheDir/FooTable")
                                                    .Apply(dropHandler).Wait();

                                            };
                                        session.DescribeTable("/Root/TheDir/FooTable")
                                            .Apply(describeHandler).Wait();
                                                                            };
                                const TString sql = "UPSERT INTO `Root/TheDir/FooTable` (Key, Value, NewColumn)"
                                   " VALUES (1, \"One\", \"йцукен\")";
                                session.ExecuteDataQuery(sql, TTxControl::
                                    BeginTx(TTxSettings::SerializableRW()).CommitTx()
                                ).Apply(upsertHandler).Wait();
                            };

                        {
                            auto type = TTypeBuilder()
                                    .BeginOptional()
                                        .Primitive(EPrimitiveType::Utf8)
                                    .EndOptional()
                                    .Build();

                            session.AlterTable("/Root/TheDir/FooTable", TAlterTableSettings()
                                .AppendAddColumns(TColumn{"NewColumn", type})).Apply(alterResultHandler).Wait();
                        }
                    };

                auto tableBuilder = client.GetTableBuilder();
                tableBuilder
                    .AddNullableColumn("Key", EPrimitiveType::Int32)
                    .AddNullableColumn("Value", EPrimitiveType::String);
                tableBuilder.SetPrimaryKeyColumn("Key");
                session.CreateTable("/Root/TheDir/FooTable", tableBuilder.Build()).Apply(createTableHandler).Wait();
            };

        client.CreateSession().Apply(createSessionHandler).Wait();
        UNIT_ASSERT(done);
    }
}

static TString CreateSession(std::shared_ptr<grpc::Channel> channel) {
    std::unique_ptr<Ydb::Table::V1::TableService::Stub> stub;
    stub = Ydb::Table::V1::TableService::NewStub(channel);
    grpc::ClientContext context;
    Ydb::Table::CreateSessionRequest request;
    Ydb::Table::CreateSessionResponse response;

    auto status = stub->CreateSession(&context, request, &response);
    auto deferred = response.operation();
    UNIT_ASSERT(status.ok());
    UNIT_ASSERT(deferred.ready() == true);
    Ydb::Table::CreateSessionResult result;

    deferred.result().UnpackTo(&result);
    return result.session_id();
}

void IncorrectConnectionStringPending(const TString& incorrectLocation) {
    auto connection = NYdb::TDriver(incorrectLocation);
    auto client = NYdb::NTable::TTableClient(connection);
    auto session = client.CreateSession().ExtractValueSync().GetSession();
}

Y_UNIT_TEST_SUITE(GrpcConnectionStringParserTest) {
    Y_UNIT_TEST(NoDatabaseFlag) {
        TKikimrWithGrpcAndRootSchemaNoSystemViews server;
        ui16 grpc = server.GetPort();

        bool done = false;

        {
            TString location = TStringBuilder() << "localhost:" << grpc;

            // by default, location won't have database path
            auto connection = NYdb::TDriver(location);
            auto client = NYdb::NTable::TTableClient(connection);
            auto session = client.CreateSession().ExtractValueSync().GetSession();

            done = true;
        }

        UNIT_ASSERT(done);
    }

    Y_UNIT_TEST(IncorrectConnectionString) {
        TString incorrectLocation = "thisIsNotURL::::";
        UNIT_CHECK_GENERATED_EXCEPTION(IncorrectConnectionStringPending(incorrectLocation), std::exception);
    }

    Y_UNIT_TEST(CommonClientSettingsFromConnectionString) {
        TKikimrWithGrpcAndRootSchemaNoSystemViews server;
        ui16 grpc = server.GetPort();

        bool done = false;

        {
            TString location = TStringBuilder() << "localhost:" << grpc;

            // by default, location won't have database path
            auto settings = GetClientSettingsFromConnectionString(location);

            done = true;
        }

        UNIT_ASSERT(done);
    }
}

Y_UNIT_TEST_SUITE(TGRpcYdbTest) {
    Y_UNIT_TEST(RemoveNotExistedDirectory) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> Channel_;
        std::unique_ptr<Ydb::Scheme::V1::SchemeService::Stub> Stub_;
        Channel_ = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        {
            Stub_ = Ydb::Scheme::V1::SchemeService::NewStub(Channel_);
            grpc::ClientContext context;

            Ydb::Scheme::RemoveDirectoryRequest request;
            TString scheme(
                "path: \"/Root/TheNotExistedDirectory\"");
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);
            Ydb::Scheme::RemoveDirectoryResponse response;

            auto status = Stub_->RemoveDirectory(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());  //GRpc layer - OK
            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::SCHEME_ERROR);
            NYql::TIssues issues;
            NYql::IssuesFromMessage(deferred.issues(), issues);
            TString tmp = issues.ToString();
            TString expected = "<main>: Error: Path does not exist, code: 200200\n";
            UNIT_ASSERT_NO_DIFF(tmp, expected);
        }
    }

    Y_UNIT_TEST(MakeListRemoveDirectory) {
        TKikimrWithGrpcAndRootSchemaNoSystemViews server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> Channel_;
        std::unique_ptr<Ydb::Scheme::V1::SchemeService::Stub> Stub_;
        Channel_ = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        TString id;
        {
            Stub_ = Ydb::Scheme::V1::SchemeService::NewStub(Channel_);
            grpc::ClientContext context;

            Ydb::Scheme::MakeDirectoryRequest request;
            TString scheme(
                "path: \"/Root/TheDirectory\"");
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);
            Ydb::Scheme::MakeDirectoryResponse response;

            auto status = Stub_->MakeDirectory(&context, request, &response);
            auto operation = response.operation();
            UNIT_ASSERT(status.ok());  //GRpc layer - OK
            //UNIT_ASSERT(operation.ready() == false); //Not finished yet
            //id = operation.id();
        }
        /*
        {
            auto status = WaitForStatus(Channel_, id);
            UNIT_ASSERT(status == Ydb::StatusIds::SUCCESS);
        }
        */
        {
            Stub_ = Ydb::Scheme::V1::SchemeService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Scheme::ListDirectoryRequest request;
            TString scheme(
                "path: \"/Roo\"");
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);
            Ydb::Scheme::ListDirectoryResponse response;

            auto status = Stub_->ListDirectory(&context, request, &response);
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT(response.operation().ready() == true);
            UNIT_ASSERT(response.operation().status() == Ydb::StatusIds::SCHEME_ERROR);
        }
        {
            Stub_ = Ydb::Scheme::V1::SchemeService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Scheme::ListDirectoryRequest request;
            TString scheme(
                "path: \"/Root\"");
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);
            Ydb::Scheme::ListDirectoryResponse response;

            auto status = Stub_->ListDirectory(&context, request, &response);
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT(response.operation().ready() == true);
            UNIT_ASSERT(response.operation().status() == Ydb::StatusIds::SUCCESS);
            Ydb::Scheme::ListDirectoryResult result;
            response.operation().result().UnpackTo(&result);
            result.mutable_self()->clear_created_at(); // variadic part
            for (auto& child : *result.mutable_children()) {
                child.clear_created_at();
            }
            TString tmp;
            google::protobuf::TextFormat::PrintToString(result, &tmp);
            const TString expected = "self {\n"
                "  name: \"Root\"\n"
                "  owner: \"root@builtin\"\n"
                "  type: DIRECTORY\n"
                "}\n"
                "children {\n"
                "  name: \"TheDirectory\"\n"
                "  owner: \"root@builtin\"\n"
                "  type: DIRECTORY\n"
                "}\n";
            UNIT_ASSERT_NO_DIFF(tmp, expected);
        }
        {
            Stub_ = Ydb::Scheme::V1::SchemeService::NewStub(Channel_);
            grpc::ClientContext context;

            Ydb::Scheme::RemoveDirectoryRequest request;
            TString scheme(
                "path: \"/Root/TheDirectory\"");
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);
            Ydb::Scheme::RemoveDirectoryResponse response;

            auto status = Stub_->RemoveDirectory(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());  //GRpc layer - OK
            UNIT_ASSERT(response.operation().status() == Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT(deferred.ready() == true);
            //id = deferred.id();
        }
        /*
        {
            auto status = WaitForStatus(Channel_, id);
            UNIT_ASSERT(status == Ydb::StatusIds::SUCCESS);
        }
        */
    }

    Y_UNIT_TEST(GetOperationBadRequest) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> Channel_;
        Channel_ = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        {
            auto status = WaitForStatus(Channel_, "");
            UNIT_ASSERT_VALUES_EQUAL(status, Ydb::StatusIds::BAD_REQUEST);
        }
        {
            auto status = WaitForStatus(Channel_, "ydb://...");
            UNIT_ASSERT_VALUES_EQUAL(status, Ydb::StatusIds::BAD_REQUEST);
        }
        {
            auto status = WaitForStatus(Channel_, "ydb://operation/1");
            UNIT_ASSERT_VALUES_EQUAL(status, Ydb::StatusIds::BAD_REQUEST);
        }
        {
            auto status = WaitForStatus(Channel_, "ydb://operation/1?txid=42");
            UNIT_ASSERT_VALUES_EQUAL(status, Ydb::StatusIds::BAD_REQUEST);
        }
        {
            auto status = WaitForStatus(Channel_, "ydb://operation/1?txid=aaa&sstid=bbbb");
            UNIT_ASSERT_VALUES_EQUAL(status, Ydb::StatusIds::BAD_REQUEST);
        }

    }
/*
    Y_UNIT_TEST(GetOperationUnknownId) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> Channel_;
        Channel_ = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        {
            auto status = WaitForStatus(Channel_, "ydb://operation/1?txid=42&sstid=66");
            UNIT_ASSERT(status == Ydb::StatusIds::BAD_REQUEST);
        }
    }
*/
    Y_UNIT_TEST(CreateTableBadRequest) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> Channel_;
        std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
        Channel_ = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
        grpc::ClientContext context;
        Ydb::Table::CreateTableRequest request;
        Ydb::Table::CreateTableResponse response;

        auto status = Stub_->CreateTable(&context, request, &response);
        auto deferred = response.operation();
        UNIT_ASSERT(status.ok()); //GRpc layer - OK
        UNIT_ASSERT(deferred.ready() == true); //Ready to get status
        UNIT_ASSERT(deferred.status() == Ydb::StatusIds::BAD_REQUEST); //But with error
    }

    static void CreateTableBadRequest(const TString& scheme,
        const TString& expectedMsg, const Ydb::StatusIds::StatusCode expectedStatus)
    {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> Channel_;
        std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
        Channel_ = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
        grpc::ClientContext context;
        Ydb::Table::CreateTableRequest request;
        ::google::protobuf::TextFormat::ParseFromString(scheme, &request);

        Ydb::Table::CreateTableResponse response;

        auto status = Stub_->CreateTable(&context, request, &response);
        auto deferred = response.operation();
        UNIT_ASSERT(status.ok()); //GRpc layer - OK
        UNIT_ASSERT(deferred.ready() == true); //Ready to get status

        NYql::TIssues issues;
        NYql::IssuesFromMessage(deferred.issues(), issues);
        TString tmp = issues.ToString();

        UNIT_ASSERT_NO_DIFF(tmp, expectedMsg);
        UNIT_ASSERT_VALUES_EQUAL(deferred.status(), expectedStatus); //But with error
    }

    Y_UNIT_TEST(CreateTableBadRequest2) {
        TString scheme(R"___(
            path: "/Root/TheTable"
            columns { name: "Key"             type: { optional_type { item { type_id: UINT64 } } } }
            columns { name: "Value"           type: { optional_type { item { type_id: UTF8   } } } }
            primary_key: ["BlaBla"]
        )___");

        TString expected("<main>: Error: Unknown column 'BlaBla' specified in key column list\n");

        CreateTableBadRequest(scheme, expected, Ydb::StatusIds::SCHEME_ERROR);
    }

    Y_UNIT_TEST(CreateTableBadRequest3) {
        TString scheme(R"___(
            path: "/Root/TheTable"
            columns { name: "Key"             type: { optional_type { item { type_id: UINT64 } } } }
            columns { name: "Value"           type: { optional_type { item { type_id: UTF8   } } } }
        )___");

        TString expected("<main>: Error: At least one primary key should be specified\n");

        CreateTableBadRequest(scheme, expected, Ydb::StatusIds::BAD_REQUEST);
    }

    Y_UNIT_TEST(DropTableBadRequest) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> Channel_;
        std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
        Channel_ = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        {
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;

            Ydb::Table::DropTableRequest request;
            Ydb::Table::DropTableResponse response;

            auto status = Stub_->DropTable(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok()); //GRpc layer - OK
            UNIT_ASSERT(deferred.ready() == true); //Ready to get status
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::BAD_REQUEST); //But with error
        }
        {
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;

            Ydb::Table::DropTableRequest request;
            TString scheme(
                "path: \"/Root/NotExists\"");
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);

            Ydb::Table::DropTableResponse response;

            auto status = Stub_->DropTable(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok()); //GRpc layer - OK
            UNIT_ASSERT(deferred.ready() == true); //Ready to get status
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::SCHEME_ERROR); //But with error
        }

    }

    Y_UNIT_TEST(AlterTableAddIndexBadRequest) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> Channel_;
        Channel_ = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::CreateTableRequest request;
            TString scheme(
                "path: \"/Root/TheTable\""
                "columns { name: \"Key\"             type: { optional_type { item { type_id: UINT64 } } } }"
                "columns { name: \"Value\"           type: { optional_type { item { type_id: UTF8   } } } }"
                "primary_key: [\"Key\"]");
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);
            Ydb::Table::CreateTableResponse response;

            auto status = Stub_->CreateTable(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());  //GRpc layer - OK
            UNIT_ASSERT(deferred.ready() == true);
        }
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::AlterTableRequest request;
            TString scheme(
                "path: \"/Root/TheTable\""
                "add_indexes { name: \"ByValue\" index_columns: \"Value\" }"
            );
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);
            Ydb::Table::AlterTableResponse response;

            auto status = Stub_->AlterTable(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::BAD_REQUEST);
            NYql::TIssues issues;
            NYql::IssuesFromMessage(deferred.issues(), issues);
            UNIT_ASSERT(issues.ToString().Contains("invalid or unset index type"));
        }
    }

    Y_UNIT_TEST(CreateAlterCopyAndDropTable) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> Channel_;
        Channel_ = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        TString id;
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::CreateTableRequest request;
            TString scheme(
                "path: \"/Root/TheTable\""
                "columns { name: \"Key\"             type: { optional_type { item { type_id: UINT64 } } } }"
                "columns { name: \"Value\"           type: { optional_type { item { type_id: UTF8   } } } }"
                "primary_key: [\"Key\"]");
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);
            Ydb::Table::CreateTableResponse response;

            auto status = Stub_->CreateTable(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());  //GRpc layer - OK
            UNIT_ASSERT(deferred.ready() == true);
            //id = deferred.id();
        }
        /*
        {
            auto status = WaitForStatus(Channel_, id);
            UNIT_ASSERT(status == Ydb::StatusIds::SUCCESS);
        }
        */
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::DescribeTableRequest request;
            TString scheme(
                "path: \"/Root/TheTable\"");
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);
            Ydb::Table::DescribeTableResponse response;

            auto status = Stub_->DescribeTable(&context, request, &response);
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT(response.operation().ready() == true);
            UNIT_ASSERT(response.operation().status() == Ydb::StatusIds::SUCCESS);
            Ydb::Table::DescribeTableResult result;
            response.operation().result().UnpackTo(&result);
            result.mutable_self()->clear_created_at(); // variadic part
            TString tmp;
            google::protobuf::TextFormat::PrintToString(result, &tmp);
            const TString expected = R"___(self {
  name: "TheTable"
  owner: "root@builtin"
  type: TABLE
}
columns {
  name: "Key"
  type {
    optional_type {
      item {
        type_id: UINT64
      }
    }
  }
}
columns {
  name: "Value"
  type {
    optional_type {
      item {
        type_id: UTF8
      }
    }
  }
}
primary_key: "Key"
partitioning_settings {
  partitioning_by_size: DISABLED
  partitioning_by_load: DISABLED
  min_partitions_count: 1
}
)___";
           UNIT_ASSERT_NO_DIFF(tmp, expected);
        }
        {
            std::unique_ptr<Ydb::Scheme::V1::SchemeService::Stub> Stub_;
            Stub_ = Ydb::Scheme::V1::SchemeService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Scheme::DescribePathRequest request;
            TString scheme(
                "path: \"/Root/TheTable\"");
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);
            Ydb::Scheme::DescribePathResponse response;

            auto status = Stub_->DescribePath(&context, request, &response);
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT(response.operation().ready() == true);
            UNIT_ASSERT(response.operation().status() == Ydb::StatusIds::SUCCESS);
            Ydb::Scheme::DescribePathResult result;
            response.operation().result().UnpackTo(&result);
            result.mutable_self()->clear_created_at(); // variadic part
            TString tmp;
            google::protobuf::TextFormat::PrintToString(result, &tmp);
            const TString expected = "self {\n"
            "  name: \"TheTable\"\n"
            "  owner: \"root@builtin\"\n"
            "  type: TABLE\n"
            "}\n";
            UNIT_ASSERT_NO_DIFF(tmp, expected);
        }
        id.clear();
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::AlterTableRequest request;
            TString scheme(
                "path: \"/Root/TheTable\""
                "add_columns { name: \"Value2\"           type: { optional_type { item { type_id: UTF8 } } } }"
                "drop_columns: [\"Value\"]");
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);
            Ydb::Table::AlterTableResponse response;

            auto status = Stub_->AlterTable(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::SUCCESS);
            //id = deferred.id();
        }
        /*
        {
            auto status = WaitForStatus(Channel_, id);
            UNIT_ASSERT(status == Ydb::StatusIds::SUCCESS);
        }
        */
        id.clear();
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::CopyTableRequest request;
            TString scheme(
                "source_path: \"/Root/TheTable\""
                "destination_path: \"/Root/TheTable2\"");
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);
            Ydb::Table::CopyTableResponse response;
            auto status = Stub_->CopyTable(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT(deferred.ready() == true);
            //id = deferred.id();
        }
        /*
        {
            auto status = WaitForStatus(Channel_, id);
            UNIT_ASSERT(status == Ydb::StatusIds::SUCCESS);
        }
        */
        id.clear();
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::CopyTablesRequest request;
            TString scheme(R"(
                tables {
                    source_path: "/Root/TheTable"
                    destination_path: "/Root/TheTable3"
                }
                tables {
                    source_path: "/Root/TheTable2"
                    destination_path: "/Root/TheTable4"
                }
            )");
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);
            Ydb::Table::CopyTablesResponse response;
            auto status = Stub_->CopyTables(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT(deferred.ready() == true);
            //id = deferred.id();
        }
        /*
        {
            auto status = WaitForStatus(Channel_, id);
            UNIT_ASSERT(status == Ydb::StatusIds::SUCCESS);
        }
        */
        id.clear();
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::DropTableRequest request;
            TString scheme(
                "path: \"/Root/TheTable\"");
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);
            Ydb::Table::DropTableResponse response;
            auto status = Stub_->DropTable(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT(response.operation().status() == Ydb::StatusIds::SUCCESS);
            //id = deferred.id();
        }
        /*
        {
            auto status = WaitForStatus(Channel_, id);
            UNIT_ASSERT(status == Ydb::StatusIds::SUCCESS);
        }
        */
    }
    Y_UNIT_TEST(CreateTableWithIndex) {
        TKikimrWithGrpcAndRootSchema server;
        server.Server_->GetRuntime()->GetAppData().AllowPrivateTableDescribeForTest = true;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> channel;
        channel = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        TString id;
/*
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(channel);
            grpc::ClientContext context;
            Ydb::Table::CreateTableRequest request;
            TString scheme(
                "path: \"/Root/TheTable\""
                "columns { name: \"Key\"             type: { optional_type { item { type_id: UINT64 } } } }"
                "columns { name: \"IValue\"          type: { optional_type { item { type_id: UTF8   } } } }"
                "primary_key: [\"Key\"]"
                "indexes { name: \"IndexedValue\"    index_columns:   [\"IValue\"]        global_index { table_profile { partitioning_policy { uniform_partitions: 16 } } } }"
            );
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);

            Ydb::Table::CreateTableResponse response;

            auto status = Stub_->CreateTable(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());  //GRpc layer - OK
            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT_VALUES_EQUAL(deferred.status(), Ydb::StatusIds::GENERIC_ERROR);
        }
*/
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(channel);
            grpc::ClientContext context;
            Ydb::Table::CreateTableRequest request;
            TString scheme(
                "path: \"/Root/TheTable1\""
                "columns { name: \"Key\"             type: { optional_type { item { type_id: UINT64 } } } }"
                "columns { name: \"IValue\"          type: { optional_type { item { type_id: UTF8   } } } }"
                "primary_key: [\"Key\"]"
            );
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);

            Ydb::TypedValue point;
            auto &keyType = *point.mutable_type()->mutable_tuple_type();
            keyType.add_elements()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UTF8);
            auto &keyVal = *point.mutable_value();
            keyVal.add_items()->set_text_value("q");
            auto index = request.add_indexes();
            index->set_name("IndexedValue");
            index->add_index_columns("IValue");
//            auto points = index->mutable_global_index()->mutable_table_profile()->mutable_partitioning_policy()->mutable_explicit_partitions();
//            points->add_split_points()->CopyFrom(point);

            Ydb::Table::CreateTableResponse response;

            auto status = Stub_->CreateTable(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT_VALUES_EQUAL(deferred.status(), Ydb::StatusIds::SUCCESS);
        }
/*
        {
            TString sessionId = CreateSession(channel);
            auto result = DescribeTable(channel, sessionId, "/Root/TheTable1/IndexedValue/indexImplTable");
            const TString expected = R"__(type {
  tuple_type {
    elements {
      optional_type {
        item {
          type_id: UTF8
        }
      }
    }
    elements {
      optional_type {
        item {
          type_id: UINT64
        }
      }
    }
  }
}
value {
  items {
    text_value: "q"
  }
  items {
    null_flag_value: NULL_VALUE
  }
}
)__";
            TString tmp;
            google::protobuf::TextFormat::PrintToString(result.shard_key_bounds(0), &tmp);
            UNIT_ASSERT_VALUES_EQUAL(result.shard_key_bounds().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(tmp, expected);
        }

        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(channel);
            grpc::ClientContext context;
            Ydb::Table::CreateTableRequest request;
            TString scheme(
                "path: \"/Root/TheTable2\""
                "columns { name: \"Key\"             type: { optional_type { item { type_id: UINT64 } } } }"
                "columns { name: \"IValue\"          type: { optional_type { item { type_id: UINT32 } } } }"
                "primary_key: [\"Key\"]"
                "indexes { name: \"IndexedValue\"    index_columns:   [\"IValue\"]        global_index { table_profile { partitioning_policy { uniform_partitions: 16 } } } }"
            );
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);

            Ydb::Table::CreateTableResponse response;

            auto status = Stub_->CreateTable(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());  //GRpc layer - OK
            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT_VALUES_EQUAL(deferred.status(), Ydb::StatusIds::SUCCESS);
        }

        {
            TString sessionId = CreateSession(channel);
            auto result = DescribeTable(channel, sessionId, "/Root/TheTable2/IndexedValue/indexImplTable");
            UNIT_ASSERT_VALUES_EQUAL(result.shard_key_bounds().size(), 15);
            UNIT_ASSERT_VALUES_EQUAL(result.shard_key_bounds(0).value().items(0).uint32_value(), ((1ull << 32) - 1) / 16);
        }
*/

        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(channel);
            grpc::ClientContext context;
            Ydb::Table::CreateTableRequest request;
            TString scheme(
                "path: \"/Root/TheTable\""
                "columns { name: \"Key\"             type: { optional_type { item { type_id: UINT64 } } } }"
                "columns { name: \"IValue\"          type: { optional_type { item { type_id: UTF8   } } } }"
                "primary_key: [\"Key\"]"
                "indexes { name: \"IndexedValue\"    index_columns:   [\"IValue\"]        global_index { } }"
            );
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);

            Ydb::Table::CreateTableResponse response;

            auto status = Stub_->CreateTable(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());  //GRpc layer - OK
            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::SUCCESS);
        }

        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(channel);
            grpc::ClientContext context;
            Ydb::Table::DescribeTableRequest request;
            TString scheme(
                "path: \"/Root/TheTable\"");
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);
            Ydb::Table::DescribeTableResponse response;

            auto status = Stub_->DescribeTable(&context, request, &response);
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT(response.operation().ready() == true);
            UNIT_ASSERT(response.operation().status() == Ydb::StatusIds::SUCCESS);
            Ydb::Table::DescribeTableResult result;
            response.operation().result().UnpackTo(&result);
            result.mutable_self()->clear_created_at(); // variadic part
            TString tmp;
            google::protobuf::TextFormat::PrintToString(result, &tmp);
            const TString expected = R"___(self {
  name: "TheTable"
  owner: "root@builtin"
  type: TABLE
}
columns {
  name: "Key"
  type {
    optional_type {
      item {
        type_id: UINT64
      }
    }
  }
}
columns {
  name: "IValue"
  type {
    optional_type {
      item {
        type_id: UTF8
      }
    }
  }
}
primary_key: "Key"
indexes {
  name: "IndexedValue"
  index_columns: "IValue"
  global_index {
    settings {
      partitioning_settings {
        partitioning_by_size: ENABLED
        partition_size_mb: 2048
        partitioning_by_load: DISABLED
        min_partitions_count: 1
      }
    }
  }
  status: STATUS_READY
}
partitioning_settings {
  partitioning_by_size: DISABLED
  partitioning_by_load: DISABLED
  min_partitions_count: 1
}
)___";
           UNIT_ASSERT_NO_DIFF(tmp, expected);
        }
        {
            TString sessionId = CreateSession(channel);
            const TString query1(R"(
            UPSERT INTO `/Root/TheTable` (Key, IValue) VALUES
                (1, "Secondary1"),
                (2, "Secondary2"),
                (3, "Secondary3");
            )");
            ExecYql(channel, sessionId, query1);
        }

        {
            TString sessionId = CreateSession(channel);
            ExecYql(channel, sessionId,
                "SELECT * FROM `/Root/TheTable`;");
        }

    }

    Y_UNIT_TEST(CreateYqlSession) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> Channel_;
        Channel_ = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::CreateSessionRequest request;
            Ydb::Table::CreateSessionResponse response;

            auto status = Stub_->CreateSession(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());  //GRpc layer - OK
            UNIT_ASSERT(deferred.ready() == true);
        }
    }

    Y_UNIT_TEST(CreateDeleteYqlSession) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> Channel_;
        Channel_ = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        TString sessionId;
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::CreateSessionRequest request;
            Ydb::Table::CreateSessionResponse response;

            auto status = Stub_->CreateSession(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());  //GRpc layer - OK
            UNIT_ASSERT(deferred.ready() == true);
            Ydb::Table::CreateSessionResult result;

            deferred.result().UnpackTo(&result);
            sessionId = result.session_id();
        }
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::DeleteSessionRequest request;
            request.set_session_id(sessionId);
            Ydb::Table::DeleteSessionResponse response;

            auto status = Stub_->DeleteSession(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT(deferred.ready() == true);
        }

    }

    Y_UNIT_TEST(ExecuteQueryBadRequest) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> Channel_;
        Channel_ = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());

        TString sessionId;
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::CreateSessionRequest request;
            Ydb::Table::CreateSessionResponse response;

            auto status = Stub_->CreateSession(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT(deferred.ready() == true);
            Ydb::Table::CreateSessionResult result;

            deferred.result().UnpackTo(&result);
            sessionId = result.session_id();
        }

        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::ExecuteDataQueryRequest request;
            request.set_session_id(sessionId);
            Ydb::Table::ExecuteDataQueryResponse response;
            auto status = Stub_->ExecuteDataQuery(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());  //GRpc layer - OK
            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT_VALUES_EQUAL(deferred.status(), Ydb::StatusIds::BAD_REQUEST);
        }
    }

    Y_UNIT_TEST(ExecuteQueryImplicitSession) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> Channel_;
        Channel_ = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::ExecuteDataQueryRequest request;
            request.mutable_query()->set_yql_text("SELECT 1 as a, 'qwerty' as b, 43.5 as c UNION ALL SELECT 11 as a, 'asdfgg' as b, Null as c;");
            Ydb::Table::ExecuteDataQueryResponse response;
            auto status = Stub_->ExecuteDataQuery(&context, request, &response);
            UNIT_ASSERT(status.ok());
            auto deferred = response.operation();
            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::BAD_REQUEST);
        }
    }

    Y_UNIT_TEST(ExecuteQueryExplicitSession) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> Channel_;
        Channel_ = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        TString sessionId;
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::CreateSessionRequest request;
            Ydb::Table::CreateSessionResponse response;

            auto status = Stub_->CreateSession(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT(deferred.ready() == true);
            Ydb::Table::CreateSessionResult result;

            deferred.result().UnpackTo(&result);
            sessionId = result.session_id();
        }

        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::KeepAliveRequest request;
            request.set_session_id(sessionId);
            Ydb::Table::KeepAliveResponse response;
            auto status = Stub_->KeepAlive(&context, request, &response);
            UNIT_ASSERT(status.ok());
            auto deferred = response.operation();
            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::SUCCESS);
        }

        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::ExecuteDataQueryRequest request;
            request.set_session_id(sessionId);
            request.mutable_query()->set_yql_text("SELECT 1 as a, 'qwerty' as b, 43.5 as c UNION ALL SELECT 11 as a, 'asdfgg' as b, Null as c;");
            request.mutable_tx_control()->mutable_begin_tx()->mutable_serializable_read_write();
            request.mutable_tx_control()->set_commit_tx(true);
            Ydb::Table::ExecuteDataQueryResponse response;
            auto status = Stub_->ExecuteDataQuery(&context, request, &response);
            UNIT_ASSERT(status.ok());
            auto deferred = response.operation();
            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::SUCCESS);

            Ydb::Table::ExecuteQueryResult result;
            deferred.result().UnpackTo(&result);
            {
                TString tmp;
                google::protobuf::TextFormat::PrintToString(result, &tmp);
                const TString expected =
                    "result_sets {\n"
                    "  columns {\n"
                    "    name: \"a\"\n"
                    "    type {\n"
                    "      type_id: INT32\n"
                    "    }\n"
                    "  }\n"
                    "  columns {\n"
                    "    name: \"b\"\n"
                    "    type {\n"
                    "      type_id: STRING\n"
                    "    }\n"
                    "  }\n"
                    "  columns {\n"
                    "    name: \"c\"\n"
                    "    type {\n"
                    "      optional_type {\n"
                    "        item {\n"
                    "          type_id: DOUBLE\n"
                    "        }\n"
                    "      }\n"
                    "    }\n"
                    "  }\n"
                    "  rows {\n"
                    "    items {\n"
                    "      int32_value: 1\n"
                    "    }\n"
                    "    items {\n"
                    "      bytes_value: \"qwerty\"\n"
                    "    }\n"
                    "    items {\n"
                    "      double_value: 43.5\n"
                    "    }\n"
                    "  }\n"
                    "  rows {\n"
                    "    items {\n"
                    "      int32_value: 11\n"
                    "    }\n"
                    "    items {\n"
                    "      bytes_value: \"asdfgg\"\n"
                    "    }\n"
                    "    items {\n"
                    "      null_flag_value: NULL_VALUE\n"
                    "    }\n"
                    "  }\n"
                    "}\n"
                    "tx_meta {\n"
                    "}\n";
                UNIT_ASSERT_NO_DIFF(tmp, expected);
                TResultSet resultSet(result.result_sets(0));
                UNIT_ASSERT_EQUAL(resultSet.ColumnsCount(), 3);

                int row = 0;
                TResultSetParser rsParser(resultSet);
                while (rsParser.TryNextRow()) {
                    switch (row) {
                        case 0: {
                            UNIT_ASSERT_EQUAL(rsParser.ColumnParser(0).GetInt32(), 1);
                            UNIT_ASSERT_EQUAL(rsParser.ColumnParser(1).GetString(), "qwerty");
                            UNIT_ASSERT_EQUAL(rsParser.ColumnParser(2).GetOptionalDouble(), 43.5);
                        }
                        break;
                        case 1: {
                            UNIT_ASSERT_EQUAL(rsParser.ColumnParser(0).GetInt32(), 11);
                            UNIT_ASSERT_EQUAL(rsParser.ColumnParser(1).GetString(), "asdfgg");
                            rsParser.ColumnParser(2).OpenOptional();
                            UNIT_ASSERT_EQUAL(rsParser.ColumnParser(2).IsNull(), true);
                        }
                        break;
                        default: {
                            UNIT_ASSERT(false);
                        }
                    }
                    row++;
                }
            }
        }
    }

    Y_UNIT_TEST(ExecuteQueryWithUuid) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> Channel_;
        Channel_ = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        TString sessionId;
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::CreateSessionRequest request;
            Ydb::Table::CreateSessionResponse response;

            auto status = Stub_->CreateSession(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT(deferred.ready() == true);
            Ydb::Table::CreateSessionResult result;

            deferred.result().UnpackTo(&result);
            sessionId = result.session_id();
        }
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::ExecuteDataQueryRequest request;
            request.set_session_id(sessionId);
            request.mutable_query()->set_yql_text(R"___(SELECT CAST("5ca32c22-841b-11e8-adc0-fa7ae01bbebc" AS Uuid);)___");
            request.mutable_tx_control()->mutable_begin_tx()->mutable_serializable_read_write();
            request.mutable_tx_control()->set_commit_tx(true);
            Ydb::Table::ExecuteDataQueryResponse response;
            auto status = Stub_->ExecuteDataQuery(&context, request, &response);
            UNIT_ASSERT(status.ok());
            auto deferred = response.operation();
            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::SUCCESS);

            Ydb::Table::ExecuteQueryResult result;
            deferred.result().UnpackTo(&result);
            {
                TString expected = R"___(result_sets {
  columns {
    name: "column0"
    type {
      optional_type {
        item {
          type_id: UUID
        }
      }
    }
  }
  rows {
    items {
      low_128: 1290426546294828066
      high_128: 13600338575655354541
    }
  }
}
tx_meta {
}
)___";
                TString tmp;
                google::protobuf::TextFormat::PrintToString(result, &tmp);
                UNIT_ASSERT_NO_DIFF(tmp, expected);
            }
        }
    }

    Y_UNIT_TEST(SdkUuid) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        TString location = TStringBuilder() << "localhost:" << grpc;

        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(location));
        auto client = NYdb::NTable::TTableClient(connection);
        auto session = client.CreateSession().ExtractValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            SELECT CAST("5ca32c22-841b-11e8-adc0-fa7ae01bbebc" AS Uuid);
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();

        UNIT_ASSERT(result.IsSuccess());

        TString expectedJson = R"({"column0":"5ca32c22-841b-11e8-adc0-fa7ae01bbebc"}
)";
        UNIT_ASSERT_VALUES_EQUAL(expectedJson, NYdb::FormatResultSetJson(result.GetResultSet(0), NYdb::EBinaryStringEncoding::Base64));

        UNIT_ASSERT_VALUES_EQUAL(R"([[["5ca32c22-841b-11e8-adc0-fa7ae01bbebc"]]])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(SdkUuidViaParams) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        TString location = TStringBuilder() << "localhost:" << grpc;

        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(location));
        auto client = NYdb::NTable::TTableClient(connection);
        auto session = client.CreateSession().ExtractValueSync().GetSession();

        auto param = client.GetParamsBuilder()
            .AddParam("$in")
                .BeginList()
                .AddListItem()
                    .BeginStruct()
                        .AddMember("u").Uuid(TUuidValue("5ca32c22-841b-11e8-adc0-fa7ae01bbebc"))
                    .EndStruct()
                .EndList()
                .Build()
            .Build();
        auto result = session.ExecuteDataQuery(R"(
            DECLARE $in AS List<Struct<u: Uuid>>;
            SELECT * FROM AS_TABLE($in);
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), param).ExtractValueSync();

        UNIT_ASSERT(result.IsSuccess());

        TString expectedJson = R"({"u":"5ca32c22-841b-11e8-adc0-fa7ae01bbebc"}
)";
        UNIT_ASSERT_VALUES_EQUAL(expectedJson, NYdb::FormatResultSetJson(result.GetResultSet(0), NYdb::EBinaryStringEncoding::Base64));

        UNIT_ASSERT_VALUES_EQUAL(R"([["5ca32c22-841b-11e8-adc0-fa7ae01bbebc"]])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
    }


    Y_UNIT_TEST(ExecuteQueryWithParametersBadRequest) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> Channel_;
        Channel_ = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        TString sessionId;
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::CreateSessionRequest request;
            Ydb::Table::CreateSessionResponse response;

            auto status = Stub_->CreateSession(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT(deferred.ready() == true);
            Ydb::Table::CreateSessionResult result;

            deferred.result().UnpackTo(&result);
            sessionId = result.session_id();
        }

        std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
        Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
        Ydb::Table::ExecuteDataQueryRequest request;
        request.set_session_id(sessionId);
        request.mutable_query()->set_yql_text("DECLARE $param1 AS Tuple<Int32,Bool>; SELECT $param1 AS Tuple;");
        request.mutable_tx_control()->mutable_begin_tx()->mutable_serializable_read_write();
        request.mutable_tx_control()->set_commit_tx(true);

        {
            // Bad type Kind
            ::google::protobuf::Map<TString, Ydb::TypedValue> parameters;

            const TString type = R"(
                type_id: TYPE_UNDEFINED
            )";
            google::protobuf::TextFormat::ParseFromString(type, parameters["$param1"].mutable_type());

            const TString value = R"(
                int32_value: 10
            )";
            google::protobuf::TextFormat::ParseFromString(value, parameters["$param1"].mutable_value());

            *request.mutable_parameters() = parameters;
            Ydb::Table::ExecuteDataQueryResponse response;
            grpc::ClientContext context;
            auto status = Stub_->ExecuteDataQuery(&context, request, &response);
            UNIT_ASSERT(status.ok());
            auto deferred = response.operation();
            UNIT_ASSERT(deferred.ready() == true);
            NYql::TIssues issues;
            NYql::IssuesFromMessage(deferred.issues(), issues);
            issues.PrintTo(Cerr);
            UNIT_ASSERT_VALUES_EQUAL(deferred.status(), Ydb::StatusIds::BAD_REQUEST);
        }

        {
            // Value mismatch
            ::google::protobuf::Map<TString, Ydb::TypedValue> parameters;

            const TString type = R"(
                tuple_type {
                    elements {
                        type_id: Int32
                    }
                    elements {
                        type_id: Bool
                    }
                }
            )";
            google::protobuf::TextFormat::ParseFromString(type, parameters["$param1"].mutable_type());

            const TString value = R"(
                int32_value: 10
            )";
            google::protobuf::TextFormat::ParseFromString(value, parameters["$param1"].mutable_value());

            *request.mutable_parameters() = parameters;
            Ydb::Table::ExecuteDataQueryResponse response;
            grpc::ClientContext context;
            auto status = Stub_->ExecuteDataQuery(&context, request, &response);
            UNIT_ASSERT(status.ok());
            auto deferred = response.operation();
            UNIT_ASSERT(deferred.ready() == true);
            NYql::TIssues issues;
            NYql::IssuesFromMessage(deferred.issues(), issues);
            issues.PrintTo(Cerr);
            UNIT_ASSERT_VALUES_EQUAL(deferred.status(), Ydb::StatusIds::BAD_REQUEST);
        }
    }

    Y_UNIT_TEST(ExecuteQueryWithParametersExplicitSession) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> Channel_;
        Channel_ = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        TString sessionId;
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::CreateSessionRequest request;
            Ydb::Table::CreateSessionResponse response;

            auto status = Stub_->CreateSession(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT(deferred.ready() == true);
            Ydb::Table::CreateSessionResult result;

            deferred.result().UnpackTo(&result);
            sessionId = result.session_id();
        }
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::ExecuteDataQueryRequest request;
            request.mutable_query()->set_yql_text("DECLARE $paramName AS String; SELECT $paramName;");
            request.mutable_tx_control()->mutable_begin_tx()->mutable_serializable_read_write();
            request.mutable_tx_control()->set_commit_tx(true);
            Ydb::TypedValue parameter;
            {
                const TString type =
                    "type_id: STRING\n";
                google::protobuf::TextFormat::ParseFromString(type, parameter.mutable_type());
                const TString value = "bytes_value: \"Paul\"\n";
                google::protobuf::TextFormat::ParseFromString(value, parameter.mutable_value());
            }

            auto& map = *request.mutable_parameters();
            map["$paramName"] = parameter;
            {
                TString tmp;
                google::protobuf::TextFormat::PrintToString(request, &tmp);
                const TString expected =
                    "tx_control {\n"
                    "  begin_tx {\n"
                    "    serializable_read_write {\n"
                    "    }\n"
                    "  }\n"
                    "  commit_tx: true\n"
                    "}\n"
                    "query {\n"
                    "  yql_text: \"DECLARE $paramName AS String; SELECT $paramName;\"\n"
                    "}\n"
                    "parameters {\n"
                    "  key: \"$paramName\"\n"
                    "  value {\n"
                    "    type {\n"
                    "      type_id: STRING\n"
                    "    }\n"
                    "    value {\n"
                    "      bytes_value: \"Paul\"\n"
                    "    }\n"
                    "  }\n"
                    "}\n";
                UNIT_ASSERT_NO_DIFF(tmp, expected);
            }
            request.set_session_id(sessionId);
            Ydb::Table::ExecuteDataQueryResponse response;
            auto status = Stub_->ExecuteDataQuery(&context, request, &response);
            UNIT_ASSERT(status.ok());
            auto deferred = response.operation();
            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::SUCCESS);

            Ydb::Table::ExecuteQueryResult result;
            deferred.result().UnpackTo(&result);
            {
                TString tmp;
                google::protobuf::TextFormat::PrintToString(result, &tmp);
                const TString expected =
                    "result_sets {\n"
                    "  columns {\n"
                    "    name: \"column0\"\n"
                    "    type {\n"
                    "      type_id: STRING\n"
                    "    }\n"
                    "  }\n"
                    "  rows {\n"
                    "    items {\n"
                    "      bytes_value: \"Paul\"\n"
                    "    }\n"
                    "  }\n"
                    "}\n"
                    "tx_meta {\n"
                    "}\n";
                UNIT_ASSERT_NO_DIFF(tmp, expected);
            }
        }
        // Check Uuid protos as parametr
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::ExecuteDataQueryRequest request;
            request.mutable_query()->set_yql_text("DECLARE $paramName AS Uuid; SELECT $paramName;");
            request.mutable_tx_control()->mutable_begin_tx()->mutable_serializable_read_write();
            request.mutable_tx_control()->set_commit_tx(true);
            Ydb::TypedValue parameter;
            {
                const TString type =
                    "type_id: UUID\n";
                UNIT_ASSERT(google::protobuf::TextFormat::ParseFromString(type, parameter.mutable_type()));
                const TString value = R"(low_128: 1290426546294828066
                                         high_128: 13600338575655354541)";
                UNIT_ASSERT(google::protobuf::TextFormat::ParseFromString(value, parameter.mutable_value()));
            }

            auto& map = *request.mutable_parameters();
            map["$paramName"] = parameter;
            request.set_session_id(sessionId);
            Ydb::Table::ExecuteDataQueryResponse response;
            auto status = Stub_->ExecuteDataQuery(&context, request, &response);
            UNIT_ASSERT(status.ok());
            auto deferred = response.operation();
            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::SUCCESS);

            Ydb::Table::ExecuteQueryResult result;
            deferred.result().UnpackTo(&result);
            {
                TString tmp;
                google::protobuf::TextFormat::PrintToString(result, &tmp);
                const TString expected = R"___(result_sets {
  columns {
    name: "column0"
    type {
      type_id: UUID
    }
  }
  rows {
    items {
      low_128: 1290426546294828066
      high_128: 13600338575655354541
    }
  }
}
tx_meta {
}
)___";
                UNIT_ASSERT_NO_DIFF(tmp, expected);
            }
        }

    }

    Y_UNIT_TEST(ExecuteDmlQuery) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> Channel_;
        Channel_ = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());

        TString sessionId;
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::CreateSessionRequest request;
            Ydb::Table::CreateSessionResponse response;

            auto status = Stub_->CreateSession(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT(deferred.ready() == true);
            Ydb::Table::CreateSessionResult result;

            deferred.result().UnpackTo(&result);
            sessionId = result.session_id();
        }

        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;

            Ydb::Table::ExecuteSchemeQueryRequest request;
            request.set_session_id(sessionId);
            request.set_yql_text(R"(
                CREATE TABLE `Root/TheTable` (
                    Key UINT64,
                    Value UTF8,
                    PRIMARY KEY (Key)
                );
            )");

            Ydb::Table::ExecuteSchemeQueryResponse response;
            auto status = Stub_->ExecuteSchemeQuery(&context, request, &response);
            UNIT_ASSERT(status.ok());
            auto deferred = response.operation();

            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::SUCCESS);
        }

        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;

            Ydb::Table::ExplainDataQueryRequest request;
            request.set_session_id(sessionId);
            request.set_yql_text(R"(
                UPSERT INTO `Root/TheTable` (Key, Value)
                VALUES (1, "One");
            )");

            Ydb::Table::ExplainDataQueryResponse response;
            auto status = Stub_->ExplainDataQuery(&context, request, &response);
            UNIT_ASSERT(status.ok());
            auto deferred = response.operation();

            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::SUCCESS);
        }

        TString txId;
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;

            Ydb::Table::ExecuteDataQueryRequest request;
            request.set_session_id(sessionId);
            request.mutable_query()->set_yql_text(R"(
                UPSERT INTO `Root/TheTable` (Key, Value)
                VALUES (1, "One");
            )");

            auto& txControl = *request.mutable_tx_control();
            txControl.mutable_begin_tx()->mutable_serializable_read_write();

            Ydb::Table::ExecuteDataQueryResponse response;
            auto status = Stub_->ExecuteDataQuery(&context, request, &response);
            UNIT_ASSERT(status.ok());
            auto deferred = response.operation();

            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::SUCCESS);

            Ydb::Table::ExecuteQueryResult result;
            deferred.result().UnpackTo(&result);

            txId = result.tx_meta().id();
            UNIT_ASSERT(!txId.empty());
        }

        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;

            Ydb::Table::ExecuteDataQueryRequest request;
            request.set_session_id(sessionId);
            request.mutable_query()->set_yql_text(R"(
                UPSERT INTO `Root/TheTable` (Key, Value)
                VALUES (2, "Two");
            )");

            auto& txControl = *request.mutable_tx_control();
            txControl.set_tx_id(txId);
            txControl.set_commit_tx(true);

            Ydb::Table::ExecuteDataQueryResponse response;
            auto status = Stub_->ExecuteDataQuery(&context, request, &response);
            UNIT_ASSERT(status.ok());
            auto deferred = response.operation();

            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::SUCCESS);
        }

        TString queryId;
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;

            Ydb::Table::PrepareDataQueryRequest request;
            request.set_session_id(sessionId);
            request.set_yql_text(R"(
                DECLARE $Key AS Uint64;
                SELECT * FROM `Root/TheTable` WHERE Key < $Key;
            )");

            Ydb::Table::PrepareDataQueryResponse response;
            auto status = Stub_->PrepareDataQuery(&context, request, &response);
            UNIT_ASSERT(status.ok());

            auto deferred = response.operation();
            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::SUCCESS);

            Ydb::Table::PrepareQueryResult result;
            deferred.result().UnpackTo(&result);
            queryId = result.query_id();
        }

        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;

            Ydb::Table::ExecuteDataQueryRequest request;
            request.set_session_id(sessionId);
            request.mutable_query()->set_id(queryId);

            auto& txControl = *request.mutable_tx_control();
            txControl.mutable_begin_tx()->mutable_online_read_only();
            txControl.set_commit_tx(true);

            Ydb::TypedValue parameter;
            {
                const TString type =
                    "type_id: UINT64\n";
                google::protobuf::TextFormat::ParseFromString(type, parameter.mutable_type());
                const TString value = "uint64_value: 5\n";
                google::protobuf::TextFormat::ParseFromString(value, parameter.mutable_value());
            }

            auto& map = *request.mutable_parameters();
            map["$Key"] = parameter;

            Ydb::Table::ExecuteDataQueryResponse response;
            auto status = Stub_->ExecuteDataQuery(&context, request, &response);
            UNIT_ASSERT(status.ok());
            auto deferred = response.operation();

            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::SUCCESS);

            Ydb::Table::ExecuteQueryResult result;
            deferred.result().UnpackTo(&result);

            UNIT_ASSERT(result.tx_meta().id().empty());
            UNIT_ASSERT_VALUES_EQUAL(result.result_sets(0).rows_size(), 2);
        }
    }

    Y_UNIT_TEST(CreateYqlSessionExecuteQuery) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> Channel_;
        Channel_ = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        TString sessionId;
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::CreateSessionRequest request;
            Ydb::Table::CreateSessionResponse response;

            auto status = Stub_->CreateSession(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());  //GRpc layer - OK
            UNIT_ASSERT(deferred.ready() == true);
            Ydb::Table::CreateSessionResult result;

            deferred.result().UnpackTo(&result);
            sessionId = result.session_id();
        }
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::ExecuteDataQueryRequest request;
            request.set_session_id(sessionId);
            request.mutable_query()->set_yql_text("SELECT 1, \"qq\"; SELECT 2;");
            request.mutable_tx_control()->mutable_begin_tx()->mutable_serializable_read_write();
            request.mutable_tx_control()->set_commit_tx(true);
            Ydb::Table::ExecuteDataQueryResponse response;
            auto status = Stub_->ExecuteDataQuery(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT(deferred.ready() == true);
        }
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::ExecuteDataQueryRequest request;
            request.set_session_id(sessionId);
            request.mutable_query()->set_yql_text("SELECT * from `Root/NotFound`");
            request.mutable_tx_control()->mutable_begin_tx()->mutable_serializable_read_write();
            request.mutable_tx_control()->set_commit_tx(true);
            Ydb::Table::ExecuteDataQueryResponse response;
            auto status = Stub_->ExecuteDataQuery(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::SCHEME_ERROR);
            NYql::TIssues issues;
            NYql::IssuesFromMessage(deferred.issues(), issues);
            UNIT_ASSERT(HasIssue(issues, NYql::TIssuesIds::KIKIMR_SCHEME_ERROR));
        }

    }

    Y_UNIT_TEST(ExecutePreparedQuery) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();
        std::shared_ptr<grpc::Channel> Channel_;
        Channel_ = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        TString sessionId;
        TString preparedQueryId;
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::CreateSessionRequest request;
            Ydb::Table::CreateSessionResponse response;

            auto status = Stub_->CreateSession(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT(deferred.ready() == true);
            Ydb::Table::CreateSessionResult result;

            deferred.result().UnpackTo(&result);
            sessionId = result.session_id();
        }
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::PrepareDataQueryRequest request;
            request.set_session_id(sessionId);
            request.set_yql_text("DECLARE $paramName AS String; SELECT $paramName;");
            Ydb::Table::PrepareDataQueryResponse response;
            auto status = Stub_->PrepareDataQuery(&context, request, &response);
            UNIT_ASSERT(status.ok());
            auto deferred = response.operation();
            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::SUCCESS);
            Ydb::Table::PrepareQueryResult result;

            deferred.result().UnpackTo(&result);
            preparedQueryId = result.query_id();
        }
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::ExecuteDataQueryRequest request;
            request.set_session_id(sessionId);
            request.mutable_query()->set_id(preparedQueryId);
            request.mutable_tx_control()->mutable_begin_tx()->mutable_serializable_read_write();
            request.mutable_tx_control()->set_commit_tx(true);
            Ydb::TypedValue parameter;
            {
                const TString type =
                    "type_id: STRING\n";
                google::protobuf::TextFormat::ParseFromString(type, parameter.mutable_type());
                const TString value = "bytes_value: \"Paul\"\n";
                google::protobuf::TextFormat::ParseFromString(value, parameter.mutable_value());
            }

            auto& map = *request.mutable_parameters();
            map["$paramName"] = parameter;

            Ydb::Table::ExecuteDataQueryResponse response;
            auto status = Stub_->ExecuteDataQuery(&context, request, &response);
            UNIT_ASSERT(status.ok());
            auto deferred = response.operation();
            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::SUCCESS);
            Ydb::Table::ExecuteQueryResult result;
            deferred.result().UnpackTo(&result);
            {
                TString tmp;
                google::protobuf::TextFormat::PrintToString(result, &tmp);
                const TString expected =
                    "result_sets {\n"
                    "  columns {\n"
                    "    name: \"column0\"\n"
                    "    type {\n"
                    "      type_id: STRING\n"
                    "    }\n"
                    "  }\n"
                    "  rows {\n"
                    "    items {\n"
                    "      bytes_value: \"Paul\"\n"
                    "    }\n"
                    "  }\n"
                    "}\n"
                    "tx_meta {\n"
                    "}\n";
                UNIT_ASSERT_NO_DIFF(tmp, expected);
            }
        }
    }

    Y_UNIT_TEST(ExecuteQueryCache) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> Channel_;
        Channel_ = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        TString sessionId;
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::CreateSessionRequest request;
            Ydb::Table::CreateSessionResponse response;

            auto status = Stub_->CreateSession(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());  //GRpc layer - OK
            UNIT_ASSERT(deferred.ready() == true);
            Ydb::Table::CreateSessionResult result;

            deferred.result().UnpackTo(&result);
            sessionId = result.session_id();
        }

        Ydb::TypedValue parameter;
        {
            const TString type =
                "type_id: STRING\n";
            google::protobuf::TextFormat::ParseFromString(type, parameter.mutable_type());
            const TString value = "bytes_value: \"Paul\"\n";
            google::protobuf::TextFormat::ParseFromString(value, parameter.mutable_value());
        }

        TString queryId;
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::ExecuteDataQueryRequest request;
            request.set_session_id(sessionId);
            request.mutable_query()->set_yql_text("DECLARE $paramName AS String; SELECT $paramName;");
            request.mutable_tx_control()->mutable_begin_tx()->mutable_serializable_read_write();
            request.mutable_tx_control()->set_commit_tx(true);
            request.mutable_query_cache_policy()->set_keep_in_cache(true);
            auto& map = *request.mutable_parameters();
            map["$paramName"] = parameter;
            Ydb::Table::ExecuteDataQueryResponse response;
            auto status = Stub_->ExecuteDataQuery(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT(deferred.ready() == true);

            Ydb::Table::ExecuteQueryResult result;
            deferred.result().UnpackTo(&result);
            UNIT_ASSERT(result.has_query_meta());
            queryId = result.query_meta().id();
            UNIT_ASSERT(!queryId.empty());
            UNIT_ASSERT(!result.query_meta().parameters_types().empty());
        }

        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::ExecuteDataQueryRequest request;
            request.set_session_id(sessionId);
            request.mutable_query()->set_id(queryId);
            request.mutable_tx_control()->mutable_begin_tx()->mutable_serializable_read_write();
            request.mutable_tx_control()->set_commit_tx(true);
            request.mutable_query_cache_policy()->set_keep_in_cache(true);
            auto& map = *request.mutable_parameters();
            map["$paramName"] = parameter;

            Ydb::Table::ExecuteDataQueryResponse response;
            auto status = Stub_->ExecuteDataQuery(&context, request, &response);
            UNIT_ASSERT(status.ok());
            auto deferred = response.operation();
            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::SUCCESS);

            Ydb::Table::ExecuteQueryResult result;
            deferred.result().UnpackTo(&result);
            UNIT_ASSERT(!result.has_query_meta());
        }
    }

    Y_UNIT_TEST(ExplainQuery) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> Channel_;
        Channel_ = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        TString id;
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::CreateTableRequest request;
            TString scheme(
                "path: \"/Root/TheTable\""
                "columns { name: \"Key\"             type: { optional_type: { item: { type_id: UINT64 } } } }"
                "columns { name: \"Value\"           type: { optional_type: { item: { type_id: UTF8   } } } }"
                "primary_key: [\"Key\"]");
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);
            Ydb::Table::CreateTableResponse response;

            auto status = Stub_->CreateTable(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());  //GRpc layer - OK
            UNIT_ASSERT(deferred.ready() == true);
            NYql::TIssues issues;
            NYql::IssuesFromMessage(deferred.issues(), issues);
            TString tmp = issues.ToString();
            Cerr << tmp << Endl;

            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::SUCCESS);
            //id = deferred.id();
        }
        /*
        {
            auto status = WaitForStatus(Channel_, id);
            UNIT_ASSERT(status == Ydb::StatusIds::SUCCESS);
        }
        */

        TString sessionId;
        TString preparedQueryId;
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::CreateSessionRequest request;
            Ydb::Table::CreateSessionResponse response;

            auto status = Stub_->CreateSession(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT(deferred.ready() == true);
            Ydb::Table::CreateSessionResult result;

            deferred.result().UnpackTo(&result);
            sessionId = result.session_id();
        }

        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::ExecuteDataQueryRequest request;
            request.set_session_id(sessionId);
            request.mutable_query()->set_yql_text("UPSERT INTO `Root/TheTable` (Key, Value) VALUES (42, \"data\");");
            request.mutable_tx_control()->mutable_begin_tx()->mutable_serializable_read_write();
            request.mutable_tx_control()->set_commit_tx(true);
            Ydb::Table::ExecuteDataQueryResponse response;
            auto status = Stub_->ExecuteDataQuery(&context, request, &response);
            UNIT_ASSERT(status.ok());
            auto deferred = response.operation();
            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::SUCCESS);
        }

        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::ExplainDataQueryRequest request;
            request.set_session_id(sessionId);
            request.set_yql_text("SELECT COUNT(*) FROM `Root/TheTable`;");
            Ydb::Table::ExplainDataQueryResponse response;
            auto status = Stub_->ExplainDataQuery(&context, request, &response);
            UNIT_ASSERT(status.ok());
            auto deferred = response.operation();
            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::SUCCESS);
        }
    }

    Y_UNIT_TEST(DeleteFromAfterCreate) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> Channel_;
        Channel_ = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        TString sessionId;

        TString id;
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::CreateTableRequest request;
            TString scheme(
                "path: \"/Root/TheTable\""
                "columns { name: \"Key\"             type: { optional_type { item { type_id: UINT64 } } } }"
                "columns { name: \"Value\"           type: { optional_type { item { type_id: UTF8   } } } }"
                "primary_key: [\"Key\", \"Value\"]");
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);
            Ydb::Table::CreateTableResponse response;

            auto status = Stub_->CreateTable(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());  //GRpc layer - OK
            UNIT_ASSERT(deferred.ready() == true);
            //id = deferred.id();
        }
        /*
        {
            auto status = WaitForStatus(Channel_, id);
            UNIT_ASSERT(status == Ydb::StatusIds::SUCCESS);
        }
        */
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::CreateTableRequest request;
            TString scheme(
                "path: \"/Root/TheTable2\""
                "columns {\n"
                "  name: \"id_a\"\n"
                "  type: { optional_type { item { type_id: INT32 } } }"
                "}\n"
                "columns {\n"
                "  name: \"id_b\"\n"
                "  type: { optional_type { item { type_id: INT64 } } }"
                "}\n"
                "columns {\n"
                "  name: \"id_c\"\n"
                "  type: { optional_type { item { type_id: STRING } } }"
                "}\n"
                "columns {\n"
                "  name: \"id_d\"\n"
                "  type: { optional_type { item { type_id: STRING } } }"
                "}\n"
                "primary_key: \"id_a\"\n"
                "primary_key: \"id_b\"\n"
                "primary_key: \"id_c\"\n"
                "primary_key: \"id_d\"");
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);
            Ydb::Table::CreateTableResponse response;

            auto status = Stub_->CreateTable(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT(deferred.ready() == true);
            //id = deferred.id();
        }
        /*
        {
            auto status = WaitForStatus(Channel_, id);
            UNIT_ASSERT(status == Ydb::StatusIds::SUCCESS);
        }
        */
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::CreateSessionRequest request;
            Ydb::Table::CreateSessionResponse response;

            auto status = Stub_->CreateSession(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT(deferred.ready() == true);
            Ydb::Table::CreateSessionResult result;

            deferred.result().UnpackTo(&result);
            sessionId = result.session_id();
        }
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::ExecuteDataQueryRequest request;
            request.set_session_id(sessionId);
            request.mutable_query()->set_yql_text("DELETE FROM `Root/TheTable`;");
            request.mutable_tx_control()->mutable_begin_tx()->mutable_serializable_read_write();
            request.mutable_tx_control()->set_commit_tx(true);
            Ydb::Table::ExecuteDataQueryResponse response;
            auto status = Stub_->ExecuteDataQuery(&context, request, &response);
            UNIT_ASSERT(status.ok());
            auto deferred = response.operation();
            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::SUCCESS);
        }
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::ExecuteDataQueryRequest request;
            request.set_session_id(sessionId);
            request.mutable_query()->set_yql_text("DELETE FROM `Root/TheTable2`;");
            request.mutable_tx_control()->mutable_begin_tx()->mutable_serializable_read_write();
            request.mutable_tx_control()->set_commit_tx(true);
            Ydb::Table::ExecuteDataQueryResponse response;
            auto status = Stub_->ExecuteDataQuery(&context, request, &response);
            UNIT_ASSERT(status.ok());
            auto deferred = response.operation();
            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::SUCCESS);
        }
    }

    Y_UNIT_TEST(ReadTable) {
        TKikimrWithGrpcAndRootSchema server;
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::GRPC_SERVER, NLog::PRI_TRACE);
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::READ_TABLE_API, NLog::PRI_TRACE);
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> Channel_;
        Channel_ = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        TString sessionId;

        TVector<std::tuple<ui64, TString>> data = {
            {42, "data42"},
            {43, "data43"},
            {44, "data44"},
            {45, "data45"},
            {46, "data46"},
            {47, "data47"},
            {48, "data48"},
            {49, "data49"},
            {50, "data50"},
            {51, "data51"},
            {52, "data52"}
        };
        TString id;
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::CreateTableRequest request;
            TString scheme(
                "path: \"/Root/TheTable\""
                "columns { name: \"Key\"             type: { optional_type { item { type_id: UINT64 } } } }"
                "columns { name: \"Value\"           type: { optional_type { item { type_id: UTF8   } } } }"
                "primary_key: [\"Key\", \"Value\"]");
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);
            Ydb::Table::CreateTableResponse response;

            auto status = Stub_->CreateTable(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());  //GRpc layer - OK
            UNIT_ASSERT(deferred.ready() == true); //Not finished yet
        }
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::CreateSessionRequest request;
            Ydb::Table::CreateSessionResponse response;

            auto status = Stub_->CreateSession(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT(deferred.ready() == true);
            Ydb::Table::CreateSessionResult result;

            deferred.result().UnpackTo(&result);
            sessionId = result.session_id();
        }
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::ExecuteDataQueryRequest request;
            request.set_session_id(sessionId);
            TStringBuilder requestBuilder;
            requestBuilder << "UPSERT INTO `Root/TheTable` (Key, Value) VALUES";
            for (auto pair : data) {
                requestBuilder << "(" << std::get<0>(pair) << ", \"" << std::get<1>(pair) << "\"),";
            }
            TString req(requestBuilder);
            req.back() = ';';
            request.mutable_query()->set_yql_text(req);

            request.mutable_tx_control()->mutable_begin_tx()->mutable_serializable_read_write();
            request.mutable_tx_control()->set_commit_tx(true);
            Ydb::Table::ExecuteDataQueryResponse response;
            auto status = Stub_->ExecuteDataQuery(&context, request, &response);
            UNIT_ASSERT(status.ok());
            auto deferred = response.operation();
            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::SUCCESS);
        }
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::ReadTableRequest request;
            Ydb::Table::ReadTableResponse response;

            auto reader = Stub_->StreamReadTable(&context, request);
            bool res = true;
            // Empty request - we expect to get BAD_REQUEST response
            while (res) {
                res = reader->Read(&response);
                if (res) {
                    UNIT_ASSERT(response.status() == Ydb::StatusIds::BAD_REQUEST);
                }
            }
        }
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::ReadTableRequest request;
            Ydb::Table::ReadTableResponse response;

            TString scheme(
                "path: \"/Root/TheTable\""
            );
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);
            auto reader = Stub_->StreamReadTable(&context, request);
            bool res = true;
            while (res) {
                res = reader->Read(&response);
                // Expect all data in first response message
                if (res) {
                    UNIT_ASSERT_EQUAL(response.status(), Ydb::StatusIds::SUCCESS);
                    if (response.result().has_result_set()) {
                        size_t i = 0;
                        UNIT_ASSERT_EQUAL((size_t)response.result().result_set().rows_size(), data.size());
                        for (const auto& row : response.result().result_set().rows()) {
                            const auto& pair = data[i++];
                            UNIT_ASSERT_EQUAL(std::get<0>(pair), row.items(0).uint64_value());
                            UNIT_ASSERT_EQUAL(std::get<1>(pair), row.items(1).text_value());
                        }
                    }
                }
            }
        }
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::ReadTableRequest request;
            Ydb::Table::ReadTableResponse response;

            TString scheme(
                "path: \"/Root/TheTable\""
            );
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);
            auto keyRange = request.mutable_key_range();
            auto greater = keyRange->mutable_greater();
            greater->mutable_type()->mutable_tuple_type()->add_elements()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UINT64);
            greater->mutable_value()->add_items()->set_uint64_value(50);
            auto reader = Stub_->StreamReadTable(&context, request);
            bool res = true;
            while (res) {
                res = reader->Read(&response);
                if (res) {
                    UNIT_ASSERT(response.status() == Ydb::StatusIds::SUCCESS);
                    if (response.result().has_result_set()) {
                        size_t i = 9;
                        UNIT_ASSERT(response.result().result_set().rows_size() == 2);
                        for (const auto& row : response.result().result_set().rows()) {
                            const auto& pair = data[i++];
                            UNIT_ASSERT_EQUAL(std::get<0>(pair), row.items(0).uint64_value());
                            UNIT_ASSERT_EQUAL(std::get<1>(pair), row.items(1).text_value());
                        }
                    }
                }
            }
        }
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::ReadTableRequest request;
            Ydb::Table::ReadTableResponse response;

            TString scheme(
                "path: \"/Root/TheTable\""
            );
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);
            auto keyRange = request.mutable_key_range();
            auto less = keyRange->mutable_less_or_equal();
            less->mutable_type()->mutable_tuple_type()->add_elements()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UINT64);
            less->mutable_value()->add_items()->set_uint64_value(50);
            auto reader = Stub_->StreamReadTable(&context, request);
            bool res = true;
            while (res) {
                res = reader->Read(&response);
                if (res) {
                    UNIT_ASSERT(response.status() == Ydb::StatusIds::SUCCESS);
                    if (response.result().has_result_set()) {
                        UNIT_ASSERT(response.result().result_set().rows_size() == 9);
                        size_t i = 0;
                        for (const auto& row : response.result().result_set().rows()) {
                            const auto& pair = data[i++];
                            UNIT_ASSERT_EQUAL(std::get<0>(pair), row.items(0).uint64_value());
                            UNIT_ASSERT_EQUAL(std::get<1>(pair), row.items(1).text_value());
                        }
                    }
                }
            }
        }
    }

    Y_UNIT_TEST(OperationTimeout) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> Channel_;
        Channel_ = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        {
            std::unique_ptr<Draft::Dummy::DummyService::Stub> Stub_;
            Stub_ = Draft::Dummy::DummyService::NewStub(Channel_);
            grpc::ClientContext context;

            Draft::Dummy::InfiniteRequest request;
            Draft::Dummy::InfiniteResponse response;

            request.mutable_operation_params()->mutable_operation_timeout()->set_nanos(100000000); // 100ms

            auto status = Stub_->Infinite(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());  //GRpc layer - OK
            UNIT_ASSERT(deferred.ready() == true);

            NYql::TIssues issues;
            NYql::IssuesFromMessage(deferred.issues(), issues);
            issues.PrintTo(Cerr);
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::TIMEOUT);
        }
    }

    Y_UNIT_TEST(OperationCancelAfter) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> Channel_;
        Channel_ = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        {
            std::unique_ptr<Draft::Dummy::DummyService::Stub> Stub_;
            Stub_ = Draft::Dummy::DummyService::NewStub(Channel_);
            grpc::ClientContext context;

            Draft::Dummy::InfiniteRequest request;
            Draft::Dummy::InfiniteResponse response;

            request.mutable_operation_params()->mutable_cancel_after()->set_nanos(100000000); // 100ms

            auto status = Stub_->Infinite(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());  //GRpc layer - OK
            UNIT_ASSERT(deferred.ready() == true);

            NYql::TIssues issues;
            NYql::IssuesFromMessage(deferred.issues(), issues);
            issues.PrintTo(Cerr);
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::CANCELLED);
        }
    }

    Y_UNIT_TEST(KeepAlive) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> channel;
        channel = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());

        TString sessionId = CreateSession(channel);

        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(channel);
            grpc::ClientContext context;
            Ydb::Table::KeepAliveRequest request;
            request.set_session_id(sessionId);
            Ydb::Table::KeepAliveResponse response;
            auto status = Stub_->KeepAlive(&context, request, &response);
            UNIT_ASSERT(status.ok());
            auto deferred = response.operation();
            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::SUCCESS);

            Ydb::Table::KeepAliveResult result;
            deferred.result().UnpackTo(&result);

            UNIT_ASSERT(result.session_status() == Ydb::Table::KeepAliveResult::SESSION_STATUS_READY);
        }
    }

    Y_UNIT_TEST(BeginTxRequestError) {
        TVector<NKikimrKqp::TKqpSetting> settings;
        auto setting = NKikimrKqp::TKqpSetting();
        setting.SetName("_KqpMaxActiveTxPerSession");
        setting.SetValue("2");
        settings.push_back(setting);

        TKikimrWithGrpcAndRootSchema server(NKikimrConfig::TAppConfig(), settings);
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> channel;
        channel = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());

        TString sessionId = CreateSession(channel);

        for (ui32 i = 0; i < 3; ++i) {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(channel);
            grpc::ClientContext context;
            Ydb::Table::ExecuteDataQueryRequest request;
            request.set_session_id(sessionId);
            request.mutable_query()->set_id("ydb://preparedqueryid/0?id=bad_query");
            request.mutable_tx_control()->mutable_begin_tx()->mutable_serializable_read_write();
            Ydb::Table::ExecuteDataQueryResponse response;
            auto status = Stub_->ExecuteDataQuery(&context, request, &response);
            auto deferred = response.operation();

            UNIT_ASSERT(status.ok());
            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT_VALUES_EQUAL(deferred.status(), Ydb::StatusIds::NOT_FOUND);
        }
    }
}

namespace {

NKikimrSchemeOp::TCompactionPolicy DEFAULT_COMPACTION_POLICY;
NKikimrSchemeOp::TCompactionPolicy COMPACTION_POLICY1;
NKikimrSchemeOp::TCompactionPolicy COMPACTION_POLICY2;
NKikimrSchemeOp::TPipelineConfig PIPELINE_CONFIG1;
NKikimrSchemeOp::TPipelineConfig PIPELINE_CONFIG2;
NKikimrSchemeOp::TStorageConfig STORAGE_CONFIG1;
NKikimrSchemeOp::TStorageConfig STORAGE_CONFIG2;
NKikimrConfig::TExecutionPolicy EXECUTION_POLICY1;
NKikimrConfig::TExecutionPolicy EXECUTION_POLICY2;
NKikimrConfig::TPartitioningPolicy PARTITIONING_POLICY1;
NKikimrConfig::TPartitioningPolicy PARTITIONING_POLICY2;
NKikimrConfig::TStoragePolicy STORAGE_POLICY1;
NKikimrConfig::TStoragePolicy STORAGE_POLICY2;
NKikimrConfig::TReplicationPolicy REPLICATION_POLICY1;
NKikimrConfig::TReplicationPolicy REPLICATION_POLICY2;
NKikimrConfig::TCachingPolicy CACHING_POLICY1;
NKikimrConfig::TCachingPolicy CACHING_POLICY2;

TStoragePools CreatePoolsForTenant(TClient& client, const TDomainsInfo::TDomain::TStoragePoolKinds& pool_types, const TString& tenant)
{
    TStoragePools result;
    for (auto& poolType: pool_types) {
        auto& poolKind = poolType.first;
        result.emplace_back(client.CreateStoragePool(poolKind, tenant), poolKind);
    }
    return result;
}

NKikimrSubDomains::TSubDomainSettings GetSubDomainDeclarationSetting(const TString& name)
{
    NKikimrSubDomains::TSubDomainSettings subdomain;
    subdomain.SetName(name);
    return subdomain;
}

NKikimrSubDomains::TSubDomainSettings GetSubDomainDefaultSetting(const TString& name, const TStoragePools& pools  = {})
{
    NKikimrSubDomains::TSubDomainSettings subdomain;
    subdomain.SetName(name);
    subdomain.SetCoordinators(1);
    subdomain.SetMediators(1);
    subdomain.SetPlanResolution(10);
    subdomain.SetTimeCastBucketsPerMediator(2);
    for (auto& pool: pools) {
        *subdomain.AddStoragePools() = pool;
    }
    return subdomain;
}

void InitConfigs(TKikimrWithGrpcAndRootSchema &server) {
    {
        TString tenant_name = "ydb_ut_tenant";
        TString tenant = Sprintf("/Root/%s", tenant_name.c_str());

        TClient client(*server.ServerSettings);

        TStoragePools tenant_pools = CreatePoolsForTenant(client, server.ServerSettings->StoragePoolTypes, tenant_name);

        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                                 client.CreateSubdomain("/Root", GetSubDomainDeclarationSetting(tenant_name)));
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_INPROGRESS,
                                 client.AlterSubdomain("/Root", GetSubDomainDefaultSetting(tenant_name, tenant_pools), TDuration::MilliSeconds(500)));

        server.Tenants_->Run(tenant);
    }

    {
        NLocalDb::TCompactionPolicyPtr policy = NLocalDb::CreateDefaultUserTablePolicy();
        DEFAULT_COMPACTION_POLICY.Clear();
        policy->Serialize(DEFAULT_COMPACTION_POLICY);
    }

    {
        NLocalDb::TCompactionPolicy policy;
        policy.Generations.push_back({0, 8, 8, 128 * 1024 * 1024, NLocalDb::LegacyQueueIdToTaskName(1), true});
        COMPACTION_POLICY1.Clear();
        policy.Serialize(COMPACTION_POLICY1);
    }

    {
        NLocalDb::TCompactionPolicy policy;
        policy.Generations.push_back({0, 8, 8, 128 * 1024 * 1024, NLocalDb::LegacyQueueIdToTaskName(1), true});
        policy.Generations.push_back({40 * 1024 * 1024, 5, 16, 512 * 1024 * 1024, NLocalDb::LegacyQueueIdToTaskName(2), false});
        COMPACTION_POLICY2.Clear();
        policy.Serialize(COMPACTION_POLICY2);
    }

    {
        PIPELINE_CONFIG1.Clear();
        PIPELINE_CONFIG1.SetNumActiveTx(1);
        PIPELINE_CONFIG1.SetEnableOutOfOrder(false);
        PIPELINE_CONFIG1.SetDisableImmediate(false);
        PIPELINE_CONFIG1.SetEnableSoftUpdates(true);
    }

    {
        PIPELINE_CONFIG2.Clear();
        PIPELINE_CONFIG2.SetNumActiveTx(8);
        PIPELINE_CONFIG2.SetEnableOutOfOrder(true);
        PIPELINE_CONFIG2.SetDisableImmediate(true);
        PIPELINE_CONFIG2.SetEnableSoftUpdates(false);
    }

    {
        EXECUTION_POLICY1.Clear();
        EXECUTION_POLICY1.MutablePipelineConfig()->CopyFrom(PIPELINE_CONFIG1);
        EXECUTION_POLICY1.SetResourceProfile("profile1");
        EXECUTION_POLICY1.SetEnableFilterByKey(true);
        EXECUTION_POLICY1.SetExecutorFastLogPolicy(false);
        EXECUTION_POLICY1.SetTxReadSizeLimit(10000000);
    }
    {
        EXECUTION_POLICY2.Clear();
        EXECUTION_POLICY2.MutablePipelineConfig()->CopyFrom(PIPELINE_CONFIG2);
        EXECUTION_POLICY2.SetResourceProfile("profile2");
        EXECUTION_POLICY2.SetEnableFilterByKey(false);
        EXECUTION_POLICY2.SetExecutorFastLogPolicy(true);
        EXECUTION_POLICY2.SetTxReadSizeLimit(20000000);
    }

    {
        PARTITIONING_POLICY1.Clear();
        PARTITIONING_POLICY1.SetUniformPartitionsCount(10);
        PARTITIONING_POLICY1.SetAutoSplit(true);
        PARTITIONING_POLICY1.SetAutoMerge(false);
        PARTITIONING_POLICY1.SetSizeToSplit(123456);
    }

    {
        PARTITIONING_POLICY2.Clear();
        PARTITIONING_POLICY2.SetUniformPartitionsCount(20);
        PARTITIONING_POLICY2.SetAutoSplit(true);
        PARTITIONING_POLICY2.SetAutoMerge(true);
        PARTITIONING_POLICY2.SetSizeToSplit(1000000000);
    }

    {
        STORAGE_CONFIG1.Clear();
        STORAGE_CONFIG1.MutableSysLog()->SetPreferredPoolKind("hdd");
        STORAGE_CONFIG1.MutableLog()->SetPreferredPoolKind("hdd");
        STORAGE_CONFIG1.MutableData()->SetPreferredPoolKind("hdd");
        STORAGE_CONFIG1.MutableExternal()->SetPreferredPoolKind("hdd");
        STORAGE_CONFIG1.SetExternalThreshold(Max<ui32>());
    }

    {
        STORAGE_CONFIG2.Clear();
        STORAGE_CONFIG2.MutableSysLog()->SetPreferredPoolKind("ssd");
        STORAGE_CONFIG2.MutableLog()->SetPreferredPoolKind("ssd");
        STORAGE_CONFIG2.MutableData()->SetPreferredPoolKind("ssd");
        STORAGE_CONFIG2.MutableExternal()->SetPreferredPoolKind("ssd");
        STORAGE_CONFIG2.SetDataThreshold(30000);
    }

    {
        REPLICATION_POLICY1.Clear();
        REPLICATION_POLICY1.SetFollowerCount(1);
        REPLICATION_POLICY1.SetCrossDataCenter(true);
        REPLICATION_POLICY1.SetAllowFollowerPromotion(false);
    }

    {
        REPLICATION_POLICY2.Clear();
        REPLICATION_POLICY2.SetFollowerCount(2);
        REPLICATION_POLICY2.SetCrossDataCenter(false);
        REPLICATION_POLICY2.SetAllowFollowerPromotion(true);
    }

    {
        CACHING_POLICY1.Clear();
        CACHING_POLICY1.SetExecutorCacheSize(10000000);
    }

    {
        CACHING_POLICY2.Clear();
        CACHING_POLICY2.SetExecutorCacheSize(20000000);
    }

    {
        STORAGE_POLICY1.Clear();
        auto &family = *STORAGE_POLICY1.AddColumnFamilies();
        family.SetId(0);
        family.SetColumnCodec(NKikimrSchemeOp::ColumnCodecLZ4);
        family.MutableStorageConfig()->CopyFrom(STORAGE_CONFIG1);
    }

    {
        STORAGE_POLICY2.Clear();
        auto &family = *STORAGE_POLICY2.AddColumnFamilies();
        family.SetId(0);
        family.SetColumnCache(NKikimrSchemeOp::ColumnCacheEver);
        family.MutableStorageConfig()->CopyFrom(STORAGE_CONFIG2);
    }

    TClient client(*server.ServerSettings);
    TAutoPtr<NMsgBusProxy::TBusConsoleRequest> request(new NMsgBusProxy::TBusConsoleRequest());
    auto &item = *request->Record.MutableConfigureRequest()->AddActions()
        ->MutableAddConfigItem()->MutableConfigItem();
    item.SetKind((ui32)NKikimrConsole::TConfigItem::TableProfilesConfigItem);
    auto &profiles = *item.MutableConfig()->MutableTableProfilesConfig();
    {
        auto &policy = *profiles.AddCompactionPolicies();
        policy.SetName("default");
    }
    {
        auto &policy = *profiles.AddCompactionPolicies();
        policy.SetName("compaction1");
        policy.MutableCompactionPolicy()->CopyFrom(COMPACTION_POLICY1);
    }
    {
        auto &policy = *profiles.AddCompactionPolicies();
        policy.SetName("compaction2");
        policy.MutableCompactionPolicy()->CopyFrom(COMPACTION_POLICY2);
    }
    {
        auto &policy = *profiles.AddExecutionPolicies();
        policy.SetName("default");
    }
    {
        auto &policy = *profiles.AddExecutionPolicies();
        policy.CopyFrom(EXECUTION_POLICY1);
        policy.SetName("execution1");
    }
    {
        auto &policy = *profiles.AddExecutionPolicies();
        policy.CopyFrom(EXECUTION_POLICY2);
        policy.SetName("execution2");
    }
    {
        auto &policy = *profiles.AddPartitioningPolicies();
        policy.SetName("default");
    }
    {
        auto &policy = *profiles.AddPartitioningPolicies();
        policy.CopyFrom(PARTITIONING_POLICY1);
        policy.SetName("partitioning1");
    }
    {
        auto &policy = *profiles.AddPartitioningPolicies();
        policy.CopyFrom(PARTITIONING_POLICY2);
        policy.SetName("partitioning2");
    }
    {
        auto &policy = *profiles.AddStoragePolicies();
        policy.SetName("default");
    }
    {
        auto &policy = *profiles.AddStoragePolicies();
        policy.CopyFrom(STORAGE_POLICY1);
        policy.SetName("storage1");
    }
    {
        auto &policy = *profiles.AddStoragePolicies();
        policy.CopyFrom(STORAGE_POLICY2);
        policy.SetName("storage2");
    }
    {
        auto &policy = *profiles.AddReplicationPolicies();
        policy.SetName("default");
    }
    {
        auto &policy = *profiles.AddReplicationPolicies();
        policy.CopyFrom(REPLICATION_POLICY1);
        policy.SetName("replication1");
    }
    {
        auto &policy = *profiles.AddReplicationPolicies();
        policy.CopyFrom(REPLICATION_POLICY2);
        policy.SetName("replication2");
    }
    {
        auto &policy = *profiles.AddCachingPolicies();
        policy.SetName("default");
    }
    {
        auto &policy = *profiles.AddCachingPolicies();
        policy.CopyFrom(CACHING_POLICY1);
        policy.SetName("caching1");
    }
    {
        auto &policy = *profiles.AddCachingPolicies();
        policy.CopyFrom(CACHING_POLICY2);
        policy.SetName("caching2");
    }
    {
        auto &profile = *profiles.AddTableProfiles();
        profile.SetName("default");
        profile.SetCompactionPolicy("default");
        profile.SetExecutionPolicy("default");
        profile.SetPartitioningPolicy("default");
        profile.SetStoragePolicy("default");
        profile.SetReplicationPolicy("default");
        profile.SetCachingPolicy("default");
    }
    {
        auto &profile = *profiles.AddTableProfiles();
        profile.SetName("profile1");
        profile.SetCompactionPolicy("compaction1");
        profile.SetExecutionPolicy("execution1");
        profile.SetPartitioningPolicy("partitioning1");
        profile.SetStoragePolicy("storage1");
        profile.SetReplicationPolicy("replication1");
        profile.SetCachingPolicy("caching1");
    }
    {
        auto &profile = *profiles.AddTableProfiles();
        profile.SetName("profile2");
        profile.SetCompactionPolicy("compaction2");
        profile.SetExecutionPolicy("execution2");
        profile.SetPartitioningPolicy("partitioning2");
        profile.SetStoragePolicy("storage2");
        profile.SetReplicationPolicy("replication2");
        profile.SetCachingPolicy("caching2");
    }
    TAutoPtr<NBus::TBusMessage> reply;
    NBus::EMessageStatus msgStatus = client.SyncCall(request, reply);
    UNIT_ASSERT_VALUES_EQUAL(msgStatus, NBus::MESSAGE_OK);
    auto resp = dynamic_cast<NMsgBusProxy::TBusConsoleResponse*>(reply.Get())->Record;
    UNIT_ASSERT_VALUES_EQUAL(resp.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
}

void CheckTableSettings(const TKikimrWithGrpcAndRootSchema &server,
                        const TString &path,
                        NKikimrSchemeOp::TTableDescription expected)
{
    TClient client(*server.ServerSettings);
    auto desc = client.Ls(path)->Record.GetPathDescription();
    NKikimrSchemeOp::TTableDescription resp = desc.GetTable();
    // Table profiles affect only few fields. Clear other fields to simplify comparison.
    THashSet<ui32> affectedFields = {
        7  // PartitionConfig
    };

    if (expected.HasUniformPartitionsCount()) {
        UNIT_ASSERT_VALUES_EQUAL(desc.TablePartitionsSize(), expected.GetUniformPartitionsCount());
        expected.ClearUniformPartitionsCount();
    } else {
        UNIT_ASSERT_VALUES_EQUAL(desc.TablePartitionsSize(), 1);
    }

    if (resp.GetPartitionConfig().GetPartitioningPolicy().HasMinPartitionsCount() &&
        !expected.GetPartitionConfig().GetPartitioningPolicy().HasMinPartitionsCount())
    {
        // SchemeShard will set some min partitions count when unspecified by the user
        expected.MutablePartitionConfig()->MutablePartitioningPolicy()->SetMinPartitionsCount(
            resp.GetPartitionConfig().GetPartitioningPolicy().GetMinPartitionsCount());
    }

    std::vector<const ::google::protobuf::FieldDescriptor*> fields;
    auto *reflection = resp.GetReflection();
    reflection->ListFields(resp, &fields);
    for (auto field : fields)
        if (!affectedFields.contains(field->number()))
            reflection->ClearField(&resp, field);

    UNIT_ASSERT_VALUES_EQUAL(resp.DebugString(), expected.DebugString());
}

void CheckTablePartitions(const TKikimrWithGrpcAndRootSchema &server,
                          const TString &path,
                          const ::google::protobuf::RepeatedPtrField<Ydb::TypedValue> &points)
{
    TClient client(*server.ServerSettings);
    auto desc = client.Ls(path)->Record.GetPathDescription();

    UNIT_ASSERT_VALUES_EQUAL(points.size(), desc.TablePartitionsSize() - 1);
    for (int i = 0; i < points.size(); ++i) {
        auto &bnd = desc.GetTablePartitions(i);
        auto &vals = points[i].value();
        auto &types = points[i].type();

        TSerializedCellVec vec;
        vec.Parse(bnd.GetEndOfRangeKeyPrefix());
        auto cells = vec.GetCells();

        UNIT_ASSERT_VALUES_EQUAL(cells.size(), vals.items_size());
        UNIT_ASSERT_VALUES_EQUAL(vals.items_size(), types.tuple_type().elements_size());

        for (size_t j = 0; j < cells.size(); ++j) {
            auto cell = cells[j];
            auto &val = vals.items(j);
            auto &type = types.tuple_type().elements(j);
            ui32 typeId = type.optional_type().item().type_id();

            TString cellStr;
            DbgPrintValue(cellStr, cell, NScheme::TTypeInfo(typeId));

            TString valStr;
            switch (typeId) {
            case Ydb::Type::UINT64:
                valStr = ToString(val.uint64_value());
                break;
            case Ydb::Type::UTF8:
                valStr = ToString(val.text_value());
                break;
            case Ydb::Type::STRING:
                valStr = ToString(val.bytes_value());
                break;
            default:
                valStr = "UNKNOWN";
            }

            UNIT_ASSERT_VALUES_EQUAL(cellStr, valStr);
            Cout << cellStr << " " << valStr << Endl;
        }
    }
}

void Apply(const NKikimrSchemeOp::TCompactionPolicy &policy,
           NKikimrSchemeOp::TTableDescription &description)
{
    description.MutablePartitionConfig()->MutableCompactionPolicy()->CopyFrom(policy);
}

void Apply(const NKikimrConfig::TExecutionPolicy &policy,
           NKikimrSchemeOp::TTableDescription &description)
{
    auto &partition = *description.MutablePartitionConfig();

    if (policy.HasPipelineConfig())
        partition.MutablePipelineConfig()->CopyFrom(policy.GetPipelineConfig());
    else
        partition.ClearPipelineConfig();
    if (policy.HasResourceProfile())
        partition.SetResourceProfile(policy.GetResourceProfile());
    else
        partition.ClearResourceProfile();
    if (policy.HasEnableFilterByKey())
        partition.SetEnableFilterByKey(policy.GetEnableFilterByKey());
    else
        partition.ClearEnableFilterByKey();
    if (policy.HasExecutorFastLogPolicy())
        partition.SetExecutorFastLogPolicy(policy.GetExecutorFastLogPolicy());
    else
        partition.ClearExecutorFastLogPolicy();
    if (policy.HasTxReadSizeLimit())
        partition.SetTxReadSizeLimit(policy.GetTxReadSizeLimit());
    else
        partition.ClearTxReadSizeLimit();
}

void Apply(const NKikimrConfig::TPartitioningPolicy &policy,
           NKikimrSchemeOp::TTableDescription &description)
{
    auto &partition = *description.MutablePartitionConfig();

    if (policy.HasUniformPartitionsCount())
        description.SetUniformPartitionsCount(policy.GetUniformPartitionsCount());
    else
        description.ClearUniformPartitionsCount();
    if (policy.GetAutoSplit()) {
        partition.MutablePartitioningPolicy()->SetSizeToSplit(policy.GetSizeToSplit());
        if (policy.HasMaxPartitionsCount())
            partition.MutablePartitioningPolicy()->SetMaxPartitionsCount(policy.GetMaxPartitionsCount());
        else
            partition.MutablePartitioningPolicy()->ClearMaxPartitionsCount();
        if (policy.GetAutoMerge()) {
            partition.MutablePartitioningPolicy()->SetMinPartitionsCount(Max((ui32)1, policy.GetUniformPartitionsCount()));
        } else {
            partition.MutablePartitioningPolicy()->ClearMinPartitionsCount();
        }
    } else {
        partition.ClearPartitioningPolicy();
    }
}

void Apply(const NKikimrConfig::TStoragePolicy &policy,
           NKikimrSchemeOp::TTableDescription &description)
{
    auto &partition = *description.MutablePartitionConfig();

    partition.ClearColumnFamilies();
    for (auto &family : policy.GetColumnFamilies())
        partition.AddColumnFamilies()->CopyFrom(family);
}

void Apply(const NKikimrConfig::TReplicationPolicy &policy,
           NKikimrSchemeOp::TTableDescription &description)
{
    auto &partition = *description.MutablePartitionConfig();

    partition.ClearFollowerCount();
    partition.ClearCrossDataCenterFollowerCount();
    partition.ClearAllowFollowerPromotion();
    partition.ClearFollowerGroups();
    if (policy.HasFollowerCount()) {
        auto& followerGroup = *partition.AddFollowerGroups();
        followerGroup.SetFollowerCount(policy.GetFollowerCount());
        if (policy.GetCrossDataCenter()) {
            followerGroup.SetRequireAllDataCenters(true);
        } else {
            followerGroup.SetRequireAllDataCenters(false);
        }
        if (policy.HasAllowFollowerPromotion()) {
            followerGroup.SetAllowLeaderPromotion(policy.GetAllowFollowerPromotion());
        }
    }
}

void Apply(const NKikimrConfig::TCachingPolicy &policy,
           NKikimrSchemeOp::TTableDescription &description)
{
    auto &partition = *description.MutablePartitionConfig();

    if (policy.HasExecutorCacheSize())
        partition.SetExecutorCacheSize(policy.GetExecutorCacheSize());
    else
        partition.ClearExecutorCacheSize();
}

void CreateTable(TKikimrWithGrpcAndRootSchema &server,
                 const Ydb::Table::CreateTableRequest &request,
                 Ydb::StatusIds::StatusCode code = Ydb::StatusIds::SUCCESS)
{
    std::shared_ptr<grpc::Channel> Channel_;
    Channel_ = grpc::CreateChannel("localhost:" + ToString(server.GetPort()), grpc::InsecureChannelCredentials());
    std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
    Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
    grpc::ClientContext context;
    Ydb::Table::CreateTableResponse response;
    auto status = Stub_->CreateTable(&context, request, &response);
    auto deferred = response.operation();
    UNIT_ASSERT(status.ok());
    UNIT_ASSERT(deferred.ready());
    if (deferred.status() != code)
        Cerr << deferred.DebugString();
    UNIT_ASSERT_VALUES_EQUAL(deferred.status(), code);
}

void CreateTable(TKikimrWithGrpcAndRootSchema &server,
                 const TString &path,
                 const Ydb::Table::CreateTableRequest &extension = Ydb::Table::CreateTableRequest(),
                 Ydb::StatusIds::StatusCode code = Ydb::StatusIds::SUCCESS)
{
    Ydb::Table::CreateTableRequest request = extension;
    request.set_path(path);
    auto &col = *request.add_columns();
    col.set_name("key");
    col.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UINT64);
    request.add_primary_key("key");

    CreateTable(server, request, code);
}

void CreateTable(TKikimrWithGrpcAndRootSchema &server,
                 const TString &path,
                 const Ydb::Table::TableProfile &profile,
                 Ydb::StatusIds::StatusCode code = Ydb::StatusIds::SUCCESS)
{
    Ydb::Table::CreateTableRequest request;
    request.mutable_profile()->CopyFrom(profile);
    CreateTable(server, path, request, code);
}

void CreateTableComplexKey(TKikimrWithGrpcAndRootSchema &server,
                           const TString &path,
                           const Ydb::Table::CreateTableRequest &extension = Ydb::Table::CreateTableRequest(),
                           Ydb::StatusIds::StatusCode code = Ydb::StatusIds::SUCCESS)
{
    Ydb::Table::CreateTableRequest request = extension;
    request.set_path(path);
    auto &col1 = *request.add_columns();
    col1.set_name("key1");
    col1.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UTF8);
    request.add_primary_key("key1");
    auto &col2 = *request.add_columns();
    col2.set_name("key2");
    col2.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UINT64);
    request.add_primary_key("key2");
    auto &col3 = *request.add_columns();
    col3.set_name("key3");
    col3.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::STRING);
    request.add_primary_key("key3");

    CreateTable(server, request, code);
}

void CreateTableComplexKey(TKikimrWithGrpcAndRootSchema &server,
                           const TString &path,
                           const Ydb::Table::TableProfile &profile,
                           Ydb::StatusIds::StatusCode code = Ydb::StatusIds::SUCCESS)
{
    Ydb::Table::CreateTableRequest request;
    request.mutable_profile()->CopyFrom(profile);
    CreateTableComplexKey(server, path, request, code);
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TTableProfileTests) {
    Y_UNIT_TEST(UseDefaultProfile) {
        TKikimrWithGrpcAndRootSchema server;
        //server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::CMS_CONFIGS, NLog::PRI_TRACE);
        //server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::CONFIGS_DISPATCHER, NLog::PRI_TRACE);
        InitConfigs(server);

        NKikimrSchemeOp::TTableDescription defaultDescription;
        defaultDescription.MutablePartitionConfig()->MutableCompactionPolicy()->CopyFrom(DEFAULT_COMPACTION_POLICY);
        //defaultDescription.MutablePartitionConfig()->SetChannelProfileId(0);

        {
            CreateTable(server, "/Root/table-1");
            CheckTableSettings(server, "/Root/table-1", defaultDescription);
        }

        {
            Ydb::Table::TableProfile profile;
            profile.set_preset_name("default");
            CreateTable(server, "/Root/table-2", profile);

            CheckTableSettings(server, "/Root/table-2", defaultDescription);
        }

        {
            Ydb::Table::TableProfile profile;
            profile.set_preset_name("default");
            profile.mutable_compaction_policy()->set_preset_name("default");
            profile.mutable_execution_policy()->set_preset_name("default");
            profile.mutable_partitioning_policy()->set_preset_name("default");
            profile.mutable_storage_policy()->set_preset_name("default");
            CreateTable(server, "/Root/table-3", profile);

            CheckTableSettings(server, "/Root/table-3", defaultDescription);
        }
    }

    Y_UNIT_TEST(UseTableProfilePreset) {
        TKikimrWithGrpcAndRootSchema server;
        InitConfigs(server);

        {
            Ydb::Table::TableProfile profile;
            profile.set_preset_name("profile1");
            CreateTable(server, "/Root/ydb_ut_tenant/table-1", profile);

            NKikimrSchemeOp::TTableDescription description;
            Apply(COMPACTION_POLICY1, description);
            Apply(EXECUTION_POLICY1, description);
            Apply(PARTITIONING_POLICY1, description);
            Apply(STORAGE_POLICY1, description);
            Apply(REPLICATION_POLICY1, description);
            Apply(CACHING_POLICY1, description);

            CheckTableSettings(server, "/Root/ydb_ut_tenant/table-1", description);
        }

        {
            Ydb::Table::TableProfile profile;
            profile.set_preset_name("profile2");
            CreateTable(server, "/Root/ydb_ut_tenant/table-2", profile);

            NKikimrSchemeOp::TTableDescription description;
            Apply(COMPACTION_POLICY2, description);
            Apply(EXECUTION_POLICY2, description);
            Apply(PARTITIONING_POLICY2, description);
            Apply(STORAGE_POLICY2, description);
            Apply(REPLICATION_POLICY2, description);
            Apply(CACHING_POLICY2, description);

            CheckTableSettings(server, "/Root/ydb_ut_tenant/table-2", description);
        }
    }

    Y_UNIT_TEST(UseTableProfilePresetViaSdk) {
        TKikimrWithGrpcAndRootSchema server;
        InitConfigs(server);
        ui16 grpc = server.GetPort();

        TString location = TStringBuilder() << "localhost:" << grpc;

        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(location));
        auto client = NYdb::NTable::TTableClient(connection);
        auto session = client.CreateSession().ExtractValueSync().GetSession();

        {
            auto tableBuilder = client.GetTableBuilder();
            tableBuilder
                .AddNullableColumn("Key", EPrimitiveType::Uint64);
            tableBuilder.SetPrimaryKeyColumn("Key");
            auto settings = TCreateTableSettings().PresetName("profile1");
            auto res = session.CreateTable("/Root/ydb_ut_tenant/table-1", tableBuilder.Build(), settings).ExtractValueSync();

            NKikimrSchemeOp::TTableDescription description;
            Apply(COMPACTION_POLICY1, description);
            Apply(EXECUTION_POLICY1, description);
            Apply(PARTITIONING_POLICY1, description);
            Apply(STORAGE_POLICY1, description);
            Apply(REPLICATION_POLICY1, description);
            Apply(CACHING_POLICY1, description);

            CheckTableSettings(server, "/Root/ydb_ut_tenant/table-1", description);
        }

        {
            auto tableBuilder = client.GetTableBuilder();
            tableBuilder
                .AddNullableColumn("Key", EPrimitiveType::Uint64);
            tableBuilder.SetPrimaryKeyColumn("Key");
            auto settings = TCreateTableSettings().PresetName("profile2");
            auto res = session.CreateTable("/Root/ydb_ut_tenant/table-2", tableBuilder.Build(), settings).ExtractValueSync();

            NKikimrSchemeOp::TTableDescription description;
            Apply(COMPACTION_POLICY2, description);
            Apply(EXECUTION_POLICY2, description);
            Apply(PARTITIONING_POLICY2, description);
            Apply(STORAGE_POLICY2, description);
            Apply(REPLICATION_POLICY2, description);
            Apply(CACHING_POLICY2, description);

            CheckTableSettings(server, "/Root/ydb_ut_tenant/table-2", description);
        }

        {
            auto res = session.ExecuteSchemeQuery("CREATE TABLE `/Root/ydb_ut_tenant/table-3` (Key Uint64, Value Utf8, PRIMARY KEY (Key))").ExtractValueSync();
            NKikimrSchemeOp::TTableDescription defaultDescription;
            defaultDescription.MutablePartitionConfig()->MutableCompactionPolicy()->CopyFrom(DEFAULT_COMPACTION_POLICY);
            //defaultDescription.MutablePartitionConfig()->SetChannelProfileId(0);

            CheckTableSettings(server, "/Root/ydb_ut_tenant/table-3", defaultDescription);
        }

    }


    Y_UNIT_TEST(OverwriteCompactionPolicy) {
        TKikimrWithGrpcAndRootSchema server;
        InitConfigs(server);

        {
            Ydb::Table::TableProfile profile;
            profile.set_preset_name("profile1");
            profile.mutable_compaction_policy()->set_preset_name("compaction2");
            CreateTable(server, "/Root/ydb_ut_tenant/table-1", profile);

            NKikimrSchemeOp::TTableDescription description;
            Apply(COMPACTION_POLICY2, description);
            Apply(EXECUTION_POLICY1, description);
            Apply(PARTITIONING_POLICY1, description);
            Apply(STORAGE_POLICY1, description);
            Apply(REPLICATION_POLICY1, description);
            Apply(CACHING_POLICY1, description);

            CheckTableSettings(server, "/Root/ydb_ut_tenant/table-1", description);
        }

        {
            Ydb::Table::TableProfile profile;
            profile.set_preset_name("profile1");
            profile.mutable_compaction_policy()->set_preset_name("default");
            CreateTable(server, "/Root/ydb_ut_tenant/table-2", profile);

            NKikimrSchemeOp::TTableDescription description;
            Apply(DEFAULT_COMPACTION_POLICY, description);
            Apply(EXECUTION_POLICY1, description);
            Apply(PARTITIONING_POLICY1, description);
            Apply(STORAGE_POLICY1, description);
            Apply(REPLICATION_POLICY1, description);
            Apply(CACHING_POLICY1, description);

            CheckTableSettings(server, "/Root/ydb_ut_tenant/table-2", description);
        }
    }


    Y_UNIT_TEST(OverwriteExecutionPolicy) {
        TKikimrWithGrpcAndRootSchema server;
        InitConfigs(server);

        {
            Ydb::Table::TableProfile profile;
            profile.set_preset_name("profile1");
            profile.mutable_execution_policy()->set_preset_name("execution2");
            CreateTable(server, "/Root/ydb_ut_tenant/table-1", profile);

            NKikimrSchemeOp::TTableDescription description;
            Apply(COMPACTION_POLICY1, description);
            Apply(EXECUTION_POLICY2, description);
            Apply(PARTITIONING_POLICY1, description);
            Apply(STORAGE_POLICY1, description);
            Apply(REPLICATION_POLICY1, description);
            Apply(CACHING_POLICY1, description);

            CheckTableSettings(server, "/Root/ydb_ut_tenant/table-1", description);
        }

        {
            Ydb::Table::TableProfile profile;
            profile.set_preset_name("profile1");
            profile.mutable_execution_policy()->set_preset_name("default");
            CreateTable(server, "/Root/ydb_ut_tenant/table-2", profile);

            NKikimrSchemeOp::TTableDescription description;
            Apply(COMPACTION_POLICY1, description);
            Apply(PARTITIONING_POLICY1, description);
            Apply(STORAGE_POLICY1, description);
            Apply(REPLICATION_POLICY1, description);
            Apply(CACHING_POLICY1, description);

            CheckTableSettings(server, "/Root/ydb_ut_tenant/table-2", description);
        }
    }

    Y_UNIT_TEST(OverwritePartitioningPolicy) {
        TKikimrWithGrpcAndRootSchema server;
        InitConfigs(server);

        {
            Ydb::Table::TableProfile profile;
            profile.set_preset_name("profile1");
            profile.mutable_partitioning_policy()->set_preset_name("partitioning2");
            CreateTable(server, "/Root/ydb_ut_tenant/table-1", profile);

            NKikimrSchemeOp::TTableDescription description;
            Apply(COMPACTION_POLICY1, description);
            Apply(EXECUTION_POLICY1, description);
            Apply(PARTITIONING_POLICY2, description);
            Apply(STORAGE_POLICY1, description);
            Apply(REPLICATION_POLICY1, description);
            Apply(CACHING_POLICY1, description);

            CheckTableSettings(server, "/Root/ydb_ut_tenant/table-1", description);
        }

        {
            Ydb::Table::TableProfile profile;
            profile.set_preset_name("profile1");
            profile.mutable_partitioning_policy()->set_preset_name("default");
            CreateTable(server, "/Root/ydb_ut_tenant/table-2", profile);

            NKikimrSchemeOp::TTableDescription description;
            Apply(COMPACTION_POLICY1, description);
            Apply(EXECUTION_POLICY1, description);
            Apply(STORAGE_POLICY1, description);
            Apply(REPLICATION_POLICY1, description);
            Apply(CACHING_POLICY1, description);

            CheckTableSettings(server, "/Root/ydb_ut_tenant/table-2", description);
        }

        {
            Ydb::Table::TableProfile profile;
            profile.set_preset_name("profile1");
            profile.mutable_partitioning_policy()->set_auto_partitioning(Ydb::Table::PartitioningPolicy::DISABLED);
            profile.mutable_partitioning_policy()->set_uniform_partitions(5);
            CreateTable(server, "/Root/ydb_ut_tenant/table-3", profile);

            NKikimrSchemeOp::TTableDescription description;
            Apply(COMPACTION_POLICY1, description);
            Apply(EXECUTION_POLICY1, description);
            Apply(REPLICATION_POLICY1, description);
            Apply(CACHING_POLICY1, description);
            auto policy = PARTITIONING_POLICY1;
            policy.SetUniformPartitionsCount(5);
            policy.SetAutoSplit(false);
            policy.SetAutoMerge(false);
            Apply(policy, description);
            Apply(STORAGE_POLICY1, description);

            CheckTableSettings(server, "/Root/ydb_ut_tenant/table-3", description);
        }

        {
            Ydb::Table::TableProfile profile;
            profile.set_preset_name("profile1");
            profile.mutable_partitioning_policy()->set_preset_name("partitioning2");
            profile.mutable_partitioning_policy()->set_auto_partitioning(Ydb::Table::PartitioningPolicy::DISABLED);
            profile.mutable_partitioning_policy()->set_uniform_partitions(5);
            CreateTable(server, "/Root/ydb_ut_tenant/table-4", profile);

            NKikimrSchemeOp::TTableDescription description;
            Apply(COMPACTION_POLICY1, description);
            Apply(EXECUTION_POLICY1, description);
            Apply(REPLICATION_POLICY1, description);
            Apply(CACHING_POLICY1, description);
            auto policy = PARTITIONING_POLICY2;
            policy.SetUniformPartitionsCount(5);
            policy.SetAutoSplit(false);
            policy.SetAutoMerge(false);
            Apply(policy, description);
            Apply(STORAGE_POLICY1, description);

            CheckTableSettings(server, "/Root/ydb_ut_tenant/table-4", description);
        }

        {
            Ydb::Table::TableProfile profile;
            profile.set_preset_name("profile1");
            profile.mutable_partitioning_policy()->set_preset_name("default");
            profile.mutable_partitioning_policy()->set_auto_partitioning(Ydb::Table::PartitioningPolicy::AUTO_SPLIT);
            CreateTable(server, "/Root/ydb_ut_tenant/table-5", profile);

            NKikimrSchemeOp::TTableDescription description;
            Apply(COMPACTION_POLICY1, description);
            Apply(EXECUTION_POLICY1, description);
            Apply(REPLICATION_POLICY1, description);
            Apply(CACHING_POLICY1, description);
            NKikimrConfig::TPartitioningPolicy policy;
            policy.SetAutoSplit(true);
            policy.SetSizeToSplit(1 << 30);
            Apply(policy, description);
            Apply(STORAGE_POLICY1, description);

            CheckTableSettings(server, "/Root/ydb_ut_tenant/table-5", description);
        }

    }

    Y_UNIT_TEST(DescribeTableWithPartitioningPolicy) {
        TKikimrWithGrpcAndRootSchema server;
        InitConfigs(server);
        ui16 grpc = server.GetPort();

        TString location = TStringBuilder() << "localhost:" << grpc;

        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(location));
        auto client = NYdb::NTable::TTableClient(connection);
        auto session = client.CreateSession().ExtractValueSync().GetSession();

        {
            auto tableBuilder = client.GetTableBuilder();
            tableBuilder
                .AddNullableColumn("Data", EPrimitiveType::String)
                .AddNullableColumn("KeyHash", EPrimitiveType::Uint64)
                .AddNullableColumn("Version", EPrimitiveType::Uint32)
                .AddNullableColumn("Ratio", EPrimitiveType::Float)
                .AddNullableColumn("SubKey", EPrimitiveType::Int32)
                .AddNullableColumn("Key", EPrimitiveType::Utf8);
            tableBuilder.SetPrimaryKeyColumns({"KeyHash", "Key", "SubKey"});
            auto settings = TCreateTableSettings().PresetName("profile1");
            auto res = session.CreateTable("/Root/ydb_ut_tenant/table-1", tableBuilder.Build(), settings).ExtractValueSync();

            NKikimrSchemeOp::TTableDescription description;
            Apply(COMPACTION_POLICY1, description);
            Apply(EXECUTION_POLICY1, description);
            Apply(PARTITIONING_POLICY1, description);
            Apply(STORAGE_POLICY1, description);
            Apply(REPLICATION_POLICY1, description);
            Apply(CACHING_POLICY1, description);

            CheckTableSettings(server, "/Root/ydb_ut_tenant/table-1", description);

            auto descResult = session.DescribeTable(
                    "/Root/ydb_ut_tenant/table-1",
                    TDescribeTableSettings().WithKeyShardBoundary(true)
                ).GetValueSync();

            UNIT_ASSERT(descResult.IsSuccess());
            auto ranges = descResult.GetTableDescription().GetKeyRanges();
            UNIT_ASSERT_VALUES_EQUAL(ranges.size(), 10);

            auto extractValue = [](const TValue& val) {
                auto parser = TValueParser(val);
                parser.OpenTuple();
                UNIT_ASSERT(parser.TryNextElement());
                return parser.GetOptionalUint64().GetRef();
            };

            int n = 0;
            const ui64 expectedRanges[10] = {
                1844674407370955264ul,
                3689348814741910528ul,
                5534023222112865280ul,
                7378697629483821056ul,
                9223372036854775808ul,
                11068046444225730560ul,
                12912720851596685312ul,
                14757395258967642112ul,
                16602069666338596864ul
            };

            for (const auto& range : ranges) {
                if (n == 0) {
                    UNIT_ASSERT(!range.From());
                } else {
                    UNIT_ASSERT(range.From()->IsInclusive());

                    auto left = extractValue(range.From()->GetValue());
                    UNIT_ASSERT_VALUES_EQUAL(left, expectedRanges[n - 1]);
                }

                if (n == 9) {
                    UNIT_ASSERT(!range.To());
                } else {
                    UNIT_ASSERT(!range.To()->IsInclusive());
                    auto right = extractValue(range.To()->GetValue());
                    UNIT_ASSERT_VALUES_EQUAL(right, expectedRanges[n]);
                }

                ++n;
            }

        }
    }

    Y_UNIT_TEST(OverwriteStoragePolicy) {
        TKikimrWithGrpcAndRootSchema server;
        server.Server_->GetRuntime()->GetAppData().FeatureFlags.SetEnablePublicApiKeepInMemory(true);
        InitConfigs(server);

        {
            Ydb::Table::TableProfile profile;
            profile.set_preset_name("profile1");
            profile.mutable_storage_policy()->set_preset_name("storage2");
            CreateTable(server, "/Root/ydb_ut_tenant/table-1", profile);

            NKikimrSchemeOp::TTableDescription description;
            Apply(COMPACTION_POLICY1, description);
            Apply(EXECUTION_POLICY1, description);
            Apply(PARTITIONING_POLICY1, description);
            Apply(STORAGE_POLICY2, description);
            Apply(REPLICATION_POLICY1, description);
            Apply(CACHING_POLICY1, description);

            CheckTableSettings(server, "/Root/ydb_ut_tenant/table-1", description);
        }

        {
            Ydb::Table::TableProfile profile;
            profile.set_preset_name("profile1");
            profile.mutable_storage_policy()->set_preset_name("default");
            CreateTable(server, "/Root/ydb_ut_tenant/table-2", profile);

            NKikimrSchemeOp::TTableDescription description;
            Apply(COMPACTION_POLICY1, description);
            Apply(EXECUTION_POLICY1, description);
            Apply(PARTITIONING_POLICY1, description);
            Apply(REPLICATION_POLICY1, description);
            Apply(CACHING_POLICY1, description);
            // TODO: remove this line when storage config is supported in SchemeShard.
            //description.MutablePartitionConfig()->SetChannelProfileId(0);

            CheckTableSettings(server, "/Root/ydb_ut_tenant/table-2", description);
        }

        {
            Ydb::Table::TableProfile profile;
            profile.set_preset_name("profile1");
            profile.mutable_storage_policy()->mutable_syslog()->set_media("ssd");
            profile.mutable_storage_policy()->mutable_data()->set_media("ssd");
            profile.mutable_storage_policy()->set_keep_in_memory(Ydb::FeatureFlag::ENABLED);
            CreateTable(server, "/Root/ydb_ut_tenant/table-3", profile);

            NKikimrSchemeOp::TTableDescription description;
            Apply(COMPACTION_POLICY1, description);
            Apply(EXECUTION_POLICY1, description);
            Apply(PARTITIONING_POLICY1, description);
            Apply(REPLICATION_POLICY1, description);
            Apply(CACHING_POLICY1, description);
            auto policy = STORAGE_POLICY1;
            policy.MutableColumnFamilies(0)->MutableStorageConfig()->MutableSysLog()->SetPreferredPoolKind("ssd");
            policy.MutableColumnFamilies(0)->MutableStorageConfig()->MutableSysLog()->SetAllowOtherKinds(false);
            policy.MutableColumnFamilies(0)->MutableStorageConfig()->MutableData()->SetPreferredPoolKind("ssd");
            policy.MutableColumnFamilies(0)->MutableStorageConfig()->MutableData()->SetAllowOtherKinds(false);
            policy.MutableColumnFamilies(0)->SetColumnCache(NKikimrSchemeOp::ColumnCacheEver);
            Apply(policy, description);

            CheckTableSettings(server, "/Root/ydb_ut_tenant/table-3", description);
        }

        {
            Ydb::Table::TableProfile profile;
            profile.set_preset_name("profile1");
            profile.mutable_storage_policy()->set_preset_name("storage2");
            profile.mutable_storage_policy()->mutable_log()->set_media("hdd");
            profile.mutable_storage_policy()->mutable_external()->set_media("hdd");
            profile.mutable_storage_policy()->set_keep_in_memory(Ydb::FeatureFlag::DISABLED);
            CreateTable(server, "/Root/ydb_ut_tenant/table-4", profile);

            NKikimrSchemeOp::TTableDescription description;
            Apply(COMPACTION_POLICY1, description);
            Apply(EXECUTION_POLICY1, description);
            Apply(PARTITIONING_POLICY1, description);
            Apply(REPLICATION_POLICY1, description);
            Apply(CACHING_POLICY1, description);
            auto policy = STORAGE_POLICY2;
            policy.MutableColumnFamilies(0)->MutableStorageConfig()->MutableLog()->SetPreferredPoolKind("hdd");
            policy.MutableColumnFamilies(0)->MutableStorageConfig()->MutableLog()->SetAllowOtherKinds(false);
            policy.MutableColumnFamilies(0)->MutableStorageConfig()->MutableExternal()->SetPreferredPoolKind("hdd");
            policy.MutableColumnFamilies(0)->MutableStorageConfig()->MutableExternal()->SetAllowOtherKinds(false);
            policy.MutableColumnFamilies(0)->ClearColumnCache();
            Apply(policy, description);

            CheckTableSettings(server, "/Root/ydb_ut_tenant/table-4", description);
        }

        {
            Ydb::Table::TableProfile profile;
            profile.set_preset_name("profile1");
            profile.mutable_storage_policy()->set_preset_name("default");
            profile.mutable_storage_policy()->mutable_syslog()->set_media("ssd");
            profile.mutable_storage_policy()->mutable_log()->set_media("ssd");
            profile.mutable_storage_policy()->set_keep_in_memory(Ydb::FeatureFlag::ENABLED);
            CreateTable(server, "/Root/ydb_ut_tenant/table-5", profile);

            NKikimrSchemeOp::TTableDescription description;
            Apply(COMPACTION_POLICY1, description);
            Apply(EXECUTION_POLICY1, description);
            Apply(PARTITIONING_POLICY1, description);
            Apply(REPLICATION_POLICY1, description);
            Apply(CACHING_POLICY1, description);
            NKikimrConfig::TStoragePolicy policy;
            policy.AddColumnFamilies()->SetId(0);
            policy.MutableColumnFamilies(0)->MutableStorageConfig()->MutableSysLog()->SetPreferredPoolKind("ssd");
            policy.MutableColumnFamilies(0)->MutableStorageConfig()->MutableSysLog()->SetAllowOtherKinds(false);
            policy.MutableColumnFamilies(0)->MutableStorageConfig()->MutableLog()->SetPreferredPoolKind("ssd");
            policy.MutableColumnFamilies(0)->MutableStorageConfig()->MutableLog()->SetAllowOtherKinds(false);
            policy.MutableColumnFamilies(0)->SetColumnCache(NKikimrSchemeOp::ColumnCacheEver);
            Apply(policy, description);

            CheckTableSettings(server, "/Root/ydb_ut_tenant/table-5", description);
        }
    }

    Y_UNIT_TEST(OverwriteCachingPolicy) {
        TKikimrWithGrpcAndRootSchema server;
        InitConfigs(server);

        {
            Ydb::Table::TableProfile profile;
            profile.set_preset_name("profile1");
            profile.mutable_caching_policy()->set_preset_name("caching2");
            CreateTable(server, "/Root/ydb_ut_tenant/table-1", profile);

            NKikimrSchemeOp::TTableDescription description;
            Apply(COMPACTION_POLICY1, description);
            Apply(EXECUTION_POLICY1, description);
            Apply(PARTITIONING_POLICY1, description);
            Apply(STORAGE_POLICY1, description);
            Apply(REPLICATION_POLICY1, description);
            Apply(CACHING_POLICY2, description);

            CheckTableSettings(server, "/Root/ydb_ut_tenant/table-1", description);
        }

        {
            Ydb::Table::TableProfile profile;
            profile.set_preset_name("profile1");
            profile.mutable_caching_policy()->set_preset_name("default");
            CreateTable(server, "/Root/ydb_ut_tenant/table-2", profile);

            NKikimrSchemeOp::TTableDescription description;
            Apply(COMPACTION_POLICY1, description);
            Apply(EXECUTION_POLICY1, description);
            Apply(PARTITIONING_POLICY1, description);
            Apply(STORAGE_POLICY1, description);
            Apply(REPLICATION_POLICY1, description);

            CheckTableSettings(server, "/Root/ydb_ut_tenant/table-2", description);
        }
    }

    Y_UNIT_TEST(WrongTableProfile) {
        TKikimrWithGrpcAndRootSchema server;
        InitConfigs(server);

        {
            Ydb::Table::TableProfile profile;
            profile.set_preset_name("unknown");
            CreateTable(server, "/Root/ydb_ut_tenant/table-1", profile, Ydb::StatusIds::BAD_REQUEST);
        }

        {
            Ydb::Table::TableProfile profile;
            profile.set_preset_name("profile1");
            profile.mutable_compaction_policy();
            CreateTable(server, "/Root/ydb_ut_tenant/table-1", profile, Ydb::StatusIds::BAD_REQUEST);
        }

        {
            Ydb::Table::TableProfile profile;
            profile.set_preset_name("profile1");
            profile.mutable_compaction_policy()->set_preset_name("unknown");
            CreateTable(server, "/Root/ydb_ut_tenant/table-1", profile, Ydb::StatusIds::BAD_REQUEST);
        }

        {
            Ydb::Table::TableProfile profile;
            profile.set_preset_name("profile1");
            profile.mutable_execution_policy();
            CreateTable(server, "/Root/ydb_ut_tenant/table-1", profile, Ydb::StatusIds::BAD_REQUEST);
        }

        {
            Ydb::Table::TableProfile profile;
            profile.set_preset_name("profile1");
            profile.mutable_execution_policy()->set_preset_name("unknown");
            CreateTable(server, "/Root/ydb_ut_tenant/table-1", profile, Ydb::StatusIds::BAD_REQUEST);
        }

        {
            Ydb::Table::TableProfile profile;
            profile.set_preset_name("profile1");
            profile.mutable_partitioning_policy()->set_preset_name("unknown");
            CreateTable(server, "/Root/ydb_ut_tenant/table-1", profile, Ydb::StatusIds::BAD_REQUEST);
        }

        {
            Ydb::Table::TableProfile profile;
            profile.set_preset_name("profile1");
            profile.mutable_storage_policy()->set_preset_name("unknown");
            CreateTable(server, "/Root/table-1", profile, Ydb::StatusIds::BAD_REQUEST);
        }

        {
            Ydb::Table::TableProfile profile;
            profile.set_preset_name("profile1");
            profile.mutable_replication_policy()->set_preset_name("unknown");
            CreateTable(server, "/Root/table-1", profile, Ydb::StatusIds::BAD_REQUEST);
        }

        {
            Ydb::Table::TableProfile profile;
            profile.set_preset_name("profile1");
            profile.mutable_caching_policy()->set_preset_name("unknown");
            CreateTable(server, "/Root/table-1", profile, Ydb::StatusIds::BAD_REQUEST);
        }
    }

    Y_UNIT_TEST(ExplicitPartitionsSimple) {
        TKikimrWithGrpcAndRootSchema server;
        InitConfigs(server);

        Ydb::Table::TableProfile profile;
        profile.set_preset_name("default");
        auto &policy = *profile.mutable_partitioning_policy();
        Ydb::TypedValue point;
        auto &keyType = *point.mutable_type()->mutable_tuple_type();
        keyType.add_elements()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UINT64);
        auto &keyVal = *point.mutable_value();
        keyVal.add_items()->set_uint64_value(10);
        auto &points = *policy.mutable_explicit_partitions();
        points.add_split_points()->CopyFrom(point);
        keyVal.mutable_items(0)->set_uint64_value(20);
        points.add_split_points()->CopyFrom(point);
        keyVal.mutable_items(0)->set_uint64_value(30);
        points.add_split_points()->CopyFrom(point);
        CreateTable(server, "/Root/ydb_ut_tenant/table-1", profile);

        CheckTablePartitions(server, "/Root/ydb_ut_tenant/table-1",
                             policy.explicit_partitions().split_points());
    }

    Y_UNIT_TEST(ExplicitPartitionsUnordered) {
        TKikimrWithGrpcAndRootSchema server;
        InitConfigs(server);

        Ydb::Table::TableProfile profile;
        profile.set_preset_name("default");
        auto &policy = *profile.mutable_partitioning_policy();
        Ydb::TypedValue point;
        auto &keyType = *point.mutable_type()->mutable_tuple_type();
        keyType.add_elements()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UINT64);
        auto &keyVal = *point.mutable_value();
        keyVal.add_items()->set_uint64_value(10);
        auto &points = *policy.mutable_explicit_partitions();
        points.add_split_points()->CopyFrom(point);
        keyVal.mutable_items(0)->set_uint64_value(30);
        points.add_split_points()->CopyFrom(point);
        keyVal.mutable_items(0)->set_uint64_value(20);
        points.add_split_points()->CopyFrom(point);
        CreateTable(server, "/Root/ydb_ut_tenant/table-1", profile, Ydb::StatusIds::SCHEME_ERROR);
    }

    Y_UNIT_TEST(ExplicitPartitionsComplex) {
        TKikimrWithGrpcAndRootSchema server;
        InitConfigs(server);

        Ydb::Table::TableProfile profile;
        profile.set_preset_name("default");
        auto &policy = *profile.mutable_partitioning_policy();
        Ydb::TypedValue point;
        auto &keyType = *point.mutable_type()->mutable_tuple_type();
        keyType.add_elements()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UTF8);
        keyType.add_elements()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UINT64);
        keyType.add_elements()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::STRING);
        auto &keyVal = *point.mutable_value();
        keyVal.add_items()->set_text_value("key1");
        keyVal.add_items()->set_uint64_value(10);
        keyVal.add_items()->set_bytes_value("value1");
        auto &points = *policy.mutable_explicit_partitions();
        points.add_split_points()->CopyFrom(point);
        keyVal.mutable_items(0)->set_text_value("key2");
        keyVal.mutable_items(1)->set_uint64_value(20);
        keyVal.mutable_items(2)->set_bytes_value("value2");
        points.add_split_points()->CopyFrom(point);
        keyVal.mutable_items(0)->set_text_value("key3");
        keyVal.mutable_items(1)->set_uint64_value(30);
        keyVal.mutable_items(2)->set_bytes_value("value3");
        points.add_split_points()->CopyFrom(point);
        CreateTableComplexKey(server, "/Root/ydb_ut_tenant/table-1", profile);

        CheckTablePartitions(server, "/Root/ydb_ut_tenant/table-1",
                             policy.explicit_partitions().split_points());
    }

    Y_UNIT_TEST(ExplicitPartitionsWrongKeyFormat) {
        TKikimrWithGrpcAndRootSchema server;
        InitConfigs(server);

        Ydb::Table::TableProfile profile;
        profile.set_preset_name("default");
        auto &policy = *profile.mutable_partitioning_policy();
        Ydb::TypedValue point;
        auto &keyType = *point.mutable_type()->mutable_tuple_type();
        keyType.add_elements()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UTF8);
        keyType.add_elements()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UINT64);
        keyType.add_elements()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::STRING);
        auto &keyVal = *point.mutable_value();
        keyVal.add_items()->set_text_value("key1");
        keyVal.add_items()->set_text_value("10");
        keyVal.add_items()->set_bytes_value("value1");
        auto &points = *policy.mutable_explicit_partitions();
        points.add_split_points()->CopyFrom(point);
        CreateTableComplexKey(server, "/Root/ydb_ut_tenant/table-1", profile,
                              Ydb::StatusIds::BAD_REQUEST);
    }

    Y_UNIT_TEST(ExplicitPartitionsWrongKeyType) {
        TKikimrWithGrpcAndRootSchema server;
        InitConfigs(server);

        Ydb::Table::TableProfile profile;
        profile.set_preset_name("default");
        auto &policy = *profile.mutable_partitioning_policy();
        Ydb::TypedValue point;
        auto &keyType = *point.mutable_type()->mutable_tuple_type();
        keyType.add_elements()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UTF8);
        keyType.add_elements()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UTF8);
        keyType.add_elements()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::STRING);
        auto &keyVal = *point.mutable_value();
        keyVal.add_items()->set_text_value("key1");
        keyVal.add_items()->set_text_value("10");
        keyVal.add_items()->set_bytes_value("value1");
        auto &points = *policy.mutable_explicit_partitions();
        points.add_split_points()->CopyFrom(point);
        CreateTableComplexKey(server, "/Root/ydb_ut_tenant/table-1", profile, Ydb::StatusIds::SCHEME_ERROR);
    }

    void CheckRepeated(const ::google::protobuf::RepeatedPtrField<TString> &array,
                       THashSet<TString> expected)
    {
        UNIT_ASSERT_VALUES_EQUAL(array.size(), expected.size());
        for (auto &val : array) {
            UNIT_ASSERT(expected.contains(val));
            expected.erase(val);
        }
    }

    void CheckAllowedPolicies(const Ydb::Table::TableProfileDescription &profile,
                              const THashSet<TString> &storage,
                              const THashSet<TString> &partitioning,
                              const THashSet<TString> &execution,
                              const THashSet<TString> &compaction,
                              const THashSet<TString> &replication,
                              const THashSet<TString> &caching)
    {
        CheckRepeated(profile.allowed_storage_policies(), storage);
        CheckRepeated(profile.allowed_partitioning_policies(), partitioning);
        CheckRepeated(profile.allowed_execution_policies(), execution);
        CheckRepeated(profile.allowed_compaction_policies(), compaction);
        CheckRepeated(profile.allowed_replication_policies(), replication);
        CheckRepeated(profile.allowed_caching_policies(), caching);
    }

    void CheckLabels(const ::google::protobuf::Map<TString, TString> &labels,
                     THashMap<TString, TString> expected)
    {
        UNIT_ASSERT_VALUES_EQUAL(labels.size(), expected.size());
        for (auto &pr : labels) {
            UNIT_ASSERT(expected.contains(pr.first));
            UNIT_ASSERT_VALUES_EQUAL(expected.at(pr.first), pr.second);
            expected.erase(pr.first);
        }
    }

    Y_UNIT_TEST(DescribeTableOptions) {
        TKikimrWithGrpcAndRootSchema server;
        InitConfigs(server);


        std::shared_ptr<grpc::Channel> Channel_;
        Channel_ = grpc::CreateChannel("localhost:" + ToString(server.GetPort()), grpc::InsecureChannelCredentials());
        std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
        Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
        grpc::ClientContext context;
        Ydb::Table::DescribeTableOptionsRequest request;
        Ydb::Table::DescribeTableOptionsResponse response;
        auto status = Stub_->DescribeTableOptions(&context, request, &response);
        auto deferred = response.operation();
        UNIT_ASSERT(status.ok());
        UNIT_ASSERT(deferred.ready());
        UNIT_ASSERT_VALUES_EQUAL(deferred.status(), Ydb::StatusIds::SUCCESS);

        Ydb::Table::DescribeTableOptionsResult result;
        deferred.result().UnpackTo(&result);

        THashSet<TString> storage = {{TString("default"), TString("storage1"), TString("storage2")}};
        THashSet<TString> partitioning = {{TString("default"), TString("partitioning1"), TString("partitioning2")}};
        THashSet<TString> execution = {{TString("default"), TString("execution1"), TString("execution2")}};
        THashSet<TString> compaction = {{TString("default"), TString("compaction1"), TString("compaction2")}};
        THashSet<TString> replication = {{TString("default"), TString("replication1"), TString("replication2")}};
        THashSet<TString> caching = {{TString("default"), TString("caching1"), TString("caching2")}};
        for (auto &profile: result.table_profile_presets()) {
            if (profile.name() == "default") {
                UNIT_ASSERT_VALUES_EQUAL(profile.default_storage_policy(), "default");
                UNIT_ASSERT_VALUES_EQUAL(profile.default_partitioning_policy(), "default");
                UNIT_ASSERT_VALUES_EQUAL(profile.default_execution_policy(), "default");
                UNIT_ASSERT_VALUES_EQUAL(profile.default_compaction_policy(), "default");
                UNIT_ASSERT_VALUES_EQUAL(profile.default_replication_policy(), "default");
                UNIT_ASSERT_VALUES_EQUAL(profile.default_caching_policy(), "default");
            } else if (profile.name() == "profile1") {
                UNIT_ASSERT_VALUES_EQUAL(profile.default_storage_policy(), "storage1");
                UNIT_ASSERT_VALUES_EQUAL(profile.default_partitioning_policy(), "partitioning1");
                UNIT_ASSERT_VALUES_EQUAL(profile.default_execution_policy(), "execution1");
                UNIT_ASSERT_VALUES_EQUAL(profile.default_compaction_policy(), "compaction1");
                UNIT_ASSERT_VALUES_EQUAL(profile.default_replication_policy(), "replication1");
                UNIT_ASSERT_VALUES_EQUAL(profile.default_caching_policy(), "caching1");
            } else {
                UNIT_ASSERT_VALUES_EQUAL(profile.name(), "profile2");
                UNIT_ASSERT_VALUES_EQUAL(profile.default_storage_policy(), "storage2");
                UNIT_ASSERT_VALUES_EQUAL(profile.default_partitioning_policy(), "partitioning2");
                UNIT_ASSERT_VALUES_EQUAL(profile.default_execution_policy(), "execution2");
                UNIT_ASSERT_VALUES_EQUAL(profile.default_compaction_policy(), "compaction2");
                UNIT_ASSERT_VALUES_EQUAL(profile.default_replication_policy(), "replication2");
                UNIT_ASSERT_VALUES_EQUAL(profile.default_caching_policy(), "caching2");
            }
            CheckAllowedPolicies(profile, storage, partitioning, execution,
                                 compaction, replication, caching);
        }

        for (auto &description : result.storage_policy_presets()) {
            if (description.name() == "default") {
                CheckLabels(description.labels(), {});
            } else if (description.name() == "storage1") {
                CheckLabels(description.labels(),
                            {{ {TString("syslog"), TString("hdd")},
                               {TString("log"), TString("hdd")},
                               {TString("data"), TString("hdd")},
                               {TString("external"), TString("hdd")},
                               {TString("external_threshold"), ToString(Max<ui32>())},
                               {TString("codec"), TString("lz4")},
                               {TString("in_memory"), TString("false")} }});
            } else {
                UNIT_ASSERT_VALUES_EQUAL(description.name(), "storage2");
                CheckLabels(description.labels(),
                            {{ {TString("syslog"), TString("ssd")},
                               {TString("log"), TString("ssd")},
                               {TString("data"), TString("ssd")},
                               {TString("external"), TString("ssd")},
                               {TString("medium_threshold"), TString("30000")},
                               {TString("codec"), TString("none")},
                               {TString("in_memory"), TString("true")} }});
            }
        }

        for (auto &description : result.partitioning_policy_presets()) {
            if (description.name() == "default") {
                CheckLabels(description.labels(),
                            {{ {TString("auto_split"), TString("disabled")},
                               {TString("auto_merge"), TString("disabled")} }});
            } else if (description.name() == "partitioning1"){
                CheckLabels(description.labels(),
                            {{ {TString("auto_split"), TString("enabled")},
                               {TString("auto_merge"), TString("disabled")},
                               {TString("split_threshold"), TString("123456")},
                               {TString("uniform_parts"), TString("10")} }});
            } else {
                UNIT_ASSERT_VALUES_EQUAL(description.name(), "partitioning2");
                CheckLabels(description.labels(),
                            {{ {TString("auto_split"), TString("enabled")},
                               {TString("auto_merge"), TString("enabled")},
                               {TString("split_threshold"), TString("1000000000")},
                               {TString("uniform_parts"), TString("20")} }});
            }
        }

        for (auto &description : result.execution_policy_presets()) {
            if (description.name() == "default") {
                CheckLabels(description.labels(),
                            {{ {TString("out_of_order"), TString("enabled")},
                               {TString("pipeline_width"), TString("8")},
                               {TString("immediate_tx"), TString("enabled")},
                               {TString("bloom_filter"), TString("disabled")} }});
            } else if (description.name() == "execution1"){
                CheckLabels(description.labels(),
                            {{ {TString("out_of_order"), TString("disabled")},
                               {TString("immediate_tx"), TString("enabled")},
                               {TString("bloom_filter"), TString("enabled")},
                               {TString("tx_read_limit"), TString("10000000")} }});
            } else {
                UNIT_ASSERT_VALUES_EQUAL(description.name(), "execution2");
                CheckLabels(description.labels(),
                            {{ {TString("out_of_order"), TString("enabled")},
                               {TString("pipeline_width"), TString("8")},
                               {TString("immediate_tx"), TString("disabled")},
                               {TString("bloom_filter"), TString("disabled")},
                               {TString("tx_read_limit"), TString("20000000")} }});
            }
        }

        for (auto &description : result.compaction_policy_presets()) {
            if (description.name() == "default") {
                CheckLabels(description.labels(), {});
            } else if (description.name() == "compaction1"){
                CheckLabels(description.labels(),
                            {{ {TString("generations"), TString("1")} }});
            } else {
                UNIT_ASSERT_VALUES_EQUAL(description.name(), "compaction2");
                CheckLabels(description.labels(),
                            {{ {TString("generations"), TString("2")} }});
            }
        }

        for (auto &description : result.replication_policy_presets()) {
            if (description.name() == "default") {
                CheckLabels(description.labels(),
                            {{ {TString("followers"), TString("disabled")} }});
            } else if (description.name() == "replication1"){
                CheckLabels(description.labels(),
                            {{ {TString("followers"), TString("1")},
                               {TString("promotion"), TString("disabled")},
                               {TString("per_zone"), TString("true")} }});
            } else {
                UNIT_ASSERT_VALUES_EQUAL(description.name(), "replication2");
                CheckLabels(description.labels(),
                            {{ {TString("followers"), TString("2")},
                               {TString("promotion"), TString("enabled")},
                               {TString("per_zone"), TString("false")} }});
            }
        }

        for (auto &description : result.caching_policy_presets()) {
            if (description.name() == "default") {
                CheckLabels(description.labels(), {});
            } else if (description.name() == "caching1"){
                CheckLabels(description.labels(),
                            {{ {TString("executor_cache"), TString("10000000")} }});
            } else {
                UNIT_ASSERT_VALUES_EQUAL(description.name(), "caching2");
                CheckLabels(description.labels(),
                            {{ {TString("executor_cache"), TString("20000000")} }});
            }
        }
    }
}

#ifndef _win_

void ExecSchemeYql(std::shared_ptr<grpc::Channel> channel, const TString &sessionId, const TString &yql)
{
    std::unique_ptr<Ydb::Table::V1::TableService::Stub> stub;
    stub = Ydb::Table::V1::TableService::NewStub(channel);
    grpc::ClientContext context;

    Ydb::Table::ExecuteSchemeQueryRequest request;
    request.set_session_id(sessionId);
    request.set_yql_text(yql);

    Ydb::Table::ExecuteSchemeQueryResponse response;
    auto status = stub->ExecuteSchemeQuery(&context, request, &response);
    UNIT_ASSERT(status.ok());
        auto deferred = response.operation();

    UNIT_ASSERT(deferred.ready() == true);
    UNIT_ASSERT(deferred.status() == Ydb::StatusIds::SUCCESS);
}

Ydb::Table::ExecuteQueryResult ExecYql(std::shared_ptr<grpc::Channel> channel, const TString &sessionId, const TString &yql, bool withStat)
{
    std::unique_ptr<Ydb::Table::V1::TableService::Stub> stub;
    stub = Ydb::Table::V1::TableService::NewStub(channel);
    grpc::ClientContext context;
    Ydb::Table::ExecuteDataQueryRequest request;
    request.set_session_id(sessionId);
    request.mutable_query()->set_yql_text(yql);
    request.mutable_tx_control()->mutable_begin_tx()->mutable_serializable_read_write();
    request.mutable_tx_control()->set_commit_tx(true);
    if (withStat) {
        request.set_collect_stats(Ydb::Table::QueryStatsCollection::STATS_COLLECTION_BASIC);
    }
    Ydb::Table::ExecuteDataQueryResponse response;
    auto status = stub->ExecuteDataQuery(&context, request, &response);
    UNIT_ASSERT(status.ok());
    auto deferred = response.operation();
    UNIT_ASSERT(deferred.ready() == true);
    NYql::TIssues issues;
    NYql::IssuesFromMessage(deferred.issues(), issues);
    issues.PrintTo(Cerr);

    UNIT_ASSERT(deferred.status() == Ydb::StatusIds::SUCCESS);

    Ydb::Table::ExecuteQueryResult result;
    Y_ABORT_UNLESS(deferred.result().UnpackTo(&result));
    return result;
}

void CheckYqlDecimalValues(std::shared_ptr<grpc::Channel> channel, const TString &sessionId, const TString &yql,
                           TVector<std::pair<i64, ui64>> vals)
{
    auto result = ExecYql(channel, sessionId, yql);
    UNIT_ASSERT_VALUES_EQUAL(result.result_sets_size(), 1);

    TVector<std::pair<ui64, ui64>> halves;
    for (auto &pr : vals) {
        NYql::NDecimal::TInt128 val = pr.first;
        val *= Power(10, NScheme::DECIMAL_SCALE);
        if (val >= 0)
            val += pr.second;
        else
            val -= pr.second;
        halves.push_back(std::make_pair(reinterpret_cast<ui64*>(&val)[0], reinterpret_cast<ui64*>(&val)[1]));
    }

    auto &result_set = result.result_sets(0);
    UNIT_ASSERT_VALUES_EQUAL(result_set.rows_size(), halves.size());
    for (size_t i = 0; i < halves.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(result_set.rows(i).items(0).low_128(), halves[i].first);
        UNIT_ASSERT_VALUES_EQUAL(result_set.rows(i).items(0).high_128(), halves[i].second);
    }
}

void CreateTable(std::shared_ptr<grpc::Channel> channel,
                 const Ydb::Table::CreateTableRequest &request)
{
    std::unique_ptr<Ydb::Table::V1::TableService::Stub> stub;
    stub = Ydb::Table::V1::TableService::NewStub(channel);
    grpc::ClientContext context;
    Ydb::Table::CreateTableResponse response;
    auto status = stub->CreateTable(&context, request, &response);
    auto deferred = response.operation();
    UNIT_ASSERT(status.ok());
    UNIT_ASSERT(deferred.ready());
    UNIT_ASSERT_VALUES_EQUAL(deferred.status(), Ydb::StatusIds::SUCCESS);
}

void CreateTable(std::shared_ptr<grpc::Channel> channel)
{
    Ydb::Table::CreateTableRequest request;
    request.set_path("/Root/table-1");
    auto &col1 = *request.add_columns();
    col1.set_name("key");
    col1.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::INT32);
    auto &col2 = *request.add_columns();
    col2.set_name("value");
    auto &decimalType = *col2.mutable_type()->mutable_optional_type()->mutable_item()->mutable_decimal_type();
    decimalType.set_precision(NScheme::DECIMAL_PRECISION);
    decimalType.set_scale(NScheme::DECIMAL_SCALE);
    request.add_primary_key("key");

    CreateTable(channel, request);
}

Y_UNIT_TEST_SUITE(TYqlDecimalTests) {
    Y_UNIT_TEST(SimpleUpsertSelect) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> channel
            = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        TString sessionId = CreateSession(channel);

        CreateTable(channel);

        ExecYql(channel, sessionId,
                "UPSERT INTO `/Root/table-1` (key, value) VALUES "
                "(1, CAST(\"1\" as DECIMAL(22,9))),"
                "(2, CAST(\"22.22\" as DECIMAL(22,9))),"
                "(3, CAST(\"9999999999999.999999999\" as DECIMAL(22,9)));");

        CheckYqlDecimalValues(channel, sessionId, "SELECT value FROM `/Root/table-1` WHERE key=1;",
                              {{1, 0}});

        CheckYqlDecimalValues(channel, sessionId, "SELECT value FROM `/Root/table-1` WHERE key >= 1 AND key <= 3;",
                              {{1, 0}, {22, 220000000}, {9999999999999, 999999999}});
    }

    Y_UNIT_TEST(NegativeValues) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> channel
            = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        TString sessionId = CreateSession(channel);

        CreateTable(channel);

        ExecYql(channel, sessionId,
                "UPSERT INTO `/Root/table-1` (key, value) VALUES "
                "(1, CAST(\"-1\" as DECIMAL(22,9))),"
                "(2, CAST(\"-22.22\" as DECIMAL(22,9))),"
                "(3, CAST(\"-9999999999999.999999999\" as DECIMAL(22,9)));");

        CheckYqlDecimalValues(channel, sessionId, "SELECT value FROM `/Root/table-1` WHERE key=1;",
                              {{-1, 0}});

        CheckYqlDecimalValues(channel, sessionId, "SELECT value FROM `/Root/table-1` WHERE key >= 1 AND key <= 3;",
                              {{-1, 0}, {-22, 220000000}, {-9999999999999, 999999999}});
    }

    Y_UNIT_TEST(DecimalKey) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> channel
            = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        TString sessionId = CreateSession(channel);

        Ydb::Table::CreateTableRequest request;
        request.set_path("/Root/table-1");
        auto &col1 = *request.add_columns();
        col1.set_name("key");
        auto &decimalType = *col1.mutable_type()->mutable_optional_type()->mutable_item()->mutable_decimal_type();
        decimalType.set_precision(NScheme::DECIMAL_PRECISION);
        decimalType.set_scale(NScheme::DECIMAL_SCALE);
        auto &col2 = *request.add_columns();
        col2.set_name("value");
        col2.mutable_type()->CopyFrom(col1.type());
        request.add_primary_key("key");

        CreateTable(channel, request);

        ExecYql(channel, sessionId, R"(
                UPSERT INTO `/Root/table-1` (key, value) VALUES
                (CAST("1" as DECIMAL(22,9)), CAST("1" as DECIMAL(22,9))),
                (CAST("22.22" as DECIMAL(22,9)), CAST("22.22" as DECIMAL(22,9))),
                (CAST("9999999999999.999999999" as DECIMAL(22,9)), CAST("9999999999999.999999999" as DECIMAL(22,9))),
                (CAST("-1" as DECIMAL(22,9)), CAST("-1" as DECIMAL(22,9))),
                (CAST("-22.22" as DECIMAL(22,9)), CAST("-22.22" as DECIMAL(22,9))),
                (CAST("-9999999999999.999999999" as DECIMAL(22,9)), CAST("-9999999999999.999999999" as DECIMAL(22,9)));
        )");

        CheckYqlDecimalValues(channel, sessionId, "SELECT value FROM `/Root/table-1` WHERE key=CAST(\"1\" as DECIMAL(22,9));",
                              {{1, 0}});

        CheckYqlDecimalValues(channel, sessionId, "SELECT value FROM `/Root/table-1` WHERE key=CAST(\"22.22\" as DECIMAL(22,9));",
                              {{22, 220000000}});

        CheckYqlDecimalValues(channel, sessionId, "SELECT value FROM `/Root/table-1` WHERE key=CAST(\"9999999999999.999999999\" as DECIMAL(22,9));",
                              {{9999999999999, 999999999}});

        CheckYqlDecimalValues(channel, sessionId, "SELECT value FROM `/Root/table-1` WHERE key=CAST(\"-1\" as DECIMAL(22,9));",
                              {{-1, 0}});

        CheckYqlDecimalValues(channel, sessionId, "SELECT value FROM `/Root/table-1` WHERE key=CAST(\"-22.22\" as DECIMAL(22,9));",
                              {{-22, 220000000}});

        CheckYqlDecimalValues(channel, sessionId, "SELECT value FROM `/Root/table-1` WHERE key=CAST(\"-9999999999999.999999999\" as DECIMAL(22,9));",
                              {{-9999999999999, 999999999}});

        CheckYqlDecimalValues(channel, sessionId, "SELECT value FROM `/Root/table-1` WHERE key >= CAST(\"-22.22\" as DECIMAL(22,9))",
                              {{-22, 220000000}, {-1, 0},
                               {1, 0}, {22, 220000000}, {9999999999999, 999999999}});

        CheckYqlDecimalValues(channel, sessionId, "SELECT value FROM `/Root/table-1` WHERE key < CAST(\"-22.22\" as DECIMAL(22,9))",
                              {{-9999999999999, 999999999}});

        CheckYqlDecimalValues(channel, sessionId, "SELECT value FROM `/Root/table-1` WHERE key > CAST(\"-22.222\" as DECIMAL(22,9)) AND key < CAST(\"22.222\" as DECIMAL(22,9))",
                              {{-22, 220000000}, {-1, 0},
                               {1, 0}, {22, 220000000}});
    }
}

void CheckDateValues(std::shared_ptr<grpc::Channel> channel,
                     const TString &sessionId,
                     const TString &yql,
                     TVector<TInstant> vals)
{
    auto result = ExecYql(channel, sessionId, yql);
    UNIT_ASSERT_VALUES_EQUAL(result.result_sets_size(), 1);

    auto &res = result.result_sets(0);
    UNIT_ASSERT_VALUES_EQUAL(res.rows_size(), vals.size());
    for (size_t i = 0; i < vals.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(res.rows(i).items(0).uint32_value(), vals[i].Days());
    }
}

void CheckDatetimeValues(std::shared_ptr<grpc::Channel> channel,
                         const TString &sessionId,
                         const TString &yql,
                         TVector<TInstant> vals)
{
    auto result = ExecYql(channel, sessionId, yql);
    UNIT_ASSERT_VALUES_EQUAL(result.result_sets_size(), 1);

    auto &res = result.result_sets(0);
    UNIT_ASSERT_VALUES_EQUAL(res.rows_size(), vals.size());
    for (size_t i = 0; i < vals.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(res.rows(i).items(0).uint32_value(), vals[i].Seconds());
    }
}

void CheckTimestampValues(std::shared_ptr<grpc::Channel> channel,
                          const TString &sessionId,
                          const TString &yql,
                          TVector<TInstant> vals)
{
    auto result = ExecYql(channel, sessionId, yql);
    UNIT_ASSERT_VALUES_EQUAL(result.result_sets_size(), 1);

    auto &res = result.result_sets(0);
    UNIT_ASSERT_VALUES_EQUAL(res.rows_size(), vals.size());
    for (size_t i = 0; i < vals.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(res.rows(i).items(0).uint64_value(), vals[i].MicroSeconds());
    }
}

void CheckIntervalValues(std::shared_ptr<grpc::Channel> channel,
                          const TString &sessionId,
                          const TString &yql,
                          TVector<i64> vals)
{
    auto result = ExecYql(channel, sessionId, yql);
    UNIT_ASSERT_VALUES_EQUAL(result.result_sets_size(), 1);

    auto &res = result.result_sets(0);
    UNIT_ASSERT_VALUES_EQUAL(res.rows_size(), vals.size());
    for (size_t i = 0; i < vals.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(res.rows(i).items(0).int64_value(), vals[i]);
    }
}

Y_UNIT_TEST_SUITE(TYqlDateTimeTests) {
    Y_UNIT_TEST(SimpleUpsertSelect) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> channel
            = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        TString sessionId = CreateSession(channel);

        Ydb::Table::CreateTableRequest request;
        TString scheme(R"___(
            path: "/Root/table-1"
            columns { name: "key" type: { optional_type { item { type_id: UINT64 } } } }
            columns { name: "val1" type: { optional_type { item { type_id: DATE } } } }
            columns { name: "val2" type: { optional_type { item { type_id: DATETIME } } } }
            columns { name: "val3" type: { optional_type { item { type_id: TIMESTAMP } } } }
            columns { name: "val4" type: { optional_type { item { type_id: INTERVAL } } } }
            primary_key: ["key"]
        )___");
        ::google::protobuf::TextFormat::ParseFromString(scheme, &request);

        CreateTable(channel, request);

        ExecYql(channel, sessionId, R"___(
                UPSERT INTO `/Root/table-1` (key, val1, val2, val3, val4) VALUES
                (1, CAST(0 as DATE),
                    CAST(0 as DATETIME),
                    CAST(0 as TIMESTAMP),
                    CAST(0 as INTERVAL)),
                (2, CAST(1000 as DATE),
                    CAST(1000 as DATETIME),
                    CAST(1000 as TIMESTAMP),
                    CAST(1000 as INTERVAL)),
                (3, CAST('2050-01-01' as DATE),
                    CAST('2050-01-01T00:00:00Z' as DATETIME),
                    CAST('2050-01-01T00:00:00.000000Z' as TIMESTAMP),
                    CAST(-1000 as INTERVAL));
        )___");

        CheckDateValues(channel, sessionId, "SELECT val1 FROM `/Root/table-1`;",
                        {TInstant::Zero(), TInstant::Days(1000), TInstant::ParseIso8601("2050-01-01T00:00:00Z")});

        CheckDatetimeValues(channel, sessionId, "SELECT val2 FROM `/Root/table-1`;",
                            {TInstant::Zero(), TInstant::Seconds(1000), TInstant::ParseIso8601("2050-01-01T00:00:00Z")});

        CheckTimestampValues(channel, sessionId, "SELECT val3 FROM `/Root/table-1`;",
                            {TInstant::Zero(), TInstant::MicroSeconds(1000), TInstant::ParseIso8601("2050-01-01T00:00:00Z")});

        CheckIntervalValues(channel, sessionId, "SELECT val4 FROM `/Root/table-1`;",
                            {0, 1000, -1000});
    }

    Y_UNIT_TEST(DateKey) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> channel
            = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        TString sessionId = CreateSession(channel);

        Ydb::Table::CreateTableRequest request;
        TString scheme(R"___(
            path: "/Root/table-1"
            columns { name: "key" type: { optional_type { item { type_id: DATE } } } }
            primary_key: ["key"]
        )___");
        ::google::protobuf::TextFormat::ParseFromString(scheme, &request);

        CreateTable(channel, request);

        ExecYql(channel, sessionId, R"___(
                UPSERT INTO `/Root/table-1` (key) VALUES
                (CAST('2000-01-01' as DATE)),
                (CAST('2020-01-01' as DATE)),
                (CAST('2050-01-01' as DATE));
        )___");

        CheckDateValues(channel, sessionId, "SELECT key FROM `/Root/table-1` WHERE key = CAST('2050-01-01' as DATE);",
                        {TInstant::ParseIso8601("2050-01-01T00:00:00Z")});
        CheckDateValues(channel, sessionId, "SELECT key FROM `/Root/table-1` WHERE key > CAST('2000-01-01' as DATE);",
                        {TInstant::ParseIso8601("2020-01-01T00:00:00Z"),
                         TInstant::ParseIso8601("2050-01-01T00:00:00Z")});
        CheckDateValues(channel, sessionId, "SELECT key FROM `/Root/table-1` WHERE key < CAST('2050-01-01' as DATE);",
                        {TInstant::ParseIso8601("2000-01-01T00:00:00Z"),
                         TInstant::ParseIso8601("2020-01-01T00:00:00Z")});
    }

    Y_UNIT_TEST(DatetimeKey) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> channel
            = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        TString sessionId = CreateSession(channel);

        Ydb::Table::CreateTableRequest request;
        TString scheme(R"___(
            path: "/Root/table-1"
            columns { name: "key" type: { optional_type { item { type_id: DATETIME } } } }
            primary_key: ["key"]
        )___");
        ::google::protobuf::TextFormat::ParseFromString(scheme, &request);

        CreateTable(channel, request);

        ExecYql(channel, sessionId, R"___(
                UPSERT INTO `/Root/table-1` (key) VALUES
                (CAST('2000-01-01T00:00:00Z' as DATETIME)),
                (CAST('2020-01-01T00:00:00Z' as DATETIME)),
                (CAST('2050-01-01T00:00:00Z' as DATETIME));
        )___");

        CheckDatetimeValues(channel, sessionId, "SELECT key FROM `/Root/table-1` WHERE key = CAST('2050-01-01T00:00:00Z' as DATETIME);",
                            {TInstant::ParseIso8601("2050-01-01T00:00:00Z")});
        CheckDatetimeValues(channel, sessionId, "SELECT key FROM `/Root/table-1` WHERE key > CAST('2000-01-01T00:00:00Z' as DATETIME);",
                            {TInstant::ParseIso8601("2020-01-01T00:00:00Z"),
                             TInstant::ParseIso8601("2050-01-01T00:00:00Z")});
        CheckDatetimeValues(channel, sessionId, "SELECT key FROM `/Root/table-1` WHERE key < CAST('2050-01-01T00:00:00Z' as DATETIME);",
                            {TInstant::ParseIso8601("2000-01-01T00:00:00Z"),
                             TInstant::ParseIso8601("2020-01-01T00:00:00Z")});
    }

    Y_UNIT_TEST(TimestampKey) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> channel
            = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        TString sessionId = CreateSession(channel);

        Ydb::Table::CreateTableRequest request;
        TString scheme(R"___(
            path: "/Root/table-1"
            columns { name: "key" type: { optional_type { item { type_id: TIMESTAMP } } } }
            primary_key: ["key"]
        )___");
        ::google::protobuf::TextFormat::ParseFromString(scheme, &request);

        CreateTable(channel, request);

        ExecYql(channel, sessionId, R"___(
                UPSERT INTO `/Root/table-1` (key) VALUES
                (CAST('2000-01-01T00:00:00.000000Z' as TIMESTAMP)),
                (CAST('2020-01-01T00:00:00.000000Z' as TIMESTAMP)),
                (CAST('2050-01-01T00:00:00.000000Z' as TIMESTAMP));
        )___");

        CheckTimestampValues(channel, sessionId, "SELECT key FROM `/Root/table-1` WHERE key = CAST('2050-01-01T00:00:00.000000Z' as TIMESTAMP);",
                             {TInstant::ParseIso8601("2050-01-01T00:00:00Z")});
        CheckTimestampValues(channel, sessionId, "SELECT key FROM `/Root/table-1` WHERE key > CAST('2000-01-01T00:00:00.000000Z' as TIMESTAMP);",
                             {TInstant::ParseIso8601("2020-01-01T00:00:00Z"),
                              TInstant::ParseIso8601("2050-01-01T00:00:00Z")});
        CheckTimestampValues(channel, sessionId, "SELECT key FROM `/Root/table-1` WHERE key < CAST('2050-01-01T00:00:00.000000Z' as TIMESTAMP);",
                             {TInstant::ParseIso8601("2000-01-01T00:00:00Z"),
                              TInstant::ParseIso8601("2020-01-01T00:00:00Z")});
    }

    Y_UNIT_TEST(IntervalKey) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> channel
            = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        TString sessionId = CreateSession(channel);

        Ydb::Table::CreateTableRequest request;
        TString scheme(R"___(
            path: "/Root/table-1"
            columns { name: "key" type: { optional_type { item { type_id: INTERVAL } } } }
            primary_key: ["key"]
        )___");
        ::google::protobuf::TextFormat::ParseFromString(scheme, &request);

        CreateTable(channel, request);

        ExecYql(channel, sessionId, R"___(
                UPSERT INTO `/Root/table-1` (key) VALUES
                (CAST(0 as INTERVAL)),
                (CAST(1000 as INTERVAL)),
                (CAST(-1000 as INTERVAL));
        )___");

        CheckIntervalValues(channel, sessionId, "SELECT key FROM `/Root/table-1` WHERE key = CAST(1000 as INTERVAL);",
                            {1000});
        CheckIntervalValues(channel, sessionId, "SELECT key FROM `/Root/table-1` WHERE key >= CAST(0 as INTERVAL);",
                            {0, 1000});
        CheckIntervalValues(channel, sessionId, "SELECT key FROM `/Root/table-1` WHERE key < CAST(1000 as INTERVAL);",
                            {-1000, 0});
    }

    Y_UNIT_TEST(SimpleOperations) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> channel
            = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        TString sessionId = CreateSession(channel);

        {
            Ydb::Table::CreateTableRequest request;
            TString scheme(R"___(
                path: "/Root/table-1"
                columns { name: "key" type: { optional_type { item { type_id: UINT32 } } } }
                columns { name: "val" type: { optional_type { item { type_id: TIMESTAMP } } } }
                primary_key: ["key"]
            )___");
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);

            CreateTable(channel, request);
        }
        {
            Ydb::Table::CreateTableRequest request;
            TString scheme(R"___(
                path: "/Root/table-2"
                columns { name: "key" type: { optional_type { item { type_id: UINT32 } } } }
                columns { name: "val" type: { optional_type { item { type_id: INTERVAL } } } }
                primary_key: ["key"]
            )___");
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);

            CreateTable(channel, request);
        }

        ExecYql(channel, sessionId, R"___(
                UPSERT INTO `/Root/table-1` (key, val) VALUES
                (1, CAST('2000-01-01T00:00:00.000000Z' as TIMESTAMP)),
                (2, CAST('2020-01-01T00:00:00.000000Z' as TIMESTAMP));
        )___");
        ExecYql(channel, sessionId, R"___(
                UPSERT INTO `/Root/table-2` (key, val) VALUES
                (1, CAST(3600000000 as INTERVAL)),
                (2, CAST(143123456 as INTERVAL));
        )___");

        ExecYql(channel, sessionId, R"___(
                $t1 = (SELECT val FROM `/Root/table-1` WHERE key = 1);
                $t2 = (SELECT val FROM `/Root/table-1` WHERE key = 2);
                $i1 = (SELECT val FROM `/Root/table-2` WHERE key = 1);
                $i2 = (SELECT val FROM `/Root/table-2` WHERE key = 2);
                UPSERT INTO `/Root/table-1` (key, val) VALUES
                (3, $t1 + $i1),
                (4, $t2 + $i2);
        )___");

        ExecYql(channel, sessionId, R"___(
                $t1 = (SELECT val FROM `/Root/table-1` WHERE key = 1);
                $t2 = (SELECT val FROM `/Root/table-1` WHERE key = 2);
                $i1 = (SELECT val FROM `/Root/table-2` WHERE key = 1);
                $i2 = (SELECT val FROM `/Root/table-2` WHERE key = 2);
                UPSERT INTO `/Root/table-2` (key, val) VALUES
                (3, $i1 + $i2),
                (4, $t2 - $t1),
                (5, $t1 - $t2);
        )___");

        CheckTimestampValues(channel, sessionId, "SELECT val FROM `/Root/table-1` WHERE key = 3;",
                             {TInstant::ParseIso8601("2000-01-01T01:00:00Z")});

        CheckTimestampValues(channel, sessionId, "SELECT val FROM `/Root/table-1` WHERE key = 4;",
                             {TInstant::ParseIso8601("2020-01-01T00:00:00Z") + TDuration::MicroSeconds(143123456)});

        CheckIntervalValues(channel, sessionId, "SELECT val FROM `/Root/table-2` WHERE key = 3;",
                            {3743123456});

        auto diff = TInstant::ParseIso8601("2020-01-01T00:00:00Z") - TInstant::ParseIso8601("2000-01-01T00:00:00Z");
        CheckIntervalValues(channel, sessionId, "SELECT val FROM `/Root/table-2` WHERE key = 4;",
                            {static_cast<i64>(diff.MicroSeconds())});

        CheckIntervalValues(channel, sessionId, "SELECT val FROM `/Root/table-2` WHERE key = 5;",
                            {- static_cast<i64>(diff.MicroSeconds())});

        CheckTimestampValues(channel, sessionId, R"___(
                             $v1 = (SELECT val FROM `/Root/table-1` WHERE key = 1);
                             $v2 = (SELECT val FROM `/Root/table-2` WHERE key = 1);
                             SELECT ($v1 + $v2);)___",
                             {TInstant::ParseIso8601("2000-01-01T01:00:00Z")});
    }
}
#endif

Y_UNIT_TEST_SUITE(LocalityOperation) {
Y_UNIT_TEST(LocksFromAnotherTenants) {
    TKikimrWithGrpcAndRootSchema server;
    //server.Server_->SetupLogging(

    auto connection = NYdb::TDriver(
        TDriverConfig()
            .SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));

    TString first_tenant_name = "ydb_tenant_0";
    TString second_tenant_name = "ydb_tenant_1";

    {
        TClient admClient(*server.ServerSettings);
        for (auto& tenant_name: TVector<TString>{first_tenant_name, second_tenant_name}) {
            TString tenant_path = Sprintf("/Root/%s", tenant_name.c_str());

            TStoragePools tenant_pools = CreatePoolsForTenant(admClient, server.ServerSettings->StoragePoolTypes, tenant_path);

            UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                                     admClient.CreateSubdomain("/Root", GetSubDomainDeclarationSetting(tenant_name)));
            UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_INPROGRESS,
                                     admClient.AlterSubdomain("/Root", GetSubDomainDefaultSetting(tenant_name, tenant_pools), TDuration::MilliSeconds(500)));

            server.Tenants_->Run(tenant_path, 1);
        }
    }

    NYdb::NTable::TTableClient client(connection);

    auto sessionResult = client.CreateSession().ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL(sessionResult.GetStatus(), EStatus::SUCCESS);
    auto session = sessionResult.GetSession();

    for (auto& tenant_name: TVector<TString>{first_tenant_name, second_tenant_name}) {
        auto tableBuilder = client.GetTableBuilder();
        tableBuilder
            .AddNullableColumn("Key", EPrimitiveType::Uint32)
            .AddNullableColumn("Value", EPrimitiveType::Utf8);
        tableBuilder.SetPrimaryKeyColumn("Key");

        TString table_path = Sprintf("/Root/%s/table", tenant_name.c_str());

        auto result = session.CreateTable(table_path, tableBuilder.Build()).ExtractValueSync();
        UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
        UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
    }

    {
        TString query = Sprintf("UPSERT INTO `Root/%s/table` (Key, Value) VALUES (1u, \"One\");", first_tenant_name.c_str());
        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
        UNIT_ASSERT_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetStatus() << result.GetIssues().ToString());
    }

    {
        TString query = Sprintf("UPSERT INTO `Root/%s/table` (Key, Value) VALUES (2u, \"Second\");", second_tenant_name.c_str());
        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
        UNIT_ASSERT_EQUAL_C(result.GetStatus(), EStatus::SUCCESS,
                            "Status: " << result.GetStatus()
                                << " Issues: " << result.GetIssues().ToString());
    }

    {
        TString query = Sprintf("UPSERT INTO `Root/%s/table` (Key, Value) SELECT Key, Value FROM `Root/%s/table`;", second_tenant_name.c_str(), first_tenant_name.c_str());
        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
        UNIT_ASSERT_EQUAL_C(result.GetStatus(), EStatus::CANCELLED,
                            "Status: " << result.GetStatus()
                                       << " Issues: " << result.GetIssues().ToString());
    }
}
}

Y_UNIT_TEST_SUITE(TDatabaseQuotas) {

NKikimrConfig::TStoragePolicy CreateDefaultStoragePolicy(const TString& poolKind) {
    NKikimrSchemeOp::TStorageConfig config;
    config.MutableSysLog()->SetPreferredPoolKind(poolKind);
    config.MutableLog()->SetPreferredPoolKind(poolKind);
    config.MutableData()->SetPreferredPoolKind(poolKind);

    NKikimrConfig::TStoragePolicy policy;
    auto* family = policy.AddColumnFamilies();
    *family->MutableStorageConfig() = std::move(config);

    return policy;
}

NKikimrConfig::TTableProfilesConfig CreateDefaultTableProfilesConfig(const TString& poolKind) {
    constexpr const char* name = "default";
    NKikimrConfig::TTableProfilesConfig profiles;
    {
        auto* policy = profiles.AddStoragePolicies();
        *policy = CreateDefaultStoragePolicy(poolKind);
        policy->SetName(name);
    }
    {
        auto* profile = profiles.AddTableProfiles();
        profile->SetName(name);
        profile->SetStoragePolicy(name);
    }

    return profiles;
}

void CompactTableAndCheckResult(TTestActorRuntime& runtime, ui64 shardId, const TTableId& tableId) {
    auto compactionResult = CompactTable(runtime, shardId, tableId);
    UNIT_ASSERT_VALUES_EQUAL(compactionResult.GetStatus(), NKikimrTxDataShard::TEvCompactTableResult::OK);
}

ui64 RunSchemeTx(
        TTestActorRuntimeBase& runtime,
        THolder<TEvTxUserProxy::TEvProposeTransaction>&& request,
        TActorId sender = {},
        bool viaActorSystem = false,
        TEvTxUserProxy::TEvProposeTransactionStatus::EStatus expectedStatus
            = TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecInProgress
) {
    if (!sender) {
        sender = runtime.AllocateEdgeActor();
    }

    runtime.Send(new IEventHandle(MakeTxProxyID(), sender, request.Release()), 0, viaActorSystem);
    auto ev = runtime.GrabEdgeEventRethrow<TEvTxUserProxy::TEvProposeTransactionStatus>(sender);
    UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetStatus(), expectedStatus);

    return ev->Get()->Record.GetTxId();
}

THolder<TEvTxUserProxy::TEvProposeTransaction> SchemeTxTemplate(
        NKikimrSchemeOp::EOperationType type,
        const TString& workingDir
) {
    auto request = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
    request->Record.SetExecTimeoutPeriod(Max<ui64>());

    auto& tx = *request->Record.MutableTransaction()->MutableModifyScheme();
    tx.SetOperationType(type);
    tx.SetWorkingDir(workingDir);

    return request;
}

void WaitTxNotification(TServer::TPtr server, TActorId sender, ui64 txId) {
    auto& runtime = *server->GetRuntime();
    auto& settings = server->GetSettings();

    auto request = MakeHolder<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion>();
    request->Record.SetTxId(txId);
    auto tid = ChangeStateStorage(SchemeRoot, settings.Domain);
    runtime.SendToPipe(tid, sender, request.Release(), 0, GetPipeConfigWithRetries());
    runtime.GrabEdgeEventRethrow<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult>(sender);
}

void CreateSubdomain(TServer::TPtr server,
                     TActorId sender,
                     const TString& workingDir,
                     const NKikimrSubDomains::TSubDomainSettings& settings
) {
    auto request = SchemeTxTemplate(NKikimrSchemeOp::ESchemeOpCreateSubDomain, workingDir);

    auto& tx = *request->Record.MutableTransaction()->MutableModifyScheme();
    *tx.MutableSubDomain() = settings;

    WaitTxNotification(server, sender, RunSchemeTx(*server->GetRuntime(), std::move(request), sender));
}

void AlterSubdomain(TServer::TPtr server,
                    TActorId sender,
                    const TString& workingDir,
                    const NKikimrSubDomains::TSubDomainSettings& settings
) {
    auto request = SchemeTxTemplate(NKikimrSchemeOp::ESchemeOpAlterSubDomain, workingDir);

    auto& tx = *request->Record.MutableTransaction()->MutableModifyScheme();
    *tx.MutableSubDomain() = settings;

    // Don't wait for completion. It won't happen until the resources (i. e. dynamic nodes) are provided.
    RunSchemeTx(*server->GetRuntime(), std::move(request), sender);
}

Y_UNIT_TEST(DisableWritesToDatabase) {
    TPortManager portManager;
    ui16 mbusPort = portManager.GetPort();
    TServerSettings serverSettings(mbusPort);
    serverSettings
        .SetUseRealThreads(false)
        .SetDynamicNodeCount(1)
        .SetDomainName("Root");

    TStoragePools storagePools = {{"/Root:ssd", "ssd"}, {"/Root:hdd", "hdd"}};
    for (const auto& pool : storagePools) {
        serverSettings.AddStoragePool(pool.GetKind(), pool.GetName());
    }
    NKikimrConfig::TAppConfig appConfig;
    // default table profile with a storage policy is needed to be able to create a table with families
    *appConfig.MutableTableProfilesConfig() = CreateDefaultTableProfilesConfig(storagePools[0].GetKind());
    serverSettings.SetAppConfig(appConfig);

    TServer::TPtr server = new TServer(serverSettings);
    auto& runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();
    InitRoot(server, sender);

    runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NLog::PRI_TRACE);
    NDataShard::gDbStatsReportInterval = TDuration::Seconds(0);
    NDataShard::gDbStatsDataSizeResolution = 1;
    NDataShard::gDbStatsRowCountResolution = 1;

    TString tenant = "tenant";
    TString tenantPath = Sprintf("/Root/%s", tenant.c_str());

    CreateSubdomain(server, sender, "/Root", GetSubDomainDeclarationSetting(tenant));

    auto subdomainSettings = GetSubDomainDefaultSetting(tenant, storagePools);
    auto* parsedQuotas = subdomainSettings.MutableDatabaseQuotas();
    constexpr const char* quotas = R"(
        storage_quotas {
            unit_kind: "hdd"
            data_size_hard_quota: 1
        }
    )";
    UNIT_ASSERT_C(NProtoBuf::TextFormat::ParseFromString(quotas, parsedQuotas), quotas);
    AlterSubdomain(server, sender, "/Root", subdomainSettings);

    TTenants tenants(server);
    tenants.Run(tenantPath, 1);

    TString table = Sprintf("%s/table", tenantPath.c_str());
    ExecSQL(server, sender, Sprintf(R"(
                CREATE TABLE `%s` (
                    Key Uint32,
                    Value Utf8 FAMILY hdd,
                    PRIMARY KEY (Key),
                    FAMILY default (
                        DATA = "ssd",
                        COMPRESSION = "off"
                    ),
                    FAMILY hdd (
                        DATA = "hdd",
                        COMPRESSION = "lz4"
                    )
                );
            )", table.c_str()
        ), false
    );

    ExecSQL(server, sender, Sprintf(R"(
                UPSERT INTO `%s` (Key, Value) VALUES (1u, "Foo");
            )", table.c_str()
        )
    );

    auto shards = GetTableShards(server, sender, table);
    UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1);
    auto& datashard = shards[0];
    auto tableId = ResolveTableId(server, sender, table);

    // Compaction is a must. Table stats are missing channels usage statistics until the table is compacted at least once.
    CompactTableAndCheckResult(runtime, datashard, tableId);
    WaitTableStats(runtime, datashard, 1);

    ExecSQL(server, sender, Sprintf(R"(
                UPSERT INTO `%s` (Key, Value) VALUES (2u, "Bar");
            )", table.c_str()
        ), true, Ydb::StatusIds::UNAVAILABLE
    );
    auto schemeEntry = Navigate(runtime, sender, tenantPath, NSchemeCache::TSchemeCacheNavigate::EOp::OpPath)->ResultSet.at(0);
    UNIT_ASSERT_C(schemeEntry.DomainDescription, schemeEntry.ToString());
    auto& domainDescription = schemeEntry.DomainDescription->Description;
    UNIT_ASSERT_C(domainDescription.HasDomainState(), domainDescription.DebugString());
    bool quotaExceeded = domainDescription.GetDomainState().GetDiskQuotaExceeded();
    UNIT_ASSERT_C(quotaExceeded, domainDescription.DebugString());
}

}

} // namespace NKikimr
