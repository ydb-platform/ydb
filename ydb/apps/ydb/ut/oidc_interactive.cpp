#include <ydb/apps/ydb/ut/mock_env.h>
#include <ydb/public/api/grpc/ydb_query_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/tests/unit/client/oidc/helpers/test_server.h>

#include <library/cpp/testing/common/env.h>

#include <util/datetime/base.h>
#include <util/generic/scope.h>
#include <util/system/env.h>
#include <util/system/shellcommand.h>

#include <atomic>
#include <csignal>

namespace {

class TInteractiveQueryService : public TMockGrpcServiceBase<Ydb::Query::V1::QueryService::Service> {
public:
    grpc::Status CreateSession(grpc::ServerContext* context, const Ydb::Query::CreateSessionRequest*,
        Ydb::Query::CreateSessionResponse* response) override;
    grpc::Status AttachSession(grpc::ServerContext* context, const Ydb::Query::AttachSessionRequest*,
        grpc::ServerWriter<Ydb::Query::SessionState>* writer) override;
    grpc::Status ExecuteQuery(grpc::ServerContext*, const Ydb::Query::ExecuteQueryRequest*,
        grpc::ServerWriter<Ydb::Query::ExecuteQueryResponsePart>* writer) override;
    grpc::Status DeleteSession(grpc::ServerContext*, const Ydb::Query::DeleteSessionRequest*,
        Ydb::Query::DeleteSessionResponse* response) override;

    std::atomic<size_t> CreatedSessions = 0;
    std::atomic<size_t> ExecutedQueries = 0;
};

class TInteractiveOidcFixture : public TCliTestFixture {
public:
    void AddServices() override;
    TString RunUnfinishedAuthorization(int signal);
};

grpc::Status TInteractiveQueryService::CreateSession(grpc::ServerContext* context,
    const Ydb::Query::CreateSessionRequest*, Ydb::Query::CreateSessionResponse* response)
{
    const auto token = context->client_metadata().find("x-ydb-auth-ticket");
    if (token == context->client_metadata().end() || token->second != "Bearer device-token") {
        return grpc::Status(grpc::StatusCode::UNAUTHENTICATED, "Unexpected credentials");
    }
    ++CreatedSessions;
    response->set_status(Ydb::StatusIds::SUCCESS);
    response->set_session_id("interactive-session");
    return grpc::Status::OK;
}

grpc::Status TInteractiveQueryService::AttachSession(grpc::ServerContext* context,
    const Ydb::Query::AttachSessionRequest*, grpc::ServerWriter<Ydb::Query::SessionState>* writer)
{
    Ydb::Query::SessionState state;
    state.set_status(Ydb::StatusIds::SUCCESS);
    writer->Write(state);
    while (!context->IsCancelled()) {
        Sleep(TDuration::MilliSeconds(10));
    }
    return grpc::Status::OK;
}

grpc::Status TInteractiveQueryService::ExecuteQuery(grpc::ServerContext*,
    const Ydb::Query::ExecuteQueryRequest*, grpc::ServerWriter<Ydb::Query::ExecuteQueryResponsePart>* writer)
{
    ++ExecutedQueries;
    Ydb::Query::ExecuteQueryResponsePart response;
    response.set_status(Ydb::StatusIds::SUCCESS);
    auto* result = response.mutable_result_set();
    auto* column = result->add_columns();
    column->set_name("version");
    column->mutable_type()->set_type_id(Ydb::Type::STRING);
    result->add_rows()->add_items()->set_bytes_value("mock-server-version");
    writer->Write(response);
    return grpc::Status::OK;
}

grpc::Status TInteractiveQueryService::DeleteSession(grpc::ServerContext*,
    const Ydb::Query::DeleteSessionRequest*, Ydb::Query::DeleteSessionResponse* response)
{
    response->set_status(Ydb::StatusIds::SUCCESS);
    return grpc::Status::OK;
}

void TInteractiveOidcFixture::AddServices() {
    TCliTestFixture::AddServices();
    AddService<TInteractiveQueryService>();
}

TString TInteractiveOidcFixture::RunUnfinishedAuthorization(int signal) {
    TOidcTestServer idp;
    idp.Enqueue(TStringBuilder() << R"({"device_code":"device-code","user_code":"CODE","verification_uri":")"
        << idp.Issuer() << R"(/verify","expires_in":)" << (signal == 0 ? 2 : 120)
        << R"(,"interval":1})", HTTP_OK);
    for (size_t i = 0; i < 30; ++i) {
        idp.Enqueue(R"({"error":"authorization_pending"})", HTTP_BAD_REQUEST);
    }

    TTempDir home;
    TShellCommandOptions options;
    options.SetUseShell(false).SetAsync(true).SetCloseInput(true);
    options.Environment = {
        {"HOME", home.Name()}, {"TERM", "dumb"}, {"SSL_CERT_FILE", GetEnv("SSL_CERT_FILE")},
    };
    TShellCommand command(BinaryPath(GetEnv("YDB_CLI_BINARY")), options);
    command << "-e" << GetEndpoint() << "-d" << GetDatabase()
        << "--oidc-issuer" << TString(idp.Issuer()) << "--oidc-client-id" << "device-client";
    command.Run();
    Y_DEFER {
        if (command.GetStatus() == TShellCommand::SHELL_RUNNING) {
            command.Terminate(SIGKILL);
        }
        command.Wait();
    };

    UNIT_ASSERT_C(idp.WaitRequests(2), "CLI did not start device authorization");
    if (signal != 0) {
        command.Terminate(signal);
    }
    const auto deadline = TInstant::Now() + TDuration::Seconds(10);
    while (command.GetStatus() == TShellCommand::SHELL_RUNNING && TInstant::Now() < deadline) {
        Sleep(TDuration::MilliSeconds(50));
    }
    UNIT_ASSERT_C(command.GetStatus() != TShellCommand::SHELL_RUNNING,
        "CLI did not stop after authorization expired or was interrupted");
    command.Wait();
    UNIT_ASSERT_VALUES_EQUAL_C(command.GetExitCode(), 1, command.GetError());
    UNIT_ASSERT(!command.GetOutput().Contains("Welcome to YDB CLI"));
    UNIT_ASSERT_VALUES_EQUAL(Service<TInteractiveQueryService>().CreatedSessions.load(), 0);
    UNIT_ASSERT_VALUES_EQUAL(Service<TInteractiveQueryService>().ExecutedQueries.load(), 0);
    return command.GetError();
}

} // namespace

Y_UNIT_TEST_SUITE(InteractiveOidcTest) {
    Y_UNIT_TEST_F(DeviceAuthorizationCanOutlastProbeTimeouts, TInteractiveOidcFixture) {
        TOidcTestServer idp;
        idp.Enqueue(TStringBuilder() << R"({"device_code":"device-code","user_code":"CODE","verification_uri":")"
            << idp.Issuer() << R"(/verify","expires_in":120,"interval":1})", HTTP_OK);
        for (size_t i = 0; i < 11; ++i) {
            idp.Enqueue(R"({"error":"authorization_pending"})", HTTP_BAD_REQUEST);
        }
        idp.Enqueue(R"({"access_token":"device-token","token_type":"Bearer","expires_in":600})", HTTP_OK);

        const auto output = RunCliWithInput({"-e", GetEndpoint(), "-d", GetDatabase(),
            "--oidc-issuer", TString(idp.Issuer()), "--oidc-client-id", "device-client"}, "quit\n");

        UNIT_ASSERT_STRING_CONTAINS(output, "mock-server-version");
        UNIT_ASSERT_STRING_CONTAINS(output, "Bye!");
        UNIT_ASSERT_VALUES_EQUAL(Service<TInteractiveQueryService>().ExecutedQueries.load(), 1);
        UNIT_ASSERT_VALUES_EQUAL(idp.Requests().size(), 13);
    }

    Y_UNIT_TEST_F(ExpiredDeviceCodeDoesNotStartProbe, TInteractiveOidcFixture) {
        UNIT_ASSERT_STRING_CONTAINS(RunUnfinishedAuthorization(0), "device authorization expired");
    }

    Y_UNIT_TEST_F(DeviceAuthorizationCanBeInterrupted, TInteractiveOidcFixture) {
        UNIT_ASSERT_STRING_CONTAINS(RunUnfinishedAuthorization(SIGINT), "OIDC sign-in interrupted.");
    }
}
