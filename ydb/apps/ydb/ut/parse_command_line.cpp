#include "run_ydb.h"

#include <ydb/public/api/grpc/ydb_discovery_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_scheme_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_auth_v1.grpc.pb.h>

#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/testing/unittest/registar.h>

#include <grpcpp/server_builder.h>

#include <util/stream/file.h>
#include <util/system/env.h>
#include <util/system/tempfile.h>

#include <fmt/format.h>

#include <thread>

using namespace fmt::literals;

const TString TEST_DATABASE = "/test_database";

class TChecker {
public:
#define CHECK_EXP(cond, err) if (!(cond)) { \
        Failures.push_back(TStringBuilder() << err); \
    }

    void AddFailure(const TString& failure) {
        Failures.push_back(failure);
    }

    void CheckExpectations() {
        if (Failures.empty()) {
            return;
        }

        TStringBuilder msg;
        for (const TString& failure : Failures) {
            msg << failure << Endl;
        }
        UNIT_FAIL(msg);
    }

    void ClearFailures() {
        Failures.clear();
    }

    std::vector<TString> Failures;
};

class TDiscoveryImpl : public Ydb::Discovery::V1::DiscoveryService::Service {
public:
    TDiscoveryImpl(const TString& address, ui16 port)
        : Address(address)
        , Port(port)
    {
    }

    grpc::Status ListEndpoints(grpc::ServerContext* context, const Ydb::Discovery::ListEndpointsRequest* request, Ydb::Discovery::ListEndpointsResponse* response) {
        Y_UNUSED(context);
        Y_UNUSED(request);
        Ydb::Discovery::ListEndpointsResult res;
        auto* endpoint = res.add_endpoints();
        endpoint->set_address(Address);
        endpoint->set_port(Port);
        auto* operation = response->mutable_operation();
        operation->set_ready(true);
        operation->set_status(Ydb::StatusIds::SUCCESS);
        operation->mutable_result()->PackFrom(res);
        return grpc::Status();
    }

private:
    TString Address;
    ui16 Port;
};

class TSchemeImpl : public Ydb::Scheme::V1::SchemeService::Service, public TChecker {
public:
    grpc::Status ListDirectory(grpc::ServerContext* context, const Ydb::Scheme::ListDirectoryRequest* request, Ydb::Scheme::ListDirectoryResponse* response) {
        Y_UNUSED(request);
        CheckClientMetadata(context, "x-ydb-database", TEST_DATABASE);
        if (Token) {
            CheckClientMetadata(context, "x-ydb-auth-ticket", Token);
        } else {
            CheckNoClientMetadata(context, "x-ydb-auth-ticket");
        }
        Ydb::Scheme::ListDirectoryResult res;
        auto* self = res.mutable_self();
        self->set_name(TEST_DATABASE);
        self->set_type(Ydb::Scheme::Entry::DATABASE);
        auto* operation = response->mutable_operation();
        operation->set_ready(true);
        operation->set_status(Ydb::StatusIds::SUCCESS);
        operation->mutable_result()->PackFrom(res);
        return grpc::Status();
    }

    void CheckClientMetadata(grpc::ServerContext* context, const TString& name, const TString& value) {
        auto [begin, end] = context->client_metadata().equal_range(name);
        CHECK_EXP(begin != end, "No " << name << " found");
        if (begin != end) {
            CHECK_EXP(begin->second == value, "Expected " << name << " \"" << value << "\". Found: \"" << begin->second << "\"");
        }
    }

    void CheckNoClientMetadata(grpc::ServerContext* context, const TString& name) {
        auto [begin, end] = context->client_metadata().equal_range(name);
        CHECK_EXP(begin == end, "Expected no " << name << ", but found");
    }

    void ExpectToken(const TString& token) {
        Token = token;
    }

    void ClearExpectations() {
        Token = {};
    }

    TString Token;
};

class TAuthImpl : public Ydb::Auth::V1::AuthService::Service, public TChecker {
public:
    grpc::Status Login(grpc::ServerContext* context, const Ydb::Auth::LoginRequest* request, Ydb::Auth::LoginResponse* response) {
        Y_UNUSED(context);
        CHECK_EXP(User, "Not expecting static credentials");
        CHECK_EXP(request->user() == User, "Expected user: \"" << User << "\", got: \"" << request->user() << "\"");
        CHECK_EXP(request->password() == Password, "Expected password: \"" << Password << "\", got: \"" << request->password() << "\"");
        Ydb::Auth::LoginResult res;
        res.set_token(Token);
        auto* operation = response->mutable_operation();
        operation->set_ready(true);
        operation->set_status(Ydb::StatusIds::SUCCESS);
        operation->mutable_result()->PackFrom(res);
        return grpc::Status();
    }

    void ExpectUserAndPassword(const TString& user, const TString& password) {
        User = user;
        Password = password;
    }

    void SetToken(const TString& token) {
        Token = token;
    }

    void ClearExpectations() {
        Token = User = Password = {};
    }

    TString User, Password;
    TString Token;
};

class TCliTestFixture : public NUnitTest::TBaseFixture {
public:
    TCliTestFixture()
        : Port(PortManager.GetPort())
        , Discovery(Address, Port)
    {
    }

    ~TCliTestFixture() {
        Shutdown();
    }

    TString GetEndpoint() const {
        return TStringBuilder() << "grpc://" << Address << ":" << Port;
    }

    TString GetDatabase() const {
        return TEST_DATABASE;
    }

    TString GetIamEndpoint() const {
        return TStringBuilder() << Address << ":" << Port;
    }

    void Start() {
        TStringBuilder address;
        address << "0.0.0.0:" << Port;
        grpc::ServerBuilder builder;
        builder.AddListeningPort(address, grpc::InsecureServerCredentials());
        builder.RegisterService(&Discovery);
        builder.RegisterService(&Scheme);
        builder.RegisterService(&Auth);
        Server = builder.BuildAndStart();
        ServerThread = std::thread([this]{
            Server->Wait();
        });
    }

    void Shutdown() {
        if (Server) {
            Server->Shutdown();
            ServerThread.join();
            Server.reset();
            ServerThread = std::thread();
        }
    }

    void SetUp(NUnitTest::TTestContext&) override {
        Start();
    }

    void TearDown(NUnitTest::TTestContext&) override {
        Shutdown();
    }

    TString EnvFile(const TString& content, const TString& fname = {}) {
        TString name = EnvFiles.emplace_back(MakeTempName(nullptr, nullptr, fname.c_str())).Name();
        TUnbufferedFileOutput(name).Write(content);
        return name;
    }

    void ExpectToken(const TString& token) {
        Scheme.ExpectToken(token);
    }

    void ExpectUserAndPassword(const TString& user, const TString& password, const TString& token) {
        Auth.ExpectUserAndPassword(user, password);
        Auth.SetToken(token);
        Scheme.ExpectToken(token);
    }

    void ExpectFail(int code = 1) {
        ExpectedExitCode = code;
    }

    void CheckExpectations() {
        Auth.CheckExpectations();
        Scheme.CheckExpectations();
    }

    void RunCli(TList<TString> args, const THashMap<TString, TString>& env = {}, const TString& profileFileContent = {}) {
        Auth.ClearFailures();
        Scheme.ClearFailures();

        if (profileFileContent) {
            TString profileFile = EnvFile(profileFileContent, "profile.yaml");
            args.emplace_front(profileFile);
            args.emplace_front("--profile-file");
        }
        RunYdb(
            args,
            {},
            true,
            false,
            env,
            ExpectedExitCode
        );
        CheckExpectations();
        // reset
        ExpectedExitCode = 0;
        Scheme.ClearExpectations();
        Auth.ClearExpectations();
    }

private:
    TPortManager PortManager;
    TString Address = "localhost";
    ui16 Port = 0;
    TDiscoveryImpl Discovery;
    TAuthImpl Auth;
    TSchemeImpl Scheme;
    std::unique_ptr<grpc::Server> Server;
    std::thread ServerThread;
    int ExpectedExitCode = 0;
    std::list<TTempFile> EnvFiles;
};

Y_UNIT_TEST_SUITE(ParseOptionsTest) {
    Y_UNIT_TEST_F(EndpointAndDatabaseFromCommandLine, TCliTestFixture) {
        RunCli(
            {
                "-v",
                "-e", GetEndpoint(),
                "-d", GetDatabase(),
                "scheme", "ls",
            }
        );
    }

    Y_UNIT_TEST_F(EndpointAndDatabaseFromActiveProfile, TCliTestFixture) {
        TString profile = fmt::format(R"yaml(
        profiles:
            test:
                endpoint: {endpoint}
                database: {database}
        active_profile: test
        )yaml",
        "endpoint"_a = GetEndpoint(),
        "database"_a = GetDatabase()
        );
        ExpectToken("42");
        RunCli(
            {
                "-v",
                "scheme", "ls",
            },
            {
                {"YDB_TOKEN", "42"}
            },
            profile
        );
    }

    Y_UNIT_TEST_F(EndpointAndDatabaseFromExplicitProfile, TCliTestFixture) {
        TString profile = fmt::format(R"yaml(
        profiles:
            test_profile:
                endpoint: {endpoint}
                database: {database}
        )yaml",
        "endpoint"_a = GetEndpoint(),
        "database"_a = GetDatabase()
        );
        ExpectToken("42");
        RunCli(
            {
                "-v",
                "-p", "test_profile",
                "scheme", "ls",
            },
            {
                {"YDB_TOKEN", "42"}
            },
            profile
        );
    }

    Y_UNIT_TEST_F(IamToken, TCliTestFixture) {
        TString profile = fmt::format(R"yaml(
        profiles:
            active_test_profile:
                endpoint: {endpoint}
                database: {database}
                authentication:
                    method: iam-token
                    data: test-iam-token
            other_test_profile:
                endpoint: {endpoint}
                database: {database}
                authentication:
                    method: iam-token
                    data: other-test-iam-token
        active_profile: active_test_profile
        )yaml",
        "endpoint"_a = GetEndpoint(),
        "database"_a = GetDatabase()
        );

        TString tokenFile = EnvFile("iam_token", "token");
        ExpectToken("iam_token");
        RunCli({
            "-v",
            "-e", GetEndpoint(),
            "-d", GetDatabase(),
            "--iam-token-file", tokenFile,
            "scheme", "ls",
        },
        {},
        profile);

        ExpectToken("iam_token");
        RunCli({
            "-v",
            "-e", GetEndpoint(),
            "-d", GetDatabase(),
            "--profile", "other_test_profile",
            "--iam-token-file", tokenFile,
            "scheme", "ls",
        },
        {},
        profile);

        ExpectToken("iam_token");
        RunCli({
            "-v",
            "-e", GetEndpoint(),
            "-d", GetDatabase(),
            "-p", "active_test_profile",
            "--iam-token-file", tokenFile,
            "scheme", "ls",
        },
        {},
        profile);

        ExpectToken("test-iam-token");
        RunCli({
            "-v",
            "scheme", "ls",
        },
        {},
        profile);

        ExpectToken("other-test-iam-token");
        RunCli({
            "-v",
            "--profile", "other_test_profile",
            "scheme", "ls",
        },
        {
            {"IAM_TOKEN", "env-iam-token"},
        },
        profile);

        ExpectToken("env-iam-token");
        RunCli({
            "-v",
            "scheme", "ls",
        },
        {
            {"IAM_TOKEN", "env-iam-token"},
        },
        profile);
    }

    Y_UNIT_TEST_F(YdbToken, TCliTestFixture) {
        TString tokenFileForProfile = EnvFile("test_token_from_file", "token");
        TString profile = fmt::format(R"yaml(
        profiles:
            active_test_profile:
                endpoint: {endpoint}
                database: {database}
                authentication:
                    method: ydb-token
                    data: test-ydb-token
            token_file_test_profile:
                endpoint: {endpoint}
                database: {database}
                authentication:
                    method: token-file
                    data: {token_file}
        active_profile: active_test_profile
        )yaml",
        "endpoint"_a = GetEndpoint(),
        "database"_a = GetDatabase(),
        "token_file"_a = tokenFileForProfile
        );

        TString tokenFile = EnvFile("test_token", "token");
        ExpectToken("test_token");
        RunCli({
            "-v",
            "-e", GetEndpoint(),
            "-d", GetDatabase(),
            "--token-file", tokenFile,
            "scheme", "ls",
        },
        {},
        profile);

        ExpectToken("test_token");
        RunCli({
            "-v",
            "-e", GetEndpoint(),
            "-d", GetDatabase(),
            "--token-file", tokenFile,
            "--profile", "token_file_test_profile",
            "scheme", "ls",
        },
        {},
        profile);

        ExpectToken("test-ydb-token");
        RunCli({
            "-v",
            "scheme", "ls",
        },
        {},
        profile);

        ExpectToken("test_token_from_file");
        RunCli({
            "-v",
            "--profile", "token_file_test_profile",
            "scheme", "ls",
        },
        {
            {"YDB_TOKEN", "env-ydb-token"},
        },
        profile);

        ExpectToken("env-ydb-token");
        RunCli({
            "-v",
            "scheme", "ls",
        },
        {
            {"YDB_TOKEN", "env-ydb-token"},
            {"YDB_USER", "not_used"},
            {"YDB_OAUTH2_KEY_FILE", "not_used"},
        },
        profile);
    }

    Y_UNIT_TEST_F(StaticCredentials, TCliTestFixture) {
        TString passwordFile = EnvFile("test-password  \n", "password");
        TString otherPasswordFile = EnvFile("pwd", "pwd");

        ExpectUserAndPassword("test-user", "test-password", "123");
        RunCli({
            "-v",
            "-e", GetEndpoint(),
            "-d", GetDatabase(),
            "--user", "test-user",
            "--password-file", passwordFile,
            "scheme", "ls",
        });

        ExpectUserAndPassword("test-user", "", "no-pwd-token");
        RunCli({
            "-v",
            "-e", GetEndpoint(),
            "-d", GetDatabase(),
            "--user", "test-user",
            "--no-password",
            "scheme", "ls",
        },
        {
            {"YDB_PASSWORD", "pwd"},
        });

        // TODO: make these options mutually exclusive
        // ExpectFail();
        // RunCli({
        //     "-v",
        //     "-e", GetEndpoint(),
        //     "-d", GetDatabase(),
        //     "--user", "test-user",
        //     "--no-password",
        //     "--password-file", passwordFile,
        //     "scheme", "ls",
        // });

        TString profile = fmt::format(R"yaml(
        profiles:
            active_test_profile:
                endpoint: {endpoint}
                database: {database}
                authentication:
                    method: static-credentials
                    data:
                        user: user-test
                        password: password-test
            test_profile:
                endpoint: {endpoint}
                database: {database}
                authentication:
                    method: static-credentials
                    data:
                        user: user_1
                        password: password_1
            test_profile_with_password_file:
                endpoint: {endpoint}
                database: {database}
                authentication:
                    method: static-credentials
                    data:
                        user: user_2
                        password-file: {password_file}
            test_profile_with_both_passwords:
                endpoint: {endpoint}
                database: {database}
                authentication:
                    method: static-credentials
                    data:
                        user: users
                        password: pwd
                        password-file: {password_file}
            test_profile_without_password:
                endpoint: {endpoint}
                database: {database}
                authentication:
                    method: static-credentials
                    data:
                        user: user_no_password
        active_profile: active_test_profile
        )yaml",
        "endpoint"_a = GetEndpoint(),
        "database"_a = GetDatabase(),
        "password_file"_a = passwordFile
        );
        ExpectFail();
        RunCli({
            "-v",
            "--password-file", otherPasswordFile,
            "scheme", "ls",
        },
        {},
        profile);

        // active profile
        ExpectUserAndPassword("user-test", "password-test", "token-test");
        RunCli({
            "-v",
            "scheme", "ls",
        },
        {},
        profile);

        // there is active profile, but we read from env
        ExpectFail();
        RunCli({
            "-v",
            "scheme", "ls",
        },
        {
            {"YDB_PASSWORD", "pwd"},
        },
        profile);

        ExpectUserAndPassword("env-user", "env-password", "env-token");
        RunCli({
            "-v",
            "scheme", "ls",
        },
        {
            {"YDB_USER", "env-user"},
            {"YDB_PASSWORD", "env-password"},
            {"YDB_OAUTH2_KEY_FILE", "not used"},
        },
        profile);

        // explicit profile
        ExpectUserAndPassword("user_1", "password_1", "token_1");
        RunCli({
            "-v",
            "-p", "test_profile",
            "scheme", "ls",
        },
        {},
        profile);

        // TODO: validate that there is no ambiguity in setting passwords
        // ExpectFail();
        // RunCli({
        //     "-v",
        //     "-p", "test_profile_with_both_passwords",
        //     "scheme", "ls",
        // },
        // {},
        // profile);

        // TODO: Fix this case
        //ExpectUserAndPassword("user_no_password", "", "no-password-token");
        RunCli({
            "-v",
            "-p", "test_profile_without_password",
            "--no-password",
            "scheme", "ls",
        },
        {},
        profile);

        // explicit profile with password file
        ExpectUserAndPassword("user_2", "test-password", "token_2");
        RunCli({
            "-v",
            "-p", "test_profile_with_password_file",
            "scheme", "ls",
        },
        {
            {"YDB_PASSWORD", "pwd"},
        },
        profile);
    }

    Y_UNIT_TEST_F(AnonymousCredentials, TCliTestFixture) {
        RunCli({
            "-v",
            "-e", GetEndpoint(),
            "-d", GetDatabase(),
            "scheme", "ls",
        });

        TString profile = fmt::format(R"yaml(
        profiles:
            active_test_profile:
                endpoint: {endpoint}
                database: {database}
                authentication:
                    method: anonymous-auth
        active_profile: active_test_profile
        )yaml",
        "endpoint"_a = GetEndpoint(),
        "database"_a = GetDatabase()
        );

        RunCli({
            "-v",
            "-e", GetEndpoint(),
            "-d", GetDatabase(),
            "scheme", "ls",
        },
        {},
        profile);

        ExpectToken("ydb-token");
        RunCli({
            "-v",
            "-e", GetEndpoint(),
            "-d", GetDatabase(),
            "scheme", "ls",
        },
        {
            {"YDB_TOKEN", "ydb-token"},
        },
        profile);

        ExpectToken("");
        RunCli({
            "-v",
            "-e", GetEndpoint(),
            "-d", GetDatabase(),
            "--profile", "active_test_profile",
            "scheme", "ls",
        },
        {
            {"YDB_TOKEN", "ydb-token"},
        },
        profile);

        ExpectUserAndPassword("user", "", "some-token");
        RunCli({
            "-v",
            "-e", GetEndpoint(),
            "-d", GetDatabase(),
            "--profile", "active_test_profile",
            "--user", "user",
            "--no-password",
            "scheme", "ls",
        },
        {
            {"YDB_TOKEN", "ydb-token"},
        },
        profile);
    }

    Y_UNIT_TEST_F(EnvPriority, TCliTestFixture) {
        ExpectToken("right-token");
        RunCli({
            "-v",
            "-e", GetEndpoint(),
            "-d", GetDatabase(),
            "scheme", "ls",
        },
        {
            {"IAM_TOKEN", "right-token"},
            {"YC_TOKEN", "wrong-token"},
            {"USE_METADATA_CREDENTIALS", "1"},
            {"SA_KEY_FILE", "wrong-file"},
            {"YDB_TOKEN", "wrong-token"},
            {"YDB_USER", "wrong-user"},
            {"YDB_PASSWORD", "wrong-password"},
        });
    }
}
