#pragma once
#include "run_ydb.h"

#include <ydb/core/security/certificate_check/test_utils/test_cert_auth_utils.h>
#include <ydb/public/api/client/yc_public/iam/iam_token_service.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_discovery_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_scheme_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_auth_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/tests/unit/client/oauth2_token_exchange/helpers/test_token_exchange_server.h>

#include <library/cpp/json/json_writer.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/testing/unittest/registar.h>

#include <grpcpp/generic/async_generic_service.h>
#include <grpcpp/server_builder.h>

#include <util/stream/file.h>
#include <util/generic/hash.h>
#include <util/system/env.h>
#include <util/folder/tempdir.h>
#include <util/system/tempfile.h>

#include <fmt/format.h>

#include <filesystem>
#include <memory>
#include <thread>
#include <typeindex>
#include <typeinfo>
#include <unordered_map>

extern const TString TEST_DATABASE;

class TChecker {
public:
#define CHECK_EXP(cond, err) if (!(cond)) { \
        Failures.push_back(TStringBuilder() << err); \
    }

    virtual ~TChecker() = default;

    void AddFailure(const TString& failure) {
        Failures.push_back(failure);
    }

    virtual void CheckExpectations() {
        if (Failures.empty()) {
            return;
        }

        TStringBuilder msg;
        for (const TString& failure : Failures) {
            msg << failure << Endl;
        }
        UNIT_FAIL(msg);
    }

    virtual void ClearFailures() {
        Failures.clear();
    }

    virtual void ClearExpectations() {
    }

    std::vector<TString> Failures;
};

class TMockGrpcService : public TChecker {
public:
    virtual grpc::Service* Service() = 0;
};

template <class TServiceImpl>
class TMockGrpcServiceBase : public TServiceImpl, public TMockGrpcService {
    grpc::Service* Service() override {
        return static_cast<TServiceImpl*>(this);
    }
};

class TDiscoveryImpl : public TMockGrpcServiceBase<Ydb::Discovery::V1::DiscoveryService::Service> {
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

class TSchemeImpl : public TMockGrpcServiceBase<Ydb::Scheme::V1::SchemeService::Service> {
public:
    struct TDirectory {
        Ydb::Scheme::Entry::Type SelfType = Ydb::Scheme::Entry::DIRECTORY;
        std::vector<std::pair<TString, Ydb::Scheme::Entry::Type>> Children;
    };

    grpc::Status ListDirectory(grpc::ServerContext* context, const Ydb::Scheme::ListDirectoryRequest* request, Ydb::Scheme::ListDirectoryResponse* response) {
        CheckClientMetadata(context, "x-ydb-database", TEST_DATABASE);
        if (Token) {
            CheckClientMetadata(context, "x-ydb-auth-ticket", Token);
        } else {
            CheckNoClientMetadata(context, "x-ydb-auth-ticket");
        }
        CheckAuthContext(context);
        Ydb::Scheme::ListDirectoryResult res;
        auto directory = Directories.find(request->path());
        if (directory == Directories.end()) {
            CHECK_EXP(Directories.empty(), "Unexpected ListDirectory path: \"" << request->path() << "\"");
            auto* self = res.mutable_self();
            self->set_name(TEST_DATABASE);
            self->set_type(Ydb::Scheme::Entry::DATABASE);
        } else {
            auto* self = res.mutable_self();
            self->set_name(TStringBuf(request->path()).RNextTok('/'));
            self->set_type(directory->second.SelfType);
            for (const auto& [name, type] : directory->second.Children) {
                auto* child = res.add_children();
                child->set_name(name);
                child->set_type(type);
            }
        }
        auto* operation = response->mutable_operation();
        operation->set_ready(true);
        operation->set_status(Ydb::StatusIds::SUCCESS);
        operation->mutable_result()->PackFrom(res);
        return grpc::Status();
    }

    TSchemeImpl& ExpectListDirectory(TString path, Ydb::Scheme::Entry::Type selfType = Ydb::Scheme::Entry::DIRECTORY) {
        Directories[std::move(path)].SelfType = selfType;
        return *this;
    }

    TSchemeImpl& ExpectChild(TString path, TString name, Ydb::Scheme::Entry::Type type) {
        auto& directory = Directories[std::move(path)];
        directory.Children.emplace_back(std::move(name), type);
        return *this;
    }

    void CheckClientMetadata(grpc::ServerContext* context, const TString& name, const TString& value) {
        auto [begin, end] = context->client_metadata().equal_range(name);
        CHECK_EXP(begin != end, "No " << name << " found");
        if (begin != end) {
            CHECK_EXP(begin->second == value, "Expected " << name << " \"" << value << "\". Found: \"" << begin->second << "\"");
        }
    }

    void CheckAuthContext(grpc::ServerContext* context) {
        std::shared_ptr<const grpc::AuthContext> authContext = context->auth_context();
        auto certs = authContext->FindPropertyValues("x509_pem_cert");
        if (ExpectedClientCert) {
            CHECK_EXP(!certs.empty(), "Expected client certificate, but not found");
            TString cert = certs.front().data();
            CHECK_EXP(cert == ExpectedClientCert, "Expected client certificate \"" << ExpectedClientCert << "\", but found: \"" << cert << "\"");
        } else {
            CHECK_EXP(certs.empty(), "Expected no client certificate, but found " << certs.size());
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
        ExpectedClientCert = Token = {};
        Directories.clear();
    }

    void ExpectClientCert(const TString& cert) {
        ExpectedClientCert = cert;
    }

    TString Token;
    TString ExpectedClientCert;
    THashMap<TString, TDirectory> Directories;
};

class TAuthImpl : public TMockGrpcServiceBase<Ydb::Auth::V1::AuthService::Service> {
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

// Special generic service that fails test if there is an unimplemented service call
class TFailingGenericService : public grpc::CallbackGenericService {
public:
    grpc::ServerGenericBidiReactor* CreateReactor(grpc::GenericCallbackServerContext* ctx) override {
        UNIT_FAIL("Called unimplemented gRPC method: " << ctx->method());

        class Reactor : public grpc::ServerGenericBidiReactor {
        public:
            Reactor() {
                this->Finish(grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "Unimplemented method"));
            }

            void OnDone() override {
                delete this;
            }
        };

        return new Reactor();
    }
};

class TIamTokenServiceImpl : public TMockGrpcServiceBase<yandex::cloud::iam::v1::IamTokenService::Service> {
public:
    grpc::Status Create(grpc::ServerContext* context, const yandex::cloud::iam::v1::CreateIamTokenRequest* request, yandex::cloud::iam::v1::CreateIamTokenResponse* response) override {
        Y_UNUSED(context);
        Y_UNUSED(request);
        // We don't test here the quality of credentials provider itself,
        // but test that YDB CLI correctly chooses the credentials provider
        ++Calls;
        response->set_iam_token(Token);
        return grpc::Status();
    }

    void SetToken(const TString& token) {
        Token = token;
    }

    void ExpectCall() {
        ++ExpectedCalls;
    }

    void CheckExpectations() override {
        CHECK_EXP(Calls == ExpectedCalls, "Expected " << ExpectedCalls << " calls, but got " << Calls);
        TChecker::CheckExpectations();
    }

    void ClearExpectations() override {
        Calls = ExpectedCalls = 0;
    }

    size_t Calls = 0;
    size_t ExpectedCalls = 0;
    TString Token;
};

class TCliTestFixture : public NUnitTest::TBaseFixture {
public:
    TCliTestFixture()
        : Port(PortManager.GetPort())
    {
    }

    virtual ~TCliTestFixture() {
        Shutdown();
    }

    TString GetEndpoint() const {
        return TStringBuilder() << ConnectSchema << "://" << Address << ":" << Port;
    }

    TString GetAddress() const {
        return TStringBuilder() << Address << ":" << Port;
    }

    TString GetDatabase() const {
        return TEST_DATABASE;
    }

    TString GetIamEndpoint() const {
        return TStringBuilder() << Address << ":" << Port;
    }

    void GenerateCerts() {
        if (RootCAFile) {
            return;
        }

        using namespace NKikimr::NCertTestUtils;
        const TCertAndKey ca = GenerateCA(TProps::AsCA());
        const TCertAndKey serverCert = GenerateSignedCert(ca, TProps::AsServer());
        const TCertAndKey clientCert = GenerateSignedCert(ca, TProps::AsClient());
        RootCA = ca.Certificate;
        RootCAFile = EnvFile(RootCA, "root_ca.pem");
        ServerCert = serverCert.Certificate;
        ServerCertFile = EnvFile(ServerCert, "server_cert.pem");
        ServerKey = serverCert.PrivateKey;
        ServerKeyFile = EnvFile(ServerKey, "server_key.pem");
        ClientCert = clientCert.Certificate;
        ClientCertFile = EnvFile(ClientCert, "client_cert.pem");
        ClientKey = clientCert.PrivateKey;
        ClientKeyFile = EnvFile(ClientKey, "client_key.pem");
        ClientKeyPassword = "test_password";
        ClientKeyPasswordFile = EnvFile(ClientKeyPassword, "client_key_password.txt");
        ClientKeyWithPassword = clientCert.GetKeyWithPassword(ClientKeyPassword);
        ClientKeyWithPasswordFile = EnvFile(ClientKeyWithPassword, "client_key_with_password.pem");

        const TCertAndKey wrongCa = GenerateCA(TProps::AsCA());
        WrongRootCA = wrongCa.Certificate;
        WrongRootCAFile = EnvFile(WrongRootCA, "root_ca.pem");
    }

#define DECL_CERT_GETTER(name) \
    TString name;              \
    TString Get##name() {      \
        GenerateCerts();       \
        return name;           \
    }

    DECL_CERT_GETTER(RootCAFile);
    DECL_CERT_GETTER(RootCA);
    DECL_CERT_GETTER(WrongRootCAFile);
    DECL_CERT_GETTER(WrongRootCA);
    DECL_CERT_GETTER(ServerCertFile);
    DECL_CERT_GETTER(ServerCert);
    DECL_CERT_GETTER(ServerKeyFile);
    DECL_CERT_GETTER(ServerKey);
    DECL_CERT_GETTER(ClientCertFile);
    DECL_CERT_GETTER(ClientCert);
    DECL_CERT_GETTER(ClientKeyFile);
    DECL_CERT_GETTER(ClientKey);
    DECL_CERT_GETTER(ClientKeyWithPasswordFile);
    DECL_CERT_GETTER(ClientKeyWithPassword);
    DECL_CERT_GETTER(ClientKeyPasswordFile);
    DECL_CERT_GETTER(ClientKeyPassword);

    void MakeSslServerCredentials() {
        ConnectSchema = "grpcs";
        grpc::SslServerCredentialsOptions sslOps;
        sslOps.pem_root_certs = GetRootCA();
        auto& certPair = sslOps.pem_key_cert_pairs.emplace_back();
        certPair.private_key = GetServerKey();
        certPair.cert_chain = GetServerCert();
        sslOps.client_certificate_request = GRPC_SSL_REQUEST_CLIENT_CERTIFICATE_AND_VERIFY;
        ServerCredentials = grpc::SslServerCredentials(sslOps);
    }

    virtual void AddServices() {
        AddService<TDiscoveryImpl>(Address, Port);
        AddService<TSchemeImpl>();
        AddService<TAuthImpl>();
        AddService<TIamTokenServiceImpl>();
    }

    void Start() {
        TStringBuilder address;
        address << "0.0.0.0:" << Port;
        grpc::ServerBuilder builder;
        if (!ServerCredentials) {
            ServerCredentials = grpc::InsecureServerCredentials();
        }
        builder.AddListeningPort(address, ServerCredentials);
        AddServices();
        FailingGenericService = std::make_unique<TFailingGenericService>();
        builder.RegisterCallbackGenericService(FailingGenericService.get());
        for (auto&& [t, srv] : Services) {
            builder.RegisterService(srv->Service());
        }
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

    TString EnvHomeFile(const std::string& relativePath, const TString& content) {
        if (!TempHomeDir) {
            TempHomeDir = std::make_unique<TTempDir>();
        }

        std::filesystem::path path(std::string(TempHomeDir->Name()));
        path /= relativePath;
        std::filesystem::create_directories(path.parent_path());
        TUnbufferedFileOutput(path).Write(content);
        return std::string(path);
    }

    void ExpectToken(const TString& token) {
        Service<TSchemeImpl>().ExpectToken(token);
    }

    void ExpectClientCert() {
        Service<TSchemeImpl>().ExpectClientCert(GetClientCert());
    }

    void ExpectUserAndPassword(const TString& user, const TString& password) {
        Service<TAuthImpl>().ExpectUserAndPassword(user, password);
        const TString token = password + "-token";
        Service<TAuthImpl>().SetToken(token);
        Service<TSchemeImpl>().ExpectToken(token);
    }

    void ExpectFail(int code = 1) {
        ExpectedExitCode = code;
    }

    void CheckExpectations() {
        for (auto&& [t, srv] : Services) {
            srv->CheckExpectations();
        }
    }

    void ClearFailures() {
        for (auto&& [t, srv] : Services) {
            srv->ClearFailures();
        }
    }

    void ClearExpectations() {
        ExpectedExitCode = 0;
        for (auto&& [t, srv] : Services) {
            srv->ClearExpectations();
        }
    }

    THashMap<TString, TString> GetEndEnv(const THashMap<TString, TString>& env) {
        THashMap<TString, TString> copy = env;
        if (TempHomeDir) {
            copy["HOME"] = TempHomeDir->Name();
        }
        return copy;
    }

    TString RunCli(TList<TString> args, const THashMap<TString, TString>& env = {}, const TString& profileFileContent = {}) {
        ClearFailures();

        if (profileFileContent) {
            TString profileFile = EnvFile(profileFileContent, "profile.yaml");
            args.emplace_front(profileFile);
            args.emplace_front("--profile-file");
        }
        TString output = RunYdb(
            args,
            {},
            true,
            false,
            GetEndEnv(env),
            ExpectedExitCode
        );
        CheckExpectations();
        // reset
        ClearExpectations();
        return output;
    }

    TString RunCliWithInput(TList<TString> args, const TString& input, const THashMap<TString, TString>& env = {}, const TString& profileFileContent = {}) {
        ClearFailures();

        if (profileFileContent) {
            TString profileFile = EnvFile(profileFileContent, "profile.yaml");
            args.emplace_front(profileFile);
            args.emplace_front("--profile-file");
        }
        TString output = RunYdbWithInput(
            args,
            {},
            input,
            true,
            false,
            GetEndEnv(env),
            ExpectedExitCode
        );
        CheckExpectations();
        // reset
        ClearExpectations();
        return output;
    }

    //
    // Services
    //

    // Add service: must be called from inside RegisterServices
    // TService must be derived from TMockGrpcService
    template <class TService, class... TArgs>
    std::shared_ptr<TService> AddService(TArgs&&... args) {
        std::shared_ptr<TService> service = std::make_shared<TService>(std::forward<TArgs>(args)...);
        auto [iter, insertedNew] = Services.emplace(std::type_index(typeid(TService)), service);
        UNIT_ASSERT_C(insertedNew, "Duplicate service: " << typeid(TService).name());
        return service;
    }

    // Get service ref by its type
    // Is used to setup expectations
    template <class TService>
    TService& Service() {
        auto serviceIter = Services.find(std::type_index(typeid(TService)));
        UNIT_ASSERT_C(serviceIter != Services.end(), "Service not found: " << typeid(TService).name());
        return static_cast<TService&>(*serviceIter->second);
    }

protected:
    TPortManager& GetPortManager() {
        return PortManager;
    }

private:
    TPortManager PortManager;
    TString ConnectSchema = "grpc";
    TString Address = "localhost";
    ui16 Port = 0;
    std::unordered_map<std::type_index, std::shared_ptr<TMockGrpcService>> Services;
    std::unique_ptr<grpc::Server> Server;
    std::thread ServerThread;
    int ExpectedExitCode = 0;
    std::list<TTempFile> EnvFiles;
    std::unique_ptr<TTempDir> TempHomeDir;

    std::shared_ptr<grpc::ServerCredentials> ServerCredentials;
    std::unique_ptr<TFailingGenericService> FailingGenericService;
};

class TCliTestFixtureWithSsl : public TCliTestFixture {
public:
    TCliTestFixtureWithSsl() = default;

    void SetUp(NUnitTest::TTestContext&) override {
        MakeSslServerCredentials();
        Start();
    }
};
