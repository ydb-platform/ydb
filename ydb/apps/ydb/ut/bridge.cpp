#include "mock_env.h"

#include <ydb/public/api/grpc/draft/ydb_bridge_v1.grpc.pb.h>

#include <util/string/cast.h>

#include <atomic>

namespace {

void CheckMetadata(TChecker& checker, grpc::ServerContext* context, const TString& name, const TString& expected) {
    const auto entry = context->client_metadata().find(name);
    if (expected.empty()) {
        if (entry != context->client_metadata().end()) {
            checker.AddFailure("Unexpected " + name + " metadata");
        }
    } else if (entry == context->client_metadata().end() || entry->second != expected) {
        checker.AddFailure("Incorrect " + name + " metadata");
    }
}

class TBridgeLogin : public TAuthImpl {
public:
    TString Database = TEST_DATABASE;
    std::atomic<size_t> Calls{0};

    grpc::Status Login(grpc::ServerContext* context, const Ydb::Auth::LoginRequest* request, Ydb::Auth::LoginResponse* response) override {
        ++Calls;
        CheckMetadata(*this, context, "x-ydb-database", Database);
        return TAuthImpl::Login(context, request, response);
    }
};

class TBridgeDiscovery : public TDiscoveryImpl {
public:
    explicit TBridgeDiscovery(const TString& endpoint)
        : TDiscoveryImpl("localhost", FromString<ui16>(TStringBuf(endpoint).RNextTok(':')))
    {}

    TString Database = TEST_DATABASE;
    std::atomic<size_t> Calls{0};

    grpc::Status ListEndpoints(grpc::ServerContext* context,
                               const Ydb::Discovery::ListEndpointsRequest* request,
                               Ydb::Discovery::ListEndpointsResponse* response) override {
        ++Calls;
        CHECK_EXP(request->database() == Database, "Discovery must use the connection database");
        CheckMetadata(*this, context, "x-ydb-database", Database);
        return TDiscoveryImpl::ListEndpoints(context, request, response);
    }
};

class TBridgeMode : public TMockGrpcServiceBase<Ydb::Bridge::V1::BridgeService::Service> {
public:
    TString Database = TEST_DATABASE;
    std::atomic<size_t> Calls{0};

    grpc::Status SetDatabaseBridgeMode(grpc::ServerContext* context,
                                       const Ydb::Bridge::SetDatabaseBridgeModeRequest* request,
                                       Ydb::Bridge::SetDatabaseBridgeModeResponse* response) override {
        ++Calls;
        CheckMetadata(*this, context, "x-ydb-database", Database);
        CheckMetadata(*this, context, "x-ydb-auth-ticket", "bridge-admin-token");
        CHECK_EXP(request->database_path() == "/Root/app", "RPC must contain the explicit target database");
        CHECK_EXP(request->has_bridge_mode() && request->bridge_mode(), "Expected bridged storage mode");
        auto* operation = response->mutable_operation();
        operation->set_ready(true);
        operation->set_status(Ydb::StatusIds::SUCCESS);
        return grpc::Status::OK;
    }
};

class TBridgeCliFixture : public TCliTestFixture {
public:
    void AddServices() override {
        AddService<TBridgeDiscovery>(GetAddress());
        AddService<TBridgeLogin>();
        AddService<TBridgeMode>();
    }

    void ExpectLogin() {
        Service<TBridgeLogin>().ExpectUserAndPassword("bridge-admin", "password");
        Service<TBridgeLogin>().SetToken("bridge-admin-token");
    }
};

} // namespace

Y_UNIT_TEST_SUITE(BridgeCli) {
    Y_UNIT_TEST_F(TargetDoesNotChangeLoginOrDiscoveryDatabase, TBridgeCliFixture) {
        ExpectLogin();
        const auto password = EnvFile("password");
        RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "-y", "--user", "bridge-admin", "--password-file", password,
                "admin", "cluster", "bridge", "set-database-mode", "--target-database", "/Root/app", "--bridged"});
        UNIT_ASSERT(Service<TBridgeLogin>().Calls.load() >= 1);
        UNIT_ASSERT(Service<TBridgeDiscovery>().Calls.load() >= 1);
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeMode>().Calls.load(), 1);
    }

    Y_UNIT_TEST_F(TargetWorksWithoutConnectionDatabase, TBridgeCliFixture) {
        ExpectLogin();
        Service<TBridgeLogin>().Database.clear();
        Service<TBridgeDiscovery>().Database.clear();
        Service<TBridgeMode>().Database.clear();
        const auto password = EnvFile("password");
        RunCli({"-e", GetEndpoint(), "-y", "--user", "bridge-admin", "--password-file", password,
                "admin", "cluster", "bridge", "set-database-mode", "--target-database", "/Root/app", "--bridged"});
        UNIT_ASSERT(Service<TBridgeLogin>().Calls.load() >= 1);
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeMode>().Calls.load(), 1);
    }

    Y_UNIT_TEST_F(ConnectionDatabaseDoesNotSupplyTarget, TBridgeCliFixture) {
        ExpectFail();
        RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "-y", "admin", "cluster", "bridge", "set-database-mode", "--bridged"});
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeLogin>().Calls.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeDiscovery>().Calls.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeMode>().Calls.load(), 0);
    }
}
