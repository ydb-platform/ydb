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

class TBridgeService : public TMockGrpcServiceBase<Ydb::Bridge::V1::BridgeService::Service> {
public:
    TString Database = TEST_DATABASE;
    bool Bridged = true;
    TString RetainPile;
    std::atomic<size_t> ModeCalls{0};
    std::atomic<size_t> JoinCalls{0};
    std::atomic<size_t> ProvisionCalls{0};
    std::atomic<size_t> DestroyCalls{0};

    grpc::Status SetDatabaseBridgeMode(grpc::ServerContext* context,
                                       const Ydb::Bridge::SetDatabaseBridgeModeRequest* request,
                                       Ydb::Bridge::SetDatabaseBridgeModeResponse* response) override {
        ++ModeCalls;
        CHECK_EXP(request->database_path() == "/Root/app", "RPC must contain the explicit target database");
        CHECK_EXP(request->has_bridge_mode() && request->bridge_mode() == Bridged, "Incorrect database bridge mode");
        CHECK_EXP(request->retain_pile_name() == RetainPile, "Incorrect pile to retain");
        return ReplySuccess(context, response);
    }

    grpc::Status UpdateClusterState(grpc::ServerContext* context,
                                    const Ydb::Bridge::UpdateClusterStateRequest* request,
                                    Ydb::Bridge::UpdateClusterStateResponse* response) override {
        ++JoinCalls;
        CHECK_EXP(request->updates_size() == 1 &&
                  request->updates(0).pile_name() == "secondary" &&
                  request->updates(0).state() == Ydb::Bridge::PileState::NOT_SYNCHRONIZED,
                  "Joining must mark the requested pile as not synchronized");
        return ReplySuccess(context, response);
    }

    grpc::Status AddPile(grpc::ServerContext* context,
                         const Ydb::Bridge::AddPileRequest* request,
                         Ydb::Bridge::AddPileResponse* response) override {
        ++ProvisionCalls;
        CHECK_EXP(request->pile_name() == "secondary", "Incorrect pile to provision");
        return ReplySuccess(context, response);
    }

    grpc::Status RemovePile(grpc::ServerContext* context,
                            const Ydb::Bridge::RemovePileRequest* request,
                            Ydb::Bridge::RemovePileResponse* response) override {
        ++DestroyCalls;
        CHECK_EXP(request->pile_name() == "secondary", "Incorrect pile to destroy");
        return ReplySuccess(context, response);
    }

private:
    template<class TResponse>
    grpc::Status ReplySuccess(grpc::ServerContext* context, TResponse* response) {
        CheckMetadata(*this, context, "x-ydb-database", Database);
        CheckMetadata(*this, context, "x-ydb-auth-ticket", "bridge-admin-token");
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
        AddService<TBridgeService>();
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
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().ModeCalls.load(), 1);
    }

    Y_UNIT_TEST_F(TargetWorksWithoutConnectionDatabase, TBridgeCliFixture) {
        ExpectLogin();
        Service<TBridgeLogin>().Database.clear();
        Service<TBridgeDiscovery>().Database.clear();
        Service<TBridgeService>().Database.clear();
        const auto password = EnvFile("password");
        RunCli({"-e", GetEndpoint(), "-y", "--user", "bridge-admin", "--password-file", password,
                "admin", "cluster", "bridge", "set-database-mode", "--target-database", "/Root/app", "--bridged"});
        UNIT_ASSERT(Service<TBridgeLogin>().Calls.load() >= 1);
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().ModeCalls.load(), 1);
    }

    Y_UNIT_TEST_F(ConnectionDatabaseDoesNotSupplyTarget, TBridgeCliFixture) {
        ExpectFail();
        RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "-y", "admin", "cluster", "bridge", "set-database-mode", "--bridged"});
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeLogin>().Calls.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeDiscovery>().Calls.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().ModeCalls.load(), 0);
    }

    Y_UNIT_TEST_F(DatabaseModeIsRequired, TBridgeCliFixture) {
        ExpectFail();
        RunCli({"-e", GetEndpoint(), "-y", "admin", "cluster", "bridge", "set-database-mode", "--target-database", "/Root/app"});
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeLogin>().Calls.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeDiscovery>().Calls.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().ModeCalls.load(), 0);
    }

    Y_UNIT_TEST_F(RetainPileRequiresOrdinaryMode, TBridgeCliFixture) {
        ExpectFail();
        RunCli({"-e", GetEndpoint(), "-y", "admin", "cluster", "bridge", "set-database-mode",
                "--target-database", "/Root/app", "--bridged", "--retain-pile", "secondary"});
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeLogin>().Calls.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeDiscovery>().Calls.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().ModeCalls.load(), 0);
    }

    Y_UNIT_TEST_F(OrdinaryModePassesRetainedPile, TBridgeCliFixture) {
        Service<TBridgeService>().Bridged = false;
        Service<TBridgeService>().RetainPile = "secondary";
        RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "-y", "admin", "cluster", "bridge", "set-database-mode",
                "--target-database", "/Root/app", "--ordinary", "--retain-pile", "secondary"},
               {{"YDB_TOKEN", "bridge-admin-token"}});
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().ModeCalls.load(), 1);
    }

    Y_UNIT_TEST_F(JoinPileSupportsLegacyRejoinAlias, TBridgeCliFixture) {
        for (const auto& command : {"join-pile", "rejoin"}) {
            RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "-y", "admin", "cluster", "bridge", command, "--pile", "secondary"},
                   {{"YDB_TOKEN", "bridge-admin-token"}});
        }
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().JoinCalls.load(), 2);
    }

    Y_UNIT_TEST_F(ProvisionPileUsesAddPileRpc, TBridgeCliFixture) {
        const auto output = RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "-y", "admin", "cluster", "bridge",
                                    "provision-pile", "--pile", "secondary"},
                                   {{"YDB_TOKEN", "bridge-admin-token"}});
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().ProvisionCalls.load(), 1);
        UNIT_ASSERT_STRING_CONTAINS(output, "Pile provisioned successfully");
    }

    Y_UNIT_TEST_F(DestroyPileRequiresConfirmation, TBridgeCliFixture) {
        ExpectFail();
        RunCliWithInput({"-e", GetEndpoint(), "-d", GetDatabase(), "admin", "cluster", "bridge", "destroy-pile", "--pile", "secondary"},
                        "n\n",
                        {{"YDB_TOKEN", "bridge-admin-token"}});
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().DestroyCalls.load(), 0);
        const auto output = RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "-y", "admin", "cluster", "bridge",
                                    "destroy-pile", "--pile", "secondary"},
                                   {{"YDB_TOKEN", "bridge-admin-token"}});
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().DestroyCalls.load(), 1);
        UNIT_ASSERT_STRING_CONTAINS(output, "Pile destroyed successfully");
    }
}
