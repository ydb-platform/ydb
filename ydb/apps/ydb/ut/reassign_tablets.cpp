#include "mock_env.h"

#include <ydb/public/api/grpc/draft/ydb_tablet_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_operation_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_tablet.h>

#include <library/cpp/json/json_reader.h>

#include <atomic>

namespace {

const TString OperationId = "ydb://reassigntablets/17?hive_id=72075186224037888&hive_generation=3&reassign_id=123";
const TString CanonicalOperationId = NKikimr::NOperationId::TOperationId(OperationId).ToString();
const TString AdminToken = "reassign-admin-token";

void CheckMetadata(TChecker& checker, grpc::ServerContext* context) {
    for (const auto& [name, value] : {std::pair<TString, TString>{"x-ydb-database", TEST_DATABASE},
                                    {"x-ydb-auth-ticket", AdminToken}}) {
        const auto entry = context->client_metadata().find(name);
        if (entry == context->client_metadata().end() || entry->second != value) {
            checker.AddFailure("Incorrect " + name + " metadata");
        }
    }
}

void SetOperation(Ydb::Operations::Operation& operation, bool ready = false) {
    operation.set_id(OperationId);
    operation.set_ready(ready);
    if (ready) {
        operation.set_status(Ydb::StatusIds::SUCCESS);
    }
    Ydb::Tablet::ReassignTabletsMetadata metadata;
    metadata.set_selected_tablets(12);
    metadata.set_completed_tablets(ready ? 12 : 3);
    operation.mutable_metadata()->PackFrom(metadata);
}

class TTabletService : public TMockGrpcServiceBase<Ydb::Tablet::V1::TabletService::Service> {
public:
    Ydb::Tablet::ReassignTabletsRequest Expected;
    std::atomic<size_t> Calls{0};
    Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::SUCCESS;
    grpc::Status RpcStatus;

    grpc::Status ReassignTablets(grpc::ServerContext* context, const Ydb::Tablet::ReassignTabletsRequest* request,
            Ydb::Tablet::ReassignTabletsResponse* response) override {
        ++Calls;
        CheckMetadata(*this, context);
        CHECK_EXP(request->SerializeAsString() == Expected.SerializeAsString(), "Unexpected reassignment request: " << request->DebugString());
        if (Status == Ydb::StatusIds::SUCCESS) {
            SetOperation(*response->mutable_operation());
        } else {
            auto& operation = *response->mutable_operation();
            operation.set_ready(true);
            operation.set_status(Status);
            operation.add_issues()->set_message("Reassignment rejected");
        }
        return RpcStatus;
    }
};

class TOperationService : public TMockGrpcServiceBase<Ydb::Operation::V1::OperationService::Service> {
public:
    std::atomic<size_t> GetCalls{0};
    std::atomic<size_t> CancelCalls{0};
    bool Ready = false;
    Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::SUCCESS;

    grpc::Status GetOperation(grpc::ServerContext* context, const Ydb::Operations::GetOperationRequest* request,
            Ydb::Operations::GetOperationResponse* response) override {
        ++GetCalls;
        CheckMetadata(*this, context);
        CHECK_EXP(request->id() == CanonicalOperationId, "Incorrect operation ID: " << request->id());
        auto& operation = *response->mutable_operation();
        SetOperation(operation, Ready);
        if (Status != Ydb::StatusIds::SUCCESS) {
            operation.set_ready(true);
            operation.set_status(Status);
            operation.add_issues()->set_message("Reassignment operation expired");
        }
        return grpc::Status::OK;
    }

    grpc::Status CancelOperation(grpc::ServerContext* context, const Ydb::Operations::CancelOperationRequest* request,
            Ydb::Operations::CancelOperationResponse* response) override {
        ++CancelCalls;
        CheckMetadata(*this, context);
        CHECK_EXP(request->id() == CanonicalOperationId, "Incorrect operation ID: " << request->id());
        response->set_status(Ydb::StatusIds::SUCCESS);
        return grpc::Status::OK;
    }
};

class TReassignTabletsFixture : public TCliTestFixture {
public:
    void AddServices() override {
        EnvHomeFile("ydb/bin/config.json", R"({"check_version":false})");
        AddService<TDiscoveryImpl>("localhost", FromString<ui16>(TStringBuf(GetAddress()).RNextTok(':')));
        auto tablet = AddService<TTabletService>();
        tablet->Expected.set_storage_pool("test-pool");
        tablet->Expected.set_max_in_flight(1);
        tablet->Expected.set_percent(100);
        AddService<TOperationService>();
    }

    TString Run(TList<TString> command, TString* error = nullptr) {
        TList<TString> args = {"-e", GetEndpoint(), "-d", GetDatabase(), "-y"};
        args.insert(args.end(), command.begin(), command.end());
        return RunCli(std::move(args), {{"YDB_TOKEN", AdminToken}}, {}, error);
    }

    TString Reassign(TList<TString> options, TString* error = nullptr) {
        TList<TString> command = {"admin", "database", "reassign-tablets"};
        command.insert(command.end(), options.begin(), options.end());
        return Run(std::move(command), error);
    }
};

} // namespace

Y_UNIT_TEST_SUITE(ReassignTabletsCli) {
    Y_UNIT_TEST_F(StartReturnsOperationWithoutPolling, TReassignTabletsFixture) {
        const auto output = Reassign({"--storage-pool", "test-pool"});
        UNIT_ASSERT_STRING_CONTAINS(output, CanonicalOperationId);
        UNIT_ASSERT_STRING_CONTAINS(output, "selected tablets");
        UNIT_ASSERT_STRING_CONTAINS(output, "completed tablets");
        UNIT_ASSERT_STRING_CONTAINS(output, "false");
        UNIT_ASSERT_STRING_CONTAINS(output, "12");
        UNIT_ASSERT_VALUES_EQUAL(Service<TTabletService>().Calls.load(), 1);
        UNIT_ASSERT_VALUES_EQUAL(Service<TOperationService>().GetCalls.load(), 0);
    }

    Y_UNIT_TEST_F(AllOptionsAndJsonOutput, TReassignTabletsFixture) {
        auto& expected = Service<TTabletService>().Expected;
        expected.set_group(123);
        expected.set_max_in_flight(4);
        expected.set_percent(25);
        expected.set_async(true);
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(Reassign({"--storage-pool", "test-pool", "--group", "123",
            "--max-in-flight", "4", "--percent", "25", "--async", "--format", "proto-json-base64"}), &json));
        UNIT_ASSERT_VALUES_EQUAL(json["id"].GetString(), OperationId);
        UNIT_ASSERT(!json["ready"].GetBoolean());
        UNIT_ASSERT_VALUES_EQUAL(json["metadata"]["selectedTablets"].GetString(), "12");
        UNIT_ASSERT_VALUES_EQUAL(json["metadata"]["completedTablets"].GetString(), "3");
        UNIT_ASSERT_VALUES_EQUAL(Service<TTabletService>().Calls.load(), 1);
        UNIT_ASSERT_VALUES_EQUAL(Service<TOperationService>().GetCalls.load(), 0);
    }

    Y_UNIT_TEST_F(GroupOnlyAndZeroPercent, TReassignTabletsFixture) {
        auto& expected = Service<TTabletService>().Expected;
        expected.clear_storage_pool();
        expected.set_group(123);
        expected.set_percent(0);
        Reassign({"--group", "123", "--percent", "0"});
        UNIT_ASSERT_VALUES_EQUAL(Service<TTabletService>().Calls.load(), 1);
    }

    Y_UNIT_TEST_F(SdkPreservesAbsentOptions, TReassignTabletsFixture) {
        auto& expected = Service<TTabletService>().Expected;
        expected.clear_max_in_flight();
        expected.clear_percent();
        NYdb::TDriver driver(NYdb::TDriverConfig().SetEndpoint(GetAddress()).SetDatabase(GetDatabase()).SetAuthToken(AdminToken));
        NYdb::NTablet::TTabletClient client(driver);
        const auto operation = client.ReassignTablets(NYdb::NTablet::TReassignTabletsSettings().StoragePool("test-pool")).GetValueSync();
        UNIT_ASSERT(!operation.Ready());
        UNIT_ASSERT_VALUES_EQUAL(operation.Id().ToString(), CanonicalOperationId);
        UNIT_ASSERT_VALUES_EQUAL(operation.Metadata().SelectedTablets, 12);
        UNIT_ASSERT_VALUES_EQUAL(operation.Metadata().CompletedTablets, 3);
        CheckExpectations();
        UNIT_ASSERT_VALUES_EQUAL(Service<TTabletService>().Calls.load(), 1);
    }

    Y_UNIT_TEST_F(InvalidOptionsDoNotSendRequests, TReassignTabletsFixture) {
        const TVector<TList<TString>> options = {
            {}, {"--group", "0"}, {"--storage-pool", ""},
            {"--storage-pool", "test-pool", "--max-in-flight", "0"},
            {"--storage-pool", "test-pool", "--percent", "101"},
            {"--storage-pool", "test-pool", "--percent", "-1"},
            {"--group", "4294967296"},
            {"--storage-pool", "test-pool", "unexpected"},
        };
        for (const auto& option : options) {
            ExpectFail();
            TString error;
            Reassign(option, &error);
            UNIT_ASSERT(!error.empty());
        }
        UNIT_ASSERT_VALUES_EQUAL(Service<TTabletService>().Calls.load(), 0);
    }

    Y_UNIT_TEST_F(StartErrorsAreReported, TReassignTabletsFixture) {
        auto& service = Service<TTabletService>();
        service.Status = Ydb::StatusIds::UNAUTHORIZED;
        ExpectFail();
        TString error;
        Reassign({"--storage-pool", "test-pool"}, &error);
        UNIT_ASSERT_STRING_CONTAINS(error, "Reassignment rejected");
        service.Status = Ydb::StatusIds::SUCCESS;
        service.RpcStatus = grpc::Status(grpc::StatusCode::UNAVAILABLE, "Tablet service unavailable");
        ExpectFail();
        Reassign({"--storage-pool", "test-pool"}, &error);
        UNIT_ASSERT_STRING_CONTAINS(error, "Tablet service unavailable");
        UNIT_ASSERT_VALUES_EQUAL(service.Calls.load(), 2);
    }

    Y_UNIT_TEST_F(GetProgressCompletionAndExpiration, TReassignTabletsFixture) {
        const auto output = Run({"operation", "get", OperationId});
        UNIT_ASSERT_STRING_CONTAINS(output, CanonicalOperationId);
        UNIT_ASSERT_STRING_CONTAINS(output, "completed tablets");
        UNIT_ASSERT_STRING_CONTAINS(output, "false");
        auto& service = Service<TOperationService>();
        service.Ready = true;
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(Run({"operation", "get", OperationId, "--format", "proto-json-base64"}), &json));
        UNIT_ASSERT(json["ready"].GetBoolean());
        UNIT_ASSERT_VALUES_EQUAL(json["status"].GetString(), "SUCCESS");
        UNIT_ASSERT_VALUES_EQUAL(json["metadata"]["completedTablets"].GetString(), "12");
        service.Status = Ydb::StatusIds::NOT_FOUND;
        ExpectFail();
        TString error;
        Run({"operation", "get", OperationId}, &error);
        UNIT_ASSERT_STRING_CONTAINS(error, "Reassignment operation expired");
        UNIT_ASSERT_VALUES_EQUAL(service.GetCalls.load(), 3);
    }

    Y_UNIT_TEST_F(CancelUsesOperationId, TReassignTabletsFixture) {
        Run({"operation", "cancel", OperationId});
        UNIT_ASSERT_VALUES_EQUAL(Service<TOperationService>().CancelCalls.load(), 1);
    }
}
