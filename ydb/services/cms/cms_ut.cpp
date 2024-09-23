#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/testing/unittest/registar.h>

#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>

#include <ydb/core/testlib/test_client.h>
#include <ydb/core/protos/console_config.pb.h>
#include <ydb/core/protos/console_base.pb.h>
#include <ydb/core/protos/console.pb.h>
#include <ydb/core/protos/cms.pb.h>
#include <ydb/core/protos/console_tenant.pb.h>

#include <ydb/library/aclib/aclib.h>

#include <ydb/public/api/grpc/ydb_cms_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_operation_v1.grpc.pb.h>

#include <ydb/library/grpc/client/grpc_client_low.h>

#include <google/protobuf/any.h>

#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>

// new grpc client
#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/ydb_params/params.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>
#include <ydb/public/sdk/cpp/client/resources/ydb_resources.h>

#include <ydb/services/ydb/ydb_common_ut.h>

namespace NKikimr {

using namespace Tests;
using namespace NYdb;

struct TCmsTestSettings : TKikimrTestSettings {
    static constexpr bool PrecreatePools = false;
};

struct TCmsTestSettingsWithAuth : TCmsTestSettings {
    static constexpr bool AUTH = true;
};

using TKikimrWithGrpcAndRootSchema = NYdb::TBasicKikimrWithGrpcAndRootSchema<TCmsTestSettings>;

static Ydb::StatusIds::StatusCode WaitForOperationStatus(std::shared_ptr<grpc::Channel> channel, const TString& opId, const TString &token = "") {
    std::unique_ptr<Ydb::Operation::V1::OperationService::Stub> stub;
    stub = Ydb::Operation::V1::OperationService::NewStub(channel);
    Ydb::Operations::GetOperationRequest request;
    request.set_id(opId);
    Ydb::Operations::GetOperationResponse response;
    bool run = true;
    while (run) {
        grpc::ClientContext context;
        if (token)
            context.AddMetadata(YDB_AUTH_TICKET_HEADER, token);
        auto status = stub->GetOperation(&context, request, &response);
        UNIT_ASSERT(status.ok());  //GRpc layer - OK
        if (response.operation().ready() == false) {
            Sleep(ITERATION_DURATION);
        } else {
            run = false;
        }
    }
    return response.operation().status();
}

static Ydb::Cms::GetDatabaseStatusResult WaitForTenantState(std::shared_ptr<grpc::Channel> channel, const TString& path, Ydb::Cms::GetDatabaseStatusResult::State state = Ydb::Cms::GetDatabaseStatusResult::RUNNING, const TString &token = "") {
    std::unique_ptr<Ydb::Cms::V1::CmsService::Stub> stub;
    stub = Ydb::Cms::V1::CmsService::NewStub(channel);
    Ydb::Cms::GetDatabaseStatusRequest request;
    request.set_path(path);
    Ydb::Cms::GetDatabaseStatusResponse response;
    while (true) {
        grpc::ClientContext context;
        if (token)
            context.AddMetadata(YDB_AUTH_TICKET_HEADER, token);

        auto status = stub->GetDatabaseStatus(&context, request, &response);
        UNIT_ASSERT(status.ok());

        UNIT_ASSERT(response.operation().ready());
        UNIT_ASSERT(response.operation().status() == Ydb::StatusIds::SUCCESS);

        Ydb::Cms::GetDatabaseStatusResult result;
        auto res = response.operation().result().UnpackTo(&result);
        UNIT_ASSERT(res);
        UNIT_ASSERT_VALUES_EQUAL(result.path(), path);
        if (result.state() == state) {
            return result;
        }
    }
}

void InitConsoleConfig(TKikimrWithGrpcAndRootSchema &server)
{
    TClient client(*server.ServerSettings);
    TAutoPtr<NMsgBusProxy::TBusConsoleRequest> request(new NMsgBusProxy::TBusConsoleRequest());
    auto &config = *request->Record.MutableSetConfigRequest()->MutableConfig()->MutableTenantsConfig();
    {
        auto &zone = *config.AddAvailabilityZoneKinds();
        zone.SetKind("dc-1");
        zone.SetDataCenterName("DC-1");
    }
    {
        auto &zone = *config.AddAvailabilityZoneKinds();
        zone.SetKind("any");
    }
    {
        auto &zoneSet = *config.AddAvailabilityZoneSets();
        zoneSet.SetName("all");
        zoneSet.AddZoneKinds("dc-1");
        zoneSet.AddZoneKinds("any");
    }
    {
        auto &kind = *config.AddComputationalUnitKinds();
        kind.SetKind("slot");
        kind.SetTenantSlotType("default");
        kind.SetAvailabilityZoneSet("all");
    }
    TAutoPtr<NBus::TBusMessage> reply;
    NBus::EMessageStatus msgStatus = client.SyncCall(request, reply);
    UNIT_ASSERT_VALUES_EQUAL(msgStatus, NBus::MESSAGE_OK);
    auto resp = dynamic_cast<NMsgBusProxy::TBusConsoleResponse*>(reply.Get())->Record;
    UNIT_ASSERT_VALUES_EQUAL(resp.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
}

template<typename TRequest>
static void SetSyncOperation(TRequest& req) {
    req.mutable_operation_params()->set_operation_mode(Ydb::Operations::OperationParams::SYNC);
}

static void doSimpleTenantsTest(bool sync) {
    TKikimrWithGrpcAndRootSchema server;

    server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::CMS_TENANTS, NLog::PRI_TRACE);

    ui16 grpc = server.GetPort();
    TString id;

    std::shared_ptr<grpc::Channel> channel;
    std::unique_ptr<Ydb::Cms::V1::CmsService::Stub> stub;
    channel = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());

    const TString tenant = "/Root/users/user-1";
    // create tenant
    {
        stub = Ydb::Cms::V1::CmsService::NewStub(channel);
        grpc::ClientContext context;

        Ydb::Cms::CreateDatabaseRequest request;
        request.set_path(tenant);
        if (sync)
            SetSyncOperation(request);
        auto unit = request.mutable_resources()->add_storage_units();
        unit->set_unit_kind("hdd");
        unit->set_count(1);

        Ydb::Cms::CreateDatabaseResponse response;
        auto status = stub->CreateDatabase(&context, request, &response);
        UNIT_ASSERT(status.ok());
        // ready flag true in sync mode
        UNIT_ASSERT(response.operation().ready() == sync);
        if (sync) {
            UNIT_ASSERT_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
        } else {
            id = response.operation().id();
        }
    }

    if (!sync) {
        auto status = WaitForOperationStatus(channel, id);
        UNIT_ASSERT_EQUAL(status, Ydb::StatusIds::SUCCESS);
    }

    {
        server.Tenants_->Run(tenant);
        WaitForTenantState(channel, tenant, Ydb::Cms::GetDatabaseStatusResult::RUNNING);
    }

    // alter tenant
    {
        stub = Ydb::Cms::V1::CmsService::NewStub(channel);
        grpc::ClientContext context;

        Ydb::Cms::AlterDatabaseRequest request;
        request.set_path(tenant);

        auto unit = request.add_storage_units_to_add();
        unit->set_unit_kind("hdd");
        unit->set_count(1);

        Ydb::Cms::AlterDatabaseResponse response;
        auto status = stub->AlterDatabase(&context, request, &response);
        UNIT_ASSERT(status.ok());
        UNIT_ASSERT(response.operation().ready());
        UNIT_ASSERT_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
    }

    // get tenant status
    {
        Ydb::Cms::GetDatabaseStatusResult result = WaitForTenantState(channel, tenant, Ydb::Cms::GetDatabaseStatusResult::RUNNING);
        UNIT_ASSERT_VALUES_EQUAL(result.path(), tenant);
        UNIT_ASSERT_VALUES_EQUAL(result.state(), Ydb::Cms::GetDatabaseStatusResult::RUNNING);
        UNIT_ASSERT_VALUES_EQUAL(result.required_resources().storage_units_size(), 1);
        auto unit = result.required_resources().storage_units(0);
        UNIT_ASSERT_VALUES_EQUAL(unit.unit_kind(), "hdd");
        UNIT_ASSERT_VALUES_EQUAL(unit.count(), 2);
    }

    // list tenants
    {
        stub = Ydb::Cms::V1::CmsService::NewStub(channel);
        grpc::ClientContext context;

        Ydb::Cms::ListDatabasesRequest request;

        Ydb::Cms::ListDatabasesResponse response;
        auto status = stub->ListDatabases(&context, request, &response);
        UNIT_ASSERT(status.ok());
        UNIT_ASSERT(response.operation().ready());
        UNIT_ASSERT(response.operation().status() == Ydb::StatusIds::SUCCESS);
        Ydb::Cms::ListDatabasesResult result;
        auto res = response.operation().result().UnpackTo(&result);
        UNIT_ASSERT(res);
        UNIT_ASSERT_VALUES_EQUAL(result.paths_size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(result.paths(0), tenant);
    }

    // remove tenants
    {
        stub = Ydb::Cms::V1::CmsService::NewStub(channel);
        grpc::ClientContext context;

        Ydb::Cms::RemoveDatabaseRequest request;
        request.set_path("/Root/users/user-1");
        if (sync)
            SetSyncOperation(request);

        Ydb::Cms::RemoveDatabaseResponse response;
        auto status = stub->RemoveDatabase(&context, request, &response);
        UNIT_ASSERT(status.ok());
        UNIT_ASSERT(response.operation().ready() == sync);
        if (sync) {
            UNIT_ASSERT(response.operation().status() == Ydb::StatusIds::SUCCESS);
        } else {
            id = response.operation().id();
        }
    }
    if (!sync) {
        auto status = WaitForOperationStatus(channel, id);
        UNIT_ASSERT(status == Ydb::StatusIds::SUCCESS);
    }

    // get tenant status
    {
        stub = Ydb::Cms::V1::CmsService::NewStub(channel);
        grpc::ClientContext context;

        Ydb::Cms::GetDatabaseStatusRequest request;
        request.set_path("/Root/users/user-1");

        Ydb::Cms::GetDatabaseStatusResponse response;
        auto status = stub->GetDatabaseStatus(&context, request, &response);
        UNIT_ASSERT(status.ok());
        UNIT_ASSERT(response.operation().ready());
        UNIT_ASSERT(response.operation().status() == Ydb::StatusIds::NOT_FOUND);
    }

    // list tenants
    {
        stub = Ydb::Cms::V1::CmsService::NewStub(channel);
        grpc::ClientContext context;

        Ydb::Cms::ListDatabasesRequest request;

        Ydb::Cms::ListDatabasesResponse response;
        auto status = stub->ListDatabases(&context, request, &response);
        UNIT_ASSERT(status.ok());
        UNIT_ASSERT(response.operation().ready());
        UNIT_ASSERT(response.operation().status() == Ydb::StatusIds::SUCCESS);
        Ydb::Cms::ListDatabasesResult result;
        auto res = response.operation().result().UnpackTo(&result);
        UNIT_ASSERT(res);
        UNIT_ASSERT_VALUES_EQUAL(result.paths_size(), 0);
    }
}

template <typename TTestSettings>
void CheckCreateDatabase(NYdb::TBasicKikimrWithGrpcAndRootSchema<TTestSettings> &server,
                         std::shared_ptr<grpc::Channel> channel,
                         const TString &path,
                         bool disableTx = false,
                         bool runNode = false,
                         const TString &token = "")
{
    std::unique_ptr<Ydb::Cms::V1::CmsService::Stub> stub;
    TString id;

    stub = Ydb::Cms::V1::CmsService::NewStub(channel);
    grpc::ClientContext context;
    if (token)
        context.AddMetadata(YDB_AUTH_TICKET_HEADER, token);

    {
        Ydb::Cms::CreateDatabaseRequest request;
        request.set_path(path);
        if (disableTx)
            request.mutable_options()->set_disable_tx_service(true);
        auto unit = request.mutable_resources()->add_storage_units();
        unit->set_unit_kind("hdd");
        unit->set_count(1);

        Ydb::Cms::CreateDatabaseResponse response;
        auto status = stub->CreateDatabase(&context, request, &response);
        UNIT_ASSERT(status.ok());
        UNIT_ASSERT(!response.operation().ready());
        id = response.operation().id();
    }
    {
        auto status = WaitForOperationStatus(channel, id, token);
        UNIT_ASSERT_VALUES_EQUAL(status, Ydb::StatusIds::SUCCESS);
    }

    if (runNode) {
        //WaitForTenantState(channel, path, Ydb::Cms::GetDatabaseStatusResult::PENDING_RESOURCES, token);
        server.Tenants_->Run(path);
        WaitForTenantState(channel, path, Ydb::Cms::GetDatabaseStatusResult::RUNNING, token);
    }
}

void CheckRemoveDatabase(std::shared_ptr<grpc::Channel> channel,
                         const TString &path,
                         const TString &token = "",
                         Ydb::StatusIds::StatusCode code = Ydb::StatusIds::SUCCESS)
{
    std::unique_ptr<Ydb::Cms::V1::CmsService::Stub> stub;
    TString id;

    stub = Ydb::Cms::V1::CmsService::NewStub(channel);
    grpc::ClientContext context;
    if (token)
        context.AddMetadata(YDB_AUTH_TICKET_HEADER, token);

    {
        Ydb::Cms::RemoveDatabaseRequest request;
        request.set_path(path);

        Ydb::Cms::RemoveDatabaseResponse response;
        auto status = stub->RemoveDatabase(&context, request, &response);
        UNIT_ASSERT(status.ok());
        UNIT_ASSERT(!response.operation().ready());
        id = response.operation().id();
    }

    {
        auto status = WaitForOperationStatus(channel, id);
        UNIT_ASSERT(status == code);
    }
}

Y_UNIT_TEST_SUITE(TGRpcCmsTest) {
    Y_UNIT_TEST(SimpleTenantsTest) {
        doSimpleTenantsTest(false);
    }

    Y_UNIT_TEST(SimpleTenantsTestSyncOperation) {
        doSimpleTenantsTest(true);
    }

    Y_UNIT_TEST(AuthTokenTest) {
        NYdb::TBasicKikimrWithGrpcAndRootSchema<TCmsTestSettingsWithAuth> server;
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::CMS_TENANTS, NLog::PRI_TRACE);
        ui16 grpc = server.GetPort();
        TString id;

        std::shared_ptr<grpc::Channel> channel;
        std::unique_ptr<Ydb::Cms::V1::CmsService::Stub> stub;
        channel = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());

        // create tenant
        CheckCreateDatabase(server, channel, "/Root/users/user-1", false, true, BUILTIN_ACL_ROOT);

        // Check owner
        {
            TClient client(*server.ServerSettings);
            auto resp = client.Ls("/Root/users/user-1");
            UNIT_ASSERT_VALUES_EQUAL(resp->Record.GetPathDescription().GetSelf().GetOwner(), BUILTIN_ACL_ROOT);
        }
    }

    Y_UNIT_TEST(DescribeOptionsTest) {
        TKikimrWithGrpcAndRootSchema server;
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::CMS_TENANTS, NLog::PRI_TRACE);
        ui16 grpc = server.GetPort();
        TString id;

        // Configure Console.
        InitConsoleConfig(server);

        std::shared_ptr<grpc::Channel> channel;
        std::unique_ptr<Ydb::Cms::V1::CmsService::Stub> stub;
        channel = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());

        stub = Ydb::Cms::V1::CmsService::NewStub(channel);
        grpc::ClientContext context;

        Ydb::Cms::DescribeDatabaseOptionsRequest request;
        Ydb::Cms::DescribeDatabaseOptionsResponse response;
        auto status = stub->DescribeDatabaseOptions(&context, request, &response);
        UNIT_ASSERT(status.ok());
        UNIT_ASSERT(response.operation().ready());
        UNIT_ASSERT(response.operation().status() == Ydb::StatusIds::SUCCESS);

        // In testing env we have pools kinds test, ssd, hdd, hdd1, hdd2 all with
        // none erasure and ROT disk.
        THashSet<TString> poolKinds = {{TString("test"), TString("ssd"), TString("hdd"), TString("hdd1"), TString("hdd2")}};
        Ydb::Cms::DescribeDatabaseOptionsResult result;
        auto res = response.operation().result().UnpackTo(&result);
        UNIT_ASSERT(res);
        UNIT_ASSERT_VALUES_EQUAL(result.storage_units_size(), poolKinds.size());
        for (auto &pool : result.storage_units()) {
            UNIT_ASSERT(poolKinds.contains(pool.kind()));
            UNIT_ASSERT_VALUES_EQUAL(pool.labels().size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(pool.labels().at("disk_type"), "ROT");
            UNIT_ASSERT_VALUES_EQUAL(pool.labels().at("erasure"), "none");
            poolKinds.erase(pool.kind());
        }

        // Check availability zone dc-1 and any.
        THashSet<TString> zones = {{TString("dc-1"), TString("any")}};
        UNIT_ASSERT_VALUES_EQUAL(result.availability_zones_size(), zones.size());
        for (auto &zone: result.availability_zones()) {
            UNIT_ASSERT(zones.contains(zone.name()));
            if (zone.name() == "dc-1") {
                UNIT_ASSERT_VALUES_EQUAL(zone.labels().size(), 2);
                UNIT_ASSERT_VALUES_EQUAL(zone.labels().at("fixed_data_center"), "DC-1");
                UNIT_ASSERT_VALUES_EQUAL(zone.labels().at("collocation"), "disabled");
            } else if (zone.name() == "any") {
                UNIT_ASSERT_VALUES_EQUAL(zone.labels().size(), 2);
                UNIT_ASSERT_VALUES_EQUAL(zone.labels().at("any_data_center"), "true");
                UNIT_ASSERT_VALUES_EQUAL(zone.labels().at("collocation"), "disabled");
            } else {
                Y_ABORT("unexpected zone");
            }
            zones.erase(zone.name());
        }

        // Check computational unit.
        THashSet<TString> unitZones = {{TString("dc-1"), TString("any")}};
        UNIT_ASSERT_VALUES_EQUAL(result.computational_units_size(), 1);
        auto &unit = result.computational_units(0);
        UNIT_ASSERT_VALUES_EQUAL(unit.kind(), "slot");
        UNIT_ASSERT_VALUES_EQUAL(unit.allowed_availability_zones_size(), unitZones.size());
        for (auto &zone : unit.allowed_availability_zones()) {
            UNIT_ASSERT(unitZones.contains(zone));
            unitZones.erase(zone);
        }
        UNIT_ASSERT_VALUES_EQUAL(unit.labels().size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(unit.labels().at("type"), "dynamic_slot");
        UNIT_ASSERT_VALUES_EQUAL(unit.labels().at("slot_type"), "default");
    }

    Y_UNIT_TEST(DisabledTxTest) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();
        TString id;

        std::shared_ptr<grpc::Channel> channel;
        std::unique_ptr<Ydb::Cms::V1::CmsService::Stub> stub;
        channel = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());

        // create tenant with no computational resources
        // it should succeed if we don't need transactions support
        CheckCreateDatabase(server, channel, "/Root/users/user-1", true);
        WaitForTenantState(channel, "/Root/users/user-1", Ydb::Cms::GetDatabaseStatusResult::RUNNING);
    }

    Y_UNIT_TEST(AlterRemoveTest) {
        TKikimrWithGrpcAndRootSchema server;
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::CMS_TENANTS, NLog::PRI_TRACE);

        ui16 grpc = server.GetPort();
        TString id;

        std::shared_ptr<grpc::Channel> channel;
        std::unique_ptr<Ydb::Cms::V1::CmsService::Stub> stub;
        channel = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());

        const TString tenant = "/Root/users/user-1";
        CheckCreateDatabase(server, channel, tenant);

        // alter tenant
        {
            stub = Ydb::Cms::V1::CmsService::NewStub(channel);
            grpc::ClientContext context;

            Ydb::Cms::AlterDatabaseRequest request;
            request.set_path(tenant);
            auto unit = request.add_storage_units_to_add();
            unit->set_unit_kind("hdd");
            unit->set_count(1);

            Ydb::Cms::AlterDatabaseResponse response;
            auto status = stub->AlterDatabase(&context, request, &response);
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT(response.operation().ready());
            UNIT_ASSERT_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
        }

        // remove tenants
        CheckRemoveDatabase(channel, "/Root/users/user-1");
    }

    Y_UNIT_TEST(RemoveWithAnotherTokenTest) {
        TKikimrWithGrpcAndRootSchema server;
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::CMS_TENANTS, NLog::PRI_TRACE);
        ui16 grpc = server.GetPort();
        TString id;

        std::shared_ptr<grpc::Channel> channel;
        std::unique_ptr<Ydb::Cms::V1::CmsService::Stub> stub;
        channel = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());

        {
            NACLib::TDiffACL acl;
            acl.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericFull,
                          "user-1@" BUILTIN_ACL_DOMAIN);
            TClient client(*server.ServerSettings);
            client.ModifyACL("/", "Root", acl.SerializeAsString());
        }

        CheckCreateDatabase(server, channel, "/Root/users/user-1",
                            false, true, "user-1@" BUILTIN_ACL_DOMAIN);

        CheckRemoveDatabase(channel, "/Root/users/user-1",
                            "user-2@" BUILTIN_ACL_DOMAIN,
                            Ydb::StatusIds::UNAUTHORIZED);

        {
            NACLib::TDiffACL acl;
            acl.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericFull,
                          "user-2@" BUILTIN_ACL_DOMAIN);
            TClient client(*server.ServerSettings);
            client.ModifyACL("/Root/users", "user-1", acl.SerializeAsString());
            client.RefreshPathCache(server.Server_->GetRuntime(), "/Root/users/user-1");
        }

        CheckRemoveDatabase(channel, "/Root/users/user-1",
                            "user-2@" BUILTIN_ACL_DOMAIN);
    }
}

} // namespace NKikimr
