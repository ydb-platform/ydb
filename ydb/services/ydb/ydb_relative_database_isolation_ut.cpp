#include "ydb_common_ut.h"

#include <ydb/core/cms/console/console.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/path_aliasing/path_normalizer.h>
#include <ydb/library/testlib/helpers.h>
#include <ydb/public/api/grpc/ydb_table_v1.grpc.pb.h>
#include <ydb/public/api/protos/ydb_cms.pb.h>

#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

#include <chrono>
#include <memory>
#include <tuple>

namespace NKikimr::NGRpcService {
namespace {

using TTable = Ydb::Table::V1::TableService::Stub;

NKikimrConfig::TAppConfig IsolationConfig(bool enableRelativePaths) {
    NKikimrConfig::TAppConfig config;
    config.MutableFeatureFlags()->SetEnableRelativePaths(enableRelativePaths);
    // The reload targets the same proxy that serves this fixture's gRPC requests.
    config.MutableGRpcConfig()->SetGRpcProxyCount(1);
    auto* security = config.MutableDomainsConfig()->MutableSecurityConfig();
    security->SetEnforceUserTokenRequirement(true);
    security->AddRegisterDynamicNodeAllowedSIDs("root@builtin");
    auto* rule = config.MutableResourcePathPrefixMapping()->AddRules();
    rule->SetSrc("/Root/logical");
    rule->SetDst("/Root/allowed");
    return config;
}

template <class TRequest, class TResponse>
TResponse Call(TTable& stub,
               grpc::Status (TTable::*method)(grpc::ClientContext*, const TRequest&, TResponse*),
               TRequest request, TStringBuf database, TStringBuf token)
{
    grpc::ClientContext context;
    context.AddMetadata("x-ydb-database", TString(database));
    context.AddMetadata("x-ydb-auth-ticket", TString(token));
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(30));
    request.mutable_operation_params()->set_operation_mode(Ydb::Operations::OperationParams::SYNC);
    TResponse response;
    const auto status = (stub.*method)(&context, request, &response);
    UNIT_ASSERT_C(status.ok(), status.error_message());
    UNIT_ASSERT_C(response.operation().ready(), response.DebugString());
    return response;
}

template <class TResult, class TResponse>
TResult Result(const TResponse& response) {
    UNIT_ASSERT_VALUES_EQUAL_C(response.operation().status(), Ydb::StatusIds::SUCCESS, response.DebugString());
    TResult result;
    UNIT_ASSERT_C(response.operation().result().UnpackTo(&result), response.DebugString());
    return result;
}

struct TFixture {
    NYdb::TKikimrWithGrpcAndRootSchemaWithAuth Server;
    std::unique_ptr<TTable> Table;

    explicit TFixture(bool enableRelativePaths)
        : Server(IsolationConfig(enableRelativePaths), {}, {}, false, nullptr,
            [](Tests::TServerSettings& settings) {
                settings.StoragePoolTypes.clear();
                settings.AddStoragePool("hdd");
                settings.StoragePoolTypes.at("hdd").SetStoragePoolId(0);
            })
        , Table(Ydb::Table::V1::TableService::NewStub(grpc::CreateChannel(
            TStringBuilder() << "localhost:" << Server.GetPort(), grpc::InsecureChannelCredentials())))
    {
        for (const auto& [name, token, key] : {
                std::tuple{"allowed", "alice@builtin", ui64(1)},
                std::tuple{"other", "bob@builtin", ui64(2)}}) {
            const TString database = TStringBuilder() << "/Root/" << name;
            Ydb::Cms::CreateDatabaseRequest create;
            create.set_path(database);
            auto* storage = create.mutable_resources()->add_storage_units();
            storage->set_unit_kind("hdd");
            storage->set_count(1);
            Server.Tenants_->CreateTenant(std::move(create));
            for (const auto node : Server.Tenants_->List(database)) {
                Server.GetServer().EnableGRpc(Server.GetPortManager().GetPort(), node, database);
            }
            const TString session = Open(database, "root@builtin");
            Ydb::Table::ExecuteSchemeQueryRequest scheme;
            scheme.set_session_id(session);
            scheme.set_yql_text(TStringBuilder() << "GRANT ALL ON `" << database << "` TO `" << token
                << "`; CREATE TABLE marker (Key Uint64 NOT NULL, PRIMARY KEY (Key));");
            const auto created = Call(*Table, &TTable::ExecuteSchemeQuery, scheme, database, "root@builtin");
            UNIT_ASSERT_VALUES_EQUAL_C(created.operation().status(), Ydb::StatusIds::SUCCESS, created.DebugString());
            const TString query = TStringBuilder() << "UPSERT INTO marker (Key) VALUES (" << key << "u);";
            Result<Ydb::Table::ExecuteQueryResult>(Query(session, database, "root@builtin", query));
            Close(session, database, "root@builtin");
        }
        // Provisioning helpers use tokenless local RPCs. Restrict administration
        // only after provisioning, before any alice/bob request can be made.
        auto& runtime = *Server.GetRuntime();
        for (ui32 node = 0; node < runtime.GetNodeCount(); ++node) {
            runtime.GetAppData(node).AdministrationAllowedSIDs = {"root@builtin"};
        }
    }

    TString Open(TStringBuf database, TStringBuf token) {
        return Result<Ydb::Table::CreateSessionResult>(
            Call(*Table, &TTable::CreateSession, Ydb::Table::CreateSessionRequest{}, database, token)).session_id();
    }

    void Close(TStringBuf session, TStringBuf database, TStringBuf token) {
        Ydb::Table::DeleteSessionRequest request;
        request.set_session_id(TString(session));
        const auto response = Call(*Table, &TTable::DeleteSession, request, database, token);
        UNIT_ASSERT_VALUES_EQUAL_C(response.operation().status(), Ydb::StatusIds::SUCCESS, response.DebugString());
    }

    Ydb::Table::ExecuteDataQueryResponse Query(
            TStringBuf session, TStringBuf database, TStringBuf token, TStringBuf sql)
    {
        Ydb::Table::ExecuteDataQueryRequest request;
        request.set_session_id(TString(session));
        request.mutable_query()->set_yql_text(TString(sql));
        request.mutable_tx_control()->mutable_begin_tx()->mutable_serializable_read_write();
        request.mutable_tx_control()->set_commit_tx(true);
        return Call(*Table, &TTable::ExecuteDataQuery, request, database, token);
    }

    void CheckAccess(TStringBuf database, TStringBuf token, TStringBuf ownTable, TStringBuf otherTable, ui64 key) {
        const TString session = Open(database, token);
        for (const TString& table : {TString("marker"), TString(ownTable)}) {
            const auto result = Result<Ydb::Table::ExecuteQueryResult>(Query(session, database, token,
                TStringBuilder() << "SELECT Key FROM `" << table << "`;"));
            UNIT_ASSERT_VALUES_EQUAL(result.result_sets_size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(result.result_sets(0).rows_size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(result.result_sets(0).rows(0).items(0).uint64_value(), key);
        }
        const auto denied = Query(session, database, token,
            TStringBuilder() << "SELECT Key FROM `" << otherTable << "`;");
        // Queries must respect the selected database boundary, independently of connect ACLs.
        UNIT_ASSERT_C(denied.operation().status() == Ydb::StatusIds::UNAUTHORIZED
            || denied.operation().status() == Ydb::StatusIds::SCHEME_ERROR, denied.DebugString());
        Close(session, database, token);
    }

    void CheckConnectDenied(TStringBuf database, TStringBuf token) {
        const auto denied = Call(*Table, &TTable::CreateSession, Ydb::Table::CreateSessionRequest{}, database, token);
        UNIT_ASSERT_VALUES_EQUAL_C(denied.operation().status(), Ydb::StatusIds::UNAUTHORIZED, denied.DebugString());
    }

    void RedirectAliasToOtherDatabase() {
        NKikimrConfig::TPathRewriteConfig rules;
        auto* rule = rules.AddRules();
        rule->SetSrc("/Root/logical");
        rule->SetDst("/Root/other");
        auto update = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationRequest>();
        update->PathNormalizer = std::make_shared<const NPathAliasing::TPathNormalizer>(rules);
        *update->Record.MutableConfig()->MutableTableServiceConfig() =
            Server.ServerSettings->AppConfig->GetTableServiceConfig();
        auto& runtime = *Server.GetRuntime();
        const auto sender = runtime.AllocateEdgeActor();
        runtime.Send(new IEventHandle(CreateGRpcRequestProxyId(), sender, update.Release()));
        // The acknowledgment is sent after the proxy has installed the new snapshot.
        auto response = runtime.GrabEdgeEvent<NConsole::TEvConsole::TEvConfigNotificationResponse>(
            sender, TDuration::Seconds(30));
        UNIT_ASSERT(response);
    }
};

} // namespace

Y_UNIT_TEST_SUITE(YdbRelativeDatabaseIsolation) {
    Y_UNIT_TEST_TWIN(DatabaseSpellingsPreserveAccessIsolation, enableRelativePaths) {
        TFixture fixture(enableRelativePaths);
        TVector<TString> allowed = {"/Root/allowed", "/Root/logical"};
        TVector<TString> other = {"/Root/other"};
        if (enableRelativePaths) {
            allowed.insert(allowed.end(), {"allowed", "logical"});
            other.push_back("other");
        } else {
            allowed.push_back("Root/allowed");
            other.push_back("Root/other");
        }
        for (const auto& database : allowed) {
            fixture.CheckAccess(database, "alice@builtin", "/Root/allowed/marker", "/Root/other/marker", 1);
            fixture.CheckConnectDenied(database, "bob@builtin");
        }
        for (const auto& database : other) {
            fixture.CheckAccess(database, "bob@builtin", "/Root/other/marker", "/Root/allowed/marker", 2);
            fixture.CheckConnectDenied(database, "alice@builtin");
        }
    }

    Y_UNIT_TEST_TWIN(AliasReloadChecksTheNewDatabasePermissions, enableRelativePaths) {
        TFixture fixture(enableRelativePaths);
        const TString logical = enableRelativePaths ? "logical" : "/Root/logical";
        fixture.CheckAccess(logical, "alice@builtin", "/Root/allowed/marker", "/Root/other/marker", 1);
        fixture.CheckConnectDenied(logical, "bob@builtin");
        const TString oldSession = fixture.Open(logical, "alice@builtin");

        fixture.RedirectAliasToOtherDatabase();

        fixture.CheckConnectDenied(logical, "alice@builtin");
        fixture.CheckAccess(logical, "bob@builtin", "/Root/other/marker", "/Root/allowed/marker", 2);
        fixture.CheckAccess("/Root/allowed", "alice@builtin", "/Root/allowed/marker", "/Root/other/marker", 1);
        const auto oldResult = Result<Ydb::Table::ExecuteQueryResult>(
            fixture.Query(oldSession, "/Root/allowed", "alice@builtin", "SELECT Key FROM marker;"));
        UNIT_ASSERT_VALUES_EQUAL(oldResult.result_sets(0).rows(0).items(0).uint64_value(), 1);
        fixture.Close(oldSession, "/Root/allowed", "alice@builtin");
    }
}

} // namespace NKikimr::NGRpcService
