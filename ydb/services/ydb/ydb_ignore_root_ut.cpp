#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/public/api/grpc/ydb_discovery_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_operation_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_query_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_scheme_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_scripting_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_table_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/discovery/discovery.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>

#include <atomic>
#include <chrono>

namespace NKikimr::NGRpcService {
namespace {

using TDiscovery = Ydb::Discovery::V1::DiscoveryService::Stub;
using TTable = Ydb::Table::V1::TableService::Stub;
using TQuery = Ydb::Query::V1::QueryService::Stub;
using TScripting = Ydb::Scripting::V1::ScriptingService::Stub;
using TOperation = Ydb::Operation::V1::OperationService::Stub;
using TScheme = Ydb::Scheme::V1::SchemeService::Stub;

template <typename TResponse>
Ydb::StatusIds::StatusCode StatusCode(const TResponse& response) {
    if constexpr (requires { response.operation(); }) {
        return response.operation().status();
    } else {
        return response.status();
    }
}

template <typename TResult, typename TResponse>
TResult Unpack(const TResponse& response) {
    TResult result;
    UNIT_ASSERT_C(response.operation().result().UnpackTo(&result), response.DebugString());
    return result;
}

class TEnvironment {
public:
    const TString Root;
    const TString Name;
    const TString OldDatabase;
    const TString Database;
    TString Token;
    bool AbsoluteSqlPaths = false;
    NKqp::TKikimrRunner Runner;
    std::unique_ptr<TDiscovery> Discovery;
    std::unique_ptr<TTable> Table;
    std::unique_ptr<TQuery> Query;
    std::unique_ptr<TScripting> Scripting;
    std::unique_ptr<TOperation> Operation;
    std::shared_ptr<grpc::Channel> Channel;

    static NKqp::TKikimrSettings Settings(const TString& root, bool ignoreRoot, bool authenticated, bool useRealThreads) {
        NKqp::TKikimrSettings settings;
        settings.SetDomainRoot(root).SetWithSampleTables(false)
            .SetDynamicNodeCount(2).SetStoragePoolTypes({"ssd"}).SetUseRealThreads(useRealThreads);
        settings.SetEnableScriptExecutionOperations(true);
        settings.AppConfig.MutableGRpcConfig()->SetIgnoreRoot(ignoreRoot);
        settings.FeatureFlags.SetCheckDatabaseAccessPermission(true);
        if (authenticated) {
            settings.SetAuthToken("root@builtin");
        }
        return settings;
    }

    TEnvironment(bool singleComponent, bool ignoreRoot, bool authenticated = true,
        bool nested = false, bool useRealThreads = true)
        : Root(singleComponent ? "root" : "backup")
        , Name(nested ? "team/mydb123" : singleComponent ? "kfront" : "mydb123")
        , OldDatabase(nested ? "/ru/team/mydb123" : singleComponent ? "/kfront" : "/ru/mydb123")
        , Database("/" + Root + "/" + Name)
        , Token(authenticated ? "root@builtin" : "")
        , Runner(Settings(Root, ignoreRoot, authenticated, useRealThreads))
    {
        if (authenticated) {
            // The runner grants the initial user's permissions before restricting
            // administrators. Apply that restriction to tenant nodes as well.
            auto& runtime = *Runner.GetTestServer().GetRuntime();
            for (ui32 node = 1; node < runtime.GetNodeCount(); ++node) {
                runtime.GetAppData(node).AdministrationAllowedSIDs.push_back(Token);
            }
        }
        if (nested) {
            auto status = Runner.RunCall([&] {
                return Runner.GetSchemeClient().MakeDirectory("/" + Root + "/team").GetValueSync();
            });
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        }
        UNIT_ASSERT_VALUES_EQUAL(Runner.CreateDatabase(Name, "ssd", {}), Database);
        Discovery = Ydb::Discovery::V1::DiscoveryService::NewStub(
            grpc::CreateChannel(Runner.GetEndpoint(), grpc::InsecureChannelCredentials()));
        Ydb::Discovery::ListEndpointsRequest request;
        request.set_database(Database);
        auto result = Unpack<Ydb::Discovery::ListEndpointsResult>(
            Call(*Discovery, &TDiscovery::ListEndpoints, request, Database));
        UNIT_ASSERT_C(result.endpoints_size() > 0, result.DebugString());
        const auto& endpoint = result.endpoints(0);
        const TString address = TStringBuilder() << endpoint.address() << ":" << endpoint.port();
        Channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
        Table = Ydb::Table::V1::TableService::NewStub(Channel);
        Query = Ydb::Query::V1::QueryService::NewStub(Channel);
        Scripting = Ydb::Scripting::V1::ScriptingService::NewStub(Channel);
        Operation = Ydb::Operation::V1::OperationService::NewStub(Channel);

        auto scheme = Ydb::Scheme::V1::SchemeService::NewStub(Channel);
        Ydb::Scheme::MakeDirectoryRequest directory;
        directory.set_path(Database + "/dir");
        Call(*scheme, &TScheme::MakeDirectory, directory, Database);
        auto session = TableSession();
        Ydb::Table::ExecuteSchemeQueryRequest create;
        create.set_session_id(session);
        create.set_yql_text("CREATE TABLE `dir/data` (Key Uint64 NOT NULL, Value Utf8 NOT NULL, PRIMARY KEY(Key));");
        Call(*Table, &TTable::ExecuteSchemeQuery, create, Database);
        SetValue("target");
    }

    void Configure(grpc::ClientContext& context, const TString& database) const {
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(30));
        if (database) {
            context.AddMetadata("x-ydb-database", database);
        }
        if (Token) {
            context.AddMetadata("x-ydb-auth-ticket", Token);
        }
    }

    template <typename TStub, typename TRequest, typename TResponse>
    TResponse Call(TStub& stub,
        grpc::Status (TStub::*method)(grpc::ClientContext*, const TRequest&, TResponse*),
        TRequest request, const TString& database, bool success = true,
        Ydb::StatusIds::StatusCode expectedStatus = Ydb::StatusIds::SUCCESS)
    {
        if constexpr (requires { request.mutable_operation_params(); }
            && !std::is_same_v<TRequest, Ydb::Query::ExecuteScriptRequest>) {
            request.mutable_operation_params()->set_operation_mode(Ydb::Operations::OperationParams::SYNC);
        }
        grpc::ClientContext context;
        Configure(context, database);
        TResponse response;
        auto status = Runner.RunCall([&] { return (stub.*method)(&context, request, &response); });
        if (success) {
            UNIT_ASSERT_C(status.ok(), request.GetTypeName() << ": " << status.error_message());
            if constexpr (std::is_same_v<TResponse, Ydb::Operations::GetOperationResponse>) {
                if (expectedStatus == Ydb::StatusIds::SUCCESS && !response.operation().ready()) {
                    return response;
                }
            }
            UNIT_ASSERT_VALUES_EQUAL_C(StatusCode(response), expectedStatus,
                request.GetTypeName() << ": " << response.DebugString());
        } else {
            UNIT_ASSERT_C(status.error_code() == grpc::StatusCode::UNAUTHENTICATED
                || (status.ok() && StatusCode(response) == Ydb::StatusIds::UNAUTHORIZED),
                request.GetTypeName() << ": " << status.error_message() << response.DebugString());
        }
        return response;
    }

    template <typename TStub, typename TRequest, typename TResponse>
    TVector<TResponse> Stream(TStub& stub,
        std::unique_ptr<grpc::ClientReader<TResponse>> (TStub::*method)(grpc::ClientContext*, const TRequest&),
        const TRequest& request, const TString& database, bool success = true,
        Ydb::StatusIds::StatusCode expectedStatus = Ydb::StatusIds::SUCCESS)
    {
        grpc::ClientContext context;
        Configure(context, database);
        auto reader = (stub.*method)(&context, request);
        TVector<TResponse> responses;
        TResponse response;
        while (reader->Read(&response)) {
            responses.push_back(response);
        }
        const auto status = reader->Finish();
        if (success) {
            UNIT_ASSERT_C(status.ok(), request.GetTypeName() << ": " << status.error_message());
            UNIT_ASSERT_C(!responses.empty(), request.GetTypeName());
            for (size_t index = 0; index < responses.size(); ++index) {
                const auto& part = responses[index];
                UNIT_ASSERT_VALUES_EQUAL_C(part.status(),
                    index + 1 == responses.size() ? expectedStatus : Ydb::StatusIds::SUCCESS, part.DebugString());
            }
        } else {
            UNIT_ASSERT_C(status.error_code() == grpc::StatusCode::UNAUTHENTICATED
                || (status.ok() && responses.size() == 1 && responses[0].status() == Ydb::StatusIds::UNAUTHORIZED),
                request.GetTypeName() << ": " << status.error_message());
        }
        return responses;
    }

    TString TableSession(const TString& database = "") {
        auto response = Call(*Table, &TTable::CreateSession, Ydb::Table::CreateSessionRequest{},
            database ? database : Database);
        return Unpack<Ydb::Table::CreateSessionResult>(response).session_id();
    }

    TString QuerySession() {
        return Call(*Query, &TQuery::CreateSession, Ydb::Query::CreateSessionRequest{}, Database).session_id();
    }

    struct TAttachment {
        grpc::ClientContext Context;
        std::unique_ptr<grpc::ClientReader<Ydb::Query::SessionState>> Reader;

        ~TAttachment() {
            Context.TryCancel();
            if (Reader) {
                Reader->Finish();
            }
        }
    };

    std::unique_ptr<TAttachment> Attach(const TString& session, const TString& database, bool success = true) {
        auto attachment = std::make_unique<TAttachment>();
        Configure(attachment->Context, database);
        Ydb::Query::AttachSessionRequest request;
        request.set_session_id(session);
        attachment->Reader = Query->AttachSession(&attachment->Context, request);
        Ydb::Query::SessionState response;
        const bool read = attachment->Reader->Read(&response);
        if (success) {
            UNIT_ASSERT_C(read, "No AttachSession response");
            UNIT_ASSERT_VALUES_EQUAL_C(response.status(), Ydb::StatusIds::SUCCESS, response.DebugString());
        } else {
            const auto status = attachment->Reader->Finish();
            attachment->Reader.reset();
            UNIT_ASSERT_C(status.error_code() == grpc::StatusCode::UNAUTHENTICATED
                || (read && response.status() == Ydb::StatusIds::UNAUTHORIZED), status.error_message());
        }
        return attachment;
    }

    TString Select() const {
        return "SELECT Value FROM `" + SqlPath("dir/data") + "` WHERE Key = 1ul;";
    }

    TString SqlPath(const TString& path) const {
        return AbsoluteSqlPaths ? Database + "/" + path : path;
    }

    Ydb::Table::ExecuteDataQueryRequest DataRequest(const TString& session, const TString& sql,
        const TString& transaction = "")
    {
        Ydb::Table::ExecuteDataQueryRequest request;
        request.set_session_id(session);
        request.mutable_query()->set_yql_text(sql);
        if (transaction) {
            request.mutable_tx_control()->set_tx_id(transaction);
        } else {
            request.mutable_tx_control()->mutable_begin_tx()->mutable_serializable_read_write();
            request.mutable_tx_control()->set_commit_tx(true);
        }
        return request;
    }

    Ydb::Query::ExecuteQueryRequest QueryRequest(const TString& sql, const TString& session = "",
        const TString& transaction = "")
    {
        Ydb::Query::ExecuteQueryRequest request;
        request.set_session_id(session);
        request.set_exec_mode(Ydb::Query::EXEC_MODE_EXECUTE);
        request.mutable_query_content()->set_syntax(Ydb::Query::SYNTAX_YQL_V1);
        request.mutable_query_content()->set_text(sql);
        if (transaction) {
            request.mutable_tx_control()->set_tx_id(transaction);
        } else {
            request.mutable_tx_control()->mutable_begin_tx()->mutable_serializable_read_write();
            request.mutable_tx_control()->set_commit_tx(true);
        }
        return request;
    }

    static void CheckRows(const Ydb::ResultSet& result, const TString& value = "target") {
        UNIT_ASSERT_VALUES_EQUAL_C(result.rows_size(), 1, result.DebugString());
        UNIT_ASSERT_VALUES_EQUAL_C(result.rows(0).items(0).text_value(), value, result.DebugString());
    }

    template <typename TPart>
    static void CheckStreamRows(const TVector<TPart>& parts, const TString& value = "target") {
        size_t rows = 0;
        for (const auto& part : parts) {
            const auto& result = [&]() -> const Ydb::ResultSet& {
                if constexpr (requires { part.result_set(); }) {
                    return part.result_set();
                } else {
                    return part.result().result_set();
                }
            }();
            if (result.rows_size()) {
                CheckRows(result, value);
                rows += result.rows_size();
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(rows, 1);
    }

    void SetValue(const TString& value) {
        Call(*Table, &TTable::ExecuteDataQuery,
            DataRequest(TableSession(), "UPSERT INTO `dir/data` (Key, Value) VALUES (1ul, '" + value + "');"), Database);
    }

    void CheckValue(const TString& value) {
        auto result = Unpack<Ydb::Table::ExecuteQueryResult>(
            Call(*Table, &TTable::ExecuteDataQuery, DataRequest(TableSession(), Select()), Database));
        UNIT_ASSERT_VALUES_EQUAL(result.result_sets_size(), 1);
        CheckRows(result.result_sets(0), value);
    }

    Ydb::Query::ExecuteScriptRequest ScriptRequest() const {
        Ydb::Query::ExecuteScriptRequest request;
        request.set_exec_mode(Ydb::Query::EXEC_MODE_EXECUTE);
        request.mutable_script_content()->set_syntax(Ydb::Query::SYNTAX_YQL_V1);
        request.mutable_script_content()->set_text(Select());
        return request;
    }

    Ydb::Operations::Operation Script() {
        return Call(*Query, &TQuery::ExecuteScript, ScriptRequest(), Database);
    }

    Ydb::Operations::Operation WaitScript(const TString& id,
        Ydb::StatusIds::StatusCode expectedStatus = Ydb::StatusIds::SUCCESS) {
        Ydb::Operations::GetOperationRequest request;
        request.set_id(id);
        const auto deadline = TInstant::Now() + TDuration::Seconds(30);
        do {
            auto response = Call(*Operation, &TOperation::GetOperation, request, Database, true, expectedStatus);
            if (response.operation().ready()) {
                return response.operation();
            }
            Sleep(TDuration::MilliSeconds(10));
        } while (TInstant::Now() < deadline);
        UNIT_FAIL("Script did not complete: " << id);
        return {};
    }
};

enum class ETransactionMethod {
    Begin,
    Commit,
    Rollback,
};

template <bool QueryService>
void TestTransaction(TEnvironment& env, const TString& db, bool success, ETransactionMethod method) {
    env.SetValue("target");
    const auto session = QueryService ? env.QuerySession() : env.TableSession();
    auto attachment = QueryService ? env.Attach(session, env.Database) : nullptr;
    TString transaction;
    const auto& beginDatabase = method == ETransactionMethod::Begin ? db : env.Database;
    const bool beginSuccess = method != ETransactionMethod::Begin || success;

    if constexpr (QueryService) {
        Ydb::Query::BeginTransactionRequest request;
        request.set_session_id(session);
        request.mutable_tx_settings()->mutable_serializable_read_write();
        const auto response = env.Call(*env.Query, &TQuery::BeginTransaction, request, beginDatabase, beginSuccess);
        transaction = response.tx_meta().id();
    } else {
        Ydb::Table::BeginTransactionRequest request;
        request.set_session_id(session);
        request.mutable_tx_settings()->mutable_serializable_read_write();
        const auto response = env.Call(*env.Table, &TTable::BeginTransaction, request, beginDatabase, beginSuccess);
        if (beginSuccess) {
            transaction = Unpack<Ydb::Table::BeginTransactionResult>(response).tx_meta().id();
        }
    }
    if (!beginSuccess) {
        return;
    }
    UNIT_ASSERT(!transaction.empty());
    const TString sql = "UPSERT INTO `dir/data` (Key, Value) VALUES (1ul, 'changed');";
    if constexpr (QueryService) {
        env.Stream(*env.Query, &TQuery::ExecuteQuery, env.QueryRequest(sql, session, transaction), env.Database);
    } else {
        env.Call(*env.Table, &TTable::ExecuteDataQuery, env.DataRequest(session, sql, transaction), env.Database);
    }

    const auto& finishDatabase = method == ETransactionMethod::Begin ? env.Database : db;
    if constexpr (QueryService) {
        if (method == ETransactionMethod::Rollback) {
            Ydb::Query::RollbackTransactionRequest request;
            request.set_session_id(session);
            request.set_tx_id(transaction);
            env.Call(*env.Query, &TQuery::RollbackTransaction, request, finishDatabase, success);
        } else {
            Ydb::Query::CommitTransactionRequest request;
            request.set_session_id(session);
            request.set_tx_id(transaction);
            env.Call(*env.Query, &TQuery::CommitTransaction, request, finishDatabase, success);
        }
        if (!success) {
            Ydb::Query::RollbackTransactionRequest request;
            request.set_session_id(session);
            request.set_tx_id(transaction);
            env.Call(*env.Query, &TQuery::RollbackTransaction, request, env.Database);
        }
    } else {
        if (method == ETransactionMethod::Rollback) {
            Ydb::Table::RollbackTransactionRequest request;
            request.set_session_id(session);
            request.set_tx_id(transaction);
            env.Call(*env.Table, &TTable::RollbackTransaction, request, finishDatabase, success);
        } else {
            Ydb::Table::CommitTransactionRequest request;
            request.set_session_id(session);
            request.set_tx_id(transaction);
            env.Call(*env.Table, &TTable::CommitTransaction, request, finishDatabase, success);
        }
        if (!success) {
            Ydb::Table::RollbackTransactionRequest request;
            request.set_session_id(session);
            request.set_tx_id(transaction);
            env.Call(*env.Table, &TTable::RollbackTransaction, request, env.Database);
        }
    }
    env.CheckValue(success && method != ETransactionMethod::Rollback ? "changed" : "target");
}
template <typename TTest>
void RunCases(bool singleComponent, TTest test, bool useRealThreads = true) {
    for (bool ignoreRoot : {false, true}) {
        TEnvironment env(singleComponent, ignoreRoot, true, false, useRealThreads);
        test(env, env.Database, true);
        test(env, env.OldDatabase, ignoreRoot);
    }
}

template <typename TTest>
void RunSqlCases(bool singleComponent, TTest test) {
    RunCases(singleComponent, [&](TEnvironment& env, const TString& db, bool success) {
        for (bool absolute : {false, true}) {
            env.AbsoluteSqlPaths = absolute;
            test(env, db, success);
        }
    });
}

} // namespace

Y_UNIT_TEST_SUITE(YdbIgnoreRoot) {

    Y_UNIT_TEST_TWIN(DiscoveryDatabaseFields, SingleComponent) {
        TEnvironment env(SingleComponent, true);
        Ydb::Discovery::ListEndpointsRequest request;
        request.set_database(env.OldDatabase);
        for (const TString& header : {TString(), env.OldDatabase, env.Database}) {
            auto response = env.Call(*env.Discovery, &TDiscovery::ListEndpoints, request, header);
            UNIT_ASSERT(Unpack<Ydb::Discovery::ListEndpointsResult>(response).endpoints_size() > 0);
        }
        env.Call(*env.Discovery, &TDiscovery::ListEndpoints, request, "/" + env.Root + "/other",
            true, Ydb::StatusIds::BAD_REQUEST);
        request.set_database("/" + env.Root);
        env.Call(*env.Discovery, &TDiscovery::ListEndpoints, request, "/" + env.Root);
    }

    Y_UNIT_TEST_TWIN(AnonymousRequests, SingleComponent) {
        TEnvironment env(SingleComponent, true, false);
        Ydb::Discovery::ListEndpointsRequest discovery;
        discovery.set_database(env.OldDatabase);
        env.Call(*env.Discovery, &TDiscovery::ListEndpoints, discovery, env.OldDatabase);
        const auto session = env.TableSession(env.OldDatabase);
        auto result = Unpack<Ydb::Table::ExecuteQueryResult>(env.Call(*env.Table, &TTable::ExecuteDataQuery,
            env.DataRequest(session, env.Select()), env.OldDatabase));
        TEnvironment::CheckRows(result.result_sets(0));
        TEnvironment::CheckStreamRows(env.Stream(*env.Query, &TQuery::ExecuteQuery,
            env.QueryRequest(env.Select()), env.OldDatabase));
    }

    Y_UNIT_TEST_TWIN(MissingAndRelativeDatabases, SingleComponent) {
        for (bool ignoreRoot : {false, true}) {
            TEnvironment env(SingleComponent, ignoreRoot);
            for (const TString& database : {env.Name, TString("missing"), TString("team/missing"),
                "/" + env.Root + "/missing", TString("/old/missing"), TString("/missing")}) {
                env.Call(*env.Table, &TTable::CreateSession, Ydb::Table::CreateSessionRequest{}, database, false);
            }
            for (const TString& database : {TString("/"), TString("///")}) {
                env.Call(*env.Table, &TTable::CreateSession, Ydb::Table::CreateSessionRequest{}, database,
                    true, Ydb::StatusIds::BAD_REQUEST);
            }
            env.Call(*env.Discovery, &TDiscovery::WhoAmI, Ydb::Discovery::WhoAmIRequest{}, "");
        }
    }

    Y_UNIT_TEST(NestedDatabase) {
        TEnvironment env(false, true, true, true);
        Ydb::Discovery::ListEndpointsRequest discovery;
        discovery.set_database(env.OldDatabase);
        env.Call(*env.Discovery, &TDiscovery::ListEndpoints, discovery, env.OldDatabase);
        const auto session = env.TableSession(env.OldDatabase);
        auto result = Unpack<Ydb::Table::ExecuteQueryResult>(env.Call(*env.Table, &TTable::ExecuteDataQuery,
            env.DataRequest(session, env.Select()), env.OldDatabase));
        TEnvironment::CheckRows(result.result_sets(0));
    }

    Y_UNIT_TEST_TWIN(SqlResourcePathsRemainUnchanged, SingleComponent) {
        TEnvironment env(SingleComponent, true);
        const TString sql = "SELECT Value FROM `" + env.OldDatabase + "/dir/data` WHERE Key = 1ul;";
        env.Call(*env.Table, &TTable::ExecuteDataQuery, env.DataRequest(env.TableSession(), sql),
            env.OldDatabase, true, Ydb::StatusIds::SCHEME_ERROR);
        env.CheckValue("target");
    }

    Y_UNIT_TEST_TWIN(UnchangedSdk, SingleComponent) {
        TEnvironment env(SingleComponent, true);
        NYdb::TDriver driver(NYdb::TDriverConfig().SetEndpoint(env.Runner.GetEndpoint())
            .SetDatabase(env.OldDatabase).SetAuthToken(env.Token).SetDiscoveryMode(NYdb::EDiscoveryMode::Sync));
        NYdb::NDiscovery::TDiscoveryClient discovery(driver);
        auto endpoints = discovery.ListEndpoints().GetValueSync();
        UNIT_ASSERT_C(endpoints.IsSuccess(), endpoints.GetIssues().ToString());
        UNIT_ASSERT(!endpoints.GetEndpointsInfo().empty());

        NYdb::NTable::TTableClient table(driver);
        const auto created = table.CreateSession().GetValueSync();
        UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
        auto session = created.GetSession();
        NYdb::NQuery::TQueryClient query(driver);
        for (const TString& path : {TString("dir/data"), env.Database + "/dir/data"}) {
            const TString sql = "SELECT Value FROM `" + path + "` WHERE Key = 1ul;";
            auto tableResult = session.ExecuteDataQuery(sql,
                NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::SerializableRW()).CommitTx()).GetValueSync();
            UNIT_ASSERT_C(tableResult.IsSuccess(), tableResult.GetIssues().ToString());
            TEnvironment::CheckRows(NYdb::TProtoAccessor::GetProto(tableResult.GetResultSet(0)));
            auto queryResult = query.ExecuteQuery(sql,
                NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(queryResult.IsSuccess(), queryResult.GetIssues().ToString());
            TEnvironment::CheckRows(NYdb::TProtoAccessor::GetProto(queryResult.GetResultSet(0)));
        }
        driver.Stop(true);
    }

    Y_UNIT_TEST_TWIN(DatabaseAndTablePermissions, SingleComponent) {
        TEnvironment env(SingleComponent, true);
        auto scheme = Ydb::Scheme::V1::SchemeService::NewStub(env.Channel);
        Ydb::Scheme::ModifyPermissionsRequest grant;
        grant.set_path(env.Database);
        auto* permissions = grant.add_actions()->mutable_grant();
        permissions->set_subject("reader@builtin");
        permissions->add_permission_names("ydb.database.connect");
        env.Call(*scheme, &TScheme::ModifyPermissions, grant, env.Database);

        env.Token = "denied@builtin";
        Ydb::Discovery::ListEndpointsRequest discovery;
        discovery.set_database(env.OldDatabase);
        env.Call(*env.Discovery, &TDiscovery::ListEndpoints, discovery, env.OldDatabase, false);
        env.Call(*env.Table, &TTable::CreateSession, Ydb::Table::CreateSessionRequest{}, env.OldDatabase, false);

        env.Token = "reader@builtin";
        const auto session = env.TableSession(env.OldDatabase);
        env.Call(*env.Table, &TTable::ExecuteDataQuery,
            env.DataRequest(session, env.Select()), env.OldDatabase, true, Ydb::StatusIds::SCHEME_ERROR);
        env.Stream(*env.Query, &TQuery::ExecuteQuery, env.QueryRequest(env.Select()), env.OldDatabase,
            true, Ydb::StatusIds::SCHEME_ERROR);

        env.Token = "root@builtin";
        env.CheckValue("target");
    }

    Y_UNIT_TEST_TWIN(SiblingDatabaseIsolation, SingleComponent) {
        TEnvironment env(SingleComponent, true);
        const TString sibling = env.Runner.CreateDatabase("sibling", "ssd", {});
        NYdb::TDriver driver(NYdb::TDriverConfig().SetEndpoint(env.Runner.GetEndpoint())
            .SetDatabase(sibling).SetAuthToken(env.Token));
        NYdb::NTable::TTableClient table(driver);
        auto created = table.CreateSession().GetValueSync();
        UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
        auto session = created.GetSession();
        NYdb::NScheme::TSchemeClient scheme(driver);
        auto directory = scheme.MakeDirectory(sibling + "/dir").GetValueSync();
        UNIT_ASSERT_C(directory.IsSuccess(), directory.GetIssues().ToString());
        auto ddl = session.ExecuteSchemeQuery(
            "CREATE TABLE `dir/data` (Key Uint64 NOT NULL, Value Utf8 NOT NULL, PRIMARY KEY(Key));").GetValueSync();
        UNIT_ASSERT_C(ddl.IsSuccess(), ddl.GetIssues().ToString());
        auto write = session.ExecuteDataQuery("UPSERT INTO `dir/data` (Key, Value) VALUES (1ul, 'sibling');",
            NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::SerializableRW()).CommitTx()).GetValueSync();
        UNIT_ASSERT_C(write.IsSuccess(), write.GetIssues().ToString());

        // A script operation belongs to its resolved database, even if the caller
        // is allowed to connect to both databases.
        const auto operation = env.Script();
        env.WaitScript(operation.id());
        auto siblingOperation = Ydb::Operation::V1::OperationService::NewStub(
            grpc::CreateChannel(env.Runner.GetTestServer().GetTenantGRpcServer(sibling).GetHost() + ":" +
                ToString(env.Runner.GetTestServer().GetTenantGRpcServer(sibling).GetPort()),
                grpc::InsecureChannelCredentials()));
        Ydb::Operations::GetOperationRequest get;
        get.set_id(operation.id());
        env.Call(*siblingOperation, &TOperation::GetOperation, get, "/old/sibling",
            true, Ydb::StatusIds::NOT_FOUND);
        TEnvironment::CheckStreamRows(env.Stream(*env.Query, &TQuery::ExecuteQuery,
            env.QueryRequest(env.Select()), env.OldDatabase));
        driver.Stop(true);
    }

    Y_UNIT_TEST_TWIN(TableBeginTransaction, SingleComponent) {
        RunCases(SingleComponent, [](TEnvironment& env, const TString& db, bool success) {
            TestTransaction<false>(env, db, success, ETransactionMethod::Begin);
        });
    }

    Y_UNIT_TEST_TWIN(TableCommitTransaction, SingleComponent) {
        RunCases(SingleComponent, [](TEnvironment& env, const TString& db, bool success) {
            TestTransaction<false>(env, db, success, ETransactionMethod::Commit);
        });
    }

    Y_UNIT_TEST_TWIN(TableRollbackTransaction, SingleComponent) {
        RunCases(SingleComponent, [](TEnvironment& env, const TString& db, bool success) {
            TestTransaction<false>(env, db, success, ETransactionMethod::Rollback);
        });
    }

    Y_UNIT_TEST_TWIN(QueryBeginTransaction, SingleComponent) {
        RunCases(SingleComponent, [](TEnvironment& env, const TString& db, bool success) {
            TestTransaction<true>(env, db, success, ETransactionMethod::Begin);
        });
    }

    Y_UNIT_TEST_TWIN(QueryCommitTransaction, SingleComponent) {
        RunCases(SingleComponent, [](TEnvironment& env, const TString& db, bool success) {
            TestTransaction<true>(env, db, success, ETransactionMethod::Commit);
        });
    }

    Y_UNIT_TEST_TWIN(QueryRollbackTransaction, SingleComponent) {
        RunCases(SingleComponent, [](TEnvironment& env, const TString& db, bool success) {
            TestTransaction<true>(env, db, success, ETransactionMethod::Rollback);
        });
    }

    Y_UNIT_TEST_TWIN(CancelOperation, SingleComponent) {
        RunCases(SingleComponent, [](TEnvironment& env, const TString& db, bool success) {
            const auto table = env.Runner.RunCall([&] {
                auto description = env.Runner.GetTestClient().Ls(env.Database + "/dir/data");
                UNIT_ASSERT_VALUES_EQUAL(description->Record.GetStatus(), NMsgBusProxy::MSTATUS_OK);
                return description->Record.GetPathDescription().GetSelf();
            });
            std::atomic<bool> blocked = false;
            auto& runtime = *env.Runner.GetTestServer().GetRuntime();
            const auto observer = runtime.AddObserver<TEvDataShard::TEvRead>(
                [&blocked, table](TEvDataShard::TEvRead::TPtr& ev) {
                    const auto& id = ev->Get()->Record.GetTableId();
                    if (id.GetOwnerId() == table.GetSchemeshardId() && id.GetTableId() == table.GetPathId()) {
                        blocked.store(true);
                        ev.Reset();
                    }
                });
            const auto operation = env.Script();
            runtime.WaitFor("script table read", [&] { return blocked.load(); }, TDuration::Seconds(30));
            UNIT_ASSERT_C(blocked.load(), "Script did not reach its table read");

            Ydb::Operations::CancelOperationRequest cancel;
            cancel.set_id(operation.id());
            env.Call(*env.Operation, &TOperation::CancelOperation, cancel, db, success);
            if (!success) {
                env.Call(*env.Operation, &TOperation::CancelOperation, cancel, env.Database);
            }
            const auto result = env.WaitScript(operation.id(), Ydb::StatusIds::CANCELLED);
            Ydb::Query::ExecuteScriptMetadata metadata;
            UNIT_ASSERT(result.metadata().UnpackTo(&metadata));
            UNIT_ASSERT_VALUES_EQUAL(metadata.exec_status(), Ydb::Query::EXEC_STATUS_CANCELLED);
        }, false);
    }

    Y_UNIT_TEST_TWIN(TableDeleteSession, SingleComponent) {
        RunCases(SingleComponent, [](TEnvironment& env, const TString& db, bool success) {
            const auto session = env.TableSession();
            Ydb::Table::DeleteSessionRequest request;
            request.set_session_id(session);
            env.Call(*env.Table, &TTable::DeleteSession, request, db, success);
            Ydb::Table::KeepAliveRequest keepAlive;
            keepAlive.set_session_id(session);
            env.Call(*env.Table, &TTable::KeepAlive, keepAlive, env.Database, true,
                success ? Ydb::StatusIds::BAD_SESSION : Ydb::StatusIds::SUCCESS);
        });
    }

    Y_UNIT_TEST_TWIN(TableKeepAlive, SingleComponent) {
        RunCases(SingleComponent, [](TEnvironment& env, const TString& db, bool success) {
            Ydb::Table::KeepAliveRequest request;
            request.set_session_id(env.TableSession());
            auto response = env.Call(*env.Table, &TTable::KeepAlive, request, db, success);
            if (success) {
                UNIT_ASSERT_VALUES_EQUAL(Unpack<Ydb::Table::KeepAliveResult>(response).session_status(),
                    Ydb::Table::KeepAliveResult::SESSION_STATUS_READY);
            }
        });
    }

    Y_UNIT_TEST_TWIN(QueryDeleteSession, SingleComponent) {
        RunCases(SingleComponent, [](TEnvironment& env, const TString& db, bool success) {
            const auto session = env.QuerySession();
            auto attachment = env.Attach(session, env.Database);
            Ydb::Query::DeleteSessionRequest request;
            request.set_session_id(session);
            env.Call(*env.Query, &TQuery::DeleteSession, request, db, success);
            if (success) {
                Ydb::Query::SessionState response;
                // Deleting an attached session closes its attachment stream.
                while (attachment->Reader->Read(&response)) {
                    UNIT_ASSERT_VALUES_EQUAL_C(response.status(), Ydb::StatusIds::SUCCESS, response.DebugString());
                }
                const auto status = attachment->Reader->Finish();
                attachment->Reader.reset();
                UNIT_ASSERT_C(status.ok(), status.error_message());
            }
        });
    }

    Y_UNIT_TEST_TWIN(QueryAttachSession, SingleComponent) {
        RunCases(SingleComponent, [](TEnvironment& env, const TString& db, bool success) {
            const auto session = env.QuerySession();
            auto attachment = env.Attach(session, db, success);
            if (success) {
                TEnvironment::CheckStreamRows(env.Stream(*env.Query, &TQuery::ExecuteQuery,
                    env.QueryRequest(env.Select(), session), env.Database));
            }
        });
    }

    Y_UNIT_TEST_TWIN(ExecuteDataQuery, SingleComponent) {
        RunSqlCases(SingleComponent, [](TEnvironment& env, const TString& db, bool success) {
            auto response = env.Call(*env.Table, &TTable::ExecuteDataQuery,
                env.DataRequest(env.TableSession(), env.Select()), db, success);
            if (success) {
                auto result = Unpack<Ydb::Table::ExecuteQueryResult>(response);
                TEnvironment::CheckRows(result.result_sets(0));
            }
        });
    }

    Y_UNIT_TEST_TWIN(PrepareDataQuery, SingleComponent) {
        RunSqlCases(SingleComponent, [](TEnvironment& env, const TString& db, bool success) {
            const auto session = env.TableSession();
            Ydb::Table::PrepareDataQueryRequest request;
            request.set_session_id(session);
            request.set_yql_text(env.Select());
            auto response = env.Call(*env.Table, &TTable::PrepareDataQuery, request, db, success);
            if (success) {
                auto prepared = Unpack<Ydb::Table::PrepareQueryResult>(response);
                UNIT_ASSERT(!prepared.query_id().empty());
                auto execute = env.DataRequest(session, "");
                execute.mutable_query()->set_id(prepared.query_id());
                auto result = Unpack<Ydb::Table::ExecuteQueryResult>(
                    env.Call(*env.Table, &TTable::ExecuteDataQuery, execute, env.Database));
                TEnvironment::CheckRows(result.result_sets(0));
            }
        });
    }

    Y_UNIT_TEST_TWIN(ExplainDataQuery, SingleComponent) {
        RunSqlCases(SingleComponent, [](TEnvironment& env, const TString& db, bool success) {
            Ydb::Table::ExplainDataQueryRequest request;
            request.set_session_id(env.TableSession());
            request.set_yql_text(env.Select());
            auto response = env.Call(*env.Table, &TTable::ExplainDataQuery, request, db, success);
            if (success) {
                UNIT_ASSERT(!Unpack<Ydb::Table::ExplainQueryResult>(response).query_ast().empty());
            }
        });
    }

    Y_UNIT_TEST_TWIN(ExecuteSchemeQuery, SingleComponent) {
        RunSqlCases(SingleComponent, [](TEnvironment& env, const TString& db, bool success) {
            const auto session = env.TableSession();
            Ydb::Table::ExecuteSchemeQueryRequest request;
            request.set_session_id(session);
            request.set_yql_text("CREATE TABLE `" + env.SqlPath("dir/created") + "` (Key Uint64, PRIMARY KEY(Key));");
            env.Call(*env.Table, &TTable::ExecuteSchemeQuery, request, db, success);
            if (success) {
                // Resolving and dropping via the canonical database proves where it was created.
                request.set_yql_text("DROP TABLE `" + env.Database + "/dir/created`;");
                env.Call(*env.Table, &TTable::ExecuteSchemeQuery, request, env.Database);
            }
        });
    }

    Y_UNIT_TEST_TWIN(StreamExecuteScanQuery, SingleComponent) {
        RunSqlCases(SingleComponent, [](TEnvironment& env, const TString& db, bool success) {
            Ydb::Table::ExecuteScanQueryRequest request;
            request.mutable_query()->set_yql_text(env.Select());
            auto parts = env.Stream(*env.Table, &TTable::StreamExecuteScanQuery, request, db, success);
            if (success) {
                TEnvironment::CheckStreamRows(parts);
            }
        });
    }

    Y_UNIT_TEST_TWIN(ExecuteQuery, SingleComponent) {
        RunSqlCases(SingleComponent, [](TEnvironment& env, const TString& db, bool success) {
            const auto session = env.QuerySession();
            auto attachment = env.Attach(session, env.Database);
            auto parts = env.Stream(*env.Query, &TQuery::ExecuteQuery,
                env.QueryRequest(env.Select(), session), db, success);
            if (success) {
                TEnvironment::CheckStreamRows(parts);
            }
        });
    }

    Y_UNIT_TEST_TWIN(ExecuteScript, SingleComponent) {
        RunSqlCases(SingleComponent, [](TEnvironment& env, const TString& db, bool success) {
            auto response = env.Call(*env.Query, &TQuery::ExecuteScript, env.ScriptRequest(), db, success);
            if (success) {
                UNIT_ASSERT(!response.id().empty());
                env.WaitScript(response.id());
                Ydb::Query::FetchScriptResultsRequest fetch;
                fetch.set_operation_id(response.id());
                fetch.set_rows_limit(10);
                auto result = env.Call(*env.Query, &TQuery::FetchScriptResults, fetch, env.Database);
                TEnvironment::CheckRows(result.result_set());
            }
        });
    }

    Y_UNIT_TEST_TWIN(FetchScriptResults, SingleComponent) {
        RunCases(SingleComponent, [](TEnvironment& env, const TString& db, bool success) {
            const auto operation = env.Script();
            env.WaitScript(operation.id());
            Ydb::Query::FetchScriptResultsRequest request;
            request.set_operation_id(operation.id());
            request.set_rows_limit(10);
            auto result = env.Call(*env.Query, &TQuery::FetchScriptResults, request, db, success);
            if (success) {
                TEnvironment::CheckRows(result.result_set());
            }
        });
    }

    Y_UNIT_TEST_TWIN(DescribeTableOptions, SingleComponent) {
        RunCases(SingleComponent, [](TEnvironment& env, const TString& db, bool success) {
            auto response = env.Call(*env.Table, &TTable::DescribeTableOptions,
                Ydb::Table::DescribeTableOptionsRequest{}, db, success);
            if (success) {
                Unpack<Ydb::Table::DescribeTableOptionsResult>(response);
            }
        });
    }

    Y_UNIT_TEST_TWIN(ExecuteYql, SingleComponent) {
        RunSqlCases(SingleComponent, [](TEnvironment& env, const TString& db, bool success) {
            Ydb::Scripting::ExecuteYqlRequest request;
            request.set_script(env.Select());
            request.set_syntax(Ydb::Query::SYNTAX_YQL_V1);
            auto response = env.Call(*env.Scripting, &TScripting::ExecuteYql, request, db, success);
            if (success) {
                auto result = Unpack<Ydb::Scripting::ExecuteYqlResult>(response);
                TEnvironment::CheckRows(result.result_sets(0));
            }
        });
    }

    Y_UNIT_TEST_TWIN(StreamExecuteYql, SingleComponent) {
        RunSqlCases(SingleComponent, [](TEnvironment& env, const TString& db, bool success) {
            Ydb::Scripting::ExecuteYqlRequest request;
            request.set_script(env.Select());
            request.set_syntax(Ydb::Query::SYNTAX_YQL_V1);
            auto parts = env.Stream(*env.Scripting, &TScripting::StreamExecuteYql, request, db, success);
            if (success) {
                TEnvironment::CheckStreamRows(parts);
            }
        });
    }

    Y_UNIT_TEST_TWIN(ExplainYql, SingleComponent) {
        RunSqlCases(SingleComponent, [](TEnvironment& env, const TString& db, bool success) {
            Ydb::Scripting::ExplainYqlRequest request;
            request.set_script(env.Select());
            request.set_mode(Ydb::Scripting::ExplainYqlRequest::PLAN);
            auto response = env.Call(*env.Scripting, &TScripting::ExplainYql, request, db, success);
            if (success) {
                UNIT_ASSERT(!Unpack<Ydb::Scripting::ExplainYqlResult>(response).plan().empty());
            }
        });
    }

    Y_UNIT_TEST_TWIN(GetOperation, SingleComponent) {
        RunCases(SingleComponent, [](TEnvironment& env, const TString& db, bool success) {
            const auto operation = env.Script();
            env.WaitScript(operation.id());
            Ydb::Operations::GetOperationRequest request;
            request.set_id(operation.id());
            auto response = env.Call(*env.Operation, &TOperation::GetOperation, request, db, success);
            if (success) {
                UNIT_ASSERT(response.operation().ready());
                UNIT_ASSERT_VALUES_EQUAL(response.operation().id(), operation.id());
            }
        });
    }

    Y_UNIT_TEST_TWIN(ListOperations, SingleComponent) {
        RunCases(SingleComponent, [](TEnvironment& env, const TString& db, bool success) {
            const auto operation = env.Script();
            env.WaitScript(operation.id());
            Ydb::Operations::ListOperationsRequest request;
            request.set_kind("scriptexec");
            request.set_page_size(100);
            auto response = env.Call(*env.Operation, &TOperation::ListOperations, request, db, success);
            if (success) {
                bool found = false;
                for (const auto& item : response.operations()) {
                    found |= item.id() == operation.id();
                }
                UNIT_ASSERT_C(found, response.DebugString());
            }
        });
    }

    Y_UNIT_TEST_TWIN(ForgetOperation, SingleComponent) {
        RunCases(SingleComponent, [](TEnvironment& env, const TString& db, bool success) {
            const auto operation = env.Script();
            env.WaitScript(operation.id());
            Ydb::Operations::ForgetOperationRequest request;
            request.set_id(operation.id());
            env.Call(*env.Operation, &TOperation::ForgetOperation, request, db, success);
            Ydb::Operations::GetOperationRequest get;
            get.set_id(operation.id());
            env.Call(*env.Operation, &TOperation::GetOperation, get, env.Database, true,
                success ? Ydb::StatusIds::NOT_FOUND : Ydb::StatusIds::SUCCESS);
        });
    }
    Y_UNIT_TEST_TWIN(ListEndpoints, SingleComponent) {
        RunCases(SingleComponent, [](TEnvironment& env, const TString& db, bool success) {
            Ydb::Discovery::ListEndpointsRequest request;
            request.set_database(db);
            auto response = env.Call(*env.Discovery, &TDiscovery::ListEndpoints, request, db, success);
            if (success) {
                auto endpoints = Unpack<Ydb::Discovery::ListEndpointsResult>(response);
                UNIT_ASSERT_C(endpoints.endpoints_size() > 0, response.DebugString());
            }
        });
    }

    Y_UNIT_TEST_TWIN(WhoAmI, SingleComponent) {
        RunCases(SingleComponent, [](TEnvironment& env, const TString& db, bool success) {
            auto response = env.Call(*env.Discovery, &TDiscovery::WhoAmI,
                Ydb::Discovery::WhoAmIRequest{}, db, success);
            if (success) {
                UNIT_ASSERT_VALUES_EQUAL(Unpack<Ydb::Discovery::WhoAmIResult>(response).user(), env.Token);
            }
        });
    }

    Y_UNIT_TEST_TWIN(TableCreateSession, SingleComponent) {
        RunCases(SingleComponent, [](TEnvironment& env, const TString& db, bool success) {
            auto response = env.Call(*env.Table, &TTable::CreateSession, Ydb::Table::CreateSessionRequest{}, db, success);
            if (success) {
                auto session = Unpack<Ydb::Table::CreateSessionResult>(response).session_id();
                UNIT_ASSERT(!session.empty());
                auto result = Unpack<Ydb::Table::ExecuteQueryResult>(env.Call(*env.Table, &TTable::ExecuteDataQuery,
                    env.DataRequest(session, env.Select()), env.Database));
                TEnvironment::CheckRows(result.result_sets(0));
            }
        });
    }

    Y_UNIT_TEST_TWIN(QueryCreateSession, SingleComponent) {
        RunCases(SingleComponent, [](TEnvironment& env, const TString& db, bool success) {
            auto response = env.Call(*env.Query, &TQuery::CreateSession, Ydb::Query::CreateSessionRequest{}, db, success);
            if (success) {
                UNIT_ASSERT(!response.session_id().empty());
                auto attachment = env.Attach(response.session_id(), env.Database);
                TEnvironment::CheckStreamRows(env.Stream(*env.Query, &TQuery::ExecuteQuery,
                    env.QueryRequest(env.Select(), response.session_id()), env.Database));
            }
        });
    }
}

} // namespace NKikimr::NGRpcService
