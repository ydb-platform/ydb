#include <ydb/core/fq/libs/config/protos/fq_config.pb.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/services/udf_store/wasm/compile.h>
#include <ydb/services/udf_store/wasm/module_catalog.h>

#include <library/cpp/http/server/http_ex.h>
#include <library/cpp/resource/resource.h>
#include <library/cpp/testing/common/network.h>
#include <util/stream/file.h>
#include <util/system/tempfile.h>

#include <atomic>

namespace NKikimr::NKqp {
namespace {
using namespace NYdb;
using namespace NYdb::NQuery;
using namespace NUdfStore::NWasm;

class TEchoServer: public THttpServer::ICallBack {
    class TRequest final: public THttpClientRequestEx {
    public:
        explicit TRequest(TEchoServer& server)
            : Server(server)
        {
        }

        bool Reply(void*) override {
            if (!ProcessHeaders()) {
                return true;
            }
            ++Server.Requests;
            for (const auto& header : ParsedHeaders) {
                if (header.first == "Authorization" && header.second == "Bearer test-token") {
                    ++Server.Authenticated;
                }
                if (header.first == "Authorization" && header.second == "Bearer rotated-token") {
                    ++Server.Rotated;
                }
            }
            Output().EnableCompressionHeader(false);
            Output() << "HTTP/1.1 200 OK\r\nContent-Length: " << Buf.Size() << "\r\nConnection: close\r\n\r\n";
            Output().Write(Buf.AsCharPtr(), Buf.Size());
            Output().Finish();
            return true;
        }

    private:
        TEchoServer& Server;
    };

    NTesting::TPortHolder Port = NTesting::GetFreePort();
    THttpServer Server;

public:
    std::atomic<size_t> Requests = 0;
    std::atomic<size_t> Authenticated = 0;
    std::atomic<size_t> Rotated = 0;

    TEchoServer()
        : Server(this, THttpServerOptions(Port).SetHost("127.0.0.1").SetThreads(4))
    {
        UNIT_ASSERT(Server.Start());
    }

    ~TEchoServer() {
        Server.Stop();
    }

    TClientRequest* CreateClient() override {
        return new TRequest(*this);
    }

    TString Url() const {
        return TStringBuilder() << "http://127.0.0.1:" << static_cast<ui16>(Port);
    }
};

struct TFixture {
    TTempFileHandle Module;
    TTempFileHandle Manifest;
    TEchoServer Service;
    std::unique_ptr<TKikimrRunner> Runner;

    explicit TFixture(bool store = false, ui32 batchRows = 1) {
        TFileOutput(Module.Name()).Write(NResource::Find("/echo.wasm"));
        TFileOutput(Manifest.Name()).Write(NResource::Find("/echo.manifest"));
        NKikimrConfig::TAppConfig config;
        config.MutableQueryServiceConfig()->AddAvailableExternalDataSources("ExternalService");
        auto& services = *config.MutableFederatedQueryConfig()->MutableWasmServices();
        services.SetEnabled(true);
        services.SetMaxBatchRows(batchRows);
        services.SetCallTimeoutMs(10000);
        if (store) {
            auto artifact = std::make_shared<TWasmModuleArtifact>();
            artifact->ModuleName = "Echo";
            artifact->Manifest = ParseManifest(NResource::Find("/echo.manifest"));
            const auto bytes = NResource::Find("/echo.wasm");
            artifact->ModuleBytecode = MakeModuleBytecode(
                bytes, CompileModuleObjectCode(bytes, NYdb::NWasm::EBytecodeFormat::Binary), NYdb::NWasm::EBytecodeFormat::Binary);
            GetWasmModuleCatalog().Register(artifact);
        } else {
            services.SetModuleSource(NFq::NConfig::TWasmServicesConfig::LOCAL_FILES);
            auto& module = *services.AddModules();
            module.SetModulePath(Module.Name());
            module.SetManifestPath(Manifest.Name());
        }
        NKikimrConfig::TFeatureFlags flags;
        flags.SetEnableExternalDataSources(true);
        flags.SetEnableScriptExecutionOperations(true);
        flags.SetEnableSchemaSecrets(true);
        Runner = std::make_unique<TKikimrRunner>(
            TKikimrSettings(config).SetFeatureFlags(flags).SetWithSampleTables(false).SetInitFederatedQuerySetupFactory(true).SetAuthToken(
                "root@builtin"));
        Scheme("CREATE SECRET `/Root/service_token` WITH (value='test-token');");
        CreateConnection(Service.Url());
    }

    ~TFixture() {
        Runner.reset();
        GetWasmModuleCatalog().Unregister("Echo");
    }

    void Scheme(const TString& sql) {
        auto result = Runner->GetQueryClient().ExecuteQuery(sql, TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    void CreateConnection(const TString& url) {
        Scheme(TStringBuilder() << "CREATE EXTERNAL DATA SOURCE `/Root/service` WITH (SOURCE_TYPE='ExternalService', "
                                << "LOCATION='" << url
                                << "', PROTOCOL='HTTP', USE_TLS='false', AUTH_METHOD='TOKEN', TOKEN_SECRET_PATH='/Root/service_token');");
    }

    static TString Query(const TString& input = "SELECT 'hello' AS message") {
        return TStringBuilder() << "$input = " << input << ";\n"
                                << "$output = PROCESS $input USING EXTERNAL FUNCTION('Echo', 'Echo') WITH CONNECTION='/Root/service', "
                                << "INPUT_TYPE=Struct<message:String>, OUTPUT_TYPE=Struct<value:String,length:Uint64,empty:Bool,delta:Int64>;\n"
                                << "SELECT value, length FROM $output ORDER BY value;";
    }
};
}   // namespace

Y_UNIT_TEST_SUITE(KqpWasmServices) {
    Y_UNIT_TEST_TWIN(LocalAndStoreModuleExecution, Store) {
        TFixture fixture(Store);
        auto result = fixture.Runner->GetQueryClient().ExecuteQuery(TFixture::Query(), TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        TResultSetParser rows(result.GetResultSet(0));
        UNIT_ASSERT(rows.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(rows.ColumnParser("value").GetString(), "hello");
        UNIT_ASSERT_VALUES_EQUAL(rows.ColumnParser("length").GetUint64(), 5);
        UNIT_ASSERT(!rows.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Service.Requests.load(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Service.Authenticated.load(), 1);
    }

    Y_UNIT_TEST_TWIN(TableEnrichment200Rows, Batch) {
        TFixture fixture(false, Batch ? 8 : 1);
        fixture.Scheme("CREATE TABLE `/Root/input` (id Uint64 NOT NULL, message String NOT NULL, PRIMARY KEY(id));");
        TStringBuilder insert;
        insert << "INSERT INTO `/Root/input` (id, message) VALUES ";
        for (ui64 i = 0; i < 200; ++i) {
            if (i) {
                insert << ',';
            }
            insert << '(' << i << "ul, 'row-" << i << "')";
        }
        insert << ';';
        auto client = fixture.Runner->GetQueryClient();
        auto written = client.ExecuteQuery(insert, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).GetValueSync();
        UNIT_ASSERT_C(written.IsSuccess(), written.GetIssues().ToString());
        auto result =
            client.ExecuteQuery(TFixture::Query("SELECT message FROM `/Root/input`"), TTxControl::BeginTx(TTxSettings::SnapshotRO()).CommitTx())
                .GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        TResultSetParser rows(result.GetResultSet(0));
        THashSet<TString> values;
        while (rows.TryNextRow()) {
            const auto value = rows.ColumnParser("value").GetString();
            UNIT_ASSERT_VALUES_EQUAL(rows.ColumnParser("length").GetUint64(), value.size());
            UNIT_ASSERT(values.insert(TString(value)).second);
        }
        UNIT_ASSERT_VALUES_EQUAL(values.size(), 200);
        for (ui64 i = 0; i < 200; ++i) {
            UNIT_ASSERT(values.contains(TStringBuilder() << "row-" << i));
        }
        UNIT_ASSERT_VALUES_EQUAL(fixture.Service.Authenticated.load(), fixture.Service.Requests.load());
    }

    Y_UNIT_TEST(ConnectionReplacementInvalidatesCachedPlan) {
        TFixture fixture;
        auto client = fixture.Runner->GetQueryClient();
        for (ui32 i = 0; i < 2; ++i) {
            const auto result =
                client.ExecuteQuery(TFixture::Query(), TTxControl::NoTx(), TExecuteQuerySettings().StatsMode(EStatsMode::Basic)).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT(result.GetStats() && result.GetStats()->GetCompilation());
            UNIT_ASSERT_VALUES_EQUAL(result.GetStats()->GetCompilation()->IsFromCache(), i != 0);
        }
        const auto oldRequests = fixture.Service.Requests.load();
        TEchoServer replacement;
        fixture.Scheme("DROP EXTERNAL DATA SOURCE `/Root/service`;");
        fixture.CreateConnection(replacement.Url());
        const auto result = client.ExecuteQuery(TFixture::Query(), TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(replacement.Requests.load(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Service.Requests.load(), oldRequests);
    }

    Y_UNIT_TEST(SecretRotationAffectsCachedPlan) {
        TFixture fixture;
        auto client = fixture.Runner->GetQueryClient();
        const auto first =
            client.ExecuteQuery(TFixture::Query(), TTxControl::NoTx(), TExecuteQuerySettings().StatsMode(EStatsMode::Full)).GetValueSync();
        UNIT_ASSERT_C(first.IsSuccess(), first.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Service.Authenticated.load(), 1);
        fixture.Scheme("ALTER SECRET `/Root/service_token` WITH (value='rotated-token');");
        const auto rotated =
            client.ExecuteQuery(TFixture::Query(), TTxControl::NoTx(), TExecuteQuerySettings().StatsMode(EStatsMode::Full)).GetValueSync();
        UNIT_ASSERT_C(rotated.IsSuccess(), rotated.GetIssues().ToString());
        UNIT_ASSERT(rotated.GetStats() && rotated.GetStats()->GetCompilation());
        UNIT_ASSERT(rotated.GetStats()->GetCompilation()->IsFromCache());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Service.Rotated.load(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Service.Authenticated.load(), 1);
        const auto plan = rotated.GetStats()->GetPlan();
        UNIT_ASSERT(plan);
        UNIT_ASSERT(plan->find("test-token") == std::string::npos);
        UNIT_ASSERT(plan->find("rotated-token") == std::string::npos);
    }

    Y_UNIT_TEST(ConnectionUseRequiresSelectRow) {
        TFixture fixture;
        fixture.Scheme("DROP EXTERNAL DATA SOURCE `/Root/service`;");
        fixture.Scheme(TStringBuilder() << "CREATE EXTERNAL DATA SOURCE `/Root/service` WITH (SOURCE_TYPE='ExternalService', "
                                        << "LOCATION='" << fixture.Service.Url() << "', PROTOCOL='HTTP', USE_TLS='false', AUTH_METHOD='NONE');");
        fixture.Scheme("GRANT 'ydb.database.connect', 'ydb.granular.describe_schema' ON `/Root` TO `reader@builtin`;");
        auto client = fixture.Runner->GetQueryClient(TClientSettings().AuthToken("reader@builtin"));
        const auto result = client.ExecuteQuery(TFixture::Query(), TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT(!result.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Service.Requests.load(), 0);
        fixture.Scheme("GRANT 'ydb.granular.select_row' ON `/Root/service` TO `reader@builtin`;");
        for (ui32 i = 0; i < 2; ++i) {
            const auto allowed = client.ExecuteQuery(TFixture::Query(), TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(allowed.IsSuccess(), allowed.GetIssues().ToString());
        }
        fixture.Scheme("REVOKE 'ydb.granular.select_row' ON `/Root/service` FROM `reader@builtin`;");
        const auto before = fixture.Service.Requests.load();
        const auto denied = client.ExecuteQuery(TFixture::Query(), TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT(!denied.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Service.Requests.load(), before);
    }
}
}   // namespace NKikimr::NKqp
