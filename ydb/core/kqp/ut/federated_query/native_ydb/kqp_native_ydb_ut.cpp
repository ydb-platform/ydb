#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/ut/federated_query/common/common.h>
#include <ydb/library/yql/providers/s3/actors/yql_s3_actors_factory_impl.h>
#include <ydb/core/security/certificate_check/test_utils/test_cert_auth_utils.h>
#include <library/cpp/testing/common/network.h>
#include <grpc/grpc_security.h>
#include <grpc/support/string_util.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {
namespace {

using namespace NYdb;
using namespace NYdb::NQuery;
using namespace NFederatedQueryTest;

// Each test runs in its own process. Install a generated test CA before the
// first TLS channel; no test certificate or private key is embedded in production.
class TTestTlsRoots {
public:
    explicit TTestTlsRoots(std::string certificate) {
        Certificate_ = std::move(certificate);
        grpc_set_ssl_roots_override_callback([](char** roots) {
            *roots = gpr_strdup(Certificate_.c_str());
            return GRPC_SSL_ROOTS_OVERRIDE_OK;
        });
    }

    ~TTestTlsRoots() {
        grpc_set_ssl_roots_override_callback(nullptr);
    }

private:
    inline static std::string Certificate_;
};

struct TNativeYdbFixture {
    TKikimrRunner Remote{TKikimrSettings().SetDomainRoot("Remote").SetWithSampleTables(false).SetAuthToken("root@builtin")};
    std::shared_ptr<TKikimrRunner> Consumer;

    explicit TNativeYdbFixture(bool enabled = true, const TString& token = "root@builtin",
                               const TString& hostnamePattern = {}, bool createSource = true) {
        NKikimrConfig::TAppConfig config;
        config.MutableFeatureFlags()->SetEnableNativeYdbProvider(enabled);
        config.MutableQueryServiceConfig()->SetAllExternalDataSourcesAreAvailable(false);
        config.MutableQueryServiceConfig()->AddAvailableExternalDataSources("Ydb");
        if (hostnamePattern) {
            config.MutableQueryServiceConfig()->AddHostnamePatterns(hostnamePattern);
        }
        // No ConnectorClient is provided, even when testing the disabled native flag.
        Consumer = MakeKikimrRunner(false, nullptr, nullptr, config,
            NYql::NDq::CreateS3ActorsFactory(),
            {.DomainRoot = "Consumer", .CredentialsFactory = CreateCredentialsFactory("root@builtin"), .AuthToken = "root@builtin"});

        auto remote = Remote.GetQueryClient();
        const auto create = remote.ExecuteQuery(
            "CREATE TABLE `/Remote/items` (Key Uint64 NOT NULL, Value Utf8, Flag Bool, PRIMARY KEY (Key));",
            TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(create.IsSuccess(), create.GetIssues().ToString());

        auto consumer = Consumer->GetQueryClient();
        const auto secret = consumer.ExecuteQuery(
            TStringBuilder() << "CREATE SECRET remote_token WITH (value = '" << token << "');",
            TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(secret.IsSuccess(), secret.GetIssues().ToString());

        if (createSource) {
            const auto eds = CreateSource(Remote.GetEndpoint(), false);
            UNIT_ASSERT_C(eds.IsSuccess(), eds.GetIssues().ToString());
        }
    }

    TExecuteQueryResult CreateSource(const TString& endpoint, bool tls, const TString& name = "remote_db") {
        const TString source = TStringBuilder()
            << "CREATE EXTERNAL DATA SOURCE " << name << " WITH (SOURCE_TYPE='Ydb', LOCATION='"
            << endpoint << "', DATABASE_NAME='/Remote', USE_TLS='" << (tls ? "true" : "false") << "', "
            << "AUTH_METHOD='TOKEN', TOKEN_SECRET_PATH='remote_token');";
        return Consumer->GetQueryClient().ExecuteQuery(source, TTxControl::NoTx()).ExtractValueSync();
    }

    void Scheme(const TString& sql, bool remote = false) {
        auto client = remote ? Remote.GetQueryClient() : Consumer->GetQueryClient();
        const auto result = client.ExecuteQuery(sql, TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    void Populate() {
        auto remote = Remote.GetQueryClient();
        const auto result = remote.ExecuteQuery(
            "UPSERT INTO `/Remote/items` (Key, Value, Flag) VALUES (1u, 'one', true), (2u, 'two', false), (3u, NULL, NULL);",
            TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    TExecuteQueryResult Read(const TString& sql) {
        return Consumer->GetQueryClient().ExecuteQuery(sql, TTxControl::BeginTx().CommitTx(),
            TExecuteQuerySettings().ClientTimeout(TDuration::Seconds(30))).ExtractValueSync();
    }
};

} // namespace

Y_UNIT_TEST_SUITE(KqpNativeYdb) {
    Y_UNIT_TEST(ReadWithoutConnector) {
        TNativeYdbFixture fixture;
        fixture.Populate();
        const auto result = fixture.Read("SELECT Key, Value FROM remote_db.`items` ORDER BY Key;");
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1);
        auto rows = result.GetResultSetParser(0);
        UNIT_ASSERT_VALUES_EQUAL(rows.RowsCount(), 3);
        UNIT_ASSERT(rows.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(rows.ColumnParser("Key").GetUint64(), 1);
        UNIT_ASSERT_VALUES_EQUAL(*rows.ColumnParser("Value").GetOptionalUtf8(), "one");
        UNIT_ASSERT(rows.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(rows.ColumnParser("Key").GetUint64(), 2);
        UNIT_ASSERT_VALUES_EQUAL(*rows.ColumnParser("Value").GetOptionalUtf8(), "two");
        UNIT_ASSERT(rows.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(rows.ColumnParser("Key").GetUint64(), 3);
        UNIT_ASSERT(!rows.ColumnParser("Value").GetOptionalUtf8());
        UNIT_ASSERT(!rows.TryNextRow());
    }

    Y_UNIT_TEST(FilterAndProjectionStayCorrectLocally) {
        TNativeYdbFixture fixture;
        fixture.Populate();
        const auto result = fixture.Read(
            "SELECT Value FROM remote_db.`items` WHERE Key = 2u;");
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        auto rows = result.GetResultSetParser(0);
        UNIT_ASSERT_VALUES_EQUAL(rows.ColumnsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(rows.RowsCount(), 1);
        UNIT_ASSERT(rows.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(*rows.ColumnParser("Value").GetOptionalUtf8(), "two");
    }

    Y_UNIT_TEST(BoolAndNullMatchQueryServiceFormat) {
        TNativeYdbFixture fixture;
        fixture.Populate();
        const auto result = fixture.Read("SELECT Flag FROM remote_db.`items` ORDER BY Key;");
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        auto rows = result.GetResultSetParser(0);
        UNIT_ASSERT_VALUES_EQUAL(rows.RowsCount(), 3);
        UNIT_ASSERT(rows.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(rows.ColumnParser("Flag").GetOptionalBool().value(), true);
        UNIT_ASSERT(rows.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(rows.ColumnParser("Flag").GetOptionalBool().value(), false);
        UNIT_ASSERT(rows.TryNextRow());
        UNIT_ASSERT(!rows.ColumnParser("Flag").GetOptionalBool());
    }

    Y_UNIT_TEST(EmptyTableCompletes) {
        TNativeYdbFixture fixture;
        const auto result = fixture.Read("SELECT * FROM remote_db.`items`;");
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(0).RowsCount(), 0);
    }

    Y_UNIT_TEST(CountWithoutProjectedColumns) {
        TNativeYdbFixture fixture;
        fixture.Populate();
        const auto result = fixture.Read("SELECT COUNT(*) AS Total FROM remote_db.`items`;");
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        auto rows = result.GetResultSetParser(0);
        UNIT_ASSERT_VALUES_EQUAL(rows.RowsCount(), 1);
        UNIT_ASSERT(rows.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(rows.ColumnParser("Total").GetUint64(), 3);
    }

    Y_UNIT_TEST(MissingRemoteTableFails) {
        TNativeYdbFixture fixture;
        const auto result = fixture.Read("SELECT * FROM remote_db.`does_not_exist`;");
        UNIT_ASSERT(!result.IsSuccess());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "DescribeTable failed");
    }

    Y_UNIT_TEST(DisabledFlagDoesNotSilentlyUseNative) {
        TNativeYdbFixture fixture(false);
        const auto result = fixture.Read("SELECT * FROM remote_db.`items`;");
        UNIT_ASSERT(!result.IsSuccess());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "generic");
    }

    Y_UNIT_TEST(TokenAndTlsReadWithoutConnector) {
        const auto ca = NCertTestUtils::GenerateCA(NCertTestUtils::TProps::AsCA().WithValid(TDuration::Days(1)));
        const auto certificate = NCertTestUtils::GenerateSignedCert(ca,
            NCertTestUtils::TProps::AsServer().WithValid(TDuration::Days(1)));
        TTestTlsRoots roots(ca.Certificate);
        TNativeYdbFixture fixture(true, "root@builtin", {}, false);
        fixture.Populate();
        const auto port = NTesting::GetFreePort();
        fixture.Remote.GetTestServer().EnableGRpc(NYdbGrpc::TServerOptions()
            .SetHost("localhost").SetPort(port)
            .SetSslData(NYdbGrpc::TSslData{
                .Cert = TString(certificate.Certificate),
                .Key = TString(certificate.PrivateKey),
                .Root = TString(ca.Certificate),
            }));
        const auto created = fixture.CreateSource(TStringBuilder() << "localhost:" << port, true);
        UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
        const auto result = fixture.Read("SELECT COUNT(*) AS Total FROM remote_db.`items`;");
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        auto rows = result.GetResultSetParser(0);
        UNIT_ASSERT(rows.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(rows.ColumnParser("Total").GetUint64(), 3);
    }

    Y_UNIT_TEST(TlsSourceCannotReuseWarmedPlaintextProviderChannel) {
        TNativeYdbFixture fixture;
        fixture.Populate();
        const auto plain = fixture.Read("SELECT COUNT(*) AS Total FROM remote_db.`items`;");
        UNIT_ASSERT_C(plain.IsSuccess(), plain.GetIssues().ToString());
        const auto created = fixture.CreateSource(fixture.Remote.GetEndpoint(), true, "remote_tls");
        UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
        // Both EDS entries use one federated setup and the same endpoint. The
        // metadata driver must keep TLS separate from the warmed plaintext cache.
        const auto tls = fixture.Consumer->GetQueryClient().ExecuteQuery(
            "SELECT COUNT(*) AS Total FROM remote_tls.`items`;", TTxControl::BeginTx().CommitTx(),
            TExecuteQuerySettings().ClientTimeout(TDuration::Seconds(3))).ExtractValueSync();
        UNIT_ASSERT(!tls.IsSuccess());
        const auto again = fixture.Read("SELECT COUNT(*) AS Total FROM remote_db.`items`;");
        UNIT_ASSERT_C(again.IsSuccess(), again.GetIssues().ToString());
        auto rows = again.GetResultSetParser(0);
        UNIT_ASSERT(rows.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(rows.ColumnParser("Total").GetUint64(), 3);
    }

    Y_UNIT_TEST(ExternalDataSourceAccessIsChecked) {
        TNativeYdbFixture fixture;
        fixture.Scheme("GRANT 'ydb.database.connect' ON `/Consumer` TO `reader@builtin`;");
        auto client = fixture.Consumer->GetQueryClient(TClientSettings().AuthToken("reader@builtin"));
        const auto result = client.ExecuteQuery("SELECT * FROM remote_db.`items`;",
            TTxControl::BeginTx().CommitTx(), TExecuteQuerySettings().ClientTimeout(TDuration::Seconds(30))).ExtractValueSync();
        // Schema lookup deliberately hides whether an inaccessible EDS exists.
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "access permissions");
    }

    Y_UNIT_TEST(SecretAccessIsChecked) {
        TNativeYdbFixture fixture;
        fixture.Scheme("GRANT ALL ON `/Consumer` TO `reader@builtin`;");
        auto client = fixture.Consumer->GetQueryClient(TClientSettings().AuthToken("reader@builtin"));
        const auto result = client.ExecuteQuery("SELECT * FROM remote_db.`items`;",
            TTxControl::BeginTx().CommitTx(), TExecuteQuerySettings().ClientTimeout(TDuration::Seconds(30))).ExtractValueSync();
        UNIT_ASSERT(!result.IsSuccess());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "secret");
        UNIT_ASSERT(result.GetIssues().ToString().find("root@builtin") == std::string::npos);
    }

    Y_UNIT_TEST(RemoteTableAccessIsChecked) {
        TNativeYdbFixture fixture(true, "restricted@builtin");
        fixture.Populate();
        fixture.Scheme("GRANT 'ydb.database.connect', 'ydb.granular.describe_schema' ON `/Remote` TO `restricted@builtin`;", true);
        const auto result = fixture.Read("SELECT * FROM remote_db.`items`;");
        UNIT_ASSERT(!result.IsSuccess());
        // Remote KQP wraps the resolver's AccessDenied in ABORTED. The source
        // preserves the status while withholding remote issues and credentials.
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(),
            TStringBuilder() << "YdbRemote query failed with status " << static_cast<size_t>(EStatus::ABORTED));
        UNIT_ASSERT(result.GetIssues().ToString().find("restricted@builtin") == std::string::npos);

        fixture.Scheme("GRANT 'ydb.granular.select_row' ON `/Remote/items` TO `restricted@builtin`;", true);
        const auto allowed = fixture.Read("SELECT COUNT(*) AS Total FROM remote_db.`items`;");
        UNIT_ASSERT_C(allowed.IsSuccess(), allowed.GetIssues().ToString());
        auto rows = allowed.GetResultSetParser(0);
        UNIT_ASSERT(rows.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(rows.ColumnParser("Total").GetUint64(), 3);
    }

    Y_UNIT_TEST(HostnameAllowlistIsChecked) {
        TNativeYdbFixture fixture(true, "root@builtin", "allowed-host.invalid", false);
        const auto result = fixture.CreateSource(fixture.Remote.GetEndpoint(), false);
        UNIT_ASSERT(!result.IsSuccess());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "host");
    }

    Y_UNIT_TEST(WritesAreRejected) {
        TNativeYdbFixture fixture;
        for (const TString mode : {"INSERT", "UPSERT"}) {
            const auto result = fixture.Read(
                mode + " INTO remote_db.`items` (Key, Value) VALUES (1u, 'one');");
            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "not supported");
        }
    }
}

} // namespace NKikimr::NKqp
