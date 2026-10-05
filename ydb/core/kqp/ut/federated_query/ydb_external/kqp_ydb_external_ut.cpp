#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/ut/federated_query/common/common.h>
#include <ydb/library/yql/providers/s3/actors/yql_s3_actors_factory_impl.h>
#include <ydb/core/security/certificate_check/test_utils/test_cert_auth_utils.h>
#include <library/cpp/testing/common/network.h>
#include <grpc/grpc_security.h>
#include <grpc/support/string_util.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/json/json_reader.h>

namespace NKikimr::NKqp {
namespace {

using namespace NYdb;
using namespace NYdb::NQuery;
using namespace NFederatedQueryTest;

// gRPC caches default roots once per process. All fixtures use one generated CA,
// including when FORK_SUBTESTS puts several TLS scenarios in the same process.
class TTestTlsRoots {
public:
    static const NCertTestUtils::TCertAndKey& GetCa() {
        static const TTestTlsRoots roots;
        return roots.Ca_;
    }

private:
    TTestTlsRoots()
        : Ca_(NCertTestUtils::GenerateCA(NCertTestUtils::TProps::AsCA().WithValid(TDuration::Days(1))))
    {
        grpc_set_ssl_roots_override_callback([](char** roots) {
            *roots = gpr_strdup(GetCa().Certificate.c_str());
            return GRPC_SSL_ROOTS_OVERRIDE_OK;
        });
    }

    const NCertTestUtils::TCertAndKey Ca_;
};

struct TYdbExternalFixture {
    // Initialize before constructing any drivers, even for plaintext fixtures
    // that later try TLS against a warmed endpoint.
    const NCertTestUtils::TCertAndKey& TrustedCa = TTestTlsRoots::GetCa();
    TKikimrRunner Remote{TKikimrSettings().SetDomainRoot("Remote").SetWithSampleTables(false).SetAuthToken("root@builtin")};
    const TString SourceType;
    std::shared_ptr<TKikimrRunner> Consumer;

    explicit TYdbExternalFixture(const TString& sourceType = "YdbExternal", const TString& token = "root@builtin",
                               const TString& hostnamePattern = {}, bool createSource = true,
                               bool enableLookup = false,
                               const std::set<TString>& available = {"Ydb", "YdbExternal"})
        : SourceType(sourceType)
    {
        NKikimrConfig::TAppConfig config;
        config.MutableTableServiceConfig()->SetEnableDqSourceStreamLookupJoin(enableLookup);
        config.MutableQueryServiceConfig()->SetAllExternalDataSourcesAreAvailable(false);
        for (const auto& type : available) {
            config.MutableQueryServiceConfig()->AddAvailableExternalDataSources(type);
        }
        if (hostnamePattern) {
            config.MutableQueryServiceConfig()->AddHostnamePatterns(hostnamePattern);
        }
        // No ConnectorClient is provided, including when checking legacy routing.
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
            << "CREATE EXTERNAL DATA SOURCE " << name << " WITH (SOURCE_TYPE='" << SourceType << "', LOCATION='"
            << endpoint << "', DATABASE_NAME='/Remote', USE_TLS='" << (tls ? "true" : "false") << "', "
            << "AUTH_METHOD='TOKEN', TOKEN_SECRET_PATH='remote_token'"
            << ");";
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

    NJson::TJsonValue ExplainSource(const TString& sql) {
        const auto result = Consumer->GetQueryClient().ExecuteQuery(sql, TTxControl::BeginTx().CommitTx(),
            TExecuteQuerySettings().ExecMode(EExecMode::Explain).ClientTimeout(TDuration::Seconds(30))).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT(result.GetStats());
        UNIT_ASSERT(result.GetStats()->GetPlan());
        NJson::TJsonValue plan;
        UNIT_ASSERT(NJson::ReadJsonTree(*result.GetStats()->GetPlan(), &plan));
        auto source = FindPlanNodeByKv(plan, "SourceType", "YdbExternal");
        UNIT_ASSERT_C(source.IsDefined(), *result.GetStats()->GetPlan());
        return source;
    }
};

void AssertMetadataConnectionFailure(const TExecuteQueryResult& result) {
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
    const TString issues = result.GetIssues().ToString();
    UNIT_ASSERT_STRING_CONTAINS(issues, "YdbExternal metadata session failed: TRANSPORT_UNAVAILABLE");
    UNIT_ASSERT(!issues.Contains("root@builtin"));
}

void CheckTlsCertificateRejected(bool wrongHostname) {
    TYdbExternalFixture fixture("YdbExternal", "root@builtin", {}, false);
    // A hostname mismatch must use the trusted issuer so it cannot accidentally
    // pass by rejecting an unrelated issuer left in gRPC's process-wide cache.
    const auto ca = wrongHostname ? fixture.TrustedCa
        : NCertTestUtils::GenerateCA(NCertTestUtils::TProps::AsCA().WithValid(TDuration::Days(1)));
    auto properties = NCertTestUtils::TProps::AsServer().WithValid(TDuration::Days(1));
    if (wrongHostname) {
        properties.CommonName = "wrong-host.invalid";
        properties.AltNames = {"DNS:wrong-host.invalid"};
    }
    const auto certificate = NCertTestUtils::GenerateSignedCert(ca, properties);
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
    const auto result = fixture.Consumer->GetQueryClient().ExecuteQuery(
        "SELECT COUNT(*) AS Total FROM remote_db.`items`;", TTxControl::BeginTx().CommitTx(),
        // Allow the provider's 60 s metadata budget to finish before the client.
        TExecuteQuerySettings().ClientTimeout(TDuration::Seconds(75))).ExtractValueSync();
    AssertMetadataConnectionFailure(result);
}

} // namespace

Y_UNIT_TEST_SUITE(KqpYdbExternal) {
    Y_UNIT_TEST(ReadWithoutConnector) {
        TYdbExternalFixture fixture;
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

    Y_UNIT_TEST(MultipleArrowPartsPreserveAggregateRowsAndBytes) {
        TYdbExternalFixture fixture;
        fixture.Scheme("CREATE TABLE `/Remote/many_rows` (Key Uint64 NOT NULL, Value String, PRIMARY KEY(Key));", true);
        const auto populated = fixture.Remote.GetQueryClient().ExecuteQuery(R"(
            $rows = ListMap(ListFromRange(0ul, 20000ul), ($key) -> (
                AsStruct($key AS Key, ListConcat(ListReplicate("abcdefgh", 25)) AS Value)));
            UPSERT INTO `/Remote/many_rows` SELECT * FROM AS_TABLE($rows);
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(populated.IsSuccess(), populated.GetIssues().ToString());
        // Several server chunks cross the soft 1 MiB threshold. All rows and
        // bytes must survive local splitting before this consumer-side aggregate.
        const auto result = fixture.Read(R"(
            SELECT COUNT(*) AS Total, SUM(Key) AS Keys, SUM(LENGTH(Value)) AS Bytes
            FROM remote_db.`many_rows`;
        )");
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        auto rows = result.GetResultSetParser(0);
        UNIT_ASSERT_VALUES_EQUAL(rows.RowsCount(), 1);
        UNIT_ASSERT(rows.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(rows.ColumnParser("Total").GetUint64(), 20000);
        UNIT_ASSERT_VALUES_EQUAL(rows.ColumnParser("Keys").GetOptionalUint64().value(), 199990000);
        UNIT_ASSERT_VALUES_EQUAL(rows.ColumnParser("Bytes").GetOptionalUint64().value(), 4000000);
    }

    Y_UNIT_TEST(SingleValuesOfTwoAndSixteenMiBAreReadable) {
        TYdbExternalFixture fixture;
        fixture.Scheme("CREATE TABLE `/Remote/large_rows` (Key Uint64 NOT NULL, Value String, PRIMARY KEY(Key));", true);
        for (ui64 size : {2 * 1024 * 1024, 16 * 1024 * 1024}) {
            auto params = TParamsBuilder().AddParam("$key").Uint64(size).Build()
                .AddParam("$value").String(std::string(size, 'x')).Build().Build();
            const auto populated = fixture.Remote.GetQueryClient().ExecuteQuery(R"(
                DECLARE $key AS Uint64;
                DECLARE $value AS String;
                UPSERT INTO `/Remote/large_rows` (Key, Value) VALUES ($key, $value);
            )", TTxControl::BeginTx().CommitTx(), params).ExtractValueSync();
            UNIT_ASSERT_C(populated.IsSuccess(), populated.GetIssues().ToString());
        }
        const auto result = fixture.Read(R"(
            SELECT Key, CAST(LENGTH(Value) AS Uint64) AS Bytes FROM remote_db.`large_rows` ORDER BY Key;
        )");
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        auto rows = result.GetResultSetParser(0);
        UNIT_ASSERT_VALUES_EQUAL(rows.RowsCount(), 2);
        for (ui64 size : {2 * 1024 * 1024, 16 * 1024 * 1024}) {
            UNIT_ASSERT(rows.TryNextRow());
            UNIT_ASSERT_VALUES_EQUAL(rows.ColumnParser("Key").GetUint64(), size);
            UNIT_ASSERT_VALUES_EQUAL(rows.ColumnParser("Bytes").GetOptionalUint64().value(), size);
        }
        UNIT_ASSERT(!rows.TryNextRow());
    }

    Y_UNIT_TEST(FilterAndProjectionStayCorrectLocally) {
        TYdbExternalFixture fixture;
        fixture.Populate();
        const auto result = fixture.Read(
            "SELECT Value FROM remote_db.`items` WHERE Key = 2u;");
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        auto rows = result.GetResultSetParser(0);
        UNIT_ASSERT_VALUES_EQUAL(rows.ColumnsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(rows.RowsCount(), 1);
        UNIT_ASSERT(rows.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(*rows.ColumnParser("Value").GetOptionalUtf8(), "two");
        const auto source = fixture.ExplainSource("SELECT Value FROM remote_db.`items` WHERE Key = 2u;");
        const auto& columns = source["ReadColumns"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(columns.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(columns[0].GetStringSafe(), "Key");
        UNIT_ASSERT_VALUES_EQUAL(columns[1].GetStringSafe(), "Value");
    }

    Y_UNIT_TEST(ProjectionAndCountPruneTheRemoteSource) {
        TYdbExternalFixture fixture;
        fixture.Populate();
        const auto projected = fixture.ExplainSource("SELECT Key FROM remote_db.`items` LIMIT 2;");
        const auto& projectedColumns = projected["ReadColumns"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(projectedColumns.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(projectedColumns.front().GetStringSafe(), "Key");
        UNIT_ASSERT_VALUES_EQUAL(projected["ReadTimeoutMs"].GetUIntegerSafe(), 60000);

        const auto count = fixture.ExplainSource("SELECT COUNT(*) AS Total FROM remote_db.`items`;");
        const auto& countColumns = count["ReadColumns"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(countColumns.size(), 1);
        // The first field in the canonical row type is the physical carrier.
        UNIT_ASSERT_VALUES_EQUAL(countColumns.front().GetStringSafe(), "Flag");
        const auto result = fixture.Read("SELECT COUNT(*) AS Total FROM remote_db.`items`;");
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        auto rows = result.GetResultSetParser(0);
        UNIT_ASSERT(rows.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(rows.ColumnParser("Total").GetUint64(), 3);
    }

    Y_UNIT_TEST(ExternalSourceReadTimeoutIsRejectedForBothTypes) {
        for (const auto* type : {"Ydb", "YdbExternal"}) {
            // The fixture first creates a source without a timeout option.
            TYdbExternalFixture fixture(type);
            for (const auto* value : {"60000", "120000", "0", "60s"}) {
                const TString sql = TStringBuilder()
                    << "CREATE EXTERNAL DATA SOURCE remote_timeout WITH (SOURCE_TYPE='" << type << "', LOCATION='"
                    << fixture.Remote.GetEndpoint() << "', DATABASE_NAME='/Remote', "
                    << "AUTH_METHOD='TOKEN', TOKEN_SECRET_PATH='remote_token', READ_TIMEOUT_MS='" << value << "');";
                const auto rejected = fixture.Consumer->GetQueryClient().ExecuteQuery(sql, TTxControl::NoTx()).ExtractValueSync();
                UNIT_ASSERT(!rejected.IsSuccess());
                UNIT_ASSERT_STRING_CONTAINS(rejected.GetIssues().ToString(), "Unknown property: read_timeout_ms");
            }
        }
    }

    Y_UNIT_TEST(StreamLookupFailsWithControlledQueryIssue) {
        TYdbExternalFixture fixture("YdbExternal", "root@builtin", {}, true, true);
        fixture.Populate();
        fixture.Scheme("CREATE TABLE local_items (Key Uint64 NOT NULL, PRIMARY KEY (Key));");
        const auto inserted = fixture.Read("UPSERT INTO local_items (Key) VALUES (1u);");
        UNIT_ASSERT_C(inserted.IsSuccess(), inserted.GetIssues().ToString());
        const auto ordinary = fixture.Read(R"(
            SELECT r.Value AS Value FROM local_items AS l
            LEFT JOIN ANY remote_db.`items` AS r ON l.Key = r.Key;
        )");
        UNIT_ASSERT_C(ordinary.IsSuccess(), ordinary.GetIssues().ToString());
        auto rows = ordinary.GetResultSetParser(0);
        UNIT_ASSERT(rows.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(rows.ColumnParser("Value").GetOptionalUtf8().value(), "one");
        const auto result = fixture.Read(R"(
            SELECT r.Value AS Value FROM local_items AS l
            LEFT JOIN /*+ streamlookup() */ ANY remote_db.`items` AS r ON l.Key = r.Key;
        )");
        UNIT_ASSERT(!result.IsSuccess());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "YdbExternal streamlookup joins are not supported");
        UNIT_ASSERT(result.GetIssues().ToString().find("yql_dq_integration_impl.cpp") == std::string::npos);
    }

    Y_UNIT_TEST(BoolAndNullMatchQueryServiceFormat) {
        TYdbExternalFixture fixture;
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
        TYdbExternalFixture fixture;
        const auto result = fixture.Read("SELECT * FROM remote_db.`items`;");
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(0).RowsCount(), 0);
    }

    Y_UNIT_TEST(CountWithoutProjectedColumns) {
        TYdbExternalFixture fixture;
        fixture.Populate();
        const auto result = fixture.Read("SELECT COUNT(*) AS Total FROM remote_db.`items`;");
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        auto rows = result.GetResultSetParser(0);
        UNIT_ASSERT_VALUES_EQUAL(rows.RowsCount(), 1);
        UNIT_ASSERT(rows.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(rows.ColumnParser("Total").GetUint64(), 3);
    }

    Y_UNIT_TEST(MissingRemoteTableFails) {
        TYdbExternalFixture fixture;
        const auto result = fixture.Read("SELECT * FROM remote_db.`does_not_exist`;");
        UNIT_ASSERT(!result.IsSuccess());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "DescribeTable failed");
    }

    Y_UNIT_TEST(LegacyYdbStillRequiresConnector) {
        TYdbExternalFixture fixture("Ydb");
        const auto result = fixture.Read("SELECT * FROM remote_db.`items`;");
        UNIT_ASSERT(!result.IsSuccess());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "generic");
    }

    Y_UNIT_TEST(ExternalAvailabilityDoesNotFollowLegacyYdb) {
        TYdbExternalFixture fixture("YdbExternal", "root@builtin", {}, false, false, {"Ydb"});
        const auto rejected = fixture.CreateSource(fixture.Remote.GetEndpoint(), false);
        UNIT_ASSERT(!rejected.IsSuccess());
        UNIT_ASSERT_STRING_CONTAINS(rejected.GetIssues().ToString(), "YdbExternal is disabled");
        const auto local = fixture.Read("SELECT 1u;");
        UNIT_ASSERT_C(local.IsSuccess(), local.GetIssues().ToString());
    }

    Y_UNIT_TEST(ExternalReadsWithOnlyItsOwnAvailability) {
        TYdbExternalFixture fixture("YdbExternal", "root@builtin", {}, true, false, {"YdbExternal"});
        fixture.Populate();
        const auto result = fixture.Read("SELECT Key FROM remote_db.`items`;");
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(0).RowsCount(), 3);
        const auto legacy = fixture.Consumer->GetQueryClient().ExecuteQuery(
            TStringBuilder() << "CREATE EXTERNAL DATA SOURCE legacy_db WITH (SOURCE_TYPE='Ydb', LOCATION='"
                << fixture.Remote.GetEndpoint() << "', DATABASE_NAME='/Remote', AUTH_METHOD='NONE');",
            TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT(!legacy.IsSuccess());
        UNIT_ASSERT_STRING_CONTAINS(legacy.GetIssues().ToString(), "Ydb is disabled");
    }

    Y_UNIT_TEST(ExternalDdlRejectsUnsupportedPropertiesAndAuthentication) {
        TYdbExternalFixture fixture;
        const auto reject = [&](const TString& settings, const TString& expected) {
            const auto result = fixture.Consumer->GetQueryClient().ExecuteQuery(
                TStringBuilder() << "CREATE EXTERNAL DATA SOURCE invalid_db WITH (SOURCE_TYPE='YdbExternal', LOCATION='"
                    << fixture.Remote.GetEndpoint() << "', " << settings << ");",
                TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT(!result.IsSuccess());
            TString issues(result.GetIssues().ToString());
            issues.to_lower();
            auto expectedLower = expected;
            expectedLower.to_lower();
            UNIT_ASSERT_STRING_CONTAINS(issues, expectedLower);
        };
        for (const auto* property : {"DATABASE_ID", "MDB_CLUSTER_ID", "SHARED_READING_GROUP"}) {
            reject(TStringBuilder() << "DATABASE_NAME='/Remote', AUTH_METHOD='NONE', " << property << "='unsupported'",
                "Unsupported property");
        }
        // SHARED_READING may be rejected by the generic DDL gate before source validation.
        reject("DATABASE_NAME='/Remote', AUTH_METHOD='NONE', SHARED_READING='true'", "SHARED_READING");
        reject("AUTH_METHOD='NONE'", "requires an absolute DATABASE_NAME");
        reject("DATABASE_NAME='Remote', AUTH_METHOD='NONE'", "requires an absolute DATABASE_NAME");
        reject("DATABASE_NAME='/Remote', USE_TLS='yes', AUTH_METHOD='NONE'", "USE_TLS must be true or false");
        reject("DATABASE_NAME='/Remote', AUTH_METHOD='BASIC', LOGIN='user', PASSWORD_SECRET_PATH='remote_token'",
            "BASIC isn't supported for this source type");
        reject("DATABASE_NAME='/Remote', AUTH_METHOD='SERVICE_ACCOUNT', SERVICE_ACCOUNT_ID='id', SERVICE_ACCOUNT_SECRET_PATH='remote_token'",
            "SERVICE_ACCOUNT isn't supported for this source type");
        // NONE is a valid definition even when the remote server requires a token for reads.
        fixture.Scheme(TStringBuilder() << "CREATE EXTERNAL DATA SOURCE no_auth WITH (SOURCE_TYPE='YdbExternal', LOCATION='"
            << fixture.Remote.GetEndpoint() << "', DATABASE_NAME='/Remote', AUTH_METHOD='NONE');");
    }

    Y_UNIT_TEST(TokenAndTlsReadWithoutConnector) {
        TYdbExternalFixture fixture("YdbExternal", "root@builtin", {}, false);
        const auto& ca = fixture.TrustedCa;
        const auto certificate = NCertTestUtils::GenerateSignedCert(ca,
            NCertTestUtils::TProps::AsServer().WithValid(TDuration::Days(1)));
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

    Y_UNIT_TEST(TlsRejectsAnUntrustedCertificate) {
        CheckTlsCertificateRejected(false);
    }

    Y_UNIT_TEST(TlsRejectsCertificateHostnameMismatch) {
        CheckTlsCertificateRejected(true);
    }

    Y_UNIT_TEST(TlsSourceCannotReuseWarmedPlaintextProviderChannel) {
        TYdbExternalFixture fixture;
        fixture.Populate();
        const auto plain = fixture.Read("SELECT COUNT(*) AS Total FROM remote_db.`items`;");
        UNIT_ASSERT_C(plain.IsSuccess(), plain.GetIssues().ToString());
        const auto created = fixture.CreateSource(fixture.Remote.GetEndpoint(), true, "remote_tls");
        UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
        // Both EDS entries use one federated setup and the same endpoint. The
        // metadata driver must keep TLS separate from the warmed plaintext cache.
        const auto tls = fixture.Consumer->GetQueryClient().ExecuteQuery(
            "SELECT COUNT(*) AS Total FROM remote_tls.`items`;", TTxControl::BeginTx().CommitTx(),
            TExecuteQuerySettings().ClientTimeout(TDuration::Seconds(75))).ExtractValueSync();
        AssertMetadataConnectionFailure(tls);
        const auto again = fixture.Read("SELECT COUNT(*) AS Total FROM remote_db.`items`;");
        UNIT_ASSERT_C(again.IsSuccess(), again.GetIssues().ToString());
        auto rows = again.GetResultSetParser(0);
        UNIT_ASSERT(rows.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(rows.ColumnParser("Total").GetUint64(), 3);
    }

    Y_UNIT_TEST(ExternalDataSourceAccessIsChecked) {
        TYdbExternalFixture fixture;
        fixture.Scheme("GRANT 'ydb.database.connect' ON `/Consumer` TO `reader@builtin`;");
        auto client = fixture.Consumer->GetQueryClient(TClientSettings().AuthToken("reader@builtin"));
        const auto result = client.ExecuteQuery("SELECT * FROM remote_db.`items`;",
            TTxControl::BeginTx().CommitTx(), TExecuteQuerySettings().ClientTimeout(TDuration::Seconds(30))).ExtractValueSync();
        // Schema lookup deliberately hides whether an inaccessible EDS exists.
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "access permissions");
    }

    Y_UNIT_TEST(SecretAccessIsChecked) {
        TYdbExternalFixture fixture;
        fixture.Scheme("GRANT ALL ON `/Consumer` TO `reader@builtin`;");
        auto client = fixture.Consumer->GetQueryClient(TClientSettings().AuthToken("reader@builtin"));
        const auto result = client.ExecuteQuery("SELECT * FROM remote_db.`items`;",
            TTxControl::BeginTx().CommitTx(), TExecuteQuerySettings().ClientTimeout(TDuration::Seconds(30))).ExtractValueSync();
        UNIT_ASSERT(!result.IsSuccess());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "secret");
        UNIT_ASSERT(result.GetIssues().ToString().find("root@builtin") == std::string::npos);
    }

    Y_UNIT_TEST(RemoteTableAccessIsChecked) {
        TYdbExternalFixture fixture("YdbExternal", "restricted@builtin");
        fixture.Populate();
        fixture.Scheme("GRANT 'ydb.database.connect', 'ydb.granular.describe_schema' ON `/Remote` TO `restricted@builtin`;", true);
        const auto result = fixture.Read("SELECT * FROM remote_db.`items`;");
        UNIT_ASSERT(!result.IsSuccess());
        // Remote KQP wraps the resolver's AccessDenied in ABORTED. The source
        // preserves the status while withholding remote issues and credentials.
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(),
            "YdbExternal query failed with status ABORTED");
        UNIT_ASSERT(result.GetIssues().ToString().find("restricted@builtin") == std::string::npos);

        fixture.Scheme("GRANT 'ydb.granular.select_row' ON `/Remote/items` TO `restricted@builtin`;", true);
        const auto allowed = fixture.Read("SELECT COUNT(*) AS Total FROM remote_db.`items`;");
        UNIT_ASSERT_C(allowed.IsSuccess(), allowed.GetIssues().ToString());
        auto rows = allowed.GetResultSetParser(0);
        UNIT_ASSERT(rows.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(rows.ColumnParser("Total").GetUint64(), 3);
    }

    Y_UNIT_TEST(HostnameAllowlistIsChecked) {
        TYdbExternalFixture fixture("YdbExternal", "root@builtin", "allowed-host.invalid", false);
        const auto result = fixture.CreateSource(fixture.Remote.GetEndpoint(), false);
        UNIT_ASSERT(!result.IsSuccess());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "host");
    }

    Y_UNIT_TEST(WritesAreRejected) {
        TYdbExternalFixture fixture;
        for (const TString mode : {"INSERT", "UPSERT"}) {
            const auto result = fixture.Read(
                mode + " INTO remote_db.`items` (Key, Value) VALUES (1u, 'one');");
            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "not supported");
        }
    }
}

} // namespace NKikimr::NKqp
