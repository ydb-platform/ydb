#include <ydb/public/lib/ydb_cli/dump/util/external_data_source_utils.h>
#include <ydb/public/api/protos/ydb_table.pb.h>

#include <library/cpp/testing/unittest/registar.h>
#include <yql/essentials/public/issue/yql_issue.h>
#include <google/protobuf/unknown_field_set.h>

namespace NYdb::NDump {
namespace {

Ydb::Table::DescribeExternalDataSourceResult Source(const TString& provider, const TString& auth) {
    Ydb::Table::DescribeExternalDataSourceResult source;
    source.set_source_type(provider);
    source.set_location("remote.example.test:2135");
    (*source.mutable_properties())["AUTH_METHOD"] = auth;
    (*source.mutable_properties())["DATABASE_NAME"] = "/Remote/database";
    return source;
}

bool Supported(const Ydb::Table::DescribeExternalDataSourceResult& source) {
    NYql::TIssues issues;
    const bool supported = ValidateDirectExternalDataSource(source, issues);
    UNIT_ASSERT_VALUES_EQUAL(supported, issues.Empty());
    return supported;
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(DirectExternalDataSourceDump) {
    Y_UNIT_TEST(KnownProvidersWithoutCredentials) {
        UNIT_ASSERT(Supported(Source("Ydb", "NONE")));
        UNIT_ASSERT(Supported(Source("YdbExternal", "NONE")));
    }

    Y_UNIT_TEST(LegacyTokenNameKeepsItsIdentity) {
        auto source = Source("Ydb", "TOKEN");
        (*source.mutable_properties())["TOKEN_SECRET_NAME"] = "eds_secret";
        UNIT_ASSERT(Supported(source));
        (*source.mutable_properties())["TOKEN_SECRET_NAME"] = "/Root/schemaSecret";
        UNIT_ASSERT(!Supported(source));
        (*source.mutable_properties())["TOKEN_SECRET_NAME"] = "";
        UNIT_ASSERT(!Supported(source));
        (*source.mutable_properties())["TOKEN_SECRET_NAME"] = "secret with spaces";
        UNIT_ASSERT(!Supported(source));
    }

    Y_UNIT_TEST(SchemaAndIamCredentialsAreRejected) {
        for (const auto& setting : {"TOKEN_SECRET_PATH", "INITIAL_TOKEN_SECRET_PATH", "INITIAL_TOKEN_SECRET_NAME",
                "AWS_ACCESS_KEY_ID_SECRET_PATH", "AWS_SECRET_ACCESS_KEY_SECRET_PATH", "PASSWORD_SECRET_PATH",
                "SERVICE_ACCOUNT_SECRET_PATH"}) {
            auto source = Source("Ydb", "NONE");
            (*source.mutable_properties())[setting] = "/Root/credential";
            UNIT_ASSERT_C(!Supported(source), setting);
        }
        for (const auto& auth : {"IAM", "AWS", "BASIC", "SERVICE_ACCOUNT", "MDB_BASIC", "FUTURE_AUTH", ""}) {
            UNIT_ASSERT_C(!Supported(Source("Ydb", auth)), auth);
        }
    }

    Y_UNIT_TEST(UnknownAndConflictingPropertiesFailClosed) {
        auto source = Source("Ydb", "NONE");
        (*source.mutable_properties())["PROVIDER_PRIVATE_CREDENTIAL"] = "/Root/secret";
        UNIT_ASSERT(!Supported(source));
        source.mutable_properties()->erase("PROVIDER_PRIVATE_CREDENTIAL");
        (*source.mutable_properties())["TOKEN_SECRET_NAME"] = "eds_secret";
        UNIT_ASSERT(!Supported(source));
        UNIT_ASSERT(!Supported(Source("FutureProvider", "NONE")));
        UNIT_ASSERT(!Supported(Source("ObjectStorage", "NONE")));
        source = Source("Ydb", "TOKEN");
        UNIT_ASSERT(!Supported(source));
        source = Source("Ydb", "NONE");
        source.GetReflection()->MutableUnknownFields(&source)->AddLengthDelimited(100, "future credential");
        UNIT_ASSERT(!Supported(source));
    }

    Y_UNIT_TEST(ProviderPropertiesAndSqlLiteralSafety) {
        auto source = Source("Ydb", "NONE");
        for (const auto& name : {"USE_TLS", "DATABASE_ID", "SHARED_READING", "SHARED_READING_GROUP"}) {
            (*source.mutable_properties())[name] = "test";
        }
        UNIT_ASSERT(Supported(source));
        source.set_source_type("YdbExternal");
        UNIT_ASSERT(!Supported(source));
        source = Source("Ydb", "NONE");
        (*source.mutable_properties())["DATABASE_NAME"] = "/Remote/quote'";
        UNIT_ASSERT(!Supported(source));
        source = Source("Ydb", "NONE");
        source.set_location("endpoint\nmalformed");
        UNIT_ASSERT(!Supported(source));
        source = Source("Ydb", "NONE");
        source.mutable_self()->set_name("source`name");
        UNIT_ASSERT(!Supported(source));
    }
}

} // namespace NYdb::NDump
