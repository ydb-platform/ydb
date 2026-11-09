#include <ydb/public/lib/ydb_cli/dump/util/external_data_source_utils.h>
#include <ydb/public/lib/ydb_cli/dump/dump.h>
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
    source.set_installation("");
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
    Y_UNIT_TEST(DirectDumpRequiresExplicitOptIn) {
        UNIT_ASSERT(!TDumpSettings().EnableDirectExternalDataSource_);
        UNIT_ASSERT(TDumpSettings().EnableDirectExternalDataSource(true).EnableDirectExternalDataSource_);
    }

    Y_UNIT_TEST(CompleteSourcePathMustBeSafe) {
        NYql::TIssues issues;
        UNIT_ASSERT(ValidateDirectExternalDataSourcePath("/Root/nested/source", issues));
        for (const auto& path : {"/Root/parent`name/source", "/Root/parent name/source", "/Root/parent\tname/source",
                "/Root/parent\\name/source", "/Root/my source"}) {
            NYql::TIssues pathIssues;
            UNIT_ASSERT_C(!ValidateDirectExternalDataSourcePath(path, pathIssues), path);
            UNIT_ASSERT(!pathIssues.Empty());
        }
    }

    Y_UNIT_TEST(KnownProvidersWithoutCredentials) {
        UNIT_ASSERT(Supported(Source("Ydb", "NONE")));
        UNIT_ASSERT(Supported(Source("YdbExternal", "NONE")));
    }

    Y_UNIT_TEST(ObjectStorageWithoutCredentialsOrProviderProperties) {
        auto source = Source("ObjectStorage", "NONE");
        source.set_location("https://bucket.example.test/non-default/prefix/");
        source.mutable_properties()->erase("DATABASE_NAME");
        UNIT_ASSERT(Supported(source));
        for (const auto& property : {"DATABASE_NAME", "USE_TLS", "DATABASE_ID", "AWS_REGION", "PROVIDER_PRIVATE_CREDENTIAL",
                "TOKEN_SECRET_NAME", "TOKEN_SECRET_PATH", "AWS_ACCESS_KEY_ID_SECRET_NAME", "AWS_SECRET_ACCESS_KEY_SECRET_PATH"}) {
            auto unsupported = source;
            (*unsupported.mutable_properties())[property] = "unexpected";
            UNIT_ASSERT_C(!Supported(unsupported), property);
        }
        for (const auto& auth : {"AWS", "SERVICE_ACCOUNT", "TOKEN", "IAM", "UNKNOWN_AUTH"}) {
            auto unsupported = source;
            (*unsupported.mutable_properties())["AUTH_METHOD"] = auth;
            UNIT_ASSERT_C(!Supported(unsupported), auth);
        }
        source.set_location("https://bucket.example.test/quote'");
        UNIT_ASSERT(!Supported(source));
    }

    Y_UNIT_TEST(ObjectStorageLocationIsSerializedVerbatim) {
        auto source = Source("ObjectStorage", "NONE");
        source.mutable_properties()->erase("DATABASE_NAME");
        source.mutable_self()->set_name("bucketSource");
        source.set_location("https://archive_user:location_password@bucket.example.test/prefix/");
        UNIT_ASSERT(Supported(source));
        UNIT_ASSERT_STRING_CONTAINS(BuildCreateExternalDataSourceQuery(source, "/Root"),
            "LOCATION = 'https://archive_user:location_password@bucket.example.test/prefix/'");
        source.set_location("https://bucket.example.test/prefix/?X-Amz-Credential=caller%2Fscope&X-Amz-Signature=caller-signature");
        UNIT_ASSERT(Supported(source));
        UNIT_ASSERT_STRING_CONTAINS(BuildCreateExternalDataSourceQuery(source, "/Root"),
            "LOCATION = 'https://bucket.example.test/prefix/?X-Amz-Credential=caller%2Fscope&X-Amz-Signature=caller-signature'");
    }

    Y_UNIT_TEST(InstallationPresenceAndSerializationForEverySupportedProvider) {
        for (const auto& provider : {"Ydb", "YdbExternal", "ObjectStorage"}) {
            auto source = Source(provider, "NONE");
            if (source.source_type() == "ObjectStorage") {
                source.mutable_properties()->erase("DATABASE_NAME");
            }
            source.mutable_self()->set_name("source");
            source.clear_installation();
            UNIT_ASSERT_C(!Supported(source), provider);
            source.set_installation("");
            UNIT_ASSERT_C(Supported(source), provider);
            UNIT_ASSERT(!BuildCreateExternalDataSourceQuery(source, "/Root").Contains("INSTALLATION"));
            source.set_installation("cloud");
            UNIT_ASSERT_C(Supported(source), provider);
            UNIT_ASSERT_STRING_CONTAINS(BuildCreateExternalDataSourceQuery(source, "/Root"), "INSTALLATION = 'cloud'");
            auto propertyInsteadOfMetadata = source;
            (*propertyInsteadOfMetadata.mutable_properties())["INSTALLATION"] = "cloud";
            UNIT_ASSERT_C(!Supported(propertyInsteadOfMetadata), provider);
            source.set_installation("cloud'unsafe");
            UNIT_ASSERT_C(!Supported(source), provider);
        }
    }

    Y_UNIT_TEST(AllNamedTokenCredentialsAreRejected) {
        // Even a simple name can be shared by a non-owner who cannot pass old restore's access preflight.
        for (const auto& name : {"eds_secret", "secret:colon", "secret`backtick", "/Root/schemaSecret", "", "secret with spaces"}) {
            auto source = Source("Ydb", "TOKEN");
            (*source.mutable_properties())["TOKEN_SECRET_NAME"] = name;
            UNIT_ASSERT_C(!Supported(source), name);
        }
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
        UNIT_ASSERT(!Supported(Source("ObjectStorage", "NONE"))); // DATABASE_NAME remains forbidden.
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
        for (const auto& name : {"source`name", "my source", "source\tname", "source\vname", "source\fname"}) {
            source.mutable_self()->set_name(name);
            UNIT_ASSERT_C(!Supported(source), name);
        }
    }
}

} // namespace NYdb::NDump
