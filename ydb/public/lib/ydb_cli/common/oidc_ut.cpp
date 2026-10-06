#include <ydb/public/lib/ydb_cli/common/oidc.h>
#include <ydb/public/lib/ydb_cli/common/oidc_options.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/core_facility/core_facility.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>
#include <util/stream/file.h>
#include <util/stream/str.h>

#include <future>

namespace NYdb::NConsoleClient {
namespace {

NOidc::TDeviceAuthInfo MakeDeviceAuthInfo();
TOidcCliOptions MakeStaticOptions();

NOidc::TDeviceAuthInfo MakeDeviceAuthInfo() {
    return {
        .UserCode = "ABCD-EFGH",
        .VerificationUrl = "https://issuer.example/device",
        .VerificationUrlComplete = std::nullopt,
        .ExpiresAt = TInstant::Seconds(2'000'000'000),
    };
}

TOidcCliOptions MakeStaticOptions() {
    TOidcCliOptions options;
    options.ResolvedConfig = NOidc::TOidcConfig{
        .Issuer = "https://issuer.example",
        .FlowConfig = NOidc::TStaticOidcConfig{.AccessToken = "session-token", .ExpiresAt = std::nullopt},
    };
    return options;
}

} // namespace

Y_UNIT_TEST_SUITE(TOidcCliAcceptor) {

    Y_UNIT_TEST(FactoryIdentitySurvivesProviderCreationAndDoesNotExposeTokens) {
        const auto factory = CreateCliOidcCredentialsProviderFactory(MakeStaticOptions());
        const auto identity = factory->GetClientIdentity();
        UNIT_ASSERT(!identity.empty());
        UNIT_ASSERT(identity.find("session-token") == std::string::npos);
        UNIT_ASSERT_VALUES_EQUAL(factory->CreateProvider()->GetAuthInfo(), "Bearer session-token");
        UNIT_ASSERT_VALUES_EQUAL(factory->GetClientIdentity(), identity);
    }

    Y_UNIT_TEST(PrintsVerificationUrlAndCode) {
        TStringStream output;
        const auto acceptor = CreateCliAuthAcceptor(output);

        acceptor->Accept(MakeDeviceAuthInfo());

        UNIT_ASSERT_VALUES_EQUAL(output.Str(),
            "To sign in, open https://issuer.example/device and enter code ABCD-EFGH\n");
    }

    Y_UNIT_TEST(PrintsCompleteVerificationUrlWhenPresent) {
        TStringStream output;
        const auto acceptor = CreateCliAuthAcceptor(output);
        auto info = MakeDeviceAuthInfo();
        info.VerificationUrlComplete = "https://issuer.example/device?user_code=ABCD-EFGH";

        acceptor->Accept(info);

        UNIT_ASSERT_VALUES_EQUAL(output.Str(),
            "To sign in, open https://issuer.example/device and enter code ABCD-EFGH\n"
            "Or open https://issuer.example/device?user_code=ABCD-EFGH\n");
    }

    Y_UNIT_TEST(EscapesControlCharacters) {
        TStringStream output;
        const auto acceptor = CreateCliAuthAcceptor(output);
        auto info = MakeDeviceAuthInfo();
        info.UserCode = "ABCD\n";
        info.VerificationUrl = "https://issuer.example/device\x1b";
        info.VerificationUrlComplete = "https://issuer.example/device?user_code=ABCD\r";

        acceptor->Accept(info);

        UNIT_ASSERT_VALUES_EQUAL(output.Str(),
            "To sign in, open https://issuer.example/device\\x1B and enter code ABCD\\x0A\n"
            "Or open https://issuer.example/device?user_code=ABCD\\x0D\n");
    }

    Y_UNIT_TEST(ProviderSurvivesDriverFacilitiesAndFactory) {
        auto factory = CreateCliOidcCredentialsProviderFactory(MakeStaticOptions());
        auto probeFacility = CreateSimpleCoreFacility();
        const auto provider = factory->CreateProvider(probeFacility);
        auto ready = provider->GetAuthInfoAsync();
        UNIT_ASSERT(ready.Wait(TDuration::Seconds(5)));
        UNIT_ASSERT_VALUES_EQUAL(ready.GetValueSync(), "Bearer session-token");
        probeFacility.reset();
        UNIT_ASSERT(provider->IsValid());

        auto sqlFacility = CreateSimpleCoreFacility();
        UNIT_ASSERT(provider == factory->CreateProvider(sqlFacility));
        UNIT_ASSERT(provider == factory->CreateProvider());
        sqlFacility.reset();
        factory.reset();
        UNIT_ASSERT(provider->IsValid());
        UNIT_ASSERT_VALUES_EQUAL(provider->GetAuthInfo(), "Bearer session-token");
    }

    Y_UNIT_TEST(ConcurrentCreationReusesOneProvider) {
        const auto factory = CreateCliOidcCredentialsProviderFactory(MakeStaticOptions());
        const auto facility = CreateSimpleCoreFacility();
        auto first = std::async(std::launch::async, [factory] {
            return factory->CreateProvider();
        });
        auto second = std::async(std::launch::async, [factory, facility] {
            return factory->CreateProvider(facility);
        });
        const auto provider = first.get();
        UNIT_ASSERT(provider == second.get());
        auto ready = provider->GetAuthInfoAsync();
        UNIT_ASSERT(ready.Wait(TDuration::Seconds(5)));
        UNIT_ASSERT_VALUES_EQUAL(ready.GetValueSync(), "Bearer session-token");
    }

    Y_UNIT_TEST(CreatesCliCredentialsFromConfig) {
        TTempDir dir;
        TFileOutput((dir.Path() / "token").GetPath()).Write("opaque-access\n");
        const auto path = dir.Path() / "oidc.yaml";
        TFileOutput(path.GetPath()).Write(R"(
issuer: https://issuer.example
static_credentials:
  access_token_file: token
)");

        const auto factory = CreateCliOidcCredentialsProviderFactory(path.GetPath());

        auto probeFacility = CreateSimpleCoreFacility();
        const auto provider = factory->CreateProvider(probeFacility);
        UNIT_ASSERT_VALUES_EQUAL(provider->GetAuthInfo(), "Bearer opaque-access");
        probeFacility.reset();
        UNIT_ASSERT(provider == factory->CreateProvider());
        UNIT_ASSERT_VALUES_EQUAL(provider->GetAuthInfo(), "Bearer opaque-access");
    }
} // Y_UNIT_TEST_SUITE(TOidcCliAcceptor)

} // namespace NYdb::NConsoleClient
