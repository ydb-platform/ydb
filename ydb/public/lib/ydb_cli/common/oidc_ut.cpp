#include <ydb/public/lib/ydb_cli/common/oidc.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>
#include <util/stream/file.h>
#include <util/stream/str.h>

namespace NYdb::NConsoleClient {
namespace {

NOidc::TDeviceAuthInfo MakeDeviceAuthInfo();

NOidc::TDeviceAuthInfo MakeDeviceAuthInfo() {
    return {
        .UserCode = "ABCD-EFGH",
        .VerificationUrl = "https://issuer.example/device",
        .VerificationUrlComplete = std::nullopt,
        .ExpiresAt = TInstant::Seconds(2'000'000'000),
    };
}

} // namespace

Y_UNIT_TEST_SUITE(TOidcCliAcceptor) {
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

    Y_UNIT_TEST(CreatesCliCredentialsFromConfig) {
        TTempDir dir;
        const auto path = dir.Path() / "oidc.yaml";
        TFileOutput(path.GetPath()).Write(R"(
issuer: https://issuer.example
static_credentials:
  access_token: opaque-access
)");

        const auto factory = CreateCliOidcCredentialsProviderFactory(path.GetPath());

        UNIT_ASSERT_VALUES_EQUAL(factory->CreateProvider()->GetAuthInfo(), "Bearer opaque-access");
    }
} // Y_UNIT_TEST_SUITE(TOidcCliAcceptor)

} // namespace NYdb::NConsoleClient
