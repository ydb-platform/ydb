#include <library/cpp/tvmauth/version.h>

#include <library/cpp/testing/unittest/registar.h>

#include <regex>

using namespace NTvmAuth;

Y_UNIT_TEST_SUITE(VersionTest) {
    Y_UNIT_TEST(base64Test) {
        const std::regex re(R"(^\d+\.\d+\.\d+$)");

        for (size_t idx = 0; idx < 2; ++idx) {
            TStringBuf ver = LibVersion();
            UNIT_ASSERT(std::regex_match(ver.begin(), ver.end(), re));
        }
    }
}
