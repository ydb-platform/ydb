#include "yt.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NConsoleClient {
namespace {

class TYtProxyParser : public TCommandWithYtProxy {
public:
    void Parse(const TString& proxy) {
        TClientCommandOptions options;
        options.AddLongOption("proxy").RequiredArgument("PROXY");
        const char* args[] = {"yt-proxy-test", "--proxy", proxy.c_str()};
        TOptionsParseResult parsed(&options, 3, args);
        TClientCommand::TConfig config(0, nullptr);
        config.ParseResult = &parsed;
        ParseYtProxy(config, "proxy");
    }

    const TString& GetHost() const {
        return YtHost;
    }

    ui16 GetPort() const {
        return YtPort;
    }
};

void AssertProxy(const TString& proxy, const TString& host, ui16 port) {
    TYtProxyParser parser;
    parser.Parse(proxy);
    UNIT_ASSERT_VALUES_EQUAL(parser.GetHost(), host);
    UNIT_ASSERT_VALUES_EQUAL(parser.GetPort(), port);
}

} // namespace

Y_UNIT_TEST_SUITE(YtProxyParsing) {
    Y_UNIT_TEST(LegacyProxies) {
        AssertProxy("hahn", "hahn.yt.yandex.net", 80);
        AssertProxy("hahn:8080", "hahn.yt.yandex.net", 8080);
        AssertProxy("hahn.yt.yandex.net", "hahn.yt.yandex.net", 80);
        AssertProxy("hahn.yt.yandex.net:8080", "hahn.yt.yandex.net", 8080);
        AssertProxy("localhost:9013", "localhost", 9013);
    }

    Y_UNIT_TEST(HttpsDefaultPort) {
        AssertProxy("https://hahn.yt.yandex.net", "https://hahn.yt.yandex.net", 443);
        AssertProxy("https://hahn", "https://hahn.yt.yandex.net", 443);
    }

    Y_UNIT_TEST(HttpsExplicitPort) {
        AssertProxy("https://hahn.yt.yandex.net:8443", "https://hahn.yt.yandex.net", 8443);
        AssertProxy("https://hahn:443", "https://hahn.yt.yandex.net", 443);
    }

    Y_UNIT_TEST(HttpScheme) {
        AssertProxy("http://hahn", "http://hahn.yt.yandex.net", 80);
        AssertProxy("http://localhost:8080", "http://localhost", 8080);
    }

    Y_UNIT_TEST(InvalidProxies) {
        for (const TString proxy : {
                 "", "https://", "http://", "hahn:invalid", "hahn:65536",
                 "ftp://hahn", "https://hahn/path", "https://hahn?query",
                 "https://hahn#fragment", "https://user@hahn"}) {
            TYtProxyParser parser;
            UNIT_ASSERT_EXCEPTION(parser.Parse(proxy), TMisuseException);
        }
    }
}

} // namespace NYdb::NConsoleClient
