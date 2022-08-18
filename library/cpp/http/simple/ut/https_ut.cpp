#include <library/cpp/http/simple/http_client.h>

#include <library/cpp/http/server/response.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <util/system/shellcommand.h>

Y_UNIT_TEST_SUITE(Https) {
    using TShellCommandPtr = std::unique_ptr<TShellCommand>;

    static TShellCommandPtr start(ui16 port) {
        const TString data = ArcadiaSourceRoot() + "/library/cpp/http/simple/ut/https_server";

        const TString command =
            TStringBuilder()
            << BuildRoot() << "/library/cpp/http/simple/ut/https_server/https_server"
            << " --port " << port
            << " --keyfile " << data << "/http_server.key"
            << " --certfile " << data << "/http_server.crt";

        auto res = std::make_unique<TShellCommand>(
            command,
            TShellCommandOptions()
                .SetAsync(true)
                .SetLatency(50)
                .SetErrorStream(&Cerr));

        res->Run();

        i32 tries = 100000;
        while (tries-- > 0) {
            try {
                TKeepAliveHttpClient client("https://localhost", port);
                client.DisableVerificationForHttps();
                client.DoGet("/ping");
                break;
            } catch (const std::exception& e) {
                Cout << "== failed to connect to new server: " << e.what() << Endl;
                Sleep(TDuration::MilliSeconds(1));
            }
        }

        return res;
    }

    static void get(TKeepAliveHttpClient & client) {
        TStringStream out;
        ui32 code = 0;

        UNIT_ASSERT_NO_EXCEPTION(code = client.DoGet("/ping", &out));
        UNIT_ASSERT_VALUES_EQUAL_C(code, 200, out.Str());
        UNIT_ASSERT_VALUES_EQUAL(out.Str(), "pong.my");
    }

    Y_UNIT_TEST(keepAlive) {
        TPortManager pm;
        ui16 port = pm.GetPort(443);
        TShellCommandPtr httpsServer = start(port);

        TKeepAliveHttpClient client("https://localhost",
                                    port,
                                    TDuration::Seconds(40),
                                    TDuration::Seconds(40));
        client.DisableVerificationForHttps();

        get(client);
        get(client);

        httpsServer->Terminate().Wait();
        httpsServer = start(port);

        get(client);
    }

    static void get(TSimpleHttpClient & client) {
        TStringStream out;

        UNIT_ASSERT_NO_EXCEPTION_C(client.DoGet("/ping", &out), out.Str());
        UNIT_ASSERT_VALUES_EQUAL(out.Str(), "pong.my");
    }

    Y_UNIT_TEST(simple) {
        TPortManager pm;
        ui16 port = pm.GetPort(443);
        TShellCommandPtr httpsServer = start(port);

        TSimpleHttpClient client("https://localhost",
                                 port,
                                 TDuration::Seconds(40),
                                 TDuration::Seconds(40));

        get(client);
        get(client);
    }
}
