#include <library/cpp/testing/mock_server/server.h>

#include <library/cpp/http/simple/http_client.h>
#include <library/cpp/testing/unittest/registar.h>

Y_UNIT_TEST_SUITE(Server) {
    int i;

    Y_UNIT_TEST(pong) {
        TPortManager pm;
        ui16 port = pm.GetPort(80);
        NMock::TMockServer server(port, []() { return new NMock::TPong; });

        TKeepAliveHttpClient cl("localhost", port);
        UNIT_ASSERT_VALUES_EQUAL(200, cl.DoGet("/ping"));
        UNIT_ASSERT_VALUES_EQUAL(404, cl.DoGet("/kek"));
    }

    Y_UNIT_TEST(custom) {
        class TCustomReplier: public TRequestReplier {
        public:
            bool DoReply(const TReplyParams& params) override {
                THttpResponse resp(HttpCodes::HTTP_OK);
                resp.SetContent("everithing is ok");
                resp.OutTo(params.Output);

                return true;
            }
        };

        TPortManager pm;
        ui16 port = pm.GetPort(80);
        NMock::TMockServer server(port, []() { return new TCustomReplier; });

        TKeepAliveHttpClient cl("localhost", port);
        TStringStream out;
        UNIT_ASSERT_VALUES_EQUAL(200, cl.DoGet("/foo", &out));
        UNIT_ASSERT_VALUES_EQUAL("everithing is ok", out.Str());
    }
}
