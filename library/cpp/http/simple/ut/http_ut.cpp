#include <library/cpp/http/simple/http_client.h>

#include <library/cpp/http/server/response.h>

#include <library/cpp/testing/mock_server/server.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <util/system/event.h>
#include <util/system/thread.h>

#include <thread>

Y_UNIT_TEST_SUITE(SimpleHttp) {
    static THttpServerOptions createOptions(ui16 port, bool keepAlive) {
        THttpServerOptions o;
        o.AddBindAddress("localhost", port);
        o.SetThreads(1);
        o.SetMaxConnections(1);
        o.SetMaxQueueSize(1);
        o.EnableKeepAlive(keepAlive);
        return o;
    }

    class TPong: public TRequestReplier {
        TDuration Sleep_;
        ui16 Port_;

    public:
        TPong(TDuration sleep = TDuration(), ui16 port = 80)
            : Sleep_(sleep)
            , Port_(port)
        {
        }

        bool DoReply(const TReplyParams& params) override {
            TStringBuf path = TParsedHttpFull(params.Input.FirstLine()).Path;
            params.Input.ReadAll();
            if (path == "/redirect") {
                params.Output << "HTTP/1.1 307 Internal Redirect\r\n"
                                 "Location: http://localhost:"
                              << Port_
                              << "/redirect2?some_param=qwe\r\n"
                                 "Non-Authoritative-Reason: HSTS\r\n\r\n"
                                 "must be missing";
                return true;
            }

            if (path == "/redirect2") {
                UNIT_ASSERT_VALUES_EQUAL("some_param=qwe", TParsedHttpFull(params.Input.FirstLine()).Cgi);
                params.Output << "HTTP/1.1 307 Internal Redirect\r\n"
                                 "Location: http://localhost:"
                              << Port_
                              << "/ping\r\n"
                                 "Non-Authoritative-Reason: HSTS\r\n\r\n"
                                 "must be missing too";
                return true;
            }

            if (path != "/ping") {
                UNIT_ASSERT_C(false, "path is incorrect: '" << path << "'");
            }

            Sleep(Sleep_);

            THttpResponse resp(HTTP_OK);
            resp.SetContent("pong");
            resp.OutTo(params.Output);

            return true;
        }
    };

    class TCodedPong: public TRequestReplier {
        HttpCodes Code_;

    public:
        TCodedPong(HttpCodes code)
            : Code_(code)
        {
        }

        bool DoReply(const TReplyParams& params) override {
            if (TParsedHttpFull(params.Input.FirstLine()).Path != "/ping") {
                UNIT_ASSERT(false);
            }

            THttpResponse resp(Code_);
            resp.SetContent("pong");
            resp.OutTo(params.Output);

            return true;
        }
    };

    class T500: public TRequestReplier {
        ui16 Port_;

    public:
        T500(ui16 port)
            : Port_(port)
        {
        }

        bool DoReply(const TReplyParams& params) override {
            TStringBuf path = TParsedHttpFull(params.Input.FirstLine()).Path;

            if (path == "/bad_redirect") {
                params.Output << "HTTP/1.1 500 Internal Redirect\r\n"
                                 "Location: http://localhost:1/qwerty\r\n"
                                 "Non-Authoritative-Reason: HSTS\r\n\r\n";
                return true;
            }

            if (path == "/redirect_to_500") {
                params.Output << "HTTP/1.1 307 Internal Redirect\r\n"
                                 "Location: http://localhost:"
                              << Port_
                              << "/500\r\n"
                                 "Non-Authoritative-Reason: HSTS\r\n\r\n";
                return true;
            }

            THttpResponse resp(HTTP_INTERNAL_SERVER_ERROR);
            resp.SetContent("bang");
            resp.OutTo(params.Output);

            return true;
        }
    };

    Y_UNIT_TEST(simpleSuccessful) {
        TPortManager pm;
        ui16 port = pm.GetPort(80);
        NMock::TMockServer server(createOptions(port, false), []() { return new TPong; });

        TSimpleHttpClient cl("localhost", port);
        UNIT_ASSERT_VALUES_EQUAL(0, server.GetClientCount());

        {
            TStringStream s;
            UNIT_ASSERT_NO_EXCEPTION(cl.DoGet("/ping", &s));
            UNIT_ASSERT_VALUES_EQUAL("pong", s.Str());
            Sleep(TDuration::MilliSeconds(500));
            UNIT_ASSERT_VALUES_EQUAL(0, server.GetClientCount());
        }
        {
            TStringStream s;
            UNIT_ASSERT_NO_EXCEPTION(cl.DoGet("/ping", &s));
            UNIT_ASSERT_VALUES_EQUAL("pong", s.Str());
            Sleep(TDuration::MilliSeconds(500));
            UNIT_ASSERT_VALUES_EQUAL(0, server.GetClientCount());
        }

        {
            TStringStream s;
            UNIT_ASSERT_NO_EXCEPTION(cl.DoPost("/ping", "", &s));
            UNIT_ASSERT_VALUES_EQUAL("pong", s.Str());
            Sleep(TDuration::MilliSeconds(500));
            UNIT_ASSERT_VALUES_EQUAL(0, server.GetClientCount());
        }
        {
            TStringStream s;
            UNIT_ASSERT_NO_EXCEPTION(cl.DoPost("/ping", "", &s));
            UNIT_ASSERT_VALUES_EQUAL("pong", s.Str());
            Sleep(TDuration::MilliSeconds(500));
            UNIT_ASSERT_VALUES_EQUAL(0, server.GetClientCount());
        }
    }

    Y_UNIT_TEST(simpleMessages) {
        TPortManager pm;
        ui16 port = pm.GetPort(80);
        NMock::TMockServer server(createOptions(port, false), []() { return new TPong; });

        TSimpleHttpClient cl("localhost", port);
        UNIT_ASSERT_VALUES_EQUAL(0, server.GetClientCount());

        {
            TStringStream s;
            UNIT_ASSERT_NO_EXCEPTION(cl.DoGet("/ping", &s));
            UNIT_ASSERT_VALUES_EQUAL("pong", s.Str());
            Sleep(TDuration::MilliSeconds(500));
            UNIT_ASSERT_VALUES_EQUAL(0, server.GetClientCount());
        }
        {
            UNIT_ASSERT_NO_EXCEPTION(cl.DoGet("/ping", nullptr));
            Sleep(TDuration::MilliSeconds(500));
            UNIT_ASSERT_VALUES_EQUAL(0, server.GetClientCount());
        }

        server.SetGenerator([]() { return new TCodedPong(HTTP_CONTINUE); });
        {
            TStringStream s;
            UNIT_ASSERT_EXCEPTION_CONTAINS(cl.DoPost("/ping", "", &s),
                                           THttpRequestException,
                                           "Got 100 at localhost/ping\n"
                                           "Full http response:\n");
            UNIT_ASSERT_VALUES_EQUAL("pong", s.Str());
            Sleep(TDuration::MilliSeconds(500));
            UNIT_ASSERT_VALUES_EQUAL(0, server.GetClientCount());
        }
        {
            UNIT_ASSERT_EXCEPTION_CONTAINS(cl.DoPost("/ping", "", nullptr),
                                           THttpRequestException,
                                           "Got 100 at localhost/ping\n"
                                           "Full http response:\n"
                                           "pong");
            Sleep(TDuration::MilliSeconds(500));
            UNIT_ASSERT_VALUES_EQUAL(0, server.GetClientCount());
        }
    }

    Y_UNIT_TEST(simpleTimeout) {
        TPortManager pm;
        ui16 port = pm.GetPort(80);
        NMock::TMockServer server(createOptions(port, true), []() { return new TPong(TDuration::MilliSeconds(300)); });

        TSimpleHttpClient cl("localhost", port, TDuration::MilliSeconds(50), TDuration::MilliSeconds(50));

        TStringStream s;
        UNIT_ASSERT_EXCEPTION_CONTAINS(cl.DoGet("/ping", &s),
                                       TSystemError,
                                       "Resource temporarily unavailable");
        UNIT_ASSERT_EXCEPTION_CONTAINS(cl.DoPost("/ping", "", &s),
                                       TSystemError,
                                       "Resource temporarily unavailable");
    }

    Y_UNIT_TEST(simpleError) {
        TPortManager pm;
        ui16 port = pm.GetPort(80);
        NMock::TMockServer server(createOptions(port, true), []() { return new TPong; });

        TSimpleHttpClient cl("localhost", port);
        UNIT_ASSERT_VALUES_EQUAL(0, server.GetClientCount());

        {
            TStringStream s;
            server.SetGenerator([]() { return new TCodedPong(HTTP_CONTINUE); });
            UNIT_ASSERT_EXCEPTION_CONTAINS(cl.DoGet("/ping", &s),
                                           THttpRequestException,
                                           "Got 100 at localhost/ping\n"
                                           "Full http response:");
            UNIT_ASSERT_VALUES_EQUAL("pong", s.Str());
            Sleep(TDuration::MilliSeconds(500));
            UNIT_ASSERT_VALUES_EQUAL(0, server.GetClientCount());
        }

        {
            TStringStream s;
            server.SetGenerator([]() { return new TCodedPong(HTTP_OK); });
            UNIT_ASSERT_NO_EXCEPTION(cl.DoGet("/ping", &s));
            UNIT_ASSERT_VALUES_EQUAL("pong", s.Str());
            Sleep(TDuration::MilliSeconds(500));
            UNIT_ASSERT_VALUES_EQUAL(0, server.GetClientCount());

            server.SetGenerator([]() { return new TCodedPong(HTTP_PARTIAL_CONTENT); });
            UNIT_ASSERT_NO_EXCEPTION(cl.DoGet("/ping", &s));
            Sleep(TDuration::MilliSeconds(500));
            UNIT_ASSERT_VALUES_EQUAL(0, server.GetClientCount());
        }

        {
            TStringStream s;
            server.SetGenerator([]() { return new TCodedPong(HTTP_MULTIPLE_CHOICES); });
            UNIT_ASSERT_EXCEPTION_CONTAINS(cl.DoGet("/ping", &s),
                                           THttpRequestException,
                                           "Got 300 at localhost/ping\n"
                                           "Full http response:");
            UNIT_ASSERT_VALUES_EQUAL("pong", s.Str());
            Sleep(TDuration::MilliSeconds(500));
            UNIT_ASSERT_VALUES_EQUAL(0, server.GetClientCount());
        }
    }

    Y_UNIT_TEST(redirectable) {
        TPortManager pm;
        ui16 port = pm.GetPort(80);
        NMock::TMockServer server(createOptions(port, true), [port]() { return new TPong(TDuration(), port); });

        TRedirectableHttpClient cl("localhost", port);
        UNIT_ASSERT_VALUES_EQUAL(0, server.GetClientCount());

        {
            TStringStream s;
            UNIT_ASSERT_NO_EXCEPTION(cl.DoGet("/redirect", &s));
            UNIT_ASSERT_VALUES_EQUAL("pong", s.Str());
            Sleep(TDuration::MilliSeconds(500));
            UNIT_ASSERT_VALUES_EQUAL(0, server.GetClientCount());
        }

        server.SetGenerator([port]() { return new T500(port); });

        TStringStream s;
        UNIT_ASSERT_EXCEPTION_CONTAINS(cl.DoGet("/bad_redirect", &s),
                                       THttpRequestException,
                                       "can not connect to ");
        Sleep(TDuration::MilliSeconds(500));
        UNIT_ASSERT_VALUES_EQUAL(0, server.GetClientCount());

        UNIT_ASSERT_EXCEPTION_CONTAINS(cl.DoGet("/redirect_to_500", &s),
                                       THttpRequestException,
                                       "Got 500 at http://localhost/500\n"
                                       "Full http response:\n");
        UNIT_ASSERT_VALUES_EQUAL("bang", s.Str());
        Sleep(TDuration::MilliSeconds(500));
        UNIT_ASSERT_VALUES_EQUAL(0, server.GetClientCount());
    }

    Y_UNIT_TEST(keepaliveSuccessful) {
        auto test = [](bool keepalive, i64 clientCount) {
            TPortManager pm;
            ui16 port = pm.GetPort(80);
            NMock::TMockServer server(createOptions(port, keepalive), []() { return new TPong; });

            TKeepAliveHttpClient cl("localhost", port);
            UNIT_ASSERT_VALUES_EQUAL(0, server.GetClientCount());
            {
                TStringStream s;
                int code = -1;
                UNIT_ASSERT_NO_EXCEPTION_C(code = cl.DoGet("/ping", &s), keepalive);
                UNIT_ASSERT_VALUES_EQUAL_C(200, code, keepalive);
                UNIT_ASSERT_VALUES_EQUAL_C("pong", s.Str(), keepalive);
                Sleep(TDuration::MilliSeconds(500));
                UNIT_ASSERT_VALUES_EQUAL(clientCount, server.GetClientCount());
            }
            {
                TStringStream s;
                int code = -1;
                UNIT_ASSERT_NO_EXCEPTION_C(code = cl.DoGet("/ping", &s), keepalive);
                UNIT_ASSERT_VALUES_EQUAL_C(200, code, keepalive);
                UNIT_ASSERT_VALUES_EQUAL_C("pong", s.Str(), keepalive);
                Sleep(TDuration::MilliSeconds(500));
                UNIT_ASSERT_VALUES_EQUAL(clientCount, server.GetClientCount());
            }

            {
                TStringStream s;
                int code = -1;
                UNIT_ASSERT_NO_EXCEPTION_C(code = cl.DoPost("/ping", "", &s), keepalive);
                UNIT_ASSERT_VALUES_EQUAL_C(200, code, keepalive);
                UNIT_ASSERT_VALUES_EQUAL_C("pong", s.Str(), keepalive);
                Sleep(TDuration::MilliSeconds(500));
                UNIT_ASSERT_VALUES_EQUAL(clientCount, server.GetClientCount());
            }
            {
                TStringStream s;
                int code = -1;
                UNIT_ASSERT_NO_EXCEPTION_C(code = cl.DoPost("/ping", "", &s), keepalive);
                UNIT_ASSERT_VALUES_EQUAL_C(200, code, keepalive);
                UNIT_ASSERT_VALUES_EQUAL_C("pong", s.Str(), keepalive);
                Sleep(TDuration::MilliSeconds(500));
                UNIT_ASSERT_VALUES_EQUAL(clientCount, server.GetClientCount());
            }
        };

        test(true, 1);
        test(false, 0);
    }

    Y_UNIT_TEST(keepaliveTimeout) {
        TPortManager pm;
        ui16 port = pm.GetPort(80);
        NMock::TMockServer server(createOptions(port, true), []() { return new TPong(TDuration::MilliSeconds(300)); });

        TKeepAliveHttpClient cl("localhost", port, TDuration::MilliSeconds(50), TDuration::MilliSeconds(50));

        TStringStream s;
        UNIT_ASSERT_EXCEPTION_CONTAINS(cl.DoGet("/ping", &s),
                                       TSystemError,
                                       "Resource temporarily unavailable");
        UNIT_ASSERT_EXCEPTION_CONTAINS(cl.DoPost("/ping", "", &s),
                                       TSystemError,
                                       "Resource temporarily unavailable");
    }

    Y_UNIT_TEST(keepaliveHeaders) {
        TPortManager pm;
        ui16 port = pm.GetPort(80);
        NMock::TMockServer server(createOptions(port, true), []() { return new TPong; });

        TKeepAliveHttpClient cl("localhost", port);

        TStringStream s;
        THttpHeaders h;
        UNIT_ASSERT_VALUES_EQUAL(200, cl.DoGet("/ping", &s, {}, &h));
        TStringStream hs;
        h.OutTo(&hs);
        UNIT_ASSERT_VALUES_EQUAL("Content-Length: 4\r\nConnection: Keep-Alive\r\n", hs.Str());
    }

    Y_UNIT_TEST(keepaliveRaw) {
        TPortManager pm;
        ui16 port = pm.GetPort(80);
        NMock::TMockServer server(createOptions(port, true), []() { return new TPong; });

        TKeepAliveHttpClient cl("localhost", port);

        TStringStream s;
        THttpHeaders h;

        TString raw = "POST /ping HTTP/1.1\r\n"
                      "Connection: Keep-Alive\r\n"
                      "Accept-Encoding: gzip, deflate\r\n"
                      "Content-Length: 9\r\n"
                      "Content-Type: application/x-www-form-urlencoded\r\n"
                      "User-Agent: Python-urllib/2.6\r\n"
                      "\r\n"
                      "some body";

        UNIT_ASSERT_VALUES_EQUAL(200, cl.DoRequestRaw(raw, &s, &h));
        TStringStream hs;
        h.OutTo(&hs);
        UNIT_ASSERT_VALUES_EQUAL("Content-Length: 4\r\nConnection: Keep-Alive\r\n", hs.Str());

        raw = "GET /ping HT TP/1.1\r\n";
        UNIT_ASSERT_EXCEPTION_CONTAINS(cl.DoRequestRaw(raw, &s, &h), TSystemError, "can not read from socket input stream");
    }

    Y_UNIT_TEST(keepaliveWithClosedByPeer) {
        TPortManager pm;
        ui16 port = pm.GetPort(80);
        NMock::TMockServer::TGenerator gen = []() { return new TPong; };
        THolder<NMock::TMockServer> server = MakeHolder<NMock::TMockServer>(createOptions(port, true), gen);

        TKeepAliveHttpClient cl("localhost", port);
        UNIT_ASSERT_NO_EXCEPTION(cl.DoGet("/ping"));

        server.Reset();
        server = MakeHolder<NMock::TMockServer>(createOptions(port, true), gen);
        UNIT_ASSERT_NO_EXCEPTION(cl.DoGet("/ping"));

        TKeepAliveHttpClient cl2("localhost", port);
        UNIT_ASSERT_NO_EXCEPTION(cl2.DoGet("/ping"));
        Sleep(TDuration::MilliSeconds(500));
        UNIT_ASSERT_NO_EXCEPTION(cl.DoGet("/ping"));
    }
}
