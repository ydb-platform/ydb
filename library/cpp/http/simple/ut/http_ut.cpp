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

    class TScenario {
    public:
        struct TElem {
            TString Url;
            int Status = HTTP_OK;
            TString Content{};
        };

        TScenario(const TVector<TElem>& seq, ui16 port = 80, TDuration sleep = TDuration())
            : Seq_(seq)
            , Sleep_(sleep)
            , Port_(port)
        {
        }

        bool DoReply(const TRequestReplier::TReplyParams& params, TRequestReplier* replier) {
            const auto parsed = TParsedHttpFull(params.Input.FirstLine());
            const auto url = parsed.Request;
            params.Input.ReadAll();

            UNIT_ASSERT(SeqIdx_ < Seq_.size());
            auto& elem = Seq_[SeqIdx_++];

            UNIT_ASSERT_VALUES_EQUAL(elem.Url, url);

            Sleep(Sleep_);

            if (elem.Status == -1) {
                replier->ResetConnection(); // RST / ECONNRESET
                return true;
            }

            THttpResponse resp((HttpCodes)elem.Status);

            if (elem.Status >= 300 && elem.Status < 400) {
                UNIT_ASSERT(SeqIdx_ < Seq_.size());
                resp.AddHeader("Location", TStringBuilder() << "http://localhost:" << Port_ << Seq_[SeqIdx_].Url);
            }

            resp.SetContent(elem.Content);
            resp.OutTo(params.Output);

            return true;
        }

        void VerifyInvariants() {
            UNIT_ASSERT_VALUES_EQUAL(SeqIdx_, Seq_.size());
        }

    private:
        TVector<TElem> Seq_;
        size_t SeqIdx_ = 0;
        TDuration Sleep_;
        ui16 Port_;
    };

    class TScenarioReplier: public TRequestReplier {
        TScenario* Scenario_ = nullptr;

    public:
        TScenarioReplier(TScenario* scenario)
            : Scenario_(scenario)
        {
        }

        bool DoReply(const TReplyParams& params) override {
            return Scenario_->DoReply(params, this);
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

    static void TestRedirectCountParam(int maxRedirectCount, int redirectCount) {
        TPortManager pm;
        ui16 port = pm.GetPort(80);

        TVector<TScenario::TElem> steps;
        for (int i = 0; i < redirectCount; ++i) {
            steps.push_back({"/any", 302});
        }
        steps.push_back({"/any", 200, "Hello"});
        TScenario scenario(steps, port);

        NMock::TMockServer server(createOptions(port, true), [&scenario]() { return new TScenarioReplier(&scenario); });

        TRedirectableHttpClient cl(TSimpleHttpClientOptions().Host("localhost").Port(port).MaxRedirectCount(maxRedirectCount));
        UNIT_ASSERT_VALUES_EQUAL(0, server.GetClientCount());

        TStringStream s;
        if (maxRedirectCount >= redirectCount) {
            UNIT_ASSERT_NO_EXCEPTION(cl.DoGet("/any", &s));
            UNIT_ASSERT_VALUES_EQUAL("Hello", s.Str());
            scenario.VerifyInvariants();
        } else {
            UNIT_ASSERT_EXCEPTION_CONTAINS(cl.DoGet("/any", &s), THttpRequestException, "");
        }
    }

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

    Y_UNIT_TEST(redirectCountDefault) {
        TPortManager pm;
        ui16 port = pm.GetPort(80);

        TScenario scenario({
            {"/any", 307},
            {"/any?param=1", 302},
            {"/any?param=1", 302},
            {"/any?param=1", 302},
            {"/any?param=1", 302},
            {"/any?param=1", 302},
            {"/any?param=1", 302},
            {"/any?param=1", 302},
            {"/any?param=1", 302},
            {"/any?param=1", 302},
            {"/any?param=2", 200, "Hello"}
        }, port);

        NMock::TMockServer server(createOptions(port, true), [&scenario]() { return new TScenarioReplier(&scenario); });

        TRedirectableHttpClient cl("localhost", port);
        UNIT_ASSERT_VALUES_EQUAL(0, server.GetClientCount());

        TStringStream s;
        UNIT_ASSERT_NO_EXCEPTION(cl.DoGet("/any", &s));
        UNIT_ASSERT_VALUES_EQUAL("Hello", s.Str());

        scenario.VerifyInvariants();
    }

    Y_UNIT_TEST(redirectCountN) {
        TestRedirectCountParam(0, 0);
        TestRedirectCountParam(0, 1);
        TestRedirectCountParam(1, 1);
        TestRedirectCountParam(3, 3);
        TestRedirectCountParam(20, 20);
        TestRedirectCountParam(20, 21);
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
                      "User-Agent: Arcadia-library/cpp/http\r\n"
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
