#include <library/cpp/http/simple/http_client.h>

#include <library/cpp/http/server/response.h>

#include <library/cpp/testing/mock_server/server.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <library/cpp/threading/future/async.h>
#include <util/thread/pool.h>

#include <util/network/sock.h>
#include <util/network/socket.h>
#include <util/system/event.h>
#include <util/system/thread.h>

#include <atomic>
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

    Y_UNIT_TEST(simpleCancel) {
        TPortManager pm;
        ui16 port = pm.GetPort(80);
        NMock::TMockServer server(createOptions(port, false), []() { return new TPong(TDuration::Seconds(1)); });

        TSimpleHttpClient cl("localhost", port);
        UNIT_ASSERT_VALUES_EQUAL(0, server.GetClientCount());

        auto tp = CreateThreadPool(3);

        {
            TStringStream s;
            NThreading::TCancellationTokenSource cancel;
            UNIT_ASSERT_NO_EXCEPTION(cl.DoGet("/ping", &s, TKeepAliveHttpClient::THeaders(), nullptr, cancel.Token()));
            cancel.Cancel();
            UNIT_ASSERT_VALUES_EQUAL("pong", s.Str());
            Sleep(TDuration::MilliSeconds(500));
            UNIT_ASSERT_VALUES_EQUAL(0, server.GetClientCount());
        }

        {
            TStringStream s;
            NThreading::TCancellationTokenSource cancel;
            auto reqFuture = NThreading::Async([&] {
                // Если DoGet() при отмене кидает исключение — оно “переедет” в future.
                return cl.DoGet("/ping",
                                &s,
                                TKeepAliveHttpClient::THeaders(),
                                nullptr,
                                cancel.Token());
            }, *tp);
            Sleep(TDuration::MilliSeconds(50));
            cancel.Cancel();
            UNIT_ASSERT_EXCEPTION(reqFuture.GetValueSync(), NThreading::TOperationCancelledException);
            Sleep(TDuration::MilliSeconds(1000));
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

    // Byte-exact server. THttpServer frames replies through THttpOutput and therefore cannot
    // emit a body shorter than the Content-Length it announces, which is the case under test.
    // One thread per connection: a redirect keeps the original connection open while it
    // fetches the target, so serving them one at a time would deadlock.
    class TRawHttpServer {
    public:
        struct TReply {
            TString Bytes;
            bool CloseAfter = false;
        };

        TRawHttpServer(ui16 port, TVector<TReply> script)
            : Script_(std::move(script))
            , Port_(port)
        {
            CheckedSetSockOpt((SOCKET)Listener_, SOL_SOCKET, SO_REUSEADDR, 1, "TRawHttpServer");
            TSockAddrInet addr("127.0.0.1", port);
            TBaseSocket::Check(Listener_.Bind(&addr), "bind");
            TBaseSocket::Check(Listener_.Listen(4), "listen");
            Acceptor_ = std::thread([this] { Accept(); });
        }

        ~TRawHttpServer() {
            Stop_.store(true);
            WakeUpAcceptor();
            Acceptor_.join();
            for (auto& conn : Connections_) {
                conn.join();
            }
        }

        size_t Served() const {
            return Served_.load();
        }

    private:
        static constexpr long PollMs = 100;

        // Accept() blocks; a self-connect lands in the backlog and releases it.
        void WakeUpAcceptor() {
            TInetStreamSocket waker;
            TSockAddrInet addr("127.0.0.1", Port_);
            waker.Connect(&addr);
        }

        void Accept() {
            while (!Stop_.load()) {
                auto conn = MakeAtomicShared<TStreamSocket>();
                if (Listener_.Accept(conn.Get()) < 0 || Stop_.load()) {
                    return;
                }
                SetSocketTimeout((SOCKET)*conn, 0, PollMs);
                Connections_.emplace_back([this, conn] { Serve(*conn); });
            }
        }

        void Serve(TStreamSocket& conn) {
            while (ReadRequest(conn)) {
                const size_t idx = Served_.fetch_add(1);
                if (idx >= Script_.size()) {
                    return;
                }

                const TReply& reply = Script_[idx];
                conn.Send(reply.Bytes.data(), reply.Bytes.size());
                if (reply.CloseAfter) {
                    try {
                        conn.ShutDown(SHUT_WR); // clean FIN mid-body
                    } catch (const TSystemError&) {
                        // peer hung up first, nothing left to half-close
                    }
                    return;
                }
            }
        }

        // The receive timeout keeps this responsive to Stop_ on an idle connection.
        bool ReadRequest(TStreamSocket& conn) {
            TString head;
            char c = 0;
            while (!head.EndsWith("\r\n\r\n")) {
                if (Stop_.load()) {
                    return false;
                }

                const ssize_t got = conn.Recv(&c, 1);
                if (got == 1) {
                    head += c;
                } else if (got == -EAGAIN || got == -EWOULDBLOCK) {
                    continue;
                } else {
                    return false; // peer closed, or a real error
                }
            }
            return true;
        }

    private:
        TVector<TReply> Script_;
        ui16 Port_ = 0;
        TInetStreamSocket Listener_;
        std::atomic<size_t> Served_{0};
        std::atomic<bool> Stop_{false};
        std::thread Acceptor_;
        TVector<std::thread> Connections_;
    };

    static TString Truncated(size_t announced, TStringBuf sent) {
        return TStringBuilder() << "HTTP/1.1 200 OK\r\nContent-Length: " << announced
                                << "\r\nConnection: Keep-Alive\r\n\r\n" << sent;
    }

    Y_UNIT_TEST(truncatedBodyIsToleratedByDefault) {
        TPortManager pm;
        ui16 port = pm.GetPort(80);
        TRawHttpServer server(port, {{Truncated(100, "truncated"), true}});

        TSimpleHttpClient cl("127.0.0.1", port);

        TStringStream s;
        UNIT_ASSERT_NO_EXCEPTION(cl.DoGet("/ping", &s));
        UNIT_ASSERT_VALUES_EQUAL("truncated", s.Str());
    }

    Y_UNIT_TEST(strictContentLengthViaOptions) {
        TPortManager pm;
        ui16 port = pm.GetPort(80);
        TRawHttpServer server(port, {{Truncated(100, "truncated"), true}});

        TSimpleHttpClient cl(TSimpleHttpClientOptions().Host("127.0.0.1").Port(port).StrictContentLength(true));

        TStringStream s;
        UNIT_ASSERT_EXCEPTION(cl.DoGet("/ping", &s), THttpTruncatedBodyException);
    }

    Y_UNIT_TEST(strictContentLengthSurvivesRedirect) {
        TPortManager pm;
        ui16 port = pm.GetPort(80);
        TRawHttpServer server(port, {
                                        {TStringBuilder() << "HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1:" << port
                                                          << "/ping2\r\nContent-Length: 0\r\nConnection: Keep-Alive\r\n\r\n",
                                         false},
                                        {Truncated(100, "truncated"), true},
                                    });

        TRedirectableHttpClient cl(TSimpleHttpClientOptions().Host("127.0.0.1").Port(port).StrictContentLength(true));

        TStringStream s;
        UNIT_ASSERT_EXCEPTION(cl.DoGet("/ping", &s), THttpTruncatedBodyException);
    }

    // A truncation on a reused connection must not be mistaken for the stale-connection case
    // that the THttpReadException handler retries: the body is already partly in `output`.
    Y_UNIT_TEST(strictContentLengthDoesNotRetryTruncatedBody) {
        TPortManager pm;
        ui16 port = pm.GetPort(80);
        TRawHttpServer server(port, {
                                        {"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: Keep-Alive\r\n\r\nfull", false},
                                        {Truncated(100, "truncated"), true},
                                        {"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: Keep-Alive\r\n\r\nAGAIN", false},
                                    });

        TKeepAliveHttpClient cl("127.0.0.1", port, TDuration::Seconds(5), TDuration::Seconds(30), true, false, true);

        {
            TStringStream s;
            UNIT_ASSERT_VALUES_EQUAL(200u, cl.DoGet("/ping", &s));
            UNIT_ASSERT_VALUES_EQUAL("full", s.Str());
        }
        {
            TStringStream s;
            UNIT_ASSERT_EXCEPTION(cl.DoGet("/ping", &s), THttpTruncatedBodyException);
            UNIT_ASSERT_VALUES_EQUAL("truncated", s.Str());
        }

        UNIT_ASSERT_VALUES_EQUAL(2u, server.Served());
    }

    // HEAD answers carry Content-Length with no body; strict mode must not read it as truncation.
    Y_UNIT_TEST(strictContentLengthAllowsHeadResponse) {
        TPortManager pm;
        ui16 port = pm.GetPort(80);
        TRawHttpServer server(port, {{"HTTP/1.1 200 OK\r\nContent-Length: 1024\r\nConnection: Keep-Alive\r\n\r\n", false}});

        TKeepAliveHttpClient cl("127.0.0.1", port, TDuration::Seconds(5), TDuration::Seconds(30), true, false, true);

        TStringStream s;
        TKeepAliveHttpClient::THttpCode code = 0;
        UNIT_ASSERT_NO_EXCEPTION(code = cl.DoRequest("HEAD", "/ping", "", &s));
        UNIT_ASSERT_VALUES_EQUAL(200u, code);
        UNIT_ASSERT_VALUES_EQUAL("", s.Str());
    }

    Y_UNIT_TEST(strictContentLengthAllowsRawHeadRequest) {
        TPortManager pm;
        ui16 port = pm.GetPort(80);
        TRawHttpServer server(port, {{"HTTP/1.1 200 OK\r\nContent-Length: 1024\r\nConnection: Keep-Alive\r\n\r\n", false}});

        TKeepAliveHttpClient cl("127.0.0.1", port, TDuration::Seconds(5), TDuration::Seconds(30), true, false, true);

        TStringStream s;
        const TString raw = "HEAD /ping HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 0\r\n\r\n";
        UNIT_ASSERT_NO_EXCEPTION(cl.DoRequestRaw(raw, &s));
        UNIT_ASSERT_VALUES_EQUAL("", s.Str());
    }

    // 304 may carry Content-Length with no body; the status code must survive strict mode.
    Y_UNIT_TEST(strictContentLengthKeepsStatusCodeForNotModified) {
        TPortManager pm;
        ui16 port = pm.GetPort(80);
        TRawHttpServer server(port, {{"HTTP/1.1 304 Not Modified\r\nContent-Length: 42\r\nConnection: Keep-Alive\r\n\r\n", false}});

        TSimpleHttpClient cl(TSimpleHttpClientOptions().Host("127.0.0.1").Port(port).StrictContentLength(true));

        TStringStream s;
        UNIT_ASSERT_EXCEPTION_CONTAINS(cl.DoGet("/ping", &s), THttpRequestException, "304");
    }
}
