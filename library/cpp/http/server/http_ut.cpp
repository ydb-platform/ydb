#include "http.h"
#include "http_ex.h"

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <util/generic/cast.h>
#include <util/stream/output.h>
#include <util/stream/zlib.h>
#include <util/system/datetime.h>
#include <util/system/sem.h>

Y_UNIT_TEST_SUITE(THttpServerTest) {
    class TEchoServer: public THttpServer::ICallBack {
        class TRequest: public THttpClientRequestEx {
        public:
            inline TRequest(TEchoServer* parent)
                : Parent_(parent)
            {
            }

            bool Reply(void* /*tsr*/) override {
                if (!ProcessHeaders()) {
                    return true;
                }

                Output() << "HTTP/1.1 200 Ok\r\n\r\n";
                if (Buf.Size()) {
                    Output().Write(Buf.AsCharPtr(), Buf.Size());
                } else {
                    Output() << Parent_->Res_;
                }
                Output().Finish();

                return true;
            }

        private:
            TEchoServer* Parent_ = nullptr;
        };

    public:
        inline TEchoServer(const TString& res)
            : Res_(res)
        {
        }

        TClientRequest* CreateClient() override {
            return new TRequest(this);
        }

    private:
        TString Res_;
    };

    class TSleepingServer: public THttpServer::ICallBack {
        class TReplier: public TRequestReplier {
        public:
            inline TReplier(TSleepingServer* server)
                : Server(server)
            {
            }

            bool DoReply(const TReplyParams& params) override {
                Server->FreeThread();
                Server->Busy(1);
                params.Output.Write("HTTP/1.0 201 Created\nX-Server: sleeping server\n\nZoooo");
                params.Output.Finish();
                Server->Replies->Inc();
                return true;
            }

        private:
            TSleepingServer* Server = nullptr;
        };

    public:
        inline TSleepingServer(unsigned int size)
            : Semaphore("conns", size)
            , Semaphore2("threads", 1)
            , Replies(new TAtomicCounter())
            , MaxConns(new TAtomicCounter())
        {
        }

        void ResetCounters() {
            Replies.Reset(new TAtomicCounter());
            MaxConns.Reset(new TAtomicCounter());
        }

        long RepliesCount() const {
            return Replies->Val();
        }

        long MaxConnsCount() const {
            return MaxConns->Val();
        }

        TClientRequest* CreateClient() override {
            return new TReplier(this);
        }

        void OnMaxConn() override {
            MaxConns->Inc();
        }

        void OnFailRequest(int) override {
            FreeThread();
            Busy(1);
        }

        void Busy(int count) {
            while (count-- > 0) {
                Semaphore.Acquire();
            }
        }

        void BusyThread() {
            Semaphore2.Acquire();
        }

        void Free(int count) {
            while (count-- > 0) {
                Semaphore.Release();
            }
        }

        void FreeThread() {
            Semaphore2.Release();
        }

    private:
        TSemaphore Semaphore;
        TSemaphore Semaphore2;
        THolder<TAtomicCounter> Replies;
        THolder<TAtomicCounter> MaxConns;
    };

    static const TString CrLf = "\r\n";

    struct TTestRequest {
        TTestRequest(ui16 port, TString content = TString())
            : Port(port)
            , Content(std::move(content))
        {
        }

        void CheckContinue(TSocketInput& si) {
            if (Expect100Continue) {
                TStringStream ss;
                TString firstLine;
                si.ReadLine(firstLine);
                for (;;) {
                    TString buf;
                    si.ReadLine(buf);
                    if (buf.size() == 0) {
                        break;
                    }
                    ss << buf << CrLf;
                }
                UNIT_ASSERT_EQUAL(firstLine, "HTTP/1.1 100 Continue");
            }
        }

        TString Execute() {
            TSocket* s = nullptr;
            THolder<TSocket> singleReqSocket;
            if (KeepAliveConnection) {
                if (!KeepAlivedSocket) {
                    KeepAlivedSocket = MakeHolder<TSocket>(TNetworkAddress("localhost", Port), TDuration::Seconds(10));
                }
                s = KeepAlivedSocket.Get();
            } else {
                TNetworkAddress addr("localhost", Port);
                singleReqSocket.Reset(new TSocket(addr, TDuration::Seconds(10)));
                s = singleReqSocket.Get();
            }
            bool isPost = Type == "POST";
            TSocketInput si(*s);

            if (UseHttpOutput) {
                TSocketOutput so(*s);
                THttpOutput output(&so);

                output.EnableKeepAlive(KeepAliveConnection);
                output.EnableCompression(EnableResponseEncoding);

                TStringStream r;
                r << Type << " / HTTP/1.1" << CrLf;
                r << "Host: localhost:" + ToString(Port) << CrLf;
                if (isPost) {
                    if (ContentEncoding.size()) {
                        r << "Content-Encoding: " << ContentEncoding << CrLf;
                    } else {
                        r << "Transfer-Encoding: chunked" << CrLf;
                    }
                    if (Expect100Continue) {
                        r << "Expect: 100-continue" << CrLf;
                    }
                }

                r << CrLf;
                if (isPost) {
                    output.Write(r.Str());
                    output.Flush();
                    CheckContinue(si);
                    output.Write(Content);
                    output.Finish();
                } else {
                    output.Write(r.Str());
                    output.Finish();
                }
            } else {
                TStringStream r;
                r << Type << " / HTTP/1.1" << CrLf;
                r << "Host: localhost:" + ToString(Port) << CrLf;
                if (KeepAliveConnection) {
                    r << "Connection: Keep-Alive" << CrLf;
                } else {
                    r << "Connection: Close" << CrLf;
                }
                if (EnableResponseEncoding) {
                    r << "Accept-Encoding: gzip, deflate, x-gzip, x-deflate, y-lzo, y-lzf, y-lzq, y-bzip2, y-lzma" << CrLf;
                }
                if (isPost && Expect100Continue) {
                    r << "Expect: 100-continue" << CrLf;
                }
                if (isPost && ContentEncoding.size() && Content.size()) {
                    r << "Content-Encoding: " << ContentEncoding << CrLf;
                    TStringStream compressedContent;
                    {
                        TZLibCompress zlib(&compressedContent);
                        zlib.Write(Content.data(), Content.size());
                        zlib.Flush();
                        zlib.Finish();
                    }
                    r << "Content-Length: " << compressedContent.Size() << CrLf;
                    r << CrLf;
                    s->Send(r.Data(), r.Size());
                    CheckContinue(si);
                    Hdr = r.Str();
                    TString tosend = compressedContent.Str();
                    s->Send(tosend.data(), tosend.size());
                } else {
                    if (isPost) {
                        r << "Content-Length: " << Content.size() << CrLf;
                        r << CrLf;
                        s->Send(r.Data(), r.Size());
                        CheckContinue(si);
                        Hdr = r.Str();
                        s->Send(Content.data(), Content.size());
                    } else {
                        r << CrLf;
                        Hdr = r.Str();
                        s->Send(r.Data(), r.Size());
                    }
                }
            }

            THttpInput input(&si);
            TStringStream ss;
            TransferData(&input, &ss);

            return ss.Str();
        }

        TString GetDescription() const {
            if (UseHttpOutput) {
                TStringStream ss;
                ss << (KeepAliveConnection ? "keep-alive " : "") << Type;
                if (ContentEncoding.size()) {
                    ss << " with encoding=" << ContentEncoding;
                }
                return ss.Str();
            } else {
                return Hdr;
            }
        }

        ui16 Port = 0;
        bool UseHttpOutput = true;
        TString Type = "GET";
        TString ContentEncoding;
        TString Content;
        bool KeepAliveConnection = false;
        THolder<TSocket> KeepAlivedSocket;
        bool EnableResponseEncoding = false;
        TString Hdr;
        bool Expect100Continue = false;
    };

    class TFailingMtpQueue: public TSimpleThreadPool {
    private:
        bool FailOnAdd_ = false;

    public:
        void SetFailOnAdd(bool fail = true) {
            FailOnAdd_ = fail;
        }
        [[nodiscard]] bool Add(IObjectInQueue* pObj) override {
            if (FailOnAdd_) {
                return false;
            }

            return TSimpleThreadPool::Add(pObj);
        }
        TFailingMtpQueue() = default;
        TFailingMtpQueue(IThreadFactory* pool)
            : TSimpleThreadPool(pool)
        {
        }
    };

    TString TestData(size_t size = 5 * 4096) {
        TString res;

        for (size_t i = 0; i < size; ++i) {
            res += (char)i;
        }
        return res;
    }

    Y_UNIT_TEST(TestEchoServer) {
        TString res = TestData();
        TPortManager pm;
        const ui16 port = pm.GetPort();
        const bool trueFalse[] = {true, false};

        TEchoServer serverImpl(res);
        THttpServer server(&serverImpl, THttpServer::TOptions(port).EnableKeepAlive(true).EnableCompression(true));

        for (int i = 0; i < 2; ++i) {
            UNIT_ASSERT(server.Start());

            TTestRequest r(port);
            r.Content = res;

            for (bool keepAlive : trueFalse) {
                r.KeepAliveConnection = keepAlive;

                // THttpOutput use chunked stream, else use Content-Length
                for (bool useHttpOutput : trueFalse) {
                    r.UseHttpOutput = useHttpOutput;

                    for (bool enableResponseEncoding : trueFalse) {
                        r.EnableResponseEncoding = enableResponseEncoding;

                        const TString reqTypes[] = {"GET", "POST"};
                        for (const TString& reqType : reqTypes) {
                            r.Type = reqType;

                            const TString encoders[] = {"", "deflate"};
                            for (const TString& encoder : encoders) {
                                r.ContentEncoding = encoder;

                                for (bool expect100Continue : trueFalse) {
                                    r.Expect100Continue = expect100Continue;
                                    TString resp = r.Execute();
                                    UNIT_ASSERT_C(resp == res, "diff echo response for request:\n" + r.GetDescription());
                                }
                            }
                        }
                    }
                }
            }

            server.Stop();
        }
    }

    Y_UNIT_TEST(TestReusePortEnabled) {
        if (!IsReusePortAvailable()) {
            return; // skip test
        }
        TString res = TestData();
        TPortManager pm;
        const ui16 port = pm.GetPort();

        TEchoServer serverImpl(res);
        TVector<THolder<THttpServer>> servers;
        for (ui32 i = 0; i < 10; i++) {
            servers.push_back(MakeHolder<THttpServer>(&serverImpl, THttpServer::TOptions(port).EnableReusePort(true)));
        }

        for (ui32 testRun = 0; testRun < 3; testRun++) {
            for (auto& server : servers) {
                // start servers one at a time and check at least one of them is replying
                UNIT_ASSERT(server->Start());

                TTestRequest r(port, res);
                UNIT_ASSERT_C(r.Execute() == res, "diff echo response for request:\n" + r.GetDescription());
            }

            for (auto& server : servers) {
                // ping servers and stop them one at a time
                // at the last iteration only one server is still working and then gets stopped as well

                TTestRequest r(port, res);
                UNIT_ASSERT_C(r.Execute() == res, "diff echo response for request:\n" + r.GetDescription());

                server->Stop();
            }
        }
    }

    Y_UNIT_TEST(TestReusePortDisabled) {
        // check that with the ReusePort option disabled it's impossible to start two servers on the same port
        // check that ReusePort option is disabled by default (don't set it explicitly in the test)
        TPortManager pm;
        const ui16 port = pm.GetPort();

        TEchoServer serverImpl(TString{});
        THttpServer server1(&serverImpl, THttpServer::TOptions(port));
        THttpServer server2(&serverImpl, THttpServer::TOptions(port));

        UNIT_ASSERT(true == server1.Start());
        UNIT_ASSERT(false == server2.Start());

        server1.Stop();
        // Stop() is a sync call, port should be free by now
        UNIT_ASSERT(true == server2.Start());
        UNIT_ASSERT(false == server1.Start());
    }

    Y_UNIT_TEST(TestFailServer) {
        /**
         * Emulate request processing failures
         * Data should be large enough not to fit into socket buffer
         **/
        TString res = TestData(10 * 1024 * 1024);
        TPortManager portManager;
        const ui16 port = portManager.GetPort();
        TEchoServer serverImpl(res);
        THttpServer::TOptions options(port);
        options.EnableKeepAlive(true);
        options.EnableCompression(true);
        using TFailingServerMtpQueue = TThreadPoolBinder<TFailingMtpQueue, THttpServer::ICallBack>;
        THttpServer::TMtpQueueRef mainWorkers = new TFailingServerMtpQueue(&serverImpl, SystemThreadFactory());
        THttpServer::TMtpQueueRef failWorkers = new TThreadPool(SystemThreadFactory());
        THttpServer server(&serverImpl, mainWorkers, failWorkers, options);

        UNIT_ASSERT(server.Start());
        for (size_t i = 0; i < 3; ++i) {
            // should fail on 2nd request
            static_cast<TFailingMtpQueue*>(mainWorkers.Get())->SetFailOnAdd(i == 1);
            TTestRequest r(port);
            r.Content = res;
            r.Type = "POST";
            TString resp = r.Execute();
            if (i == 1) {
                UNIT_ASSERT(resp.Contains("Service Unavailable"));
            } else {
                UNIT_ASSERT_C(resp == res, "diff echo response for request:\n" + r.GetDescription());
            }
        }
        server.Stop();
    }

    class TReleaseConnectionServer: public THttpServer::ICallBack {
        class TRequest: public THttpClientRequestEx {
        public:
            bool Reply(void* /*tsr*/) override {
                Output() << "HTTP/1.1 200 Ok\r\n\r\n";
                Output() << "reply";
                Output().Finish();

                ReleaseConnection();

                throw yexception() << "some error";

                return true;
            }
        };

    public:
        TClientRequest* CreateClient() override {
            return new TRequest();
        }

        void OnException() override {
            ExceptionMessage = CurrentExceptionMessage();
        }

        TString ExceptionMessage;
    };

    class TResetConnectionServer: public THttpServer::ICallBack {
        class TRequest: public TClientRequest {
        public:
            bool Reply(void* /*tsr*/) override {
                Output() << "HTTP/1.1";
                ResetConnection();

                return true;
            }
        };

    public:
        TClientRequest* CreateClient() override {
            return new TRequest();
        }

        void OnException() override {
            ExceptionMessage = CurrentExceptionMessage();
        }

        TString ExceptionMessage;
    };

    class TListenerSockAddrReplyServer: public THttpServer::ICallBack {
        class TRequest: public TClientRequest {
        public:
            bool Reply(void* /*tsr*/) override {
                Output() << "HTTP/1.1 200 Ok\r\n\r\n";
                Output() << PrintHostAndPort(*GetListenerSockAddrRef());

                Output().Finish();

                return true;
            }
        };

        public:
        TClientRequest* CreateClient() override {
            return new TRequest();
        }
    };

    Y_UNIT_TEST(TTestResetConnection) {
        TPortManager pm;
        const ui16 port = pm.GetPort();

        TResetConnectionServer serverImpl;
        THttpServer server(&serverImpl, THttpServer::TOptions(port));
        UNIT_ASSERT(server.Start());

        TTestRequest r(port, "request");

        UNIT_ASSERT_EXCEPTION_CONTAINS(r.Execute(), TSystemError, "Connection reset by peer");

        server.Stop();
    }

    Y_UNIT_TEST(TTestReleaseConnection) {
        TPortManager pm;
        const ui16 port = pm.GetPort();

        TReleaseConnectionServer serverImpl;
        THttpServer server(&serverImpl, THttpServer::TOptions(port).EnableKeepAlive(true));
        UNIT_ASSERT(server.Start());

        TTestRequest r(port, "request");
        r.KeepAliveConnection = true;

        UNIT_ASSERT_C(r.Execute() == "reply", "diff echo response for request:\n" + r.GetDescription());

        server.Stop();

        UNIT_ASSERT_STRINGS_EQUAL(serverImpl.ExceptionMessage, "(yexception) some error");
    }

    THttpInput SendRequest(TSocket& socket, ui16 port) {
        TSocketInput si(socket);
        TSocketOutput so(socket);
        THttpOutput out(&so);
        out.EnableKeepAlive(true);
        out << "GET / HTTP/1.1" << CrLf;
        out << "Host: localhost:" + ToString(port) << CrLf;
        out << CrLf;
        out.Flush();

        THttpInput input(&si);
        input.ReadAll();
        return input;
    }

    THttpInput SendRequestWithBody(TSocket& socket, ui16 port, TString body) {
        TSocketInput si(socket);
        TSocketOutput so(socket);
        THttpOutput out(&so);
        out << "POST / HTTP/1.1" << CrLf;
        out << "Host: localhost:" + ToString(port) << CrLf;
        out << "Content-Length: " + ToString(body.size()) << CrLf;
        out << CrLf;
        out << body;
        out.Flush();

        THttpInput input(&si);
        input.ReadAll();
        return input;
    }

    Y_UNIT_TEST(TTestExpirationTimeout) {
        TPortManager pm;
        const ui16 port = pm.GetPort();

        TEchoServer serverImpl("test_data");
        THttpServer::TOptions options(port);
        options.nThreads = 1;
        options.MaxQueueSize = 0;
        options.MaxConnections = 0;
        options.KeepAliveEnabled = true;
        options.ExpirationTimeout = TDuration::Seconds(1);
        options.PollTimeout = TDuration::MilliSeconds(100);
        THttpServer server(&serverImpl, options);
        UNIT_ASSERT(server.Start());

        TSocket socket(TNetworkAddress("localhost", port), TDuration::Seconds(10));

        SendRequest(socket, port);
        SendRequest(socket, port);

        Sleep(TDuration::Seconds(5));
        UNIT_ASSERT_EXCEPTION(SendRequest(socket, port), THttpReadException);

        server.Stop();
    }

    Y_UNIT_TEST(TTestContentLengthTooLarge) {
        TPortManager pm;
        const ui16 port = pm.GetPort();

        TEchoServer serverImpl("test_data");
        THttpServer::TOptions options(port);
        options.nThreads = 1;
        options.MaxQueueSize = 0;
        options.MaxInputContentLength = 2_KB;
        options.MaxConnections = 0;
        options.KeepAliveEnabled = false;
        options.ExpirationTimeout = TDuration::Seconds(1);
        options.PollTimeout = TDuration::MilliSeconds(100);
        THttpServer server(&serverImpl, options);
        UNIT_ASSERT(server.Start());

        TSocket socket(TNetworkAddress("localhost", port), TDuration::Seconds(5));
        UNIT_ASSERT_STRING_CONTAINS(SendRequestWithBody(socket, port, TString(1_KB, 'a')).FirstLine(), "HTTP/1.1 200 Ok");

        TSocket socket2(TNetworkAddress("localhost", port), TDuration::Seconds(5));
        UNIT_ASSERT_STRING_CONTAINS(SendRequestWithBody(socket2, port, TString(10_KB, 'a')).FirstLine(), "HTTP/1.1 413 Payload Too Large");

        server.Stop();
    }

    Y_UNIT_TEST(TTestNullInRequest) {
        TPortManager pm;
        const ui16 port = pm.GetPort();

        TEchoServer serverImpl("test_data");
        THttpServer::TOptions options(port);
        options.nThreads = 1;
        options.MaxQueueSize = 0;
        options.MaxConnections = 0;
        options.KeepAliveEnabled = false;
        options.ExpirationTimeout = TDuration::Seconds(1);
        options.PollTimeout = TDuration::MilliSeconds(100);
        THttpServer server(&serverImpl, options);
        UNIT_ASSERT(server.Start());

        TSocket socket(TNetworkAddress("localhost", port), TDuration::Seconds(5));


        TSocketInput si(socket);
        TSocketOutput so(socket);
        THttpOutput out(&so);
        out << "GET \0/ggg HTTP/1.1" << CrLf;
        out << "Host: localhost:" + ToString(port) << CrLf;
        out << CrLf;
        out.Flush();

        THttpInput input(&si);
        input.ReadAll();

        UNIT_ASSERT_STRING_CONTAINS(input.FirstLine(), "HTTP/1.1 4");
        server.Stop();
    }


    Y_UNIT_TEST(TTestCloseConnectionOnRequestLimit) {
        TPortManager pm;
        const ui16 port = pm.GetPort();

        TEchoServer serverImpl("test_data");
        THttpServer server(&serverImpl, THttpServer::TOptions(port).EnableKeepAlive(true).SetMaxRequestsPerConnection(2));
        UNIT_ASSERT(server.Start());

        TSocket socket(TNetworkAddress("localhost", port), TDuration::Seconds(10));

        UNIT_ASSERT(SendRequest(socket, port).IsKeepAlive());
        UNIT_ASSERT(!SendRequest(socket, port).IsKeepAlive());

        UNIT_ASSERT_EXCEPTION(SendRequest(socket, port), THttpReadException);

        server.Stop();
    }

    Y_UNIT_TEST(TTestListenerSockAddrConnection) {
        TPortManager pm;
        const ui16 port1 = pm.GetPort();
        const ui16 port2 = pm.GetPort();

        TListenerSockAddrReplyServer serverImpl;
        THttpServer server(&serverImpl, THttpServer::TOptions().EnableKeepAlive(true).AddBindAddress("127.0.0.1", port1).AddBindAddress("127.0.0.1", port2));
        UNIT_ASSERT(server.Start());

        TTestRequest r1(port1);
        r1.KeepAliveConnection = true;

        TString resp = r1.Execute();
        UNIT_ASSERT(resp == TString::Join("127.0.0.1", ":", ToString(port1)));

        TTestRequest r2(port2);
        r2.KeepAliveConnection = true;

        resp = r2.Execute();
        UNIT_ASSERT(resp == TString::Join("127.0.0.1", ":", ToString(port2)));

        server.Stop();
    }

#if 0
    Y_UNIT_TEST(TestSocketsLeak) {
        const bool trueFalse[] = {true, false};
        TPortManager portManager;
        const ui16 port = portManager.GetPort();
        TString res = TestData(25);
        TSleepingServer server(3);
        THttpServer::TOptions options(port);
        options.MaxConnections = 1;
        options.MaxQueueSize = 1;
        options.MaxFQueueSize = 2;
        options.nFThreads = 2;
        options.KeepAliveEnabled = true;
        options.RejectExcessConnections = true;
        THttpServer srv(&server, options);
        UNIT_ASSERT(srv.Start());

        for (bool keepAlive : trueFalse) {
            server.ResetCounters();
            TVector<TAutoPtr<IThreadFactory::IThread>> threads;

            server.Busy(3);
            server.BusyThread();

            for (size_t i = 0; i < 3; ++i) {
                auto func = [&server, port, keepAlive]() {
                    server.BusyThread();
                    THolder<TTestRequest> r = MakeHolder<TTestRequest>(port);
                    r->KeepAliveConnection = keepAlive;
                    r->Execute();
                };
                threads.push_back(SystemThreadFactory()->Run(func));
            }

            server.FreeThread(); // all threads get connection & go to processing
            Sleep(TDuration::MilliSeconds(100));
            server.BusyThread(); // we wait while connections are established by the
                                 // system and accepted by the server
            server.Free(3);      // we release all connections processing

            for (auto&& thread : threads) {
                thread->Join();
            }

            server.Free(3);
            server.FreeThread();

            UNIT_ASSERT_EQUAL_C(server.MaxConnsCount(), 2, "we should get MaxConn notification 2 times, got " + ToString(server.MaxConnsCount()));
            UNIT_ASSERT_EQUAL_C(server.RepliesCount(), 1, "only one request should have been processed, got " + ToString(server.RepliesCount()));
        }
    }
#endif
}
