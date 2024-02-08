#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <library/cpp/http/io/stream.h>
#include <library/cpp/http/server/http_ex.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_value.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <util/stream/null.h>
#include <util/system/event.h>

#include "proto_ready_actor.h"
#include "name_service_client_protocol.h"
#include "connect_socket_protocol.h"
#include "send_data_protocol.h"
#include "read_http_reply_protocol.h"
#include "http_request_protocol.h"

using namespace NActors;

auto& Ctest = Cnull;
//auto& Ctest = Cerr;

namespace {
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

    using TFailingServerMtpQueue =
        TThreadPoolBinder<TFailingMtpQueue, THttpServer::ICallBack>;

    class THTTP200OkServer: public THttpServer::ICallBack {
        class TRequest: public THttpClientRequestEx {
        public:
            inline TRequest(THTTP200OkServer* parent)
                : Parent_(parent)
            {
            }

            bool Reply(void* /*tsr*/) override {
                if (!ProcessHeaders()) {
                    return true;
                }

                if (strncmp(RequestString.data(), "GET /hosts HTTP/1.", 18) == 0) {
                    TString list = Sprintf("[\"localhost\"]");
                    Output() << "HTTP/1.1 200 Ok\r\n";
                    Output() << "Connection: close\r\n";
                    Output() << "Content-Length: " << list.size() << "\r\n";
                    Output() << "\r\n";
                    Output() << list;
                    return true;
                }

                Output() << "HTTP/1.1 200 Ok\r\n";
                if (Buf.Size()) {
                    Output() << "Content-Length: " << Buf.Size() << "\r\n\r\n";
                    Output().Write(Buf.AsCharPtr(), Buf.Size());
                } else {
                    Output() << "Content-Length: " << (Parent_->Res_).size()
                             << "\r\n\r\n";
                    Output() << Parent_->Res_;
                }
                Output().Finish();

                return true;
            }

        private:
            THTTP200OkServer* Parent_ = nullptr;
        };

    public:
        inline THTTP200OkServer(TString res)
            : Res_(std::move(res))
        {
        }

        TClientRequest* CreateClient() override {
            return new TRequest(this);
        }

    private:
        TString Res_;
    };
}

Y_UNIT_TEST_SUITE(TestProtocols) {
    class TResolveTester
        : public TProtoReadyActor<TActorBootstrapped, TResolveTester>
        , public TResolveClientProtocol
    {
    public:
        void Bootstrap(const TActorContext& ctx) {
            Become(&TThis::DefaultFunc);
            SendResolveMessage(this, ctx, "localhost", 8080);
        }

        STFUNC(DefaultFunc) {
            Y_UNUSED(ev);
        }

        void CatchHostAddress(
                const TActorContext& ctx,
                NAddr::IRemoteAddrPtr address) noexcept override
        {
            Address = std::move(address);
            ctx.Send(Edge, new TEvents::TEvWakeup);
        }

        void CatchResolveError(
                const TActorContext& ctx, TString error) noexcept override
        {
            Error = std::move(error);
            ctx.Send(Edge, new TEvents::TEvWakeup);
        }

        NAddr::IRemoteAddrPtr Address;
        TString Error;
        TActorId Edge;
    };


    Y_UNIT_TEST(TestResolveProtocol) {
        for (size_t i = 0; i < 10; ++i) {
            TTestBasicRuntime runtime(2);
            runtime.Initialize(NKikimr::TAppPrepare().Unwrap());

            auto tester = new TResolveTester;
            tester->Edge = runtime.AllocateEdgeActor(0);
            runtime.Register(tester, 0);

            TAutoPtr<IEventHandle> handle;
            runtime.GrabEdgeEvent<TEvents::TEvWakeup>(handle);

            if (NAddr::PrintHostAndPort(*tester->Address) ==
                "[::1]:8080") {
                return;
            }
        }
        UNIT_FAIL("Could not get ip address of yandex.ru in 10 tries");
    }


    class TConnectTester
        : public TProtoReadyActor<TActorBootstrapped, TConnectTester>
        , public TResolveClientProtocol
        , public TConnectSocketProtocol
    {
    public:
        void Bootstrap(const TActorContext& ctx) {
            Become(&TThis::DefaultFunc);
            SendResolveMessage(this, ctx, "localhost", Port);
        }

        STFUNC(DefaultFunc) {
            Y_UNUSED(ev);
        }

        void CatchHostAddress(
                const TActorContext& ctx,
                NAddr::IRemoteAddrPtr address) noexcept override
        {
            Address = std::move(address);
            ConnectSocket(this, ctx, Address);
        }

        void CatchResolveError(
                const TActorContext& ctx, TString error) noexcept override
        {
            Error = std::move(error);
            ctx.Send(Edge, new TEvents::TEvWakeup);
        }

        void CatchConnectError(
                const TActorContext& ctx, TString error) noexcept override
        {
            Error = std::move(error);
            ctx.Send(Edge, new TEvents::TEvWakeup);
        }

        void CatchConnectedSocket(
                const TActorContext& ctx,
                TIntrusivePtr<NInterconnect::TStreamSocket> socket
                    ) noexcept override
        {
            Socket = std::move(socket);
            ctx.Send(Edge, new TEvents::TEvWakeup);
        }


        NAddr::IRemoteAddrRef Address;
        TString Error;
        TActorId Edge;
        TIntrusivePtr<NInterconnect::TStreamSocket> Socket;
        ui64 Port;
    };


    Y_UNIT_TEST(TestConnectProtocol) {
        TPortManager portManager;
        const ui16 port = portManager.GetPort();
        THTTP200OkServer serverImpl("");

        THttpServer::TOptions options(port);
        THttpServer::TMtpQueueRef mainWorkers =
            new TFailingServerMtpQueue(&serverImpl, SystemThreadFactory());
        THttpServer::TMtpQueueRef failWorkers =
            new TThreadPool(SystemThreadFactory());

        THttpServer server(&serverImpl, mainWorkers, failWorkers, options);
        UNIT_ASSERT(server.Start());

        TTestBasicRuntime runtime(2);
        runtime.Initialize(NKikimr::TAppPrepare().Unwrap());

        auto tester = new TConnectTester;
        tester->Port = port;
        tester->Edge = runtime.AllocateEdgeActor(0);
        runtime.Register(tester, 0);

        TAutoPtr<IEventHandle> handle;
        runtime.GrabEdgeEvent<TEvents::TEvWakeup>(handle);

        UNIT_ASSERT_EQUAL(NAddr::PrintHostAndPort(*tester->Address),
                          Sprintf("[::1]:%u", port));
        UNIT_ASSERT(tester->Socket.Get() != nullptr);
        UNIT_ASSERT(*tester->Socket != -1);
    }


    class THTTPTester
        : public TProtoReadyActor<TActorBootstrapped, THTTPTester>
        , public TResolveClientProtocol
        , public TConnectSocketProtocol
        , public TSendDataProtocol
        , public TReadHTTPReplyProtocol
    {
    private:
        friend class TActorBootstrapped<THTTPTester>;
        friend class TResolveClientProtocol;
        friend class TConnectSocketProtocol;
        friend class TSendDataProtocol;
        friend class TReadHTTPReplyProtocol;

        void Bootstrap(const TActorContext& ctx) {
            Become(&TThis::DefaultFunc);
            SendResolveMessage(this, ctx, "localhost", Port);
        }

        STFUNC(DefaultFunc) {
            Y_UNUSED(ev);
        }

        void CatchHostAddress(
                const TActorContext& ctx,
                NAddr::IRemoteAddrPtr address) noexcept override
        {
            ConnectSocket(this, ctx, std::move(address));
        }

        void CatchResolveError(
                const TActorContext& ctx, TString error) noexcept override
        {
            Error = std::move(error);
            ctx.Send(Edge, new TEvents::TEvWakeup);
        }

        void CatchConnectError(
                const TActorContext& ctx, TString error) noexcept override
        {
            Error = std::move(error);
            ctx.Send(Edge, new TEvents::TEvWakeup);
        }

        void CatchConnectedSocket(
                const TActorContext& ctx,
                TIntrusivePtr<NInterconnect::TStreamSocket> socket
                    ) noexcept override
        {
            Socket = std::move(socket);
            const char* request =
                "GET / HTTP/1.1\r\n"
                "Host: yandex.ru\r\n"
                "Connection: closer\r\n"
                "\r\n";

            SendData(this, ctx, Socket.Get(), request, strlen(request));
        }

        void CatchSendDataError(TString error) noexcept override
        {
            Error = std::move(error);
            Send(Edge, new TEvents::TEvWakeup);
        }

        void CatchReadDataError(TString error) noexcept override
        {
            Error = std::move(error);
            Send(Edge, new TEvents::TEvWakeup);
        }

        void CatchSendDataComplete(const TActorContext& ctx) noexcept override {
            ReadHTTPReply<THTTPTester>(this, ctx, Socket);
        }

        void CatchHTTPReply(
                const TActorContext& ctx,
                TVector<char> buf,
                size_t httpMessageSize) noexcept override
        {
            Y_UNUSED(httpMessageSize);
            Data.assign(buf.begin(), buf.end());
            ctx.Send(Edge, new TEvents::TEvWakeup);
        }

    public:
        TString Error;
        TString Data;
        TActorId Edge;
        ui16 Port;

    private:
        TIntrusivePtr<NInterconnect::TStreamSocket> Socket;
    };


    Y_UNIT_TEST(TestHTTPCollected) {
        TPortManager portManager;
        const ui16 port = portManager.GetPort();
        THTTP200OkServer serverImpl("");

        THttpServer::TOptions options(port);
        THttpServer::TMtpQueueRef mainWorkers =
            new TFailingServerMtpQueue(&serverImpl, SystemThreadFactory());
        THttpServer::TMtpQueueRef failWorkers =
            new TThreadPool(SystemThreadFactory());

        THttpServer server(&serverImpl, mainWorkers, failWorkers, options);
        UNIT_ASSERT(server.Start());

        TTestBasicRuntime runtime(2);
        runtime.Initialize(NKikimr::TAppPrepare().Unwrap());

        auto tester = new THTTPTester;
        tester->Edge = runtime.AllocateEdgeActor(0);
        tester->Port = port;
        runtime.Register(tester, 0);

        TAutoPtr<IEventHandle> handle;
        runtime.GrabEdgeEvent<TEvents::TEvWakeup>(handle);
    }


    /**
     * Server that replies to a single request very very slowly
     */
    static void VerySlowServer(ui16 port, TManualEvent& listening) {
        TSockAddrInet6 listenAddr("::", port);

        TInet6StreamSocket listener;
        listener.CheckSock();
        SetReuseAddressAndPort(listener);
        TBaseSocket::Check(listener.Bind(&listenAddr), "bind");
        TBaseSocket::Check(listener.Listen(5), "listen");
        listening.Signal();
        Ctest << "Listening on port " << port << Endl;

        TStreamSocket client;
        TBaseSocket::Check(listener.Accept(&client), "accept");
        Ctest << "Accepted connection" << Endl;

        SetNoDelay(client, true);

        // Pretend to read the request
        TStreamSocketInput input(&client);
        for (;;) {
            auto line = input.ReadLine();
            Ctest << "Received: " << line << Endl;
            if (line.empty()) {
                break;
            }
        }

        // It would take ~2 seconds to send the reply
        TString reply("HTTP/1.1 200 Ok\r\nContent-Length: 0\r\nX-Header: aaa");
        reply.resize(2048 + 1024 * 3 - 4, 'a');
        reply.append("\r\n\r\n");
        client.Send(reply.data(), 17); // 1 byte less than MINIMUM_HTTP_REPLY_SIZE
        Sleep(TDuration::MilliSeconds(500));
        client.Send(reply.data() + 17, 2048 - 17); // fill the initial buffer
        Sleep(TDuration::MilliSeconds(500));
        client.Send(reply.data() + 2048, 1024); // fill half the buffer (not enough to grow)
        Sleep(TDuration::MilliSeconds(500));
        client.Send(reply.data() + 3072, 1024); // fill the rest of the buffer
        Sleep(TDuration::MilliSeconds(500));
        client.Send(reply.data() + 4096, 1024); // fill the rest of the buffer
    }


    /**
     * Regression test for KIKIMR-7944
     */
    Y_UNIT_TEST(TestHTTPCollectedVerySlow) {
        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        TManualEvent listening;
        auto serverThread = SystemThreadFactory()->Run([port, &listening](){
            VerySlowServer(port, listening);
        });
        listening.WaitI();

        TTestBasicRuntime runtime(2);
        runtime.Initialize(NKikimr::TAppPrepare().Unwrap());

        auto tester = new THTTPTester;
        tester->Edge = runtime.AllocateEdgeActor(0);
        tester->Port = port;
        runtime.Register(tester, 0);

        TAutoPtr<IEventHandle> handle;
        runtime.GrabEdgeEvent<TEvents::TEvWakeup>(handle);

        UNIT_ASSERT_VALUES_EQUAL(tester->Error, "");
    }


    class THTTPRequestTester
        : public TProtoReadyActor<TActorBootstrapped, THTTPRequestTester>
        , public THTTPRequestProtocol<THTTPRequestTester>
        , public TReadDataProtocol<THTTPRequestTester>
    {
        friend class TReadDataProtocol<THTTPRequestTester>;
        friend class TActorBootstrapped<THTTPRequestTester>;
        friend class THTTPRequestProtocol<THTTPRequestTester>;

        enum class EState {
            READ_PROXY_LIST,
            EXPECT_CONTINUE,
            WRITE_DATA_COMPLETE,
        };

        EState State = EState::READ_PROXY_LIST;

    private:
        void Bootstrap(const TActorContext& ctx) {
            Become(&TThis::DefaultFunc);
            HTTPRequest(this, ctx,
                "localhost",
                "GET /hosts",
                "Accept: application/json\r\n",
                Port);
        }

        STFUNC(DefaultFunc) {
            switch (ev->GetTypeRewrite()) {
                HFunc(TEvHTTPSendContent, YetMoreContent);
            }
        }

        void YetMoreContent(TEvHTTPSendContent::TPtr& ev,
                            const TActorContext& ctx) noexcept
        {
            if (State != EState::WRITE_DATA_COMPLETE)
                Y_ABORT("Stream is not ready to write data");

            TEvHTTPSendContent const* msg = ev->Get();
            HTTPWriteContent(this, ctx, msg->Data, msg->Len,msg->Last);
        }

        void CatchHTTPRequestError(TString error) noexcept override
        {
            Error = std::move(error);
            exit(1);
        }


        void CatchHTTPReply(
                const TActorContext& ctx,
                TVector<char> buf,
                size_t httpMessageSize) noexcept override
        {
            Ctest << "HTTP message size: " << httpMessageSize << Endl;
            Ctest << "Total read size: " << buf.size() - 1 << Endl;
            TMemoryInput bufStream(buf.data(), httpMessageSize);
            THttpInput httpMsg(&bufStream);
            httpMsg.Headers().OutTo(&Ctest);

            switch (State) {
            case EState::READ_PROXY_LIST:
                CatchProxyList(ctx, std::move(buf), httpMessageSize, httpMsg);
                break;
            case EState::EXPECT_CONTINUE:
                CatchWriteData(ctx, httpMsg);
                break;
            case EState::WRITE_DATA_COMPLETE:
                HTTPWriteComplete(ctx, httpMsg);
                break;
            }
        }

        void CatchProxyList(
                const TActorContext& ctx,
                TVector<char> buf,
                size_t httpMessageSize,
                THttpInput& httpMsg) noexcept
        {
            do {
                auto status = ParseHttpRetCode(httpMsg.FirstLine());

                if (status < 200 || status >= 300) {
                    Error = "bad http reply status";
                    break;
                }

                if (!httpMsg.HasContent()) {
                    Error = "reply has no content";
                    break;
                }

                ui64 value;
                if (httpMsg.GetContentLength(value)) {
                    Ctest << "Content length is: " << value << Endl;
                    value -= buf.size() - 1 - httpMessageSize;
                    Ctest << "Yet to read: " << value << Endl;
                    Buf = std::move(buf);
                    Buf.erase(Buf.begin(), Buf.begin() + httpMessageSize);
                    Buf.pop_back();
                    if (value == 0) {
                        CatchHTTPContent(ctx, TVector<char>());
                        return;
                    }
                    HTTPReadContent(this, ctx, value);
                    return;
                } else {
                }
            } while (0);
            Ctest << "Got an error: " << Error << Endl;
            exit(1);
        }


        void CatchWriteData(
                const TActorContext& ctx,
                THttpInput& httpMsg) noexcept
        {
            do {
                auto status = ParseHttpRetCode(httpMsg.FirstLine());

                if (status < 100 || status >= 200) {
                    Error = "bad http reply status";
                    break;
                }

                if (httpMsg.HasContent()) {
                    Error = "reply must not have content";
                    break;
                }

                Ctest << "Ready to send PUT content" << Endl;

                auto msg = new TEvHTTPStreamStatus;
                msg->Status = TEvHTTPStreamStatus::READY;
                ctx.Send(Edge, msg);
                State = EState::WRITE_DATA_COMPLETE;
                HTTPExpectReply(this, ctx);
                return;
            } while (0);
            Ctest << "Got an error: " << Error << Endl;
            exit(1);
        }

        void CatchHTTPContent(
                const TActorContext& ctx, TVector<char> buf) noexcept override
        {
            Buf.insert(Buf.end(), buf.begin(), buf.end());
            buf.clear();
            Ctest << "Content complete" << Endl;
            Ctest << TString(Buf.data(), Buf.size()) << Endl;

            do {
                TMemoryInput input(Buf.data(), Buf.size());
                NJson::TJsonValue jsonValue;

                if (!NJson::ReadJsonTree(&input, &jsonValue))
                    break;

                if (!jsonValue.IsArray())
                    break;

                auto result = jsonValue.GetArray();

                Ctest << "First proxy is: " << result.front() << Endl;

                TString headers =
                    "Accept: application/json\r\n"
                    "Content-Type: text/tab-separated-values\r\n"
                    "Transfer-Encoding: chunked\r\n"
                    "Expect: 100-continue\r\n"
                    "Authorization:"
                    " OAuth fake-token\r\n";

                HTTPRequest(this, ctx,
                    result.front().GetString(),
                    "PUT /api/v3/write_table?path=//tmp/agri-ws/test",
                    headers,
                    Port);

                State = EState::EXPECT_CONTINUE;
                return;
            } while (0);
            Ctest << "Damn! Something is wrong" << Endl;
        }

        void HTTPWriteComplete(
                const TActorContext& ctx,
                THttpInput& httpMsg) noexcept
        {
            do {
                auto status = ParseHttpRetCode(httpMsg.FirstLine());

                if (status < 200 || status >= 300) {
                    Error = Sprintf("bad http reply status: %u", status);
                    break;
                }

                if (!httpMsg.HasContent()) {
                    Error = "reply must have content";
                    break;
                }

                Ctest << "Complete" << Endl;
                ctx.Send(Edge, new TEvents::TEvWakeup);
                return;
            } while (0);
            Ctest << "Got an error: " << Error << Endl;
            exit(1);
        }

        void CatchHTTPWriteComplete(const TActorContext&) noexcept override {
            auto msg = new TEvHTTPStreamStatus;
            msg->Status = TEvHTTPStreamStatus::READY;
            State = EState::WRITE_DATA_COMPLETE;
            Send(Edge, msg);
        }

        void CatchHTTPConnectionClosed() noexcept override {
            Ctest << "Connection closed prematurely" << Endl;
            exit(1);
        }

    public:
        TString Error;
        TActorId Edge;
        ui16 Port;

    private:
        TVector<char> Buf;
    };


    Y_UNIT_TEST(TestHTTPRequest) {
        TPortManager portManager;
        const ui16 port = portManager.GetPort();
        THTTP200OkServer serverImpl("");

        THttpServer::TOptions options(port);
        THttpServer::TMtpQueueRef mainWorkers =
            new TFailingServerMtpQueue(&serverImpl, SystemThreadFactory());
        THttpServer::TMtpQueueRef failWorkers =
            new TThreadPool(SystemThreadFactory());

        THttpServer server(&serverImpl, mainWorkers, failWorkers, options);
        UNIT_ASSERT(server.Start());

        TTestBasicRuntime runtime(2);
        runtime.Initialize(NKikimr::TAppPrepare().Unwrap());

        auto tester = new THTTPRequestTester;
        tester->Edge = runtime.AllocateEdgeActor(0);
        tester->Port = port;
        auto testerId = runtime.Register(tester, 0);

        TAutoPtr<IEventHandle> handle;
        runtime.GrabEdgeEvent<TEvHTTPStreamStatus>(handle);
        Ctest << "Grabed TEvHTTPStreamStatus" << Endl;

        const char* writeData =
            "11\r\n"
            "love=me\tleave=me\n\r\n";

        auto msg = new TEvHTTPSendContent;
        msg->Data = writeData;
        msg->Len = strlen(writeData);
        msg->Last = false;
        runtime.Send(new IEventHandle(testerId, tester->Edge, msg), 0);

        runtime.GrabEdgeEvent<TEvHTTPStreamStatus>(handle);

        const char* moreData =
            "15\r\n"
            "trust=is a\tnew=glory\n\r\n"
            "0\r\n\r\n";

        msg = new TEvHTTPSendContent;
        msg->Data = moreData;
        msg->Len = strlen(moreData);
        msg->Last = true;
        runtime.Send(new IEventHandle(testerId, tester->Edge, msg), 0);

        runtime.GrabEdgeEvent<TEvents::TEvWakeup>(handle);
    }
}
