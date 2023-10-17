#include <library/cpp/testing/unittest/registar.h>

#include <library/cpp/messagebus/test/helper/example.h>
#include <library/cpp/messagebus/test/helper/fixed_port.h>
#include <library/cpp/messagebus/test/helper/hanging_server.h>
#include <library/cpp/messagebus/test/helper/object_count_check.h>
#include <library/cpp/messagebus/test/helper/wait_for.h>

#include <library/cpp/messagebus/misc/test_sync.h>

#include <util/network/sock.h>

#include <utility>

using namespace NBus;
using namespace NBus::NTest;

namespace {
    struct TExampleClientSlowOnMessageSent: public TExampleClient {
        TAtomic SentCompleted;

        TSystemEvent ReplyReceived;

        TExampleClientSlowOnMessageSent()
            : SentCompleted(0)
        {
        }

        ~TExampleClientSlowOnMessageSent() override {
            Session->Shutdown();
        }

        void OnReply(TAutoPtr<TBusMessage> mess, TAutoPtr<TBusMessage> reply) override {
            Y_ABORT_UNLESS(AtomicGet(SentCompleted), "must be completed");

            TExampleClient::OnReply(mess, reply);

            ReplyReceived.Signal();
        }

        void OnMessageSent(TBusMessage*) override {
            Sleep(TDuration::MilliSeconds(100));
            AtomicSet(SentCompleted, 1);
        }
    };

}

Y_UNIT_TEST_SUITE(TMessageBusTests) {
    void TestDestinationTemplate(bool useCompression, bool ackMessageBeforeReply,
                                 const TBusServerSessionConfig& sessionConfig) {
        TObjectCountCheck objectCountCheck;

        TExampleServer server;

        TExampleClient client(sessionConfig);
        client.CrashOnError = true;

        server.UseCompression = useCompression;
        client.UseCompression = useCompression;

        server.AckMessageBeforeSendReply = ackMessageBeforeReply;

        client.SendMessagesWaitReplies(100, server.GetActualListenAddr());
        UNIT_ASSERT_EQUAL(server.Session->GetInFlight(), 0);
        UNIT_ASSERT_EQUAL(client.Session->GetInFlight(), 0);
    }

    Y_UNIT_TEST(TestDestination) {
        TestDestinationTemplate(false, false, TBusServerSessionConfig());
    }

    Y_UNIT_TEST(TestDestinationUsingAck) {
        TestDestinationTemplate(false, true, TBusServerSessionConfig());
    }

    Y_UNIT_TEST(TestDestinationWithCompression) {
        TestDestinationTemplate(true, false, TBusServerSessionConfig());
    }

    Y_UNIT_TEST(TestCork) {
        TBusServerSessionConfig config;
        config.SendThreshold = 1000000000000;
        config.Cork = TDuration::MilliSeconds(10);
        TestDestinationTemplate(false, false, config);
        // TODO: test for cork hanging
    }

    Y_UNIT_TEST(TestReconnect) {
        if (!IsFixedPortTestAllowed()) {
            return;
        }

        TObjectCountCheck objectCountCheck;

        unsigned port = FixedPort;
        TNetAddr serverAddr("localhost", port);
        THolder<TExampleServer> server;

        TBusClientSessionConfig clientConfig;
        clientConfig.RetryInterval = 0;
        TExampleClient client(clientConfig);

        server.Reset(new TExampleServer(port, "TExampleServer 1"));

        client.SendMessagesWaitReplies(17, serverAddr);

        server.Destroy();

        // Making the client to detect disconnection.
        client.SendMessages(1, serverAddr);
        EMessageStatus error = client.WaitForError();
        if (error == MESSAGE_DELIVERY_FAILED) {
            client.SendMessages(1, serverAddr);
            error = client.WaitForError();
        }
        UNIT_ASSERT_VALUES_EQUAL(MESSAGE_CONNECT_FAILED, error);

        server.Reset(new TExampleServer(port, "TExampleServer 2"));

        client.SendMessagesWaitReplies(19, serverAddr);
    }

    struct TestNoServerImplClient: public TExampleClient {
        TTestSync TestSync;
        int failures = 0;

        template <typename... Args>
        TestNoServerImplClient(Args&&... args)
            : TExampleClient(std::forward<Args>(args)...)
        {
        }

        ~TestNoServerImplClient() override {
            Session->Shutdown();
        }

        void OnError(TAutoPtr<TBusMessage> message, EMessageStatus status) override {
            Y_UNUSED(message);

            Y_ABORT_UNLESS(status == MESSAGE_CONNECT_FAILED, "must be MESSAGE_CONNECT_FAILED, got %s", ToString(status).data());

            TestSync.CheckAndIncrement((failures++) * 2);
        }
    };

    void TestNoServerImpl(unsigned port, bool oneWay) {
        TNetAddr noServerAddr("localhost", port);

        TestNoServerImplClient client;

        int count = 0;
        for (; count < 200; ++count) {
            EMessageStatus status;
            if (oneWay) {
                status = client.Session->SendMessageOneWay(new TExampleRequest(&client.Proto.RequestCount), &noServerAddr);
            } else {
                TAutoPtr<TBusMessage> message(new TExampleRequest(&client.Proto.RequestCount));
                status = client.Session->SendMessageAutoPtr(message, &noServerAddr);
            }

            Y_ABORT_UNLESS(status == MESSAGE_OK, "must be MESSAGE_OK, got %s", ToString(status).data());

            if (count == 0) {
                // lame way to wait until it is connected
                Sleep(TDuration::MilliSeconds(10));
            }
            client.TestSync.WaitForAndIncrement(count * 2 + 1);
        }

        client.TestSync.WaitForAndIncrement(count * 2);
    }

    void HangingServerImpl(unsigned port) {
        TNetAddr noServerAddr("localhost", port);

        TExampleClient client;

        int count = 0;
        for (;; ++count) {
            TAutoPtr<TBusMessage> message(new TExampleRequest(&client.Proto.RequestCount));
            EMessageStatus status = client.Session->SendMessageAutoPtr(message, &noServerAddr);
            if (status == MESSAGE_BUSY) {
                break;
            }
            UNIT_ASSERT_VALUES_EQUAL(int(MESSAGE_OK), int(status));

            if (count == 0) {
                // lame way to wait until it is connected
                Sleep(TDuration::MilliSeconds(10));
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(client.Session->GetConfig()->MaxInFlight, count);
    }

    Y_UNIT_TEST(TestHangindServer) {
        TObjectCountCheck objectCountCheck;

        THangingServer server(0);

        HangingServerImpl(server.GetPort());
    }

    Y_UNIT_TEST(TestNoServer) {
        TObjectCountCheck objectCountCheck;

        TestNoServerImpl(17, false);
    }

    Y_UNIT_TEST(PauseInput) {
        TObjectCountCheck objectCountCheck;

        TExampleServer server;
        server.Session->PauseInput(true);

        TBusClientSessionConfig clientConfig;
        clientConfig.MaxInFlight = 1000;
        TExampleClient client(clientConfig);

        client.SendMessages(100, server.GetActualListenAddr());

        server.TestSync.Check(0);

        server.Session->PauseInput(false);

        server.TestSync.WaitFor(100);

        client.WaitReplies();

        server.Session->PauseInput(true);

        client.SendMessages(200, server.GetActualListenAddr());

        server.TestSync.Check(100);

        server.Session->PauseInput(false);

        server.TestSync.WaitFor(300);

        client.WaitReplies();
    }

    struct TSendTimeoutCheckerExampleClient: public TExampleClient {
        static TBusClientSessionConfig SessionConfig(bool periodLessThanConnectTimeout) {
            TBusClientSessionConfig sessionConfig;
            if (periodLessThanConnectTimeout) {
                sessionConfig.SendTimeout = 1;
                sessionConfig.Secret.TimeoutPeriod = TDuration::MilliSeconds(50);
            } else {
                sessionConfig.SendTimeout = 50;
                sessionConfig.Secret.TimeoutPeriod = TDuration::MilliSeconds(1);
            }
            return sessionConfig;
        }

        TSendTimeoutCheckerExampleClient(bool periodLessThanConnectTimeout)
            : TExampleClient(SessionConfig(periodLessThanConnectTimeout))
        {
        }

        ~TSendTimeoutCheckerExampleClient() override {
            Session->Shutdown();
        }

        TSystemEvent ErrorHappened;

        void OnError(TAutoPtr<TBusMessage>, EMessageStatus status) override {
            Y_ABORT_UNLESS(status == MESSAGE_CONNECT_FAILED || status == MESSAGE_TIMEOUT, "got status: %s", ToString(status).data());
            ErrorHappened.Signal();
        }
    };

    void NoServer_SendTimeout_Callback_Impl(bool periodLessThanConnectTimeout) {
        TObjectCountCheck objectCountCheck;

        TNetAddr serverAddr("localhost", 17);

        TSendTimeoutCheckerExampleClient client(periodLessThanConnectTimeout);

        client.SendMessages(1, serverAddr);

        client.ErrorHappened.WaitI();
    }

    Y_UNIT_TEST(NoServer_SendTimeout_Callback_PeriodLess) {
        NoServer_SendTimeout_Callback_Impl(true);
    }

    Y_UNIT_TEST(NoServer_SendTimeout_Callback_TimeoutLess) {
        NoServer_SendTimeout_Callback_Impl(false);
    }

    Y_UNIT_TEST(TestOnReplyCalledAfterOnMessageSent) {
        TObjectCountCheck objectCountCheck;

        TExampleServer server;
        TNetAddr serverAddr = server.GetActualListenAddr();
        TExampleClientSlowOnMessageSent client;

        TAutoPtr<TExampleRequest> message(new TExampleRequest(&client.Proto.RequestCount));
        EMessageStatus s = client.Session->SendMessageAutoPtr(message, &serverAddr);
        UNIT_ASSERT_EQUAL(s, MESSAGE_OK);

        UNIT_ASSERT(client.ReplyReceived.WaitT(TDuration::Seconds(5)));
    }

    struct TDelayReplyServer: public TBusServerHandlerError {
        TBusMessageQueuePtr Bus;
        TExampleProtocol Proto;
        TSystemEvent MessageReceivedEvent; // 1 wait for 1 message
        TBusServerSessionPtr Session;
        TMutex Lock_;
        TDeque<TAutoPtr<TOnMessageContext>> DelayedMessages;

        TDelayReplyServer()
            : MessageReceivedEvent(TEventResetType::rAuto)
        {
            Bus = CreateMessageQueue("TDelayReplyServer");
            TBusServerSessionConfig sessionConfig;
            sessionConfig.SendTimeout = 1000;
            sessionConfig.TotalTimeout = 2001;
            Session = TBusServerSession::Create(&Proto, this, sessionConfig, Bus);
            if (!Session) {
                ythrow yexception() << "Failed to create destination session";
            }
        }

        void OnMessage(TOnMessageContext& mess) override {
            Y_ABORT_UNLESS(mess.IsConnectionAlive(), "connection should be alive here");
            TAutoPtr<TOnMessageContext> delayedMsg(new TOnMessageContext);
            delayedMsg->Swap(mess);
            auto g(Guard(Lock_));
            DelayedMessages.push_back(delayedMsg);
            MessageReceivedEvent.Signal();
        }

        bool CheckClientIsAlive() {
            auto g(Guard(Lock_));
            for (auto& delayedMessage : DelayedMessages) {
                if (!delayedMessage->IsConnectionAlive()) {
                    return false;
                }
            }
            return true;
        }

        bool CheckClientIsDead() const {
            auto g(Guard(Lock_));
            for (const auto& delayedMessage : DelayedMessages) {
                if (delayedMessage->IsConnectionAlive()) {
                    return false;
                }
            }
            return true;
        }

        void ReplyToDelayedMessages() {
            while (true) {
                TOnMessageContext msg;
                {
                    auto g(Guard(Lock_));
                    if (DelayedMessages.empty()) {
                        break;
                    }
                    DelayedMessages.front()->Swap(msg);
                    DelayedMessages.pop_front();
                }
                TAutoPtr<TBusMessage> reply(new TExampleResponse(&Proto.ResponseCount));
                msg.SendReplyMove(reply);
            }
        }

        size_t GetDelayedMessageCount() const {
            auto g(Guard(Lock_));
            return DelayedMessages.size();
        }

        void OnError(TAutoPtr<TBusMessage> mess, EMessageStatus status) override {
            Y_UNUSED(mess);
            Y_ABORT_UNLESS(status == MESSAGE_SHUTDOWN, "only shutdown allowed, got %s", ToString(status).data());
        }
    };

    Y_UNIT_TEST(TestReplyCalledAfterClientDisconnected) {
        TObjectCountCheck objectCountCheck;

        TDelayReplyServer server;

        THolder<TExampleClient> client(new TExampleClient);

        client->SendMessages(1, TNetAddr("localhost", server.Session->GetActualListenPort()));

        UNIT_ASSERT(server.MessageReceivedEvent.WaitT(TDuration::Seconds(5)));

        UNIT_ASSERT_VALUES_EQUAL(1, server.Session->GetInFlight());

        client.Destroy();

        UNIT_WAIT_FOR(server.CheckClientIsDead());

        server.ReplyToDelayedMessages();

        // wait until all server message are delivered
        UNIT_WAIT_FOR(0 == server.Session->GetInFlight());
    }

    struct TPackUnpackServer: public TBusServerHandlerError {
        TBusMessageQueuePtr Bus;
        TExampleProtocol Proto;
        TSystemEvent MessageReceivedEvent;
        TSystemEvent ClientDiedEvent;
        TBusServerSessionPtr Session;

        TPackUnpackServer() {
            Bus = CreateMessageQueue("TPackUnpackServer");
            TBusServerSessionConfig sessionConfig;
            Session = TBusServerSession::Create(&Proto, this, sessionConfig, Bus);
        }

        void OnMessage(TOnMessageContext& mess) override {
            TBusIdentity ident;
            mess.AckMessage(ident);

            char packed[BUS_IDENTITY_PACKED_SIZE];
            ident.Pack(packed);
            TBusIdentity resurrected;
            resurrected.Unpack(packed);

            mess.GetSession()->SendReply(resurrected, new TExampleResponse(&Proto.ResponseCount));
        }

        void OnError(TAutoPtr<TBusMessage> mess, EMessageStatus status) override {
            Y_UNUSED(mess);
            Y_ABORT_UNLESS(status == MESSAGE_SHUTDOWN, "only shutdown allowed");
        }
    };

    Y_UNIT_TEST(PackUnpack) {
        TObjectCountCheck objectCountCheck;

        TPackUnpackServer server;

        THolder<TExampleClient> client(new TExampleClient);

        client->SendMessagesWaitReplies(1, TNetAddr("localhost", server.Session->GetActualListenPort()));
    }

    Y_UNIT_TEST(ClientRequestTooLarge) {
        TObjectCountCheck objectCountCheck;

        TExampleServer server;

        TBusClientSessionConfig clientConfig;
        clientConfig.MaxMessageSize = 100;
        TExampleClient client(clientConfig);

        client.DataSize = 10;
        client.SendMessagesWaitReplies(1, server.GetActualListenAddr());

        client.DataSize = 1000;
        client.SendMessages(1, server.GetActualListenAddr());
        client.WaitForError(MESSAGE_MESSAGE_TOO_LARGE);

        client.DataSize = 20;
        client.SendMessagesWaitReplies(10, server.GetActualListenAddr());

        client.DataSize = 10000;
        client.SendMessages(1, server.GetActualListenAddr());
        client.WaitForError(MESSAGE_MESSAGE_TOO_LARGE);
    }

    struct TServerForResponseTooLarge: public TExampleServer {
        TTestSync TestSync;

        static TBusServerSessionConfig Config() {
            TBusServerSessionConfig config;
            config.MaxMessageSize = 100;
            return config;
        }

        TServerForResponseTooLarge()
            : TExampleServer("TServerForResponseTooLarge", Config())
        {
        }

        ~TServerForResponseTooLarge() override {
            Session->Shutdown();
        }

        void OnMessage(TOnMessageContext& mess) override {
            TAutoPtr<TBusMessage> response;

            if (TestSync.Get() == 0) {
                TestSync.CheckAndIncrement(0);
                response.Reset(new TExampleResponse(&Proto.ResponseCount, 1000));
            } else {
                TestSync.WaitForAndIncrement(3);
                response.Reset(new TExampleResponse(&Proto.ResponseCount, 10));
            }

            mess.SendReplyMove(response);
        }

        void OnError(TAutoPtr<TBusMessage>, EMessageStatus status) override {
            TestSync.WaitForAndIncrement(1);

            Y_ABORT_UNLESS(status == MESSAGE_MESSAGE_TOO_LARGE, "status");
        }
    };

    Y_UNIT_TEST(ServerResponseTooLarge) {
        TObjectCountCheck objectCountCheck;

        TServerForResponseTooLarge server;

        TExampleClient client;
        client.DataSize = 10;

        client.SendMessages(1, server.GetActualListenAddr());
        server.TestSync.WaitForAndIncrement(2);
        client.ResetCounters();

        client.SendMessages(1, server.GetActualListenAddr());

        client.WorkDone.WaitI();

        server.TestSync.CheckAndIncrement(4);

        UNIT_ASSERT_VALUES_EQUAL(1, client.Session->GetInFlight());
    }

    struct TServerForRequestTooLarge: public TExampleServer {
        TTestSync TestSync;

        static TBusServerSessionConfig Config() {
            TBusServerSessionConfig config;
            config.MaxMessageSize = 100;
            return config;
        }

        TServerForRequestTooLarge()
            : TExampleServer("TServerForRequestTooLarge", Config())
        {
        }

        ~TServerForRequestTooLarge() override {
            Session->Shutdown();
        }

        void OnMessage(TOnMessageContext& req) override {
            unsigned n = TestSync.Get();
            if (n < 2) {
                TestSync.CheckAndIncrement(n);
                TAutoPtr<TExampleResponse> resp(new TExampleResponse(&Proto.ResponseCount, 10));
                req.SendReplyMove(resp);
            } else {
                Y_ABORT("wrong");
            }
        }
    };

    Y_UNIT_TEST(ServerRequestTooLarge) {
        TObjectCountCheck objectCountCheck;

        TServerForRequestTooLarge server;

        TExampleClient client;
        client.DataSize = 10;

        client.SendMessagesWaitReplies(2, server.GetActualListenAddr());

        server.TestSync.CheckAndIncrement(2);

        client.DataSize = 200;
        client.SendMessages(1, server.GetActualListenAddr());
        // server closes connection, so MESSAGE_DELIVERY_FAILED is returned to client
        client.WaitForError(MESSAGE_DELIVERY_FAILED);
    }

    Y_UNIT_TEST(ClientResponseTooLarge) {
        TObjectCountCheck objectCountCheck;

        TExampleServer server;

        server.DataSize = 10;

        TBusClientSessionConfig clientSessionConfig;
        clientSessionConfig.MaxMessageSize = 100;
        TExampleClient client(clientSessionConfig);
        client.DataSize = 10;

        client.SendMessagesWaitReplies(3, server.GetActualListenAddr());

        server.DataSize = 1000;

        client.SendMessages(1, server.GetActualListenAddr());
        client.WaitForError(MESSAGE_DELIVERY_FAILED);
    }

    Y_UNIT_TEST(ServerUnknownMessage) {
        TObjectCountCheck objectCountCheck;

        TExampleServer server;
        TNetAddr serverAddr = server.GetActualListenAddr();

        TExampleClient client;

        client.SendMessagesWaitReplies(2, serverAddr);

        TAutoPtr<TBusMessage> req(new TExampleRequest(&client.Proto.RequestCount));
        req->GetHeader()->Type = 11;
        client.Session->SendMessageAutoPtr(req, &serverAddr);
        client.MessageCount = 1;

        client.WaitForError(MESSAGE_DELIVERY_FAILED);
    }

    Y_UNIT_TEST(ServerMessageReservedIds) {
        TObjectCountCheck objectCountCheck;

        TExampleServer server;
        TNetAddr serverAddr = server.GetActualListenAddr();

        TExampleClient client;

        client.SendMessagesWaitReplies(2, serverAddr);

        // This test doens't check 0, 1, YBUS_KEYINVALID because there are asserts() on sending side

        TAutoPtr<TBusMessage> req(new TExampleRequest(&client.Proto.RequestCount));
        req->GetHeader()->Id = 2;
        client.Session->SendMessageAutoPtr(req, &serverAddr);
        client.MessageCount = 1;
        client.WaitForError(MESSAGE_DELIVERY_FAILED);

        req.Reset(new TExampleRequest(&client.Proto.RequestCount));
        req->GetHeader()->Id = YBUS_KEYLOCAL;
        client.Session->SendMessageAutoPtr(req, &serverAddr);
        client.MessageCount = 1;
        client.WaitForError(MESSAGE_DELIVERY_FAILED);
    }

    Y_UNIT_TEST(TestGetInFlightForDestination) {
        TObjectCountCheck objectCountCheck;

        TDelayReplyServer server;

        TExampleClient client;

        TNetAddr addr("localhost", server.Session->GetActualListenPort());

        UNIT_ASSERT_VALUES_EQUAL(size_t(0), client.Session->GetInFlight(addr));

        client.SendMessages(2, &addr);

        for (size_t i = 0; i < 5; ++i) {
            // One MessageReceivedEvent indicates one message, we need to wait for two
            UNIT_ASSERT(server.MessageReceivedEvent.WaitT(TDuration::Seconds(5)));
            if (server.GetDelayedMessageCount() == 2) {
                break;
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(server.GetDelayedMessageCount(), 2);

        size_t inFlight = client.Session->GetInFlight(addr);
        // 4 is for messagebus1 that adds inFlight counter twice for some reason
        UNIT_ASSERT(inFlight == 2 || inFlight == 4);

        UNIT_ASSERT(server.CheckClientIsAlive());

        server.ReplyToDelayedMessages();

        client.WaitReplies();
    }

    struct TResetAfterSendOneWayErrorInCallbackClient: public TExampleClient {
        TTestSync TestSync;

        static TBusClientSessionConfig SessionConfig() {
            TBusClientSessionConfig config;
            // 1 ms is not enough when test is running under valgrind
            config.ConnectTimeout = 10;
            config.SendTimeout = 10;
            config.Secret.TimeoutPeriod = TDuration::MilliSeconds(1);
            return config;
        }

        TResetAfterSendOneWayErrorInCallbackClient()
            : TExampleClient(SessionConfig())
        {
        }

        ~TResetAfterSendOneWayErrorInCallbackClient() override {
            Session->Shutdown();
        }

        void OnError(TAutoPtr<TBusMessage> mess, EMessageStatus status) override {
            TestSync.WaitForAndIncrement(0);
            Y_ABORT_UNLESS(status == MESSAGE_CONNECT_FAILED || status == MESSAGE_TIMEOUT, "must be connection failed, got %s", ToString(status).data());
            mess.Destroy();
            TestSync.CheckAndIncrement(1);
        }
    };

    Y_UNIT_TEST(ResetAfterSendOneWayErrorInCallback) {
        TObjectCountCheck objectCountCheck;

        TNetAddr noServerAddr("localhost", 17);

        TResetAfterSendOneWayErrorInCallbackClient client;

        EMessageStatus ok = client.Session->SendMessageOneWayMove(new TExampleRequest(&client.Proto.RequestCount), &noServerAddr);
        UNIT_ASSERT_VALUES_EQUAL(MESSAGE_OK, ok);

        client.TestSync.WaitForAndIncrement(2);
    }

    struct TResetAfterSendMessageOneWayDuringShutdown: public TExampleClient {
        TTestSync TestSync;

        ~TResetAfterSendMessageOneWayDuringShutdown() override {
            Session->Shutdown();
        }

        void OnError(TAutoPtr<TBusMessage> message, EMessageStatus status) override {
            TestSync.CheckAndIncrement(0);

            Y_ABORT_UNLESS(status == MESSAGE_CONNECT_FAILED, "must be MESSAGE_CONNECT_FAILED, got %s", ToString(status).data());

            // check reset is possible here
            message->Reset();

            // intentionally don't destroy the message
            // we will try to resend it
            Y_UNUSED(message.Release());

            TestSync.CheckAndIncrement(1);
        }
    };

    Y_UNIT_TEST(ResetAfterSendMessageOneWayDuringShutdown) {
        TObjectCountCheck objectCountCheck;

        TNetAddr noServerAddr("localhost", 17);

        TResetAfterSendMessageOneWayDuringShutdown client;

        TExampleRequest* message = new TExampleRequest(&client.Proto.RequestCount);
        EMessageStatus ok = client.Session->SendMessageOneWay(message, &noServerAddr);
        UNIT_ASSERT_VALUES_EQUAL(MESSAGE_OK, ok);

        client.TestSync.WaitForAndIncrement(2);

        client.Session->Shutdown();

        ok = client.Session->SendMessageOneWay(message);
        Y_ABORT_UNLESS(ok == MESSAGE_SHUTDOWN, "must be shutdown when sending during shutdown, got %s", ToString(ok).data());

        // check reset is possible here
        message->Reset();
        client.TestSync.CheckAndIncrement(3);

        delete message;
    }

    Y_UNIT_TEST(ResetAfterSendOneWayErrorInReturn) {
        TObjectCountCheck objectCountCheck;

        TestNoServerImpl(17, true);
    }

    struct TResetAfterSendOneWaySuccessClient: public TExampleClient {
        TTestSync TestSync;

        ~TResetAfterSendOneWaySuccessClient() override {
            Session->Shutdown();
        }

        void OnMessageSentOneWay(TAutoPtr<TBusMessage> sent) override {
            TestSync.WaitForAndIncrement(0);
            sent->Reset();
            TestSync.CheckAndIncrement(1);
        }
    };

    Y_UNIT_TEST(ResetAfterSendOneWaySuccess) {
        TObjectCountCheck objectCountCheck;

        TExampleServer server;
        TNetAddr serverAddr = server.GetActualListenAddr();

        TResetAfterSendOneWaySuccessClient client;

        EMessageStatus ok = client.Session->SendMessageOneWay(new TExampleRequest(&client.Proto.RequestCount), &serverAddr);
        UNIT_ASSERT_VALUES_EQUAL(MESSAGE_OK, ok);
        // otherwize message might go to OnError(MESSAGE_SHUTDOWN)
        server.WaitForOnMessageCount(1);

        client.TestSync.WaitForAndIncrement(2);
    }

    Y_UNIT_TEST(GetStatus) {
        TObjectCountCheck objectCountCheck;

        TExampleServer server;

        TExampleClient client;
        // make sure connected
        client.SendMessagesWaitReplies(3, server.GetActualListenAddr());

        server.Bus->GetStatus();
        server.Bus->GetStatus();
        server.Bus->GetStatus();

        client.Bus->GetStatus();
        client.Bus->GetStatus();
        client.Bus->GetStatus();
    }

    Y_UNIT_TEST(BindOnRandomPort) {
        TObjectCountCheck objectCountCheck;

        TBusServerSessionConfig serverConfig;
        TExampleServer server;

        TExampleClient client;
        TNetAddr addr(TNetAddr("127.0.0.1", server.Session->GetActualListenPort()));
        client.SendMessagesWaitReplies(3, &addr);
    }

    Y_UNIT_TEST(UnbindOnShutdown) {
        TBusMessageQueuePtr queue(CreateMessageQueue());

        TExampleProtocol proto;
        TBusServerHandlerError handler;
        TBusServerSessionPtr session = TBusServerSession::Create(
            &proto, &handler, TBusServerSessionConfig(), queue);

        unsigned port = session->GetActualListenPort();
        UNIT_ASSERT(port > 0);

        session->Shutdown();

        // fails is Shutdown() didn't unbind
        THangingServer hangingServer(port);
    }

    Y_UNIT_TEST(VersionNegotiation) {
        TObjectCountCheck objectCountCheck;

        TExampleServer server;

        TSockAddrInet addr(IpFromString("127.0.0.1"), server.Session->GetActualListenPort());

        TInetStreamSocket socket;
        int r1 = socket.Connect(&addr);
        UNIT_ASSERT(r1 >= 0);

        TStreamSocketOutput output(&socket);

        TBusHeader request;
        Zero(request);
        request.Size = sizeof(request);
        request.SetVersionInternal(0xF); // max
        output.Write(&request, sizeof(request));

        UNIT_ASSERT_VALUES_EQUAL(IsVersionNegotiation(request), true);

        TStreamSocketInput input(&socket);

        TBusHeader response;
        size_t pos = 0;

        while (pos < sizeof(response)) {
            size_t count = input.Read(((char*)&response) + pos, sizeof(response) - pos);
            pos += count;
        }

        UNIT_ASSERT_VALUES_EQUAL(sizeof(response), pos);

        UNIT_ASSERT_VALUES_EQUAL(YBUS_VERSION, response.GetVersionInternal());
    }

    struct TOnConnectionEventClient: public TExampleClient {
        TTestSync Sync;

        ~TOnConnectionEventClient() override {
            Session->Shutdown();
        }

        void OnClientConnectionEvent(const TClientConnectionEvent& event) override {
            if (Sync.Get() > 2) {
                // Test OnClientConnectionEvent_Disconnect is broken.
                // Sometimes reconnect happens during server shutdown
                // when acceptor connections is still alive, and
                // server connection is already closed
                return;
            }

            if (event.GetType() == TClientConnectionEvent::CONNECTED) {
                Sync.WaitForAndIncrement(0);
            } else if (event.GetType() == TClientConnectionEvent::DISCONNECTED) {
                Sync.WaitForAndIncrement(2);
            }
        }

        void OnError(TAutoPtr<TBusMessage>, EMessageStatus) override {
            // We do not check for message errors in this test.
        }

        void OnMessageSentOneWay(TAutoPtr<TBusMessage>) override {
        }
    };

    struct TOnConnectionEventServer: public TExampleServer {
        TOnConnectionEventServer()
            : TExampleServer("TOnConnectionEventServer")
        {
        }

        ~TOnConnectionEventServer() override {
            Session->Shutdown();
        }

        void OnError(TAutoPtr<TBusMessage>, EMessageStatus) override {
            // We do not check for server message errors in this test.
        }
    };

    Y_UNIT_TEST(OnClientConnectionEvent_Shutdown) {
        TObjectCountCheck objectCountCheck;

        TOnConnectionEventServer server;

        TOnConnectionEventClient client;

        TNetAddr addr("127.0.0.1", server.Session->GetActualListenPort());

        client.Session->SendMessageOneWay(new TExampleRequest(&client.Proto.RequestCount), &addr);

        client.Sync.WaitForAndIncrement(1);

        client.Session->Shutdown();

        client.Sync.WaitForAndIncrement(3);
    }

    Y_UNIT_TEST(OnClientConnectionEvent_Disconnect) {
        TObjectCountCheck objectCountCheck;

        THolder<TOnConnectionEventServer> server(new TOnConnectionEventServer);

        TOnConnectionEventClient client;
        TNetAddr addr("127.0.0.1", server->Session->GetActualListenPort());

        client.Session->SendMessageOneWay(new TExampleRequest(&client.Proto.RequestCount), &addr);

        client.Sync.WaitForAndIncrement(1);

        server.Destroy();

        client.Sync.WaitForAndIncrement(3);
    }

    struct TServerForQuotaWake: public TExampleServer {
        TSystemEvent GoOn;
        TMutex OneLock;

        TOnMessageContext OneMessage;

        static TBusServerSessionConfig Config() {
            TBusServerSessionConfig config;

            config.PerConnectionMaxInFlight = 1;
            config.PerConnectionMaxInFlightBySize = 1500;
            config.MaxMessageSize = 1024;

            return config;
        }

        TServerForQuotaWake()
            : TExampleServer("TServerForQuotaWake", Config())
        {
        }

        ~TServerForQuotaWake() override {
            Session->Shutdown();
        }

        void OnMessage(TOnMessageContext& req) override {
            if (!GoOn.Wait(0)) {
                TGuard<TMutex> guard(OneLock);

                UNIT_ASSERT(!OneMessage);

                OneMessage.Swap(req);
            } else
                TExampleServer::OnMessage(req);
        }

        void WakeOne() {
            TGuard<TMutex> guard(OneLock);

            UNIT_ASSERT(!!OneMessage);

            TExampleServer::OnMessage(OneMessage);

            TOnMessageContext().Swap(OneMessage);
        }
    };

    Y_UNIT_TEST(WakeReaderOnQuota) {
        const size_t test_msg_count = 64;

        TBusClientSessionConfig clientConfig;

        clientConfig.MaxInFlight = test_msg_count;

        TExampleClient client(clientConfig);
        TServerForQuotaWake server;
        TInstant start;

        client.MessageCount = test_msg_count;

        const NBus::TNetAddr addr = server.GetActualListenAddr();

        for (unsigned count = 0;;) {
            UNIT_ASSERT(count <= test_msg_count);

            TAutoPtr<TBusMessage> message(new TExampleRequest(&client.Proto.RequestCount));
            EMessageStatus status = client.Session->SendMessageAutoPtr(message, &addr);

            if (status == MESSAGE_OK) {
                count++;

            } else if (status == MESSAGE_BUSY) {
                if (count == test_msg_count) {
                    TInstant now = TInstant::Now();

                    if (start.GetValue() == 0) {
                        start = now;

                        // TODO: properly check that server is blocked
                    } else if (start + TDuration::MilliSeconds(100) < now) {
                        break;
                    }
                }

                Sleep(TDuration::MilliSeconds(10));

            } else
                UNIT_ASSERT(false);
        }

        server.GoOn.Signal();
        server.WakeOne();

        client.WaitReplies();

        server.WaitForOnMessageCount(test_msg_count);
    }

    Y_UNIT_TEST(TestConnectionAttempts) {
        TObjectCountCheck objectCountCheck;

        TNetAddr noServerAddr("localhost", 17);
        TBusClientSessionConfig clientConfig;
        clientConfig.RetryInterval = 100;
        TestNoServerImplClient client(clientConfig);

        int count = 0;
        for (; count < 10; ++count) {
            EMessageStatus status = client.Session->SendMessageOneWay(new TExampleRequest(&client.Proto.RequestCount),
                                                                      &noServerAddr);

            Y_ABORT_UNLESS(status == MESSAGE_OK, "must be MESSAGE_OK, got %s", ToString(status).data());
            client.TestSync.WaitForAndIncrement(count * 2 + 1);

            // First connection attempt is for connect call; second one is to get connect result.
            UNIT_ASSERT_EQUAL(client.Session->GetConnectSyscallsNumForTest(noServerAddr), 2);
        }
        Sleep(TDuration::MilliSeconds(clientConfig.RetryInterval));
        for (; count < 10; ++count) {
            EMessageStatus status = client.Session->SendMessageOneWay(new TExampleRequest(&client.Proto.RequestCount),
                                                                      &noServerAddr);

            Y_ABORT_UNLESS(status == MESSAGE_OK, "must be MESSAGE_OK, got %s", ToString(status).data());
            client.TestSync.WaitForAndIncrement(count * 2 + 1);

            // First connection attempt is for connect call; second one is to get connect result.
            UNIT_ASSERT_EQUAL(client.Session->GetConnectSyscallsNumForTest(noServerAddr), 4);
        }
    }

    Y_UNIT_TEST(TestConnectionAttemptsOnNoMessagesAndNotReconnectWhenIdle) {
        TObjectCountCheck objectCountCheck;

        TNetAddr noServerAddr("localhost", 17);
        TBusClientSessionConfig clientConfig;
        clientConfig.RetryInterval = 100;
        clientConfig.ReconnectWhenIdle = false;
        TestNoServerImplClient client(clientConfig);

        int count = 0;
        for (; count < 10; ++count) {
            EMessageStatus status = client.Session->SendMessageOneWay(new TExampleRequest(&client.Proto.RequestCount),
                                                                      &noServerAddr);

            Y_ABORT_UNLESS(status == MESSAGE_OK, "must be MESSAGE_OK, got %s", ToString(status).data());
            client.TestSync.WaitForAndIncrement(count * 2 + 1);

            // First connection attempt is for connect call; second one is to get connect result.
            UNIT_ASSERT_EQUAL(client.Session->GetConnectSyscallsNumForTest(noServerAddr), 2);
        }

        Sleep(TDuration::MilliSeconds(clientConfig.RetryInterval / 2));
        UNIT_ASSERT_EQUAL(client.Session->GetConnectSyscallsNumForTest(noServerAddr), 2);
        Sleep(TDuration::MilliSeconds(10 * clientConfig.RetryInterval));
        UNIT_ASSERT_EQUAL(client.Session->GetConnectSyscallsNumForTest(noServerAddr), 2);
    }

    Y_UNIT_TEST(TestConnectionAttemptsOnNoMessagesAndReconnectWhenIdle) {
        TObjectCountCheck objectCountCheck;

        TNetAddr noServerAddr("localhost", 17);
        TBusClientSessionConfig clientConfig;
        clientConfig.ReconnectWhenIdle = true;
        clientConfig.RetryInterval = 100;
        TestNoServerImplClient client(clientConfig);

        int count = 0;
        for (; count < 10; ++count) {
            EMessageStatus status = client.Session->SendMessageOneWay(new TExampleRequest(&client.Proto.RequestCount),
                                                                      &noServerAddr);

            Y_ABORT_UNLESS(status == MESSAGE_OK, "must be MESSAGE_OK, got %s", ToString(status).data());
            client.TestSync.WaitForAndIncrement(count * 2 + 1);

            // First connection attempt is for connect call; second one is to get connect result.
            UNIT_ASSERT_VALUES_EQUAL(client.Session->GetConnectSyscallsNumForTest(noServerAddr), 2);
        }

        Sleep(TDuration::MilliSeconds(clientConfig.RetryInterval / 2));
        UNIT_ASSERT_EQUAL(client.Session->GetConnectSyscallsNumForTest(noServerAddr), 2);
        Sleep(TDuration::MilliSeconds(10 * clientConfig.RetryInterval));
        // it is undeterministic how many reconnects will be during that amount of time
        // but it should occur at least once
        UNIT_ASSERT(client.Session->GetConnectSyscallsNumForTest(noServerAddr) > 2);
    }
}
