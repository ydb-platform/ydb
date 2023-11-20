///////////////////////////////////////////////////////////////////
/// \file
/// \brief Example of reply-less communication

/// This example demostrates how asynchronous message passing library
/// can be used to send message and do not wait for reply back.
/// The usage of reply-less communication should be restricted to
/// low-throughput clients and high-throughput server to provide reasonable
/// utility. Removing replies from the communication removes any restriction
/// on how many message can be send to server and rougue clients may overwelm
/// server without thoughtput control.

/// 1) To implement reply-less client \n

/// Call NBus::TBusSession::AckMessage()
/// from within NBus::IMessageHandler::OnSent() handler when message has
/// gone into wire on client end. See example in NBus::NullClient::OnMessageSent().
/// Discard identity for reply message.

/// 2) To implement reply-less server \n

/// Call NBus::TBusSession::AckMessage() from within NBus::IMessageHandler::OnMessage()
/// handler when message has been received on server end.
/// See example in NBus::NullServer::OnMessage().
/// Discard identity for reply message.

#include <library/cpp/messagebus/test/helper/alloc_counter.h>
#include <library/cpp/messagebus/test/helper/example.h>
#include <library/cpp/messagebus/test/helper/hanging_server.h>
#include <library/cpp/messagebus/test/helper/message_handler_error.h>
#include <library/cpp/messagebus/test/helper/object_count_check.h>
#include <library/cpp/messagebus/test/helper/wait_for.h>

#include <library/cpp/messagebus/ybus.h>

using namespace std;
using namespace NBus;
using namespace NBus::NPrivate;
using namespace NBus::NTest;

////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////
/// \brief Reply-less client and handler
struct NullClient : TBusClientHandlerError {
    TNetAddr ServerAddr;

    TBusMessageQueuePtr Queue;
    TBusClientSessionPtr Session;
    TExampleProtocol Proto;

    /// constructor creates instances of protocol and session
    NullClient(const TNetAddr& serverAddr, const TBusClientSessionConfig& sessionConfig = TBusClientSessionConfig())
        : ServerAddr(serverAddr)
    {
        UNIT_ASSERT(serverAddr.GetPort() > 0);

        /// create or get instance of message queue, need one per application
        Queue = CreateMessageQueue();

        /// register source/client session
        Session = TBusClientSession::Create(&Proto, this, sessionConfig, Queue);

        /// register service, announce to clients via LocatorService
        Session->RegisterService("localhost");
    }

    ~NullClient() override {
        Session->Shutdown();
    }

    /// dispatch of requests is done here
    void Work() {
        int batch = 10;

        for (int i = 0; i < batch; i++) {
            TExampleRequest* mess = new TExampleRequest(&Proto.RequestCount);
            mess->Data = "TADA";
            Session->SendMessageOneWay(mess, &ServerAddr);
        }
    }

    void OnMessageSentOneWay(TAutoPtr<TBusMessage>) override {
    }
};

/////////////////////////////////////////////////////////////////////
/// \brief Reply-less server and handler
class NullServer: public TBusServerHandlerError {
public:
    /// session object to maintian
    TBusMessageQueuePtr Queue;
    TBusServerSessionPtr Session;
    TExampleProtocol Proto;

public:
    TAtomic NumMessages;

    NullServer() {
        NumMessages = 0;

        /// create or get instance of single message queue, need one for application
        Queue = CreateMessageQueue();

        /// register destination session
        TBusServerSessionConfig sessionConfig;
        Session = TBusServerSession::Create(&Proto, this, sessionConfig, Queue);
    }

    ~NullServer() override {
        Session->Shutdown();
    }

    /// when message comes do not send reply, just acknowledge
    void OnMessage(TOnMessageContext& mess) override {
        TExampleRequest* fmess = static_cast<TExampleRequest*>(mess.GetMessage());

        Y_ASSERT(fmess->Data == "TADA");

        /// tell session to forget this message and never expect any reply
        mess.ForgetRequest();

        AtomicIncrement(NumMessages);
    }

    /// this handler should not be called because this server does not send replies
    void OnSent(TAutoPtr<TBusMessage> mess) override {
        Y_UNUSED(mess);
        Y_ABORT("This server does not sent replies");
    }
};

Y_UNIT_TEST_SUITE(TMessageBusTests_OneWay) {
    Y_UNIT_TEST(Simple) {
        TObjectCountCheck objectCountCheck;

        NullServer server;
        NullClient client(TNetAddr("localhost", server.Session->GetActualListenPort()));

        client.Work();

        // wait until all client message are delivered
        UNIT_WAIT_FOR(AtomicGet(server.NumMessages) == 10);

        // assert correct number of messages
        UNIT_ASSERT_VALUES_EQUAL(AtomicGet(server.NumMessages), 10);
        UNIT_ASSERT_VALUES_EQUAL(server.Session->GetInFlight(), 0);
        UNIT_ASSERT_VALUES_EQUAL(client.Session->GetInFlight(), 0);
    }

    struct TMessageTooLargeClient: public NullClient {
        TSystemEvent GotTooLarge;

        TBusClientSessionConfig Config() {
            TBusClientSessionConfig r;
            r.MaxMessageSize = 1;
            return r;
        }

        TMessageTooLargeClient(unsigned port)
            : NullClient(TNetAddr("localhost", port), Config())
        {
        }

        ~TMessageTooLargeClient() override {
            Session->Shutdown();
        }

        void OnError(TAutoPtr<TBusMessage> mess, EMessageStatus status) override {
            Y_UNUSED(mess);

            Y_ABORT_UNLESS(status == MESSAGE_MESSAGE_TOO_LARGE, "wrong status: %s", ToCString(status));

            GotTooLarge.Signal();
        }
    };

    Y_UNIT_TEST(MessageTooLargeOnClient) {
        TObjectCountCheck objectCountCheck;

        NullServer server;

        TMessageTooLargeClient client(server.Session->GetActualListenPort());

        EMessageStatus ok = client.Session->SendMessageOneWayMove(new TExampleRequest(&client.Proto.RequestCount), &client.ServerAddr);
        UNIT_ASSERT_VALUES_EQUAL(MESSAGE_OK, ok);

        client.GotTooLarge.WaitI();
    }

    struct TCheckTimeoutClient: public NullClient {
        ~TCheckTimeoutClient() override {
            Session->Shutdown();
        }

        static TBusClientSessionConfig SessionConfig() {
            TBusClientSessionConfig sessionConfig;
            sessionConfig.SendTimeout = 1;
            sessionConfig.ConnectTimeout = 1;
            sessionConfig.Secret.TimeoutPeriod = TDuration::MilliSeconds(10);
            return sessionConfig;
        }

        TCheckTimeoutClient(const TNetAddr& serverAddr)
            : NullClient(serverAddr, SessionConfig())
        {
        }

        TSystemEvent GotError;

        /// message that could not be delivered
        void OnError(TAutoPtr<TBusMessage> mess, EMessageStatus status) override {
            Y_UNUSED(mess);
            Y_UNUSED(status); // TODO: check status

            GotError.Signal();
        }
    };

    Y_UNIT_TEST(SendTimeout_Callback_NoServer) {
        TObjectCountCheck objectCountCheck;

        TCheckTimeoutClient client(TNetAddr("localhost", 17));

        EMessageStatus ok = client.Session->SendMessageOneWay(new TExampleRequest(&client.Proto.RequestCount), &client.ServerAddr);
        UNIT_ASSERT_EQUAL(ok, MESSAGE_OK);

        client.GotError.WaitI();
    }

    Y_UNIT_TEST(SendTimeout_Callback_HangingServer) {
        THangingServer server;

        TObjectCountCheck objectCountCheck;

        TCheckTimeoutClient client(TNetAddr("localhost", server.GetPort()));

        bool first = true;
        for (;;) {
            EMessageStatus ok = client.Session->SendMessageOneWayMove(new TExampleRequest(&client.Proto.RequestCount), &client.ServerAddr);
            if (ok == MESSAGE_BUSY) {
                UNIT_ASSERT(!first);
                break;
            }
            UNIT_ASSERT_VALUES_EQUAL(ok, MESSAGE_OK);
            first = false;
        }

        // BUGBUG: The test is buggy: the client might not get any error when sending one-way messages.
        // All the messages that the client has sent before he gets first MESSAGE_BUSY error might get
        // serailized and written to the socket buffer, so the write queue gets drained and there are
        // no messages to timeout when periodic timeout check happens.

        client.GotError.WaitI();
    }
}
