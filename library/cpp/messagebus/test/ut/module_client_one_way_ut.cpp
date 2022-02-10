#include <library/cpp/testing/unittest/registar.h>

#include <library/cpp/messagebus/test/helper/example.h>
#include <library/cpp/messagebus/test/helper/message_handler_error.h>

#include <library/cpp/messagebus/misc/test_sync.h>
#include <library/cpp/messagebus/oldmodule/module.h>

using namespace NBus;
using namespace NBus::NTest;

Y_UNIT_TEST_SUITE(ModuleClientOneWay) {
    struct TTestServer: public TBusServerHandlerError {
        TExampleProtocol Proto;

        TTestSync* const TestSync;

        TBusMessageQueuePtr Queue;
        TBusServerSessionPtr ServerSession;

        TTestServer(TTestSync* testSync)
            : TestSync(testSync)
        {
            Queue = CreateMessageQueue();
            ServerSession = TBusServerSession::Create(&Proto, this, TBusServerSessionConfig(), Queue);
        }

        void OnMessage(TOnMessageContext& context) override {
            TestSync->WaitForAndIncrement(1);
            context.ForgetRequest();
        }
    };

    struct TClientModule: public TBusModule {
        TExampleProtocol Proto;

        TTestSync* const TestSync;
        unsigned const Port;

        TBusClientSessionPtr ClientSession;

        TClientModule(TTestSync* testSync, unsigned port)
            : TBusModule("m")
            , TestSync(testSync)
            , Port(port)
        {
        }

        TJobHandler Start(TBusJob* job, TBusMessage*) override {
            TestSync->WaitForAndIncrement(0);

            job->SendOneWayTo(new TExampleRequest(&Proto.RequestCount), ClientSession.Get(), TNetAddr("localhost", Port));

            return &TClientModule::Sent;
        }

        TJobHandler Sent(TBusJob* job, TBusMessage*) {
            TestSync->WaitForAndIncrement(2);
            job->Cancel(MESSAGE_DONT_ASK);
            return nullptr;
        }

        TBusServerSessionPtr CreateExtSession(TBusMessageQueue& queue) override {
            ClientSession = CreateDefaultSource(queue, &Proto, TBusServerSessionConfig());
            return nullptr;
        }
    };

    Y_UNIT_TEST(Simple) {
        TTestSync testSync;

        TTestServer server(&testSync);

        TBusMessageQueuePtr queue = CreateMessageQueue();
        TClientModule clientModule(&testSync, server.ServerSession->GetActualListenPort());

        clientModule.CreatePrivateSessions(queue.Get());
        clientModule.StartInput();

        clientModule.StartJob(new TExampleRequest(&clientModule.Proto.StartCount));

        testSync.WaitForAndIncrement(3);

        clientModule.Shutdown();
    }

    struct TSendErrorModule: public TBusModule {
        TExampleProtocol Proto;

        TTestSync* const TestSync;

        TBusClientSessionPtr ClientSession;

        TSendErrorModule(TTestSync* testSync)
            : TBusModule("m")
            , TestSync(testSync)
        {
        }

        TJobHandler Start(TBusJob* job, TBusMessage*) override {
            TestSync->WaitForAndIncrement(0);

            job->SendOneWayTo(new TExampleRequest(&Proto.RequestCount), ClientSession.Get(), TNetAddr("localhost", 1));

            return &TSendErrorModule::Sent;
        }

        TJobHandler Sent(TBusJob* job, TBusMessage*) {
            TestSync->WaitForAndIncrement(1);
            job->Cancel(MESSAGE_DONT_ASK);
            return nullptr;
        }

        TBusServerSessionPtr CreateExtSession(TBusMessageQueue& queue) override {
            TBusServerSessionConfig sessionConfig;
            sessionConfig.ConnectTimeout = 1;
            sessionConfig.SendTimeout = 1;
            sessionConfig.TotalTimeout = 1;
            sessionConfig.Secret.TimeoutPeriod = TDuration::MilliSeconds(1);
            ClientSession = CreateDefaultSource(queue, &Proto, sessionConfig);
            return nullptr;
        }
    };

    Y_UNIT_TEST(SendError) {
        TTestSync testSync;

        TBusQueueConfig queueConfig;
        queueConfig.NumWorkers = 5;

        TBusMessageQueuePtr queue = CreateMessageQueue(queueConfig);
        TSendErrorModule clientModule(&testSync);

        clientModule.CreatePrivateSessions(queue.Get());
        clientModule.StartInput();

        clientModule.StartJob(new TExampleRequest(&clientModule.Proto.StartCount));

        testSync.WaitForAndIncrement(2);

        clientModule.Shutdown();
    }
}
