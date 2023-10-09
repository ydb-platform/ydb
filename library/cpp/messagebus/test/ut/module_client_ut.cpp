#include <library/cpp/testing/unittest/registar.h>

#include "count_down_latch.h"
#include "moduletest.h"

#include <library/cpp/messagebus/test/helper/example.h>
#include <library/cpp/messagebus/test/helper/example_module.h>
#include <library/cpp/messagebus/test/helper/object_count_check.h>
#include <library/cpp/messagebus/test/helper/wait_for.h>

#include <library/cpp/messagebus/misc/test_sync.h>
#include <library/cpp/messagebus/oldmodule/module.h>

#include <util/generic/cast.h>
#include <util/system/event.h>

using namespace NBus;
using namespace NBus::NTest;

// helper class that cleans TBusJob instance, so job's destructor can
// be completed without assertion fail.
struct TJobGuard {
public:
    TJobGuard(NBus::TBusJob* job)
        : Job(job)
    {
    }

    ~TJobGuard() {
        Job->ClearAllMessageStates();
    }

private:
    NBus::TBusJob* Job;
};

class TMessageOk: public NBus::TBusMessage {
public:
    TMessageOk()
        : NBus::TBusMessage(1)
    {
    }
};

class TMessageError: public NBus::TBusMessage {
public:
    TMessageError()
        : NBus::TBusMessage(2)
    {
    }
};

Y_UNIT_TEST_SUITE(BusJobTest) {
#if 0
    Y_UNIT_TEST(TestPending) {
        TObjectCountCheck objectCountCheck;

        TDupDetectModule module;
        TBusJob job(&module, new TBusMessage(0));
        // Guard will clear the job if unit-assertion fails.
        TJobGuard g(&job);

        NBus::TBusMessage* msg = new NBus::TBusMessage(1);
        job.Send(msg, NULL);
        NBus::TJobStateVec pending;
        job.GetPending(&pending);

        UNIT_ASSERT_VALUES_EQUAL(pending.size(), 1u);
        UNIT_ASSERT_EQUAL(msg, pending[0].Message);
    }

    Y_UNIT_TEST(TestCallReplyHandler) {
        TObjectCountCheck objectCountCheck;

        TDupDetectModule module;
        NBus::TBusJob job(&module, new NBus::TBusMessage(0));
        // Guard will clear the job if unit-assertion fails.
        TJobGuard g(&job);

        NBus::TBusMessage* msgOk = new TMessageOk;
        NBus::TBusMessage* msgError = new TMessageError;
        job.Send(msgOk, NULL);
        job.Send(msgError, NULL);

        UNIT_ASSERT_EQUAL(job.GetState<TMessageOk>(), NULL);
        UNIT_ASSERT_EQUAL(job.GetState<TMessageError>(), NULL);

        NBus::TBusMessage* reply = new NBus::TBusMessage(0);
        job.CallReplyHandler(NBus::MESSAGE_OK, msgOk, reply);
        job.CallReplyHandler(NBus::MESSAGE_TIMEOUT, msgError, NULL);

        UNIT_ASSERT_UNEQUAL(job.GetState<TMessageOk>(), NULL);
        UNIT_ASSERT_UNEQUAL(job.GetState<TMessageError>(), NULL);

        UNIT_ASSERT_VALUES_EQUAL(job.GetStatus<TMessageError>(), NBus::MESSAGE_TIMEOUT);
        UNIT_ASSERT_EQUAL(job.GetState<TMessageError>()->Status, NBus::MESSAGE_TIMEOUT);

        UNIT_ASSERT_VALUES_EQUAL(job.GetStatus<TMessageOk>(), NBus::MESSAGE_OK);
        UNIT_ASSERT_EQUAL(job.GetState<TMessageOk>()->Reply, reply);
    }
#endif

    struct TParallelOnReplyModule : TExampleClientModule {
        TNetAddr ServerAddr;

        TCountDownLatch RepliesLatch;

        TParallelOnReplyModule(const TNetAddr& serverAddr)
            : ServerAddr(serverAddr)
            , RepliesLatch(2)
        {
        }

        TJobHandler Start(TBusJob* job, TBusMessage* mess) override {
            Y_UNUSED(mess);
            job->Send(new TExampleRequest(&Proto.RequestCount), Source, TReplyHandler(&TParallelOnReplyModule::ReplyHandler), 0, ServerAddr);
            return &TParallelOnReplyModule::HandleReplies;
        }

        void ReplyHandler(TBusJob*, EMessageStatus status, TBusMessage* mess, TBusMessage* reply) {
            Y_UNUSED(mess);
            Y_UNUSED(reply);
            Y_ABORT_UNLESS(status == MESSAGE_OK, "failed to get reply: %s", ToCString(status));
        }

        TJobHandler HandleReplies(TBusJob* job, TBusMessage* mess) {
            Y_UNUSED(mess);
            RepliesLatch.CountDown();
            Y_ABORT_UNLESS(RepliesLatch.Await(TDuration::Seconds(10)), "failed to get answers");
            job->Cancel(MESSAGE_UNKNOWN);
            return nullptr;
        }
    };

    Y_UNIT_TEST(TestReplyHandlerCalledInParallel) {
        TObjectCountCheck objectCountCheck;

        TExampleServer server;

        TExampleProtocol proto;

        TBusQueueConfig config;
        config.NumWorkers = 5;

        TParallelOnReplyModule module(server.GetActualListenAddr());
        module.StartModule();

        module.StartJob(new TExampleRequest(&proto.StartCount));
        module.StartJob(new TExampleRequest(&proto.StartCount));

        UNIT_ASSERT(module.RepliesLatch.Await(TDuration::Seconds(10)));

        module.Shutdown();
    }

    struct TErrorHandlerCheckerModule : TExampleModule {
        TNetAddr ServerAddr;

        TBusClientSessionPtr Source;

        TCountDownLatch GotReplyLatch;

        TBusMessage* SentMessage;

        TErrorHandlerCheckerModule()
            : ServerAddr("localhost", 17)
            , GotReplyLatch(2)
            , SentMessage()
        {
        }

        TJobHandler Start(TBusJob* job, TBusMessage* mess) override {
            Y_UNUSED(mess);
            TExampleRequest* message = new TExampleRequest(&Proto.RequestCount);
            job->Send(message, Source, TReplyHandler(&TErrorHandlerCheckerModule::ReplyHandler), 0, ServerAddr);
            SentMessage = message;
            return &TErrorHandlerCheckerModule::HandleReplies;
        }

        void ReplyHandler(TBusJob*, EMessageStatus status, TBusMessage* req, TBusMessage* resp) {
            Y_ABORT_UNLESS(status == MESSAGE_CONNECT_FAILED || status == MESSAGE_TIMEOUT, "got wrong status: %s", ToString(status).data());
            Y_ABORT_UNLESS(req == SentMessage, "checking request");
            Y_ABORT_UNLESS(resp == nullptr, "checking response");
            GotReplyLatch.CountDown();
        }

        TJobHandler HandleReplies(TBusJob* job, TBusMessage* mess) {
            Y_UNUSED(mess);
            job->Cancel(MESSAGE_UNKNOWN);
            GotReplyLatch.CountDown();
            return nullptr;
        }

        TBusServerSessionPtr CreateExtSession(TBusMessageQueue& queue) override {
            TBusClientSessionConfig sessionConfig;
            sessionConfig.SendTimeout = 1; // TODO: allow 0
            sessionConfig.Secret.TimeoutPeriod = TDuration::MilliSeconds(10);
            Source = CreateDefaultSource(queue, &Proto, sessionConfig);
            Source->RegisterService("localhost");
            return nullptr;
        }
    };

    Y_UNIT_TEST(ErrorHandler) {
        TExampleProtocol proto;

        TBusQueueConfig config;
        config.NumWorkers = 5;

        TErrorHandlerCheckerModule module;

        TBusModuleConfig moduleConfig;
        moduleConfig.Secret.SchedulePeriod = TDuration::MilliSeconds(10);
        module.SetConfig(moduleConfig);

        module.StartModule();

        module.StartJob(new TExampleRequest(&proto.StartCount));

        module.GotReplyLatch.Await();

        module.Shutdown();
    }

    struct TSlowReplyServer: public TBusServerHandlerError {
        TTestSync* const TestSync;
        TBusMessageQueuePtr Bus;
        TBusServerSessionPtr ServerSession;
        TExampleProtocol Proto;

        TAtomic OnMessageCount;

        TSlowReplyServer(TTestSync* testSync)
            : TestSync(testSync)
            , OnMessageCount(0)
        {
            Bus = CreateMessageQueue("TSlowReplyServer");
            TBusServerSessionConfig sessionConfig;
            ServerSession = TBusServerSession::Create(&Proto, this, sessionConfig, Bus);
        }

        void OnMessage(TOnMessageContext& req) override {
            if (AtomicIncrement(OnMessageCount) == 1) {
                TestSync->WaitForAndIncrement(0);
            }
            TAutoPtr<TBusMessage> response(new TExampleResponse(&Proto.ResponseCount));
            req.SendReplyMove(response);
        }
    };

    struct TModuleThatSendsReplyEarly: public TExampleClientModule {
        TTestSync* const TestSync;
        const unsigned ServerPort;

        TBusServerSessionPtr ServerSession;
        TAtomic ReplyCount;

        TModuleThatSendsReplyEarly(TTestSync* testSync, unsigned serverPort)
            : TestSync(testSync)
            , ServerPort(serverPort)
            , ServerSession(nullptr)
            , ReplyCount(0)
        {
        }

        TJobHandler Start(TBusJob* job, TBusMessage* mess) override {
            Y_UNUSED(mess);
            for (unsigned i = 0; i < 2; ++i) {
                job->Send(
                    new TExampleRequest(&Proto.RequestCount),
                    Source,
                    TReplyHandler(&TModuleThatSendsReplyEarly::ReplyHandler),
                    0,
                    TNetAddr("127.0.0.1", ServerPort));
            }
            return &TModuleThatSendsReplyEarly::HandleReplies;
        }

        void ReplyHandler(TBusJob* job, EMessageStatus status, TBusMessage* mess, TBusMessage* reply) {
            Y_UNUSED(mess);
            Y_UNUSED(reply);
            Y_ABORT_UNLESS(status == MESSAGE_OK, "failed to get reply");
            if (AtomicIncrement(ReplyCount) == 1) {
                TestSync->WaitForAndIncrement(1);
                job->SendReply(new TExampleResponse(&Proto.ResponseCount));
            } else {
                TestSync->WaitForAndIncrement(3);
            }
        }

        TJobHandler HandleReplies(TBusJob* job, TBusMessage* mess) {
            Y_UNUSED(mess);
            job->Cancel(MESSAGE_UNKNOWN);
            return nullptr;
        }

        TBusServerSessionPtr CreateExtSession(TBusMessageQueue& queue) override {
            TExampleClientModule::CreateExtSession(queue);
            TBusServerSessionConfig sessionConfig;
            return ServerSession = CreateDefaultDestination(queue, &Proto, sessionConfig);
        }
    };

    Y_UNIT_TEST(SendReplyCalledBeforeAllRepliesReceived) {
        TTestSync testSync;

        TSlowReplyServer slowReplyServer(&testSync);

        TModuleThatSendsReplyEarly module(&testSync, slowReplyServer.ServerSession->GetActualListenPort());
        module.StartModule();

        TExampleClient client;
        TNetAddr addr("127.0.0.1", module.ServerSession->GetActualListenPort());
        client.SendMessagesWaitReplies(1, &addr);

        testSync.WaitForAndIncrement(2);

        module.Shutdown();
    }

    struct TShutdownCalledBeforeReplyReceivedModule: public TExampleClientModule {
        unsigned ServerPort;

        TTestSync TestSync;

        TShutdownCalledBeforeReplyReceivedModule(unsigned serverPort)
            : ServerPort(serverPort)
        {
        }

        TJobHandler Start(TBusJob* job, TBusMessage*) override {
            TestSync.CheckAndIncrement(0);

            job->Send(new TExampleRequest(&Proto.RequestCount), Source,
                      TReplyHandler(&TShutdownCalledBeforeReplyReceivedModule::HandleReply),
                      0, TNetAddr("localhost", ServerPort));
            return &TShutdownCalledBeforeReplyReceivedModule::End;
        }

        void HandleReply(TBusJob*, EMessageStatus status, TBusMessage*, TBusMessage*) {
            Y_ABORT_UNLESS(status == MESSAGE_SHUTDOWN, "got %s", ToCString(status));
            TestSync.CheckAndIncrement(1);
        }

        TJobHandler End(TBusJob* job, TBusMessage*) {
            TestSync.CheckAndIncrement(2);
            job->Cancel(MESSAGE_SHUTDOWN);
            return nullptr;
        }
    };

    Y_UNIT_TEST(ShutdownCalledBeforeReplyReceived) {
        TExampleServer server;
        server.ForgetRequest = true;

        TShutdownCalledBeforeReplyReceivedModule module(server.GetActualListenPort());

        module.StartModule();

        module.StartJob(new TExampleRequest(&module.Proto.RequestCount));

        server.TestSync.WaitFor(1);

        module.Shutdown();

        module.TestSync.CheckAndIncrement(3);
    }
}
