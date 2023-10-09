#include <library/cpp/testing/unittest/registar.h>

#include "count_down_latch.h"
#include "moduletest.h"

#include <library/cpp/messagebus/test/helper/example.h>
#include <library/cpp/messagebus/test/helper/example_module.h>
#include <library/cpp/messagebus/test/helper/object_count_check.h>
#include <library/cpp/messagebus/test/helper/wait_for.h>

#include <library/cpp/messagebus/oldmodule/module.h>

#include <util/generic/cast.h>

using namespace NBus;
using namespace NBus::NTest;

Y_UNIT_TEST_SUITE(ModuleServerTests) {
    Y_UNIT_TEST(TestModule) {
        TObjectCountCheck objectCountCheck;

        /// create or get instance of message queue, need one per application
        TBusMessageQueuePtr bus(CreateMessageQueue());
        THostInfoHandler hostHandler(bus.Get());
        TDupDetectModule module(hostHandler.GetActualListenAddr());
        bool success;
        success = module.Init(bus.Get());
        UNIT_ASSERT_C(success, "failed to initialize dupdetect module");

        success = module.StartInput();
        UNIT_ASSERT_C(success, "failed to start dupdetect module");

        TDupDetectHandler dupHandler(module.ListenAddr, bus.Get());
        dupHandler.Work();

        UNIT_WAIT_FOR(dupHandler.NumMessages == dupHandler.NumReplies);

        module.Shutdown();
        dupHandler.DupDetect->Shutdown();
    }

    struct TParallelOnMessageModule: public TExampleServerModule {
        TCountDownLatch WaitTwoRequestsLatch;

        TParallelOnMessageModule()
            : WaitTwoRequestsLatch(2)
        {
        }

        TJobHandler Start(TBusJob* job, TBusMessage* mess) override {
            WaitTwoRequestsLatch.CountDown();
            Y_ABORT_UNLESS(WaitTwoRequestsLatch.Await(TDuration::Seconds(5)), "oops");

            VerifyDynamicCast<TExampleRequest*>(mess);

            job->SendReply(new TExampleResponse(&Proto.ResponseCount));
            return nullptr;
        }
    };

    Y_UNIT_TEST(TestOnMessageHandlerCalledInParallel) {
        TObjectCountCheck objectCountCheck;

        TBusQueueConfig config;
        config.NumWorkers = 5;

        TParallelOnMessageModule module;
        module.StartModule();

        TExampleClient client;

        client.SendMessagesWaitReplies(2, module.ServerAddr);

        module.Shutdown();
    }

    struct TDelayReplyServer: public TExampleServerModule {
        TSystemEvent MessageReceivedEvent;
        TSystemEvent ClientDiedEvent;

        TJobHandler Start(TBusJob* job, TBusMessage* mess) override {
            Y_UNUSED(mess);

            MessageReceivedEvent.Signal();

            Y_ABORT_UNLESS(ClientDiedEvent.WaitT(TDuration::Seconds(5)), "oops");

            job->SendReply(new TExampleResponse(&Proto.ResponseCount));
            return nullptr;
        }
    };

    Y_UNIT_TEST(TestReplyCalledAfterClientDisconnected) {
        TObjectCountCheck objectCountCheck;

        TBusQueueConfig config;
        config.NumWorkers = 5;

        TDelayReplyServer server;
        server.StartModule();

        THolder<TExampleClient> client(new TExampleClient);

        client->SendMessages(1, server.ServerAddr);

        UNIT_ASSERT(server.MessageReceivedEvent.WaitT(TDuration::Seconds(5)));

        UNIT_ASSERT_VALUES_EQUAL(1, server.GetModuleSessionInFlight());

        client.Destroy();

        server.ClientDiedEvent.Signal();

        // wait until all server message are delivered
        UNIT_WAIT_FOR(0 == server.GetModuleSessionInFlight());

        server.Shutdown();
    }
}
