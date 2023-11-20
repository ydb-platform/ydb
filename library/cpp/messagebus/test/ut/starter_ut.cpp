#include <library/cpp/testing/unittest/registar.h>

#include <library/cpp/messagebus/test/helper/example_module.h>
#include <library/cpp/messagebus/test/helper/object_count_check.h>
#include <library/cpp/messagebus/test/helper/wait_for.h>

using namespace NBus;
using namespace NBus::NTest;

Y_UNIT_TEST_SUITE(TBusStarterTest) {
    struct TStartJobTestModule: public TExampleModule {
        using TBusModule::CreateDefaultStarter;

        TAtomic StartCount;

        TStartJobTestModule()
            : StartCount(0)
        {
        }

        TJobHandler Start(TBusJob* job, TBusMessage* mess) override {
            Y_UNUSED(mess);
            AtomicIncrement(StartCount);
            job->Sleep(10);
            return &TStartJobTestModule::End;
        }

        TJobHandler End(TBusJob* job, TBusMessage* mess) {
            Y_UNUSED(mess);
            AtomicIncrement(StartCount);
            job->Cancel(MESSAGE_UNKNOWN);
            return nullptr;
        }
    };

    Y_UNIT_TEST(Test) {
        TObjectCountCheck objectCountCheck;

        TBusMessageQueuePtr bus(CreateMessageQueue());

        TStartJobTestModule module;

        //module.StartModule();
        module.CreatePrivateSessions(bus.Get());
        module.StartInput();

        TBusSessionConfig config;
        config.SendTimeout = 10;

        module.CreateDefaultStarter(*bus, config);

        UNIT_WAIT_FOR(AtomicGet(module.StartCount) >= 3);

        module.Shutdown();
        bus->Stop();
    }

    Y_UNIT_TEST(TestModuleStartJob) {
        TObjectCountCheck objectCountCheck;

        TExampleProtocol proto;

        TStartJobTestModule module;

        TBusModuleConfig moduleConfig;
        moduleConfig.Secret.SchedulePeriod = TDuration::MilliSeconds(10);
        module.SetConfig(moduleConfig);

        module.StartModule();

        module.StartJob(new TExampleRequest(&proto.RequestCount));

        UNIT_WAIT_FOR(AtomicGet(module.StartCount) != 2);

        module.Shutdown();
    }

    struct TSleepModule: public TExampleServerModule {
        TSystemEvent MessageReceivedEvent;

        TJobHandler Start(TBusJob* job, TBusMessage* mess) override {
            Y_UNUSED(mess);

            MessageReceivedEvent.Signal();

            job->Sleep(1000000000);

            return TJobHandler(&TSleepModule::Never);
        }

        TJobHandler Never(TBusJob*, TBusMessage*) {
            Y_ABORT("happens");
            throw 1;
        }
    };

    Y_UNIT_TEST(StartJobDestroyDuringSleep) {
        TObjectCountCheck objectCountCheck;

        TExampleProtocol proto;

        TSleepModule module;

        module.StartModule();

        module.StartJob(new TExampleRequest(&proto.StartCount));

        module.MessageReceivedEvent.WaitI();

        module.Shutdown();
    }

    struct TSendReplyModule: public TExampleServerModule {
        TSystemEvent MessageReceivedEvent;

        TJobHandler Start(TBusJob* job, TBusMessage* mess) override {
            Y_UNUSED(mess);

            job->SendReply(new TExampleResponse(&Proto.ResponseCount));

            MessageReceivedEvent.Signal();

            return nullptr;
        }
    };

    Y_UNIT_TEST(AllowSendReplyInStarted) {
        TObjectCountCheck objectCountCheck;

        TExampleProtocol proto;

        TSendReplyModule module;
        module.StartModule();
        module.StartJob(new TExampleRequest(&proto.StartCount));

        module.MessageReceivedEvent.WaitI();

        module.Shutdown();
    }
}
