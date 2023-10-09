#include <library/cpp/testing/unittest/registar.h>

#include "messagebus_client.h"

#include <library/cpp/messagebus/rain_check/test/ut/test.h>

#include <library/cpp/messagebus/test/helper/example.h>
#include <library/cpp/messagebus/test/helper/object_count_check.h>

#include <util/generic/cast.h>

using namespace NBus;
using namespace NBus::NTest;
using namespace NRainCheck;

struct TMessageBusClientEnv: public TTestEnvTemplate<TMessageBusClientEnv> {
    // TODO: use same thread pool
    TBusMessageQueuePtr Queue;
    TExampleProtocol Proto;
    TBusClientService BusClientService;

    static TBusQueueConfig QueueConfig() {
        TBusQueueConfig r;
        r.NumWorkers = 4;
        return r;
    }

    TMessageBusClientEnv()
        : Queue(CreateMessageQueue(GetExecutor()))
        , BusClientService(TBusSessionConfig(), &Proto, Queue.Get())
    {
    }
};

Y_UNIT_TEST_SUITE(RainCheckMessageBusClient) {
    struct TSimpleTask: public ISimpleTask {
        TMessageBusClientEnv* const Env;

        const unsigned ServerPort;

        TSimpleTask(TMessageBusClientEnv* env, unsigned serverPort)
            : Env(env)
            , ServerPort(serverPort)
        {
        }

        TVector<TSimpleSharedPtr<TBusFuture>> Requests;

        TContinueFunc Start() override {
            for (unsigned i = 0; i < 3; ++i) {
                Requests.push_back(new TBusFuture);
                TNetAddr addr("localhost", ServerPort);
                Env->BusClientService.Send(new TExampleRequest(&Env->Proto.RequestCount), addr, Requests[i].Get());
            }

            return TContinueFunc(&TSimpleTask::GotReplies);
        }

        TContinueFunc GotReplies() {
            for (unsigned i = 0; i < Requests.size(); ++i) {
                Y_ABORT_UNLESS(Requests[i]->GetStatus() == MESSAGE_OK);
                VerifyDynamicCast<TExampleResponse*>(Requests[i]->GetResponse());
            }
            Env->TestSync.CheckAndIncrement(0);
            return nullptr;
        }
    };

    Y_UNIT_TEST(Simple) {
        TObjectCountCheck objectCountCheck;

        TExampleServer server;

        TMessageBusClientEnv env;

        TIntrusivePtr<TSimpleTaskRunner> task = env.SpawnTask<TSimpleTask>(server.GetActualListenPort());

        env.TestSync.WaitForAndIncrement(1);
    }

    struct TOneWayServer: public NBus::IBusServerHandler {
        TTestSync* const TestSync;
        TExampleProtocol Proto;
        NBus::TBusMessageQueuePtr Queue;
        NBus::TBusServerSessionPtr Session;

        TOneWayServer(TTestSync* testSync)
            : TestSync(testSync)
        {
            Queue = CreateMessageQueue();
            Session = Queue->CreateDestination(&Proto, this, NBus::TBusSessionConfig());
        }

        void OnMessage(NBus::TOnMessageContext& context) override {
            TestSync->CheckAndIncrement(1);
            context.ForgetRequest();
        }
    };

    struct TOneWayTask: public ISimpleTask {
        TMessageBusClientEnv* const Env;

        const unsigned ServerPort;

        TOneWayTask(TMessageBusClientEnv* env, unsigned serverPort)
            : Env(env)
            , ServerPort(serverPort)
        {
        }

        TVector<TSimpleSharedPtr<TBusFuture>> Requests;

        TContinueFunc Start() override {
            Env->TestSync.CheckAndIncrement(0);

            for (unsigned i = 0; i < 1; ++i) {
                Requests.push_back(new TBusFuture);
                TNetAddr addr("localhost", ServerPort);
                Env->BusClientService.SendOneWay(new TExampleRequest(&Env->Proto.RequestCount), addr, Requests[i].Get());
            }

            return TContinueFunc(&TOneWayTask::GotReplies);
        }

        TContinueFunc GotReplies() {
            for (unsigned i = 0; i < Requests.size(); ++i) {
                Y_ABORT_UNLESS(Requests[i]->GetStatus() == MESSAGE_OK);
                Y_ABORT_UNLESS(!Requests[i]->GetResponse());
            }
            Env->TestSync.WaitForAndIncrement(2);
            return nullptr;
        }
    };

    Y_UNIT_TEST(OneWay) {
        TObjectCountCheck objectCountCheck;

        TMessageBusClientEnv env;

        TOneWayServer server(&env.TestSync);

        TIntrusivePtr<TSimpleTaskRunner> task = env.SpawnTask<TOneWayTask>(server.Session->GetActualListenPort());

        env.TestSync.WaitForAndIncrement(3);
    }
}
