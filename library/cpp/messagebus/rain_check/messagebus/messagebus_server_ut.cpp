#include <library/cpp/testing/unittest/registar.h>

#include "messagebus_server.h"

#include <library/cpp/messagebus/rain_check/test/ut/test.h>

#include <library/cpp/messagebus/test/helper/example.h>

using namespace NBus;
using namespace NBus::NTest;
using namespace NRainCheck;

struct TMessageBusServerEnv: public TTestEnvTemplate<TMessageBusServerEnv> {
    TExampleProtocol Proto;
};

Y_UNIT_TEST_SUITE(RainCheckMessageBusServer) {
    struct TSimpleServerTask: public ISimpleTask {
    private:
        TMessageBusServerEnv* const Env;
        TOnMessageContext MessageContext;

    public:
        TSimpleServerTask(TMessageBusServerEnv* env, TOnMessageContext& messageContext)
            : Env(env)
        {
            MessageContext.Swap(messageContext);
        }

        TContinueFunc Start() override {
            MessageContext.SendReplyMove(new TExampleResponse(&Env->Proto.ResponseCount));
            return nullptr;
        }
    };

    Y_UNIT_TEST(Simple) {
        TMessageBusServerEnv env;

        THolder<TBusTaskStarter> starter(TBusTaskStarter::NewStarter<TSimpleServerTask>(&env));

        TBusMessageQueuePtr queue(CreateMessageQueue(env.GetExecutor()));

        TExampleProtocol proto;

        TBusServerSessionPtr session = queue->CreateDestination(&env.Proto, starter.Get(), TBusSessionConfig());

        TExampleClient client;

        client.SendMessagesWaitReplies(1, TNetAddr("localhost", session->GetActualListenPort()));
    }
}
