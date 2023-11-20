#include <library/cpp/messagebus/test/example/common/proto.h>

#include <util/random/random.h>

using namespace NBus;
using namespace NCalculator;

namespace NCalculator {
    struct TCalculatorClient: public IBusClientHandler {
        TCalculatorProtocol Proto;
        TBusMessageQueuePtr MessageQueue;
        TBusClientSessionPtr ClientSession;

        TCalculatorClient() {
            MessageQueue = CreateMessageQueue();
            TBusClientSessionConfig config;
            config.TotalTimeout = 2 * 1000;
            ClientSession = TBusClientSession::Create(&Proto, this, config, MessageQueue);
        }

        ~TCalculatorClient() override {
            MessageQueue->Stop();
        }

        void OnReply(TAutoPtr<TBusMessage> request, TAutoPtr<TBusMessage> response0) override {
            Y_ABORT_UNLESS(response0->GetHeader()->Type == TResponse::MessageType, "wrong response");
            TResponse* response = VerifyDynamicCast<TResponse*>(response0.Get());
            if (request->GetHeader()->Type == TRequestSum::MessageType) {
                TRequestSum* requestSum = VerifyDynamicCast<TRequestSum*>(request.Get());
                int a = requestSum->Record.GetA();
                int b = requestSum->Record.GetB();
                Cerr << a << " + " << b << " = " << response->Record.GetResult() << "\n";
            } else if (request->GetHeader()->Type == TRequestMul::MessageType) {
                TRequestMul* requestMul = VerifyDynamicCast<TRequestMul*>(request.Get());
                int a = requestMul->Record.GetA();
                int b = requestMul->Record.GetB();
                Cerr << a << " * " << b << " = " << response->Record.GetResult() << "\n";
            } else {
                Y_ABORT("unknown request");
            }
        }

        void OnError(TAutoPtr<TBusMessage>, EMessageStatus status) override {
            Cerr << "got error " << status << "\n";
        }
    };

}

int main(int, char**) {
    TCalculatorClient client;

    for (;;) {
        TNetAddr addr(TNetAddr("127.0.0.1", TCalculatorProtocol().GetPort()));

        int a = RandomNumber<unsigned>(10);
        int b = RandomNumber<unsigned>(10);
        EMessageStatus ok;
        if (RandomNumber<bool>()) {
            TAutoPtr<TRequestSum> request(new TRequestSum);
            request->Record.SetA(a);
            request->Record.SetB(b);
            Cerr << "sending " << a << " + " << b << "\n";
            ok = client.ClientSession->SendMessageAutoPtr(request, &addr);
        } else {
            TAutoPtr<TRequestMul> request(new TRequestMul);
            request->Record.SetA(a);
            request->Record.SetB(b);
            Cerr << "sending " << a << " * " << b << "\n";
            ok = client.ClientSession->SendMessageAutoPtr(request, &addr);
        }

        if (ok != MESSAGE_OK) {
            Cerr << "failed to send message " << ok << "\n";
        }

        Sleep(TDuration::Seconds(1));
    }

    return 0;
}
