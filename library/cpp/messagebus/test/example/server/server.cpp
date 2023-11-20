#include <library/cpp/messagebus/test/example/common/proto.h>

using namespace NBus;
using namespace NCalculator;

namespace NCalculator {
    struct TCalculatorServer: public IBusServerHandler {
        TCalculatorProtocol Proto;
        TBusMessageQueuePtr MessageQueue;
        TBusServerSessionPtr ServerSession;

        TCalculatorServer() {
            MessageQueue = CreateMessageQueue();
            TBusServerSessionConfig config;
            ServerSession = TBusServerSession::Create(&Proto, this, config, MessageQueue);
        }

        ~TCalculatorServer() override {
            MessageQueue->Stop();
        }

        void OnMessage(TOnMessageContext& request) override {
            if (request.GetMessage()->GetHeader()->Type == TRequestSum::MessageType) {
                TRequestSum* requestSum = VerifyDynamicCast<TRequestSum*>(request.GetMessage());
                int a = requestSum->Record.GetA();
                int b = requestSum->Record.GetB();
                int result = a + b;
                Cerr << "requested " << a << " + " << b << ", sending " << result << "\n";
                TAutoPtr<TResponse> response(new TResponse);
                response->Record.SetResult(result);
                request.SendReplyMove(response);
            } else if (request.GetMessage()->GetHeader()->Type == TRequestMul::MessageType) {
                TRequestMul* requestMul = VerifyDynamicCast<TRequestMul*>(request.GetMessage());
                int a = requestMul->Record.GetA();
                int b = requestMul->Record.GetB();
                int result = a * b;
                Cerr << "requested " << a << " * " << b << ", sending " << result << "\n";
                TAutoPtr<TResponse> response(new TResponse);
                response->Record.SetResult(result);
                request.SendReplyMove(response);
            } else {
                Y_ABORT("unknown request");
            }
        }
    };
}

int main(int, char**) {
    TCalculatorServer server;

    Cerr << "listening on port " << server.ServerSession->GetActualListenPort() << "\n";

    for (;;) {
        Sleep(TDuration::Seconds(1));
    }

    return 0;
}
