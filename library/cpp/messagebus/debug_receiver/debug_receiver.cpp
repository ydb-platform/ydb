#include "debug_receiver_handler.h"
#include "debug_receiver_proto.h"

#include <library/cpp/messagebus/ybus.h>

#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/lwtrace/all.h>

using namespace NBus;

int main(int argc, char** argv) {
    NLWTrace::StartLwtraceFromEnv();

    TBusQueueConfig queueConfig;
    TBusServerSessionConfig sessionConfig;

    NLastGetopt::TOpts opts;

    queueConfig.ConfigureLastGetopt(opts);
    sessionConfig.ConfigureLastGetopt(opts);

    opts.AddLongOption("port").Required().RequiredArgument("PORT").StoreResult(&sessionConfig.ListenPort);

    opts.SetFreeArgsMax(0);

    NLastGetopt::TOptsParseResult r(&opts, argc, argv);

    TBusMessageQueuePtr q(CreateMessageQueue(queueConfig));

    TDebugReceiverProtocol proto;
    TDebugReceiverHandler handler;

    TBusServerSessionPtr serverSession = TBusServerSession::Create(&proto, &handler, sessionConfig, q);
    // TODO: race is here
    handler.ServerSession = serverSession.Get();

    for (;;) {
        Sleep(TDuration::Hours(17));
    }

    return 0;
}
