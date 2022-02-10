#include "cli.h"
#include <ydb/public/lib/deprecated/client/msgbus_client.h>
#include <ydb/public/lib/deprecated/client/msgbus_player.h>

namespace NKikimr {
namespace NDriverClient {

struct TCmdMessageBusTraceConfig : public TCliCmdConfig {
    TString Command;
    TString Path;
    ui32 MaxInFlight;

    TCmdMessageBusTraceConfig() : MaxInFlight(1) {}
    void Parse(int argc, char **argv);
};

int MessageBusTrace(TCommandConfig &cmdConf, int argc, char** argv) {
    Y_UNUSED(cmdConf);

#ifdef _win32_
    WSADATA dummy;
    WSAStartup(MAKEWORD(2, 2), &dummy);
#endif

    TCmdMessageBusTraceConfig messageBusTraceConfig;
    messageBusTraceConfig.Parse(argc, argv);

    NMsgBusProxy::TMsgBusClient client(std::get<NMsgBusProxy::TMsgBusClientConfig>(*messageBusTraceConfig.ClientConfig));
    client.Init();

    if (messageBusTraceConfig.Command == "play") {
        try {
            bool displayProgress = isatty(fileno(stdout)); // don't know better way to check on all platforms for redirected output
            NMessageBusTracer::TMsgBusPlayer player(client);
            player.PlayTrace(messageBusTraceConfig.Path, messageBusTraceConfig.MaxInFlight, displayProgress ? [](int p){ Cout << p << "%\r"; Cout.Flush(); } : std::function<void(int)>());
            if (displayProgress)
                Cout << Endl;
        }
        catch(const yexception& e) {
            Cerr << "Caught exception: " << e.what() << Endl;
            return 1;
        }
        return 0;
    } else {
        NMsgBusProxy::TBusMessageBusTraceRequest *request(new NMsgBusProxy::TBusMessageBusTraceRequest());
        if (messageBusTraceConfig.Command == "start") {
            request->Record.SetCommand(NKikimrClient::TMessageBusTraceRequest::START);
        } else if (messageBusTraceConfig.Command == "stop") {
            request->Record.SetCommand(NKikimrClient::TMessageBusTraceRequest::STOP);
        }
        if (messageBusTraceConfig.Path)
            request->Record.SetPath(messageBusTraceConfig.Path);

        TAutoPtr<NBus::TBusMessage> reply;
        NBus::EMessageStatus status = client.SyncCall(request, reply);

        switch (status) {
        case NBus::MESSAGE_OK:
            {
                const NKikimrClient::TMessageBusTraceStatus &response = static_cast<NMsgBusProxy::TBusMessageBusTraceStatus *>(reply.Get())->Record;
//                Cout << "status: " << response.GetStatus() << Endl;
//                Cout << "status transcript: " << static_cast<NMsgBusProxy::EResponseStatus>(response.GetStatus()) << Endl;
                Cout << "trace active: " << response.GetTraceActive() << Endl;
                if (response.GetTraceActive()) {
                    Cout << "trace file: " << response.GetPath() << Endl;
                }

                return 0;
            }
        default:
            {
                const char *description = NBus::MessageStatusDescription(status);
                Cerr << description << Endl;
            }
            return 1;
        }
    }
}

void TCmdMessageBusTraceConfig::Parse(int argc, char **argv) {
    using namespace NLastGetopt;

    TOpts opts = TOpts::Default();
    opts.AddLongOption('c', "cmd", "command").Required().RequiredArgument("[start|stop|play]").StoreResult(&Command); // {start|stop|play}
    opts.AddLongOption("path", "name of trace file").Optional().RequiredArgument("PATH").StoreResult(&Path); // path to trace file
    opts.AddLongOption("inflight", "max inflight messages").Optional().RequiredArgument("NUM").StoreResult(&MaxInFlight); // max inflight messages
    ConfigureBaseLastGetopt(opts);
    TOptsParseResult res(&opts, argc, argv);
    if (Command != "start" && Command != "stop" && Command != "play") {
        ythrow yexception() << "command should be one of [start|stop|play].";
    }
    if (MaxInFlight == 0)
        ythrow yexception() << "inflight should be greater than 0";
    ConfigureMsgBusLastGetopt(res, argc, argv);
}

}
}
