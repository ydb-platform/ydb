#include "cli.h"
#include "cli_cmds.h"

namespace NKikimr {
namespace NDriverClient {

TClientCommandWhoAmI::TClientCommandWhoAmI()
    : TClientCommand("whoami", {}, "Who am I?")
{}

void TClientCommandWhoAmI::Config(TConfig& config) {
    TClientCommand::Config(config);
    config.Opts->AddLongOption('g', "groups", "With groups").NoArgument().SetFlag(&WithGroups);
}

int TClientCommandWhoAmI::Run(TConfig& config) {
    TAutoPtr<NMsgBusProxy::TBusWhoAmI> request(new NMsgBusProxy::TBusWhoAmI());
    request->Record.SetReturnGroups(WithGroups);
    return MessageBusCall<NMsgBusProxy::TBusWhoAmI, NMsgBusProxy::TBusResponse>(config, request,
        [this](const NMsgBusProxy::TBusResponse& response) -> int {
            return PrintResponse(response);
    });
}

int TClientCommandWhoAmI::PrintResponse(const NMsgBusProxy::TBusResponse& response) const {
    const NKikimrClient::TResponse& record(response.Record);
    if (!record.GetUserName().empty()) {
        Cout << record.GetUserName() << Endl;
        if (record.GroupsSize() > 0) {
            Cout << TString(30, '-') << Endl;
            for (const TString& group : record.GetGroups()) {
                Cout << group << Endl;
            }
        }
    }
    return 0;
}

}
}
