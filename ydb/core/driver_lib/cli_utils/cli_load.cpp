#include "cli.h"
#include <google/protobuf/text_format.h>

namespace NKikimr {
namespace NDriverClient {

struct TCmdLoadConfig : public TCliCmdConfig {
    TString Proto;

    void Parse(int argc, char **argv) {
        using namespace NLastGetopt;
        TOpts opts = TOpts::Default();
        opts.AddLongOption("protobuf", "string representation of the keyvalue request protobuf").Required()
            .RequiredArgument("PROTOBUF").StoreResult(&Proto);

        ConfigureBaseLastGetopt(opts);
        TOptsParseResult res(&opts, argc, argv);
        ConfigureMsgBusLastGetopt(res, argc, argv);
    }
};

int LoadRequest(TCommandConfig& /*cmdConf*/, int argc, char **argv) {
    TCmdLoadConfig config;
    config.Parse(argc, argv);

    TAutoPtr<NMsgBusProxy::TBusBsTestLoadRequest> request(new NMsgBusProxy::TBusBsTestLoadRequest);
    const bool isOk = ::google::protobuf::TextFormat::ParseFromString(config.Proto, &request->Record);
    if (!isOk) {
        ythrow TWithBackTrace<yexception>() << "Error parsing protobuf: '" << config.Proto << "'";
    }

    TAutoPtr<NBus::TBusMessage> reply;
    NBus::EMessageStatus status = config.SyncCall(request, reply);

    switch (status) {
        case NBus::MESSAGE_OK: {
            const NKikimrClient::TBsTestLoadResponse& response =
                static_cast<NMsgBusProxy::TBusBsTestLoadResponse *>(reply.Get())->Record;
            bool status = true;
            for (const NKikimrClient::TBsTestLoadResponse::TItem& item : response.GetItems()) {
                if (item.GetStatus() != NMsgBusProxy::MSTATUS_OK) {
                    Cerr << "NodeId# " << item.GetNodeId() << " ErrorReason# " << item.GetErrorReason() << Endl;
                    status = false;
                }
            }
            if (!status) {
                return EXIT_FAILURE;
            }
            break;
        }

        default: {
            const char *description = NBus::MessageStatusDescription(status);
            Cerr << description << Endl;
            return EXIT_FAILURE;
        }
    }

    return EXIT_SUCCESS;
}

} // NKikimr
} // NDriverClient
