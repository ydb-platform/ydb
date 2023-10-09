#include "cli.h"
#include <ydb/public/lib/deprecated/client/msgbus_client.h>
#include <ydb/core/base/tablet_types.h>

namespace NKikimr {
namespace NDriverClient {

struct TCmdPersQueueStressConfig : public TCliCmdConfig {
    TString Proto; //for config
    TCmdPersQueueStressConfig();

    void Parse(int argc, char **argv);
};

int PersQueueRequest(TCommandConfig &cmdConf, int argc, char** argv) {
    Y_UNUSED(cmdConf);


    TCmdPersQueueStressConfig config;
    config.Parse(argc, argv);

    TAutoPtr<NMsgBusProxy::TBusPersQueue> request(new NMsgBusProxy::TBusPersQueue);
    const bool isOk = ::google::protobuf::TextFormat::ParseFromString(config.Proto, &request->Record);
    if (!isOk) {
        ythrow TWithBackTrace<yexception>() << "Error parsing protobuf: \'" << config.Proto << "\'";
    }
    TAutoPtr<NBus::TBusMessage> reply;
    NBus::EMessageStatus status = config.SyncCall(request, reply);
    Cerr << status << "\n";
    Y_ABORT_UNLESS(status == NBus::MESSAGE_OK);
    const auto& result = static_cast<NMsgBusProxy::TBusResponse *>(reply.Get())->Record;
    Cerr << result.DebugString() << "\n";
    return 0;
}

TCmdPersQueueStressConfig::TCmdPersQueueStressConfig()
{}

void TCmdPersQueueStressConfig::Parse(int argc, char **argv) {
    using namespace NLastGetopt;

    TString fileName;

    TOpts opts = TOpts::Default();
    opts.AddLongOption("protobuf", "string representation of the request protobuf").Optional().StoreResult(&Proto);
    opts.AddLongOption("protofile", "file with protobuf").Optional().StoreResult(&fileName);
    ConfigureBaseLastGetopt(opts);

    TOptsParseResult res(&opts, argc, argv);
    if (fileName.empty() == Proto.empty()) {
        Cerr << "one of protobuf or protofile must be set\n";
        exit(1);
    }
    if (fileName) {
        Proto = TUnbufferedFileInput(fileName).ReadAll();
    }
    ConfigureMsgBusLastGetopt(res, argc, argv);
}

}
}
