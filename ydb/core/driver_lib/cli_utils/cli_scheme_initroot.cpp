#include "cli.h"
#include <ydb/public/lib/deprecated/client/msgbus_client.h>
#include <ydb/core/scheme/tablet_scheme.h>
#include <ydb/core/scheme/scheme_types_defs.h>
#include <ydb/core/scheme/scheme_type_registry.h>
#include <ydb/core/protos/tx_scheme.pb.h>

namespace NKikimr {
namespace NDriverClient {

struct TCmdSchemeInitShardConfig : public TCliCmdConfig {
    TString TagName;

    TAutoPtr<NKikimrTxScheme::TConfig> GlobalConfig;

    TCmdSchemeInitShardConfig();

    void Parse(int argc, char **argv);
};

int SchemeInitRoot(TCommandConfig &cmdConf, int argc, char** argv) {
    Y_UNUSED(cmdConf);

#ifdef _win32_
    WSADATA dummy;
    WSAStartup(MAKEWORD(2, 2), &dummy);
#endif

    TCmdSchemeInitShardConfig schemeInitShardConfig;
    schemeInitShardConfig.Parse(argc, argv);

    TAutoPtr<NMsgBusProxy::TBusSchemeInitRoot> request(new NMsgBusProxy::TBusSchemeInitRoot());

    request->Record.SetTagName(schemeInitShardConfig.TagName);

    if (schemeInitShardConfig.GlobalConfig)
        request->Record.MutableGlobalConfig()->MergeFrom(*schemeInitShardConfig.GlobalConfig);

    TAutoPtr<NBus::TBusMessage> reply;
    NBus::EMessageStatus status = schemeInitShardConfig.SyncCall(request, reply);

    switch (status) {
    case NBus::MESSAGE_OK:
        {
            const NKikimrClient::TResponse &response = static_cast<NMsgBusProxy::TBusResponse *>(reply.Get())->Record;
            Cout << "status: " << response.GetStatus() << Endl;
            Cout << "status transcript: " << static_cast<NMsgBusProxy::EResponseStatus>(response.GetStatus()) << Endl;
            if (response.HasTabletId())
                Cout << "tabletid: " << response.GetTabletId() << Endl;

            return response.GetStatus() == NMsgBusProxy::MSTATUS_OK ? 0 : 1;
        }
    default:
        {
            const char *description = NBus::MessageStatusDescription(status);
            Cerr << description << Endl;
        }
        return 1;
    }
}

TCmdSchemeInitShardConfig::TCmdSchemeInitShardConfig()
    : TagName()
{}

void TCmdSchemeInitShardConfig::Parse(int argc, char **argv) {
    using namespace NLastGetopt;

    TString configPb;
    TString configPbFile;

    TOpts opts = TOpts::Default();
    opts.AddLongOption('n', "name", "domain name").Required().RequiredArgument("STR").StoreResult(&TagName);
    opts.AddLongOption("config", "apply global config").RequiredArgument("STR").StoreResult(&configPb);
    opts.AddLongOption("config-file", "load global config from file").RequiredArgument("PATH").StoreResult(&configPbFile);

    ConfigureBaseLastGetopt(opts);
    TOptsParseResult res(&opts, argc, argv);
    ConfigureMsgBusLastGetopt(res, argc, argv);

#if 0
    if (!configPbFile.empty()) {
        GlobalConfig.Reset(new NKikimrTxScheme::TConfig);
        Y_ABORT_UNLESS(ParsePBFromFile(configPbFile, GlobalConfig.Get()));
    } else if (!configPb.empty()) {
        GlobalConfig.Reset(new NKikimrTxScheme::TConfig);
        Y_ABORT_UNLESS(::google::protobuf::TextFormat::ParseFromString(configPb, GlobalConfig.Get()));
    }
#else
    Cout << "config options for init-root are not used anymore (deprecated)" << Endl;
#endif
}

}
}
