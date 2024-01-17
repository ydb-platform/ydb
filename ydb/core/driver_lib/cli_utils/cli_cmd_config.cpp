#include <ydb/core/driver_lib/cli_config_base/config_base.h>
#include "cli_cmd_config.h"
#include <ydb/public/lib/deprecated/client/msgbus_client.h>
#include <util/system/env.h>

namespace NKikimr {
namespace NDriverClient {

        void TCliCmdConfig::ConfigureBaseLastGetopt(NLastGetopt::TOpts &opts) {
            opts.AddHelpOption('h');

            opts.AddLongOption('s', "server", "server address to connect")
                    .RequiredArgument("ADDR").StoreResult(&Address);

            opts.SetFreeArgTitle(0, "mbus", "- use to override message bus client default settings");
            opts.SetFreeArgsMin(0);
            opts.ArgPermutation_ = NLastGetopt::REQUIRE_ORDER;
        }

        void TCliCmdConfig::ConfigureMsgBusLastGetopt(const NLastGetopt::TOptsParseResult& res, int argc, char** argv) {
            if (Address.empty()) {
                TString kikimrServer = GetEnv("KIKIMR_SERVER");
                if (kikimrServer != nullptr) {
                    Address = kikimrServer;
                }
            }
            TCommandConfig::TServerEndpoint endpoint = TCommandConfig::ParseServerAddress(Address);
            switch (endpoint.ServerType) {
            case TCommandConfig::EServerType::GRpc:
                ClientConfig = NYdbGrpc::TGRpcClientConfig(endpoint.Address);
                if (endpoint.EnableSsl.Defined()) {
                    auto *p = std::get_if<NYdbGrpc::TGRpcClientConfig>(&ClientConfig.GetRef());
                    p->EnableSsl = endpoint.EnableSsl.GetRef();
                }
                break;
            case TCommandConfig::EServerType::MessageBus:
                if (!endpoint.Address.empty()) {
                    NMsgBusProxy::TMsgBusClientConfig::CrackAddress(
                                endpoint.Address,
                                MsgBusClientConfig.Ip,
                                MsgBusClientConfig.Port);
                }
                SetMsgBusDefaults(MsgBusClientConfig.BusSessionConfig, MsgBusClientConfig.BusQueueConfig);
                if ((res.GetFreeArgCount() > 0) && (res.GetFreeArgs().at(0) == "mbus")) {
                    size_t freeArgsPos = res.GetFreeArgsPos();
                    argc -= freeArgsPos;
                    argv += freeArgsPos;
                    NLastGetopt::TOpts msgBusOpts = NLastGetopt::TOpts::Default();
                    msgBusOpts.AddHelpOption('h');
                    MsgBusClientConfig.ConfigureLastGetopt(msgBusOpts);
                    NLastGetopt::TOptsParseResult mbusRes(&msgBusOpts, argc, argv);
                    Y_UNUSED(mbusRes);
                }

                ClientConfig = MsgBusClientConfig;
                break;
            }
        }

} // namespace NDriverClient
} // namespace NKikimr
