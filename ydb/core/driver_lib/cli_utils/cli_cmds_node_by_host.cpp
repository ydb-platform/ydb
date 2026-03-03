#include "cli_cmds_standalone.h"

#include <ydb/core/driver_lib/base_utils/base_utils.h>
#include <ydb/core/protos/config.pb.h>

namespace NKikimr::NDriverClient {

class TCommandNodeByHost : public NYdb::NConsoleClient::TClientCommand {
    TString NamingFile;
    TString Hostname;
    i32 Port = -1;

public:
    TCommandNodeByHost()
        : TClientCommand("node-by-host", {}, "Get node id by hostname")
    {}

    void Config(TConfig& config) override {
        TClientCommand::Config(config);
        config.Opts->AddLongOption("naming-file", "Static nameservice config file")
            .RequiredArgument("PATH").Required().StoreResult(&NamingFile);
        config.Opts->AddLongOption('n', "hostname", "Hostname to detect node id for")
            .RequiredArgument("STR").Required().StoreResult(&Hostname);
        config.Opts->AddLongOption('p', "port", "Port to detect node id for")
            .OptionalArgument("NUM").Optional().StoreResult(&Port);
        config.SetFreeArgsMin(0);
    }

    int Run(TConfig& config) override {
        Y_UNUSED(config);

        Y_ABORT_UNLESS(Port == -1 || (Port >= 0 && Port <= 0xffff));

        TAutoPtr<NKikimrConfig::TStaticNameserviceConfig> nameserviceConfig;
        nameserviceConfig.Reset(new NKikimrConfig::TStaticNameserviceConfig());
        Y_ABORT_UNLESS(ParsePBFromFile(NamingFile, nameserviceConfig.Get()));

        size_t nodeSize = nameserviceConfig->NodeSize();
        ui32 nodeId = 0;
        bool isMatchingNodeFound = false;
        bool isPortPresent = (Port != -1);
        for (size_t nodeIdx = 0; nodeIdx < nodeSize; ++nodeIdx) {
            const NKikimrConfig::TStaticNameserviceConfig::TNode& node = nameserviceConfig->GetNode(nodeIdx);
            Y_ABORT_UNLESS(node.HasPort());
            Y_ABORT_UNLESS(node.HasHost());
            Y_ABORT_UNLESS(node.HasNodeId());
            if (node.GetHost() == Hostname) {
                if (!isPortPresent || (ui32)Port == node.GetPort()) {
                    Y_ABORT_UNLESS(!isMatchingNodeFound);
                    isMatchingNodeFound = true;
                    nodeId = node.GetNodeId();
                }
            }
        }
        Y_ABORT_UNLESS(isMatchingNodeFound);
        Cout << nodeId;
        Cout.Flush();

        return 0;
    }
};

std::unique_ptr<NYdb::NConsoleClient::TClientCommand> NewCommandNodeByHost() {
    return std::make_unique<TCommandNodeByHost>();
}

} // namespace NKikimr::NDriverClient
