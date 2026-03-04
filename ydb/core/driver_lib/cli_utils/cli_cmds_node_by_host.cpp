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

        if (Port != -1 && (Port < 0 || Port > 0xffff)) {
            throw NYdb::NConsoleClient::TMisuseException()
                << "Invalid port value: " << Port << ". Must be in range [0, 65535]";
        }

        TAutoPtr<NKikimrConfig::TStaticNameserviceConfig> nameserviceConfig;
        nameserviceConfig.Reset(new NKikimrConfig::TStaticNameserviceConfig());
        if (!ParsePBFromFile(NamingFile, nameserviceConfig.Get())) {
            throw NYdb::NConsoleClient::TMisuseException()
                << "Failed to parse naming file: " << NamingFile;
        }

        size_t nodeSize = nameserviceConfig->NodeSize();
        ui32 nodeId = 0;
        bool isMatchingNodeFound = false;
        bool isPortPresent = (Port != -1);
        for (size_t nodeIdx = 0; nodeIdx < nodeSize; ++nodeIdx) {
            const NKikimrConfig::TStaticNameserviceConfig::TNode& node = nameserviceConfig->GetNode(nodeIdx);
            auto checkField = [&](bool has, const char* name) {
                if (!has) {
                    throw NYdb::NConsoleClient::TMisuseException()
                        << "Node entry at index " << nodeIdx << " in " << NamingFile
                        << " is missing required field: " << name;
                }
            };
            checkField(node.HasPort(), "Port");
            checkField(node.HasHost(), "Host");
            checkField(node.HasNodeId(), "NodeId");
            if (node.GetHost() == Hostname) {
                if (!isPortPresent || (ui32)Port == node.GetPort()) {
                    if (isMatchingNodeFound) {
                        throw NYdb::NConsoleClient::TMisuseException()
                            << "Multiple nodes match hostname '" << Hostname << "'"
                            << (isPortPresent ? TString(" and port ") + ToString(Port) : TString())
                            << " in " << NamingFile;
                    }
                    isMatchingNodeFound = true;
                    nodeId = node.GetNodeId();
                }
            }
        }
        if (!isMatchingNodeFound) {
            throw NYdb::NConsoleClient::TMisuseException()
                << "No node found for hostname '" << Hostname << "'"
                << (isPortPresent ? TString(" with port ") + ToString(Port) : TString())
                << " in " << NamingFile;
        }
        Cout << nodeId;
        Cout.Flush();

        return 0;
    }
};

std::unique_ptr<NYdb::NConsoleClient::TClientCommand> NewCommandNodeByHost() {
    return std::make_unique<TCommandNodeByHost>();
}

} // namespace NKikimr::NDriverClient
