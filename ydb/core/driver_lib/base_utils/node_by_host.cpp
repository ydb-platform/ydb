#include "node_by_host.h"
#include "base_utils.h"

namespace NKikimr {


int MainNodeByHost(const TCommandConfig &cmdConf, int argc, char** argv) {
    Y_UNUSED(cmdConf);

    TCmdNodeByHostConfig nodeByHostConfig;
    nodeByHostConfig.Parse(argc, argv);

    Y_ABORT_UNLESS(nodeByHostConfig.Port == -1 || (nodeByHostConfig.Port >= 0 && nodeByHostConfig.Port <= 0xffff));

    TAutoPtr<NKikimrConfig::TStaticNameserviceConfig> nameserviceConfig;
    nameserviceConfig.Reset(new NKikimrConfig::TStaticNameserviceConfig());
    Y_ABORT_UNLESS(ParsePBFromFile(nodeByHostConfig.NamingFile, nameserviceConfig.Get()));

    size_t nodeSize = nameserviceConfig->NodeSize();
    ui32 nodeId = 0;
    bool isMatchingNodeFound = false;
    bool isPortPresent = (nodeByHostConfig.Port != -1);
    for (size_t nodeIdx = 0; nodeIdx < nodeSize; ++nodeIdx) {
        const NKikimrConfig::TStaticNameserviceConfig::TNode& node = nameserviceConfig->GetNode(nodeIdx);
        Y_ABORT_UNLESS(node.HasPort());
        Y_ABORT_UNLESS(node.HasHost());
        Y_ABORT_UNLESS(node.HasNodeId());
        if (node.GetHost() == nodeByHostConfig.Hostname) {
            if (!isPortPresent || (ui32)nodeByHostConfig.Port == node.GetPort()) {
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

TCmdNodeByHostConfig::TCmdNodeByHostConfig()
    : Port(-1)
{}

void TCmdNodeByHostConfig::Parse(int argc, char **argv) {
    using namespace NLastGetopt;
    TOpts opts = TOpts::Default();

    opts.AddLongOption("naming-file", "static nameservice config file").RequiredArgument("PATH").Required()
        .StoreResult(&NamingFile);
    opts.AddLongOption('n', "hostname", "Hostname to detect node id for").RequiredArgument("STR").Required()
        .StoreResult(&Hostname);
    opts.AddLongOption('p', "port", "Port to detect node id for").OptionalArgument("NUM").Optional()
        .StoreResult(&Port);

    opts.AddHelpOption('h');

    TOptsParseResult res(&opts, argc, argv);
}

}
