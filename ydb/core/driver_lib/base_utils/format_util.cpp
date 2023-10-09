#include "format_util.h"
#include "base_utils.h"

namespace NKikimr {


int MainFormatUtil(const TCommandConfig &cmdConf, int argc, char** argv) {
    Y_UNUSED(cmdConf);

    TCmdFormatUtilConfig config;
    config.Parse(argc, argv);

    Y_ABORT_UNLESS(config.NodeId != 0);

    TAutoPtr<NKikimrConfig::TBlobStorageFormatConfig> formatConfig;
    formatConfig.Reset(new NKikimrConfig::TBlobStorageFormatConfig());
    Y_ABORT_UNLESS(ParsePBFromFile(config.FormatFile, formatConfig.Get()));

    size_t driveSize = formatConfig->DriveSize();
    bool isFirst = true;
    for (size_t driveIdx = 0; driveIdx < driveSize; ++driveIdx) {
        const NKikimrConfig::TBlobStorageFormatConfig::TDrive& drive = formatConfig->GetDrive(driveIdx);
        Y_ABORT_UNLESS(drive.HasNodeId());
        Y_ABORT_UNLESS(drive.HasType());
        Y_ABORT_UNLESS(drive.HasPath());
        Y_ABORT_UNLESS(drive.HasGuid());
        Y_ABORT_UNLESS(drive.HasPDiskId());
        if (drive.GetNodeId() == config.NodeId) {
            if (isFirst) {
                isFirst = false;
            } else {
                Cout << Endl;
            }
            Cout << drive.GetType() << "," << drive.GetPath() << "," << drive.GetGuid() << "," << drive.GetPDiskId();
        }
    }
    Cout.Flush();
    return 0;
}

TCmdFormatUtilConfig::TCmdFormatUtilConfig()
    : NodeId(0)
{}

void TCmdFormatUtilConfig::Parse(int argc, char **argv) {
    using namespace NLastGetopt;
    TOpts opts = TOpts::Default();

    opts.AddLongOption("format-file", "Blob storage fromat config file").RequiredArgument("PATH").Required()
        .StoreResult(&FormatFile);
    opts.AddLongOption('n', "node-id", "Node ID to get drive info for").RequiredArgument("NUM").Required()
        .StoreResult(&NodeId);

    opts.AddHelpOption('h');

    TOptsParseResult res(&opts, argc, argv);
}

}
