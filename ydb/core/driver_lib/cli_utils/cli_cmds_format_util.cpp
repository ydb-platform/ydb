#include "cli_cmds_standalone.h"

#include <ydb/core/driver_lib/base_utils/base_utils.h>
#include <ydb/core/protos/config.pb.h>

namespace NKikimr::NDriverClient {

class TCommandFormatUtil : public NYdb::NConsoleClient::TClientCommand {
    TString FormatFile;
    ui32 NodeId = 0;

public:
    TCommandFormatUtil()
        : TClientCommand("format-util", {}, "Query blob storage format configuration file")
    {}

    void Config(TConfig& config) override {
        TClientCommand::Config(config);
        config.Opts->AddLongOption("format-file", "Blob storage format config file")
            .RequiredArgument("PATH").Required().StoreResult(&FormatFile);
        config.Opts->AddLongOption('n', "node-id", "Node ID to get drive info for")
            .RequiredArgument("NUM").Required().StoreResult(&NodeId);
        config.SetFreeArgsMin(0);
    }

    int Run(TConfig& config) override {
        Y_UNUSED(config);

        Y_ABORT_UNLESS(NodeId != 0);

        TAutoPtr<NKikimrConfig::TBlobStorageFormatConfig> formatConfig;
        formatConfig.Reset(new NKikimrConfig::TBlobStorageFormatConfig());
        Y_ABORT_UNLESS(ParsePBFromFile(FormatFile, formatConfig.Get()));

        size_t driveSize = formatConfig->DriveSize();
        bool isFirst = true;
        for (size_t driveIdx = 0; driveIdx < driveSize; ++driveIdx) {
            const NKikimrConfig::TBlobStorageFormatConfig::TDrive& drive = formatConfig->GetDrive(driveIdx);
            Y_ABORT_UNLESS(drive.HasNodeId());
            Y_ABORT_UNLESS(drive.HasType());
            Y_ABORT_UNLESS(drive.HasPath());
            Y_ABORT_UNLESS(drive.HasGuid());
            Y_ABORT_UNLESS(drive.HasPDiskId());
            if (drive.GetNodeId() == NodeId) {
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
};

std::unique_ptr<NYdb::NConsoleClient::TClientCommand> NewCommandFormatUtil() {
    return std::make_unique<TCommandFormatUtil>();
}

} // namespace NKikimr::NDriverClient
