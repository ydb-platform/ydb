#include "cli_cmds_standalone.h"

#include <ydb/core/protos/config.pb.h>
#include <ydb/core/util/pb.h>

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

        if (NodeId == 0) {
            throw NYdb::NConsoleClient::TMisuseException()
                << "Node ID must be non-zero";
        }

        TAutoPtr<NKikimrConfig::TBlobStorageFormatConfig> formatConfig;
        formatConfig.Reset(new NKikimrConfig::TBlobStorageFormatConfig());
        if (!ParsePBFromFile(FormatFile, formatConfig.Get())) {
            throw NYdb::NConsoleClient::TMisuseException()
                << "Failed to parse format file: " << FormatFile;
        }

        size_t driveSize = formatConfig->DriveSize();
        bool isFirst = true;
        for (size_t driveIdx = 0; driveIdx < driveSize; ++driveIdx) {
            const NKikimrConfig::TBlobStorageFormatConfig::TDrive& drive = formatConfig->GetDrive(driveIdx);
            auto checkField = [&](bool has, const char* name) {
                if (!has) {
                    throw NYdb::NConsoleClient::TMisuseException()
                        << "Drive entry at index " << driveIdx << " in " << FormatFile
                        << " is missing required field: " << name;
                }
            };
            checkField(drive.HasNodeId(), "NodeId");
            checkField(drive.HasType(), "Type");
            checkField(drive.HasPath(), "Path");
            checkField(drive.HasGuid(), "Guid");
            checkField(drive.HasPDiskId(), "PDiskId");
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
