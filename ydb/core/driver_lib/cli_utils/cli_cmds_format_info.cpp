#include "cli_cmds_standalone.h"

#include <ydb/core/driver_lib/base_utils/base_utils.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_tools.h>

namespace NKikimr::NDriverClient {

class TCommandFormatInfo : public NYdb::NConsoleClient::TClientCommand {
    TString Path;
    TVector<NPDisk::TKey> MainKeyTmp;
    NPDisk::TMainKey MainKey;
    bool IsVerbose = false;

public:
    TCommandFormatInfo()
        : TClientCommand("format-info", {}, "Read pdisk format info")
    {}

    void Config(TConfig& config) override {
        TClientCommand::Config(config);
        config.Opts->AddLongOption('p', "pdisk-path", "Path to pdisk to read format info")
            .RequiredArgument("PATH").Required().StoreResult(&Path);
        config.Opts->AddLongOption('k', "main-key", "Encryption main-key to use while reading")
            .RequiredArgument("NUM").Optional().AppendTo(&MainKeyTmp);
        config.Opts->AddLongOption("master-key", "Obsolete: use main-key")
            .RequiredArgument("NUM").Optional().AppendTo(&MainKeyTmp);
        config.Opts->AddLongOption('v', "verbose", "Output detailed information for debugging")
            .Optional().NoArgument().StoreTrue(&IsVerbose);
        config.SetFreeArgsMin(0);
    }

    void Parse(TConfig& config) override {
        TClientCommand::Parse(config);

        const auto& parseResult = config.ParseResult->GetCommandLineParseResult();
        bool hasMainOption = parseResult.FindLongOptParseResult("main-key");
        bool hasMasterOption = parseResult.FindLongOptParseResult("master-key");
        bool hasKOption = parseResult.FindCharOptParseResult('k');
        if (!hasMainOption && !hasMasterOption && !hasKOption) {
            throw NYdb::NConsoleClient::TMisuseException() << "Missing required key option: --main-key (-k) or --master-key";
        }

        MainKey = {};
        for (auto& key : MainKeyTmp) {
            MainKey.Keys.push_back(key);
        }
    }

    int Run(TConfig& config) override {
        Y_UNUSED(config);

        TPDiskInfo info;
        bool isOk = ReadPDiskFormatInfo(Path, MainKey, info);
        if (isOk) {
            Cout << "Version: " << info.Version << Endl;
            Cout << "DiskSize: " << info.DiskSize << Endl;
            Cout << "SectorSizeBytes: " << info.SectorSizeBytes << Endl;
            Cout << "UserAccessibleChunkSizeBytes: " << info.UserAccessibleChunkSizeBytes << Endl;
            Cout << "RawChunkSizeBytes: " << info.RawChunkSizeBytes << Endl;
            Cout << "Guid: " << info.DiskGuid << Endl;
            Cout << "TextMessage: " << info.TextMessage << Endl;
            if (IsVerbose) {
                Cout << Endl;
                Cout << "SysLogSectorCount: " << info.SysLogSectorCount << Endl;
                Cout << "SystemChunkCount: " << info.SystemChunkCount << Endl;
                Cout << Endl;
                Cout << "sysLogSectorInfo: " << Endl;
                ui32 sectors = info.SectorInfo.size();
                ui32 lines = (sectors + 5) / 6;
                ui32 nonceReversalCount = 0;
                for (ui32 line = 0; line < lines; ++line) {
                    for (ui32 column = 0; column < 6; ++column) {
                        ui32 idx = line + column * lines;
                        if (idx < sectors) {
                            bool isSeq = (info.SectorInfo[idx].Nonce >= info.SectorInfo[(sectors + idx - 1) % sectors].Nonce);
                            if (!isSeq) {
                                nonceReversalCount++;
                            }
                            Cout << Sprintf("[%03" PRIu32 "v%" PRIu64 " n%010" PRIu64 "%s%s]", idx,
                                    info.SectorInfo[idx].Version, info.SectorInfo[idx].Nonce,
                                    (isSeq ? "  " : "N-"),
                                    (info.SectorInfo[idx].IsCrcOk ? "  " : "C-"));
                        }
                    }
                    Cout << Endl;
                }
                Cout << Endl;
                Cout << "nonceReversalCount: " << nonceReversalCount << Endl;
            }
        } else {
            Cout << "Error. Can't read PDisk format info. Reason# " << info.ErrorReason << Endl;
        }
        return 0;
    }
};

std::unique_ptr<NYdb::NConsoleClient::TClientCommand> NewCommandFormatInfo() {
    return std::make_unique<TCommandFormatInfo>();
}

} // namespace NKikimr::NDriverClient
