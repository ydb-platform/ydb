#include "format_info.h"
#include "base_utils.h"

#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_tools.h>

#include <util/random/entropy.h>

namespace NKikimr {

int MainFormatInfo(const TCommandConfig &cmdConf, int argc, char** argv) {
    Y_UNUSED(cmdConf);

    TCmdFormatInfoConfig formatInfoConfig;
    formatInfoConfig.Parse(argc, argv);

    TPDiskInfo info;
    bool isOk = ReadPDiskFormatInfo(formatInfoConfig.Path, formatInfoConfig.MainKey, info);
    if (isOk) {
        Cout << "Version: " << info.Version << Endl;
        Cout << "DiskSize: " << info.DiskSize << Endl;
        Cout << "SectorSizeBytes: " << info.SectorSizeBytes << Endl;
        Cout << "UserAccessibleChunkSizeBytes: " << info.UserAccessibleChunkSizeBytes << Endl;
        Cout << "RawChunkSizeBytes: " << info.RawChunkSizeBytes << Endl;
        Cout << "Guid: " << info.DiskGuid << Endl;
        Cout << "TextMessage: " << info.TextMessage << Endl;
        if (formatInfoConfig.IsVerbose) {
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

TCmdFormatInfoConfig::TCmdFormatInfoConfig()
    : MainKey({})
    , IsVerbose(false)
{}

void TCmdFormatInfoConfig::Parse(int argc, char **argv) {
    using namespace NLastGetopt;
    TOpts opts = TOpts::Default();

    opts.AddLongOption('p', "pdisk-path", "path to pdisk to read format info").RequiredArgument("PATH").Required()
        .StoreResult(&Path);
    opts.AddLongOption('k', "main-key", "encryption main-key to use while reading").RequiredArgument("NUM")
        .Optional().AppendTo(&MainKeyTmp); // TODO: make required
    opts.AddLongOption("master-key", "obsolete: use main-key").RequiredArgument("NUM")
        .Optional().AppendTo(&MainKeyTmp); // TODO: remove after migration
    opts.AddLongOption('v', "verbose", "output detailed information for debugging").Optional().NoArgument()
        .SetFlag(&IsVerbose);

    opts.AddHelpOption('h');

    TOptsParseResult res(&opts, argc, argv);

    // TODO: remove after master->main key migration
    bool hasMainOption = res.FindLongOptParseResult("main-key");
    bool hasMasterOption = res.FindLongOptParseResult("master-key");
    bool hasKOption = res.FindCharOptParseResult('k');
    if (!hasMainOption && !hasMasterOption && !hasKOption)
        ythrow yexception() << "missing main-key param";

    MainKey = {};
    for (auto& key : MainKeyTmp) {
        MainKey.Keys.push_back(key);
    }
}

}
