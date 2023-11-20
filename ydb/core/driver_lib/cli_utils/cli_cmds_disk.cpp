#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_tools.h>
#include <util/random/entropy.h>
#include "cli.h"
#include "cli_cmds.h"

namespace NKikimr {
namespace NDriverClient {

class TClientCommandDiskInfo : public TClientCommand {
public:
    TClientCommandDiskInfo()
        : TClientCommand("info", {}, "Get info about disk")
    {}

    bool IsVerbose;
    bool LockDevice;
    TVector<NPDisk::TKey> MainKeyTmp; // required for .AppendTo()
    NPDisk::TMainKey MainKey;
    TString Path;

    virtual void Config(TConfig& config) override {
        TClientCommand::Config(config);
        IsVerbose = false;
        LockDevice = false;
        MainKeyTmp = {};
        config.SetFreeArgsNum(1);
        SetFreeArgTitle(0, "<PATH>", "Disk path");
        config.Opts->AddLongOption('k', "main-key", "encryption main-key to use while reading").RequiredArgument("NUM")
            .Optional().AppendTo(&MainKeyTmp); // TODO: make required
        config.Opts->AddLongOption("master-key", "obsolete: use main-key").RequiredArgument("NUM")
            .Optional().AppendTo(&MainKeyTmp); // TODO: remove after migration
        config.Opts->AddLongOption('v', "verbose", "output detailed information for debugging").Optional().NoArgument()
            .SetFlag(&IsVerbose);
        config.Opts->AddLongOption('l', "lock", "lock device before reading disk info").Optional().NoArgument()
            .SetFlag(&LockDevice);
    }

    virtual void Parse(TConfig& config) override {
        TClientCommand::Parse(config);
        Path = config.ParseResult->GetFreeArgs()[0];
        // TODO: remove after master->main key migration
        bool hasMainOption = config.ParseResult->FindLongOptParseResult("main-key");
        bool hasMasterOption = config.ParseResult->FindLongOptParseResult("master-key");
        bool hasKOption = config.ParseResult->FindCharOptParseResult('k');
        if (!hasMainOption && !hasMasterOption && !hasKOption)
            ythrow yexception() << "missing main-key param";

        MainKey = {};
        for (auto& key : MainKeyTmp) {
            MainKey.Keys.push_back(key);
        }
    }

    virtual int Run(TConfig&) override {
        TPDiskInfo info;
        bool isOk = ReadPDiskFormatInfo(Path, MainKey, info, LockDevice);
        if (isOk) {
            Cout << "Version: " << info.Version << Endl;
            Cout << "DiskSize: " << info.DiskSize << Endl;
            Cout << "SectorSizeBytes: " << info.SectorSizeBytes << Endl;
            Cout << "UserAccessibleChunkSizeBytes: " << info.UserAccessibleChunkSizeBytes << Endl;
            Cout << "RawChunkSizeBytes: " << info.RawChunkSizeBytes << Endl;
            Cout << "Guid: " << info.DiskGuid << Endl;
            Cout << "Timestamp: " << info.Timestamp.ToString() << Endl;
            Cout << "FormatFlags: " << info.FormatFlags << Endl;
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
            return 0;
        } else {
            Cerr << "Could not read disk format info"
                << " Path = " << Path
                << " ErrorReason = " << info.ErrorReason
                << Endl;
            return 1;
        }
    }
};

class TClientCommandDiskFormat : public TClientCommand {
public:
    TClientCommandDiskFormat()
        : TClientCommand("format", {}, "Format local disk")
    {}

    TString Path;
    NSize::TSize DiskSize;
    NSize::TSize ChunkSize;
    NSize::TSize SectorSize;
    ui64 Guid;
    TVector<NPDisk::TKey> MainKeyTmp;
    TString TextMessage;
    bool IsErasureEncode;

    virtual void Config(TConfig& config) override {
        TClientCommand::Config(config);
        MainKey = {};
        DiskSize = 0;
        ChunkSize = 128 << 20;
        SectorSize = 4 << 10;
        Guid = 0;
        IsErasureEncode = false;
        config.SetFreeArgsNum(1);
        SetFreeArgTitle(0, "<PATH>", "Disk path");
        config.Opts->AddLongOption('d', "disk-size", "disk size to set (supports K/M/G/T suffixes, 0 to autodetect, default = 0)\n"
            "kikimr needs disk of at least 16 GiB, disk must be large enough to contain at least 100 chunks")
            .OptionalArgument("BYTES").StoreResult(&DiskSize);
        config.Opts->AddLongOption('c', "chunk-size", "chunk size to set (supports K/M/G/T suffixes, default = 128M)\n"
            "kikimr needs chunks of at least 32 MiB, but was designed to work with 128 MiB chunks in mind.\n"
            "It is not recommended to format disks with more than 64000 chunks.")
            .OptionalArgument("BYTES").StoreResult(&ChunkSize);
        config.Opts->AddLongOption('s', "sector-size", "sector size to set (suppords K/M/G/T suffixes, default = 4k)\n"
            "you must specify here the actual sector size of the physical device!")
            .OptionalArgument("BYTES").StoreResult(&SectorSize);
        config.Opts->AddLongOption('g', "guid", "guid to set while formatting").RequiredArgument("NUM").Required()
            .StoreResult(&Guid);
        config.Opts->AddLongOption('k', "main-key", "encryption main-key to set while formatting.\n"
            "Make sure you use the same master key when you format your pdisks and when you run kikimr.")
            .RequiredArgument("NUM").Optional().AppendTo(&MainKeyTmp);
        config.Opts->AddLongOption("master-key", "obsolete: user main-key")
            .RequiredArgument("NUM").Optional().AppendTo(&MainKeyTmp);
        config.Opts->AddLongOption('t', "text-message", "text message to store in format sector (up to 4000 characters long)")
            .OptionalArgument("STR").Optional().StoreResult(&TextMessage);
        config.Opts->AddLongOption('e', "erasure-encode", "erasure-encode data to recover from single-sector failures")
            .Optional().NoArgument().SetFlag(&IsErasureEncode);

        config.Opts->SetCmdLineDescr("\n\n"
            "Kikimr was designed to work with large block-devices, like 4 TiB HDDs and 1 TiB SSDs\n"
            "It will work with files, but don't expect good performance.\n"
            "If you still want to run kikimr with a tiny file, specify --disk-size 17179869184 --chunk-size 33554432");
    }

    NPDisk::TKey ChunkKey;
    NPDisk::TKey LogKey;
    NPDisk::TKey SysLogKey;
    NPDisk::TMainKey MainKey;

    virtual void Parse(TConfig& config) override {
        TClientCommand::Parse(config);
        Path = config.ParseResult->GetFreeArgs()[0];
        EntropyPool().Read(&ChunkKey, sizeof(NKikimr::NPDisk::TKey));
        EntropyPool().Read(&LogKey, sizeof(NKikimr::NPDisk::TKey));
        EntropyPool().Read(&SysLogKey, sizeof(NKikimr::NPDisk::TKey));
        bool hasMainOption = config.ParseResult->FindLongOptParseResult("main-key");
        bool hasMasterOption = config.ParseResult->FindLongOptParseResult("master-key");
        bool hasKOption = config.ParseResult->FindCharOptParseResult('k');
        if (!hasMainOption && !hasMasterOption && !hasKOption)
            ythrow yexception() << "missing main-key param";

        for (auto& key : MainKeyTmp) {
            MainKey.Keys.push_back(key);
        }
    }

    virtual int Run(TConfig&) override {
        FormatPDisk(Path, DiskSize, SectorSize, ChunkSize, Guid, ChunkKey, LogKey, SysLogKey, MainKey.Keys.back(), TextMessage,
                IsErasureEncode, false, nullptr, true);
        return 0;
    }
};

class TClientCommandDiskObliterate : public TClientCommand {
public:
    TClientCommandDiskObliterate()
        : TClientCommand("obliterate", {}, "Obliterate local disk, so it will be self-formatted on startup")
    {}

    TString Path;

    virtual void Config(TConfig& config) override {
        TClientCommand::Config(config);
        config.SetFreeArgsNum(1);
        SetFreeArgTitle(0, "<PATH>", "Disk path");
    }

    virtual void Parse(TConfig& config) override {
        TClientCommand::Parse(config);
        Path = config.ParseResult->GetFreeArgs()[0];
    }

    virtual int Run(TConfig&) override {
        try {
            ObliterateDisk(Path);
        } catch (TFileError& e) {
            Cerr << "Error, what# " << e.what() << Endl;
            return 1;
        }
        return 0;
    }
};

class TClientCommandDisk : public TClientCommandTree {
public:
    TClientCommandDisk()
        : TClientCommandTree("disk", {}, "Disk management")
    {
        AddCommand(std::make_unique<TClientCommandDiskInfo>());
        AddCommand(std::make_unique<TClientCommandDiskFormat>());
        AddCommand(std::make_unique<TClientCommandDiskObliterate>());
    }
};

std::unique_ptr<TClientCommand> CreateClientCommandDisk() {
    return std::make_unique<TClientCommandDisk>();
}

}
}
