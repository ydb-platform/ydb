#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_tools.h>
#include <ydb/core/blobstorage/pdisk/metadata/blobstorage_pdisk_metadata.h>
#include <ydb/core/util/random.h>
#include "cli.h"
#include "cli_cmds.h"
#include <ydb/core/protos/blobstorage_distributed_config.pb.h>
#include <google/protobuf/text_format.h>
#include <google/protobuf/util/json_util.h>
#include <ydb/core/blobstorage/nodewarden/distconf.h>

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
        config.Opts->AddLongOption('v', "verbose", "output detailed information for debugging").Optional()
            .StoreTrue(&IsVerbose);
        config.Opts->AddLongOption('l', "lock", "lock device before reading disk info").Optional()
            .StoreTrue(&LockDevice);
    }

    virtual void Parse(TConfig& config) override {
        TClientCommand::Parse(config);
        Path = config.ParseResult->GetFreeArgs()[0];
        // TODO: remove after master->main key migration
        bool hasMainOption = config.ParseResult->Has("main-key");
        bool hasMasterOption = config.ParseResult->Has("master-key");
        bool hasKOption = config.ParseResult->Has('k');
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
            .Optional().StoreTrue(&IsErasureEncode);

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
        SafeEntropyPoolRead(&ChunkKey, sizeof(NKikimr::NPDisk::TKey));
        SafeEntropyPoolRead(&LogKey, sizeof(NKikimr::NPDisk::TKey));
        SafeEntropyPoolRead(&SysLogKey, sizeof(NKikimr::NPDisk::TKey));
        bool hasMainOption = config.ParseResult->Has("main-key");
        bool hasMasterOption = config.ParseResult->Has("master-key");
        bool hasKOption = config.ParseResult->Has('k');
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
        SetFreeArgTitle(0, "<PATH>", "Disk device path");
    }

    virtual void Parse(TConfig& config) override {
        TClientCommand::Parse(config);
        Path = config.ParseResult->GetFreeArgs()[0];
    }

    virtual int Run(TConfig&) override {
        try {
            ObliterateDisk(Path);
        } catch (const yexception& e) {
            Cerr << "Error, what# " << e.what() << Endl;
            return 1;
        }
        return 0;
    }
};

class TClientCommandDiskMetadataRead : public TClientCommand {
public:
    TClientCommandDiskMetadataRead()
        : TClientCommand("read", {}, "Read PDisk metadata")
    {}

    TString Path;
    TVector<NPDisk::TKey> MainKeyTmp;
    NPDisk::TMainKey MainKey;
    TString CommittedYamlPath;
    TString ProposedYamlPath;
    TString PrevYamlPath;
    bool JsonOut = false;

    void Config(TConfig& config) override {
        TClientCommand::Config(config);
        config.SetFreeArgsNum(1);
        SetFreeArgTitle(0, "<PATH>", "PDisk device path");
        config.Opts->AddLongOption('k', "main-key", "Encryption main-key to use while reading metadata").RequiredArgument("NUM")
            .Optional().AppendTo(&MainKeyTmp);
        config.Opts->AddLongOption("committed-yaml", "Extract CommittedStorageConfig.ConfigComposite to file")
            .Optional().RequiredArgument("PATH").StoreResult(&CommittedYamlPath);
        config.Opts->AddLongOption("proposed-yaml", "Extract ProposedStorageConfig.ConfigComposite to file")
            .Optional().RequiredArgument("PATH").StoreResult(&ProposedYamlPath);
        config.Opts->AddLongOption("prev-yaml", "Extract CommittedStorageConfig.PrevConfig.ConfigComposite to file")
            .Optional().RequiredArgument("PATH").StoreResult(&PrevYamlPath);
        config.Opts->AddLongOption("json", "Print metadata as JSON to stdout")
            .Optional().StoreTrue(&JsonOut);
    }

    void Parse(TConfig& config) override {
        TClientCommand::Parse(config);
        Path = config.ParseResult->GetFreeArgs()[0];
        MainKey = {};
        for (auto& key : MainKeyTmp) {
            MainKey.Keys.push_back(key);
        }
        if (MainKey.Keys.empty()) {
            MainKey.Initialize();
        } else {
            MainKey.IsInitialized = true;
        }
    }

    int Run(TConfig& /*config*/) override {
        auto rec = ReadPDiskMetadata(Path, MainKey);
        if (rec.ByteSizeLong() == 0) {
            Cerr << "Failed to read PDisk metadata from: " << Path << Endl;
            return EXIT_FAILURE;
        }

        const bool requestedYaml = !CommittedYamlPath.empty() || !ProposedYamlPath.empty() || !PrevYamlPath.empty();

        auto decomposeToFile = [&](const TString& outPath, const char* label, auto getComposite, bool hasComposite) -> bool {
            if (outPath.empty()) {
                return true;
            }
            if (!hasComposite) {
                Cerr << label << " not found" << Endl;
                return false;
            }
            TString yaml;
            if (auto err = NStorage::DecomposeConfig(getComposite(), &yaml, nullptr, nullptr)) {
                Cerr << "Failed to extract " << label << " YAML: " << *err << Endl;
                return false;
            }
            try {
                TUnbufferedFileOutput out(outPath);
                out.Write(yaml.data(), yaml.size());
            } catch (const yexception& ex) {
                Cerr << "Failed to write YAML file '" << outPath << "': " << ex.what() << Endl;
                return false;
            }
            return true;
        };

        if (!decomposeToFile(CommittedYamlPath, "CommittedStorageConfig.ConfigComposite",
                [&]{ return rec.GetCommittedStorageConfig().GetConfigComposite(); },
                rec.HasCommittedStorageConfig() && rec.GetCommittedStorageConfig().HasConfigComposite())) {
            return EXIT_FAILURE;
        }
        if (!decomposeToFile(ProposedYamlPath, "ProposedStorageConfig.ConfigComposite",
                [&]{ return rec.GetProposedStorageConfig().GetConfigComposite(); },
                rec.HasProposedStorageConfig() && rec.GetProposedStorageConfig().HasConfigComposite())) {
            return EXIT_FAILURE;
        }
        if (!decomposeToFile(PrevYamlPath, "CommittedStorageConfig.PrevConfig.ConfigComposite",
                [&]{ return rec.GetCommittedStorageConfig().GetPrevConfig().GetConfigComposite(); },
                rec.HasCommittedStorageConfig() && rec.GetCommittedStorageConfig().HasPrevConfig() && rec.GetCommittedStorageConfig().GetPrevConfig().HasConfigComposite())) {
            return EXIT_FAILURE;
        }

        if (JsonOut) {
            using namespace google::protobuf::util;
            TString json;
            JsonPrintOptions opts;
            opts.preserve_proto_field_names = true;
            auto st = MessageToJsonString(rec, &json, opts);
            if (!st.ok()) {
                Cerr << "Could not serialize metadata to JSON: " << st.ToString() << Endl;
                return EXIT_FAILURE;
            }
            Cout << json << Endl;
            return EXIT_SUCCESS;
        }

        if (!requestedYaml) {
            TString text;
            if (!google::protobuf::TextFormat::PrintToString(rec, &text)) {
                Cerr << "Could not serialize metadata to text" << Endl;
                return EXIT_FAILURE;
            }
            Cout << text;
        }

        return EXIT_SUCCESS;
    }
};

class TClientCommandDiskMetadataWrite : public TClientCommand {
public:
    TClientCommandDiskMetadataWrite()
        : TClientCommand("write", {}, "Write PDisk metadata from proto or JSON")
    {}

    TString Path;
    TString ProtoFile;
    TString JsonFile;
    TString CommittedYamlPath;
    TString ProposedYamlPath;
    TString PrevYamlPath;
    TVector<NPDisk::TKey> MainKeyTmp;
    NPDisk::TMainKey MainKey;
    bool ClearProposed = false;
    bool ValidateCfg = false;
    bool AutoPrev = false;

    void Config(TConfig& config) override {
        TClientCommand::Config(config);
        config.SetFreeArgsNum(1);
        SetFreeArgTitle(0, "<PATH>", "PDisk device path");
        config.Opts->AddLongOption("from-proto", "Read TPDiskMetadataRecord from text-proto file")
            .Optional().RequiredArgument("PATH").StoreResult(&ProtoFile);
        config.Opts->AddLongOption("from-json", "Read TPDiskMetadataRecord from JSON file")
            .Optional().RequiredArgument("PATH").StoreResult(&JsonFile);
        config.Opts->AddLongOption("committed-yaml", "YAML file for CommittedStorageConfig.ConfigComposite")
            .Optional().RequiredArgument("PATH").StoreResult(&CommittedYamlPath);
        config.Opts->AddLongOption("proposed-yaml", "YAML file for ProposedStorageConfig.ConfigComposite")
            .Optional().RequiredArgument("PATH").StoreResult(&ProposedYamlPath);
        config.Opts->AddLongOption("clear-proposed", "Clear ProposedStorageConfig before writing")
            .Optional().StoreTrue(&ClearProposed);
        config.Opts->AddLongOption("validate-config", "Validate storage configs before writing")
            .Optional().StoreTrue(&ValidateCfg);
        config.Opts->AddLongOption("auto-prev", "Automatically save current CommittedStorageConfig to PrevConfig when applying new committed-yaml")
            .Optional().StoreTrue(&AutoPrev);
        config.Opts->AddLongOption('k', "main-key", "Encryption main-key to use for metadata operations").RequiredArgument("NUM")
            .Optional().AppendTo(&MainKeyTmp);
    }

    void Parse(TConfig& config) override {
        TClientCommand::Parse(config);
        Path = config.ParseResult->GetFreeArgs()[0];
        MainKey = {};
        for (auto& key : MainKeyTmp) {
            MainKey.Keys.push_back(key);
        }
        if (MainKey.Keys.empty()) {
            MainKey.Initialize();
        } else {
            MainKey.IsInitialized = true;
        }
    }

    int Run(TConfig& /*config*/) override {
        const bool hasProto = !ProtoFile.empty();
        const bool hasJson  = !JsonFile.empty();
        if (hasProto == hasJson) {
            Cerr << "Specify exactly one of --from-proto or --from-json" << Endl;
            return EXIT_FAILURE;
        }
        const TString& inputPath = hasProto ? ProtoFile : JsonFile;

        TString data;
        try {
            data = TUnbufferedFileInput(inputPath).ReadAll();
        } catch (const yexception& ex) {
            Cerr << "Failed to read input file '" << inputPath << "': " << ex.what() << Endl;
            return EXIT_FAILURE;
        }
        NKikimrBlobStorage::TPDiskMetadataRecord rec;
        bool parsed = false;
        if (hasProto) {
            parsed = google::protobuf::TextFormat::ParseFromString(data, &rec);
        } else {
            auto st = google::protobuf::util::JsonStringToMessage(std::string(data.data(), data.size()), &rec);
            parsed = st.ok();
            if (!st.ok()) {
                Cerr << "JSON parse error: " << st.ToString() << Endl;
            }
        }
        if (!parsed) {
            Cerr << "Failed to parse TPDiskMetadataRecord from '" << inputPath << "'" << Endl;
            return EXIT_FAILURE;
        }

        auto applyYaml = [&](const TString& path, const char* label, auto* cfg) -> bool {
            if (path.empty()) {
                return true;
            }
            TString yaml;
            try {
                yaml = TUnbufferedFileInput(path).ReadAll();
            } catch (const yexception& ex) {
                Cerr << "Failed to read " << label << " YAML file '" << path << "': " << ex.what() << Endl;
                return false;
            }
            if (auto err = NStorage::TDistributedConfigKeeper::UpdateConfigComposite(*cfg, yaml, TString(""))) {
                Cerr << "Failed to pack " << label << " YAML: " << *err << Endl;
                return false;
            }
            return true;
        };

        if (AutoPrev && !CommittedYamlPath.empty() && rec.HasCommittedStorageConfig()) {
            auto oldCommitted = rec.GetCommittedStorageConfig();
            *rec.MutableCommittedStorageConfig()->MutablePrevConfig() = oldCommitted;
        }

        if (!applyYaml(CommittedYamlPath, "committed", rec.MutableCommittedStorageConfig())) {
            return EXIT_FAILURE;
        }
        if (!applyYaml(ProposedYamlPath, "proposed", rec.MutableProposedStorageConfig())) {
            return EXIT_FAILURE;
        }

        if (rec.HasCommittedStorageConfig() && rec.GetCommittedStorageConfig().HasPrevConfig()) {
            rec.MutableCommittedStorageConfig()->MutablePrevConfig()->ClearPrevConfig();
        }

        if (ClearProposed) {
            rec.ClearProposedStorageConfig();
        }

        if (ValidateCfg) {
            if (rec.HasCommittedStorageConfig()) {
                if (auto err = NStorage::ValidateConfig(rec.GetCommittedStorageConfig())) {
                    Cerr << "CommittedStorageConfig validation failed: " << *err << Endl;
                    return EXIT_FAILURE;
                }
            }
            if (rec.HasProposedStorageConfig()) {
                if (auto err = NStorage::ValidateConfig(rec.GetProposedStorageConfig())) {
                    Cerr << "ProposedStorageConfig validation failed: " << *err << Endl;
                    return EXIT_FAILURE;
                }
            }
        }

        try {
            WritePDiskMetadata(Path, rec, MainKey);
            Cout << "OK" << Endl;
            return EXIT_SUCCESS;
        } catch (const yexception& ex) {
            Cerr << ex.what() << Endl;
            return EXIT_FAILURE;
        }
    }
};

class TClientCommandDiskMetadata : public TClientCommandTree {
public:
    TClientCommandDiskMetadata()
        : TClientCommandTree("metadata", {}, "PDisk metadata management")
    {
        AddCommand(std::make_unique<TClientCommandDiskMetadataRead>());
        AddCommand(std::make_unique<TClientCommandDiskMetadataWrite>());
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
        AddHiddenCommand(std::make_unique<TClientCommandDiskMetadata>());
    }
};

std::unique_ptr<TClientCommand> CreateClientCommandDisk() {
    return std::make_unique<TClientCommandDisk>();
}

}
}
