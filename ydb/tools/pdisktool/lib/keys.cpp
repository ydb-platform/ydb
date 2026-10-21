#include "keys.h"

#include <algorithm>

#include <ydb/core/blobstorage/crypto/crypto.h>
#include <ydb/core/blobstorage/crypto/default.h>
#include <ydb/core/blobstorage/crypto/secured_block.h>
#include <ydb/core/protos/key.pb.h>

#include <google/protobuf/text_format.h>

#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/string/cast.h>
#include <util/string/strip.h>
#include <util/system/file.h>

namespace NKikimr::NPDiskTool {

ui64 ParseMainKeyArg(TStringBuf value) {
    const TStringBuf trimmed = StripString(value);
    if (trimmed.empty()) {
        ythrow yexception() << "empty --main-key value";
    }
    if (trimmed == "YdbDefaultPDiskSequence" || trimmed == "default") {
        return NPDisk::YdbDefaultPDiskSequence;
    }
    ui64 parsed = 0;
    if (trimmed.StartsWith("0x") || trimmed.StartsWith("0X")) {
        if (!TryIntFromString<16>(trimmed.substr(2), parsed)) {
            ythrow yexception() << "cannot parse hex --main-key value " << TString(trimmed).Quote();
        }
        return parsed;
    }
    if (!TryFromString<ui64>(trimmed, parsed)) {
        ythrow yexception() << "cannot parse --main-key value " << TString(trimmed).Quote()
            << " (use decimal, 0x-hex, or YdbDefaultPDiskSequence)";
    }
    return parsed;
}

// Same derivation Node Warden uses for PDisk TMainKey::Keys[i] = first ui64 of ObtainKey().
// ObtainKey calls SetKey(pin) but never Clear() afterwards, so Poly1305 still uses the dummy
// constructor key: the Pin does not currently affect the ui64 installed as the PDisk main key.
static bool HashKeyContainer(const TString& path, const TString& pinIn, ui64& outKey, TIssueLog& issues) {
    TFileHandle file(path, OpenExisting | RdOnly);
    if (!file.IsOpen()) {
        issues.Error("key-file", TStringBuilder() << "Cannot open key container " << path.Quote());
        return false;
    }
    const ui64 length = file.GetLength();
    if (length == 0) {
        issues.Error("key-file", TStringBuilder() << "Empty key container " << path.Quote());
        return false;
    }
    TString data = TString::Uninitialized(length);
    const size_t bytesRead = file.Read(data.Detach(), length);
    if (bytesRead != length) {
        issues.Error("key-file", TStringBuilder() << "Short read of key container " << path.Quote());
        return false;
    }

    TString pin = pinIn;
    if (pin.empty()) {
        pin = "EmptyPin";
    }

    NKikimr::THashCalculator hasher;
    hasher.SetKey(reinterpret_cast<const ui8*>(pin.data()), pin.size());
    hasher.Hash(data.Detach(), data.size());
    ui64 hash2 = 0;
    outKey = hasher.GetHashResult(&hash2);
    SecureWipeBuffer(reinterpret_cast<ui8*>(data.Detach()), data.size());
    return true;
}

static bool LooksLikeKeyConfig(const TString& content) {
    return content.Contains("ContainerPath")
        || content.Contains("container_path")
        || content.Contains("PDiskKeyConfig")
        || content.Contains("pdisk_key_config")
        || content.Contains("Keys {")
        || content.Contains("Keys{");
}

static bool ParseKeyConfig(const TString& content, NKikimrProto::TKeyConfig& cfg) {
    cfg.Clear();
    if (google::protobuf::TextFormat::ParseFromString(content, &cfg) && cfg.KeysSize() > 0) {
        return true;
    }
    cfg.Clear();
    if (cfg.ParseFromString(content) && cfg.KeysSize() > 0) {
        return true;
    }
    return false;
}

static bool LoadKeysFromConfig(
    const NKikimrProto::TKeyConfig& keyConfig,
    const TString& pinOverride,
    TMainKey& mainKey,
    TIssueLog& issues)
{
    struct TParsed {
        ui64 Version = 0;
        ui64 Key = 0;
        TString Id;
    };
    TVector<TParsed> parsed;
    parsed.reserve(keyConfig.KeysSize());

    for (ui32 i = 0; i < keyConfig.KeysSize(); ++i) {
        const auto& record = keyConfig.GetKeys(i);
        TParsed item;
        item.Version = record.GetVersion();
        item.Id = record.GetId();

        if (record.GetId() == "0" && record.GetContainerPath().empty()) {
            item.Key = NPDisk::YdbDefaultPDiskSequence;
            issues.Info("key-file", "TKeyRecord Id=0 with empty ContainerPath; using YdbDefaultPDiskSequence");
        } else {
            if (record.GetContainerPath().empty()) {
                issues.Error("key-file", TStringBuilder()
                    << "TKeyRecord Id=" << record.GetId().Quote()
                    << " has empty ContainerPath");
                return false;
            }
            const TString pin = pinOverride ? pinOverride : record.GetPin();
            if (!HashKeyContainer(record.GetContainerPath(), pin, item.Key, issues)) {
                return false;
            }
            issues.Info("key-file", TStringBuilder()
                << "Loaded PDisk main key from container " << record.GetContainerPath().Quote()
                << " Id=" << record.GetId().Quote()
                << " Version=" << record.GetVersion());
        }
        parsed.push_back(item);
    }

    std::sort(parsed.begin(), parsed.end(), [](const TParsed& a, const TParsed& b) {
        return a.Version < b.Version;
    });
    for (const auto& item : parsed) {
        mainKey.Keys.push_back(item.Key);
    }
    return !parsed.empty();
}

TMainKey MakeMainKey(
    const TVector<ui64>& numericKeys,
    const TString& keyFile,
    const TString& pin,
    bool keySpecified,
    TIssueLog& issues)
{
    TMainKey mainKey;
    for (ui64 k : numericKeys) {
        mainKey.Keys.push_back(k);
    }
    if (keyFile) {
        TString content;
        try {
            content = TFileInput(keyFile).ReadAll();
        } catch (const yexception& e) {
            issues.Error("key-file", TStringBuilder() << "Cannot read " << keyFile.Quote() << ": " << e.what());
        }

        if (content) {
            NKikimrProto::TKeyConfig cfg;
            if (LooksLikeKeyConfig(content)) {
                if (!ParseKeyConfig(content, cfg)) {
                    issues.Error("key-file", TStringBuilder()
                        << keyFile.Quote()
                        << " looks like a PDisk key config protobuf, but it did not parse as NKikimrProto.TKeyConfig. "
                        << "Pass the same text proto ydbd uses for --pdisk-key-file "
                        << "(Keys { ContainerPath: \"...\" Id: \"...\" Version: 1 }), "
                        << "not a YAML config and not the raw container unless the file is the container itself.");
                    mainKey.IsInitialized = true;
                    return mainKey;
                } else if (!LoadKeysFromConfig(cfg, pin, mainKey, issues)) {
                    mainKey.IsInitialized = true;
                    return mainKey;
                }
            } else {
                cfg.Clear();
                if (cfg.ParseFromString(content) && cfg.KeysSize() > 0) {
                    if (!LoadKeysFromConfig(cfg, pin, mainKey, issues)) {
                        mainKey.IsInitialized = true;
                        return mainKey;
                    }
                } else {
                    ui64 hashed = 0;
                    if (HashKeyContainer(keyFile, pin, hashed, issues)) {
                        mainKey.Keys.push_back(hashed);
                        issues.Info("key-file", TStringBuilder()
                            << "Hashed " << keyFile.Quote()
                            << " as a Node Warden key container (not a TKeyConfig proto)");
                    }
                }
            }
        }
    }
    if (mainKey.Keys.empty()) {
        if (issues.HasErrors()) {
            mainKey.IsInitialized = true;
            return mainKey;
        }
        mainKey.Keys.push_back(NPDisk::YdbDefaultPDiskSequence);
        if (!keySpecified) {
            issues.Warning("main-key",
                "No --main-key / --key-file given; using the built-in default PDisk sequence "
                "(0x7e5700007e570000 / YdbDefaultPDiskSequence)");
        }
    }
    mainKey.IsInitialized = true;
    return mainKey;
}

} // namespace NKikimr::NPDiskTool
