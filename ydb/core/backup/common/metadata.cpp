#include "metadata.h"

#include <library/cpp/json/json_writer.h>
#include <library/cpp/json/json_reader.h>

namespace NKikimr::NBackup {

void TMetadata::AddFullBackup(TFullBackupMetadata::TPtr fb) {
    FullBackups.emplace(fb->SnapshotVts, fb);
}

void TMetadata::SetVersion(ui64 version) {
    Version = version;
}

bool TMetadata::HasVersion() const {
    return Version.Defined();
}

ui64 TMetadata::GetVersion() const {
    return *Version;
}

TString TMetadata::Serialize() const {
    NJson::TJsonMap m;
    if (Version.Defined()) {
        m["version"] = *Version;
    }

    NJson::TJsonArray fullBackups;
    for (auto &[tp, b] : FullBackups) {
        NJson::TJsonMap backupMap;
        NJson::TJsonArray vts;
        vts.AppendValue(tp.Step);
        vts.AppendValue(tp.TxId);
        backupMap["snapshot_vts"] = std::move(vts);
        fullBackups.AppendValue(std::move(backupMap));
    }
    m["full_backups"] = fullBackups;
    return NJson::WriteJson(&m, false);
}

TMetadata TMetadata::Deserialize(const TString& metadata) {
    NJson::TJsonValue json;
    NJson::ReadJsonTree(metadata, &json);
    const auto& value = json["version"];

    TMetadata result;
    if (value.IsUInteger()) {
        result.Version = value.GetUIntegerSafe();
    }

    return result;
}

bool TSchemaMapping::Deserialize(const TString& jsonContent, TString& error) {
    NJson::TJsonValue json;
    if (!NJson::ReadJsonTree(jsonContent, &json)) {
        error = "Failed to parse schema mapping json";
        return false;
    }
    const NJson::TJsonValue& mapping = json["exportedObjects"];
    if (!mapping.IsMap()) {
        error = "No mapping in mapping json";
        return false;
    }

    bool hasIV = false;
    bool first = true;
    for (const auto& [exportObject, info] : mapping.GetMap()) {
        const NJson::TJsonValue& exportPrefix = info["exportPrefix"];
        const NJson::TJsonValue& iv = info["iv"];
        if (!exportPrefix.IsString()) {
            error = "Incorrect exportPrefix";
            return false;
        }
        if (first) {
            hasIV = info.Has("iv");
        } else {
            first = false;
            if (hasIV != info.Has("iv")) {
                error = "Incorrect iv in schema mapping json";
                return false;
            }
        }
        if (hasIV && !iv.IsString()) {
            error = "IV in schema mapping json is not a string";
            return false;
        }
        TItem& item = Items.emplace_back();
        item.ExportPrefix = exportPrefix.GetString();
        item.ObjectPath = exportObject;
        if (hasIV) {
            item.IV = TEncryptionIV::FromHexString(iv.GetString());
        }
    }
    return true;
}

}
