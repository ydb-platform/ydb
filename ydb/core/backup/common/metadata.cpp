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

void TMetadata::AddIndex(const TIndexMetadata& index) {
    if (!Indexes) {
        Indexes.emplace();
    }
    Indexes->push_back(index);
}

const std::optional<std::vector<TIndexMetadata>>& TMetadata::GetIndexes() const {
    return Indexes;
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

    NJson::TJsonArray indexes;
    if (Indexes) {
        for (const auto& index : *Indexes) {
            NJson::TJsonMap indexMap;
            indexMap["export_prefix"] = index.ExportPrefix;
            indexMap["impl_table_prefix"] = index.ImplTablePrefix;
            indexes.AppendValue(std::move(indexMap));
        }
    }
    m["indexes"] = indexes;

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

    if (json.Has("indexes")) {
        result.Indexes.emplace();
        const NJson::TJsonValue& indexes = json["indexes"];
        for (const NJson::TJsonValue& index : indexes.GetArray()) {
            result.AddIndex({
                .ExportPrefix = index["export_prefix"].GetString(),
                .ImplTablePrefix = index["impl_table_prefix"].GetString(),
            });
        }
    }

    return result;
}

}
