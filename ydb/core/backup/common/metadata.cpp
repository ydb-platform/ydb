#include "metadata.h"

#include <ydb/core/base/path.h>
#include <ydb/core/base/table_index.h>

#include <library/cpp/json/json_writer.h>
#include <library/cpp/json/json_reader.h>

#include <util/generic/yexception.h>
#include <util/string/builder.h>
#include <util/string/printf.h>

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

void TMetadata::AddChangefeed(const TChangefeedMetadata& changefeed) {
    if (!Changefeeds) {
        Changefeeds.emplace();
    }
    Changefeeds->push_back(changefeed);
}

const std::optional<std::vector<TChangefeedMetadata>>& TMetadata::GetChangefeeds() const {
    return Changefeeds;
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

TVector<TIndexMetadata> MakeIndexesMetadata(
    const google::protobuf::RepeatedPtrField<Ydb::Table::TableIndex>& indexes,
    ui32 firstExportPrefixNumber,
    bool encrypted,
    bool enableFulltextCompact)
{
    TVector<TIndexMetadata> result;
    ui32 exportPrefixNumber = firstExportPrefixNumber;
    for (const auto& index : indexes) {
        const auto indexType = NTableIndex::TryConvertIndexType(index.type_case(), enableFulltextCompact);
        if (!indexType) {
            continue;
        }

        const TVector<TString> indexColumns(index.index_columns().begin(), index.index_columns().end());
        for (const auto& implTable : NTableIndex::GetImplTables(*indexType, indexColumns)) {
            const TString implTablePrefix = TStringBuilder() << index.name() << "/" << implTable;
            TString exportPrefix;
            if (encrypted) {
                // Anonymize index name in export
                exportPrefix = Sprintf("%03u", static_cast<unsigned>(exportPrefixNumber++));
            } else {
                exportPrefix = implTablePrefix;
            }

            result.push_back(TIndexMetadata{
                .ExportPrefix = std::move(exportPrefix),
                .ImplTablePrefix = implTablePrefix,
            });
        }
    }
    return result;
}

void TMetadata::SetEnablePermissions(bool enablePermissions) {
    EnablePermissions = enablePermissions;
}

bool TMetadata::HasEnablePermissions() const {
    return EnablePermissions.has_value();
}

bool TMetadata::GetEnablePermissions() const {
    return *EnablePermissions;
}

TString TMetadata::Serialize() const {
    NJson::TJsonMap m;
    if (Version.Defined()) {
        m["version"] = *Version;
    }
    if (EnablePermissions) {
        m["permissions"] = static_cast<int>(*EnablePermissions);
    }

    NJson::TJsonArray fullBackups;
    for (const auto& [tp, b] : FullBackups) {
        NJson::TJsonMap backupMap;
        NJson::TJsonArray vts;
        vts.AppendValue(tp.Step);
        vts.AppendValue(tp.TxId);
        backupMap["snapshot_vts"] = std::move(vts);
        fullBackups.AppendValue(std::move(backupMap));
    }
    m["full_backups"] = fullBackups;

    NJson::TJsonArray changefeeds;
    if (Changefeeds) {
        for (const auto& changefeed : *Changefeeds) {
            NJson::TJsonMap changefeedMap;
            changefeedMap["prefix"] = changefeed.ExportPrefix;
            changefeedMap["name"] = changefeed.Name;
            changefeeds.AppendValue(std::move(changefeedMap));
        }
    }
    m["changefeeds"] = changefeeds;

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

    if (json.Has("permissions")) {
        const int val = json["permissions"].GetInteger();
        result.EnablePermissions = val != 0;
    }

    if (json.Has("changefeeds")) {
        // Changefeeds can be absent in older versions of metadata
        result.Changefeeds.emplace(); // explicitly say that the listing of changefeeds is throgh metadata
        const NJson::TJsonValue& changefeeds = json["changefeeds"];
        for (const NJson::TJsonValue& changefeed : changefeeds.GetArray()) {
            result.AddChangefeed({
                .ExportPrefix = changefeed["prefix"].GetString(),
                .Name = changefeed["name"].GetString(),
            });
        }
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

TString TSchemaMapping::Serialize() const {
    TString content;
    TStringOutput ss(content);
    NJson::TJsonWriter writer(&ss, false);

    writer.OpenMap();
    writer.WriteKey("exportedObjects");
    writer.OpenMap();
    for (const auto& item : Items) {
        writer.WriteKey(item.ObjectPath);
        writer.OpenMap();
        writer.Write("exportPrefix", item.ExportPrefix);
        if (item.IV) {
            writer.Write("iv", item.IV->GetHexString());
        }
        if (!item.IndexIVs.empty()) {
            writer.WriteKey("indexIVs");
            writer.OpenMap();
            for (const auto& [path, iv] : item.IndexIVs) {
                writer.Write(path, iv.GetHexString());
            }
            writer.CloseMap();
        }
        writer.CloseMap();
    }
    writer.CloseMap();
    writer.CloseMap();

    writer.Flush();
    ss.Flush();
    return content;
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
        if (info.Has("indexIVs")) {
            const auto& indexIVs = info["indexIVs"];
            if (!hasIV || !indexIVs.IsMap()) {
                error = "Incorrect indexIVs in schema mapping json";
                return false;
            }
            for (const auto& [path, indexIV] : indexIVs.GetMap()) {
                if (!indexIV.IsString()) {
                    error = "Index IV in schema mapping json is not a string";
                    return false;
                }
                try {
                    auto parsedIV = TEncryptionIV::FromHexString(indexIV.GetString());
                    Y_ENSURE(parsedIV.Size() == TEncryptionIV::SIZE, "Invalid index IV size");
                    item.IndexIVs.emplace(path, std::move(parsedIV));
                } catch (const std::exception& ex) {
                    error = TStringBuilder() << "Invalid index IV in schema mapping json: " << ex.what();
                    return false;
                }
            }
        }
    }
    return true;
}

TString NormalizeItemPath(const TString& path) {
    TString result = CanonizePath(path);
    if (result.size() > 1 && result.front() == '/') {
        result.erase(0, 1);
    }
    return result;
}

TString NormalizeItemPrefix(TString prefix) {
    // Cut slshes from the beginning and from the end
    size_t toCut = 0;
    while (toCut < prefix.size() && prefix[toCut] == '/') {
        ++toCut;
    }
    if (toCut) {
        prefix.erase(0, toCut);
    }

    while (prefix && prefix.back() == '/') {
        prefix.pop_back();
    }
    return prefix;
}

TString NormalizeExportPrefix(TString prefix) {
    while (prefix && prefix.back() == '/') {
        prefix.pop_back();
    }
    return prefix;
}

}
