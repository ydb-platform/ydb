#include "index_metadata.h"

#include <ydb/core/base/table_index.h>

#include <util/generic/hash.h>
#include <util/string/builder.h>
#include <util/string/printf.h>

#include <algorithm>

namespace NKikimr::NBackup {

namespace {

// Compact relevance has docs/stats settings but no dictionary. Before per-table settings
// were introduced, all relevance indexes had a dictionary and exported only the main settings.
std::span<const std::string_view> GetBackupImplTables(const Ydb::Table::TableIndex& index) {
    const auto& relevance = index.global_fulltext_relevance_index();
    const bool compact = !relevance.has_dict_table_settings()
        && (relevance.has_docs_table_settings() || relevance.has_stats_table_settings());
    const auto type = NTableIndex::TryConvertIndexType(index.type_case(), compact);
    if (!type || index.has_global_async_index() || index.has_global_hnsw_index()) {
        return {};
    }
    const TVector<TString> keys(index.index_columns().begin(), index.index_columns().end());
    return NTableIndex::GetImplTables(*type, keys);
}

} // namespace

TVector<TIndexMetadata> MakeIndexesMetadata(
    const google::protobuf::RepeatedPtrField<Ydb::Table::TableIndex>& indexes,
    ui32 firstExportPrefixNumber,
    bool encrypted)
{
    TVector<TIndexMetadata> result;
    ui32 exportPrefixNumber = firstExportPrefixNumber;
    for (const auto& index : indexes) {
        for (const auto& implTable : GetBackupImplTables(index)) {
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

TString GetIndexMetadataIssue(const google::protobuf::RepeatedPtrField<Ydb::Table::TableIndex>& indexes,
        std::span<const TIndexMetadata> materialized) {
    THashMap<TStringBuf, THashSet<TStringBuf>> listedTables;
    for (const auto& entry : materialized) {
        const TStringBuf path(entry.ImplTablePrefix);
        listedTables[path.Before('/')].insert(path.After('/'));
    }
    for (const auto& index : indexes) {
        const auto* listed = listedTables.FindPtr(index.name());
        if (!listed) {
            // Definition-only indexes have no entries and can be rebuilt.
            continue;
        }
        const auto expected = GetBackupImplTables(index);
        if (expected.empty()) {
            continue;
        }
        for (const auto& table : expected) {
            if (!listed->contains(table)) {
                return TStringBuilder() << "materialized index data is missing from the backup: " << index.name() << "/" << table;
            }
        }
        for (const auto& table : *listed) {
            if (std::find(expected.begin(), expected.end(), std::string_view(table)) == expected.end()) {
                return TStringBuilder() << "unexpected materialized index table in the backup: " << index.name() << "/" << table;
            }
        }
    }
    return {};
}

} // namespace NKikimr::NBackup
