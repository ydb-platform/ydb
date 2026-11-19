#include "index_metadata.h"

#include <ydb/core/base/table_index.h>

#include <util/string/builder.h>
#include <util/string/printf.h>

namespace NKikimr::NBackup {

namespace {

// FillIndexDescription emits dictionary settings only for a stored non-compact
// relevance index. The two layouts of plain fulltext and JSON use the same table names.
std::span<const std::string_view> GetBackupImplTables(const Ydb::Table::TableIndex& index) {
    const bool compact = index.has_global_fulltext_relevance_index()
        && !index.global_fulltext_relevance_index().has_dict_table_settings();
    const auto type = NTableIndex::TryConvertIndexType(index.type_case(), compact);
    if (!type || index.has_global_async_index()) {
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

TString FindMissingIndexImplTable(const google::protobuf::RepeatedPtrField<Ydb::Table::TableIndex>& indexes,
        std::span<const TIndexMetadata> materialized) {
    THashSet<TStringBuf> listedTables;
    THashSet<TStringBuf> listedIndexes;
    for (const auto& entry : materialized) {
        const TStringBuf path(entry.ImplTablePrefix);
        listedTables.insert(path);
        listedIndexes.insert(path.Before('/'));
    }
    for (const auto& index : indexes) {
        // An index with no materialized tables may be rebuilt from the base table.
        if (!listedIndexes.contains(index.name()) || index.has_global_hnsw_index()) {
            continue;
        }
        for (const auto& implTable : GetBackupImplTables(index)) {
            TString path = TStringBuilder() << index.name() << "/" << implTable;
            if (!listedTables.contains(path)) {
                return path;
            }
        }
    }
    return {};
}

} // namespace NKikimr::NBackup
