#include "index_metadata.h"

#include <ydb/core/base/table_index.h>

#include <util/string/builder.h>
#include <util/string/printf.h>

namespace NKikimr::NBackup {

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

} // namespace NKikimr::NBackup
