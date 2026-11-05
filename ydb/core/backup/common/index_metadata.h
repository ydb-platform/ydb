#pragma once

#include "metadata.h"

#include <ydb/public/api/protos/ydb_table.pb.h>

#include <util/generic/vector.h>

namespace NKikimr::NBackup {

// Index impl tables of a table as listed in its metadata.json.
// SchemeShard uses the same function to choose the export prefixes of separate
// index impl-table items, so both sides must compute the list identically.
// firstExportPrefixNumber is used for anonymized (encrypted) prefixes, which
// share the numbering with changefeed prefixes.
TVector<TIndexMetadata> MakeIndexesMetadata(
    const google::protobuf::RepeatedPtrField<Ydb::Table::TableIndex>& indexes,
    ui32 firstExportPrefixNumber,
    bool encrypted,
    bool enableFulltextCompact);

} // namespace NKikimr::NBackup
