#pragma once

#include <ydb/core/tablet_flat/flat_cxx_database.h>

namespace NKikimr::NDataShard {

// Separate from the user-table schema and logical secondary-index metadata.
struct THnswSchema : NIceDb::Schema {
    struct LocalHnswIndexes : Table<43> {
        struct TablePathId : Column<1, NScheme::NTypeIds::Uint64> {};
        struct IndexOwnerId : Column<2, NScheme::NTypeIds::Uint64> {};
        struct IndexLocalId : Column<3, NScheme::NTypeIds::Uint64> {};
        struct SchemaVersion : Column<4, NScheme::NTypeIds::Uint64> {};
        struct Description : Column<5, NScheme::NTypeIds::String> { using Type = TString; };

        using TKey = TableKey<TablePathId, IndexOwnerId, IndexLocalId>;
        using TColumns = TableColumns<TablePathId, IndexOwnerId, IndexLocalId, SchemaVersion, Description>;
    };
};

} // namespace NKikimr::NDataShard
