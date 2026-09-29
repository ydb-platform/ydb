#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/storage/core/tablet_schema.h>

#include <ydb/core/tablet_flat/flat_cxx_database.h>

namespace NYdb::NBS::NStorage {

////////////////////////////////////////////////////////////////////////////////

// Local DB schema of the volume tablet.
struct TVolumeSchema: public NKikimr::NIceDb::Schema
{
    // The single-row table with the id of the partition tablet.
    struct Meta: public NBlockStore::NStorage::TTableSchema<1>
    {
        struct Id: public Column<1, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        struct PartitionTabletId
            : public Column<2, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        using TKey = TableKey<Id>;
        using TColumns = TableColumns<Id, PartitionTabletId>;
    };

    using TTables = SchemaTables<Meta>;

    using TSettings =
        SchemaSettings<ExecutorLogBatching<true>, ExecutorLogFlushPeriod<0>>;
};

}   // namespace NYdb::NBS::NStorage
