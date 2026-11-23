#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/storage/core/tablet_schema.h>

#include <ydb/core/tablet_flat/flat_cxx_database.h>

namespace NYdb::NBS::NStorage {

////////////////////////////////////////////////////////////////////////////////

// Local database schema of the NBS 2.0 volume tablet.
struct TVolumeSchema: public NKikimr::NIceDb::Schema
{
    // One row (Id = 1) with the partition tablet id last stored by the volume.
    struct TabletInfo: public NBlockStore::NStorage::TTableSchema<1>
    {
        struct Id: public Column<1, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        // Tablet id of the single partition, as last received in
        // UpdateVolumeConfig. 0 means not known yet: the volume has not
        // received any UpdateVolumeConfig.
        struct PartitionTabletId
            : public Column<2, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        using TKey = TableKey<Id>;
        using TColumns = TableColumns<Id, PartitionTabletId>;
    };

    using TTables = SchemaTables<TabletInfo>;

    using TSettings =
        SchemaSettings<ExecutorLogBatching<true>, ExecutorLogFlushPeriod<0>>;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NStorage
