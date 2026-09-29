#pragma once

#include <ydb/core/tablet_flat/flat_cxx_database.h>

namespace NYdb::NBS::NStorage {

////////////////////////////////////////////////////////////////////////////////

// Reads and writes the volume tablet's local DB.
class TVolumeDatabase final: public NKikimr::NIceDb::TNiceDb
{
public:
    explicit TVolumeDatabase(NKikimr::NTable::TDatabase& database);

    // Creates the tables of TVolumeSchema.
    void InitSchema();

    // Reads the stored partition tablet id; returns false when the data is not
    // in memory yet. The id stays 0 when nothing was stored.
    bool ReadPartitionTabletId(ui64* partitionTabletId);

    // Writes the partition tablet id.
    void StorePartitionTabletId(ui64 partitionTabletId);
};

}   // namespace NYdb::NBS::NStorage
