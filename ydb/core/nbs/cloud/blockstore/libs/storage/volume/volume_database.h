#pragma once

#include <ydb/core/tablet_flat/flat_cxx_database.h>

#include <util/system/types.h>

namespace NYdb::NBS::NStorage {

////////////////////////////////////////////////////////////////////////////////

// Read and write access to the volume tablet local database.
class TVolumeDatabase final: public NKikimr::NIceDb::TNiceDb
{
public:
    explicit TVolumeDatabase(NKikimr::NTable::TDatabase& database)
        : NKikimr::NIceDb::TNiceDb(database)
    {}

    // Creates the volume tables. Safe to call again on an existing schema.
    void InitSchema();

    // Writes the stored id into partitionTabletId. Returns false when the
    // read is not ready and must be retried. A missing row yields 0.
    bool ReadPartitionTabletId(ui64* partitionTabletId);

    // Persists the partition tablet id, replacing any previously stored one.
    void StorePartitionTabletId(ui64 partitionTabletId);
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NStorage
