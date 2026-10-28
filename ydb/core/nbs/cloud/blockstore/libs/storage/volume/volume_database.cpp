#include "volume_database.h"

#include "volume_schema.h"

namespace NYdb::NBS::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 TabletInfoRowId = 1;

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TVolumeDatabase::InitSchema()
{
    Materialize<TVolumeSchema>();

    NBlockStore::NStorage::TSchemaInitializer<
        TVolumeSchema::TTables>::InitStorage(Database.Alter());
}

////////////////////////////////////////////////////////////////////////////////

bool TVolumeDatabase::ReadPartitionTabletId(ui64* partitionTabletId)
{
    using TTable = TVolumeSchema::TabletInfo;

    auto it = Table<TTable>()
                  .Key(TabletInfoRowId)
                  .Select<TTable::PartitionTabletId>();

    if (!it.IsReady()) {
        return false;
    }

    if (it.IsValid() && it.HaveValue<TTable::PartitionTabletId>()) {
        *partitionTabletId = it.GetValue<TTable::PartitionTabletId>();
    } else {
        *partitionTabletId = 0;
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeDatabase::StorePartitionTabletId(ui64 partitionTabletId)
{
    using TTable = TVolumeSchema::TabletInfo;

    Table<TTable>()
        .Key(TabletInfoRowId)
        .Update(NKikimr::NIceDb::TUpdate<TTable::PartitionTabletId>(
            partitionTabletId));
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NStorage
