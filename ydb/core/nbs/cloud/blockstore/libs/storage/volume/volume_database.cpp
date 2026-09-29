#include "volume_database.h"

#include "volume_schema.h"

namespace NYdb::NBS::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

// The Meta table has a single row.
constexpr ui32 MetaRowId = 1;

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TVolumeDatabase::TVolumeDatabase(NKikimr::NTable::TDatabase& database)
    : NKikimr::NIceDb::TNiceDb(database)
{}

void TVolumeDatabase::InitSchema()
{
    Materialize<TVolumeSchema>();

    NBlockStore::NStorage::TSchemaInitializer<
        TVolumeSchema::TTables>::InitStorage(Database.Alter());
}

bool TVolumeDatabase::ReadPartitionTabletId(ui64* partitionTabletId)
{
    using TTable = TVolumeSchema::Meta;

    auto it =
        Table<TTable>().Key(MetaRowId).Select<TTable::PartitionTabletId>();

    if (!it.IsReady()) {
        return false;
    }

    if (it.IsValid()) {
        *partitionTabletId = it.GetValueOrDefault<TTable::PartitionTabletId>(0);
    }

    return true;
}

void TVolumeDatabase::StorePartitionTabletId(ui64 partitionTabletId)
{
    using TTable = TVolumeSchema::Meta;

    Table<TTable>().Key(MetaRowId).Update(
        NKikimr::NIceDb::TUpdate<TTable::PartitionTabletId>(partitionTabletId));
}

}   // namespace NYdb::NBS::NStorage
