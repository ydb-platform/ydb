#include "part_database.h"

#include "part_schema.h"

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

void TPartitionDatabase::InitSchema()
{
    Materialize<TPartitionSchema>();

    TSchemaInitializer<TPartitionSchema::TTables>::InitStorage(
        Database.Alter());
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionDatabase::WriteMeta(const TString& meta)
{
    using TTable = TPartitionSchema::Meta;

    Table<TTable>().Key(1).Update(
        NKikimr::NIceDb::TUpdate<TTable::PartitionMeta>(meta));
}

bool TPartitionDatabase::ReadMeta(TMaybe<TString>& meta)
{
    using TTable = TPartitionSchema::Meta;

    auto it = Table<TTable>().Key(1).Select();

    if (!it.IsReady()) {
        return false;   // not ready
    }

    if (it.IsValid()) {
        meta = it.GetValue<TTable::PartitionMeta>();
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
