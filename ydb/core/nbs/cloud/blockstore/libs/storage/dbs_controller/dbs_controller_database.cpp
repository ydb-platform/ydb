#include "dbs_controller_database.h"

#include "dbs_controller_schema.h"

namespace NYdb::NBS::NBlockStore::NStorage::NDbsController {

////////////////////////////////////////////////////////////////////////////////

void TDbsControllerDatabase::InitSchema()
{
    Materialize<TDbsControllerSchema>();

    TSchemaInitializer<TDbsControllerSchema::TTables>::InitStorage(
        Database.Alter());
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NDbsController
