#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/storage/dbs_controller/protos/dbs_controller.pb.h>

#include <ydb/core/tablet_flat/flat_cxx_database.h>

#include <util/generic/maybe.h>

namespace NYdb::NBS::NBlockStore::NStorage::NDbsController {

////////////////////////////////////////////////////////////////////////////////

class TDbsControllerDatabase: public NKikimr::NIceDb::TNiceDb
{
    using TDbsControllerState =
        ::NYdb::NBS::DbsController::NProto::TDbsControllerState;

public:
    TDbsControllerDatabase(NKikimr::NTable::TDatabase& database)
        : NKikimr::NIceDb::TNiceDb(database)
    {}

    void InitSchema();
};

}   // namespace NYdb::NBS::NBlockStore::NStorage::NDbsController
