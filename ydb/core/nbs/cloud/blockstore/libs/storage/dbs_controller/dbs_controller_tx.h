#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/storage/dbs_controller/protos/dbs_controller.pb.h>

#include <util/generic/maybe.h>

namespace NYdb::NBS::NBlockStore::NStorage::NDbsController {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_DBS_CONTROLLER_TRANSACTIONS(xxx, ...) \
    xxx(InitSchema, __VA_ARGS__)                         \
    xxx(LoadState, __VA_ARGS__)

// BLOCKSTORE_DBS_CONTROLLER_TRANSACTIONS

////////////////////////////////////////////////////////////////////////////////

struct TTxDbsController
{
    //
    // InitSchema
    //
    struct TInitSchema
    {
        explicit TInitSchema()
        {}

        void Clear()
        {}
    };

    //
    // LoadState
    //
    struct TLoadState
    {
        explicit TLoadState()
        {}

        void Clear()
        {}
    };
};

}   // namespace NYdb::NBS::NBlockStore::NStorage::NDbsController
