#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/storage/dbs_controller/dbs_controller_tx.h>

namespace NYdb::NBS::NBlockStore::NStorage::NDbsController {

////////////////////////////////////////////////////////////////////////////////

struct TDbsControllerCounters
{
    enum ETransactionType
    {
#define BLOCKSTORE_TRANSACTION_TYPE(name, ...) TX_##name,

        BLOCKSTORE_DBS_CONTROLLER_TRANSACTIONS(BLOCKSTORE_TRANSACTION_TYPE)
        TX_SIZE

#undef BLOCKSTORE_TRANSACTION_TYPE
    };
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NDbsController
