#pragma once

#include "volume_tx.h"

namespace NYdb::NBS::NStorage {

////////////////////////////////////////////////////////////////////////////////

// Transaction type ids for the volume tablet, one per local-database
// transaction.
struct TVolumeCounters
{
    enum ETransactionType
    {
#define BLOCKSTORE_TRANSACTION_TYPE(name, ...) TX_##name,

        BLOCKSTORE_VOLUME_TRANSACTIONS(BLOCKSTORE_TRANSACTION_TYPE) TX_SIZE

#undef BLOCKSTORE_TRANSACTION_TYPE
    };
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NStorage
