#pragma once

#include "public.h"

#include <library/cpp/yt/misc/enum.h>

namespace NYT::NCellMasterClient {

///////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TCellDirectory;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

// Keep these two enums consistent.

DEFINE_BIT_ENUM(EMasterCellRoles,
    ((None)                      (0x0000))
    ((CypressNodeHost)           (0x0001))
    ((TransactionCoordinator)    (0x0002))
    ((ChunkHost)                 (0x0004))
    ((DedicatedChunkHost)        (0x0008))
    ((ExTransactionCoordinator)  (0x0010))
);

DEFINE_ENUM(EMasterCellRole,
    ((CypressNodeHost)           (0x0001))
    ((TransactionCoordinator)    (0x0002))
    ((ChunkHost)                 (0x0004))
    ((DedicatedChunkHost)        (0x0008))
    ((ExTransactionCoordinator)  (0x0010))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMasterClient
