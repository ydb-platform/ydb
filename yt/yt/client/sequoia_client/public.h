#pragma once

#include <yt/yt/core/misc/error_code.h>

namespace NYT::NSequoiaClient {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_ERROR_ENUM(
    ((SequoiaClientNotReady)                (6000))
    ((SequoiaTableCorrupted)                (6001))
    ((SequoiaRetriableError)                (6002))
    ((InvalidSequoiaReign)                  (6003))
    ((TransactionActionFailedOnMasterCell)  (6004))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient
