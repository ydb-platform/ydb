#pragma once

#include <ydb/core/protos/tx_datashard.pb.h>

namespace NKikimr::NSchemeShard {

// Single source of truth for "did this scan termination warrant a retry?"
// DS reports the cause (EOpEndStatus); SS owns the policy.
inline bool ShouldRetryIncrementalRestore(NKikimrTxDataShard::EOpEndStatus s) {
    using namespace NKikimrTxDataShard;
    switch (s) {
        case END_SUCCESS:           return false;
        case END_TRANSIENT_FAILURE: return true;
        case END_FATAL_FAILURE:     return false;
        case END_UNSPECIFIED:       return false;
    }
    return false;
}

}
