#pragma once

#include <ydb/core/protos/tx_datashard.pb.h>

namespace NKikimr::NSchemeShard {

// Single source of truth for "did this scan termination warrant a retry?"
// DS reports the cause (EEndStatus); SS owns the policy.
inline bool ShouldRetryIncrementalRestore(NKikimrTxDataShard::TEvIncrementalRestoreShardProgress::EEndStatus s) {
    using TProgress = NKikimrTxDataShard::TEvIncrementalRestoreShardProgress;
    switch (s) {
        case TProgress::END_SUCCESS:           return false;
        case TProgress::END_TRANSIENT_FAILURE: return true;
        case TProgress::END_FATAL_FAILURE:     return false;
        case TProgress::END_UNSPECIFIED:       return false;
    }
    return false;
}

}
